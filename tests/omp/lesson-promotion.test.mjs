import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { promoteLesson } from '../../scripts/omp-promote-lesson.mjs';

function fixture(options = {}) {
  const lessonId = createHash('sha256').update('reviewed fixture lesson').digest('hex');
  const marker = `peers-lesson:${lessonId}`;
  const body = 'Cue: zero-exit filtered test\nLesson: inspect test counts';
  const tags = [marker, 'source:peers', 'kind:workflow-lesson'];
  const binding = { workspaceBindingId: 'wb_' + '1'.repeat(32), identityConfidence: 'low', bindingStatus: 'observed' };
  let status = 'prepared', attempt = null, candidate = null, saves = 0, searches = 0, blocked = false;
  const observations = [];
  const input = { channel_id: 'fixture-channel', proposal: { cue: 'filtered tests', lesson: 'inspect counts' } };
  const output = () => ({ status, lesson_id: lessonId, marker, body, tags, scope: 'project',
    binding_request: { workingDir: '/fixture', gitRemoteUrl: null, gitFirstCommitSha: 'a'.repeat(40) },
    attempt_id: attempt, observation_id: observations.length === 1 ? observations[0].observationId : null,
    observation_ids: observations.map(o => o.observationId), identity_confidence: binding.identityConfidence });
  async function call(path, args) {
    if (path.endsWith('peers_review_lesson')) {
      switch (args.action) {
        case 'prepare': return options.noEntry ? { status: 'no_entry', reason: 'confounded' } : output();
        case 'begin_save':
          if (attempt) return { ...output(), dispatch_allowed: false };
          attempt = 'attempt-1'; status = 'saving';
          if (options.lostGrant) throw Error('grant response lost');
          return { ...output(), dispatch_allowed: true, save_request: { workspaceBindingId: binding.workspaceBindingId, body, profileScope: 'project', tags } };
        case 'record':
          if (args.result.failure) status = 'unknown';
          else { candidate = args.result.save_result.observationId; status = 'saving'; }
          return output();
        case 'reconcile': {
          const rows = args.result.observations;
          for (const row of rows) assert.equal(row.body, body);
          if (rows.length > 1) status = 'duplicate';
          else if (rows.length === 1 && (!candidate || rows[0].observationId === candidate)) status = 'saved';
          else status = attempt ? 'unknown' : 'prepared';
          return output();
        }
        default: throw Error('Unexpected review action');
      }
    }
    if (path.endsWith('bind_workspace')) return options.bindingError ?? binding;
    if (path.endsWith('search_observations')) {
      searches++;
      assert.equal(args.workspaceBindingId, binding.workspaceBindingId);
      assert.equal(args.profileScope, 'project');
      assert.equal(args.query, marker);
      if (blocked) throw Error('temporary reconciliation outage');
      if (options.fullPage) return { query: marker, count: 200, results: Array.from({ length: 200 }, (_, i) => ({ observationId: `obs_${i}` })) };
      return { query: marker, count: observations.length, results: observations.map(o => ({ observationId: o.observationId })) };
    }
    if (path.endsWith('get_observation')) {
      if (blocked) throw Error('temporary reconciliation outage');
      if (options.getError) return { error: 'OBSERVATION_NOT_FOUND', observationId: args.observationId };
      return observations.find(o => o.observationId === args.observationId);
    }
    if (path.endsWith('save_observation')) {
      saves++;
      assert.deepEqual(Object.keys(args).sort(), ['body', 'profileScope', 'tags', 'workspaceBindingId']);
      if (options.saveError) return { text: JSON.stringify({ error: 'SAVE_FAILED' }), hasError: false };
      if (!options.zeroMatches) {
        const count = options.bridgeDoubleInsert ? 2 : 1;
        for (let i = 0; i < count; i++) observations.push({ observationId: `obs_${observations.length + 1}`, workspaceBindingId: binding.workspaceBindingId,
          profileScope: 'project', body, tags, occurredAt: '2026-01-01T00:00:00Z' });
      }
      if (options.lostSave) { blocked = true; throw Error('committed response lost'); }
      return { text: JSON.stringify({ observationId: observations[0]?.observationId ?? 'obs_ambiguous', profileScope: 'project',
        occurredAt: '2026-01-01T00:00:00Z', relations: { warnings: ['link warning does not undo save'] } }), hasError: false };
    }
    throw Error('Unexpected route: ' + path);
  }
  return { call, input, observations, get saves() { return saves; }, get searches() { return searches; }, unblock() { blocked = false; } };
}

test('committed lost response reconciles later without dispatching a second save', async () => {
  const f = fixture({ lostSave: true });
  assert.equal((await promoteLesson(f.call, f.input)).status, 'unknown');
  assert.equal(f.saves, 1);
  f.unblock();
  const recovered = await promoteLesson(f.call, f.input);
  assert.equal(recovered.status, 'saved');
  assert.equal(recovered.observation_id, 'obs_1');
  assert.equal(f.saves, 1);
});

test('bridge replay inserting twice returns duplicate and every visible ID', async () => {
  const f = fixture({ bridgeDoubleInsert: true });
  const result = await promoteLesson(f.call, f.input);
  assert.equal(result.status, 'duplicate');
  assert.deepEqual(result.observation_ids, ['obs_1', 'obs_2']);
  assert.equal(f.saves, 1);
});

test('saved intent rechecks marker uniqueness and discovers later duplicates', async () => {
  const f = fixture();
  assert.equal((await promoteLesson(f.call, f.input)).status, 'saved');
  f.observations.push({ ...f.observations[0], observationId: 'obs_2' });
  assert.equal((await promoteLesson(f.call, f.input)).status, 'duplicate');
  assert.equal(f.saves, 1);
});

test('zero-match ambiguity never authorizes another save', async () => {
  const f = fixture({ zeroMatches: true });
  assert.equal((await promoteLesson(f.call, f.input)).status, 'unknown');
  assert.equal((await promoteLesson(f.call, f.input)).status, 'unknown');
  assert.equal(f.saves, 1);
});

test('a lost grant response leaves its dispatch unrepeatable', async () => {
  const f = fixture({ lostGrant: true });
  await assert.rejects(promoteLesson(f.call, f.input), /grant response lost/);
  assert.equal((await promoteLesson(f.call, f.input)).status, 'unknown');
  assert.equal(f.saves, 0);
});

test('application save errors count as errors without transport isError', async () => {
  const f = fixture({ saveError: true });
  assert.equal((await promoteLesson(f.call, f.input)).status, 'unknown');
  assert.equal(f.saves, 1);
  assert.equal((await promoteLesson(f.call, f.input)).status, 'unknown');
  assert.equal(f.saves, 1);
});

test('relation warnings do not cause resave and low observed bindings survive', async () => {
  const f = fixture();
  const result = await promoteLesson(f.call, f.input);
  assert.equal(result.status, 'saved');
  assert.equal(result.identity_confidence, 'low');
  assert.equal(f.saves, 1);
});

test('confounded no_entry makes no binding/search/memory calls', async () => {
  const f = fixture({ noEntry: true });
  assert.equal((await promoteLesson(f.call, f.input)).status, 'no_entry');
  assert.equal(f.searches, 0);
  assert.equal(f.saves, 0);
});

test('quarantined/application diagnostic bindings stop before save', async () => {
  for (const bindingError of [{ error: 'INVALID_BINDING' }, '[bind_workspace: unavailable]',
    { workspaceBindingId: 'wb_fixture', identityConfidence: 'low', bindingStatus: 'quarantined' }]) {
    const f = fixture({ bindingError });
    await assert.rejects(promoteLesson(f.call, f.input));
    assert.equal(f.saves, 0);
  }
});

test('a full 200-result page does not establish uniqueness or permit save', async () => {
  const f = fixture({ fullPage: true });
  await assert.rejects(promoteLesson(f.call, f.input), /completeness/);
  assert.equal(f.saves, 0);
});

test('get application error never becomes verified save evidence', async () => {
  const f = fixture({ getError: true });
  assert.equal((await promoteLesson(f.call, f.input)).status, 'unknown');
  assert.equal(f.saves, 1);
});
