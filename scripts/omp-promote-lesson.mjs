const routes = Object.freeze({
  review: 'xd://mcp__peers_review_lesson',
  bind: 'xd://mcp__aletheia_bind_workspace',
  search: 'xd://mcp__aletheia_search_observations',
  get: 'xd://mcp__aletheia_get_observation',
  save: 'xd://mcp__aletheia_save_observation',
});

function normalize(value) {
  for (let depth = 0; depth < 8; depth++) {
    if (typeof value === 'string') {
      try { value = JSON.parse(value); } catch { throw new Error('Tool returned non-JSON application content'); }
      continue;
    }
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Tool returned a non-object result');
    if (value.hasError || value.isError || value.error != null) throw new Error(`Tool application error: ${JSON.stringify(value)}`);
    if (typeof value.text === 'string') { value = value.text; continue; }
    if (value.details && typeof value.details === 'object') { value = value.details; continue; }
    return value;
  }
  throw new Error('Tool result nesting exceeds supported envelope');
}

/** Explicit, one-grant promotion through the caller's existing mounted tool/approval bridge. */
export async function promoteLesson(callTool, input) {
  if (typeof callTool !== 'function' || !input || typeof input.channel_id !== 'string' || !input.proposal)
    throw new Error('promoteLesson requires callTool and {channel_id,proposal}');
  const call = async (route, args) => normalize(await callTool(route, args));
  const review = args => call(routes.review, { channel_id: input.channel_id, ...args });
  let prepared = await review({ action: 'prepare', proposal: input.proposal });
  if (prepared.status === 'no_entry') return prepared;
  if (!['prepared', 'saving', 'unknown', 'saved', 'duplicate'].includes(prepared.status) ||
      !/^peers-lesson:[0-9a-f]{64}$/.test(prepared.marker) || !prepared.binding_request ||
      typeof prepared.body !== 'string' || !Array.isArray(prepared.tags))
    throw new Error('Malformed frozen lesson intent');
  const binding = await call(routes.bind, prepared.binding_request);
  if (typeof binding.workspaceBindingId !== 'string' || !binding.workspaceBindingId ||
      typeof binding.identityConfidence !== 'string' || !binding.identityConfidence ||
      typeof binding.bindingStatus !== 'string' || !binding.bindingStatus || binding.bindingStatus === 'quarantined')
    throw new Error('Workspace binding is malformed or quarantined');
  let attempt = prepared.attempt_id ?? null;
  const reconcile = async () => {
    const search = await call(routes.search, {
      query: prepared.marker, workspaceBindingId: binding.workspaceBindingId, profileScope: prepared.scope, limit: 200,
    });
    if (search.query !== prepared.marker || !Number.isInteger(search.count) || !Array.isArray(search.results) ||
        search.count !== search.results.length)
      throw new Error('Malformed scoped marker search result');
    if (search.results.length >= 200) throw new Error('Full marker search page cannot establish completeness');
    const observations = [];
    for (const row of search.results) {
      if (!row || typeof row.observationId !== 'string') throw new Error('Marker search result lacks observationId');
      const observation = await call(routes.get, { observationId: row.observationId });
      if (observation.observationId !== row.observationId) throw new Error('Observation hydration identity mismatch');
      observations.push(observation);
    }
    return review({ action: 'reconcile', binding_result: binding, ...(attempt ? { attempt_id: attempt } : {}), result: { observations } });
  };
  let reconciled = await reconcile();
  if (reconciled.status !== 'prepared' || attempt) return reconciled;
  const grant = await review({ action: 'begin_save', binding_result: binding });
  attempt = grant.attempt_id ?? attempt;
  if (!grant.dispatch_allowed) return reconcile();
  if (!attempt || !grant.save_request || grant.status !== 'saving') throw new Error('Malformed one-dispatch save grant');
  const request = grant.save_request;
  if (request.workspaceBindingId !== binding.workspaceBindingId || request.body !== prepared.body ||
      request.profileScope !== prepared.scope || !Array.isArray(request.tags) ||
      request.tags.length !== prepared.tags.length || request.tags.some((tag, i) => tag !== prepared.tags[i]) ||
      Object.keys(request).some(key => !['workspaceBindingId', 'body', 'profileScope', 'tags'].includes(key)))
    throw new Error('Granted save payload differs from frozen lesson');
  let stage = 'save';
  try {
    // Exactly one dispatch by this invocation. The mounted bridge itself can retry; reconcile detects visible duplicates.
    const saved = await call(routes.save, request);
    if (typeof saved.observationId !== 'string' || !saved.observationId) throw new Error('Save response lacks observationId');
    stage = 'verify';
    const observation = await call(routes.get, { observationId: saved.observationId });
    const recorded = await review({ action: 'record', attempt_id: attempt, result: { save_result: saved, observation } });
    if (recorded.status === 'unknown') throw new Error(recorded.failure ?? 'Save verification did not match frozen intent');
    reconciled = await reconcile();
    if (reconciled.status === 'saved' && reconciled.observation_id !== saved.observationId)
      throw new Error('Reconciled observation differs from returned save candidate');
    return reconciled;
  } catch (error) {
    let unknown = { status: 'unknown', lesson_id: prepared.lesson_id, marker: prepared.marker, attempt_id: attempt,
      identity_confidence: binding.identityConfidence, failure: `${stage}: ${error instanceof Error ? error.message : String(error)}` };
    try { unknown = await review({ action: 'record', attempt_id: attempt, result: { failure: { stage, message: unknown.failure } } }); }
    catch { /* A lost record response never authorizes another save. */ }
    try { return await reconcile(); }
    catch { return unknown; }
  }
}
