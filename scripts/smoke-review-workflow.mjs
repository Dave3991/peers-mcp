#!/usr/bin/env node
import assert from 'node:assert/strict';
import { spawn, execFileSync } from 'node:child_process';
import { createHash, randomBytes } from 'node:crypto';
import { access, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { homedir, tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import net from 'node:net';
import { promoteLesson } from './omp-promote-lesson.mjs';

const B = await realpath(resolve(dirname(fileURLToPath(import.meta.url)), '..'));
const A = '/Users/david/Documents/projects/personal/projects/Aletheia';
const flags = new Set(process.argv.slice(2));
assert([...flags].every(x => ['--with-aletheia', '--with-omp'].includes(x)), 'Unknown smoke mode');
assert(!flags.has('--with-omp') || flags.has('--with-aletheia'), '--with-omp requires --with-aletheia');
assert(process.env.PEERS_REVIEW_BIN, 'PEERS_REVIEW_BIN must select the isolated published MCP executable');
const peersBin = await realpath(process.env.PEERS_REVIEW_BIN);
await access(peersBin);
const started = Date.now();
const root = await realpath(await mkdtemp(join(tmpdir(), 'peers-review-smoke-')));
const repo = join(root, 'repo'), state = join(root, 'state');
await mkdir(repo); await mkdir(state);
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const hash = value => createHash('sha256').update(value).digest('hex');
const git = (...args) => execFileSync('git', ['-C', repo, ...args], { encoding: 'utf8' }).trimEnd();
const composeFile = join(A, 'docker/compose/docker-compose.yml');
const compose = (...args) => execFileSync('docker', ['compose', '-f', composeFile, ...args], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
async function unusedPort() {
  const server = net.createServer();
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const port = server.address().port;
  await new Promise(resolve => server.close(resolve));
  return port;
}
const port = await unusedPort();
const peersEnv = { ...process.env, PEERS_STATE_DIR: state, PEERS_DB_PATH: join(state, 'peers.db'), PEERS_PORT: String(port),
  PEERS_BROKER_PROJECT: join(B, 'src/Peers.Broker/Peers.Broker.csproj') };
delete peersEnv.PEERS_HOME;
// The broker under test is built from this checkout, so its /health contract must be BrokerContract.Current here.
const contract = Number(/const int Current = (\d+);/.exec(await readFile(join(B, 'src/Peers.Shared/BrokerRuntime.cs'), 'utf8'))?.[1]);
assert(Number.isInteger(contract), 'BrokerContract.Current not found in src/Peers.Shared/BrokerRuntime.cs');

// NDJSON framing from Aletheia/scripts/try-mcp-framed.mjs; no copied environment defaults.
class Mcp {
  constructor(bin, args, env, name) {
    this.child = spawn(bin, args, { cwd: repo, env, stdio: ['pipe', 'pipe', 'pipe'] });
    this.name = name; this.nextId = 1; this.pending = new Map(); this.buffer = ''; this.stderr = '';
    this.exit = new Promise(resolve => this.child.once('exit', resolve));
    this.child.stderr.on('data', chunk => { this.stderr = (this.stderr + chunk).slice(-65536); });
    this.child.stdout.on('data', chunk => {
      this.buffer += chunk.toString('utf8');
      for (;;) {
        const newline = this.buffer.indexOf('\n');
        if (newline < 0) break;
        const line = this.buffer.slice(0, newline).trim(); this.buffer = this.buffer.slice(newline + 1);
        if (!line) continue;
        let message;
        try { message = JSON.parse(line); } catch { continue; }
        const waiter = this.pending.get(message.id);
        if (!waiter) continue;
        this.pending.delete(message.id); clearTimeout(waiter.timer);
        message.error ? waiter.reject(new Error(JSON.stringify(message.error))) : waiter.resolve(message.result);
      }
    });
    this.child.once('error', error => this.fail(error));
    this.child.once('exit', code => this.fail(new Error(`${name} exited (${code}): ${this.stderr}`)));
  }
  fail(error) { for (const waiter of this.pending.values()) { clearTimeout(waiter.timer); waiter.reject(error); } this.pending.clear(); }
  send(message) { this.child.stdin.write(JSON.stringify(message) + '\n'); }
  request(method, params = {}) {
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error(`${this.name}: ${method} timed out: ${this.stderr}`)); }, 120000);
      this.pending.set(id, { resolve, reject, timer }); this.send({ jsonrpc: '2.0', id, method, params });
    });
  }
  async initialize() {
    await this.request('initialize', { protocolVersion: '2025-11-25', capabilities: {}, clientInfo: { name: this.name, version: '1.0.0' } });
    this.send({ jsonrpc: '2.0', method: 'notifications/initialized' }); return this;
  }
  async call(name, args = {}) {
    const result = await this.request('tools/call', { name, arguments: args });
    if (result.isError || result.hasError) throw new Error(`${name}: ${result.content?.map(x => x.text).join('\n')}`);
    const text = result.content?.filter(x => x.type === 'text').map(x => x.text).join('\n');
    if (typeof text !== 'string') throw new Error(`${name}: missing text result`);
    let parsed; try { parsed = JSON.parse(text); } catch { return text; }
    if (parsed?.error != null) throw new Error(`${name}: ${JSON.stringify(parsed)}`);
    return parsed;
  }
  async close() {
    if (this.child.exitCode !== null) return;
    this.child.stdin.end();
    await Promise.race([this.exit, sleep(5000)]);
    if (this.child.exitCode === null) { this.child.kill('SIGTERM'); await this.exit; }
  }
}
const children = new Set();
async function peer(name) { const child = new Mcp(peersBin, [], peersEnv, name); children.add(child); return child.initialize(); }
let driver, reviewer, memory, broker, ownedBroker = false, smokeDb, createdDb = false, ownedDaemon, aletheiaEnv;
function sql(database, query) {
  assert(database === 'aletheia' || /^peers_review_smoke_[0-9a-f]{24}$/.test(database));
  return compose('exec', '-T', 'postgres', 'psql', '-U', 'aletheia', '-d', database, '-Atc', query);
}
function diagnostics(text) {
  assert.equal(typeof text, 'string', 'diagnose_server returns plain key:value text, not observation JSON');
  const field = key => {
    const lines = text.split(/\r?\n/).filter(line => line.startsWith(key + ':'));
    assert.equal(lines.length, 1, `Missing/ambiguous diagnose_server ${key}`);
    return lines[0].slice(key.length + 1).trim();
  };
  const result = { pid: Number(field('pid')), process_path: field('process_path'), started_at: field('started_at') };
  assert(Number.isInteger(result.pid) && result.pid > 0 && Number.isFinite(Date.parse(result.started_at)));
  return result;
}
async function startMemory() {
  const bin = process.env.ALETHEIA_MCP_BIN ?? join(A, 'bin/Release/net10.0/Aletheia');
  await access(bin); // Never build or modify Aletheia for this smoke.
  compose('up', '-d', 'postgres');
  let ready = false;
  for (let i = 0; i < 60; i++) {
    try { compose('exec', '-T', 'postgres', 'pg_isready', '-U', 'aletheia'); ready = true; break; } catch { await sleep(1000); }
  }
  assert(ready, 'Documented Docker postgres did not become ready');
  smokeDb = 'peers_review_smoke_' + randomBytes(12).toString('hex');
  compose('exec', '-T', 'postgres', 'createdb', '-U', 'aletheia', smokeDb); createdDb = true;
  aletheiaEnv = { DOTNET_ENVIRONMENT: 'Production',
    ConnectionStrings__Postgres: `Host=localhost;Port=5433;Database=${smokeDb};Username=aletheia;Password=aletheia` };
  memory = new Mcp(bin, ['--mcp'], { ...process.env, ...aletheiaEnv }, 'peers-review-memory-smoke'); children.add(memory); await memory.initialize();
  assert.equal(sql(smokeDb, "SELECT to_regclass('memory.workspace_bindings')"), 'memory.workspace_bindings', 'Startup migration did not create private workspace bindings');
  const binding = await memory.call('bind_workspace', { workingDir: repo, gitFirstCommitSha: git('rev-list', '--max-parents=0', 'HEAD').split('\n')[0] });
  assert(/^wb_[0-9a-f]{32}$/.test(binding.workspaceBindingId));
  assert.equal(sql(smokeDb, `SELECT count(*) FROM memory.workspace_bindings WHERE public_id='${binding.workspaceBindingId}'`), '1', 'Binding did not reach the runner-owned database');
  const identity = diagnostics(await memory.call('diagnose_server', {}));
  assert(Date.parse(identity.started_at) >= started, 'Daemon predates this runner; do not claim or stop it');
  assert.equal(await realpath(identity.process_path), await realpath(bin));
  ownedDaemon = identity;
  console.log(JSON.stringify({ memory_database_proof: true, identity, identity_confidence: binding.identityConfidence, binding_status: binding.bindingStatus }));
  return { bin, binding, identity };
}
async function markerObservations(marker, binding, scope) {
  assert(/^peers-lesson:[0-9a-f]{64}$/.test(marker));
  const search = await memory.call('search_observations', { query: marker, workspaceBindingId: binding.workspaceBindingId, profileScope: scope, limit: 200 });
  assert(search.count < 200 && search.count === search.results.length);
  return Promise.all(search.results.map(row => memory.call('get_observation', { observationId: row.observationId })));
}
async function runOmp(channelId, packetId, proposal, memoryInfo, expectedMarker) {
  await driver.close(); // Explicit adoption is required; never impersonate the scripted driver.
  await mkdir(join(repo, '.omp'), { recursive: true });
  // Persist only fixture overrides, never inherited provider credentials or the user's auth profile.
  const fixturePeersEnv = Object.fromEntries(['PEERS_STATE_DIR', 'PEERS_DB_PATH', 'PEERS_PORT', 'PEERS_BROKER_PROJECT'].map(key => [key, peersEnv[key]]));
  await writeFile(join(repo, '.omp/mcp.json'), JSON.stringify({ mcpServers: {
    peers: { type: 'stdio', command: peersBin, cwd: repo, env: fixturePeersEnv, timeout: 120000 },
    aletheia: { type: 'stdio', command: memoryInfo.bin, args: ['--mcp'], cwd: repo, env: aletheiaEnv, timeout: 120000 },
  } }, null, 2));
  const overlay = join(root, 'fixture-overlay.yml');
  await writeFile(overlay, 'advisor:\n  enabled: false\nautolearn:\n  enabled: false\nmemory:\n  backend: off\nmcp:\n  enableProjectConfig: true\n');
  let skillPath = join(dirname(B), 'peers-SKILL.md');
  try { await access(skillPath); } catch { skillPath = join(homedir(), '.claude/skills/peers/SKILL.md'); }
  const skill = await readFile(skillPath, 'utf8');
  assert.equal(skill.split('### Promote a reviewed lesson').length, 2, 'Skill promotion heading must be unique');
  const block = skill.split('### Promote a reviewed lesson')[1].match(/```js\n([\s\S]*?)\n```/)?.[1];
  assert(block?.includes('console.log(JSON.stringify(await promoteLesson('));
  const identityGate = `const response = await tool.write({i:"Checking isolated memory identity",path:"xd://mcp__aletheia_diagnose_server",content:"{}"});
if(response?.hasError) throw new Error("diagnose_server failed");
const diagnosticText = typeof response === "string" ? response : response?.text;
if(typeof diagnosticText !== "string") throw new Error("diagnose_server returned no plain text");
const expectedIdentity = ${JSON.stringify(memoryInfo.identity)};
for(const key of ["pid","process_path","started_at"]){const rows=diagnosticText.split(/\\r?\\n/).filter(line=>line.startsWith(key+":"));if(rows.length!==1||rows[0].slice(key.length+1).trim()!==String(expectedIdentity[key]))throw new Error("Isolated daemon identity mismatch: "+key);}
`;
  const source = `const peersRoot = ${JSON.stringify(B)};\nconst channel_id = ${JSON.stringify(channelId)};\nconst proposal = ${JSON.stringify(proposal)};\n${identityGate}
const nativePromotionOutput = [];
const nativeConsole = console;
{
const console = {log(...values){nativePromotionOutput.push(...values);nativeConsole.log(...values);}};
{
${block}
}
{
${block}
}
}
const nativePromotionResults = nativePromotionOutput.map(value=>JSON.parse(value));
if(nativePromotionResults.length!==2||nativePromotionResults.some(result=>result.status!=="saved"||!result.observation_id)||nativePromotionResults[0].observation_id!==nativePromotionResults[1].observation_id)throw new Error("Promotion did not return the same verified saved ID twice");
nativeConsole.log(JSON.stringify({native_promotion_results:nativePromotionResults.map(result=>({status:result.status,observation_id:result.observation_id,marker:result.marker}))}));
`;
  const sourcePath = join(root, 'native-promotion-cell.js'); await writeFile(sourcePath, source);
  const prompt = `Run this bounded integration smoke, not implementation work. Read the native mounted tool schemas for peers review_checkpoint/review_packet/review_lesson and Aletheia diagnose_server. Do not edit source or create tests. Use native peers to explicitly adopt the offline completed driver: review_checkpoint action resume channel_id=${JSON.stringify(channelId)}, source_root=${JSON.stringify(repo)}, reason="Native omp smoke adoption". Require state completed. Then fetch the exact isolated packet ${JSON.stringify(packetId)} with native review_packet fetch on that same channel and read its task_path/manifest_path. Do not fall back to another channel or raw verdict. Read ${JSON.stringify(sourcePath)} and execute its EXACT JavaScript in one native Eval cell with timeout=300. The gate in that cell MUST compare plain diagnose_server output with the SQL-proven PID/process_path/started_at BEFORE binding or saving. It uses the candidate skill's unchanged executable block twice. Do not rewrite the block, use a second MCP client, use an auth profile, override denial policy, substitute a model, or run promotion via a shell. Print both JSON helper results through their console.log calls. Both must be saved with the same observation_id. If any prerequisite or gate fails, report it and stop. Final prose alone is not evidence.`;
  const version = execFileSync('omp', ['--version'], { encoding: 'utf8' }).trim(); console.log('omp_version=' + version);
  const ompEnv = { ...process.env, OMP_MCP_REQUIRE_READY: '1', OMP_MCP_TIMEOUT_MS: '120000' };
  delete ompEnv.PEERS_HOME;
  const child = spawn('omp', ['--cwd', repo, '--no-session', '--no-extensions', '--no-skills', '--no-rules', '--config', overlay,
    '--model', process.env.OMP_SMOKE_MODEL ?? 'openai-codex/gpt-6.1-sol', '--approval-mode', 'yolo', '--max-time', '6m', '--mode', 'json', '-p', prompt],
  { cwd: repo, env: ompEnv, stdio: ['ignore', 'pipe', 'pipe'] });
  const stdout = [], stderr = []; child.stdout.on('data', chunk => stdout.push(chunk)); child.stderr.on('data', chunk => stderr.push(chunk));
  const code = await new Promise((resolve, reject) => { child.once('error', reject); child.once('exit', resolve); });
  const output = Buffer.concat(stdout).toString('utf8'); const errors = Buffer.concat(stderr).toString('utf8');
  const transcript = join(root, 'omp-events.jsonl'); await writeFile(transcript, output); await writeFile(join(root, 'omp-stderr.log'), errors);
  // Keep complete output as evidence outside the source repository, even when native verification fails.
  const durable = join(dirname(B), 'omp-review-smoke-' + smokeDb + '.jsonl'); await writeFile(durable, output);
  assert.equal(code, 0, `Native omp failed (${code}); evidence: ${durable}; stderr: ${errors.slice(-4000)}`);
  const events = output.split('\n').filter(Boolean).flatMap(line => { try { return [JSON.parse(line)]; } catch { return []; } });
  const toolEvents = events.filter(event => event.type?.startsWith('tool_execution_'));
  const nativePeerCalls = toolEvents.filter(event => event.type === 'tool_execution_end').map(event => event.result?.details?.xdev).filter(Boolean);
  assert(nativePeerCalls.some(call => call.tool === 'mcp__peers_review_checkpoint' && call.args.action === 'resume' && call.args.channel_id === channelId), 'Native driver did not explicitly adopt the isolated channel');
  assert(nativePeerCalls.some(call => call.tool === 'mcp__peers_review_packet' && call.args.action === 'fetch' && call.args.channel_id === channelId && call.args.packet_id === packetId), 'Native driver did not fetch the exact isolated packet');
  const cell = toolEvents.filter(event => event.type === 'tool_execution_end' && event.toolName === 'eval')
    .flatMap(event => event.result?.details?.cells ?? []).find(cell => cell.code?.trimEnd() === source.trimEnd());
  assert(cell?.status === 'complete' && cell.exitCode === 0, `Exact candidate-skill Eval cell did not complete; evidence: ${durable}`);
  const trace = cell.statusEvents ?? [];
  assert.equal(trace[0]?.path, 'xd://mcp__aletheia_diagnose_server', 'Daemon gate did not precede promotion');
  assert.equal(trace.filter(event => event.path === 'xd://mcp__aletheia_save_observation').length, 1, 'Native helper dispatched a save other than once');
  const proof = cell.output.split('\n').flatMap(line => { try { return [JSON.parse(line)]; } catch { return []; } })
    .find(value => Array.isArray(value.native_promotion_results))?.native_promotion_results;
  assert.equal(proof?.length, 2, `Missing complete compact saved results; evidence: ${durable}`);
  assert(proof.every(result => result.status === 'saved' && result.marker === expectedMarker && /^obs_[0-9a-f]{32}$/.test(result.observation_id)));
  assert.equal(proof[0].observation_id, proof[1].observation_id);
  console.log(JSON.stringify({ native_omp_events: toolEvents.length, native_evidence: durable, observation_id: proof[0].observation_id, saved_results: proof.length, version }));
}

try {
  git('init', '-q'); git('config', 'user.email', 'review-smoke@example.invalid'); git('config', 'user.name', 'Peers review smoke');
  await writeFile(join(repo, '.gitignore'), '.omp/\n');
  await writeFile(join(repo, 'value.txt'), '1\n');
  await writeFile(join(repo, 'value.test.mjs'), `import {test} from 'node:test';\nimport assert from 'node:assert/strict';\nimport {readFileSync} from 'node:fs';\ntest('value is two', t => {assert.equal(readFileSync('value.txt','utf8'),'2\\n');t.diagnostic('Příliš žluťoučký kůň 🧪\\n'.repeat(50000));});\n`);
  git('add', '.'); git('commit', '-qm', 'smoke baseline'); const base = git('rev-parse', 'HEAD');
  const indexBefore = await readFile(join(repo, '.git/index'));
  const planPath = join(root, 'plan.md'); await writeFile(planPath, 'Set value.txt to 2\\n. Inspect actual executed/pass/skip counts before approval.\n');
  driver = await peer('peers-review-smoke-driver'); reviewer = await peer('peers-review-smoke-reviewer');
  await driver.call('presence', { role: 'implementing', summary: 'isolated review smoke driver' });
  await reviewer.call('presence', { role: 'reviewing', summary: 'isolated read-only smoke reviewer' });
  const roster = await driver.call('peers');
  const driverId = roster.find(p => p.tool === 'peers-review-smoke-driver').peer_id;
  const reviewerId = roster.find(p => p.tool === 'peers-review-smoke-reviewer').peer_id;
  const channel = await driver.call('channel_create', { name: 'review-smoke', repo_root: repo }); const channelId = channel.channel_id;
  broker = JSON.parse(await readFile(join(state, 'broker.json'), 'utf8'));
  assert.equal(broker.port, port); await access(join(state, 'peers.db'));
  const brokerPath = execFileSync('ps', ['-ww', '-p', String(broker.pid), '-o', 'comm='], { encoding: 'utf8' }).trim();
  assert(brokerPath.startsWith(B + '/src/Peers.Broker/') && brokerPath.endsWith('/Peers.Broker'), 'Private discovery points outside candidate broker'); ownedBroker = true;
  assert.equal((await (await fetch(`http://127.0.0.1:${port}/health`)).json()).contract, contract);
  const request = { phase: 'PLAN', round: 1, task: 'Change value.txt from 1\\n to 2\\n. Prove that the intended Node test ran. Preserve Czech/emoji and binary evidence exactly.',
    base_sha: base, reviewer_peer_id: reviewerId, change_summary: 'Review the verification plan', uncertainties: 'A zero exit code can hide a filtered-out test.', source_root: repo, plan_path: planPath };
  const publish = request => driver.call('review_packet', { action: 'publish', channel_id: channelId, request });
  let packet = await publish(request); const planPacket = packet.packet_id;
  const fetchPacket = id => reviewer.call('review_packet', { action: 'fetch', channel_id: channelId, packet_id: id });
  const verdict = (id, verdict, summary, findings = []) => reviewer.call('review_packet', { action: 'reply', channel_id: channelId, packet_id: id, reply: { verdict, summary, findings } });
  let fetched = await fetchPacket(planPacket); assert.equal(await readFile(fetched.task_path, 'utf8'), request.task);
  await verdict(planPacket, 'APPROVE', 'Plan covers the task and exact evidence requirements.');
  assert.equal((await publish(request)).packet_id, planPacket);
  const checkpoint = (action, extra = {}) => driver.call('review_checkpoint', { action, channel_id: channelId, ...extra });
  await writeFile(join(repo, 'value.txt'), '2\n');
  const binary = Buffer.from(Array.from({ length: 4096 }, (_, i) => i % 256)); await writeFile(join(repo, 'binary.bin'), binary);
  let status = await checkpoint('status'); const fixedRevision = status.revision_sha256;
  function runTest(filtered, name) {
    const args = ['--test', '--test-reporter=tap', ...(filtered ? ['--test-name-pattern=absent'] : []), 'value.test.mjs'];
    const output = execFileSync('node', args, { cwd: repo, maxBuffer: 32 * 1024 * 1024 });
    const path = join(root, name + '.log');
    return { output, path, command: 'node ' + args.join(' ') };
  }
  let check = runTest(true, 'code1'); await writeFile(check.path, check.output); const beforeText = check.output.toString('utf8');
  assert.match(beforeText, /^1\.\.0$/m);
  assert(!beforeText.includes('# Subtest: value is two'), 'The excluding filter unexpectedly ran the intended test');
  const code = { ...request, phase: 'CODE', round: 1, expected_previous_packet_id: planPacket, verification_revision: fixedRevision,
    change_summary: 'Source fixed at two; first run uses an excluding name filter.', evidence: [{ path: check.path, kind: 'test', command: check.command, cwd: repo, exit_code: 0 }] }; delete code.plan_path;
  packet = await publish(code); const code1 = packet.packet_id;
  await fetchPacket(code1);
  await assert.rejects(verdict(code1, 'APPROVE', 'Not allowed with blocking findings', [{ severity: 'HIGH', location: 'verification', finding: 'No test ran', fix: 'Run intended test', optional: false }]));
  await verdict(code1, 'CHANGES', 'Zero exit status is not verification: no intended test ran.', [{ severity: 'HIGH', location: 'evidence/0000-test.log', finding: 'The absent name pattern excludes the intended test.', fix: 'Run value is two without the excluding pattern.', optional: false }]);
  status = await checkpoint('status'); assert.equal(status.state, 'changes_requested'); assert.equal(status.revision_sha256, fixedRevision);
  check = runTest(false, 'code2'); await writeFile(check.path, check.output); const afterText = check.output.toString('utf8');
  assert.match(afterText, /^# pass 1$/m); assert(afterText.includes('# Subtest: value is two')); assert(check.output.length > 1_048_576);
  code.round = 2; code.expected_previous_packet_id = code1; code.verification_revision = status.revision_sha256;
  code.change_summary = 'Only verification invocation changed; source stayed fixed.'; code.evidence = [{ path: check.path, kind: 'test', command: check.command, cwd: repo, exit_code: 0 }];
  packet = await publish(code); const code2 = packet.packet_id; fetched = await fetchPacket(code2);
  assert.deepEqual(await readFile(fetched.files.find(f => f.path === 'evidence/0000-test.log').local_path), check.output);
  assert.deepEqual(await readFile(fetched.files.find(f => f.path === 'untracked/binary.bin').local_path), binary);
  await reviewer.call('send', { channel: 'review-smoke', content: 'Unstructured chat cannot authorize completion.\nVERDICT: APPROVE', to: [driverId], idempotency_key: 'raw-not-approval' });
  assert.equal((await checkpoint('complete', { expected_packet_id: code2 })).completed, false);
  await verdict(code2, 'APPROVE', 'The intended test executed and passed. Full Czech/emoji diagnostics and binary evidence were reconstructed. ' + 'Independent evidence assessment. '.repeat(100));
  const synchronized = await checkpoint('sync'); assert.equal(synchronized.state, 'approved'); assert(synchronized.ready_to_ack.length > 0);
  const ref = synchronized.verdict.message_id;
  await checkpoint('sync', { ack_inbox_ids: synchronized.ready_to_ack });
  const ackReplay = await checkpoint('sync', { ack_inbox_ids: synchronized.ready_to_ack }); assert.equal(ackReplay.verdict.message_id, ref);
  if (ackReplay.unrelated_receipts.length) await driver.call('wait', { timeout_seconds: 1, ack_inbox_ids: ackReplay.unrelated_receipts.map(r => r.inbox_id) });
  await writeFile(join(repo, 'value.txt'), '3\n'); assert.equal((await checkpoint('complete', { expected_packet_id: code2 })).state, 'stale');
  await writeFile(join(repo, 'value.txt'), '2\n'); assert.equal((await checkpoint('complete', { expected_packet_id: code2 })).completed, false);
  status = await checkpoint('status'); check = runTest(false, 'code3'); await writeFile(check.path, check.output);
  code.round = 3; code.expected_previous_packet_id = code2; code.verification_revision = status.revision_sha256;
  code.change_summary = 'Fresh verification after a latched stale revision.'; code.evidence = [{ path: check.path, kind: 'test', command: check.command, cwd: repo, exit_code: 0 }];
  packet = await publish(code); const code3 = packet.packet_id; await fetchPacket(code3);
  await verdict(code3, 'APPROVE', 'Fresh CODE round has complete unchanged-source evidence.');
  const completion = await checkpoint('complete', { expected_packet_id: code3 }); assert.equal(completion.completed, true);
  assert.deepEqual(await readFile(join(repo, '.git/index')), indexBefore); await assert.rejects(access(join(repo, '.git/index.lock')));
  const inbox = await (await fetch(`http://127.0.0.1:${port}/peers/${reviewerId}/inbox?limit=500`)).json();
  assert(!inbox.items.some(i => i.event.type === 'artifact.published'), 'Internal artifacts caused inbox fan-out');
  await driver.close(); driver = await peer('peers-review-smoke-driver'); await driver.call('presence', { role: 'implementing', summary: 'same-ID restart' });
  assert((await driver.call('peers')).some(p => p.peer_id === driverId && p.tool === 'peers-review-smoke-driver'));
  assert.equal((await checkpoint('status')).state, 'completed');
  const successor = await peer('peers-review-smoke-successor'); await successor.call('presence', { role: 'implementing', summary: 'explicit adoption fixture' });
  await assert.rejects(successor.call('review_checkpoint', { action: 'resume', channel_id: channelId, source_root: repo, reason: 'Take over completed fixture' }), /still live/);
  const oldReceipts = await (await fetch(`http://127.0.0.1:${port}/peers/${driverId}/inbox?limit=500`)).json();
  await driver.close(); driver = successor;
  assert.equal((await checkpoint('resume', { source_root: repo, reason: 'Take over offline completed fixture' })).state, 'completed');
  await checkpoint('sync');
  const oldReceiptsAfter = await (await fetch(`http://127.0.0.1:${port}/peers/${driverId}/inbox?limit=500`)).json();
  assert.deepEqual(oldReceiptsAfter.items.map(i => i.inboxId), oldReceipts.items.map(i => i.inboxId), 'Successor acknowledged former-driver receipts');
  await rm(join(state, 'review-workflows', hash(channelId), 'driver/state.json'));
  assert.equal((await checkpoint('status')).state, 'completed');
  git('add', '.'); git('commit', '-qm', 'approved smoke fixture');
  status = await checkpoint('status'); assert.equal(status.state, 'completed'); assert.equal(status.source_changed_since_completion, true);
  const quote = text => text.slice(text.lastIndexOf('# tests ')).trimEnd();
  const proposal = {
    cue: 'When a Node test invocation exits zero but all tests were filtered out',
    lesson: 'Inspect executed/pass/skip counts before treating a Node run as verification, and rerun the intended test without an excluding name pattern',
    effect: 'helped', confidence: 'high', confidence_reason: 'Source stayed fixed between CODE/1 and CODE/2. Only the excluding filter changed. The filtered TAP has an empty 1..0 plan, while the rerun names value is two. Node can count the empty file wrapper as one pass; summary counts alone are not proof.',
    before: [{ packet_id: code1, path: 'evidence/0000-test.log', quote: beforeText.trimEnd() }],
    after: [{ packet_id: code2, path: 'evidence/0000-test.log', quote: quote(afterText) },
      { packet_id: code2, path: 'evidence/0000-test.log', quote: '# Subtest: value is two' }], scope: 'project',
  };
  assert.equal((await driver.call('review_lesson', { action: 'prepare', channel_id: channelId, proposal: { ...proposal, effect: 'confounded' } })).status, 'no_entry');
  const prepared = await driver.call('review_lesson', { action: 'prepare', channel_id: channelId, proposal }); assert.equal(prepared.status, 'prepared');
  console.log(JSON.stringify({ workflow_smoke: 'passed', channel_id: channelId, completed_packet: code3, evidence_bytes: Buffer.byteLength(afterText), source_changed_since_completion: true, contract, index_unchanged_by_capture: true }));
  if (flags.has('--with-aletheia')) {
    const memoryInfo = await startMemory();
    if (flags.has('--with-omp')) await runOmp(channelId, code3, proposal, memoryInfo, prepared.marker);
    else {
      let saves = 0, first = true, refuseReads = false;
      const ordinary = async (path, args) => path === 'xd://mcp__peers_review_lesson' ? driver.call('review_lesson', args) : memory.call(path.replace('xd://mcp__aletheia_', ''), args);
      const lostResponse = async (path, args) => {
        if (refuseReads && /aletheia_(search_observations|get_observation)$/.test(path)) throw new Error('Fault injection: bounded reconciliation outage');
        if (path.endsWith('aletheia_save_observation')) { saves++; const result = await ordinary(path, args); if (first) { first = false; refuseReads = true; throw new Error('Fault injection: real committed save response lost'); } return result; }
        return ordinary(path, args);
      };
      assert.equal((await promoteLesson(lostResponse, { channel_id: channelId, proposal })).status, 'unknown');
      const recovered = await promoteLesson(ordinary, { channel_id: channelId, proposal }); assert.equal(recovered.status, 'saved'); assert.equal(saves, 1);
      const repeated = await promoteLesson(ordinary, { channel_id: channelId, proposal }); assert.equal(repeated.observation_id, recovered.observation_id);
      console.log(JSON.stringify({ aletheia_lost_response_recovered: true, save_dispatches: saves, observation_id: recovered.observation_id }));
    }
    const matches = await markerObservations(prepared.marker, memoryInfo.binding, prepared.scope); assert.equal(matches.length, 1);
    assert.equal(matches[0].body, prepared.body);
    assert.equal(sql(smokeDb, `SELECT count(*) FROM memory.observations WHERE '${prepared.marker}' = ANY(tags)`), '1');
    assert.equal(sql('aletheia', `SELECT count(*) FROM memory.observations WHERE '${prepared.marker}' = ANY(tags)`), '0');
    console.log(JSON.stringify({ memory_mcp_and_sql: 'passed', marker: prepared.marker, observation_id: matches[0].observationId, private_count: 1, normal_count: 0 }));
  }
} finally {
  for (const child of children) await child.close();
  if (ownedDaemon) {
    try {
      const path = execFileSync('ps', ['-ww', '-p', String(ownedDaemon.pid), '-o', 'comm='], { encoding: 'utf8' }).trim();
      if (path === ownedDaemon.process_path) process.kill(ownedDaemon.pid, 'SIGTERM');
    } catch { /* Already stopped; never kill an unproven PID. */ }
  }
  if (ownedBroker && broker) {
    try {
      const path = execFileSync('ps', ['-ww', '-p', String(broker.pid), '-o', 'comm='], { encoding: 'utf8' }).trim();
      if (path.startsWith(B + '/src/Peers.Broker/') && path.endsWith('/Peers.Broker')) process.kill(broker.pid, 'SIGTERM');
    } catch { /* Already stopped. */ }
  }
  if (createdDb) compose('exec', '-T', 'postgres', 'dropdb', '-U', 'aletheia', '--force', smokeDb);
  await rm(root, { recursive: true, force: true });
}
