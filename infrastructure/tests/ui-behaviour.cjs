'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const jsRoot = path.resolve(__dirname, '../../experiment-platform/experiment-runner/src/main/resources/static/js');

function loadUi(fetch, extra = {}) {
  const context = vm.createContext({ Intl, setTimeout, clearTimeout, AbortController, fetch, ...extra });
  vm.runInContext(fs.readFileSync(path.join(jsRoot, 'ui.js'), 'utf8'), context);
  return context.RunnerUi;
}

test('UI overrides only SLO inputs and seed; preserves full experiment and does not mutate profile', () => {
  const source = { schemaVersion: 1, seed: 1, slo: { produceP99Ms: 100, maxErrorRate: 0.01, maxViolationTime: '2m' },
    recovery: { throttlePolicy: 'AIMD' }, load: { schedule: [{ duration: '30s', requestsPerSecond: 100 }] },
    resources: { profile: 'test-lab-only' }, fault: { target: 'broker-2', at: '1s' } };
  const before = JSON.stringify(source);
  const result = loadUi().withOverrides(source, { produceP99Ms: '120', maxErrorRate: '0.02', seed: '77' });
  assert.equal(JSON.stringify(source), before);
  assert.equal(result.seed, 77);
  assert.equal(result.slo.produceP99Ms, 120);
  assert.equal(result.slo.maxErrorRate, 0.02);
  assert.equal(result.slo.maxViolationTime, '2m');
  for (const field of ['recovery', 'load', 'resources', 'fault']) assert.deepEqual(result[field], source[field]);
});

test('UI rejects empty inputs, invalid SLO and imprecise seed rather than rounding a Java long', () => {
  const ui = loadUi();
  const source = { slo: { produceP99Ms: 100, maxErrorRate: 0.01 } };
  for (const values of [
    { produceP99Ms: '', maxErrorRate: '0.01', seed: '1' },
    { produceP99Ms: '100', maxErrorRate: '2', seed: '1' },
    { produceP99Ms: '100', maxErrorRate: '0.01', seed: '9007199254740993' },
    { produceP99Ms: '100', maxErrorRate: '0.01', seed: '1.5' }
  ]) assert.throws(() => ui.withOverrides(source, values));
  assert.throws(() => ui.withOverrides({ ...source, seed: 9007199254740992 },
    { produceP99Ms: '100', maxErrorRate: '0.01', seed: '1' }), /точно/);
  assert.equal(ui.number(null), 'нет данных');
  assert.equal(ui.percent(undefined), 'нет данных');
  assert.equal(ui.number(0), '0');
});

test('API errors retain field and status; ambiguous POST is not retried', async () => {
  let calls = 0;
  const rejected = loadUi(async () => {
    calls++;
    return { ok: false, status: 400, json: async () => ({ field: 'slo.maxErrorRate', detail: 'must be in [0,1]' }) };
  });
  await assert.rejects(rejected.requestJson('/api/experiments', { method: 'POST' }), error => {
    assert.equal(error.status, 400);
    assert.match(error.message, /slo.maxErrorRate/);
    assert.equal(error.ambiguous, undefined);
    return true;
  });
  assert.equal(calls, 1);
  calls = 0;
  const lost = loadUi(async () => { calls++; throw new Error('connection lost'); });
  await assert.rejects(lost.requestJson('/api/experiments', { method: 'POST' }), error => error.ambiguous === true);
  assert.equal(calls, 1);
  const serverFailed = loadUi(async () => ({ ok: false, status: 500, json: async () => ({ detail: 'storage failed' }) }));
  await assert.rejects(serverFailed.requestJson('/api/experiments', { method: 'POST' }), error => error.ambiguous === true);
});

test('poll does not overlap requests when one endpoint fails before the other endpoints settle', async () => {
  let settleMetrics;
  let settleEvents;
  const calls = [];
  const scheduled = [];
  const errors = [];
  const elements = new Map();
  function element(id) {
    if (!elements.has(id)) elements.set(id, { disabled: false, textContent: '', addEventListener() {} });
    return elements.get(id);
  }
  const context = vm.createContext({
    document: { body: { dataset: { experimentId: 'run-1' } }, getElementById: element },
    window: { addEventListener() {} },
    setTimeout: callback => { scheduled.push(callback); return scheduled.length; },
    clearTimeout() {},
    RunnerUi: {
      requestJson(url) {
        calls.push(url);
        if (url.endsWith('/metrics')) return new Promise(resolve => { settleMetrics = resolve; });
        if (url.endsWith('/events')) return new Promise(resolve => { settleEvents = resolve; });
        return Promise.reject(new Error('offline'));
      },
      error: (id, value) => errors.push(value)
    }
  });
  vm.runInContext(fs.readFileSync(path.join(jsRoot, 'active-experiment.js'), 'utf8'), context);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(calls.length, 3);
  assert.equal(scheduled.length, 0);
  settleMetrics([]);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(scheduled.length, 0);
  settleEvents({ events: [], decisions: [] });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(scheduled.length, 1);
  assert.match(errors[0], /offline/);
});

test('Stop stays disabled and sends only one POST after repeated clicks and a lost response', async () => {
  const elements = new Map();
  const postCalls = [];
  const errors = [];
  function element(id) {
    if (!elements.has(id)) elements.set(id, { disabled: false, textContent: '',
      addEventListener(event, handler) { this.handler = handler; } });
    return elements.get(id);
  }
  const context = vm.createContext({
    document: { body: { dataset: { experimentId: 'run-1' } }, getElementById: element },
    window: { addEventListener() {} }, setTimeout() {}, clearTimeout() {},
    RunnerUi: {
      requestJson(url, options) {
        if (!options?.method) return new Promise(() => {});
        postCalls.push(url);
        return Promise.reject(new Error('lost response'));
      },
      error: (id, value) => errors.push(value)
    }
  });
  vm.runInContext(fs.readFileSync(path.join(jsRoot, 'active-experiment.js'), 'utf8'), context);
  const button = element('stop-button');
  await Promise.all([button.handler(), button.handler()]);
  assert.equal(button.disabled, true);
  assert.deepEqual(postCalls, ['/api/experiments/run-1/stop']);
  assert.match(errors[0], /lost response/);
});

test('BFCache restore reloads pages so stopped polling and disabled forms are not reused', () => {
  const handlers = {};
  let reloads = 0;
  loadUi(undefined, { window: {
    addEventListener(event, callback) { handlers[event] = callback; },
    location: { reload() { reloads++; } }
  } });
  handlers.pageshow({ persisted: false });
  assert.equal(reloads, 0);
  handlers.pageshow({ persisted: true });
  assert.equal(reloads, 1);
});
