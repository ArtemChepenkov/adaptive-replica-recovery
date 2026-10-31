(function (global) {
  'use strict';

  const terminalStates = new Set(['COMPLETED', 'FAILED', 'CANCELLED']);
  const numberFormat = new Intl.NumberFormat('ru-RU', { maximumFractionDigits: 2 });

  async function requestJson(url, options = {}) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 15000);
    let response;
    try {
      response = await fetch(url, { ...options, signal: controller.signal, credentials: 'same-origin' });
      const body = await response.json();
      if (!response.ok) {
        const field = body.field ? `${body.field}: ` : '';
        const error = new Error(`${field}${body.detail || body.title || `HTTP ${response.status}`}`);
        error.status = response.status;
        throw error;
      }
      return body;
    } catch (error) {
      if (!response) {
        const failure = new Error(error.name === 'AbortError'
          ? 'Сервер не ответил вовремя.' : 'Не удалось связаться с Runner.');
        failure.ambiguous = options.method && options.method !== 'GET';
        throw failure;
      }
      if (options.method && options.method !== 'GET'
          && (response.ok || response.status >= 500 || response.status === 408)) error.ambiguous = true;
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }

  function number(value, suffix = '') {
    return typeof value === 'number' && Number.isFinite(value)
      ? `${numberFormat.format(value)}${suffix}` : 'нет данных';
  }

  function percent(value) {
    return typeof value === 'number' && Number.isFinite(value) ? number(value * 100, ' %') : 'нет данных';
  }

  function timestamp(value) {
    if (!value) return '—';
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? 'нет данных' : date.toLocaleString('ru-RU');
  }

  function text(id, value) {
    const element = document.getElementById(id);
    if (element) element.textContent = value == null || value === '' ? 'нет данных' : String(value);
  }

  function error(id, message) {
    const element = document.getElementById(id);
    element.textContent = message || '';
    element.hidden = !message;
  }

  function events(tableId, rows) {
    const body = document.getElementById(tableId);
    body.replaceChildren();
    for (const event of rows) {
      const row = document.createElement('tr');
      for (const value of [timestamp(event.timestamp), event.source, event.severity, event.message]) {
        const cell = document.createElement('td');
        cell.textContent = value == null ? '—' : String(value);
        row.appendChild(cell);
      }
      body.appendChild(row);
    }
    document.getElementById(`${tableId}-empty`).hidden = rows.length > 0;
  }

  function withOverrides(source, overrides) {
    if (!source || typeof source !== 'object' || Array.isArray(source)) {
      throw new Error('Сценарий YAML должен содержать объект.');
    }
    if (!source.slo || typeof source.slo !== 'object' || Array.isArray(source.slo)) {
      throw new Error('В сценарии отсутствует объект slo.');
    }
    // Browser numbers cannot represent all Java long values. Never submit a rounded integer.
    JSON.stringify(source, (key, value) => {
      if (typeof value === 'number' && (!Number.isFinite(value)
          || (Number.isInteger(value) && !Number.isSafeInteger(value)))) {
        throw new Error(`Поле ${key}: число нельзя точно представить в браузере. Используйте CLI.`);
      }
      return value;
    });
    const p99 = Number(overrides.produceP99Ms);
    const rate = Number(overrides.maxErrorRate);
    const seed = Number(overrides.seed);
    if (String(overrides.produceP99Ms).trim() === '' || !Number.isFinite(p99) || p99 <= 0) {
      throw new Error('slo.produceP99Ms: укажите положительное число.');
    }
    if (String(overrides.maxErrorRate).trim() === '' || !Number.isFinite(rate) || rate < 0 || rate > 1) {
      throw new Error('slo.maxErrorRate: укажите долю от 0 до 1.');
    }
    if (String(overrides.seed).trim() === '' || !Number.isSafeInteger(seed)) {
      throw new Error('seed: укажите целое число в точном диапазоне браузера.');
    }
    return { ...source, seed, slo: { ...source.slo, produceP99Ms: p99, maxErrorRate: rate } };
  }

  global.RunnerUi = { requestJson, number, percent, timestamp, text, error, events,
    withOverrides, terminal: state => terminalStates.has(state) };
  // A back/forward-cache restore also restores disabled forms and disposed polling.
  if (typeof window !== 'undefined') {
    window.addEventListener('pageshow', event => {
      if (event.persisted) window.location.reload();
    });
  }
})(globalThis);
