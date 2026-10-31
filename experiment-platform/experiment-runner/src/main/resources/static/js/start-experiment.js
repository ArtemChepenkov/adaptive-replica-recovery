(async function () {
  'use strict';
  const ui = RunnerUi;
  const scenario = document.getElementById('scenario');
  const start = document.getElementById('start-button');
  const form = document.getElementById('experiment-form');
  let profiles = [];
  let selected;
  let posting = false;

  function selectProfile() {
    selected = undefined;
    ui.error('start-error', '');
    const profile = profiles.find(item => item.name === scenario.value);
    ui.text('scenario-yaml', profile ? profile.yaml : 'Сценарий не выбран.');
    start.disabled = true;
    if (!profile) return;
    try {
      selected = jsyaml.load(profile.yaml, { schema: jsyaml.JSON_SCHEMA });
      if (!selected || typeof selected !== 'object' || Array.isArray(selected)) {
        throw new Error('Сценарий YAML должен содержать объект.');
      }
      document.getElementById('produce-p99').value = selected.slo?.produceP99Ms ?? '';
      document.getElementById('error-rate').value = selected.slo?.maxErrorRate ?? '';
      document.getElementById('seed').value = selected.seed ?? 20260910;
      ui.text('scenario-description', `Ресурсы: ${selected.resources?.profile ?? 'не указаны'} · Политика: ${selected.recovery?.throttlePolicy ?? 'не указана'}`);
      start.disabled = posting;
    } catch (error) {
      ui.error('start-error', error.message);
    }
  }

  async function refreshHistory() {
    const refresh = document.getElementById('refresh-history');
    refresh.disabled = true;
    try {
      const runs = await ui.requestJson('/api/experiments');
      const history = document.getElementById('history');
      history.replaceChildren();
      for (const run of runs.slice().sort((a, b) => b.createdAt.localeCompare(a.createdAt))) {
        const row = document.createElement('tr');
        const name = document.createElement('td');
        const link = document.createElement('a');
        link.href = `/experiments/${encodeURIComponent(run.id)}`;
        link.textContent = run.spec.name;
        const id = document.createElement('small');
        id.className = 'run-id mono';
        id.textContent = run.id;
        name.append(link, id);
        const state = document.createElement('td');
        state.textContent = run.state;
        const time = document.createElement('td');
        time.textContent = ui.timestamp(run.createdAt);
        const result = document.createElement('td');
        if (ui.terminal(run.state)) {
          const resultLink = document.createElement('a');
          resultLink.href = `/experiments/${encodeURIComponent(run.id)}/results`;
          resultLink.textContent = 'Открыть результат';
          result.appendChild(resultLink);
        } else {
          result.textContent = 'Выполняется';
        }
        row.append(name, state, time, result);
        history.appendChild(row);
      }
      document.getElementById('history-empty').hidden = runs.length > 0;
      ui.error('history-error', '');
    } catch (error) {
      ui.error('history-error', error.message);
    } finally {
      refresh.disabled = false;
    }
  }

  scenario.addEventListener('change', selectProfile);
  document.getElementById('refresh-history').addEventListener('click', refreshHistory);
  document.getElementById('reset-lab').addEventListener('click', async event => {
    const button = event.currentTarget;
    button.disabled = true;
    ui.text('reset-status', 'Проверяется очистка стенда…');
    try {
      await ui.requestJson('/api/lab/reset', { method: 'POST' });
      ui.text('reset-status', 'Очистка стенда подтверждена.');
      ui.error('history-error', '');
      await refreshHistory();
    } catch (error) {
      ui.text('reset-status', 'Очистка не подтверждена.');
      ui.error('history-error', error.message);
    } finally {
      button.disabled = false;
    }
  });

  form.addEventListener('submit', async event => {
    event.preventDefault();
    if (posting || !selected || !form.reportValidity()) return;
    posting = true;
    start.disabled = true;
    scenario.disabled = true;
    ui.error('start-error', '');
    try {
      const payload = ui.withOverrides(selected, {
        produceP99Ms: document.getElementById('produce-p99').value,
        maxErrorRate: document.getElementById('error-rate').value,
        seed: document.getElementById('seed').value
      });
      const accepted = await ui.requestJson('/api/experiments', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
      });
      window.location.assign(`/experiments/${encodeURIComponent(accepted.id)}`);
    } catch (error) {
      ui.error('start-error', error.ambiguous
        ? `${error.message} Запуск мог быть принят. Обновите список запусков перед повтором; повторный POST автоматически не отправляется.`
        : error.message);
      // A lost POST response is not proof that no experiment was started.
      posting = Boolean(error.ambiguous);
      start.disabled = posting;
      scenario.disabled = posting;
      await refreshHistory();
    }
  });

  await refreshHistory();
  try {
    profiles = await ui.requestJson('/api/profiles');
    scenario.replaceChildren();
    for (const profile of profiles) {
      const option = document.createElement('option');
      option.value = profile.name;
      option.textContent = profile.name;
      scenario.appendChild(option);
    }
    if (profiles.length === 0) throw new Error('В каталоге сценариев нет YAML-файлов.');
    if (document.body.dataset.simulated === 'true' && profiles.some(item => item.name === 'runner-smoke.yaml')) {
      scenario.value = 'runner-smoke.yaml';
    }
    scenario.disabled = false;
    selectProfile();
  } catch (error) {
    ui.error('start-error', error.message);
  }
})();
