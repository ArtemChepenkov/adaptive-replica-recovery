(function () {
  'use strict';
  const ui = RunnerUi;
  const id = document.body.dataset.experimentId;
  const base = `/api/experiments/${encodeURIComponent(id)}`;
  const stop = document.getElementById('stop-button');
  const charts = {};
  let disposed = false;
  let timer;
  let stopRequested = false;
  let chartError = '';

  function line(label, color, dashed = false) {
    return { label, data: [], borderColor: color, backgroundColor: color,
      borderWidth: 2, pointRadius: 1, spanGaps: false, tension: 0,
      borderDash: dashed ? [6, 5] : [] };
  }

  if (typeof Chart === 'undefined') {
    chartError = 'Не удалось загрузить локальную библиотеку графиков.';
  } else {
    const definitions = {
      latency: [line('p99', '#156b67'), line('SLO', '#c27528', true)],
      throttle: [line('Лимит', '#4269a5')],
      remaining: [line('Данные', '#156b67')],
      error: [line('Ошибки', '#9f4c65'), line('Предел SLO', '#c27528', true)]
    };
    for (const [name, datasets] of Object.entries(definitions)) {
      charts[name] = new Chart(document.getElementById(`${name}-chart`), {
        type: 'line', data: { labels: [], datasets },
        options: { responsive: true, maintainAspectRatio: false, animation: false,
          interaction: { mode: 'index', intersect: false },
          plugins: { legend: { labels: { usePointStyle: true, boxWidth: 8, font: { size: 11 } } } },
          scales: { x: { grid: { display: false }, ticks: { maxTicksLimit: 6, maxRotation: 0, font: { size: 10 } } },
            y: { beginAtZero: true, ticks: { maxTicksLimit: 5, font: { size: 10 } }, grid: { color: '#edf0ef' } } }
        }
      });
    }
  }

  function updateCharts(metrics, slo) {
    const labels = metrics.map(row => new Date(row.timestamp).toLocaleTimeString('ru-RU'));
    const values = {
      latency: [metrics.map(row => row.produceP99Ms), metrics.map(() => slo.produceP99Ms)],
      throttle: [metrics.map(row => row.throttleBytesPerSecond == null ? null : row.throttleBytesPerSecond / 1048576)],
      remaining: [metrics.map(row => row.remainingBytes == null ? null : row.remainingBytes / 1048576)],
      error: [metrics.map(row => row.errorRate == null ? null : row.errorRate * 100), metrics.map(() => slo.maxErrorRate * 100)]
    };
    for (const [name, chart] of Object.entries(charts)) {
      chart.data.labels = labels;
      values[name].forEach((data, index) => { chart.data.datasets[index].data = data; });
      chart.update('none');
    }
  }

  async function poll() {
    if (disposed) return;
    let terminal = false;
    try {
      const responses = await Promise.allSettled([
        ui.requestJson(base), ui.requestJson(`${base}/metrics`), ui.requestJson(`${base}/events`)
      ]);
      const rejected = responses.find(response => response.status === 'rejected');
      if (rejected) throw rejected.reason;
      const [record, metrics, trace] = responses.map(response => response.value);
      if (disposed) return;
      ui.text('experiment-name', record.spec.name);
      ui.text('experiment-state', record.state);
      ui.text('update-time', `Обновлено ${new Date().toLocaleTimeString('ru-RU')}`);
      ui.text('run-reason', record.reason ? `Причина: ${record.reason}` : '');
      const latest = metrics.at(-1);
      ui.text('current-throttle', ui.number(latest?.throttleBytesPerSecond == null ? null : latest.throttleBytesPerSecond / 1048576));
      ui.text('current-remaining', ui.number(latest?.remainingBytes == null ? null : latest.remainingBytes / 1048576, ' МиБ'));
      ui.text('current-progress', `Сокращение отставания: ${ui.number(latest?.progressBytesPerSecond == null ? null : latest.progressBytesPerSecond / 1048576, ' МиБ/с')}`);
      const decision = trace.decisions.at(-1);
      ui.text('constraint-state', decision?.constraintState);
      ui.text('last-decision', decision
        ? `${ui.timestamp(decision.timestamp)} · ${decision.lastAction ?? decision.action ?? 'действие не передано'} · ${decision.reason ?? 'причина не передана'}`
        : 'Решений пока нет.');
      const warnings = trace.events.filter(event => event.severity === 'WARNING' || event.severity === 'ERROR');
      ui.text('warnings', warnings.length ? warnings.map(event => event.message).join('\n') : 'Предупреждений пока нет.');
      const simulated = document.body.dataset.simulated === 'true'
        || trace.events.some(event => event.message === 'SIMULATED_TEST_LAB_NO_PHYSICAL_INFRASTRUCTURE');
      document.getElementById('simulation-notice').hidden = !simulated;
      ui.events('events', trace.events);
      updateCharts(metrics, record.spec.slo);
      terminal = ui.terminal(record.state);
      stop.disabled = stopRequested || terminal || ['STOPPING', 'CLEANING_UP'].includes(record.state);
      document.getElementById('results-link').hidden = !terminal;
      if (terminal) stop.textContent = 'Опыт завершён';
      ui.error('poll-error', chartError);
    } catch (error) {
      ui.error('poll-error', `${error.message} Данные могут быть устаревшими. Повторный опрос через секунду.`);
    } finally {
      if (!disposed && !terminal) timer = setTimeout(poll, 1000);
    }
  }

  stop.addEventListener('click', async () => {
    if (stopRequested) return;
    stopRequested = true;
    stop.disabled = true;
    stop.textContent = 'Остановка запрошена…';
    try {
      await ui.requestJson(`${base}/stop`, { method: 'POST' });
      ui.error('stop-error', '');
    } catch (error) {
      ui.error('stop-error', `${error.message} Проверьте состояние опыта. Повторный запрос автоматически не отправляется.`);
    }
  });
  window.addEventListener('pagehide', () => {
    disposed = true;
    clearTimeout(timer);
    Object.values(charts).forEach(chart => chart.destroy());
  });
  poll();
})();
