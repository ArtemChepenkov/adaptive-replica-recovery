(async function () {
  'use strict';
  const ui = RunnerUi;
  const id = document.body.dataset.experimentId;
  const base = `/api/experiments/${encodeURIComponent(id)}`;
  try {
    const record = await ui.requestJson(base);
    ui.text('experiment-name', record.spec.name);
    ui.text('final-state', record.state);
    if (!ui.terminal(record.state)) {
      ui.error('result-error', 'Опыт ещё не завершён. Откройте «Графики и состояние», чтобы наблюдать за ним.');
      return;
    }
    const [summary, trace] = await Promise.all([
      ui.requestJson(`${base}/summary`), ui.requestJson(`${base}/events`)
    ]);
    ui.text('final-reason', summary.reason);
    ui.text('cleanup-state', summary.cleanup?.success ? 'Подтверждена' : 'Не подтверждена');
    ui.text('time-to-safe', ui.number(summary.timeToSafeSeconds, ' с'));
    ui.text('recovery-time', ui.number(summary.recoveryTimeSeconds, ' с'));
    ui.text('critical-seconds', ui.number(summary.criticalPartitionSeconds));
    ui.text('slo-violation', ui.number(summary.sloViolationTimeSeconds, ' с'));
    ui.text('max-p99', ui.number(summary.produceP99Ms, ' мс'));
    ui.text('mean-error', ui.percent(summary.errorRate));
    const coverage = [];
    if (!summary.riskDataComplete) coverage.push(`Показатели риска неполные: ${ui.number(summary.missingRiskSeconds, ' с')} без измерений.`);
    if (summary.missingLatencySeconds > 0) coverage.push(`Задержки не измерены на интервале ${ui.number(summary.missingLatencySeconds, ' с')}.`);
    if (!summary.diagnosticDataComplete) coverage.push('Ресурсная диагностика неполная.');
    ui.text('coverage', coverage.length ? coverage.join(' ') : 'Все предусмотренные интервалы измерений покрыты.');
    ui.text('result-location', `Каталог запуска в настроенном хранилище Runner: ${id}/`);
    ui.text('files-state', summary.filesComplete
      ? 'Сохранены experiment.json, metrics.csv, decisions.jsonl, events.log и summary.json.'
      : 'Полнота файлов результата не подтверждена.');
    document.getElementById('simulation-notice').hidden = !(document.body.dataset.simulated === 'true'
      || trace.events.some(event => event.message === 'SIMULATED_TEST_LAB_NO_PHYSICAL_INFRASTRUCTURE'));
    ui.events('events', trace.events);
  } catch (error) {
    ui.error('result-error', error.message);
  }
})();
