# Adaptive Replica Recovery

Дипломный проект по адаптивному управлению восстановлением реплик Apache Kafka. Метод реализуется в локальной копии Cruise Control, а экспериментальная платформа управляет нагрузкой, стендом и сбором результатов.

## Требования

- Windows с WSL2;
- Docker Desktop с интеграцией WSL2;
- Java 17;
- Bash, `curl` и `jq` внутри WSL2.

Gradle устанавливать отдельно не требуется: Java-проекты запускаются через wrapper из `experiment-platform`.

## Исходники Cruise Control

`cruise-control/` — Git submodule форка
[ArtemChepenkov/cruise-control](https://github.com/ArtemChepenkov/cruise-control).
Основной репозиторий хранит ссылку на конкретный коммит, исходники и их история
находятся в форке.

После клонирования проекта выполните из его корня:

```bash
git submodule update --init --recursive
```

Для нового клонирования можно сразу использовать `git clone --recurse-submodules <URL проекта>`.
Обновление родительского репозитория не обновляет версию Cruise Control автоматически:
`git submodule update --init --recursive` устанавливает именно закреплённый коммит.
Перед изменениями кода Cruise Control переключитесь внутри submodule на рабочую
ветку: после `submodule update` обычно используется detached HEAD.

В текущей рабочей копии `origin` Cruise Control указывает на форк, `upstream` — на
официальный репозиторий. В новом клоне `upstream` при необходимости добавляется отдельно:

```bash
git -C cruise-control remote add upstream https://github.com/cruise-control-for-kafka/cruise-control.git
```

## Основные команды

Команды выполняются внутри WSL2 из `/mnt/c/diplom`:

```bash
bash infrastructure/scripts/lab-up.sh dev
bash infrastructure/scripts/lab-down.sh
```

Базовый стенд реализован в Task 2.
Проверка стенда: `bash infrastructure/tests/compose-smoke.sh dev`.
Настройка и протокол проверок: [Task 2 — базовый стенд](docs/implementation/task-02-base-kafka-cruise-control-lab.md).

Ядро генератора нагрузки реализовано в Task 3: расписание, асинхронные отправки
и остановка. Проверка из `experiment-platform`: `bash ./gradlew :load-generator:test`.
В Task 4 добавлены HTTP API и CSV-журнал измерений.

Из `experiment-platform` запустите `bash ./gradlew :load-generator:bootRun`.
Статус: `http://localhost:8081/api/load/status`, метрики:
`http://localhost:8081/api/load/metrics/current`.
При работающих Kafka и API проверка из корня проекта:
`bash infrastructure/tests/load-api-smoke.sh`.
Команды ручного запуска нагрузки и описание результатов:
[Task 4 — метрики и HTTP API](docs/implementation/task-04-load-metrics-and-http-api.md).

## Документация

Фундамент Runner реализован в Task 5: YAML-конфигурации, состояния, файловое
хранилище, summary и обработка незавершённых запусков после рестарта.
Проверка из `experiment-platform`: `bash ./gradlew :experiment-runner:test`.
HTTP API и координатор Runner реализованы в Task 6. Профиль `test-lab` проверяет
полный цикл без физических отказов; настоящий DockerLabController относится к Task 9.
[Описание и демонстрационный результат Task 5](docs/implementation/task-05-runner-state-and-result-store.md).

Сборка Runner: `bash ./gradlew :experiment-runner:bootJar` из `experiment-platform`.
Для имитационного запуска используйте `--spring.profiles.active=test-lab`.
[Task 6 — запуск API и проверка](docs/implementation/task-06-experiment-orchestration-api.md).

В Task 7 добавлены страницы запуска `/`, наблюдения `/experiments/{id}` и
результатов `/experiments/{id}/results`. Графики и YAML-парсер входят в JAR;
доступ к CDN при работе не нужен.

Для проверки UI без изменения Kafka запустите из `experiment-platform`:

```bash
bash ./gradlew :experiment-runner:bootRun --args='--spring.profiles.active=test-lab'
```

Откройте `http://localhost:8080`. Режим TEST-LAB явно отмечен: показатели
искусственные, физические отказ и восстановление не выполняются.
CLI использует тот же REST API. Из корня проекта:

```bash
bash infrastructure/scripts/run-experiment.sh infrastructure/scenarios/runner-smoke.yaml
bash infrastructure/scripts/stop-experiment.sh <experiment-id>
bash infrastructure/scripts/reset-lab.sh
```

`RUNNER_API_URL` меняет адрес API для скриптов. Настоящий Docker-адаптер
остаётся Task 9; `runner-smoke.yaml` предназначен для test-lab.
Для JS-тестов нужен Node.js 20+ (можно запускать из Windows, независимо от Java в WSL).
Для JS-тестов нужен Node.js 20+ (можно запускать из Windows, независимо от Java в WSL).
Проверки: `node --test infrastructure/tests/ui-behaviour.cjs` и
`bash infrastructure/tests/api-scripts-smoke.sh` (при работающем test-lab).
[Task 7 — интерфейс, скрипты и результаты проверок](docs/implementation/task-07-experiment-ui-and-scripts.md).

В Task 8 добавлен служебный мониторинг: JMX exporters Kafka/Cruise Control,
Prometheus на `http://localhost:9091`, read-only Docker stats и сборщик Runner.
`lab-up.sh dev` запускает 4 брокера и 5 targets; `final` — 6 брокеров и 7 targets.
Scrape — 5 секунд, хранение Prometheus — 24 часа. Проверка после запуска стенда:

```bash
bash infrastructure/tests/monitoring-smoke.sh dev
```

В обычном профиле Runner использует реальный мониторинг. `MONITORING_BROKER_IDS`
задаёт ожидаемые брокеры (по умолчанию `1,2,3,4`; для final — `1,2,3,4,5,6`),
`COMPOSE_PROJECT_NAME` должен совпадать с проектом стенда. Пользовательские SLO
по-прежнему измеряет Load Generator; служебные метрики их не заменяют.
Недоступность диагностики даёт warning и неполный diagnostic summary, но сама
по себе не останавливает опыт. В `metrics.csv` добавлена последняя колонка
`monitoringSnapshot` с JSON по брокерам; старые файлы из 13 колонок читаются.

Для проверки отказа Prometheus используйте test-lab с явным подключением реальной
диагностики: `--runner.test-lab.live-monitoring=true --runner.test-lab.recovery-polls=30`.
Тогда `monitoring-outage-smoke.sh` останавливает только Prometheus и восстанавливает
его готовность; отказы Kafka и восстановление остаются имитационными.
[Task 8 — мониторинг и контрольная точка](docs/implementation/task-08-lab-monitoring.md).

- [Отчёты по реализации задач](docs/implementation/README.md). После каждой задачи отчёт сохраняется в `docs/implementation/`.
- [Спецификация](docs/superpowers/specs/2026-09-10-adaptive-replica-recovery-design.md)
- [План реализации](docs/superpowers/plans/2026-09-10-adaptive-replica-recovery.md)

