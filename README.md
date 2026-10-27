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
bash infrastructure/scripts/run-experiment.sh infrastructure/scenarios/smoke-fixed.yaml
bash infrastructure/scripts/stop-experiment.sh <experiment-id>
bash infrastructure/scripts/lab-down.sh
```

Базовый стенд реализован в Task 2. Команды `run-experiment.sh` и
`stop-experiment.sh` появятся вместе с Runner на следующих этапах.
Проверка стенда: `bash infrastructure/tests/compose-smoke.sh dev`.
Настройка и протокол проверок: [Task 2 — базовый стенд](docs/implementation/task-02-base-kafka-cruise-control-lab.md).

Ядро генератора нагрузки реализовано в Task 3: расписание, асинхронные отправки
и остановка. Проверка из `experiment-platform`: `bash ./gradlew :load-generator:test`.
HTTP API и журнал измерений добавляются в Task 4.

## Документация

- [Отчёты по реализации задач](docs/implementation/README.md). После каждой задачи отчёт сохраняется в `docs/implementation/`.
- [Спецификация](docs/superpowers/specs/2026-09-10-adaptive-replica-recovery-design.md)
- [План реализации](docs/superpowers/plans/2026-09-10-adaptive-replica-recovery.md)

