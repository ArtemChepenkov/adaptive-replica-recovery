# Adaptive Replica Recovery

Дипломный проект по адаптивному управлению восстановлением реплик Apache Kafka. Метод реализуется в локальной копии Cruise Control, а экспериментальная платформа управляет нагрузкой, стендом и сбором результатов.

## Требования

- Windows с WSL2;
- Docker Desktop с интеграцией WSL2;
- Java 17;
- Bash, `curl` и `jq` внутри WSL2.

Gradle устанавливать отдельно не требуется: Java-проекты запускаются через wrapper из `experiment-platform`.

## Основные команды

Команды выполняются внутри WSL2 из `/mnt/c/diplom`:

```bash
bash infrastructure/scripts/lab-up.sh dev
bash infrastructure/scripts/run-experiment.sh infrastructure/scenarios/smoke-fixed.yaml
bash infrastructure/scripts/stop-experiment.sh <experiment-id>
bash infrastructure/scripts/lab-down.sh
```

Скрипты стенда будут добавлены на следующих этапах реализации.

## Документация

- [Спецификация](docs/superpowers/specs/2026-09-10-adaptive-replica-recovery-design.md)
- [План реализации](docs/superpowers/plans/2026-09-10-adaptive-replica-recovery.md)

