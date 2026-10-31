# Lab monitoring contract

Kafka 4.0.0 and the pinned Cruise Control JVMs run JMX Exporter 1.4.0 on internal
port 9404. Prometheus 3.5.1 scrapes every 5 seconds and retains data for 24 hours.
Only Prometheus port 9091 is published, bound to localhost. Exporters have no
published host ports. No Docker socket or privileged monitoring container is used.

`lab-up.sh dev` selects four Kafka targets; `lab-up.sh final` selects six.
For direct Compose commands use `MONITORING_PROFILE=dev` or `final` to select the
corresponding read-only `targets-*.json` mount. Kafka series have `job="kafka"`
and `broker_id="1"` through `"4"`/`"6"`; Cruise Control uses `job="cruise-control"`.
Inactive final-profile brokers are not targets in the dev configuration.

| Exported metric | Meaning |
|---|---|
| `broker_network_out_bytes_total` / `broker_network_in_bytes_total` | Recording rules summing client + replication payload counters; no per-topic duplicates |
| `broker_client_out_bytes_total` / `broker_client_in_bytes_total` | Aggregate BrokerTopicMetrics BytesOutPerSec/BytesInPerSec Count |
| `broker_replication_out_bytes_total` / `broker_replication_in_bytes_total` | Aggregate ReplicationBytesOutPerSec/ReplicationBytesInPerSec Count |
| `broker_under_replicated_partitions` | ReplicaManager UnderReplicatedPartitions Value |
| `broker_request_latency_ms{request,statistic}` | TotalTimeMs Mean/95thPercentile/99thPercentile; service diagnostics, not producer SLO latency |
| `broker_replica_fetcher_lag{client_id,topic,partition}` | Follower lag in offsets; absent if no follower fetcher exists |
| `cruise_control_executor_replica_actions{type,state}` | Task counts by `replica-action`, `intra-broker-replica-action`, `leadership-action` and six states |
| `cruise_control_executor_movements_total{type}` | Completed task meter Count |
| `cruise_control_executor_partition_data_movement_mebibytes_total` | Estimated data size of terminal tasks |
| `cruise_control_executor_partition_data_movement_mebibytes_per_second` | Terminal-task estimated data OneMinuteRate, not wire throughput |

Cruise Control's data movement meter is updated for completed, aborted and dead
tasks; it is not a measurement of physically copied bytes. Actual replica lag
and progress for the recovery method must continue to come from its own signals.
Active replica action count excludes leadership tasks and sums `in-progress`
and `aborting`; pending task count is a separate queue state.

Kafka 4 routes follower responses to ReplicationBytesOutPerSec instead of
BytesOutPerSec. Therefore client and replication counters are added by recording
rules, evaluated every 5 seconds. ReassignmentBytesOutPerSec is a subset of
replication traffic and is not added a second time. These are Kafka payload bytes,
not physical NIC traffic including protocol/packet overhead or other processes.

CPU, memory and block-write rates are read separately from Docker stats in Runner.
Prometheus service diagnostics never replace Load Generator's SLO measurements.

Read-only check after starting the lab:

```bash
bash infrastructure/tests/monitoring-smoke.sh dev
```

The smoke requires all expected target identities to be healthy, validates real
metric names and finite values, and verifies scrape/retention settings. It does
not accept a matching count of unrelated `up` metrics as proof. Replicated topics
created by lab-up provide follower-fetcher metrics.

Sources: [Kafka 4.0 monitoring](https://kafka.apache.org/40/operations/monitoring/),
[JMX Exporter Java agent](https://prometheus.github.io/jmx_exporter/1.4.0/deployment/java-agent/),
[Prometheus configuration](https://prometheus.io/docs/prometheus/latest/configuration/configuration/).
Cruise Control names are checked in the local pinned `ExecutionTaskTracker`,
`KafkaCruiseControlApp` and `LegacyObjectNameFactory` sources.

The exporter JAR is fetched from the official 1.4.0 release and verified against
SHA-256 `db1492e95a7ee95cd5e0a969875c0d4f0ef6413148d750351a41cc71d775f59a`
(also published by the GitHub release API). No unversioned download is used.
