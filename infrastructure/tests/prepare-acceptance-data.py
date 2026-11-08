"""Stage and verify the replica placements for a dedicated acceptance lab."""

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import subprocess
import time


ROOT = Path(__file__).resolve().parents[2]
RECORD_BYTES = 32 * 1024
MIB = 1024 * 1024
RACKS = {1: "zone-a", 2: "zone-b", 3: "zone-c", 4: "zone-a", 5: "zone-b", 6: "zone-c"}
PARTITION = re.compile(
    r"\bTopic:\s+(\S+)\s+Partition:\s+(\d+)\s+Leader:\s+(-?\d+)"
    r"\s+Replicas:\s*([\d,]+)\s+Isr:\s*([\d,]*)"
)
OFFSET = re.compile(r"^([^:\r\n]+):(\d+):(\d+)\r?$", re.MULTILINE)


@dataclass(frozen=True)
class Topic:
    name: str
    size_mib: int
    initial: tuple
    expected: tuple

    @property
    def records(self):
        return self.size_mib * MIB // RECORD_BYTES


def balanced(partitions):
    leaders = ((1, 2, 3), (2, 3, 1), (3, 1, 2))
    return tuple(leaders[partition % len(leaders)] for partition in range(partitions))


def skewed(name, size_mib, partitions):
    placement = balanced(partitions)
    return Topic(name, size_mib, tuple(replicas[:2] + (4,) for replicas in placement), placement)


FIXED = skewed("acceptance-fixed", 48, 6)
ADAPTIVE = skewed("acceptance-recovery", 256, 12)
SIGNALS = skewed("acceptance-signals", 128, 12)
POSITIVE = (
    Topic("acceptance-critical-small", 32, ((1, 2),), ((1, 6),)),
    Topic("acceptance-domain-repair", 64, ((1, 4, 2, 3),), ((1, 4, 2, 6),)),
    Topic("acceptance-normal-large", 128, ((1, 2, 3, 4),), ((1, 2, 3, 6),)),
)
NEGATIVE = Topic("acceptance-negative-safe", 32, ((1, 2, 3),) * 2, ((4, 2, 3),) * 2)
SCENARIOS = {
    "smoke-fixed": (FIXED,),
    "smoke-adaptive-step": (ADAPTIVE,),
    "domain-positive": POSITIVE,
    "domain-negative": (NEGATIVE,),
    "metrics-stale": (SIGNALS,),
    "unsatisfiable": (SIGNALS,),
}
BACKGROUND = {
    "acceptance-load": balanced(12),
    "__CruiseControlMetrics": ((1, 2, 3),),
    "__KafkaCruiseControlPartitionMetricSamples": ((1, 2, 3),),
    "__KafkaCruiseControlModelTrainingSamples": ((1, 2, 3),),
}


class Lab:
    def __init__(self):
        project = os.environ.get("COMPOSE_PROJECT_NAME", "")
        override = Path(os.environ.get("ACCEPTANCE_COMPOSE_OVERRIDE", ""))
        if not re.fullmatch(r"task14-[a-z0-9][a-z0-9-]*", project) or not override.is_absolute() or not override.is_file():
            raise RuntimeError("Expected a task14- Compose project and absolute ACCEPTANCE_COMPOSE_OVERRIDE")
        self.exec = [
            "docker", "compose", "--profile", "final", "-p", project,
            "-f", str(ROOT / "infrastructure/docker-compose.yml"), "-f", str(override),
            "exec", "-T", "-e", "KAFKA_HEAP_OPTS=-Xmx256m", "broker-1",
        ]

    def run(self, args, data=None, timeout=90):
        result = subprocess.run(self.exec + args, input=data, capture_output=True, text=True, timeout=timeout)
        if result.returncode:
            raise RuntimeError(f"{args[0]} exited {result.returncode}: {(result.stdout + result.stderr).strip()[-1600:]}")
        return result.stdout

    def kafka(self, tool, *args):
        return self.run([f"/opt/kafka/bin/kafka-{tool}.sh", "--bootstrap-server", "broker-1:9092", *args])

    def with_json(self, tool, payload, *args):
        command = 'file=$(mktemp); trap \'rm -f -- "$file"\' EXIT; cat > "$file"; "$@" "$file"'
        return self.run(
            ["bash", "-eu", "-c", command, "acceptance-json", f"/opt/kafka/bin/kafka-{tool}.sh",
             "--bootstrap-server", "broker-1:9092", *args],
            data=json.dumps(payload),
        )

    def create(self, name, placement, *configs):
        assignment = ",".join(":".join(map(str, replicas)) for replicas in placement)
        self.kafka("topics", "--create", "--topic", name, "--replica-assignment", assignment, *configs)

    def describe(self, names):
        output = self.kafka("topics", "--describe", "--topic", topic_pattern(names))
        return [
            {"topic": topic, "partition": int(partition), "leader": int(leader),
             "replicas": list(map(int, replicas.split(","))), "isr": list(map(int, isr.split(","))) if isr else []}
            for topic, partition, leader, replicas, isr in PARTITION.findall(output)
        ]

    def idle(self):
        output = self.kafka("reassign-partitions", "--list")
        if "No partition reassignments found" not in output:
            raise RuntimeError(f"Expected no active reassignments, got {output.strip()}")
        return output


def topic_pattern(names):
    return "^(?:" + "|".join(re.escape(name) for name in names) + ")$"


def save(path, data):
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def expected_rows(placements):
    return {(topic, partition): list(replicas)
            for topic, partitions in placements.items() for partition, replicas in enumerate(partitions)}


def matches(rows, expected, preferred):
    return len(rows) == len(expected) and {(row["topic"], row["partition"]) for row in rows} == set(expected) and all(
        expected.get((row["topic"], row["partition"])) == row["replicas"]
        and sorted(row["isr"]) == sorted(row["replicas"])
        and (not preferred or row["leader"] == row["replicas"][0])
        for row in rows
    )


def wait_for_placement(lab, placements, preferred):
    expected = expected_rows(placements)
    deadline = time.monotonic() + 300
    while True:
        rows = lab.describe(placements)
        if matches(rows, expected, preferred):
            return rows
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Expected stable replica placement {expected}, got {rows}")
        time.sleep(3)


def verify_brokers(lab, evidence):
    output = lab.kafka("broker-api-versions")
    (evidence / "broker-racks.txt").write_text(output, encoding="utf-8")
    racks = {int(broker): rack for broker, rack in re.findall(r"\bid:\s+(\d+)\s+rack:\s+([^\s)]+)", output)}
    if racks != RACKS:
        raise RuntimeError(f"Expected six live brokers with racks {RACKS}, got {racks}")


def verify_data(lab, topics, evidence):
    output = lab.kafka("get-offsets", "--topic", topic_pattern(topic.name for topic in topics), "--time", "-1")
    offsets = {(name, int(partition)): int(offset) for name, partition, offset in OFFSET.findall(output)}
    measurements = {}
    for topic in topics:
        counts = [offsets.get((topic.name, i), 0) for i in range(len(topic.expected))]
        if any(count <= 0 for count in counts) or sum(counts) != topic.records:
            raise RuntimeError(f"Expected {topic.records} seeded records on {topic.name}, got {counts}")
        measurements[topic.name] = counts
    save(evidence / "seed-offsets.json", measurements)


def prepare(lab, topics, evidence):
    lab.idle()
    verify_brokers(lab, evidence)
    existing = set(lab.kafka("topics", "--list").splitlines())
    required = {topic.name for topic in topics}
    obsolete = {topic.name for fixtures in SCENARIOS.values() for topic in fixtures} - required
    for name in sorted(existing & obsolete):
        lab.kafka("topics", "--delete", "--topic", name)
    for name, placement in BACKGROUND.items():
        if name not in existing:
            configs = ("--config", "retention.bytes=8388608", "--config", "segment.bytes=2097152") if name == "acceptance-load" else ()
            lab.create(name, placement, *configs)

    new_topics = [topic for topic in topics if topic.name not in existing]
    quota = ("--entity-type", "clients", "--entity-default")
    if new_topics and "producer_byte_rate=" in lab.kafka("configs", "--describe", *quota):
        lab.kafka("configs", "--alter", *quota, "--delete-config", "producer_byte_rate")
    try:
        for topic in new_topics:
            lab.create(topic.name, topic.initial, "--config", "min.insync.replicas=1")
            output = lab.run([
                "/opt/kafka/bin/kafka-producer-perf-test.sh", "--topic", topic.name,
                "--num-records", str(topic.records), "--record-size", str(RECORD_BYTES), "--throughput", "-1",
                "--producer-props", "bootstrap.servers=broker-1:9092", "acks=all",
                "partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner",
            ], timeout=240)
            (evidence / f"seed-{topic.name}.log").write_text(output, encoding="utf-8")
    finally:
        (evidence / "producer-quota.txt").write_text(
            lab.kafka("configs", "--alter", *quota, "--add-config", "producer_byte_rate=6291456"), encoding="utf-8")

    placements = {topic.name: topic.expected for topic in topics}
    expected = expected_rows(placements)
    current = {(row["topic"], row["partition"]): row["replicas"] for row in lab.describe(placements)}
    plan = {"version": 1, "partitions": [
        {"topic": topic, "partition": partition, "replicas": replicas, "log_dirs": ["any"] * len(replicas)}
        for (topic, partition), replicas in expected.items() if current.get((topic, partition)) != replicas
    ]}
    save(evidence / "reassignment.json", plan)
    if plan["partitions"]:
        output = lab.with_json("reassign-partitions", plan, "--execute", "--reassignment-json-file")
        (evidence / "reassignment-execute.txt").write_text(output, encoding="utf-8")
    rows = wait_for_placement(lab, placements, preferred=False)
    election = {"partitions": [
        {"topic": row["topic"], "partition": row["partition"]} for row in rows if row["leader"] != row["replicas"][0]
    ]}
    if election["partitions"]:
        output = lab.with_json("leader-election", election, "--election-type", "preferred", "--path-to-json-file")
        (evidence / "preferred-leaders.txt").write_text(output, encoding="utf-8")
    return wait_for_placement(lab, placements, preferred=True)


def manifest(scenario, topics):
    starts = [{"topic": topic.name, "partition": partition}
              for topic in topics for partition in range(len(topic.expected))]
    return {
        "scenario": scenario, "brokerRacks": RACKS, "faultBroker": 6,
        "topics": [{"topic": topic.name, "payloadBytes": topic.records * RECORD_BYTES, "records": topic.records,
                    "partitions": [{"partition": partition, "replicas": replicas}
                                   for partition, replicas in enumerate(topic.expected)]} for topic in topics],
        "expectedStarts": starts if scenario.startswith("domain-") else [],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--verify", action="store_true")
    modes.add_argument("--snapshot", action="store_true")
    parser.add_argument("scenario", choices=SCENARIOS)
    parser.add_argument("evidence", type=Path)
    args = parser.parse_args()
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=True)
    topics = SCENARIOS[args.scenario]
    placements = {topic.name: topic.expected for topic in topics}
    lab = Lab()
    if args.snapshot:
        rows = lab.describe(placements)
        save(evidence / "placement-after.json", rows)
        if {(row["topic"], row["partition"]) for row in rows} != set(expected_rows(placements)):
            raise RuntimeError(f"Incomplete replica placement snapshot {rows}")
        return
    if args.verify:
        verify_brokers(lab, evidence)
        rows = lab.describe(placements)
        if not matches(rows, expected_rows(placements), preferred=True):
            raise RuntimeError(f"Fixture drift before submission: {rows}")
        output = "verified-placement.json"
    else:
        rows = prepare(lab, topics, evidence)
        output = "placement-before.json"
    save(evidence / output, rows)
    background = lab.describe(BACKGROUND)
    save(evidence / "background-placement.json", background)
    if not matches(background, expected_rows(BACKGROUND), preferred=False):
        raise RuntimeError(f"Unexpected background topic placement: {background}")
    (evidence / "reassignments-before.txt").write_text(lab.idle(), encoding="utf-8")
    verify_data(lab, topics, evidence)
    save(evidence / "fixture.json", manifest(args.scenario, topics))
    print(f"Verified {args.scenario} data and placement in {evidence}")


if __name__ == "__main__":
    main()
