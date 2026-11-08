"""Prepare scenario inputs and check evidence emitted by a real acceptance run."""

import csv
import json
import re
import sys
from pathlib import Path


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def json_lines(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def render(source, seed, variant):
    import yaml

    scenario = yaml.safe_load(source.read_text(encoding="utf-8"))
    if seed:
        scenario["seed"] = int(seed)
    if variant:
        variants = yaml.safe_load((source.parent / "variants/methods.yaml").read_text(encoding="utf-8"))
        override = variants[variant]["recovery"].copy()
        execution = override.pop("execution", {})
        scenario["recovery"].update(override)
        scenario["recovery"].setdefault("execution", {}).update(execution)
    print(json.dumps(scenario, ensure_ascii=False))


def check_result(result, case, outcome):
    for name in ("experiment.json", "metrics.csv", "decisions.jsonl", "events.log", "summary.json"):
        assert (result / name).is_file(), f"Missing {name} in {result}"
    experiment = read(result / "experiment.json")
    summary = read(result / "summary.json")
    assert experiment["id"] == summary["experimentId"] == result.name
    assert experiment["schemaVersion"] == summary["schemaVersion"] == experiment["spec"]["schemaVersion"]
    assert experiment["state"] == summary["finalState"] == outcome, summary
    assert summary["filesComplete"] and summary["cleanup"]["attempted"] and summary["cleanup"]["success"], summary
    with (result / "metrics.csv").open(encoding="utf-8", newline="") as file:
        metrics = list(csv.DictReader(file))
    decisions = json_lines(result / "decisions.jsonl")
    if outcome == "COMPLETED":
        assert metrics and decisions and experiment["recoveryFinishedAt"], "Missing live measurements"
        if not experiment["spec"]["recovery"]["adaptiveThrottleEnabled"]:
            expected = experiment["spec"]["recovery"]["initialThrottleMiBps"] * 1048576
            assert {row["throttleBytesPerSecond"] for row in metrics} == {str(expected)}, metrics
    if outcome == "FAILED":
        assert experiment["faultAt"] and summary["reason"] == "EXECUTION_FAILED", summary
        assert "LOAD_GENERATOR_NOT_RUNNING_OR_REPLACED" in (result / "events.log").read_text(encoding="utf-8")
    if outcome == "CANCELLED":
        assert experiment["recoveryStartedAt"] and summary["reason"] == "USER_STOP", summary
    if case.startswith("adaptive-"):
        actions = {row.get("lastAction") for row in decisions}
        progress = {float(row["progressBytesPerSecond"]) for row in metrics if row["progressBytesPerSecond"]}
        limits = {int(row["throttleBytesPerSecond"]) for row in metrics if row["throttleBytesPerSecond"]}
        assert len(decisions) > 2 and len(progress) >= 2 and len(limits) >= 2, (len(decisions), progress, limits)
        assert {"INCREASE", "DECREASE"} <= actions, actions
    if case == "metrics-stale":
        assert any(row.get("constraintState") == "METRICS_STALE" for row in decisions), decisions
        fallback = experiment["spec"]["recovery"]["safeFallbackThrottleMiBps"] * 1048576
        assert any(row["throttleBytesPerSecond"] == str(fallback) for row in metrics), fallback
    if case == "unsatisfiable":
        assert any(row.get("constraintState") == "CONSTRAINTS_UNSATISFIABLE" for row in decisions), decisions
        assert any(row.get("lastAction") == "PROGRESS_GUARD" or row.get("reason") == "PROGRESS_GUARD"
                   for row in decisions), decisions
    print(json.dumps({"case": case, "experimentId": experiment["id"], "seed": experiment["spec"]["seed"],
                      "schemaVersion": experiment["schemaVersion"], "finalState": outcome,
                      "durationSeconds": summary["totalDurationSeconds"], "directory": str(result)}))


def check_order(log, result, fixture, placement_after):
    events = (result / "events.log").read_text(encoding="utf-8")
    task_id = re.search(r"RECOVERY_TASK ([0-9a-f-]{36})", events)[1]
    source = log.read_text(encoding="utf-8")
    expected = [(item["topic"], item["partition"]) for item in fixture["expectedStarts"]]
    events = []
    for line in source.splitlines():
        if f"User task {task_id}: Starting inter-broker task " in line:
            match = re.search(r"EXE_ID: (\d+),.*\{([\w.-]+)-(\d+), oldLeader:", line)
            assert match, line
            events.append({"event": "TASK_STARTED", "topic": match[2], "partition": int(match[3]),
                           "executionId": int(match[1]), "line": line})
    actual = [(item["topic"], item["partition"]) for item in events]
    assert [key for key in actual if key in expected] == expected, (expected, actual)
    racks = fixture["brokerRacks"]
    if fixture["scenario"] == "domain-positive":
        repaired = next(row for row in placement_after if row["topic"] == "acceptance-domain-repair")
        assert len(repaired["isr"]) == 4 and len({racks[str(id)] for id in repaired["isr"]}) == 3, repaired
    else:
        for row in placement_after:
            assert len(row["isr"]) == 3 and len({racks[str(id)] for id in row["isr"]}) == 3, row
    print(json.dumps({"userTaskId": task_id, "starts": events, "placementAfter": placement_after}, indent=2))


def check_repeats(index, order):
    adaptive = [row for row in index if re.fullmatch(r"adaptive-[1-5]", row["case"])]
    assert len(adaptive) == 5, adaptive
    assert [row["seed"] for row in adaptive] == order and sorted(order) == list(range(1, 6)), adaptive
    assert len({row["experimentId"] for row in adaptive}) == 5, adaptive
    assert len({row["schemaVersion"] for row in adaptive}) == 1, adaptive
    print(json.dumps(adaptive, indent=2))


def check_matrix(index):
    rows = [row for row in index if row["case"].startswith("method-")]
    assert len(rows) == 6 and len({row["experimentId"] for row in rows}) == 6, rows
    baseline = None
    for row in rows:
        directory = Path(row["directory"])
        spec = read(directory / "experiment.json")["spec"]
        controls = tuple(spec[key] for key in ("seed", "slo", "load", "fault", "resources"))
        if baseline is None:
            baseline = controls
        assert controls == baseline, row
        summary = read(directory / "summary.json")
        row.update(recovery=spec["recovery"], recoveryTimeSeconds=summary["recoveryTimeSeconds"],
                   criticalPartitionSeconds=summary["criticalPartitionSeconds"], riskDataComplete=summary["riskDataComplete"],
                   sloViolationTimeSeconds=summary["sloViolationTimeSeconds"])
    print(json.dumps(rows, indent=2))


if __name__ == "__main__":
    command, *args = sys.argv[1:]
    if command == "render":
        render(Path(args[0]), *args[1:])
    elif command == "result":
        check_result(Path(args[0]), *args[1:])
    elif command == "order":
        check_order(Path(args[0]), Path(args[1]), read(Path(args[2])), read(Path(args[3])))
    elif command == "repeats":
        check_repeats(json_lines(Path(args[0])), read(Path(args[1])))
    elif command == "matrix":
        check_matrix(json_lines(Path(args[0])))
    else:
        sys.exit(f"Unknown acceptance check: {command}")
