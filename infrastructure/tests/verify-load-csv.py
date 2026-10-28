"""Recompute a generator snapshot from its raw CSV without HdrHistogram."""
import argparse
import csv
import datetime as dt
import json
import math
from pathlib import Path


def epoch_millis(value):
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    delta = parsed - dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    return delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000


def verify(snapshot_path, csv_path):
    snapshot = json.loads(Path(snapshot_path).read_text(encoding="utf-8"))
    start, end = map(epoch_millis, (snapshot["windowStart"], snapshot["windowEnd"]))
    with Path(csv_path).open(encoding="utf-8", newline="") as source:
        all_rows = list(csv.DictReader(source))
    assert len({int(row["sequence"]) for row in all_rows}) == len(all_rows), "duplicate sequence"
    rows = [row for row in all_rows if start <= int(row["timestampEpochMs"]) <= end]
    successes = [row for row in rows if row["success"] == "true"]
    latencies = sorted(int(row["latencyMicros"]) for row in successes)
    errors = len(rows) - len(successes)
    assert snapshot["requestCount"] == len(rows), "request count differs"
    assert snapshot["errorCount"] == errors, "error count differs"
    assert snapshot["bytesSent"] == sum(int(row["bytes"]) for row in successes), "bytes differ"
    assert math.isclose(snapshot["errorRate"], errors / len(rows) if rows else 0, abs_tol=1e-12)
    comparisons = {}
    for rank in (50, 95, 99):
        field = f"p{rank}Ms"
        exact = latencies[math.ceil(len(latencies) * rank / 100) - 1] / 1000 if latencies else None
        actual = snapshot[field]
        difference = abs(actual - exact) if exact is not None else None
        if exact is None:
            assert actual is None, f"{field} must be null without successful writes"
        else:
            assert math.isfinite(actual) and difference <= 1.0, f"{field} differs by {difference} ms"
        comparisons[field] = {"exact": exact, "api": actual, "differenceMs": difference}
    return {"status": "PASS", "csvRows": len(all_rows), "windowRequests": len(rows),
            "windowErrors": errors, "percentiles": comparisons}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot")
    parser.add_argument("csv")
    args = parser.parse_args()
    print(json.dumps(verify(args.snapshot, args.csv), indent=2))
