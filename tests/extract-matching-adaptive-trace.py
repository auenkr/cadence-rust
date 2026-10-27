#!/usr/bin/env python3
"""Extract matching_adaptive_trace records into compact correlation JSON."""

import argparse
import json
import shlex
from collections import defaultdict
from pathlib import Path


PREFIXES = ("matching_adaptive_trace", "matching_retry_boundary")
ALIASES = {
    "workflow_id": ("workflow", "workflow_id", "workflowID", "workflowId"),
    "run_id": ("run", "run_id", "runID", "runId"),
    "schedule_id": ("schedule_id", "scheduleID", "scheduleId"),
}
POLL_BOUNDARIES = {"poll_wait_start", "poll_wait_result", "poll_received", "poll_returned"}


def parse_record(line, source, line_number):
    try:
        words = shlex.split(line, comments=False, posix=True)
    except ValueError:
        return None
    if not words or words[0] not in PREFIXES:
        return None

    fields = {}
    for word in words[1:]:
        if "=" in word:
            key, value = word.split("=", 1)
            fields[key] = value
    record = {
        "source": str(source),
        "line": line_number,
        "timestamp": fields.get("ts"),
        "task_list": fields.get("task_list"),
        "boundary": fields.get("boundary"),
        "identity": {},
        "fields": fields,
    }
    for normalized, aliases in ALIASES.items():
        for alias in aliases:
            if alias in fields:
                record["identity"][normalized] = fields[alias]
                break
    return record


def extract(paths):
    groups = defaultdict(list)
    scaler_ticks = []
    poll_only = []
    ungrouped = []
    records_seen = 0

    for path in paths:
        with path.open(encoding="utf-8", errors="replace") as stream:
            for line_number, line in enumerate(stream, 1):
                record = parse_record(line, path, line_number)
                if record is None:
                    continue
                records_seen += 1
                boundary = record["boundary"]
                identity = record["identity"]
                if boundary == "scaler_tick":
                    scaler_ticks.append(record)
                elif identity:
                    key = tuple((name, identity.get(name)) for name in ALIASES)
                    groups[key].append(record)
                elif boundary in POLL_BOUNDARIES or (boundary and boundary.startswith("poll_")):
                    poll_only.append(record)
                else:
                    ungrouped.append(record)

    task_groups = []
    for key, events in groups.items():
        task_groups.append({
            "identity": dict(key_item for key_item in key if key_item[1] is not None),
            "events": events,
        })
    task_groups.sort(key=lambda group: tuple(group["identity"].get(k, "") for k in ALIASES))
    return {
        "records_seen": records_seen,
        "task_events": task_groups,
        "scaler_ticks": scaler_ticks,
        "poll_only_events": poll_only,
        "ungrouped_events": ungrouped,
        "notes": [
            "Fields retain their input string values; unknown key=value fields remain in fields.",
            "Missing timestamps and boundaries are null; records are not joined by timestamps.",
            "Token presence or unrelated identifiers do not establish acknowledgement or success.",
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", type=Path, help="downloaded Matching log files")
    parser.add_argument("-o", "--output", type=Path, help="write JSON here (default: stdout)")
    args = parser.parse_args()
    result = extract(args.logs)
    rendered = json.dumps(result, separators=(",", ":"), ensure_ascii=False) + "\n"
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")


if __name__ == "__main__":
    main()
