"""Exporteer een begrensde motion-replaydataset read-only via de HA SSH-add-on."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shlex
import sys

from paramiko.client import AutoAddPolicy, SSHClient


SOURCE = "/config/.storage/storm_tracker_v3_calibration.sqlite3"


def main() -> None:
    destination = Path(sys.argv[1]).resolve()
    region_id = sys.argv[2] if len(sys.argv) > 2 else "region-2"
    target_id = sys.argv[3] if len(sys.argv) > 3 else "test_tracker"
    remote_code = f"""
import json, sqlite3
db = sqlite3.connect("file:{SOURCE}?mode=ro&immutable=1", uri=True)
db.row_factory = sqlite3.Row
def rows(sql, args):
    return [dict(row) for row in db.execute(sql, args).fetchall()]
payload = {{
    "region_id": {region_id!r},
    "target_id": {target_id!r},
    "frames": rows(
        "SELECT region_id, source, nominal_timestamp, saved_at, frame_json "
        "FROM pysteps_frame_cache WHERE region_id=? "
        "ORDER BY nominal_timestamp DESC LIMIT 24",
        ({region_id!r},),
    ),
    "preflight": rows(
        "SELECT * FROM pysteps_preflight_samples WHERE region_key=? "
        "ORDER BY nominal_minute DESC LIMIT 48",
        ({region_id!r},),
    ),
    "verification": rows(
        "SELECT nominal_minute, observed_at, own_status, own_distance_km, "
        "own_eta_minutes, own_passage, own_confidence, "
        "own_forecast_available, snapshot_json "
        "FROM forecast_verification_samples WHERE target_id=? "
        "ORDER BY nominal_minute DESC LIMIT 96",
        ({target_id!r},),
    ),
}}
db.close()
print(json.dumps(payload, separators=(",", ":")))
"""
    client = SSHClient()
    client.set_missing_host_key_policy(AutoAddPolicy())
    client.connect(
        "192.168.0.144",
        username=os.environ.get("STV3_SSH_USERNAME", "wim"),
        password=os.environ["STV3_SSH_PASSWORD"],
        timeout=15,
        look_for_keys=False,
        allow_agent=False,
    )
    try:
        _, stdout, stderr = client.exec_command(
            f"python3 -c {shlex.quote(remote_code)}", timeout=120
        )
        content = stdout.read()
        error = stderr.read().decode("utf-8", "replace")
        status = stdout.channel.recv_exit_status()
        if status:
            raise RuntimeError(error or f"remote export gaf status {status}")
        parsed = json.loads(content)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            json.dumps(parsed, indent=2, sort_keys=True), encoding="utf-8"
        )
        print(
            f"{destination}: {len(parsed['frames'])} frames, "
            f"{len(parsed['preflight'])} preflight, "
            f"{len(parsed['verification'])} verificaties"
        )
    finally:
        client.close()


if __name__ == "__main__":
    main()
