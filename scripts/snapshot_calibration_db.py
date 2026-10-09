"""Maak via SSH een consistente lokale snapshot van de live kalibratiedatabase."""
from __future__ import annotations

import os
from pathlib import Path
import shlex
import sys

from paramiko.client import AutoAddPolicy, SSHClient


SOURCE = "/config/.storage/storm_tracker_v3_calibration.sqlite3"
def main() -> None:
    destination = Path(sys.argv[1]).resolve()
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
        code = (
            "import sqlite3,sys;"
            f"s=sqlite3.connect('file:{SOURCE}?mode=ro',uri=True);"
            "d=sqlite3.connect(':memory:');"
            "s.backup(d);s.close();"
            "sys.stdout.buffer.write(d.serialize());d.close()"
        )
        _, stdout, stderr = client.exec_command(
            f"python3 -c {shlex.quote(code)}", timeout=120
        )
        content = stdout.read()
        error = stderr.read().decode("utf-8", "replace")
        status = stdout.channel.recv_exit_status()
        if status:
            raise RuntimeError(error or f"remote backup gaf status {status}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
        print(f"{destination} ({len(content)} bytes)")
    finally:
        client.close()


if __name__ == "__main__":
    main()
