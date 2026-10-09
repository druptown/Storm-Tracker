"""Download one file through the configured HA SSH add-on."""
from __future__ import annotations

import os
from pathlib import Path
import sys
import paramiko

source = sys.argv[1]
destination = Path(sys.argv[2]).resolve()
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(
    "192.168.0.144",
    username="wim",
    password=os.environ["STV3_SSH_PASSWORD"],
    timeout=15,
    look_for_keys=False,
    allow_agent=False,
)
try:
    _, stdout, stderr = client.exec_command(f"cat {source}", timeout=60)
    content = stdout.read()
    error = stderr.read().decode("utf-8", "replace")
    status = stdout.channel.recv_exit_status()
    if status:
        raise RuntimeError(error)
    destination.write_bytes(content)
finally:
    client.close()
