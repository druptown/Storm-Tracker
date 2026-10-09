"""Run one bounded command through the configured HA SSH add-on."""
from __future__ import annotations

import os
import sys
from paramiko.client import AutoAddPolicy, SSHClient

password = os.environ.get("STV3_SSH_PASSWORD")
key_filename = os.environ.get("STV3_SSH_KEY")
client = SSHClient()
client.set_missing_host_key_policy(AutoAddPolicy())
client.connect(
    "192.168.0.15",
    username="hassio",
    password=password,
    key_filename=key_filename,
    timeout=15,
    look_for_keys=not bool(password or key_filename),
    allow_agent=False,
)
try:
    _, stdout, stderr = client.exec_command(sys.argv[1], timeout=900)
    output = stdout.read().decode("utf-8", "replace")
    error = stderr.read().decode("utf-8", "replace")
    status = stdout.channel.recv_exit_status()
    print(output, end="")
    print(error, end="", file=sys.stderr)
    raise SystemExit(status)
finally:
    client.close()
