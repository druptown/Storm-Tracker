"""Upload one file through the configured HA SSH add-on."""
from __future__ import annotations

import os
from pathlib import Path
import sys
from paramiko.client import AutoAddPolicy, SSHClient

source = Path(sys.argv[1]).resolve()
destination = sys.argv[2]
password = os.environ.get("STV3_SSH_PASSWORD")
key_filename = os.environ.get("STV3_SSH_KEY")
client = SSHClient()
client.set_missing_host_key_policy(AutoAddPolicy())
client.connect(
    "192.168.0.144",
    username="wim",
    password=password,
    key_filename=key_filename,
    timeout=15,
    look_for_keys=not bool(password or key_filename),
    allow_agent=False,
)
try:
    channel = client.get_transport().open_session()
    channel.exec_command(f"cat > {destination}")
    with source.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            channel.sendall(chunk)
    channel.shutdown_write()
    raise SystemExit(channel.recv_exit_status())
finally:
    client.close()
