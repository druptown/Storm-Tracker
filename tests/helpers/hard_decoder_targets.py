"""Tiny importable targets for hard-decoder isolation subprocess tests."""
from __future__ import annotations

import os
from pathlib import Path
import time


def echo(request):
    return {"echo": request, "pid": os.getpid()}


def sleep_then_echo(request):
    time.sleep(float(request["seconds"]))
    return {"echo": request, "pid": os.getpid()}


def environment_presence(request):
    return {
        str(key): str(key) in os.environ
        for key in request["keys"]
    }


def sleep_forever(_request):
    while True:
        time.sleep(60)


def mark_then_sleep_forever(request):
    Path(request["path"]).write_text("started", encoding="utf-8")
    while True:
        time.sleep(60)


def crash(_request):
    os._exit(73)


def oversized_result(request):
    return {"value": "x" * int(request["bytes"])}


def non_json_result(_request):
    return {"not-json"}


def raise_with_secret(request):
    raise RuntimeError(f"provider rejected credential {request['secret']}")


def write_marker(request):
    Path(request["path"]).write_text("decoder-ran", encoding="utf-8")
    return {"written": True}


def burn_cpu(_request):
    value = 1
    while True:
        value = (value * 1_103_515_245 + 12_345) & 0x7FFFFFFF


def exhaust_memory(request):
    chunks = []
    chunk_bytes = int(request.get("chunk_bytes", 8 * 1024 * 1024))
    while True:
        chunk = bytearray(chunk_bytes)
        for offset in range(0, len(chunk), 4096):
            chunk[offset] = 1
        chunks.append(chunk)
