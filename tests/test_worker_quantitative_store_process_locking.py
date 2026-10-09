from __future__ import annotations

from importlib import import_module
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest


_LOCK_HOLDER = r"""
from importlib import import_module
from pathlib import Path
import sys
import time

module = import_module(sys.argv[1])
store_type = getattr(module, sys.argv[2])
root = Path(sys.argv[3]).resolve()
ready = Path(sys.argv[4])
release = Path(sys.argv[5])
store_type(root, max_objects=1)
with module._exclusive_store_lock(root):
    ready.write_text("ready", encoding="ascii")
    deadline = time.monotonic() + 15.0
    while not release.exists():
        if time.monotonic() >= deadline:
            raise RuntimeError("release signal timed out")
        time.sleep(0.01)
"""


_PUBLISHER = r"""
from importlib import import_module
from pathlib import Path
import sys

module = import_module(sys.argv[1])
store_type = getattr(module, sys.argv[2])
root = Path(sys.argv[3]).resolve()
attempt = Path(sys.argv[4])
done = Path(sys.argv[5])
store = store_type(root, max_objects=1)
attempt.write_text("attempt", encoding="ascii")
store.publish(b"cross-process-payload")
done.write_text("done", encoding="ascii")
"""


_PROFILE_OPENER = r"""
from importlib import import_module
from pathlib import Path
import sys

module = import_module(sys.argv[1])
store_type = getattr(module, sys.argv[2])
root = Path(sys.argv[3]).resolve()
try:
    store_type(root, max_objects=int(sys.argv[4]))
except Exception as error:
    if "immutable limits" not in str(error):
        raise
    print("profile-conflict")
else:
    print("opened")
"""


def _wait_for(path: Path, process: subprocess.Popen[str], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while not path.exists():
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise AssertionError(
                f"child exited before {path.name}: {stdout!r} {stderr!r}"
            )
        if time.monotonic() >= deadline:
            raise AssertionError(f"child did not create {path.name}")
        time.sleep(0.01)


@pytest.mark.parametrize(
    ("module_name", "store_name"),
    [
        (
            "storm_tracker_worker.dwd_rv_quantitative",
            "DwdRvDerivedArtifactStore",
        ),
        (
            "storm_tracker_worker.knmi_rtcor_quantitative",
            "KnmiRtcorDerivedArtifactStore",
        ),
    ],
)
def test_derived_store_lock_serializes_separate_processes(
    tmp_path: Path,
    module_name: str,
    store_name: str,
) -> None:
    root = (tmp_path / "derived").resolve()
    ready = tmp_path / "ready"
    release = tmp_path / "release"
    attempt = tmp_path / "attempt"
    done = tmp_path / "done"
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"

    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            _LOCK_HOLDER,
            module_name,
            store_name,
            str(root),
            str(ready),
            str(release),
        ],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    publisher: subprocess.Popen[str] | None = None
    try:
        _wait_for(ready, holder, 10.0)
        publisher = subprocess.Popen(
            [
                sys.executable,
                "-c",
                _PUBLISHER,
                module_name,
                store_name,
                str(root),
                str(attempt),
                str(done),
            ],
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        _wait_for(attempt, publisher, 10.0)
        time.sleep(0.25)
        assert done.exists() is False
        assert publisher.poll() is None
        release.write_text("release", encoding="ascii")

        publisher_stdout, publisher_stderr = publisher.communicate(timeout=10)
        holder_stdout, holder_stderr = holder.communicate(timeout=10)
        assert holder.returncode == 0, (holder_stdout, holder_stderr)
        assert publisher.returncode == 0, (publisher_stdout, publisher_stderr)
        assert done.read_text(encoding="ascii") == "done"
    finally:
        release.write_text("release", encoding="ascii")
        for process in (publisher, holder):
            if process is not None and process.poll() is None:
                process.kill()
                process.communicate(timeout=5)


@pytest.mark.parametrize(
    ("module_name", "store_name"),
    [
        (
            "storm_tracker_worker.dwd_rv_quantitative",
            "DwdRvDerivedArtifactStore",
        ),
        (
            "storm_tracker_worker.knmi_rtcor_quantitative",
            "KnmiRtcorDerivedArtifactStore",
        ),
    ],
)
def test_derived_store_process_lock_wait_obeys_callers_deadline(
    tmp_path: Path,
    module_name: str,
    store_name: str,
) -> None:
    root = (tmp_path / "deadline-derived").resolve()
    ready = tmp_path / "deadline-ready"
    release = tmp_path / "deadline-release"
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"

    module = import_module(module_name)
    store_type = getattr(module, store_name)
    store = store_type(root, max_objects=1)
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            _LOCK_HOLDER,
            module_name,
            store_name,
            str(root),
            str(ready),
            str(release),
        ],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_for(ready, holder, 10.0)
        deadline = time.monotonic() + 0.2
        stages: list[str] = []

        def checkpoint(stage: str) -> None:
            stages.append(stage)
            if time.monotonic() >= deadline:
                raise TimeoutError("test deadline expired")

        started = time.monotonic()
        with pytest.raises(TimeoutError, match="test deadline"):
            store.publish(b"must-not-publish", checkpoint=checkpoint)
        elapsed = time.monotonic() - started

        assert elapsed < 2.0
        assert "artifact_store_process_lock_wait" in stages
        assert list(store.objects_path.iterdir()) == []
        assert list(store.staging_path.iterdir()) == []
    finally:
        release.write_text("release", encoding="ascii")
        stdout, stderr = holder.communicate(timeout=10)
        assert holder.returncode == 0, (stdout, stderr)


@pytest.mark.parametrize(
    ("module_name", "store_name"),
    [
        (
            "storm_tracker_worker.dwd_rv_quantitative",
            "DwdRvDerivedArtifactStore",
        ),
        (
            "storm_tracker_worker.knmi_rtcor_quantitative",
            "KnmiRtcorDerivedArtifactStore",
        ),
    ],
)
def test_first_process_atomically_binds_one_immutable_store_profile(
    tmp_path: Path,
    module_name: str,
    store_name: str,
) -> None:
    root = (tmp_path / "profile-race").resolve()
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    processes = [
        subprocess.Popen(
            [
                sys.executable,
                "-c",
                _PROFILE_OPENER,
                module_name,
                store_name,
                str(root),
                str(limit),
            ],
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for limit in (1, 2)
    ]
    outputs: list[str] = []
    for process in processes:
        stdout, stderr = process.communicate(timeout=15)
        assert process.returncode == 0, (stdout, stderr)
        outputs.append(stdout.strip())

    assert sorted(outputs) == ["opened", "profile-conflict"]
    profile = json.loads(
        (root / ".store-profile.json").read_text(encoding="utf-8")
    )
    winner = int(profile["limits"]["max_objects"])
    assert winner in {1, 2}
    module = import_module(module_name)
    store_type = getattr(module, store_name)
    artifact_error = getattr(
        module,
        store_name.replace("DerivedArtifactStore", "ArtifactError"),
    )
    assert store_type(root, max_objects=winner).object_count() == 0
    with pytest.raises(artifact_error, match="immutable limits"):
        store_type(root, max_objects=3 - winner)
