from pathlib import Path

from storm_tracker_worker.cpu_topology import worker_cpu_set


def _write_siblings(root: Path, cpu: int, siblings: str) -> None:
    path = root / f"cpu{cpu}" / "topology"
    path.mkdir(parents=True)
    (path / "thread_siblings_list").write_text(siblings, encoding="ascii")


def test_worker_reserves_one_complete_hyperthreaded_core(tmp_path: Path) -> None:
    for cpu, siblings in enumerate(("0,4", "1,5", "2,6", "3,7", "0,4", "1,5", "2,6", "3,7")):
        _write_siblings(tmp_path, cpu, siblings)

    assert worker_cpu_set(sysfs_root=tmp_path) == (0, 1, 2, 4, 5, 6)
    assert worker_cpu_set(
        sysfs_root=tmp_path,
        maximum_logical_cpus=2,
    ) == (0, 1)


def test_worker_fallback_reserves_two_logical_cpus(tmp_path: Path) -> None:
    assert worker_cpu_set(sysfs_root=tmp_path, logical_cpu_count=8) == (0, 1, 2, 3, 4, 5)


def test_worker_intersects_topology_with_container_affinity(tmp_path: Path) -> None:
    for cpu, siblings in enumerate(
        ("0,4", "1,5", "2,6", "3,7", "0,4", "1,5", "2,6", "3,7")
    ):
        _write_siblings(tmp_path, cpu, siblings)

    assert worker_cpu_set(
        sysfs_root=tmp_path,
        allowed_cpus={1, 2, 3, 5, 6, 7},
    ) == (1, 2, 5, 6)


def test_worker_affinity_fallback_never_returns_disallowed_cpu(
    tmp_path: Path,
) -> None:
    assert worker_cpu_set(
        sysfs_root=tmp_path,
        allowed_cpus={2, 4, 6, 8},
    ) == (2, 4)


def test_run_script_applies_nice_and_reserved_cpu_set() -> None:
    runner = (
        Path(__file__).resolve().parents[1]
        / "storm_tracker_worker"
        / "run.sh"
    ).read_text(encoding="utf-8")
    assert "storm_tracker_worker.cpu_topology" in runner
    assert 'taskset --cpu-list "${STV3_WORKER_CPU_SET}"' in runner
    assert 'nice -n "${STV3_WORKER_NICE}"' in runner
    assert 'STV3_CPU_THREADS="2"' in runner
    assert 'STV3_MAX_WORKER_LOGICAL_CPUS="${STV3_CPU_THREADS}"' in runner
    assert "storm_tracker_worker.cpu_limited_launcher" in runner
    assert '--limit "${STV3_CPU_LIMIT_PERCENT}"' in runner


def test_worker_compute_profiles_keep_shared_ha_safe_and_bound_nas() -> None:
    root = Path(__file__).resolve().parents[1]
    runner = (root / "storm_tracker_worker" / "run.sh").read_text(
        encoding="utf-8"
    )
    config = (root / "storm_tracker_worker" / "config.yaml").read_text(
        encoding="utf-8"
    )

    assert 'compute_profile: "shared_ha"' in config
    assert (
        'compute_profile: "list(shared_ha|dedicated_nas|dedicated_nas_fast)"'
        in config
    )
    assert 'cpu_limit_percent: "int(25,200)"' in config
    assert 'case "${STV3_COMPUTE_PROFILE}" in' in runner
    assert "shared_ha)" in runner
    assert "dedicated_nas)" in runner
    assert "dedicated_nas_fast)" in runner
    assert 'STV3_CPU_LIMIT_PERCENT="25"' in runner
    assert 'STV3_CPU_LIMIT_PERCENT="50"' in runner
    assert 'STV3_CPU_LIMIT_PERCENT="200"' in runner
    assert 'STV3_WORKER_NICE="15"' in runner
    assert 'STV3_WORKER_NICE="10"' in runner
    assert 'STV3_CPU_THREADS="2"' in runner
    assert "minstens een fysieke core blijft vrij" in runner
