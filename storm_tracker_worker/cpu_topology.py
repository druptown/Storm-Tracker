"""Selecteer worker-CPU's terwijl één volledige fysieke core vrij blijft."""
from __future__ import annotations

import os
from pathlib import Path
from collections.abc import Iterable


def _cpu_number(path: Path) -> int:
    return int(path.parent.parent.name.removeprefix("cpu"))


def physical_core_groups(sysfs_root: Path = Path("/sys/devices/system/cpu")) -> list[tuple[int, ...]]:
    """Lees unieke thread-siblinggroepen uit Linux sysfs."""
    groups: set[tuple[int, ...]] = set()
    for topology_file in sysfs_root.glob("cpu[0-9]*/topology/thread_siblings_list"):
        values: set[int] = set()
        for part in topology_file.read_text(encoding="ascii").strip().split(","):
            if "-" in part:
                start, end = (int(value) for value in part.split("-", 1))
                values.update(range(start, end + 1))
            elif part:
                values.add(int(part))
        if not values:
            values.add(_cpu_number(topology_file))
        groups.add(tuple(sorted(values)))
    return sorted(groups, key=lambda group: min(group))


def worker_cpu_set(
    *,
    sysfs_root: Path = Path("/sys/devices/system/cpu"),
    logical_cpu_count: int | None = None,
    allowed_cpus: Iterable[int] | None = None,
) -> tuple[int, ...]:
    """Reserve the last physical core inside this container's CPU affinity."""
    allowed = (
        {int(cpu) for cpu in allowed_cpus}
        if allowed_cpus is not None else None
    )
    groups = []
    for group in physical_core_groups(sysfs_root):
        visible = tuple(
            cpu for cpu in group if allowed is None or cpu in allowed
        )
        if visible and visible not in groups:
            groups.append(visible)
    if len(groups) >= 2:
        reserved = set(groups[-1])
        available = sorted({cpu for group in groups for cpu in group} - reserved)
        if available:
            return tuple(available)
    if allowed is not None:
        candidates = sorted(allowed)
        if not candidates:
            return ()
    else:
        count = (
            logical_cpu_count
            if logical_cpu_count is not None else (os.cpu_count() or 1)
        )
        candidates = list(range(count))
    reserve = 2 if len(candidates) >= 4 else 1
    kept = max(1, len(candidates) - reserve)
    return tuple(candidates[:kept])


def main() -> None:
    affinity = (
        os.sched_getaffinity(0)
        if hasattr(os, "sched_getaffinity") else None
    )
    print(",".join(
        str(cpu) for cpu in worker_cpu_set(allowed_cpus=affinity)
    ))


if __name__ == "__main__":
    main()
