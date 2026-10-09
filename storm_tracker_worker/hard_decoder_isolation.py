"""Hard, one-shot process isolation for future quantitative decoder jobs.

This module is deliberately not wired into :mod:`storm_tracker_worker.service`.
It is a candidate execution primitive: callers must first prove that a decoder
is safe to kill between any two instructions and that every durable side
effect is either idempotent or transactionally published.

Only canonical JSON crosses the explicit parent/child protocol.  Python's
``spawn`` bootstrap still pickles the parent-created ``request_bytes`` object
internally; this module never accepts or decodes an externally supplied pickle.
"""
from __future__ import annotations

from dataclasses import dataclass
import ctypes
from ctypes import wintypes
import errno
import importlib
import json
import math
import multiprocessing
from multiprocessing.connection import Connection
import os
import re
import signal
import stat
import sys
import tempfile
import threading
import time
from typing import Any, Literal, Mapping


_TARGET_RE = re.compile(
    r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*$",
    re.ASCII,
)
_CONTRACT_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{2,127}$", re.ASCII)
_GLOBAL_JOB_SLOT = threading.Lock()
# ``multiprocessing.Process`` has no per-child environment argument.  Raw I/O
# children may execute concurrently, but their short parent-side spawn window
# must remain serialized while os.environ is temporarily narrowed.
_ENVIRONMENT_SPAWN_LOCK = threading.Lock()
_PROCESS_SLOT_NAME = "storm-tracker-v3-hard-decoder-v1"
_POLL_SECONDS = 0.02
_TERMINATE_GRACE_SECONDS = 0.35
_PROTOCOL_OVERHEAD_BYTES = 512
_MAX_JSON_DEPTH = 64
_MAX_JSON_NODES = 262_144
_ENVIRONMENT_KEY_RE = re.compile(r"^[A-Z][A-Z0-9_]{0,127}$", re.ASCII)
_MAX_ENVIRONMENT_OVERRIDE_BYTES = 8192

JsonScalar = None | bool | int | float | str
JsonValue = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]


@dataclass(frozen=True, slots=True)
class DecoderResourceLimits:
    """Per-job limits; a fresh child makes CPU accounting job-local."""

    wall_seconds: float = 60.0
    cpu_seconds: float = 45.0
    memory_bytes: int = 1_073_741_824
    max_request_bytes: int = 1_048_576
    max_result_bytes: int = 16_777_216

    def __post_init__(self) -> None:
        if type(self.wall_seconds) not in {float, int}:
            raise TypeError("wall_seconds must be numeric")
        if type(self.cpu_seconds) not in {float, int}:
            raise TypeError("cpu_seconds must be numeric")
        for name in (
            "memory_bytes",
            "max_request_bytes",
            "max_result_bytes",
        ):
            if type(getattr(self, name)) is not int:
                raise TypeError(f"{name} must be an integer")
        if not math.isfinite(self.wall_seconds) or not (
            0.05 <= self.wall_seconds <= 3_600.0
        ):
            raise ValueError("wall_seconds must be finite and within 0.05..3600")
        if not math.isfinite(self.cpu_seconds) or not (
            0.05 <= self.cpu_seconds <= 3_600.0
        ):
            raise ValueError("cpu_seconds must be finite and within 0.05..3600")
        if not (32 * 1024 * 1024 <= self.memory_bytes <= 64 * 1024**3):
            raise ValueError("memory_bytes must be within 32 MiB..64 GiB")
        if not (1 <= self.max_request_bytes <= 16 * 1024**2):
            raise ValueError("max_request_bytes must be within 1 B..16 MiB")
        if not (1 <= self.max_result_bytes <= 256 * 1024**2):
            raise ValueError("max_result_bytes must be within 1 B..256 MiB")


@dataclass(frozen=True, slots=True)
class DecoderEffectContract:
    """Proof claim required before a decoder may enter the child.

    ``durable_effects`` describes the only accepted kill-safety strategies.
    ``none`` is suitable for pure decode work; durable writers must be either
    idempotent or transactional.  ``hard_kill_safe`` remains explicit so that
    merely selecting a strategy never silently asserts that the implementation
    has actually been audited.
    """

    contract_id: str
    hard_kill_safe: bool
    durable_effects: Literal["none", "idempotent", "transactional"]
    spawns_descendants: bool

    def __post_init__(self) -> None:
        if not _CONTRACT_ID_RE.fullmatch(self.contract_id):
            raise ValueError("invalid decoder effect contract id")
        if self.durable_effects not in {
            "none",
            "idempotent",
            "transactional",
        }:
            raise ValueError("unverified decoder durable-effect strategy")
        if type(self.hard_kill_safe) is not bool:
            raise TypeError("hard_kill_safe must be boolean")
        if type(self.spawns_descendants) is not bool:
            raise TypeError("spawns_descendants must be boolean")

    @property
    def admitted(self) -> bool:
        return (
            self.hard_kill_safe
            and not self.spawns_descendants
            and self.durable_effects
            in {"none", "idempotent", "transactional"}
        )


DecoderOutcome = Literal[
    "succeeded",
    "busy",
    "unsafe_effect_contract",
    "invalid_request",
    "unsupported_platform",
    "boundary_setup_failed",
    "deadline_exceeded",
    "cancelled",
    "cpu_limit_exceeded",
    "memory_limit_exceeded",
    "target_load_failed",
    "decoder_failed",
    "result_not_json",
    "result_too_large",
    "child_crashed",
    "protocol_error",
]

ProcessSlotPolicy = Literal["exclusive", "independent"]


@dataclass(frozen=True, slots=True)
class DecoderProcessResult:
    """Technical execution outcome; this object never asserts weather state."""

    outcome: DecoderOutcome
    result: JsonValue | None = None
    detail_code: str | None = None
    child_exit_code: int | None = None
    elapsed_seconds: float = 0.0
    child_cpu_seconds: float | None = None
    child_peak_rss_bytes: int | None = None
    child_peak_boundary_bytes: int | None = None

    @property
    def succeeded(self) -> bool:
        return self.outcome == "succeeded"


@dataclass(frozen=True, slots=True)
class HardBoundarySupport:
    supported: bool
    platform: str
    mechanism: str
    scope: str


class _CpuLimitReached(BaseException):
    """Raised only by the POSIX SIGXCPU handler."""


class _JsonSizeLimitExceeded(ValueError):
    """Canonical JSON cannot fit the configured byte envelope."""


def _platform_family() -> str:
    if sys.platform.startswith("linux"):
        return "linux"
    if sys.platform == "win32":
        return "windows"
    return "unsupported"


def hard_boundary_support() -> HardBoundarySupport:
    """Report only boundaries this implementation can set up fail-closed."""
    family = _platform_family()
    if family == "linux":
        try:
            import resource
        except ImportError:
            return HardBoundarySupport(
                False, "linux", "unavailable", "decoder_process_only"
            )
        required = ("RLIMIT_AS", "RLIMIT_CPU", "setrlimit")
        if not all(hasattr(resource, name) for name in required):
            return HardBoundarySupport(
                False, "linux", "unavailable", "decoder_process_only"
            )
        try:
            getattr(ctypes.CDLL(None), "prctl")
        except (AttributeError, OSError):
            return HardBoundarySupport(
                False, "linux", "unavailable", "decoder_process_only"
            )
        try:
            clock_ticks = int(os.sysconf("SC_CLK_TCK"))
        except (OSError, TypeError, ValueError):
            clock_ticks = 0
        if clock_ticks <= 0 or not os.path.isfile("/proc/self/stat"):
            return HardBoundarySupport(
                False, "linux", "unavailable", "decoder_process_only"
            )
        return HardBoundarySupport(
            True,
            "linux",
            "RLIMIT_AS+RLIMIT_CPU+/proc_accounting+PDEATHSIG+process_group_kill",
            "decoder_process_only_no_descendant_budget",
        )
    if family == "windows" and os.name == "nt":
        return HardBoundarySupport(
            True,
            "windows",
            "JobObject(process_memory+process_time+kill_on_close)",
            "decoder_job_object",
        )
    return HardBoundarySupport(
        False,
        sys.platform,
        "unavailable",
        "none",
    )


class _CrossProcessDecoderSlot:
    """One non-blocking decoder slot across parent processes.

    Linux uses a same-runtime-namespace file lock.  Windows uses a named local
    kernel mutex.  Both are released by the operating system if the owner
    process dies.
    """

    _WINDOWS_MUTEX_NAME = rf"Local\{_PROCESS_SLOT_NAME}"
    _WAIT_OBJECT_0 = 0x00000000
    _WAIT_ABANDONED = 0x00000080
    _WAIT_TIMEOUT = 0x00000102
    _WAIT_FAILED = 0xFFFFFFFF

    def __init__(self) -> None:
        self._fd: int | None = None
        self._handle = None
        self._acquired = False

    def try_acquire(self) -> Literal["acquired", "busy", "error"]:
        family = _platform_family()
        if family == "linux":
            return self._try_acquire_linux()
        if family == "windows" and os.name == "nt":
            return self._try_acquire_windows()
        return "error"

    def _try_acquire_linux(self) -> Literal["acquired", "busy", "error"]:
        try:
            import fcntl

            lock_path = os.path.join(
                tempfile.gettempdir(), f"{_PROCESS_SLOT_NAME}.lock"
            )
            flags = os.O_RDWR | os.O_CREAT
            flags |= getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(lock_path, flags, 0o600)
            metadata = os.fstat(fd)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_nlink != 1
                or metadata.st_uid != os.geteuid()
            ):
                os.close(fd)
                return "error"
            os.fchmod(fd, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                os.close(fd)
                return "busy"
            self._fd = fd
            self._acquired = True
            return "acquired"
        except (ImportError, OSError):
            return "error"

    def _try_acquire_windows(self) -> Literal["acquired", "busy", "error"]:
        try:
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.CreateMutexW.argtypes = [
                wintypes.LPVOID,
                wintypes.BOOL,
                wintypes.LPCWSTR,
            ]
            kernel32.CreateMutexW.restype = wintypes.HANDLE
            kernel32.WaitForSingleObject.argtypes = [
                wintypes.HANDLE,
                wintypes.DWORD,
            ]
            kernel32.WaitForSingleObject.restype = wintypes.DWORD
            handle = kernel32.CreateMutexW(
                None, False, self._WINDOWS_MUTEX_NAME
            )
            if not handle:
                return "error"
            self._handle = (kernel32, handle)
            wait = int(kernel32.WaitForSingleObject(handle, 0))
            if wait in {self._WAIT_OBJECT_0, self._WAIT_ABANDONED}:
                self._acquired = True
                return "acquired"
            self.release()
            if wait == self._WAIT_TIMEOUT:
                return "busy"
            return "error"
        except (AttributeError, OSError, ValueError):
            self.release()
            return "error"

    def release(self) -> None:
        if self._fd is not None:
            try:
                import fcntl

                if self._acquired:
                    fcntl.flock(self._fd, fcntl.LOCK_UN)
            except (ImportError, OSError):
                pass
            try:
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None
        if self._handle is not None:
            kernel32, handle = self._handle
            try:
                kernel32.ReleaseMutex.argtypes = [wintypes.HANDLE]
                kernel32.ReleaseMutex.restype = wintypes.BOOL
                kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
                kernel32.CloseHandle.restype = wintypes.BOOL
                if self._acquired:
                    kernel32.ReleaseMutex(handle)
                kernel32.CloseHandle(handle)
            except (AttributeError, OSError, ValueError):
                pass
            self._handle = None
        self._acquired = False


def _validate_json_tree(
    value: Any,
    *,
    max_encoded_bytes: int | None = None,
) -> None:
    stack: list[tuple[Any, int]] = [(value, 0)]
    seen_containers: set[int] = set()
    nodes = 0
    encoded_size_floor = 0
    while stack:
        current, depth = stack.pop()
        nodes += 1
        encoded_size_floor += 1
        if nodes > _MAX_JSON_NODES:
            raise ValueError("JSON node limit exceeded")
        if (
            max_encoded_bytes is not None
            and encoded_size_floor > max_encoded_bytes
        ):
            raise _JsonSizeLimitExceeded("JSON byte lower bound exceeded")
        if depth > _MAX_JSON_DEPTH:
            raise ValueError("JSON nesting limit exceeded")
        if type(current) is str:
            encoded_size_floor += len(current)
            if (
                max_encoded_bytes is not None
                and encoded_size_floor > max_encoded_bytes
            ):
                raise _JsonSizeLimitExceeded(
                    "JSON string lower bound exceeded"
                )
            continue
        if type(current) is int:
            encoded_size_floor += max(1, current.bit_length() // 4)
            if (
                max_encoded_bytes is not None
                and encoded_size_floor > max_encoded_bytes
            ):
                raise _JsonSizeLimitExceeded(
                    "JSON integer lower bound exceeded"
                )
            continue
        if current is None or type(current) is bool:
            continue
        if type(current) is float:
            if not math.isfinite(current):
                raise ValueError("non-finite JSON number")
            continue
        if type(current) not in {list, dict}:
            raise TypeError("value is not canonical JSON")
        identity = id(current)
        if identity in seen_containers:
            raise ValueError("aliased or cyclic JSON container")
        seen_containers.add(identity)
        if type(current) is list:
            stack.extend((item, depth + 1) for item in current)
            continue
        for key, nested in current.items():
            if type(key) is not str:
                raise TypeError("JSON object key is not a string")
            encoded_size_floor += len(key)
            if (
                max_encoded_bytes is not None
                and encoded_size_floor > max_encoded_bytes
            ):
                raise _JsonSizeLimitExceeded(
                    "JSON key lower bound exceeded"
                )
            stack.append((nested, depth + 1))


def _canonical_json_bytes(
    value: Any,
    *,
    max_encoded_bytes: int | None = None,
) -> bytes:
    _validate_json_tree(value, max_encoded_bytes=max_encoded_bytes)
    payload = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if max_encoded_bytes is not None and len(payload) > max_encoded_bytes:
        raise _JsonSizeLimitExceeded("canonical JSON exceeds byte envelope")
    return payload


def _valid_target(target: str) -> bool:
    if not isinstance(target, str) or not _TARGET_RE.fullmatch(target):
        return False
    module_name, function_name = target.split(":", 1)
    return (
        module_name != "__main__"
        and not module_name.startswith("__")
        and not function_name.startswith("__")
    )


def _resolve_target(target: str):
    module_name, function_name = target.split(":", 1)
    function = getattr(importlib.import_module(module_name), function_name)
    if not callable(function):
        raise TypeError("decoder target is not callable")
    return function


def _send_json(connection: Connection, value: Mapping[str, Any]) -> None:
    connection.send_bytes(_canonical_json_bytes(value))


def _receive_json(
    connection: Connection,
    *,
    max_bytes: int,
) -> dict[str, Any]:
    payload = connection.recv_bytes(maxlength=max_bytes)
    value = json.loads(payload.decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("protocol root is not an object")
    return value


def _apply_linux_boundary(limits: Mapping[str, int | float]) -> None:
    import resource

    # The wall deadline is parent-enforced.  PDEATHSIG closes the otherwise
    # dangerous gap where an abruptly crashed parent could leave an orphaned
    # decoder running without that monitor.
    parent_pid = os.getppid()
    libc = ctypes.CDLL(None, use_errno=True)
    prctl = libc.prctl
    prctl.argtypes = [
        ctypes.c_int,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
    ]
    prctl.restype = ctypes.c_int
    if prctl(1, int(signal.SIGKILL), 0, 0, 0) != 0:  # PR_SET_PDEATHSIG
        raise OSError(ctypes.get_errno(), "PR_SET_PDEATHSIG failed")
    if os.getppid() != parent_pid:
        os.kill(os.getpid(), signal.SIGKILL)

    # A separate process group lets the parent terminate ordinary descendants
    # on a wall timeout.  RLIMIT_AS/CPU themselves remain per-process limits.
    os.setsid()
    memory_bytes = int(limits["memory_bytes"])
    resource.setrlimit(resource.RLIMIT_AS, (memory_bytes, memory_bytes))
    cpu_soft = max(1, int(math.ceil(float(limits["cpu_seconds"]))))
    resource.setrlimit(resource.RLIMIT_CPU, (cpu_soft, cpu_soft + 1))
    if hasattr(resource, "RLIMIT_CORE"):
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))

    def cpu_signal(_signum, _frame) -> None:
        raise _CpuLimitReached()

    signal.signal(signal.SIGXCPU, cpu_signal)


def _decoder_child_main(
    connection: Connection,
    target: str,
    request_bytes: bytes,
    limits: Mapping[str, int | float],
    environment_keys: tuple[str, ...] | None,
) -> None:
    """Set the boundary, await parent admission, invoke exactly once, exit."""
    try:
        if environment_keys is not None:
            # ``multiprocessing`` has no per-Process ``env`` argument.  Scrub
            # at the first child-owned instruction, before importing the
            # selected target or decoding any provider request.  The parent
            # environment remains untouched and legacy callers can preserve
            # their existing contract by leaving the policy as ``None``.
            retained = {
                key: os.environ[key]
                for key in environment_keys
                if key in os.environ
            }
            os.environ.clear()
            os.environ.update(retained)
        try:
            signal.signal(signal.SIGINT, signal.SIG_IGN)
        except (AttributeError, OSError, ValueError):
            pass
        if _platform_family() == "linux":
            try:
                _apply_linux_boundary(limits)
            except BaseException:
                _send_json(
                    connection,
                    {"kind": "boundary_setup_failed"},
                )
                return
        _send_json(connection, {"kind": "ready"})
        try:
            command = _receive_json(connection, max_bytes=64)
        except BaseException:
            return
        if command != {"kind": "start"}:
            return

        try:
            request = json.loads(request_bytes.decode("utf-8"))
        except BaseException:
            _send_json(connection, {"kind": "invalid_request"})
            return
        try:
            decoder = _resolve_target(target)
        except BaseException:
            _send_json(connection, {"kind": "target_load_failed"})
            return
        try:
            result = decoder(request)
        except _CpuLimitReached:
            _send_json(connection, {"kind": "cpu_limit_exceeded"})
            return
        except MemoryError:
            _send_json(connection, {"kind": "memory_limit_exceeded"})
            return
        except OSError as err:
            if err.errno == errno.ENOMEM:
                _send_json(connection, {"kind": "memory_limit_exceeded"})
            else:
                _send_json(connection, {"kind": "decoder_failed"})
            return
        except BaseException:
            # No exception text, traceback, arguments or provider input crosses
            # the boundary; this keeps technical failures secret-free.
            _send_json(connection, {"kind": "decoder_failed"})
            return
        try:
            result_bytes = _canonical_json_bytes(
                result,
                max_encoded_bytes=int(limits["max_result_bytes"]),
            )
        except _JsonSizeLimitExceeded:
            _send_json(connection, {"kind": "result_too_large"})
            return
        except (TypeError, ValueError, OverflowError):
            _send_json(connection, {"kind": "result_not_json"})
            return
        if len(result_bytes) > int(limits["max_result_bytes"]):
            _send_json(connection, {"kind": "result_too_large"})
            return
        connection.send_bytes(b'{"kind":"succeeded","result":' + result_bytes + b"}")
    except (_CpuLimitReached, MemoryError):
        # A second failure while constructing the explicit resource message may
        # prevent IPC; the parent then classifies kernel accounting or fails as
        # a child crash, never as meteorological evidence.
        try:
            kind = (
                "cpu_limit_exceeded"
                if sys.exc_info()[0] is _CpuLimitReached
                else "memory_limit_exceeded"
            )
            _send_json(connection, {"kind": kind})
        except BaseException:
            pass
    finally:
        connection.close()


if os.name == "nt":
    _ULONG_PTR = ctypes.c_size_t

    class _IoCounters(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_ulonglong),
            ("WriteOperationCount", ctypes.c_ulonglong),
            ("OtherOperationCount", ctypes.c_ulonglong),
            ("ReadTransferCount", ctypes.c_ulonglong),
            ("WriteTransferCount", ctypes.c_ulonglong),
            ("OtherTransferCount", ctypes.c_ulonglong),
        ]

    class _BasicLimitInformation(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_longlong),
            ("PerJobUserTimeLimit", ctypes.c_longlong),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", _ULONG_PTR),
            ("MaximumWorkingSetSize", _ULONG_PTR),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", _ULONG_PTR),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class _ExtendedLimitInformation(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", _BasicLimitInformation),
            ("IoInfo", _IoCounters),
            ("ProcessMemoryLimit", _ULONG_PTR),
            ("JobMemoryLimit", _ULONG_PTR),
            ("PeakProcessMemoryUsed", _ULONG_PTR),
            ("PeakJobMemoryUsed", _ULONG_PTR),
        ]

    class _BasicAccountingInformation(ctypes.Structure):
        _fields_ = [
            ("TotalUserTime", ctypes.c_longlong),
            ("TotalKernelTime", ctypes.c_longlong),
            ("ThisPeriodTotalUserTime", ctypes.c_longlong),
            ("ThisPeriodTotalKernelTime", ctypes.c_longlong),
            ("TotalPageFaultCount", wintypes.DWORD),
            ("TotalProcesses", wintypes.DWORD),
            ("ActiveProcesses", wintypes.DWORD),
            ("TotalTerminatedProcesses", wintypes.DWORD),
        ]

    class _ProcessMemoryCountersEx(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("PageFaultCount", wintypes.DWORD),
            ("PeakWorkingSetSize", _ULONG_PTR),
            ("WorkingSetSize", _ULONG_PTR),
            ("QuotaPeakPagedPoolUsage", _ULONG_PTR),
            ("QuotaPagedPoolUsage", _ULONG_PTR),
            ("QuotaPeakNonPagedPoolUsage", _ULONG_PTR),
            ("QuotaNonPagedPoolUsage", _ULONG_PTR),
            ("PagefileUsage", _ULONG_PTR),
            ("PeakPagefileUsage", _ULONG_PTR),
            ("PrivateUsage", _ULONG_PTR),
        ]


class _LinuxProcessMonitor:
    """Parent-side exact-process accounting fallback from procfs."""

    def __init__(self, pid: int, limits: DecoderResourceLimits) -> None:
        if _platform_family() != "linux":
            raise OSError("Linux procfs accounting is unavailable")
        self._pid = pid
        self._cpu_seconds_limit = limits.cpu_seconds
        self._memory_bytes_limit = limits.memory_bytes
        self._clock_ticks = int(os.sysconf("SC_CLK_TCK"))
        self._cpu_seconds = 0.0
        self._peak_virtual_bytes = 0
        self._peak_rss_bytes = 0
        self.sample()

    def sample(self) -> None:
        try:
            with open(
                f"/proc/{self._pid}/stat", encoding="ascii"
            ) as stat_file:
                stat_text = stat_file.read()
            fields = stat_text[stat_text.rfind(")") + 2 :].split()
            user_ticks = int(fields[11])
            system_ticks = int(fields[12])
            self._cpu_seconds = max(
                self._cpu_seconds,
                (user_ticks + system_ticks) / self._clock_ticks,
            )
        except (IndexError, OSError, UnicodeError, ValueError):
            pass
        try:
            with open(
                f"/proc/{self._pid}/status", encoding="ascii"
            ) as status_file:
                for line in status_file:
                    if line.startswith(("VmPeak:", "VmSize:")):
                        parts = line.split()
                        if len(parts) >= 2:
                            self._peak_virtual_bytes = max(
                                self._peak_virtual_bytes,
                                int(parts[1]) * 1024,
                            )
                    elif line.startswith(("VmHWM:", "VmRSS:")):
                        parts = line.split()
                        if len(parts) >= 2:
                            self._peak_rss_bytes = max(
                                self._peak_rss_bytes,
                                int(parts[1]) * 1024,
                            )
        except (OSError, UnicodeError, ValueError):
            pass

    def metrics(self) -> tuple[float, int, int]:
        """Return exact-child CPU, RSS and RLIMIT_AS-accounted peak."""
        self.sample()
        return self._cpu_seconds, self._peak_rss_bytes, self._peak_virtual_bytes

    def resource_outcome(
        self,
        *,
        require_limit_reached: bool = False,
    ) -> DecoderOutcome | None:
        del require_limit_reached
        self.sample()
        if self._peak_virtual_bytes >= self._memory_bytes_limit:
            return "memory_limit_exceeded"
        if self._cpu_seconds >= self._cpu_seconds_limit:
            return "cpu_limit_exceeded"
        return None


class _WindowsJobBoundary:
    """Small owned Job Object wrapper; construction is fail-closed."""

    _PROCESS_TIME = 0x00000002
    _PROCESS_MEMORY = 0x00000100
    _KILL_ON_CLOSE = 0x00002000
    _EXTENDED_INFORMATION = 9
    _BASIC_ACCOUNTING = 1
    _PROCESS_TERMINATE = 0x0001
    _PROCESS_SET_QUOTA = 0x0100
    _PROCESS_QUERY_LIMITED_INFORMATION = 0x1000

    def __init__(self, pid: int, limits: DecoderResourceLimits) -> None:
        if os.name != "nt":
            raise OSError("Windows Job Objects are unavailable")
        self._kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        self._psapi = ctypes.WinDLL("psapi", use_last_error=True)
        self._job = wintypes.HANDLE()
        self._process = wintypes.HANDLE()
        self._closed = False
        self._memory_bytes = limits.memory_bytes
        self._cpu_seconds = limits.cpu_seconds
        self._observed_cpu_seconds = 0.0
        self._peak_rss_bytes = 0
        self._peak_boundary_bytes = 0
        self._configure_signatures()
        job = self._kernel32.CreateJobObjectW(None, None)
        if not job:
            self.close()
            raise ctypes.WinError(ctypes.get_last_error())
        self._job = job
        try:
            info = _ExtendedLimitInformation()
            info.BasicLimitInformation.PerProcessUserTimeLimit = max(
                1, int(limits.cpu_seconds * 10_000_000)
            )
            info.BasicLimitInformation.LimitFlags = (
                self._PROCESS_TIME
                | self._PROCESS_MEMORY
                | self._KILL_ON_CLOSE
            )
            info.ProcessMemoryLimit = limits.memory_bytes
            if not self._kernel32.SetInformationJobObject(
                self._job,
                self._EXTENDED_INFORMATION,
                ctypes.byref(info),
                ctypes.sizeof(info),
            ):
                raise ctypes.WinError(ctypes.get_last_error())
            process = self._kernel32.OpenProcess(
                self._PROCESS_TERMINATE
                | self._PROCESS_SET_QUOTA
                | self._PROCESS_QUERY_LIMITED_INFORMATION,
                False,
                pid,
            )
            if not process:
                raise ctypes.WinError(ctypes.get_last_error())
            self._process = process
            if not self._kernel32.AssignProcessToJobObject(
                self._job, self._process
            ):
                raise ctypes.WinError(ctypes.get_last_error())
        except BaseException:
            self.close()
            raise

    def _configure_signatures(self) -> None:
        kernel32 = self._kernel32
        kernel32.CreateJobObjectW.argtypes = [wintypes.LPVOID, wintypes.LPCWSTR]
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.SetInformationJobObject.argtypes = [
            wintypes.HANDLE,
            ctypes.c_int,
            wintypes.LPVOID,
            wintypes.DWORD,
        ]
        kernel32.SetInformationJobObject.restype = wintypes.BOOL
        kernel32.OpenProcess.argtypes = [
            wintypes.DWORD,
            wintypes.BOOL,
            wintypes.DWORD,
        ]
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.AssignProcessToJobObject.argtypes = [
            wintypes.HANDLE,
            wintypes.HANDLE,
        ]
        kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
        kernel32.QueryInformationJobObject.argtypes = [
            wintypes.HANDLE,
            ctypes.c_int,
            wintypes.LPVOID,
            wintypes.DWORD,
            wintypes.LPVOID,
        ]
        kernel32.QueryInformationJobObject.restype = wintypes.BOOL
        kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
        kernel32.TerminateJobObject.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL
        self._psapi.GetProcessMemoryInfo.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(_ProcessMemoryCountersEx),
            wintypes.DWORD,
        ]
        self._psapi.GetProcessMemoryInfo.restype = wintypes.BOOL

    def resource_outcome(
        self,
        *,
        require_limit_reached: bool = False,
    ) -> DecoderOutcome | None:
        if self._closed or not self._job:
            return None
        self.metrics()
        extended = _ExtendedLimitInformation()
        accounting = _BasicAccountingInformation()
        got_extended = bool(self._kernel32.QueryInformationJobObject(
            self._job,
            self._EXTENDED_INFORMATION,
            ctypes.byref(extended),
            ctypes.sizeof(extended),
            None,
        ))
        got_accounting = bool(self._kernel32.QueryInformationJobObject(
            self._job,
            self._BASIC_ACCOUNTING,
            ctypes.byref(accounting),
            ctypes.sizeof(accounting),
            None,
        ))
        # The Job Object limit is enforced against the process memory charge,
        # while the separate reported metric below is actual peak working set
        # (resident memory).  Do not conflate the two quantities.
        if got_extended and int(extended.PeakProcessMemoryUsed) >= int(
            self._memory_bytes
        ):
            return "memory_limit_exceeded"
        if got_accounting:
            cpu_seconds = (
                int(accounting.TotalUserTime) + int(accounting.TotalKernelTime)
            ) / 10_000_000.0
            if cpu_seconds >= self._cpu_seconds:
                return "cpu_limit_exceeded"
        return None

    def metrics(self) -> tuple[float | None, int | None, int | None]:
        """Read CPU, working set and Job-limit memory charge evidence."""
        if self._closed or not self._job:
            return None, None, None
        extended = _ExtendedLimitInformation()
        if self._kernel32.QueryInformationJobObject(
            self._job,
            self._EXTENDED_INFORMATION,
            ctypes.byref(extended),
            ctypes.sizeof(extended),
            None,
        ):
            self._peak_boundary_bytes = max(
                self._peak_boundary_bytes, int(extended.PeakProcessMemoryUsed)
            )
        accounting = _BasicAccountingInformation()
        got_accounting = bool(self._kernel32.QueryInformationJobObject(
            self._job,
            self._BASIC_ACCOUNTING,
            ctypes.byref(accounting),
            ctypes.sizeof(accounting),
            None,
        ))
        if got_accounting:
            self._observed_cpu_seconds = max(
                self._observed_cpu_seconds,
                int(accounting.TotalUserTime) + int(accounting.TotalKernelTime)
            )
        memory = _ProcessMemoryCountersEx()
        memory.cb = ctypes.sizeof(memory)
        if self._process and self._psapi.GetProcessMemoryInfo(
            self._process,
            ctypes.byref(memory),
            ctypes.sizeof(memory),
        ):
            self._peak_rss_bytes = max(
                self._peak_rss_bytes, int(memory.PeakWorkingSetSize)
            )
        cpu_seconds = self._observed_cpu_seconds / 10_000_000.0
        return cpu_seconds, self._peak_rss_bytes, self._peak_boundary_bytes

    def terminate(self) -> None:
        if not self._closed and self._job:
            self._kernel32.TerminateJobObject(self._job, 0xE0000001)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._process:
            self._kernel32.CloseHandle(self._process)
            self._process = wintypes.HANDLE()
        if self._job:
            # KILL_ON_JOB_CLOSE is the final containment fallback.
            self._kernel32.CloseHandle(self._job)
            self._job = wintypes.HANDLE()


def _terminate_process(
    process: multiprocessing.Process,
    boundary: _WindowsJobBoundary | None,
) -> None:
    if not process.is_alive():
        process.join(timeout=0)
        return
    if boundary is not None:
        boundary.terminate()
    elif _platform_family() == "linux" and process.pid is not None:
        try:
            if os.getpgid(process.pid) == process.pid:
                os.killpg(process.pid, signal.SIGTERM)
            else:
                process.terminate()
        except (OSError, ProcessLookupError):
            process.terminate()
    else:
        process.terminate()
    process.join(timeout=_TERMINATE_GRACE_SECONDS)
    if process.is_alive():
        if _platform_family() == "linux" and process.pid is not None:
            try:
                if os.getpgid(process.pid) == process.pid:
                    os.killpg(process.pid, signal.SIGKILL)
                elif hasattr(process, "kill"):
                    process.kill()
            except (OSError, ProcessLookupError):
                if hasattr(process, "kill"):
                    process.kill()
        elif hasattr(process, "kill"):
            process.kill()
        process.join(timeout=_TERMINATE_GRACE_SECONDS)


def _result(
    outcome: DecoderOutcome,
    *,
    started: float,
    result: JsonValue | None = None,
    detail_code: str | None = None,
    child_exit_code: int | None = None,
    child_cpu_seconds: float | None = None,
    child_peak_rss_bytes: int | None = None,
    child_peak_boundary_bytes: int | None = None,
) -> DecoderProcessResult:
    return DecoderProcessResult(
        outcome=outcome,
        result=result,
        detail_code=detail_code,
        child_exit_code=child_exit_code,
        elapsed_seconds=max(0.0, time.monotonic() - started),
        child_cpu_seconds=child_cpu_seconds,
        child_peak_rss_bytes=child_peak_rss_bytes,
        child_peak_boundary_bytes=child_peak_boundary_bytes,
    )


def _resource_metrics(
    monitor: _WindowsJobBoundary | _LinuxProcessMonitor | None,
) -> tuple[float | None, int | None, int | None]:
    if monitor is None:
        return None, None, None
    try:
        return monitor.metrics()
    except (OSError, ValueError):
        return None, None, None


def _maximum_metric(
    first: float | int | None,
    second: float | int | None,
) -> float | int | None:
    values = tuple(value for value in (first, second) if value is not None)
    return max(values) if values else None


def _map_child_message(
    message: Mapping[str, Any],
    *,
    started: float,
    child_exit_code: int | None,
    child_cpu_seconds: float | None = None,
    child_peak_rss_bytes: int | None = None,
    child_peak_boundary_bytes: int | None = None,
) -> DecoderProcessResult:
    kind = message.get("kind")
    allowed_without_result: set[str] = {
        "boundary_setup_failed",
        "invalid_request",
        "cpu_limit_exceeded",
        "memory_limit_exceeded",
        "target_load_failed",
        "decoder_failed",
        "result_not_json",
        "result_too_large",
    }
    if kind == "succeeded" and set(message) == {"kind", "result"}:
        return _result(
            "succeeded",
            started=started,
            result=message["result"],
            child_exit_code=child_exit_code,
            child_cpu_seconds=child_cpu_seconds,
            child_peak_rss_bytes=child_peak_rss_bytes,
            child_peak_boundary_bytes=child_peak_boundary_bytes,
        )
    if isinstance(kind, str) and kind in allowed_without_result and set(
        message
    ) == {"kind"}:
        return _result(
            kind,  # type: ignore[arg-type]
            started=started,
            child_exit_code=child_exit_code,
            child_cpu_seconds=child_cpu_seconds,
            child_peak_rss_bytes=child_peak_rss_bytes,
            child_peak_boundary_bytes=child_peak_boundary_bytes,
        )
    return _result(
        "protocol_error",
        started=started,
        detail_code="invalid_child_envelope",
        child_exit_code=child_exit_code,
        child_cpu_seconds=child_cpu_seconds,
        child_peak_rss_bytes=child_peak_rss_bytes,
        child_peak_boundary_bytes=child_peak_boundary_bytes,
    )


class HardDecoderExecutor:
    """Execute one importable decoder function in one fresh bounded process."""

    def __init__(
        self,
        *,
        limits: DecoderResourceLimits | None = None,
        context: multiprocessing.context.BaseContext | None = None,
        environment_allowlist: frozenset[str] | None = None,
        process_slot_policy: ProcessSlotPolicy = "exclusive",
    ) -> None:
        self._limits = limits or DecoderResourceLimits()
        selected_context = context or multiprocessing.get_context("spawn")
        if selected_context.get_start_method() != "spawn":
            raise ValueError("hard decoder isolation requires spawn")
        if environment_allowlist is not None and (
            type(environment_allowlist) is not frozenset
            or any(
                type(key) is not str
                or _ENVIRONMENT_KEY_RE.fullmatch(key) is None
                for key in environment_allowlist
            )
        ):
            raise TypeError(
                "environment_allowlist must be a frozenset of canonical keys"
            )
        if process_slot_policy not in {"exclusive", "independent"}:
            raise ValueError("process_slot_policy is invalid")
        self._context = selected_context
        self._process_slot_policy = process_slot_policy
        self._environment_keys = (
            None
            if environment_allowlist is None
            else tuple(sorted(environment_allowlist))
        )

    def execute(
        self,
        *,
        target: str,
        request: JsonValue,
        effect_contract: DecoderEffectContract,
        cancel_event: threading.Event | None = None,
        environment_overrides: Mapping[str, str] | None = None,
    ) -> DecoderProcessResult:
        started = time.monotonic()
        if cancel_event is not None and not callable(
            getattr(cancel_event, "is_set", None)
        ):
            raise TypeError("cancel_event must provide is_set()")
        if cancel_event is not None and cancel_event.is_set():
            return _result(
                "cancelled",
                started=started,
                detail_code="cancelled_before_spawn",
            )
        if not isinstance(effect_contract, DecoderEffectContract) or not (
            effect_contract.admitted
        ):
            return _result(
                "unsafe_effect_contract",
                started=started,
                detail_code="hard_kill_safety_unproven",
            )
        if not _valid_target(target):
            return _result(
                "target_load_failed",
                started=started,
                detail_code="invalid_import_reference",
            )
        try:
            overrides = self._validate_environment_overrides(
                environment_overrides
            )
        except (TypeError, ValueError):
            # Never include a key or value in the technical result.  In
            # particular, provider credentials must not become IPC data.
            return _result(
                "invalid_request",
                started=started,
                detail_code="environment_override_invalid",
            )
        try:
            request_bytes = _canonical_json_bytes(
                request,
                max_encoded_bytes=self._limits.max_request_bytes,
            )
        except _JsonSizeLimitExceeded:
            return _result(
                "invalid_request",
                started=started,
                detail_code="request_too_large",
            )
        except (TypeError, ValueError, OverflowError):
            return _result(
                "invalid_request",
                started=started,
                detail_code="request_not_canonical_json",
            )
        if len(request_bytes) > self._limits.max_request_bytes:
            return _result(
                "invalid_request",
                started=started,
                detail_code="request_too_large",
            )
        support = hard_boundary_support()
        if not support.supported:
            return _result(
                "unsupported_platform",
                started=started,
                detail_code="hard_kernel_boundary_unavailable",
            )
        if self._process_slot_policy == "independent":
            # Exact-raw acquisition has its own per-workload admission,
            # durable capacity reservations and idempotent capture identity.
            # It is I/O-bound and must not contend for the singleton decoder
            # slot used by CPU-heavy decode/fusion processes.  Each child
            # still receives the same hard wall, CPU and memory boundary.
            try:
                return self._execute_locked(
                    target=target,
                    request_bytes=request_bytes,
                    started=started,
                    cancel_event=cancel_event,
                    environment_overrides=overrides,
                )
            except Exception:
                return _result(
                    "boundary_setup_failed",
                    started=started,
                    detail_code="unexpected_parent_setup_failure",
                )
        if not _GLOBAL_JOB_SLOT.acquire(blocking=False):
            return _result(
                "busy",
                started=started,
                detail_code="decoder_slot_occupied",
            )
        process_slot = _CrossProcessDecoderSlot()
        try:
            process_slot_state = process_slot.try_acquire()
            if process_slot_state == "busy":
                return _result(
                    "busy",
                    started=started,
                    detail_code="decoder_process_slot_occupied",
                )
            if process_slot_state != "acquired":
                return _result(
                    "boundary_setup_failed",
                    started=started,
                    detail_code="decoder_process_slot_unavailable",
                )
            try:
                return self._execute_locked(
                    target=target,
                    request_bytes=request_bytes,
                    started=started,
                    cancel_event=cancel_event,
                    environment_overrides=overrides,
                )
            except Exception:
                return _result(
                    "boundary_setup_failed",
                    started=started,
                    detail_code="unexpected_parent_setup_failure",
                )
        finally:
            process_slot.release()
            _GLOBAL_JOB_SLOT.release()

    def _execute_locked(
        self,
        *,
        target: str,
        request_bytes: bytes,
        started: float,
        cancel_event: threading.Event | None,
        environment_overrides: Mapping[str, str],
    ) -> DecoderProcessResult:
        parent, child = self._context.Pipe(duplex=True)
        limits_wire = {
            "memory_bytes": self._limits.memory_bytes,
            "cpu_seconds": self._limits.cpu_seconds,
            "max_result_bytes": self._limits.max_result_bytes,
        }
        try:
            process = self._context.Process(
                target=_decoder_child_main,
                args=(
                    child,
                    target,
                    request_bytes,
                    limits_wire,
                    self._environment_keys,
                ),
                name="storm-tracker-decoder-one-shot",
                daemon=False,
            )
        except BaseException:
            parent.close()
            child.close()
            raise
        boundary: _WindowsJobBoundary | None = None
        resource_monitor: (
            _WindowsJobBoundary | _LinuxProcessMonitor | None
        ) = None
        message: dict[str, Any] | None = None
        deadline = started + self._limits.wall_seconds
        try:
            try:
                self._start_process(
                    process,
                    environment_overrides=environment_overrides,
                )
            except BaseException:
                return _result(
                    "boundary_setup_failed",
                    started=started,
                    detail_code="spawn_failed",
                )
            finally:
                # ``execute`` supplied a validated private copy, never the
                # caller's mapping.  Drop bearer material immediately after
                # the OS inherited it; the long-running parent wait has no
                # reason to retain another token copy.
                if isinstance(environment_overrides, dict):
                    environment_overrides.clear()
                child.close()

            ready = self._wait_for_message(
                parent,
                process,
                deadline=deadline,
                max_bytes=256,
                cancel_event=cancel_event,
            )
            if ready == {"kind": "parent_cancelled"}:
                _terminate_process(process, boundary)
                return _result(
                    "cancelled",
                    started=started,
                    detail_code="parent_cancelled",
                    child_exit_code=process.exitcode,
                )
            if ready is None:
                if time.monotonic() >= deadline:
                    _terminate_process(process, boundary)
                    return _result(
                        "deadline_exceeded",
                        started=started,
                        detail_code="deadline_during_boot",
                        child_exit_code=process.exitcode,
                    )
                process.join(timeout=0.05)
                return _result(
                    "boundary_setup_failed",
                    started=started,
                    detail_code="child_failed_before_boundary",
                    child_exit_code=process.exitcode,
                )
            if ready != {"kind": "ready"}:
                _terminate_process(process, boundary)
                if ready == {"kind": "boundary_setup_failed"}:
                    return _result(
                        "boundary_setup_failed",
                        started=started,
                        detail_code="kernel_limit_setup_failed",
                        child_exit_code=process.exitcode,
                    )
                return _result(
                    "protocol_error",
                    started=started,
                    detail_code="invalid_ready_envelope",
                    child_exit_code=process.exitcode,
                )

            if _platform_family() == "windows":
                try:
                    if process.pid is None:
                        raise RuntimeError("spawned child has no pid")
                    boundary = _WindowsJobBoundary(process.pid, self._limits)
                    resource_monitor = boundary
                except BaseException:
                    _terminate_process(process, boundary)
                    return _result(
                        "boundary_setup_failed",
                        started=started,
                        detail_code="windows_job_assignment_failed",
                        child_exit_code=process.exitcode,
                    )
            elif _platform_family() == "linux":
                try:
                    if process.pid is None:
                        raise RuntimeError("spawned child has no pid")
                    resource_monitor = _LinuxProcessMonitor(
                        process.pid, self._limits
                    )
                except BaseException:
                    _terminate_process(process, boundary)
                    return _result(
                        "boundary_setup_failed",
                        started=started,
                        detail_code="linux_accounting_setup_failed",
                        child_exit_code=process.exitcode,
                    )
            try:
                _send_json(parent, {"kind": "start"})
            except (BrokenPipeError, EOFError, OSError):
                _terminate_process(process, boundary)
                return _result(
                    "child_crashed",
                    started=started,
                    detail_code="child_closed_before_start",
                    child_exit_code=process.exitcode,
                )

            ipc_limit = self._limits.max_result_bytes + _PROTOCOL_OVERHEAD_BYTES
            message = self._wait_for_message(
                parent,
                process,
                deadline=deadline,
                max_bytes=ipc_limit,
                resource_monitor=resource_monitor,
                cancel_event=cancel_event,
            )
            if message == {"kind": "parent_cancelled"}:
                _terminate_process(process, boundary)
                return _result(
                    "cancelled",
                    started=started,
                    detail_code="parent_cancelled",
                    child_exit_code=process.exitcode,
                )
            if message is not None:
                (
                    cpu_before_join,
                    rss_before_join,
                    boundary_before_join,
                ) = _resource_metrics(
                    resource_monitor
                )
                process.join(timeout=_TERMINATE_GRACE_SECONDS)
                (
                    cpu_after_join,
                    rss_after_join,
                    boundary_after_join,
                ) = _resource_metrics(
                    resource_monitor
                )
                child_cpu_seconds = _maximum_metric(
                    cpu_before_join, cpu_after_join
                )
                child_peak_rss_bytes = _maximum_metric(
                    rss_before_join, rss_after_join
                )
                child_peak_boundary_bytes = _maximum_metric(
                    boundary_before_join, boundary_after_join
                )
                if process.is_alive():
                    _terminate_process(process, boundary)
                    return _result(
                        "protocol_error",
                        started=started,
                        detail_code="child_survived_terminal_message",
                        child_exit_code=process.exitcode,
                        child_cpu_seconds=child_cpu_seconds,
                        child_peak_rss_bytes=child_peak_rss_bytes,
                        child_peak_boundary_bytes=child_peak_boundary_bytes,
                    )
                if message.get("kind") == "succeeded" and process.exitcode != 0:
                    resource_outcome = (
                        resource_monitor.resource_outcome()
                        if resource_monitor is not None
                        else None
                    )
                    return _result(
                        resource_outcome or "child_crashed",
                        started=started,
                        detail_code=(
                            "kernel_resource_limit"
                            if resource_outcome is not None
                            else "nonzero_exit_after_result"
                        ),
                        child_exit_code=process.exitcode,
                        child_cpu_seconds=child_cpu_seconds,
                        child_peak_rss_bytes=child_peak_rss_bytes,
                        child_peak_boundary_bytes=child_peak_boundary_bytes,
                    )
                return _map_child_message(
                    message,
                    started=started,
                    child_exit_code=process.exitcode,
                    child_cpu_seconds=child_cpu_seconds,
                    child_peak_rss_bytes=child_peak_rss_bytes,
                    child_peak_boundary_bytes=child_peak_boundary_bytes,
                )

            if time.monotonic() >= deadline:
                resource_outcome = (
                    resource_monitor.resource_outcome()
                    if resource_monitor is not None
                    else None
                )
                _terminate_process(process, boundary)
                return _result(
                    resource_outcome or "deadline_exceeded",
                    started=started,
                    detail_code=(
                        "kernel_resource_limit"
                        if resource_outcome is not None
                        else "hard_wall_deadline"
                    ),
                    child_exit_code=process.exitcode,
                )
            process.join(timeout=0.05)
            resource_outcome = (
                resource_monitor.resource_outcome()
                if resource_monitor is not None
                else None
            )
            if resource_outcome is None:
                resource_outcome = _classify_posix_exit(process.exitcode)
            return _result(
                resource_outcome or "child_crashed",
                started=started,
                detail_code=(
                    "kernel_resource_limit"
                    if resource_outcome is not None
                    else "abnormal_child_exit"
                ),
                child_exit_code=process.exitcode,
            )
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError, OSError):
            # A native child can close its pipe and process handle between the
            # final IPC poll and resource sampling.  Preserve that observed
            # non-zero exit as a crash/resource outcome instead of making the
            # classification depend on the parent-side sampling race.
            process.join(timeout=0.05)
            if not process.is_alive() and process.exitcode not in {None, 0}:
                try:
                    resource_outcome = (
                        resource_monitor.resource_outcome()
                        if resource_monitor is not None
                        else None
                    )
                except OSError:
                    resource_outcome = None
                if resource_outcome is None:
                    resource_outcome = _classify_posix_exit(process.exitcode)
                return _result(
                    resource_outcome or "child_crashed",
                    started=started,
                    detail_code=(
                        "kernel_resource_limit"
                        if resource_outcome is not None
                        else "abnormal_child_exit"
                    ),
                    child_exit_code=process.exitcode,
                )
            _terminate_process(process, boundary)
            return _result(
                "protocol_error",
                started=started,
                detail_code="invalid_or_oversized_ipc",
                child_exit_code=process.exitcode,
            )
        finally:
            if process.is_alive():
                _terminate_process(process, boundary)
            parent.close()
            if boundary is not None:
                boundary.close()
            if not process.is_alive():
                try:
                    process.close()
                except (OSError, ValueError):
                    pass

    def _validate_environment_overrides(
        self,
        value: Mapping[str, str] | None,
    ) -> dict[str, str]:
        """Validate ephemeral spawn material without retaining it on self."""

        if value is None:
            return {}
        if not isinstance(value, Mapping):
            raise TypeError("environment_overrides must be a mapping")
        if not value:
            return {}
        if self._environment_keys is None:
            raise ValueError("environment overrides require a closed allowlist")
        allowed = frozenset(self._environment_keys)
        result: dict[str, str] = {}
        for key, nested in value.items():
            if type(key) is not str or key not in allowed:
                raise ValueError("environment override key is not allowed")
            if (
                type(nested) is not str
                or not nested
                or len(nested.encode("ascii", errors="ignore"))
                > _MAX_ENVIRONMENT_OVERRIDE_BYTES
                or not nested.isascii()
                or any(
                    ord(character) <= 0x20 or ord(character) == 0x7F
                    for character in nested
                )
            ):
                raise ValueError("environment override value is invalid")
            result[key] = nested
        return result

    def _start_process(
        self,
        process: multiprocessing.Process,
        *,
        environment_overrides: Mapping[str, str] | None = None,
    ) -> None:
        overrides = self._validate_environment_overrides(
            environment_overrides
        )
        with _ENVIRONMENT_SPAWN_LOCK:
            # Every process start takes this short lock, including callers
            # without an allowlist: otherwise they could inherit another
            # thread's temporarily narrowed environment.
            if self._environment_keys is None:
                process.start()
                return
            # Narrow the process environment only for the synchronous OS
            # spawn, then restore the parent exactly. The child repeats this
            # scrub as defence in depth before importing its selected target.
            original = dict(os.environ)
            retained = {
                key: original[key]
                for key in self._environment_keys
                if key in original
            }
            retained.update(overrides)
            try:
                os.environ.clear()
                os.environ.update(retained)
                process.start()
            finally:
                os.environ.clear()
                os.environ.update(original)

    @staticmethod
    def _wait_for_message(
        connection: Connection,
        process: multiprocessing.Process,
        *,
        deadline: float,
        max_bytes: int,
        resource_monitor: (
            _WindowsJobBoundary | _LinuxProcessMonitor | None
        ) = None,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, Any] | None:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            if connection.poll(min(_POLL_SECONDS, remaining)):
                try:
                    return _receive_json(connection, max_bytes=max_bytes)
                except EOFError:
                    return None
            if cancel_event is not None and cancel_event.is_set():
                return {"kind": "parent_cancelled"}
            if resource_monitor is not None:
                resource_outcome = resource_monitor.resource_outcome(
                    require_limit_reached=True
                )
                if resource_outcome is not None:
                    if isinstance(resource_monitor, _WindowsJobBoundary):
                        resource_monitor.terminate()
                    else:
                        _terminate_process(process, None)
                    process.join(timeout=_TERMINATE_GRACE_SECONDS)
                    return {"kind": resource_outcome}
            if not process.is_alive():
                # A final short poll prevents losing a message already buffered
                # just before the child exit became observable.
                if connection.poll(0.05):
                    try:
                        return _receive_json(connection, max_bytes=max_bytes)
                    except EOFError:
                        return None
                return None


def _classify_posix_exit(exit_code: int | None) -> DecoderOutcome | None:
    if _platform_family() != "linux" or exit_code is None:
        return None
    if exit_code == -int(getattr(signal, "SIGXCPU", -9999)):
        return "cpu_limit_exceeded"
    return None


__all__ = [
    "DecoderEffectContract",
    "DecoderProcessResult",
    "DecoderResourceLimits",
    "HardBoundarySupport",
    "HardDecoderExecutor",
    "hard_boundary_support",
]
