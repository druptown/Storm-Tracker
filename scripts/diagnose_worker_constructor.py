"""Construct the configured worker once and print only fixed safe outcomes."""
from __future__ import annotations

import asyncio
import hashlib
import os
import re

from storm_tracker_worker import service as worker_service


_SAFE_TOKEN = re.compile(r"^[a-z][a-z0-9_]{0,95}$")


def _fixed_reason(error: BaseException) -> str:
    reason = str(error)
    return reason if _SAFE_TOKEN.fullmatch(reason) else "unclassified"


def _reason_sha256(error: BaseException) -> str:
    return hashlib.sha256(str(error).encode("utf-8")).hexdigest()


def main() -> None:
    plan, runtime_kwargs = worker_service._main_runtime_kwargs()
    print(
        "activation_mode="
        + str(getattr(plan, "runtime_mode", "unknown"))
    )
    runtime_factory = runtime_kwargs.get("worker_first_runtime_factory")
    try:
        runtime = runtime_factory(
            publisher=lambda _payload: None,
            status_supplier=lambda: {
                "state": "online",
                "queue_depth": 0,
                "failed_jobs": 0,
            },
        )
    except BaseException as error:
        current: BaseException | None = error
        for depth in range(6):
            if current is None:
                break
            print(f"error_{depth}_class=" + type(current).__name__)
            print(f"error_{depth}_reason=" + _fixed_reason(current))
            print(f"error_{depth}_sha256=" + _reason_sha256(current))
            current = current.__cause__ or current.__context__
        return
    else:
        print("factory=ready")
        try:
            asyncio.run(runtime.async_stop())
        except BaseException as error:
            print("factory_cleanup_class=" + type(error).__name__)
        return

    try:
        worker = worker_service.WorkerService(
            host=os.environ["STV3_MQTT_HOST"],
            port=int(os.getenv("STV3_MQTT_PORT", "1883")),
            username=os.getenv("STV3_MQTT_USERNAME"),
            password=os.getenv("STV3_MQTT_PASSWORD"),
            tls=os.getenv("STV3_MQTT_TLS", "false").lower() == "true",
            topic_root=os.getenv(
                "STV3_MQTT_TOPIC_ROOT",
                worker_service.DEFAULT_TOPIC_ROOT,
            ),
            queue_size=int(os.getenv("STV3_QUEUE_SIZE", "16")),
            heartbeat_seconds=float(
                os.getenv("STV3_HEARTBEAT_SECONDS", "10")
            ),
            receipt_path=(
                None
                if getattr(
                    plan,
                    "isolated_runtime_selected",
                    plan.worker_first_selected,
                )
                else os.getenv(
                    "STV3_JOB_RECEIPT_DB",
                    "/data/storm_tracker_worker_receipts.sqlite3",
                )
            ),
            **runtime_kwargs,
        )
    except worker_service._WorkerFirstRuntimeFatalError as error:
        print("fatal_code=" + str(error))
        return
    except BaseException as error:
        print("unexpected_class=" + type(error).__name__)
        return

    print("constructor=ready")
    try:
        worker.client.disconnect()
    except BaseException:
        pass
    print(
        "cleanup_terminal="
        + str(worker._stop_worker_first_runtime_before_run()).lower()
    )


if __name__ == "__main__":
    main()
