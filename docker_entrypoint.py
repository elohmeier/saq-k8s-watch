from __future__ import annotations

import asyncio
import os
from typing import Iterable

from saq.job import Status
from saq.queue import Queue

from saq_k8s_watch import KubernetesSaqEventMonitor


def _getenv(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    if value is None:
        return None
    value = value.strip()
    return value if value else None


def _getenv_bool(name: str, default: bool) -> bool:
    value = _getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _getenv_int(name: str, default: int) -> int:
    value = _getenv(name)
    if value is None:
        return default
    return int(value)


def _getenv_status(name: str, default: Status) -> Status:
    value = _getenv(name)
    if value is None:
        return default
    return Status[value.upper()]


def _getenv_set(name: str, default: Iterable[str]) -> set[str]:
    value = _getenv(name)
    if value is None:
        return set(default)
    return {item.strip() for item in value.split(",") if item.strip()}


async def main() -> None:
    queue_url = _getenv("SAQ_QUEUE_URL")
    if not queue_url:
        raise SystemExit("SAQ_QUEUE_URL is required")

    queue_name = _getenv("SAQ_QUEUE_NAME", "default") or "default"
    queue = Queue.from_url(queue_url, name=queue_name)

    stop_reasons = _getenv_set(
        "SAQ_STOP_REASONS",
        [
            "OOMKilled",
            "Evicted",
            "Failed",
            "Killing",
            "NodeLost",
            "Preempted",
            "Preempting",
            "Shutdown",
        ],
    )
    stop_message_substrings = _getenv_set(
        "SAQ_STOP_MESSAGE_SUBSTRINGS",
        [
            "oomkilled",
            "oom killed",
            "evicted",
            "node lost",
            "preempt",
            "shutdown",
            "containercannotrun",
        ],
    )

    monitor = KubernetesSaqEventMonitor(
        queue=queue,
        namespace=_getenv("SAQ_NAMESPACE"),
        label_selector=_getenv("SAQ_LABEL_SELECTOR"),
        worker_id_label=_getenv("SAQ_WORKER_ID_LABEL", "saq.io/worker-id"),
        worker_id_annotation=_getenv("SAQ_WORKER_ID_ANNOTATION", "saq.io/worker-id"),
        use_pod_name_as_worker_id=_getenv_bool("SAQ_USE_POD_NAME_AS_WORKER_ID", True),
        retry_on_stop=_getenv_bool("SAQ_RETRY_ON_STOP", True),
        terminal_status=_getenv_status("SAQ_TERMINAL_STATUS", Status.FAILED),
        event_dedupe_ttl_s=_getenv_int("SAQ_EVENT_DEDUPE_TTL_S", 600),
        watch_timeout_s=_getenv_int("SAQ_WATCH_TIMEOUT_S", 300),
        stop_reasons=stop_reasons,
        stop_message_substrings=stop_message_substrings,
    )

    await monitor.run()


if __name__ == "__main__":
    asyncio.run(main())
