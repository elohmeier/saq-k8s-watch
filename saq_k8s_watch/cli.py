from __future__ import annotations

import asyncio
import importlib
import logging
import os
from collections.abc import Iterable
from importlib.metadata import version
from urllib.parse import urlparse, urlunparse

from saq.job import Status
from saq.queue import Queue

from saq_k8s_watch import KubernetesSaqEventMonitor
from saq_k8s_watch.server import start_server

logger = logging.getLogger(__name__)


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


def _getenv_float(name: str, default: float) -> float:
    value = _getenv(name)
    if value is None:
        return default
    return float(value)


def _getenv_set(name: str, default: Iterable[str]) -> set[str]:
    value = _getenv(name)
    if value is None:
        return set(default)
    return {item.strip() for item in value.split(",") if item.strip()}


def _redact_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.username or parsed.password:
        netloc = f"***:***@{parsed.hostname}"
        if parsed.port:
            netloc += f":{parsed.port}"
        return urlunparse(parsed._replace(netloc=netloc))
    return url


async def main() -> None:
    queue_url = _getenv("SAQ_QUEUE_URL")
    if not queue_url:
        db_host = _getenv("DB_HOST", "localhost")
        db_port = _getenv("DB_PORT", "5432")
        db_user = _getenv("DB_USER")
        db_password = _getenv("DB_PASSWORD")
        db_name = _getenv("DB_NAME")
        if db_user and db_password:
            queue_url = (
                f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            )
        else:
            message = "SAQ_QUEUE_URL or DB_USER + DB_PASSWORD + DB_NAME is required"
            raise SystemExit(message)

    queue_names_raw = (
        _getenv("SAQ_QUEUE_NAMES") or _getenv("SAQ_QUEUE_NAME", "default") or "default"
    )
    queue_names = [n.strip() for n in queue_names_raw.split(",") if n.strip()]

    queue_class_path = _getenv("SAQ_QUEUE_CLASS")
    if queue_class_path:
        module_path, class_name = queue_class_path.rsplit(".", 1)
        mod = importlib.import_module(module_path)
        queue_cls = getattr(mod, class_name)
        logger.info("Using custom queue class: %s", queue_class_path)
    else:
        queue_cls = None

    from psycopg_pool import AsyncConnectionPool

    pool = AsyncConnectionPool(
        queue_url,
        check=AsyncConnectionPool.check_connection,
        open=False,
        kwargs={"autocommit": True},
    )
    await pool.open()

    queues: list[Queue] = []
    for name in queue_names:
        if queue_cls is not None:
            q = queue_cls(pool=pool, name=name)
        else:
            q = Queue(pool=pool, name=name)  # type: ignore[call-arg]
        queues.append(q)

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

    namespace = _getenv("SAQ_NAMESPACE")
    label_selector = _getenv("SAQ_LABEL_SELECTOR")
    retry_on_stop = _getenv_bool("SAQ_RETRY_ON_STOP", True)
    terminal_status = _getenv_status("SAQ_TERMINAL_STATUS", Status.FAILED)
    event_dedupe_ttl_s = _getenv_int("SAQ_EVENT_DEDUPE_TTL_S", 600)
    watch_timeout_s = _getenv_int("SAQ_WATCH_TIMEOUT_S", 300)

    orphan_sweep_interval_s = _getenv_float("SAQ_ORPHAN_SWEEP_INTERVAL_S", 60.0)
    orphan_sweep_min_age_s = _getenv_float("SAQ_ORPHAN_SWEEP_MIN_AGE_S", 300.0)

    logger.info(
        "queue=%s queues=%s namespace=%s label_selector=%s",
        _redact_url(queue_url),
        queue_names,
        namespace,
        label_selector,
    )
    logger.info(
        "retry_on_stop=%s terminal_status=%s stop_reasons=%s",
        retry_on_stop,
        terminal_status.name,
        sorted(stop_reasons),
    )
    logger.info(
        "orphan_sweep_interval_s=%s orphan_sweep_min_age_s=%s",
        orphan_sweep_interval_s,
        orphan_sweep_min_age_s,
    )

    monitor = KubernetesSaqEventMonitor(
        queues=queues,
        namespace=namespace,
        label_selector=label_selector,
        worker_id_label=_getenv("SAQ_WORKER_ID_LABEL", "saq.io/worker-id"),
        worker_id_annotation=_getenv("SAQ_WORKER_ID_ANNOTATION", "saq.io/worker-id"),
        use_pod_name_as_worker_id=_getenv_bool("SAQ_USE_POD_NAME_AS_WORKER_ID", True),
        retry_on_stop=retry_on_stop,
        terminal_status=terminal_status,
        event_dedupe_ttl_s=event_dedupe_ttl_s,
        watch_timeout_s=watch_timeout_s,
        stop_reasons=stop_reasons,
        stop_message_substrings=stop_message_substrings,
        orphan_sweep_interval_s=orphan_sweep_interval_s,
        orphan_sweep_min_age_s=orphan_sweep_min_age_s,
    )

    runner = await start_server()
    try:
        await monitor.run()
    finally:
        await runner.cleanup()
        await pool.close()


class _ExceptionReprFilter(logging.Filter):
    """Use repr() for exception args in log records.

    psycopg_pool logs connection errors as ``"error connecting in %r: %s"``
    where ``%s`` calls ``str(ex)``.  Some exceptions produce an empty string,
    yielding truncated lines like ``error connecting in 'pool-1': ``.
    ``repr()`` always includes the class name and constructor args.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if record.args:
            args = record.args
            if isinstance(args, dict):
                record.args = {
                    k: repr(v) if isinstance(v, BaseException) else v
                    for k, v in args.items()
                }
            elif isinstance(args, tuple):
                record.args = tuple(
                    repr(a) if isinstance(a, BaseException) else a for a in args
                )
        return True


def cli() -> None:
    log_level = _getenv("SAQ_LOG_LEVEL", "INFO") or "INFO"
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
        level=getattr(logging, log_level.upper(), logging.INFO),
    )

    logging.getLogger("psycopg.pool").addFilter(_ExceptionReprFilter())

    pkg_version = version("saq-k8s-watch")
    logger.info("saq-k8s-watch %s starting", pkg_version)

    asyncio.run(main())
