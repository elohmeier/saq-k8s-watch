from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
import time
import typing as t
from dataclasses import dataclass, field

if t.TYPE_CHECKING:
    k8s_client: t.Any = None
    k8s_config: t.Any = None
    k8s_watch: t.Any = None
    K8sApiClient: t.Any = None
    K8sCoreV1Api: t.Any = None
    K8sEvent: t.Any = None
    K8sPod: t.Any = None
    K8sApiException: t.Any = None
    K8sWatch: t.Any = None
else:
    k8s_module = importlib.import_module("kubernetes_asyncio")
    k8s_client = k8s_module.client
    k8s_config = k8s_module.config
    k8s_watch = k8s_module.watch
    K8sApiException = importlib.import_module("kubernetes_asyncio.client").ApiException
    K8sApiClient = k8s_client.ApiClient
    K8sCoreV1Api = k8s_client.CoreV1Api
    K8sEvent = (
        getattr(k8s_client, "V1Event", None)
        or getattr(k8s_client, "CoreV1Event", None)
        or getattr(k8s_client, "EventsV1Event", None)
    )
    K8sPod = k8s_client.V1Pod
    K8sWatch = k8s_watch.Watch
from saq.job import Job, Status
from saq.queue.base import Queue

try:
    from saq.queue.postgres import PostgresQueue
except Exception:  # pragma: no cover - optional dependency
    PostgresQueue = None  # type: ignore[assignment]

try:
    from saq.queue.redis import RedisQueue
except Exception:  # pragma: no cover - optional dependency
    RedisQueue = None  # type: ignore[assignment]

logger = logging.getLogger("saq_k8s_watch")


DEFAULT_STOP_REASONS = {
    "OOMKilled",
    "Evicted",
    "Failed",
    "Killing",
    "NodeLost",
    "Preempted",
    "Preempting",
    "Shutdown",
}

DEFAULT_STOP_MESSAGE_SUBSTRINGS = {
    "oomkilled",
    "oom killed",
    "evicted",
    "node lost",
    "preempt",
    "shutdown",
    "containercannotrun",
}


@dataclass(slots=True)
class EventDeduper:
    ttl_s: int = 600
    _seen: dict[str, float] = field(default_factory=dict)

    def seen(self, uid: str) -> bool:
        now = time.monotonic()
        cutoff = now - self.ttl_s
        if self._seen:
            stale = [key for key, ts in self._seen.items() if ts < cutoff]
            for key in stale:
                self._seen.pop(key, None)
        if uid in self._seen:
            return True
        self._seen[uid] = now
        return False


@dataclass(slots=True)
class KubernetesSaqEventMonitor:
    queue: Queue
    namespace: str | None = None
    label_selector: str | None = None
    worker_id_label: str | None = "saq.io/worker-id"
    worker_id_annotation: str | None = "saq.io/worker-id"
    use_pod_name_as_worker_id: bool = True
    stop_reasons: set[str] = field(default_factory=lambda: set(DEFAULT_STOP_REASONS))
    stop_message_substrings: set[str] = field(
        default_factory=lambda: set(DEFAULT_STOP_MESSAGE_SUBSTRINGS)
    )
    job_statuses: tuple[Status, ...] = (Status.ACTIVE, Status.ABORTING)
    retry_on_stop: bool = True
    terminal_status: Status = Status.FAILED
    event_dedupe_ttl_s: int = 600
    watch_timeout_s: int = 300
    backoff_base_s: float = 1.0
    backoff_max_s: float = 30.0
    field_selector: str = "involvedObject.kind=Pod"

    _api_client: K8sApiClient | None = field(default=None, init=False)
    _core_v1: K8sCoreV1Api | None = field(default=None, init=False)
    _deduper: EventDeduper = field(default_factory=EventDeduper, init=False)
    _resource_version: str | None = field(default=None, init=False)
    _watched_pods: set[str] = field(default_factory=set, init=False)
    _watched_pods_refreshed_at: float = field(default=0.0, init=False)

    async def run(self) -> None:
        logger.info("Connecting to queue")
        try:
            await self.queue.connect()
        except Exception:
            logger.exception("Failed to connect to queue")
            raise
        logger.info("Connected to queue")

        await self._ensure_client()

        backoff = self.backoff_base_s
        while True:
            try:
                await self._watch_events()
                backoff = self.backoff_base_s
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Kubernetes events watch failed; retrying")
                await asyncio.sleep(backoff)
                backoff = min(self.backoff_max_s, backoff * 2)

    async def close(self) -> None:
        if self._api_client is not None:
            await self._api_client.close()
            self._api_client = None
            self._core_v1 = None

    async def _ensure_client(self) -> None:
        if self._api_client is not None:
            return

        await self._load_kube_config()
        self._api_client = K8sApiClient()
        self._core_v1 = K8sCoreV1Api(self._api_client)
        self._deduper = EventDeduper(ttl_s=self.event_dedupe_ttl_s)

    async def _load_kube_config(self) -> None:
        try:
            await _maybe_await(k8s_config.load_incluster_config())
        except Exception:
            logger.debug("Falling back to kube config")
        else:
            return
        await _maybe_await(k8s_config.load_kube_config())

    async def _refresh_watched_pods(self) -> None:
        """List pods matching ``label_selector`` and cache their names.

        The label_selector cannot be passed to the Events API because Event
        objects do not carry pod labels.  Instead we periodically list the
        matching Pods and filter events by pod name.
        """
        if not self.label_selector or self._core_v1 is None:
            return
        try:
            if self.namespace:
                result = await self._core_v1.list_namespaced_pod(
                    self.namespace,
                    label_selector=self.label_selector,
                )
            else:
                result = await self._core_v1.list_pod_for_all_namespaces(
                    label_selector=self.label_selector,
                )
            self._watched_pods = {
                pod.metadata.name
                for pod in (result.items or [])
                if pod.metadata and pod.metadata.name
            }
            self._watched_pods_refreshed_at = time.monotonic()
            logger.debug("Refreshed watched pods: %s", self._watched_pods)
        except Exception:
            logger.exception("Failed to refresh watched pods")

    async def _is_watched_pod(self, pod_name: str) -> bool:
        """Check whether *pod_name* belongs to a watched pod.

        Returns ``True`` immediately when no ``label_selector`` is configured
        or when the pod is already in the cache.  On a cache miss, refreshes
        the cache at most once every 15 s so that newly-scaled pods are picked
        up without hammering the API on events from non-watched pods.
        """
        if not self.label_selector:
            return True
        if pod_name in self._watched_pods:
            return True
        now = time.monotonic()
        if now - self._watched_pods_refreshed_at < 15:
            return False
        await self._refresh_watched_pods()
        return pod_name in self._watched_pods

    async def _watch_events(self) -> None:
        assert self._core_v1 is not None
        watcher = K8sWatch()

        await self._refresh_watched_pods()

        # NOTE: label_selector is intentionally NOT passed to the Events API.
        # Kubernetes Event objects do not carry the labels of the involved Pod,
        # so filtering by label_selector here would silently drop all events.
        # Pod filtering is handled in _handle_event via _is_watched_pod.
        stream_kwargs: dict[str, t.Any] = {
            "timeout_seconds": self.watch_timeout_s,
            "field_selector": self.field_selector,
            "resource_version": self._resource_version,
        }

        if self.namespace:
            list_events = self._core_v1.list_namespaced_event
            stream_kwargs["namespace"] = self.namespace
        else:
            list_events = self._core_v1.list_event_for_all_namespaces

        async for payload in watcher.stream(list_events, **stream_kwargs):
            event = payload.get("object")
            if event is None:
                continue
            metadata = event.metadata
            if metadata and metadata.resource_version:
                self._resource_version = metadata.resource_version
            await self._handle_event(event)

    async def _handle_event(self, event: K8sEvent) -> None:
        if not self._is_stop_event(event):
            return

        uid = self._event_uid(event)
        if uid and self._deduper.seen(uid):
            return

        pod_name, pod_namespace = self._pod_ref(event)
        if not pod_name:
            return

        if not await self._is_watched_pod(pod_name):
            return

        pod = await self._fetch_pod(pod_name, pod_namespace)
        worker_id = self._resolve_worker_id(pod, pod_name)
        if not worker_id:
            logger.debug("Skipping pod %s; no worker id", pod_name)
            return

        reason = (event.reason or "PodStopped").strip() or "PodStopped"
        message = (event.message or "").strip()
        await self._handle_worker_stop(
            worker_id=worker_id,
            pod_name=pod_name,
            pod_namespace=pod_namespace,
            reason=reason,
            message=message,
        )

    def _is_stop_event(self, event: K8sEvent) -> bool:
        involved = event.involved_object
        if not involved or involved.kind != "Pod":
            return False

        reason = (event.reason or "").strip()
        if reason in self.stop_reasons:
            return True

        if event.type != "Warning":
            return False

        message = (event.message or "").lower()
        return any(fragment in message for fragment in self.stop_message_substrings)

    def _event_uid(self, event: K8sEvent) -> str | None:
        if event.metadata and event.metadata.uid:
            return event.metadata.uid
        if event.involved_object and event.involved_object.uid:
            return event.involved_object.uid
        return None

    def _pod_ref(self, event: K8sEvent) -> tuple[str | None, str | None]:
        involved = event.involved_object
        if not involved:
            return None, None
        return involved.name, involved.namespace or self.namespace

    async def _fetch_pod(
        self, pod_name: str, pod_namespace: str | None
    ) -> K8sPod | None:
        if self._core_v1 is None or not pod_namespace:
            return None
        try:
            return await self._core_v1.read_namespaced_pod(pod_name, pod_namespace)
        except K8sApiException as exc:
            if exc.status != 404:
                logger.warning(
                    "Failed to load pod metadata for %s/%s: %s",
                    pod_namespace,
                    pod_name,
                    exc,
                )
        except Exception:
            logger.exception(
                "Failed to load pod metadata for %s/%s", pod_namespace, pod_name
            )
        return None

    def _resolve_worker_id(self, pod: K8sPod | None, pod_name: str) -> str | None:
        if (
            pod
            and pod.metadata
            and self.worker_id_annotation
            and pod.metadata.annotations
            and (worker_id := pod.metadata.annotations.get(self.worker_id_annotation))
        ):
            return worker_id

        if (
            pod
            and pod.metadata
            and self.worker_id_label
            and pod.metadata.labels
            and (worker_id := pod.metadata.labels.get(self.worker_id_label))
        ):
            return worker_id

        if self.use_pod_name_as_worker_id:
            return pod_name
        return None

    async def _handle_worker_stop(
        self,
        *,
        worker_id: str,
        pod_name: str,
        pod_namespace: str | None,
        reason: str,
        message: str,
    ) -> None:
        error = (
            f"worker pod stopped: worker_id={worker_id} pod={pod_name}"
            f" namespace={pod_namespace or 'unknown'} reason={reason} message={message}"
        ).strip()

        jobs = await self._jobs_for_worker(worker_id)
        for job in jobs:
            await self._update_job_for_stop(job, error)

        await self._delete_worker_info(worker_id)
        logger.info(
            "Handled worker stop for %s (%s/%s) with %d job(s)",
            worker_id,
            pod_namespace,
            pod_name,
            len(jobs),
        )

    async def _jobs_for_worker(self, worker_id: str) -> list[Job]:
        jobs: list[Job] = []
        async for job in self.queue.iter_jobs(list(self.job_statuses)):
            if job.worker_id == worker_id:
                jobs.append(job)
        return jobs

    async def _update_job_for_stop(self, job: Job, error: str) -> None:
        if self.retry_on_stop and job.retryable:
            await job.retry(error)
        else:
            await job.finish(self.terminal_status, error=error)

    async def _delete_worker_info(self, worker_id: str) -> None:
        if RedisQueue is not None and isinstance(self.queue, RedisQueue):
            key = self.queue.namespace(f"worker_info:{worker_id}")
            async with self.queue.redis.pipeline(transaction=True) as pipe:
                await pipe.delete(key).zrem(self.queue._stats, key).execute()
            return

        if PostgresQueue is not None and isinstance(self.queue, PostgresQueue):
            await _delete_postgres_worker_info(self.queue, worker_id)


async def _delete_postgres_worker_info(queue: Queue, worker_id: str) -> None:
    try:
        from psycopg.sql import SQL
    except Exception:
        logger.debug("psycopg not available; skipping worker cleanup")
        return

    assert isinstance(queue, PostgresQueue)
    async with queue.pool.connection() as conn:
        await conn.execute(
            SQL("DELETE FROM {stats_table} WHERE worker_id = %(worker_id)s").format(
                stats_table=queue.stats_table
            ),
            {"worker_id": worker_id},
        )


async def _maybe_await(result: t.Any) -> None:
    if inspect.isawaitable(result):
        await result
