"""Unit tests for label_selector pod filtering.

These tests verify that label_selector is applied by listing Pods (not by
filtering Events) and that events for non-matching pods are skipped.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from saq.job import Status
from saq_k8s_watch.monitor import EventDeduper, KubernetesSaqEventMonitor

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FakeJob:
    worker_id: str
    retryable: bool = True
    retry_called: bool = False
    error: str | None = None

    async def retry(self, error: str) -> None:
        self.retry_called = True
        self.error = error

    async def finish(self, status: Status, *, error: str) -> None:
        pass


@dataclass(slots=True)
class FakeQueue:
    jobs: list[FakeJob] = field(default_factory=list)

    async def connect(self) -> None:
        return None

    async def iter_jobs(self, statuses: list[Status]):
        for job in self.jobs:
            yield job


def _make_event(
    *,
    pod_name: str,
    namespace: str = "default",
    reason: str = "OOMKilled",
    uid: str = "evt-1",
) -> MagicMock:
    event = MagicMock()
    event.metadata = MagicMock()
    event.metadata.uid = uid
    event.metadata.resource_version = None
    event.involved_object = MagicMock()
    event.involved_object.kind = "Pod"
    event.involved_object.name = pod_name
    event.involved_object.namespace = namespace
    event.involved_object.uid = "pod-uid-1"
    event.reason = reason
    event.message = "container was OOMKilled"
    event.type = "Warning"
    return event


def _make_pod(name: str, labels: dict[str, str] | None = None) -> MagicMock:
    pod = MagicMock()
    pod.metadata = MagicMock()
    pod.metadata.name = name
    pod.metadata.labels = labels or {}
    pod.metadata.annotations = {}
    return pod


def _make_pod_list(pods: list[MagicMock]) -> MagicMock:
    result = MagicMock()
    result.items = pods
    return result


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

_MONITOR_CLS = "saq_k8s_watch.monitor.KubernetesSaqEventMonitor"


@pytest.mark.asyncio
async def test_event_processed_when_pod_matches_label_selector() -> None:
    """Event for a pod matching the label_selector should be handled."""
    job = FakeJob(worker_id="saq-extraction-abc")
    queue = FakeQueue(jobs=[job])
    monitor = KubernetesSaqEventMonitor(
        queue=queue,
        namespace="default",
        label_selector="app in (saq-extraction)",
        use_pod_name_as_worker_id=True,
    )
    monitor._deduper = EventDeduper(ttl_s=60)
    monitor._watched_pods = {"saq-extraction-abc"}

    pod = _make_pod("saq-extraction-abc", labels={"app": "saq-extraction"})
    event = _make_event(pod_name="saq-extraction-abc", namespace="default")

    with patch(f"{_MONITOR_CLS}._fetch_pod", new_callable=AsyncMock, return_value=pod):
        await monitor._handle_event(event)

    assert job.retry_called is True
    assert job.error is not None
    assert "OOMKilled" in job.error


@pytest.mark.asyncio
async def test_event_skipped_when_pod_not_in_label_selector() -> None:
    """Event for a pod NOT matching the label_selector should be skipped."""
    job = FakeJob(worker_id="postgres-1")
    queue = FakeQueue(jobs=[job])
    monitor = KubernetesSaqEventMonitor(
        queue=queue,
        namespace="default",
        label_selector="app in (saq-extraction)",
        use_pod_name_as_worker_id=True,
    )
    monitor._deduper = EventDeduper(ttl_s=60)
    monitor._watched_pods = {"saq-extraction-abc"}
    monitor._watched_pods_refreshed_at = 1e12  # far future, skip refresh

    # Targeted check returns empty → pod doesn't match selector.
    core_v1 = AsyncMock()
    core_v1.list_namespaced_pod = AsyncMock(return_value=_make_pod_list([]))
    monitor._core_v1 = core_v1

    event = _make_event(pod_name="postgres-1", namespace="default")

    fetch_mock = AsyncMock(return_value=None)
    with patch(f"{_MONITOR_CLS}._fetch_pod", fetch_mock):
        await monitor._handle_event(event)

    assert job.retry_called is False
    # _fetch_pod should not even be called for a non-watched pod
    fetch_mock.assert_not_called()
    # Targeted check was called and the pod is now in the negative cache.
    assert "postgres-1" in monitor._unwatched_pods


@pytest.mark.asyncio
async def test_event_processed_when_no_label_selector() -> None:
    """Without label_selector all pods should be watched."""
    job = FakeJob(worker_id="any-pod")
    queue = FakeQueue(jobs=[job])
    monitor = KubernetesSaqEventMonitor(
        queue=queue,
        namespace="default",
        label_selector=None,
        use_pod_name_as_worker_id=True,
    )
    monitor._deduper = EventDeduper(ttl_s=60)

    pod = _make_pod("any-pod")
    event = _make_event(pod_name="any-pod", namespace="default")

    with patch(f"{_MONITOR_CLS}._fetch_pod", new_callable=AsyncMock, return_value=pod):
        await monitor._handle_event(event)

    assert job.retry_called is True


@pytest.mark.asyncio
async def test_cache_miss_triggers_refresh() -> None:
    """A cache miss for a new pod should refresh the watched pods set."""
    job = FakeJob(worker_id="saq-extraction-new")
    queue = FakeQueue(jobs=[job])
    monitor = KubernetesSaqEventMonitor(
        queue=queue,
        namespace="default",
        label_selector="app in (saq-extraction)",
        use_pod_name_as_worker_id=True,
    )
    monitor._deduper = EventDeduper(ttl_s=60)
    monitor._watched_pods = set()  # empty cache
    monitor._watched_pods_refreshed_at = 0.0  # stale

    new_pod = _make_pod("saq-extraction-new", labels={"app": "saq-extraction"})
    core_v1 = AsyncMock()
    core_v1.list_namespaced_pod = AsyncMock(return_value=_make_pod_list([new_pod]))
    core_v1.read_namespaced_pod = AsyncMock(return_value=new_pod)
    monitor._core_v1 = core_v1

    event = _make_event(pod_name="saq-extraction-new", namespace="default")

    with patch(
        f"{_MONITOR_CLS}._fetch_pod",
        new_callable=AsyncMock,
        return_value=new_pod,
    ):
        await monitor._handle_event(event)

    core_v1.list_namespaced_pod.assert_called_once_with(
        "default",
        label_selector="app in (saq-extraction)",
    )
    assert "saq-extraction-new" in monitor._watched_pods
    assert job.retry_called is True


@pytest.mark.asyncio
async def test_refresh_rate_limited() -> None:
    """Full refresh is rate-limited; targeted checks fill the gap."""
    queue = FakeQueue(jobs=[])
    monitor = KubernetesSaqEventMonitor(
        queue=queue,
        namespace="default",
        label_selector="app in (saq-extraction)",
        use_pod_name_as_worker_id=True,
    )
    monitor._deduper = EventDeduper(ttl_s=60)
    monitor._watched_pods = set()

    core_v1 = AsyncMock()
    core_v1.list_namespaced_pod = AsyncMock(return_value=_make_pod_list([]))
    monitor._core_v1 = core_v1

    # First miss triggers a full refresh (no namespace → no targeted check).
    result1 = await monitor._is_watched_pod("unknown-pod")
    assert result1 is False
    assert core_v1.list_namespaced_pod.call_count == 1

    # Second miss within 15s does a targeted check when namespace is given.
    result2 = await monitor._is_watched_pod("another-unknown-pod", "default")
    assert result2 is False
    # One full refresh + one targeted check = 2 list calls total.
    assert core_v1.list_namespaced_pod.call_count == 2
    assert "another-unknown-pod" in monitor._unwatched_pods

    # Third miss for the same pod hits the negative cache — no extra API call.
    result3 = await monitor._is_watched_pod("another-unknown-pod", "default")
    assert result3 is False
    assert core_v1.list_namespaced_pod.call_count == 2


@pytest.mark.asyncio
async def test_targeted_check_discovers_new_pod() -> None:
    """A pod created after the last full refresh is found via targeted check."""
    job = FakeJob(worker_id="saq-extraction-new")
    queue = FakeQueue(jobs=[job])
    monitor = KubernetesSaqEventMonitor(
        queue=queue,
        namespace="default",
        label_selector="app in (saq-extraction)",
        use_pod_name_as_worker_id=True,
    )
    monitor._deduper = EventDeduper(ttl_s=60)
    monitor._watched_pods = {"saq-extraction-old"}
    monitor._watched_pods_refreshed_at = 1e12  # far future, skip full refresh

    new_pod = _make_pod("saq-extraction-new", labels={"app": "saq-extraction"})
    core_v1 = AsyncMock()
    # Targeted check returns the new pod.
    core_v1.list_namespaced_pod = AsyncMock(return_value=_make_pod_list([new_pod]))
    core_v1.read_namespaced_pod = AsyncMock(return_value=new_pod)
    monitor._core_v1 = core_v1

    event = _make_event(
        pod_name="saq-extraction-new", namespace="default", uid="evt-new"
    )

    with patch(
        f"{_MONITOR_CLS}._fetch_pod",
        new_callable=AsyncMock,
        return_value=new_pod,
    ):
        await monitor._handle_event(event)

    assert "saq-extraction-new" in monitor._watched_pods
    assert job.retry_called is True
    # Only the targeted check was called, no full refresh.
    core_v1.list_namespaced_pod.assert_called_once_with(
        "default",
        label_selector="app in (saq-extraction)",
        field_selector="metadata.name=saq-extraction-new",
    )
