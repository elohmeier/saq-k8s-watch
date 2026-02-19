"""Tests for the orphan sweep feature."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

import pytest
from saq.job import Status
from saq_k8s_watch.monitor import KubernetesSaqEventMonitor


@dataclass(slots=True)
class FakeJob:
    worker_id: str | None = None
    started: float = 0.0
    retryable: bool = True
    retry_called: bool = False
    finish_called: bool = False
    finish_status: Status | None = None
    error: str | None = None

    async def retry(self, error: str) -> None:
        self.retry_called = True
        self.error = error

    async def finish(self, status: Status, *, error: str) -> None:
        self.finish_called = True
        self.finish_status = status
        self.error = error


@dataclass(slots=True)
class FakeQueue:
    name: str = "default"
    jobs: list[FakeJob] = field(default_factory=list)

    async def connect(self) -> None:
        return None

    async def iter_jobs(self, statuses: list[Status]):
        for job in self.jobs:
            yield job


def _old_started_ms() -> float:
    """Return a started timestamp 10 minutes ago (in milliseconds)."""
    return (time.time() - 600) * 1000


def _recent_started_ms() -> float:
    """Return a started timestamp 30 seconds ago (in milliseconds)."""
    return (time.time() - 30) * 1000


@pytest.mark.asyncio
async def test_orphan_sweep_recovers_orphaned_jobs() -> None:
    """Jobs whose worker_id is not in watched pods should be retried."""
    job = FakeJob(worker_id="dead-worker-1", started=_old_started_ms())
    queue = FakeQueue(jobs=[job])
    monitor = KubernetesSaqEventMonitor(
        queues=[queue],
        namespace="default",
        label_selector="app in (saq-extraction)",
        orphan_sweep_min_age_s=300.0,
    )
    # Simulate watched pods that don't include the dead worker
    monitor._watched_pods = {"alive-worker-1", "alive-worker-2"}
    monitor._watched_pods_refreshed_at = time.monotonic()

    await monitor._orphan_sweep()

    assert job.retry_called is True
    assert job.error is not None
    assert "orphan sweep" in job.error
    assert "dead-worker-1" in job.error


@pytest.mark.asyncio
async def test_orphan_sweep_respects_grace_period() -> None:
    """Jobs with recent started timestamps should not be recovered."""
    job = FakeJob(worker_id="new-worker-1", started=_recent_started_ms())
    queue = FakeQueue(jobs=[job])
    monitor = KubernetesSaqEventMonitor(
        queues=[queue],
        namespace="default",
        label_selector="app in (saq-extraction)",
        orphan_sweep_min_age_s=300.0,
    )
    monitor._watched_pods = {"alive-worker-1"}
    monitor._watched_pods_refreshed_at = time.monotonic()

    await monitor._orphan_sweep()

    assert job.retry_called is False
    assert job.finish_called is False


@pytest.mark.asyncio
async def test_orphan_sweep_skips_jobs_with_running_workers() -> None:
    """Jobs whose worker_id matches a running pod should not be touched."""
    job = FakeJob(worker_id="alive-worker-1", started=_old_started_ms())
    queue = FakeQueue(jobs=[job])
    monitor = KubernetesSaqEventMonitor(
        queues=[queue],
        namespace="default",
        label_selector="app in (saq-extraction)",
        orphan_sweep_min_age_s=300.0,
    )
    monitor._watched_pods = {"alive-worker-1"}
    monitor._watched_pods_refreshed_at = time.monotonic()

    await monitor._orphan_sweep()

    assert job.retry_called is False
    assert job.finish_called is False


@pytest.mark.asyncio
async def test_orphan_sweep_spans_multiple_queues() -> None:
    """Orphan sweep should check jobs across all queues."""
    job1 = FakeJob(worker_id="dead-worker-1", started=_old_started_ms())
    job2 = FakeJob(worker_id="dead-worker-2", started=_old_started_ms())
    job3 = FakeJob(worker_id="alive-worker-1", started=_old_started_ms())

    queue1 = FakeQueue(name="extraction", jobs=[job1])
    queue2 = FakeQueue(name="orchestrator", jobs=[job2, job3])

    monitor = KubernetesSaqEventMonitor(
        queues=[queue1, queue2],
        namespace="default",
        label_selector="app in (saq-extraction)",
        orphan_sweep_min_age_s=300.0,
    )
    monitor._watched_pods = {"alive-worker-1"}
    monitor._watched_pods_refreshed_at = time.monotonic()

    await monitor._orphan_sweep()

    assert job1.retry_called is True
    assert "extraction" in (job1.error or "")

    assert job2.retry_called is True
    assert "orchestrator" in (job2.error or "")

    # Job3's worker is alive — should not be touched
    assert job3.retry_called is False
    assert job3.finish_called is False


@pytest.mark.asyncio
async def test_orphan_sweep_uses_finish_when_not_retryable() -> None:
    """Non-retryable orphaned jobs should be finished with terminal status."""
    job = FakeJob(
        worker_id="dead-worker-1",
        started=_old_started_ms(),
        retryable=False,
    )
    queue = FakeQueue(jobs=[job])
    monitor = KubernetesSaqEventMonitor(
        queues=[queue],
        namespace="default",
        label_selector="app in (saq-extraction)",
        orphan_sweep_min_age_s=300.0,
        retry_on_stop=True,
    )
    monitor._watched_pods = {"alive-worker-1"}
    monitor._watched_pods_refreshed_at = time.monotonic()

    await monitor._orphan_sweep()

    assert job.retry_called is False
    assert job.finish_called is True
    assert job.finish_status == Status.FAILED


@pytest.mark.asyncio
async def test_orphan_sweep_recovers_jobs_without_worker_id() -> None:
    """Jobs with no worker_id past the grace period should be recovered."""
    job = FakeJob(worker_id=None, started=_old_started_ms())
    queue = FakeQueue(jobs=[job])
    monitor = KubernetesSaqEventMonitor(
        queues=[queue],
        namespace="default",
        label_selector="app in (saq-extraction)",
        orphan_sweep_min_age_s=300.0,
    )
    monitor._watched_pods = {"alive-worker-1"}
    monitor._watched_pods_refreshed_at = time.monotonic()

    await monitor._orphan_sweep()

    assert job.retry_called is True
    assert job.error is not None
    assert "no worker_id" in job.error


@pytest.mark.asyncio
async def test_orphan_sweep_respects_grace_period_for_missing_worker_id() -> None:
    """Jobs with no worker_id within the grace period should not be touched."""
    job = FakeJob(worker_id=None, started=_recent_started_ms())
    queue = FakeQueue(jobs=[job])
    monitor = KubernetesSaqEventMonitor(
        queues=[queue],
        namespace="default",
        label_selector="app in (saq-extraction)",
        orphan_sweep_min_age_s=300.0,
    )
    monitor._watched_pods = {"alive-worker-1"}
    monitor._watched_pods_refreshed_at = time.monotonic()

    await monitor._orphan_sweep()

    assert job.retry_called is False
    assert job.finish_called is False


@pytest.mark.asyncio
async def test_orphan_sweep_finishes_non_retryable_job_without_worker_id() -> None:
    """Non-retryable jobs with no worker_id should be finished, not retried."""
    job = FakeJob(worker_id=None, started=_old_started_ms(), retryable=False)
    queue = FakeQueue(jobs=[job])
    monitor = KubernetesSaqEventMonitor(
        queues=[queue],
        namespace="default",
        label_selector="app in (saq-extraction)",
        orphan_sweep_min_age_s=300.0,
        retry_on_stop=True,
    )
    monitor._watched_pods = {"alive-worker-1"}
    monitor._watched_pods_refreshed_at = time.monotonic()

    await monitor._orphan_sweep()

    assert job.retry_called is False
    assert job.finish_called is True
    assert job.finish_status == Status.FAILED
    assert "no worker_id" in (job.error or "")
