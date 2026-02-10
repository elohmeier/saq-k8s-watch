from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass

import pytest
from kubernetes_asyncio import client
from saq.job import Status
from saq_k8s_watch.monitor import EventDeduper, KubernetesSaqEventMonitor


@dataclass(slots=True)
class FakeJob:
    worker_id: str
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
    jobs: list[FakeJob]

    async def connect(self) -> None:
        return None

    async def iter_jobs(self, statuses: list[Status]):
        for job in self.jobs:
            yield job


@pytest.mark.asyncio
async def test_monitor_handles_stop_event(
    kube_client: client.ApiClient, kube_namespace: str
) -> None:
    core_v1 = client.CoreV1Api(kube_client)
    pod_name = "saq-k8s-watch-test-pod"
    worker_id = "worker-1"

    pod_manifest = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=pod_name,
            namespace=kube_namespace,
            labels={"saq.io/worker-id": worker_id},
        ),
        spec=client.V1PodSpec(
            containers=[
                client.V1Container(
                    name="pause",
                    image="registry.k8s.io/pause:3.9",
                )
            ]
        ),
    )

    created_pod = await core_v1.create_namespaced_pod(
        namespace=kube_namespace, body=pod_manifest
    )

    try:
        job = FakeJob(worker_id=worker_id, retryable=True)
        queue = FakeQueue(jobs=[job])
        monitor = KubernetesSaqEventMonitor(
            queue=queue,
            namespace=kube_namespace,
        )
        monitor._api_client = kube_client
        monitor._core_v1 = core_v1
        monitor._deduper = EventDeduper(ttl_s=60)

        event = client.CoreV1Event(
            metadata=client.V1ObjectMeta(uid="event-1"),
            involved_object=client.V1ObjectReference(
                kind="Pod",
                name=pod_name,
                namespace=kube_namespace,
                uid=created_pod.metadata.uid if created_pod.metadata else None,
            ),
            reason="OOMKilled",
            message="container was OOMKilled",
            type="Warning",
        )

        await monitor._handle_event(event)

        assert job.retry_called is True
        assert job.finish_called is False
        assert job.error is not None
        assert "OOMKilled" in job.error
    finally:
        with contextlib.suppress(Exception):
            await core_v1.delete_namespaced_pod(name=pod_name, namespace=kube_namespace)
        await asyncio.sleep(0.1)
