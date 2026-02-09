from __future__ import annotations

import asyncio
import contextlib
import shutil

import pytest
from kubernetes_asyncio import client

from saq.job import Status
from saq.queue import Queue
from saq_k8s_watch.monitor import KubernetesSaqEventMonitor


def _skip_reason() -> str | None:
    if shutil.which("kubectl") is None:
        return "kubectl not found in PATH."
    return None


async def _wait_for_tcp(host: str, port: int, timeout_s: int = 10) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        try:
            reader, writer = await asyncio.open_connection(host, port)
            writer.close()
            await writer.wait_closed()
            return
        except OSError:
            await asyncio.sleep(0.5)
    raise RuntimeError(f"Timed out waiting for TCP {host}:{port}")


async def _wait_for_pod_ready(
    core_v1: client.CoreV1Api, namespace: str, label_selector: str, timeout_s: int = 10
) -> str:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        pods = await core_v1.list_namespaced_pod(
            namespace, label_selector=label_selector
        )
        for pod in pods.items:
            if pod.status and pod.status.phase == "Running":
                statuses = pod.status.container_statuses or []
                if statuses and all(s.ready for s in statuses):
                    return pod.metadata.name if pod.metadata else ""
        await asyncio.sleep(1)
    raise RuntimeError("Timed out waiting for pod to be ready")


async def _wait_for_deployment_ready(
    apps_v1: client.AppsV1Api, namespace: str, name: str, timeout_s: int = 10
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        deployment = await apps_v1.read_namespaced_deployment(
            name=name, namespace=namespace
        )
        status = deployment.status
        if status and status.ready_replicas and status.ready_replicas >= 1:
            return
        await asyncio.sleep(1)
    raise RuntimeError("Timed out waiting for deployment to be ready")


@pytest.mark.asyncio
async def test_monitor_detects_oom_and_updates_job(
    kube_client: client.ApiClient, kube_namespace: str, oom_worker_image: str
) -> None:
    reason = _skip_reason()
    if reason:
        pytest.skip(reason)

    core_v1 = client.CoreV1Api(kube_client)

    redis_labels = {"app": "saq-redis"}
    redis_service_name = "saq-redis"
    redis_deploy = client.V1Deployment(
        metadata=client.V1ObjectMeta(name="saq-redis", namespace=kube_namespace),
        spec=client.V1DeploymentSpec(
            replicas=1,
            selector=client.V1LabelSelector(match_labels=redis_labels),
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels=redis_labels),
                spec=client.V1PodSpec(
                    containers=[
                        client.V1Container(
                            name="redis",
                            image="redis:7-alpine",
                            ports=[client.V1ContainerPort(container_port=6379)],
                        )
                    ]
                ),
            ),
        ),
    )

    redis_service = client.V1Service(
        metadata=client.V1ObjectMeta(name=redis_service_name, namespace=kube_namespace),
        spec=client.V1ServiceSpec(
            selector=redis_labels,
            ports=[client.V1ServicePort(port=6379, target_port=6379)],
        ),
    )

    apps_v1 = client.AppsV1Api(kube_client)
    pod_name = ""
    try:
        await apps_v1.create_namespaced_deployment(
            namespace=kube_namespace, body=redis_deploy
        )
        await core_v1.create_namespaced_service(
            namespace=kube_namespace, body=redis_service
        )
        await _wait_for_deployment_ready(apps_v1, kube_namespace, "saq-redis")

        worker_labels = {"app": "saq-worker"}
        worker_image = oom_worker_image
        command = None
        args = None

        redis_host = f"{redis_service_name}.{kube_namespace}.svc.cluster.local"
        try:
            await _wait_for_tcp(redis_host, 6379, timeout_s=10)
        except Exception as exc:
            pytest.skip(
                f"Redis service not reachable from host at {redis_host}: {exc}. "
                "Ensure your local environment can access cluster DNS or set up routing."
            )

        queue = Queue.from_url(f"redis://{redis_host}:6379/0", name="default")
        await queue.connect()

        monitor = KubernetesSaqEventMonitor(
            queue=queue,
            namespace=kube_namespace,
            label_selector="app=saq-worker",
            use_pod_name_as_worker_id=True,
            retry_on_stop=False,
            terminal_status=Status.FAILED,
            watch_timeout_s=10,
        )

        monitor_task = asyncio.create_task(monitor.run())
        try:
            job = await queue.enqueue("oom", retries=0, timeout=10)
            assert job is not None

            worker_container = client.V1Container(
                name="worker",
                image=worker_image,
                env=[
                    client.V1EnvVar(
                        name="SAQ_QUEUE_URL",
                        value="redis://saq-redis:6379/0",
                    ),
                    client.V1EnvVar(
                        name="PIP_ROOT_USER_ACTION",
                        value="ignore",
                    ),
                    client.V1EnvVar(
                        name="SAQ_WORKER_ID",
                        value_from=client.V1EnvVarSource(
                            field_ref=client.V1ObjectFieldSelector(
                                field_path="metadata.name"
                            )
                        ),
                    ),
                ],
                resources=client.V1ResourceRequirements(
                    limits={"memory": "32Mi"},
                    requests={"memory": "16Mi"},
                ),
            )
            if command:
                worker_container.command = command
            if args:
                worker_container.args = args

            worker_pod = client.V1Pod(
                metadata=client.V1ObjectMeta(
                    name="saq-oom-worker",
                    namespace=kube_namespace,
                    labels=worker_labels,
                ),
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    containers=[worker_container],
                ),
            )

            created_pod = await core_v1.create_namespaced_pod(
                namespace=kube_namespace, body=worker_pod
            )
            pod_name = (
                created_pod.metadata.name
                if created_pod.metadata and created_pod.metadata.name
                else "saq-oom-worker"
            )
            await queue.update(job, status=Status.ACTIVE, worker_id=pod_name)
            event = client.CoreV1Event(
                metadata=client.V1ObjectMeta(
                    generate_name="saq-oom-",
                    namespace=kube_namespace,
                    labels={"app": "saq-worker"},
                ),
                involved_object=client.V1ObjectReference(
                    kind="Pod",
                    name=pod_name,
                    namespace=kube_namespace,
                ),
                reason="OOMKilled",
                message="container was OOMKilled",
                type="Warning",
            )
            await core_v1.create_namespaced_event(namespace=kube_namespace, body=event)

            deadline = asyncio.get_running_loop().time() + 10
            while asyncio.get_running_loop().time() < deadline:
                current = await queue.job(job.key)
                if current and current.status in (Status.FAILED, Status.ABORTED):
                    assert current.error and "OOM" in current.error.upper()
                    break
                await asyncio.sleep(2)
            else:
                raise AssertionError(
                    "Timed out waiting for job to reach terminal state"
                )
        finally:
            monitor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await monitor_task
            await monitor.close()
    finally:
        # Best-effort cleanup.
        with contextlib.suppress(Exception):
            if pod_name:
                await core_v1.delete_namespaced_pod(
                    name=pod_name, namespace=kube_namespace
                )
        with contextlib.suppress(Exception):
            await apps_v1.delete_namespaced_deployment(
                name="saq-redis", namespace=kube_namespace
            )
        with contextlib.suppress(Exception):
            await core_v1.delete_namespaced_service(
                name=redis_service_name, namespace=kube_namespace
            )
