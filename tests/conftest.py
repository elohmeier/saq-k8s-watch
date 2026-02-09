from __future__ import annotations

import os
import shutil
import subprocess
import uuid

import pytest
import pytest_asyncio
from kubernetes_asyncio import client, config


async def _load_kube_config() -> None:
    try:
        await config.load_incluster_config()
    except Exception:
        await config.load_kube_config()


@pytest_asyncio.fixture
async def kube_client() -> client.ApiClient:
    try:
        await _load_kube_config()
    except Exception as exc:
        pytest.skip(f"Kubernetes config not available: {exc}")

    api_client = client.ApiClient()
    try:
        # Basic connectivity check
        await client.CoreV1Api(api_client).list_namespace(limit=1)
    except Exception as exc:
        await api_client.close()
        pytest.skip(f"Kubernetes API not reachable: {exc}")

    yield api_client
    await api_client.close()


@pytest_asyncio.fixture
async def kube_namespace(kube_client: client.ApiClient) -> str:
    core_v1 = client.CoreV1Api(kube_client)
    namespace = os.environ.get("SAQ_K8S_TEST_NAMESPACE")
    if namespace:
        yield namespace
        return

    name = f"saq-k8s-watch-test-{uuid.uuid4().hex[:8]}"
    body = client.V1Namespace(metadata=client.V1ObjectMeta(name=name))
    await core_v1.create_namespace(body=body)

    try:
        yield name
    finally:
        try:
            await core_v1.delete_namespace(name=name)
        except Exception:
            # Best-effort cleanup.
            pass


@pytest.fixture(scope="session")
def oom_worker_image() -> str:
    docker = shutil.which("docker") or shutil.which("nerdctl")
    if docker is None:
        pytest.skip("Docker/nerdctl not found; cannot build OOM test image.")

    tag = "saq-k8s-watch-oom-test:latest"
    dockerfile = os.path.join(os.path.dirname(__file__), "oom-worker.Dockerfile")
    context = os.path.dirname(__file__)

    result = subprocess.run(
        [
            docker,
            "build",
            "-f",
            dockerfile,
            "-t",
            tag,
            context,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to build OOM test image:\n{result.stdout}\n{result.stderr}"
        )

    return tag
