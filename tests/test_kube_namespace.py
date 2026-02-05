from __future__ import annotations

import pytest
from kubernetes_asyncio import client


@pytest.mark.asyncio
async def test_temporary_namespace_exists(
    kube_client: client.ApiClient, kube_namespace: str
) -> None:
    core_v1 = client.CoreV1Api(kube_client)
    namespaces = await core_v1.list_namespace()
    names = {item.metadata.name for item in namespaces.items if item.metadata}
    assert kube_namespace in names
