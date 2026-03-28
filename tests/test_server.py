from __future__ import annotations

import pytest
from aiohttp.test_utils import TestClient, TestServer

from saq_k8s_watch.server import start_server


@pytest.fixture
async def client():
    """Create a test client for the HTTP server."""
    from aiohttp import web

    from saq_k8s_watch.server import _livez, _metrics

    app = web.Application()
    app.router.add_get("/-/livez", _livez)
    app.router.add_get("/metrics", _metrics)

    async with TestClient(TestServer(app)) as c:
        yield c


@pytest.mark.asyncio
async def test_metrics_endpoint_returns_200(client: TestClient) -> None:
    resp = await client.get("/metrics")
    assert resp.status == 200
    body = await resp.text()
    assert "python_info" in body


@pytest.mark.asyncio
async def test_livez_returns_204(client: TestClient) -> None:
    resp = await client.get("/-/livez")
    assert resp.status == 204
