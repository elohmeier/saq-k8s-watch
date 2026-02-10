from __future__ import annotations

import logging

from aiohttp import web
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    GCCollector,
    PlatformCollector,
    ProcessCollector,
    generate_latest,
)

logger = logging.getLogger(__name__)

REGISTRY = CollectorRegistry()
ProcessCollector(registry=REGISTRY)
PlatformCollector(registry=REGISTRY)
GCCollector(registry=REGISTRY)

PORT = 8080


async def _livez(_request: web.Request) -> web.Response:
    return web.Response(status=204)


async def _metrics(_request: web.Request) -> web.Response:
    payload = generate_latest(REGISTRY)
    return web.Response(body=payload, content_type=CONTENT_TYPE_LATEST)


async def start_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/-/livez", _livez)
    app.router.add_get("/-/livez/", _livez)
    app.router.add_get("/metrics", _metrics)

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("HTTP server listening on port %s", PORT)
    return runner
