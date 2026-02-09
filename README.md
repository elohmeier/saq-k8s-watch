# saq-k8s-watch

[![CI](https://github.com/elohmeier/saq-k8s-watch/actions/workflows/ci.yaml/badge.svg)](https://github.com/elohmeier/saq-k8s-watch/actions/workflows/ci.yaml)
[![CD](https://github.com/elohmeier/saq-k8s-watch/actions/workflows/cd.yaml/badge.svg)](https://github.com/elohmeier/saq-k8s-watch/actions/workflows/cd.yaml)
[![GHCR](https://img.shields.io/badge/ghcr.io-saq--k8s--watch-blue)](https://ghcr.io/elohmeier/saq-k8s-watch)

Kubernetes events watcher that detects worker pod stops (e.g., OOMKilled) and updates SAQ job/worker state.

## Usage

```python
import asyncio
from saq.queue import Queue
from saq_k8s_watch import KubernetesSaqEventMonitor

async def main() -> None:
    queue = Queue.from_url("postgres://user:pass@host/db", name="default")
    monitor = KubernetesSaqEventMonitor(
        queue,
        namespace="default",
        label_selector="app.kubernetes.io/name=saq-main",
        worker_id_label="saq.io/worker-id",
    )
    await monitor.run()

asyncio.run(main())
```

If you don't set a worker-id label or annotation, the monitor falls back to using the pod name as the SAQ worker id.

## Tests

Install dev dependencies:

```bash
uv sync --dev
```

Run the test suite (requires access to a local Kubernetes cluster):

```bash
uv run pytest -q
```

You can set `SAQ_K8S_TEST_NAMESPACE` to reuse an existing namespace instead of creating a temporary one.

### OOM Integration Test

There is an integration test that deploys Redis and a SAQ worker pod with a low memory limit, then verifies the monitor reacts to an OOM kill. It builds a small worker image from `tests/oom-worker.Dockerfile` using `docker` or `nerdctl`.

```bash
uv run pytest -q tests/test_monitor_oom_integration.py
```

If `docker`/`nerdctl` is not available, the test will skip.
