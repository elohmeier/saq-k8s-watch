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
    queues = [
        Queue.from_url("postgres://user:pass@host/db", name=name)
        for name in ("extraction", "orchestrator", "responsive")
    ]
    monitor = KubernetesSaqEventMonitor(
        queues,
        namespace="default",
        label_selector="app.kubernetes.io/name=saq-main",
        worker_id_label="saq.io/worker-id",
    )
    await monitor.run()

asyncio.run(main())
```

If you don't set a worker-id label or annotation, the monitor falls back to using the pod name as the SAQ worker id.

### Multi-Queue Support

The monitor accepts a list of queues and scans all of them when looking for
active jobs belonging to a stopped worker. The CLI reads queue names from
`SAQ_QUEUE_NAMES` (comma-separated) with a fallback to `SAQ_QUEUE_NAME` for
backward compatibility.

### Orphan Sweep

A periodic background sweep detects active jobs whose `worker_id` no longer
matches any running pod. This catches jobs that slip through the event-based
detection (e.g., monitor restart, missed events). Configure via:

| Environment Variable | Default | Description |
|---|---|---|
| `SAQ_QUEUE_NAMES` | `default` | Comma-separated queue names to monitor |
| `SAQ_ORPHAN_SWEEP_INTERVAL_S` | `60` | Seconds between sweep runs |
| `SAQ_ORPHAN_SWEEP_MIN_AGE_S` | `300` | Minimum job age (seconds) before considering it orphaned |

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
