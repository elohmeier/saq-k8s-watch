# Repository Guidelines

## Project Structure & Module Organization

The codebase is a small Python package. Source lives under `saq_k8s_watch/` with the public API in `saq_k8s_watch/__init__.py` and the Kubernetes monitor implementation in `saq_k8s_watch/monitor.py`. There are currently no test directories or non-code assets in the repository.

## Build, Test, and Development Commands

No dedicated build or test scripts are defined. The project is configured as a Python package in `pyproject.toml` with a `uv_build` backend. If you use `uv`, the typical build command is:

```bash
uv build
```

Runtime usage is shown in `README.md`, for example:

```python
from saq_k8s_watch import KubernetesSaqEventMonitor
```

## Coding Style & Naming Conventions

No formatter or linter is configured in `pyproject.toml`. Keep changes consistent with existing style:

- Python with 4-space indentation.
- `snake_case` for functions/variables.
- `PascalCase` for classes.
- Prefer type annotations on public APIs (matching the existing code).

## Testing Guidelines

Tests live under `tests/` and use `pytest` naming (e.g., `tests/test_monitor.py`). Install test extras with `uv pip install -e ".[test]"` and run with `uv run pytest -q`. The suite expects access to a local Kubernetes cluster; it will skip if no kube config is available.

### Local Environment Notes (OrbStack/Kubernetes)

- Kubernetes access: tests rely on a reachable local kube cluster (e.g., OrbStack). `kubectl` must be available in `PATH`.
- DNS access: tests assume the host can reach cluster DNS for services like `saq-redis.<namespace>.svc.cluster.local`.
- OOM integration test: builds a worker image from `tests/oom-worker.Dockerfile`. Requires `docker` or `nerdctl`. If missing, the test skips.
- Timeouts: integration waits are capped at 10 seconds for faster feedback.
- Dependencies: `typing-extensions` is required at runtime because `saq` imports it.

## Commit & Pull Request Guidelines

The Git history currently contains a single commit, so no message convention is established. Use clear, imperative commit messages (e.g., "Add pod restart handling"). For pull requests:

- Include a concise summary and the problem being solved.
- Call out any Kubernetes or SAQ behavior changes.
- Add example configuration or usage notes if APIs change.

## Configuration & Usage Notes

The package depends on Python `>=3.14`, `kubernetes-asyncio`, and `saq`. When adding new configuration options, document them in `README.md` and include a short code example of the intended usage.
