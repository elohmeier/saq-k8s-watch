from importlib.metadata import PackageNotFoundError, version

from saq_k8s_watch.monitor import KubernetesSaqEventMonitor

try:
    __version__ = version("saq-k8s-watch")
except PackageNotFoundError:
    __version__ = "unknown"

__all__ = ["KubernetesSaqEventMonitor", "__version__"]
