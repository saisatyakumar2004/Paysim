"""Resource measurement: peak resident memory (RSS) of this process AND its children (e.g. the dbt subprocess).

Uses psutil (optional: `pip install -e ".[bench]"`). Without psutil the measurement is simply unavailable (None) and
everything else keeps working; nothing here changes pipeline behaviour.
"""

from __future__ import annotations

import os
import threading
import time
from types import TracebackType

try:                                                    # pragma: no cover - import guard
    import psutil
except ImportError:                                     # pragma: no cover
    psutil = None


def available() -> bool:
    return psutil is not None


def rss_mb(include_children: bool = True) -> float | None:
    """Current RSS of this process tree in MB (None when psutil is missing)."""
    if psutil is None:
        return None
    proc = psutil.Process(os.getpid())
    total = 0
    try:
        procs = [proc, *(proc.children(recursive=True) if include_children else [])]
        for p in procs:
            try:
                total += p.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None
    return total / 1024 / 1024


class PeakMemory:
    """Context manager sampling the RSS of the process tree; `.peak_mb` is the highest sample seen.

    Sampling (default every 50 ms) can miss a spike shorter than the interval, so treat the figure as a lower bound
    on the true peak. `.start_mb` is the RSS when the block began, `.delta_mb` = peak - start.
    """

    def __init__(self, interval: float = 0.05, include_children: bool = True):
        self.interval, self.include_children = interval, include_children
        self.peak_mb: float | None = None
        self.start_mb: float | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _run(self) -> None:
        while not self._stop.is_set():
            cur = rss_mb(self.include_children)
            if cur is not None and (self.peak_mb is None or cur > self.peak_mb):
                self.peak_mb = cur
            time.sleep(self.interval)

    def __enter__(self) -> PeakMemory:
        if psutil is None:
            return self
        self.start_mb = self.peak_mb = rss_mb(self.include_children)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None,
                 tb: TracebackType | None) -> None:
        if self._thread is not None:
            self._stop.set()
            self._thread.join(timeout=1)
            cur = rss_mb(self.include_children)
            if cur is not None and (self.peak_mb is None or cur > self.peak_mb):
                self.peak_mb = cur

    @property
    def delta_mb(self) -> float | None:
        return None if self.peak_mb is None or self.start_mb is None else self.peak_mb - self.start_mb
