"""Simple Prometheus-style metrics output."""

from __future__ import annotations

from collections import Counter
import threading


class MetricsCollector:
    """Track runtime counters that are not persisted in the database."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._lock_outcomes: Counter[str] = Counter()

    def record_lock_outcome(self, outcome: str) -> None:
        with self._lock:
            self._lock_outcomes[outcome] += 1

    def snapshot(self) -> dict[str, Counter[str]]:
        with self._lock:
            return {
                "lock_outcomes": Counter(self._lock_outcomes),
            }


def render_metric_lines(name: str, value: int | float, labels: dict[str, str] | None = None) -> str:
    """Render one Prometheus metric line."""
    if labels:
        rendered = ",".join(f'{key}="{val}"' for key, val in sorted(labels.items()))
        return f"{name}{{{rendered}}} {value}"
    return f"{name} {value}"
