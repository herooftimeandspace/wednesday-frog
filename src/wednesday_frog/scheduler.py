"""Scheduler wrapper around APScheduler."""

from __future__ import annotations

from typing import Any, Callable

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger


class SchedulerService:
    """Manage the single scheduled frog-send job."""

    def __init__(self, *, enabled: bool = True) -> None:
        self._enabled = enabled
        self._scheduler = BackgroundScheduler()
        self._job_id = "wednesday-frog-delivery"
        self._job: Callable[[], Any] | None = None

    def start(self) -> None:
        """Start the background scheduler if enabled."""
        if self._enabled and not self._scheduler.running:
            self._scheduler.start()

    def shutdown(self) -> None:
        """Shut down the scheduler cleanly."""
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)

    def configure(self, *, cron: str, timezone: str, enabled: bool, job: Callable[[], Any]) -> None:
        """Refresh the one scheduled job from persisted settings."""
        self._job = job
        self._enabled = enabled
        if not self._enabled:
            if self._scheduler.get_job(self._job_id):
                self._scheduler.remove_job(self._job_id)
            return
        trigger = CronTrigger.from_crontab(cron, timezone=timezone)
        options = {
            "trigger": trigger,
            "id": self._job_id,
            "replace_existing": True,
            "misfire_grace_time": 900,
            "coalesce": True,
            "max_instances": 1,
        }
        if self._scheduler.get_job(self._job_id):
            self._scheduler.reschedule_job(self._job_id, trigger=trigger)
            self._scheduler.modify_job(self._job_id, misfire_grace_time=900, coalesce=True, max_instances=1)
        else:
            self._scheduler.add_job(job, **options)

    def next_run_time(self):
        """Return the next scheduled run time if present."""
        job = self._scheduler.get_job(self._job_id)
        return None if job is None else job.next_run_time
