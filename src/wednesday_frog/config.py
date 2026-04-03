"""Application configuration helpers."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


BOOTSTRAP_PLACEHOLDERS = {
    "change-me-to-a-long-random-secret",
    "change-me-to-a-second-long-random-secret",
    "change-me-to-a-one-time-bootstrap-token",
    "development-master-key",
    "development-session-secret",
    "setup-token",
}


def _to_bool(value: str | None, *, default: bool = False) -> bool:
    """Convert a text environment value to a boolean."""
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _read_env_or_file(name: str, *, default: str | None = None) -> str | None:
    """Read a plain env var or its matching _FILE variant."""
    direct = os.getenv(name)
    if direct:
        return direct
    file_path = os.getenv(f"{name}_FILE")
    if file_path:
        return Path(file_path).read_text(encoding="utf-8").strip()
    return default


def _split_csv(value: str | None) -> tuple[str, ...]:
    """Split a comma-delimited env var into a normalized tuple."""
    if not value:
        return ()
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _is_app_root(candidate: Path) -> bool:
    """Return whether a directory looks like the app asset root."""
    return (
        candidate.is_dir()
        and (candidate / "templates").is_dir()
        and (candidate / "static").is_dir()
        and (candidate / "wednesday-frog.png").is_file()
    )


def _resolve_repo_root() -> Path:
    """Find the application root for installed and editable layouts."""
    candidates: list[Path] = []
    env_root = os.getenv("WEDNESDAY_FROG_APP_ROOT")
    if env_root:
        candidates.append(Path(env_root).expanduser())
    candidates.extend([Path.cwd(), Path(__file__).resolve().parents[2]])

    seen: set[Path] = set()
    ordered_candidates: list[Path] = []
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved not in seen:
            seen.add(resolved)
            ordered_candidates.append(resolved)

    for candidate in ordered_candidates:
        if _is_app_root(candidate):
            return candidate

    checked = ", ".join(str(candidate) for candidate in ordered_candidates)
    raise RuntimeError(
        "Could not locate the Wednesday Frog app root. "
        "Expected templates/, static/, and wednesday-frog.png under one directory. "
        f"Checked: {checked}"
    )


@dataclass(slots=True, frozen=True)
class AppConfig:
    """Resolved runtime configuration for the application."""

    database_url: str
    master_key: str
    previous_master_key: str | None
    session_secret: str
    setup_token: str
    metrics_token: str | None
    timezone_env: str
    scheduler_disabled: bool
    redis_url: str | None
    outbound_allowlist: tuple[str, ...]
    shutdown_grace_seconds: int
    repo_root: Path
    template_dir: Path
    static_dir: Path
    data_dir: Path
    assets_dir: Path
    logs_dir: Path

    @property
    def bundled_asset_path(self) -> Path:
        """Return the checked-in frog image path."""
        return self.repo_root / "wednesday-frog.png"

    @property
    def package_plugins_dir(self) -> Path:
        """Return the bundled plugin discovery directory."""
        return Path(__file__).resolve().parent / "plugins"

    @property
    def future_data_plugins_dir(self) -> Path:
        """Return the reserved future plugin directory under /data."""
        return self.data_dir / "plugins"

    @property
    def ha_enabled(self) -> bool:
        """Return whether Redis-backed HA coordination is enabled."""
        return bool(self.redis_url) and self.database_url.startswith("postgres")

    @classmethod
    def from_env(cls) -> "AppConfig":
        """Build configuration from the current process environment."""
        repo_root = _resolve_repo_root()
        database_url = os.getenv("WEDNESDAY_FROG_DATABASE_URL") or os.getenv("DATABASE_URL") or "sqlite:////data/wednesday_frog.db"
        sqlite_path = database_url.removeprefix("sqlite:////")
        if sqlite_path == database_url:
            data_dir = Path("/data")
        else:
            data_dir = Path("/") / sqlite_path if not sqlite_path.startswith("/") else Path(sqlite_path)
            data_dir = data_dir.parent
        metrics_token = _read_env_or_file("WEDNESDAY_FROG_METRICS_TOKEN")
        return cls(
            database_url=database_url,
            master_key=_read_env_or_file("WEDNESDAY_FROG_MASTER_KEY", default="development-master-key") or "",
            previous_master_key=_read_env_or_file("WEDNESDAY_FROG_PREVIOUS_MASTER_KEY"),
            session_secret=_read_env_or_file("WEDNESDAY_FROG_SESSION_SECRET", default="development-session-secret") or "",
            setup_token=_read_env_or_file("WEDNESDAY_FROG_SETUP_TOKEN", default="setup-token") or "",
            metrics_token=metrics_token,
            timezone_env=os.getenv("TZ", "UTC"),
            scheduler_disabled=_to_bool(os.getenv("WEDNESDAY_FROG_DISABLE_SCHEDULER"), default=False),
            redis_url=os.getenv("WEDNESDAY_FROG_REDIS_URL") or os.getenv("REDIS_URL"),
            outbound_allowlist=_split_csv(os.getenv("WEDNESDAY_FROG_OUTBOUND_ALLOWLIST")),
            shutdown_grace_seconds=max(int(os.getenv("WEDNESDAY_FROG_SHUTDOWN_GRACE_SECONDS", "60")), 1),
            repo_root=repo_root,
            template_dir=repo_root / "templates",
            static_dir=repo_root / "static",
            data_dir=data_dir,
            assets_dir=data_dir / "assets",
            logs_dir=data_dir / "logs",
        )

    def ensure_runtime_dirs(self) -> None:
        """Create the writable runtime directories."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.assets_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.future_data_plugins_dir.mkdir(parents=True, exist_ok=True)

    def bootstrap_issues(self) -> list[str]:
        """Return any fatal bootstrap secret issues."""
        issues: list[str] = []
        for name, value in (
            ("WEDNESDAY_FROG_MASTER_KEY", self.master_key),
            ("WEDNESDAY_FROG_SESSION_SECRET", self.session_secret),
            ("WEDNESDAY_FROG_SETUP_TOKEN", self.setup_token),
        ):
            if not value:
                issues.append(f"{name} is required.")
                continue
            lowered = value.lower()
            if value in BOOTSTRAP_PLACEHOLDERS or "change-me" in lowered or "replace-with" in lowered:
                issues.append(f"{name} must be replaced with a unique secret.")
            if len(value) < 32:
                issues.append(f"{name} must be at least 32 characters long.")
        return issues
