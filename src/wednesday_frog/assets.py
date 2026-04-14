"""Asset persistence and transformation helpers."""

from __future__ import annotations

import base64
from concurrent.futures import Future, ThreadPoolExecutor
import hashlib
import io
import logging
import mimetypes
from pathlib import Path
import shutil
import tempfile
import uuid

from PIL import Image, UnidentifiedImageError
from sqlalchemy.orm import Session, sessionmaker

from .config import AppConfig
from .models import AssetRecord


LOGGER = logging.getLogger(__name__)
ALLOWED_MEDIA_TYPES = {"image/png", "image/jpeg"}
ALLOWED_IMAGE_FORMATS = {
    "PNG": "image/png",
    "JPEG": "image/jpeg",
}
MAX_UPLOAD_BYTES = 5_000_000


def guess_media_type(filename: str) -> str:
    """Guess a media type from a filename and fall back to PNG."""
    guessed, _ = mimetypes.guess_type(filename)
    return guessed or "image/png"


def validate_image_bytes(payload: bytes, media_type: str) -> tuple[int, int]:
    """Open the bytes with Pillow to verify the uploaded image is valid."""
    if media_type not in ALLOWED_MEDIA_TYPES:
        raise ValueError("Only PNG and JPEG images are supported.")
    if len(payload) > MAX_UPLOAD_BYTES:
        raise ValueError("Uploads must be 5 MB or smaller.")
    try:
        with Image.open(io.BytesIO(payload), formats=tuple(ALLOWED_IMAGE_FORMATS)) as image:
            actual_format = image.format
            if actual_format not in ALLOWED_IMAGE_FORMATS:
                raise ValueError("Only PNG and JPEG images are supported.")
            actual_media_type = ALLOWED_IMAGE_FORMATS[actual_format]
            if actual_media_type != media_type:
                raise ValueError("Uploaded file contents do not match the selected image type.")
            image.verify()
        with Image.open(io.BytesIO(payload), formats=(actual_format,)) as image:
            return image.size
    except UnidentifiedImageError as exc:
        raise ValueError("Only valid PNG and JPEG images are supported.") from exc


def _final_extension(media_type: str) -> str:
    return ".png" if media_type == "image/png" else ".jpg"


def store_uploaded_asset(
    session: Session,
    config: AppConfig,
    *,
    filename: str,
    payload: bytes,
    media_type: str,
    is_default: bool = False,
) -> AssetRecord:
    """Persist a validated asset on disk and in the database."""
    validate_image_bytes(payload, media_type)
    extension = _final_extension(media_type)
    stored_name = f"{uuid.uuid4().hex}{extension}"
    target = config.assets_dir / stored_name
    target.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    asset = AssetRecord(
        original_filename=filename,
        stored_filename=stored_name,
        media_type=media_type,
        size_bytes=len(payload),
        sha256=digest,
        is_default=is_default,
        processing_status="ready",
        processing_error=None,
    )
    session.add(asset)
    session.flush()
    return asset


def create_pending_asset(
    session: Session,
    config: AppConfig,
    *,
    filename: str,
    payload: bytes,
    media_type: str,
) -> AssetRecord:
    """Stage an uploaded asset for background processing."""
    validate_image_bytes(payload, media_type)
    temp_name = f"{uuid.uuid4().hex}.upload"
    (config.assets_dir / temp_name).write_bytes(payload)
    asset = AssetRecord(
        original_filename=filename,
        stored_filename=temp_name,
        media_type=media_type,
        size_bytes=len(payload),
        sha256="",
        is_default=False,
        processing_status="pending",
        processing_error=None,
    )
    session.add(asset)
    session.flush()
    return asset


def process_pending_asset(session_factory: sessionmaker[Session], config: AppConfig, asset_id: int) -> None:
    """Validate a staged asset and promote it to a ready asset."""
    from .db import session_scope

    with session_scope(session_factory) as session:
        asset = session.get(AssetRecord, asset_id)
        if asset is None:
            return
        staged_path = config.assets_dir / asset.stored_filename
        try:
            payload = staged_path.read_bytes()
            validate_image_bytes(payload, asset.media_type)
            final_name = f"{uuid.uuid4().hex}{_final_extension(asset.media_type)}"
            final_path = config.assets_dir / final_name
            shutil.move(str(staged_path), str(final_path))
            asset.stored_filename = final_name
            asset.sha256 = hashlib.sha256(payload).hexdigest()
            asset.processing_status = "ready"
            asset.processing_error = None
        except Exception as exc:
            LOGGER.warning("Asset processing failed for asset_id=%s: %s", asset_id, exc)
            asset.processing_status = "failed"
            asset.processing_error = str(exc)
            if staged_path.exists():
                staged_path.unlink(missing_ok=True)


def ensure_default_asset(session: Session, config: AppConfig) -> AssetRecord:
    """Seed the checked-in frog image into the asset store on first run."""
    existing = session.query(AssetRecord).filter_by(is_default=True).order_by(AssetRecord.id.asc()).first()
    if existing is not None:
        default_path = resolve_asset_path(config, existing)
        if default_path.is_file():
            if existing.processing_status != "ready":
                existing.processing_status = "ready"
                existing.processing_error = None
            return existing
        existing.processing_status = "ready"
        bundled_path = config.bundled_asset_path
        shutil.copyfile(bundled_path, config.assets_dir / existing.stored_filename)
        existing.size_bytes = bundled_path.stat().st_size
        existing.sha256 = hashlib.sha256(bundled_path.read_bytes()).hexdigest()
        existing.media_type = guess_media_type(bundled_path.name)
        session.flush()
        return existing
    bundled_path = config.bundled_asset_path
    payload = bundled_path.read_bytes()
    media_type = guess_media_type(bundled_path.name)
    return store_uploaded_asset(
        session,
        config,
        filename=bundled_path.name,
        payload=payload,
        media_type=media_type,
        is_default=True,
    )


def resolve_asset_path(config: AppConfig, asset: AssetRecord) -> Path:
    """Return the on-disk path for a stored asset."""
    return config.assets_dir / asset.stored_filename


def load_asset_bytes(config: AppConfig, asset: AssetRecord) -> bytes:
    """Read an asset's bytes from local storage."""
    path = resolve_asset_path(config, asset)
    if not path.is_file() and asset.is_default:
        return config.bundled_asset_path.read_bytes()
    return path.read_bytes()


def build_teams_data_uri(payload: bytes, media_type: str, *, max_payload_bytes: int = 20_000) -> str:
    """Create a compressed data URI small enough for Teams webhook payloads."""
    image = Image.open(io.BytesIO(payload)).convert("RGB")
    size_candidates = [680, 512, 384, 256, 192, 160, 128, 96]
    quality_candidates = [85, 70, 55, 40, 30]
    for max_side in size_candidates:
        resized = image.copy()
        resized.thumbnail((max_side, max_side))
        for quality in quality_candidates:
            buffer = io.BytesIO()
            resized.save(buffer, format="JPEG", optimize=True, quality=quality)
            if buffer.tell() <= max_payload_bytes:
                encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
                return f"data:image/jpeg;base64,{encoded}"
    raise ValueError("Unable to compress the image enough for the Teams webhook payload budget.")


class AssetProcessor:
    """Handle background validation and promotion of uploaded assets."""

    def __init__(self, *, session_factory: sessionmaker[Session], config: AppConfig) -> None:
        self._session_factory = session_factory
        self._config = config
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="wednesday-frog-asset")
        self._futures: set[Future[None]] = set()

    def queue(self, asset_id: int) -> None:
        """Queue one pending asset for background processing."""
        future = self._executor.submit(process_pending_asset, self._session_factory, self._config, asset_id)
        self._futures.add(future)
        future.add_done_callback(lambda done: self._futures.discard(done))

    def shutdown(self) -> None:
        """Stop the worker pool."""
        self._executor.shutdown(wait=True)
