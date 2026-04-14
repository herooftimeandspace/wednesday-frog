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
import uuid

from PIL import Image, UnidentifiedImageError
from sqlalchemy.orm import Session, sessionmaker

from .config import AppConfig
from .delivery.base import PreparedAsset
from .models import AssetRecord


LOGGER = logging.getLogger(__name__)
ALLOWED_MEDIA_TYPES = {"image/png", "image/jpeg"}
ALLOWED_IMAGE_FORMATS = {
    "PNG": "image/png",
    "JPEG": "image/jpeg",
}
MAX_UPLOAD_BYTES = 5_000_000
STREAM_CHUNK_BYTES = 64 * 1024


def guess_media_type(filename: str) -> str:
    """Guess a media type from a filename and fall back to PNG."""
    guessed, _ = mimetypes.guess_type(filename)
    return guessed or "image/png"


def _validate_media_type(media_type: str) -> None:
    """Raise when the requested media type is unsupported."""
    if media_type not in ALLOWED_MEDIA_TYPES:
        raise ValueError("Only PNG and JPEG images are supported.")


def _inspect_image_stream(stream, media_type: str, *, size_bytes: int) -> tuple[int, int, str]:
    """Decode one image stream once and return its metadata."""
    _validate_media_type(media_type)
    if size_bytes > MAX_UPLOAD_BYTES:
        raise ValueError("Uploads must be 5 MB or smaller.")
    try:
        with Image.open(stream, formats=tuple(ALLOWED_IMAGE_FORMATS)) as image:
            actual_format = image.format
            if actual_format not in ALLOWED_IMAGE_FORMATS:
                raise ValueError("Only PNG and JPEG images are supported.")
            actual_media_type = ALLOWED_IMAGE_FORMATS[actual_format]
            if actual_media_type != media_type:
                raise ValueError("Uploaded file contents do not match the selected image type.")
            width, height = image.size
            image.load()
            return width, height, actual_format
    except UnidentifiedImageError as exc:
        raise ValueError("Only valid PNG and JPEG images are supported.") from exc


def validate_image_bytes(payload: bytes, media_type: str) -> tuple[int, int]:
    """Open image bytes once with Pillow to verify the uploaded image is valid."""
    width, height, _ = _inspect_image_stream(io.BytesIO(payload), media_type, size_bytes=len(payload))
    return width, height


def validate_image_path(path: Path, media_type: str, *, size_bytes: int | None = None) -> tuple[int, int]:
    """Open an on-disk image once with Pillow to verify it is valid."""
    resolved_size = size_bytes if size_bytes is not None else path.stat().st_size
    with path.open("rb") as handle:
        width, height, _ = _inspect_image_stream(handle, media_type, size_bytes=resolved_size)
    return width, height


def _final_extension(media_type: str) -> str:
    return ".png" if media_type == "image/png" else ".jpg"


def _copy_stream_to_path(source, target: Path) -> tuple[int, str]:
    """Copy a binary source to disk while enforcing the upload size limit."""
    digest = hashlib.sha256()
    total = 0
    if hasattr(source, "seek"):
        source.seek(0)
    with target.open("wb") as destination:
        while True:
            chunk = source.read(STREAM_CHUNK_BYTES)
            if not chunk:
                break
            if isinstance(chunk, str):
                chunk = chunk.encode("utf-8")
            total += len(chunk)
            if total > MAX_UPLOAD_BYTES:
                raise ValueError("Uploads must be 5 MB or smaller.")
            digest.update(chunk)
            destination.write(chunk)
    return total, digest.hexdigest()


def _copy_file_with_digest(source: Path, target: Path) -> tuple[int, str]:
    """Copy a file to a new path while computing its size and digest."""
    with source.open("rb") as handle:
        return _copy_stream_to_path(handle, target)


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
    staged_path = config.assets_dir / temp_name
    staged_path.write_bytes(payload)
    asset = AssetRecord(
        original_filename=filename,
        stored_filename=temp_name,
        media_type=media_type,
        size_bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        is_default=False,
        processing_status="pending",
        processing_error=None,
    )
    session.add(asset)
    session.flush()
    return asset


def create_pending_asset_from_upload(
    session: Session,
    config: AppConfig,
    *,
    filename: str,
    upload_file,
    media_type: str,
) -> AssetRecord:
    """Stream an uploaded asset to disk for background processing."""
    _validate_media_type(media_type)
    temp_name = f"{uuid.uuid4().hex}.upload"
    staged_path = config.assets_dir / temp_name
    try:
        size_bytes, digest = _copy_stream_to_path(upload_file, staged_path)
        validate_image_path(staged_path, media_type, size_bytes=size_bytes)
    except Exception:
        staged_path.unlink(missing_ok=True)
        raise
    asset = AssetRecord(
        original_filename=filename,
        stored_filename=temp_name,
        media_type=media_type,
        size_bytes=size_bytes,
        sha256=digest,
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
            validate_image_path(staged_path, asset.media_type, size_bytes=asset.size_bytes)
            final_name = f"{uuid.uuid4().hex}{_final_extension(asset.media_type)}"
            final_path = config.assets_dir / final_name
            shutil.move(str(staged_path), str(final_path))
            asset.stored_filename = final_name
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
        target_path = config.assets_dir / existing.stored_filename
        existing.size_bytes, existing.sha256 = _copy_file_with_digest(bundled_path, target_path)
        existing.media_type = guess_media_type(bundled_path.name)
        session.flush()
        return existing
    bundled_path = config.bundled_asset_path
    media_type = guess_media_type(bundled_path.name)
    stored_name = f"{uuid.uuid4().hex}{_final_extension(media_type)}"
    target_path = config.assets_dir / stored_name
    size_bytes, digest = _copy_file_with_digest(bundled_path, target_path)
    asset = AssetRecord(
        original_filename=bundled_path.name,
        stored_filename=stored_name,
        media_type=media_type,
        size_bytes=size_bytes,
        sha256=digest,
        is_default=True,
        processing_status="ready",
        processing_error=None,
    )
    session.add(asset)
    session.flush()
    return asset


def resolve_asset_path(config: AppConfig, asset: AssetRecord) -> Path:
    """Return the on-disk path for a stored asset."""
    return config.assets_dir / asset.stored_filename


def load_asset_bytes(config: AppConfig, asset: AssetRecord) -> bytes:
    """Read an asset's bytes from local storage."""
    path = resolve_asset_path(config, asset)
    if not path.is_file() and asset.is_default:
        return config.bundled_asset_path.read_bytes()
    return path.read_bytes()


def build_teams_data_uri(asset: PreparedAsset, *, max_payload_bytes: int = 20_000) -> str:
    """Create a compressed data URI small enough for Teams webhook payloads."""
    if asset.source_path and asset.source_path.is_file():
        image_source = asset.source_path
    elif asset.payload:
        image_source = io.BytesIO(asset.payload)
    else:
        raise ValueError("The active asset is unavailable for Teams delivery.")
    size_candidates = [680, 512, 384, 256, 192, 160, 128, 96]
    quality_candidates = [85, 70, 55, 40, 30]
    with Image.open(image_source) as opened:
        image = opened.convert("RGB")
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
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="wednesday-frog-asset")
        self._futures: set[Future[None]] = set()

    def queue(self, asset_id: int) -> None:
        """Queue one pending asset for background processing."""
        future = self._executor.submit(process_pending_asset, self._session_factory, self._config, asset_id)
        self._futures.add(future)
        future.add_done_callback(lambda done: self._futures.discard(done))

    def shutdown(self) -> None:
        """Stop the worker pool."""
        self._executor.shutdown(wait=True)
