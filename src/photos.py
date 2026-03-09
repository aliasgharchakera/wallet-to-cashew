"""Download photo attachments from Budget Bakers records."""

from __future__ import annotations

from pathlib import Path

import requests

from src.models import BBRecord

PHOTOS_DIR = Path("data/photos")


def download_photos(records: list[BBRecord]) -> dict[str, list[str]]:
    """Download all photo attachments from records.

    Returns a mapping of record_id -> list of local file paths.
    """
    PHOTOS_DIR.mkdir(parents=True, exist_ok=True)

    photo_map: dict[str, list[str]] = {}
    total = sum(len(r.photos) for r in records)

    if total == 0:
        print("No photos found in records.")
        return photo_map

    print(f"Downloading {total} photos from {sum(1 for r in records if r.photos)} records...")

    downloaded = 0
    failed = 0

    for record in records:
        if not record.photos:
            continue

        record_photos: list[str] = []
        for i, photo in enumerate(record.photos):
            if not photo.temporaryUrl:
                continue

            ext = _guess_extension(photo.temporaryUrl)
            filename = f"{record.id}_{i}{ext}"
            filepath = PHOTOS_DIR / filename

            if filepath.exists():
                record_photos.append(str(filepath))
                downloaded += 1
                continue

            try:
                resp = requests.get(photo.temporaryUrl, timeout=30)
                resp.raise_for_status()
                filepath.write_bytes(resp.content)
                record_photos.append(str(filepath))
                downloaded += 1
            except requests.RequestException as e:
                print(f"  Failed to download photo for record {record.id}: {e}")
                failed += 1

        if record_photos:
            photo_map[record.id] = record_photos

    print(f"Photos: {downloaded} downloaded, {failed} failed")
    return photo_map


def _guess_extension(url: str) -> str:
    """Guess file extension from URL."""
    lower = url.lower().split("?")[0]
    for ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".heic"):
        if lower.endswith(ext):
            return ext
    return ".jpg"
