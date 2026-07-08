from __future__ import annotations

from datetime import date, datetime, timedelta, timezone


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    return ensure_utc(datetime.fromisoformat(normalized))


def parse_date_arg(value: str) -> date:
    lowered = value.lower()
    today = utc_now().date()
    if lowered == "today":
        return today
    if lowered == "yesterday":
        return today - timedelta(days=1)
    if lowered == "tomorrow":
        return today + timedelta(days=1)
    return date.fromisoformat(value)


def epoch_ms(value: datetime | None = None) -> int:
    return int((value or utc_now()).timestamp() * 1000)

