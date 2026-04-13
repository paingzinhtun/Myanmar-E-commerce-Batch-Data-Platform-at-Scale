from __future__ import annotations

from datetime import date, datetime, timedelta


def to_date_string(value: str | date | datetime) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return datetime.strptime(value, "%Y-%m-%d").date().isoformat()


def date_range(start_date: str, days: int) -> list[str]:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    return [(start + timedelta(days=offset)).isoformat() for offset in range(days)]
