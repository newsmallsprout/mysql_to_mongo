import json
import os
import datetime
from typing import Any, Dict, List, Optional, Tuple


BASE_DIR = os.path.join("state", "inspection_reports")
DAILY_DIR = os.path.join(BASE_DIR, "daily")
WEEKLY_DIR = os.path.join(BASE_DIR, "weekly")
MONTHLY_DIR = os.path.join(BASE_DIR, "monthly")


def ensure_dirs():
    os.makedirs(DAILY_DIR, exist_ok=True)
    os.makedirs(WEEKLY_DIR, exist_ok=True)
    os.makedirs(MONTHLY_DIR, exist_ok=True)


def today_id() -> str:
    return datetime.date.today().isoformat()


def _write_json(path: str, data: Dict[str, Any]):
    ensure_dirs()
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _read_json(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _list_ids(dir_path: str) -> List[str]:
    if not os.path.isdir(dir_path):
        return []
    items = []
    for name in os.listdir(dir_path):
        if not name.endswith(".json"):
            continue
        items.append(name[:-5])
    items.sort(reverse=True)
    return items


def list_reports(kind: str, limit: int = 30) -> List[str]:
    ensure_dirs()
    if kind == "daily":
        ids = _list_ids(DAILY_DIR)
    elif kind == "weekly":
        ids = _list_ids(WEEKLY_DIR)
    elif kind == "monthly":
        ids = _list_ids(MONTHLY_DIR)
    else:
        return []
    return ids[: max(1, limit)]


def load_report(kind: str, report_id: str) -> Optional[Dict[str, Any]]:
    ensure_dirs()
    if kind == "daily":
        p = os.path.join(DAILY_DIR, f"{report_id}.json")
    elif kind == "weekly":
        p = os.path.join(WEEKLY_DIR, f"{report_id}.json")
    elif kind == "monthly":
        p = os.path.join(MONTHLY_DIR, f"{report_id}.json")
    else:
        return None
    return _read_json(p)


def save_daily(report_id: str, data: Dict[str, Any], keep_days: int = 30) -> str:
    ensure_dirs()
    p = os.path.join(DAILY_DIR, f"{report_id}.json")
    _write_json(p, data)
    prune_daily(keep_days=keep_days)
    return p


def prune_daily(keep_days: int = 30):
    ensure_dirs()
    ids = _list_ids(DAILY_DIR)
    if len(ids) <= keep_days:
        return
    for report_id in ids[keep_days:]:
        p = os.path.join(DAILY_DIR, f"{report_id}.json")
        try:
            os.remove(p)
        except Exception:
            pass


def _parse_date(date_id: str) -> Optional[datetime.date]:
    try:
        return datetime.date.fromisoformat(date_id)
    except Exception:
        return None


def _date_range_ids(start: datetime.date, end: datetime.date) -> List[str]:
    ids = []
    cur = start
    while cur <= end:
        ids.append(cur.isoformat())
        cur = cur + datetime.timedelta(days=1)
    return ids


def _iso_week_id(d: datetime.date) -> str:
    iso = d.isocalendar()
    try:
        year = iso.year
        week = iso.week
    except Exception:
        year = iso[0]
        week = iso[1]
    return f"{int(year)}-W{int(week):02d}"


def _month_id(d: datetime.date) -> str:
    return f"{d.year}-{d.month:02d}"


def iso_week_id(d: datetime.date) -> str:
    return _iso_week_id(d)


def month_id(d: datetime.date) -> str:
    return _month_id(d)


def save_weekly(week_id: str, data: Dict[str, Any]) -> str:
    ensure_dirs()
    p = os.path.join(WEEKLY_DIR, f"{week_id}.json")
    _write_json(p, data)
    return p


def save_monthly(month_id: str, data: Dict[str, Any]) -> str:
    ensure_dirs()
    p = os.path.join(MONTHLY_DIR, f"{month_id}.json")
    _write_json(p, data)
    return p


def load_daily_window(end_date_id: str, days: int) -> List[Tuple[str, Dict[str, Any]]]:
    end_date = _parse_date(end_date_id)
    if not end_date:
        return []
    start_date = end_date - datetime.timedelta(days=days - 1)
    out = []
    for d in _date_range_ids(start_date, end_date):
        data = load_report("daily", d)
        if data:
            out.append((d, data))
    return out
