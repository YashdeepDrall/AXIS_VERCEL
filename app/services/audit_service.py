import json
import os
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from threading import Lock

from app.core.config import LOCAL_ACTIVITY_LOG_FILE


STORE_VERSION = 1
STORE_LOCK = Lock()


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _default_store():
    return {"version": STORE_VERSION, "events": []}


def _ensure_store_dir():
    directory = os.path.dirname(LOCAL_ACTIVITY_LOG_FILE)
    if directory:
        os.makedirs(directory, exist_ok=True)


def _load_store_unlocked():
    if not os.path.exists(LOCAL_ACTIVITY_LOG_FILE):
        return _default_store()

    try:
        with open(LOCAL_ACTIVITY_LOG_FILE, "r", encoding="utf-8") as store_file:
            payload = json.load(store_file)
    except Exception:
        return _default_store()

    if not isinstance(payload, dict):
        return _default_store()

    events = payload.get("events")
    if not isinstance(events, list):
        events = []

    return {"version": STORE_VERSION, "events": events}


def _save_store_unlocked(store):
    _ensure_store_dir()
    with open(LOCAL_ACTIVITY_LOG_FILE, "w", encoding="utf-8") as store_file:
        json.dump(store, store_file, ensure_ascii=True, indent=2)


def log_activity(action, actor=None, target_type="", target_id="", summary="", details=None):
    actor = actor if isinstance(actor, dict) else {}
    event = {
        "id": uuid.uuid4().hex,
        "timestamp": _now_iso(),
        "action": str(action or "").strip() or "activity",
        "actorUserId": str(actor.get("userId") or "").strip().lower(),
        "actorDisplayName": str(actor.get("displayName") or "").strip() or str(actor.get("userId") or "").strip(),
        "actorRole": str(actor.get("role") or "").strip().lower(),
        "targetType": str(target_type or "").strip(),
        "targetId": str(target_id or "").strip(),
        "summary": str(summary or "").strip(),
        "details": deepcopy(details) if isinstance(details, dict) else {},
    }

    with STORE_LOCK:
        store = _load_store_unlocked()
        events = store.get("events", [])
        events.insert(0, event)
        store["events"] = events
        _save_store_unlocked(store)

    return event


def list_activity_events(limit=None):
    with STORE_LOCK:
        store = _load_store_unlocked()

    events = [deepcopy(item) for item in store.get("events", [])]

    if limit is None:
        return events

    try:
        resolved_limit = int(limit)
    except Exception:
        resolved_limit = 50

    if resolved_limit <= 0:
        return []

    return events[:resolved_limit]
