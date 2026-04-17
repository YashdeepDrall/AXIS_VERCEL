import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from threading import Lock

from app.core.config import AXIS_BANK_ID, AXIS_BANK_NAME, LOCAL_USER_STORE_FILE


STORE_VERSION = 1
STORE_LOCK = Lock()
VALID_ROLES = {"investigator", "supervisor", "admin", "auditor"}


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _default_seed_users():
    default_password = os.getenv("AXIS_DEFAULT_PASSWORD", "0000")
    return [
        {
            "userId": os.getenv("AXIS_DEFAULT_USER_ID", "axis001"),
            "password": default_password,
            "displayName": os.getenv("AXIS_DEFAULT_DISPLAY_NAME", "Axis Investigator"),
            "bankId": AXIS_BANK_ID,
            "bankName": AXIS_BANK_NAME,
            "role": "investigator",
            "active": True,
        },
        {
            "userId": os.getenv("AXIS_SUPERVISOR_USER_ID", "axis_supervisor"),
            "password": os.getenv("AXIS_SUPERVISOR_PASSWORD", default_password),
            "displayName": os.getenv("AXIS_SUPERVISOR_DISPLAY_NAME", "Axis Supervisor"),
            "bankId": AXIS_BANK_ID,
            "bankName": AXIS_BANK_NAME,
            "role": "supervisor",
            "active": True,
        },
        {
            "userId": os.getenv("AXIS_ADMIN_USER_ID", "axis_admin"),
            "password": os.getenv("AXIS_ADMIN_PASSWORD", default_password),
            "displayName": os.getenv("AXIS_ADMIN_DISPLAY_NAME", "Axis Admin"),
            "bankId": AXIS_BANK_ID,
            "bankName": AXIS_BANK_NAME,
            "role": "admin",
            "active": True,
        },
        {
            "userId": os.getenv("AXIS_AUDITOR_USER_ID", "axis_auditor"),
            "password": os.getenv("AXIS_AUDITOR_PASSWORD", default_password),
            "displayName": os.getenv("AXIS_AUDITOR_DISPLAY_NAME", "Axis Auditor"),
            "bankId": AXIS_BANK_ID,
            "bankName": AXIS_BANK_NAME,
            "role": "auditor",
            "active": True,
        },
    ]


def _ensure_store_dir():
    directory = os.path.dirname(LOCAL_USER_STORE_FILE)
    if directory:
        os.makedirs(directory, exist_ok=True)


def _normalize_user(raw_user, created_at=None):
    if not isinstance(raw_user, dict):
        return None

    user_id = str(raw_user.get("userId") or "").strip()
    if not user_id:
        return None

    role = str(raw_user.get("role") or "investigator").strip().lower()
    if role not in VALID_ROLES:
        role = "investigator"

    now = _now_iso()
    return {
        "userId": user_id,
        "password": str(raw_user.get("password") or "").strip(),
        "displayName": str(raw_user.get("displayName") or user_id).strip() or user_id,
        "bankId": AXIS_BANK_ID,
        "bankName": AXIS_BANK_NAME,
        "role": role,
        "active": bool(raw_user.get("active", True)),
        "createdAt": str(raw_user.get("createdAt") or created_at or now),
        "updatedAt": str(raw_user.get("updatedAt") or now),
    }


def _default_store():
    seed_time = _now_iso()
    users = []

    for raw_user in _default_seed_users():
        normalized = _normalize_user(raw_user, created_at=seed_time)
        if normalized:
            users.append(normalized)

    return {"version": STORE_VERSION, "users": users}


def _load_store_unlocked():
    if not os.path.exists(LOCAL_USER_STORE_FILE):
        return _default_store()

    try:
        with open(LOCAL_USER_STORE_FILE, "r", encoding="utf-8") as store_file:
            payload = json.load(store_file)
    except Exception:
        return _default_store()

    if not isinstance(payload, dict):
        return _default_store()

    users = []
    seen = set()

    for raw_user in payload.get("users") or []:
        normalized = _normalize_user(raw_user)
        if not normalized:
            continue
        user_key = normalized["userId"].lower()
        if user_key in seen:
            continue
        seen.add(user_key)
        users.append(normalized)

    existing_ids = {user["userId"].lower() for user in users}
    for raw_user in _default_seed_users():
        normalized = _normalize_user(raw_user)
        if not normalized:
            continue
        user_key = normalized["userId"].lower()
        if user_key in existing_ids:
            continue
        users.append(normalized)

    return {"version": STORE_VERSION, "users": users}


def _save_store_unlocked(store):
    _ensure_store_dir()
    with open(LOCAL_USER_STORE_FILE, "w", encoding="utf-8") as store_file:
        json.dump(store, store_file, ensure_ascii=True, indent=2)


def list_workspace_user_records(include_inactive=False):
    with STORE_LOCK:
        store = _load_store_unlocked()

    users = []
    for user in store.get("users", []):
        if user.get("bankId") != AXIS_BANK_ID:
            continue
        if not include_inactive and not user.get("active", True):
            continue
        users.append(deepcopy(user))

    users.sort(key=lambda item: (item["displayName"].lower(), item["userId"].lower()))
    return users


def get_workspace_user(user_id, include_inactive=False):
    needle = str(user_id or "").strip().lower()
    if not needle:
        return None

    for user in list_workspace_user_records(include_inactive=True):
        if str(user.get("userId") or "").strip().lower() != needle:
            continue
        if not include_inactive and not user.get("active", True):
            return None
        return deepcopy(user)

    return None


def find_workspace_user(identifier, include_inactive=False):
    needle = str(identifier or "").strip().lower()
    if not needle:
        return None

    for user in list_workspace_user_records(include_inactive=include_inactive):
        user_id = str(user.get("userId") or "").strip().lower()
        display_name = str(user.get("displayName") or "").strip().lower()
        if needle in {user_id, display_name}:
            return deepcopy(user)

    return None


def verify_workspace_user_credentials(user_id, password):
    user = get_workspace_user(user_id, include_inactive=False)
    if not user or str(user.get("password") or "") != str(password or ""):
        raise Exception("Invalid userId or password")
    return user


def create_workspace_user(user_id, password, display_name, role):
    normalized_role = str(role or "investigator").strip().lower()
    if normalized_role not in VALID_ROLES:
        raise ValueError("Invalid role")

    normalized_user_id = str(user_id or "").strip()
    normalized_password = str(password or "").strip()
    normalized_display_name = str(display_name or normalized_user_id).strip() or normalized_user_id

    if not normalized_user_id or not normalized_password:
        raise ValueError("userId and password are required")

    with STORE_LOCK:
        store = _load_store_unlocked()
        users = store.get("users", [])

        if any(str(item.get("userId") or "").strip().lower() == normalized_user_id.lower() for item in users):
            raise ValueError("User ID already exists")

        created = _normalize_user(
            {
                "userId": normalized_user_id,
                "password": normalized_password,
                "displayName": normalized_display_name,
                "role": normalized_role,
                "active": True,
            }
        )
        users.append(created)
        store["users"] = users
        _save_store_unlocked(store)

    return deepcopy(created)


def update_workspace_user(target_user_id, updates, actor_user_id=None):
    normalized_target_id = str(target_user_id or "").strip()
    if not normalized_target_id:
        raise ValueError("target_user_id is required")

    updates = updates if isinstance(updates, dict) else {}

    with STORE_LOCK:
        store = _load_store_unlocked()
        users = store.get("users", [])
        existing = next(
            (item for item in users if str(item.get("userId") or "").strip().lower() == normalized_target_id.lower()),
            None,
        )

        if not existing:
            raise ValueError("User not found")

        actor_id = str(actor_user_id or "").strip().lower()
        target_id = str(existing.get("userId") or "").strip().lower()

        if actor_id and actor_id == target_id:
            if "active" in updates and not bool(updates.get("active")):
                raise ValueError("You cannot deactivate your own account.")
            if "role" in updates and str(updates.get("role") or "").strip().lower() != str(existing.get("role") or "").strip().lower():
                raise ValueError("You cannot change your own role from this panel.")

        updated = deepcopy(existing)

        if "displayName" in updates:
            display_name = str(updates.get("displayName") or "").strip()
            if not display_name:
                raise ValueError("displayName is required")
            updated["displayName"] = display_name

        if "password" in updates and str(updates.get("password") or "").strip():
            updated["password"] = str(updates.get("password") or "").strip()

        if "role" in updates:
            role = str(updates.get("role") or "").strip().lower()
            if role not in VALID_ROLES:
                raise ValueError("Invalid role")
            updated["role"] = role

        if "active" in updates:
            updated["active"] = bool(updates.get("active"))

        updated["updatedAt"] = _now_iso()

        users[users.index(existing)] = _normalize_user(updated, created_at=existing.get("createdAt"))
        store["users"] = users
        _save_store_unlocked(store)

    return get_workspace_user(normalized_target_id, include_inactive=True)
