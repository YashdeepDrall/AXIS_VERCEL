from copy import deepcopy

from app.core.config import AXIS_BANK_ID
from app.db.mongodb import users_collection

DEFAULT_ROLE = "investigator"
ROLE_LABELS = {
    "investigator": "Investigator",
    "supervisor": "Supervisor",
    "admin": "Admin",
    "auditor": "Auditor",
}
ROLE_PERMISSIONS = {
    "investigator": {
        "canChat": True,
        "canCreateChat": True,
        "canRenameChat": True,
        "canDeleteChat": True,
        "canManageMembers": False,
        "canGenerateReport": True,
        "canViewHistoricalCases": False,
        "canDownloadDocuments": True,
        "canManageUsers": False,
    },
    "supervisor": {
        "canChat": True,
        "canCreateChat": True,
        "canRenameChat": True,
        "canDeleteChat": True,
        "canManageMembers": True,
        "canGenerateReport": True,
        "canViewHistoricalCases": True,
        "canDownloadDocuments": True,
        "canManageUsers": False,
    },
    "admin": {
        "canChat": True,
        "canCreateChat": True,
        "canRenameChat": True,
        "canDeleteChat": True,
        "canManageMembers": True,
        "canGenerateReport": True,
        "canViewHistoricalCases": True,
        "canDownloadDocuments": True,
        "canManageUsers": True,
    },
    "auditor": {
        "canChat": False,
        "canCreateChat": False,
        "canRenameChat": False,
        "canDeleteChat": False,
        "canManageMembers": False,
        "canGenerateReport": False,
        "canViewHistoricalCases": True,
        "canDownloadDocuments": True,
        "canManageUsers": False,
    },
}


def _normalize_role(role_value: str | None) -> str:
    role = str(role_value or "").strip().lower()
    return role if role in ROLE_PERMISSIONS else DEFAULT_ROLE


def _resolve_supported_bank(user: dict) -> str:
    bank_id = (user or {}).get("bankId") or AXIS_BANK_ID

    if str(bank_id).strip().lower() != AXIS_BANK_ID:
        raise Exception("Only AXIS Bank users are supported in this application")

    return AXIS_BANK_ID


def get_role_permissions(role_value: str | None) -> dict:
    return deepcopy(ROLE_PERMISSIONS[_normalize_role(role_value)])


def _build_user_context(user: dict) -> dict:
    bank_id = _resolve_supported_bank(user)
    role = _normalize_role((user or {}).get("role"))
    user_id = str((user or {}).get("userId") or "").strip()
    display_name = str((user or {}).get("displayName") or user_id).strip() or user_id

    return {
        "userId": user_id,
        "displayName": display_name,
        "bankId": bank_id,
        "role": role,
        "roleLabel": ROLE_LABELS.get(role, ROLE_LABELS[DEFAULT_ROLE]),
        "permissions": get_role_permissions(role),
    }


def _build_workspace_user_summary(user: dict) -> dict:
    bank_id = _resolve_supported_bank(user)
    role = _normalize_role((user or {}).get("role"))
    user_id = str((user or {}).get("userId") or "").strip()
    display_name = str((user or {}).get("displayName") or user_id).strip() or user_id

    return {
        "userId": user_id,
        "displayName": display_name,
        "bankId": bank_id,
        "role": role,
        "roleLabel": ROLE_LABELS.get(role, ROLE_LABELS[DEFAULT_ROLE]),
    }


def list_workspace_users() -> list[dict]:
    users = []

    for user in users_collection.find({"bankId": AXIS_BANK_ID}):
        user_id = str((user or {}).get("userId") or "").strip()
        if not user_id:
            continue
        users.append(_build_workspace_user_summary(user))

    users.sort(key=lambda item: (item["displayName"].lower(), item["userId"].lower()))
    return users


def find_workspace_user(identifier: str | None) -> dict | None:
    needle = str(identifier or "").strip().lower()

    if not needle:
        return None

    for user in list_workspace_users():
        user_id = str(user.get("userId") or "").strip().lower()
        display_name = str(user.get("displayName") or "").strip().lower()

        if needle in {user_id, display_name}:
            return deepcopy(user)

    return None


def get_user_context(user_id: str) -> dict:
    user = users_collection.find_one({"userId": user_id})
    if not user:
        raise Exception(f"User {user_id} not found in database")
    return _build_user_context(user)


def verify_user(user_id: str):
    return get_user_context(user_id)["bankId"]


def verify_user_credentials(user_id: str, password: str):
    user = users_collection.find_one({"userId": user_id, "password": password})
    if not user:
        raise Exception("Invalid userId or password")
    return _build_user_context(user)


def require_permission(user_context: dict, permission_key: str, message: str | None = None):
    permissions = (user_context or {}).get("permissions") or {}

    if permissions.get(permission_key):
        return

    raise Exception(message or f"{(user_context or {}).get('roleLabel', 'This role')} does not have access to {permission_key}.")
