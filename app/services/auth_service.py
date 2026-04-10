from app.core.config import AXIS_BANK_ID
from app.db.mongodb import users_collection


def _resolve_supported_bank(user: dict) -> str:
    bank_id = (user or {}).get("bankId") or AXIS_BANK_ID

    if str(bank_id).strip().lower() != AXIS_BANK_ID:
        raise Exception("Only AXIS Bank users are supported in this application")

    return AXIS_BANK_ID


def verify_user(user_id: str):
    user = users_collection.find_one({"userId": user_id})
    if not user:
        raise Exception(f"User {user_id} not found in database")
    return _resolve_supported_bank(user)


def verify_user_credentials(user_id: str, password: str):
    user = users_collection.find_one({"userId": user_id, "password": password})
    if not user:
        raise Exception("Invalid userId or password")
    return _resolve_supported_bank(user)
