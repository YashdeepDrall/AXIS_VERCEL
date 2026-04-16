import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from threading import Lock

from app.core.config import AXIS_BANK_ID, LOCAL_CONVERSATION_STORE_FILE


STORE_VERSION = 1
STORE_LOCK = Lock()


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _default_store():
    return {"version": STORE_VERSION, "conversations": []}


def _ensure_store_dir():
    directory = os.path.dirname(LOCAL_CONVERSATION_STORE_FILE)
    if directory:
        os.makedirs(directory, exist_ok=True)


def _load_store_unlocked():
    if not os.path.exists(LOCAL_CONVERSATION_STORE_FILE):
        return _default_store()

    try:
        with open(LOCAL_CONVERSATION_STORE_FILE, "r", encoding="utf-8") as store_file:
            payload = json.load(store_file)
    except Exception:
        return _default_store()

    if not isinstance(payload, dict):
        return _default_store()

    conversations = payload.get("conversations")

    if not isinstance(conversations, list):
        conversations = []

    return {
        "version": STORE_VERSION,
        "conversations": conversations,
    }


def _save_store_unlocked(store):
    _ensure_store_dir()
    with open(LOCAL_CONVERSATION_STORE_FILE, "w", encoding="utf-8") as store_file:
        json.dump(store, store_file, ensure_ascii=True, indent=2)


def _normalize_member(member):
    if not isinstance(member, dict):
        return None

    member_id = str(member.get("id") or "").strip().lower()
    member_name = str(member.get("name") or member_id).strip()

    if not member_id:
        return None

    normalized = {
        "id": member_id,
        "name": member_name or member_id,
    }

    color = str(member.get("color") or "").strip()
    if color:
        normalized["color"] = color

    return normalized


def _normalize_members(members, user_context, existing=None):
    normalized = []
    seen = set()

    for raw_member in members or []:
        member = _normalize_member(raw_member)
        if not member or member["id"] in seen:
            continue
        seen.add(member["id"])
        normalized.append(member)

    owner_id = str((existing or {}).get("ownerUserId") or user_context["userId"]).strip().lower()
    owner_name = str((existing or {}).get("ownerDisplayName") or user_context["displayName"]).strip() or owner_id

    if owner_id and owner_id not in seen:
        normalized.insert(0, {"id": owner_id, "name": owner_name})

    return normalized


def _normalize_chat_history(items):
    normalized = []

    for item in items or []:
        if not isinstance(item, dict):
            continue

        role = str(item.get("role") or "").strip()
        if not role:
            continue

        entry = {"role": role}

        if "content" in item and item.get("content") is not None:
            entry["content"] = str(item.get("content"))

        if role == "documents":
            documents = []
            for document in item.get("items") or []:
                if not isinstance(document, dict):
                    continue
                doc = {
                    "name": str(document.get("name") or "Document"),
                    "path": str(document.get("path") or ""),
                    "fileId": str(document.get("fileId") or ""),
                    "downloadUrl": str(document.get("downloadUrl") or ""),
                }
                documents.append(doc)
            entry["items"] = documents

        normalized.append(entry)

    return normalized


def _normalize_conversation_state(state):
    if not isinstance(state, dict):
        return {}

    return {
        "step": state.get("step"),
        "analysis": state.get("analysis") if isinstance(state.get("analysis"), dict) else {},
        "case_query": state.get("case_query"),
        "sessionId": state.get("sessionId"),
    }


def _conversation_member_ids(conversation):
    return {
        str(member.get("id") or "").strip().lower()
        for member in (conversation.get("members") or [])
        if isinstance(member, dict) and str(member.get("id") or "").strip()
    }


def _has_all_conversation_visibility(user_context):
    return str(user_context.get("role") or "").lower() in {"supervisor", "admin", "auditor"}


def can_access_conversation(user_context, conversation):
    if not conversation or conversation.get("bankId") != AXIS_BANK_ID:
        return False

    if _has_all_conversation_visibility(user_context):
        return True

    user_id = str(user_context.get("userId") or "").strip().lower()
    owner_id = str(conversation.get("ownerUserId") or "").strip().lower()

    if user_id and user_id == owner_id:
        return True

    return user_id in _conversation_member_ids(conversation)


def _can_modify_conversation(user_context, conversation):
    role = str(user_context.get("role") or "").lower()

    if role in {"supervisor", "admin"}:
        return True

    if role == "auditor":
        return False

    return can_access_conversation(user_context, conversation)


def _can_delete_conversation(user_context, conversation):
    role = str(user_context.get("role") or "").lower()

    if role in {"supervisor", "admin"}:
        return True

    if role == "auditor":
        return False

    owner_id = str(conversation.get("ownerUserId") or "").strip().lower()
    user_id = str(user_context.get("userId") or "").strip().lower()
    return bool(user_id and user_id == owner_id)


def _sort_key(conversation):
    return conversation.get("updatedAt") or conversation.get("createdAt") or ""


def list_conversations_for_user(user_context):
    with STORE_LOCK:
        store = _load_store_unlocked()

    conversations = [
        deepcopy(conversation)
        for conversation in store.get("conversations", [])
        if can_access_conversation(user_context, conversation)
    ]

    conversations.sort(key=_sort_key, reverse=True)
    return conversations


def _prepare_conversation_for_store(user_context, payload, existing=None):
    now = _now_iso()
    payload = payload if isinstance(payload, dict) else {}
    existing = existing if isinstance(existing, dict) else {}

    title = str(payload.get("title") or existing.get("title") or "New chat").strip() or "New chat"
    created_at = str(existing.get("createdAt") or payload.get("createdAt") or now)
    updated_at = str(payload.get("updatedAt") or now)

    return {
        "id": str(payload.get("id") or existing.get("id") or "").strip(),
        "title": title[:80],
        "bankId": AXIS_BANK_ID,
        "ownerUserId": str(existing.get("ownerUserId") or user_context["userId"]).strip().lower(),
        "ownerDisplayName": str(existing.get("ownerDisplayName") or user_context["displayName"]).strip() or user_context["displayName"],
        "createdBy": str(existing.get("createdBy") or user_context["userId"]).strip(),
        "createdAt": created_at,
        "updatedAt": updated_at,
        "members": _normalize_members(payload.get("members") or existing.get("members") or [], user_context, existing=existing),
        "chatHistory": _normalize_chat_history(payload.get("chatHistory") or []),
        "fraudCategory": str(payload.get("fraudCategory") or ""),
        "conversationState": _normalize_conversation_state(payload.get("conversationState") or existing.get("conversationState") or {}),
    }


def upsert_conversation_for_user(user_context, payload):
    if not (user_context.get("permissions") or {}).get("canCreateChat"):
        raise PermissionError("Your role does not have access to save conversation history.")

    payload = payload if isinstance(payload, dict) else {}
    conversation_id = str(payload.get("id") or "").strip()

    if not conversation_id:
        raise ValueError("conversation.id is required")

    with STORE_LOCK:
        store = _load_store_unlocked()
        conversations = store.get("conversations", [])
        existing = next((item for item in conversations if item.get("id") == conversation_id), None)

        if existing:
            if not can_access_conversation(user_context, existing):
                raise PermissionError("You do not have access to this conversation.")
            if not _can_modify_conversation(user_context, existing):
                raise PermissionError("Your role cannot change this conversation.")

        prepared = _prepare_conversation_for_store(user_context, payload, existing=existing)

        if existing:
            index = conversations.index(existing)
            conversations[index] = prepared
        else:
            conversations.append(prepared)

        store["conversations"] = conversations
        _save_store_unlocked(store)

    return deepcopy(prepared)


def delete_conversation_for_user(user_context, conversation_id):
    conversation_id = str(conversation_id or "").strip()

    if not conversation_id:
        raise ValueError("conversation_id is required")

    with STORE_LOCK:
        store = _load_store_unlocked()
        conversations = store.get("conversations", [])
        existing = next((item for item in conversations if item.get("id") == conversation_id), None)

        if not existing:
            return False

        if not can_access_conversation(user_context, existing):
            raise PermissionError("You do not have access to this conversation.")

        if not _can_delete_conversation(user_context, existing):
            raise PermissionError("Your role cannot delete this conversation.")

        store["conversations"] = [item for item in conversations if item.get("id") != conversation_id]
        _save_store_unlocked(store)

    return True
