import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from threading import Lock

from app.core.config import AXIS_BANK_ID, LOCAL_CONVERSATION_STORE_FILE
from app.services.audit_service import log_activity
from app.services.auth_service import find_workspace_user


STORE_VERSION = 1
STORE_LOCK = Lock()
MEMBER_COLORS = ["#a50034", "#0f766e", "#3454d1", "#996c00"]
CASE_STATUS_OPTIONS = {"open", "under review", "escalated", "closed"}


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


def _member_color(value):
    total = sum(ord(char) for char in str(value or ""))
    return MEMBER_COLORS[total % len(MEMBER_COLORS)]


def _normalize_member(member, strict=True):
    if not isinstance(member, dict):
        return None

    candidates = [
        member.get("id"),
        member.get("userId"),
        member.get("name"),
        member.get("displayName"),
    ]
    user_summary = None

    for candidate in candidates:
        user_summary = find_workspace_user(candidate, include_inactive=not strict)
        if user_summary:
            break

    if not user_summary:
        if strict:
            raise ValueError("Only valid AXIS workspace users can be added as conversation members.")
        return None

    member_id = str(user_summary.get("userId") or "").strip().lower()
    member_name = str(user_summary.get("displayName") or member_id).strip() or member_id

    normalized = {
        "id": member_id,
        "name": member_name,
        "role": str(user_summary.get("role") or "").strip().lower(),
        "roleLabel": str(user_summary.get("roleLabel") or "").strip() or "Investigator",
    }

    color = str(member.get("color") or "").strip() or _member_color(member_id)
    normalized["color"] = color

    return normalized


def _owner_summary(existing, user_context):
    owner_id = str((existing or {}).get("ownerUserId") or user_context["userId"]).strip().lower()
    owner = find_workspace_user(owner_id, include_inactive=True)

    if owner:
        return owner

    return {
        "userId": owner_id or str(user_context.get("userId") or "").strip().lower(),
        "displayName": str((existing or {}).get("ownerDisplayName") or user_context.get("displayName") or owner_id).strip() or owner_id,
        "role": str((existing or {}).get("ownerRole") or user_context.get("role") or "").strip().lower(),
        "roleLabel": str((existing or {}).get("ownerRoleLabel") or user_context.get("roleLabel") or "Investigator").strip() or "Investigator",
    }


def _normalize_members(members, user_context, existing=None, strict=True):
    normalized = []
    seen = set()

    for raw_member in members or []:
        member = _normalize_member(raw_member, strict=strict)
        if not member or member["id"] in seen:
            continue
        seen.add(member["id"])
        normalized.append(member)

    owner = _owner_summary(existing, user_context)
    owner_id = str(owner.get("userId") or "").strip().lower()
    owner_name = str(owner.get("displayName") or owner_id).strip() or owner_id

    if owner_id and owner_id not in seen:
        normalized.insert(
            0,
            {
                "id": owner_id,
                "name": owner_name,
                "role": str(owner.get("role") or "").strip().lower(),
                "roleLabel": str(owner.get("roleLabel") or "").strip() or "Investigator",
                "color": _member_color(owner_id),
            },
        )

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

        if "title" in item and item.get("title") is not None:
            entry["title"] = str(item.get("title"))

        if role == "documents":
            documents = []
            for document in item.get("items") or []:
                if not isinstance(document, dict):
                    continue
                doc = deepcopy(document)
                doc["name"] = str(document.get("name") or "Document")
                doc["path"] = ""
                doc["fileId"] = str(document.get("fileId") or "")
                doc["downloadUrl"] = str(document.get("downloadUrl") or "")
                doc["suspiciousIndicators"] = [str(item).strip() for item in document.get("suspiciousIndicators") or [] if str(item).strip()]
                doc["investigationHighlights"] = [str(item).strip() for item in document.get("investigationHighlights") or [] if str(item).strip()]
                documents.append(doc)
            entry["items"] = documents
        elif role == "analysis" and isinstance(item.get("analysis"), dict):
            entry["analysis"] = deepcopy(item.get("analysis"))

        normalized.append(entry)

    return normalized


def _normalize_conversation_state(state):
    if not isinstance(state, dict):
        return {}

    return deepcopy(state)


def _normalize_case_status(value):
    normalized = str(value or "").strip().lower()
    if normalized not in CASE_STATUS_OPTIONS:
        return "Open"
    return normalized.title() if normalized != "under review" else "Under Review"


def _normalize_investigator_notes(value):
    if value is None:
        return ""

    lines = [line.rstrip() for line in str(value).replace("\r\n", "\n").replace("\r", "\n").split("\n")]
    normalized = "\n".join(lines).strip()
    return normalized[:6000]


def _conversation_member_ids(conversation):
    return {
        str(member.get("id") or "").strip().lower()
        for member in (conversation.get("members") or [])
        if isinstance(member, dict) and str(member.get("id") or "").strip()
    }


def _has_all_conversation_visibility(user_context):
    return str(user_context.get("role") or "").lower() in {"supervisor", "admin", "auditor"}


def _is_owner(user_context, conversation):
    user_id = str(user_context.get("userId") or "").strip().lower()
    owner_id = str(conversation.get("ownerUserId") or "").strip().lower()
    return bool(user_id and user_id == owner_id)


def can_access_conversation(user_context, conversation):
    if not conversation or conversation.get("bankId") != AXIS_BANK_ID:
        return False

    if _has_all_conversation_visibility(user_context):
        return True

    if _is_owner(user_context, conversation):
        return True

    user_id = str(user_context.get("userId") or "").strip().lower()
    return user_id in _conversation_member_ids(conversation)


def _can_update_conversation_content(user_context, conversation):
    role = str(user_context.get("role") or "").lower()
    permissions = user_context.get("permissions") or {}

    if role == "auditor" or not permissions.get("canCreateChat"):
        return False

    return can_access_conversation(user_context, conversation)


def _can_rename_conversation(user_context, conversation):
    role = str(user_context.get("role") or "").lower()
    permissions = user_context.get("permissions") or {}

    if not permissions.get("canRenameChat"):
        return False

    if role in {"supervisor", "admin"}:
        return True

    if role == "auditor":
        return False

    return _is_owner(user_context, conversation)


def _can_manage_conversation_members(user_context, conversation):
    role = str(user_context.get("role") or "").lower()
    permissions = user_context.get("permissions") or {}

    if not permissions.get("canManageMembers"):
        return False

    if role in {"supervisor", "admin"}:
        return can_access_conversation(user_context, conversation)

    return False


def _can_delete_conversation(user_context, conversation):
    role = str(user_context.get("role") or "").lower()
    permissions = user_context.get("permissions") or {}

    if not permissions.get("canDeleteChat"):
        return False

    if role in {"supervisor", "admin"}:
        return True

    if role == "auditor":
        return False

    return _is_owner(user_context, conversation)


def _conversation_capabilities(user_context, conversation):
    permissions = user_context.get("permissions") or {}
    is_owner = _is_owner(user_context, conversation)
    user_id = str(user_context.get("userId") or "").strip().lower()
    is_shared = bool(user_id and user_id in _conversation_member_ids(conversation) and not is_owner)

    return {
        "canChat": bool(permissions.get("canChat") and can_access_conversation(user_context, conversation)),
        "canRename": _can_rename_conversation(user_context, conversation),
        "canDelete": _can_delete_conversation(user_context, conversation),
        "canManageMembers": _can_manage_conversation_members(user_context, conversation),
        "isOwner": is_owner,
        "isShared": is_shared,
        "isReadOnly": not bool(permissions.get("canChat")),
    }


def _enrich_conversation_for_user(user_context, conversation):
    enriched = deepcopy(conversation)
    owner = _owner_summary(enriched, user_context)

    enriched["ownerUserId"] = str(owner.get("userId") or enriched.get("ownerUserId") or "").strip().lower()
    enriched["ownerDisplayName"] = str(owner.get("displayName") or enriched.get("ownerDisplayName") or enriched["ownerUserId"]).strip() or enriched["ownerUserId"]
    enriched["ownerRole"] = str(owner.get("role") or enriched.get("ownerRole") or "").strip().lower()
    enriched["ownerRoleLabel"] = str(owner.get("roleLabel") or enriched.get("ownerRoleLabel") or "Investigator").strip() or "Investigator"
    enriched["members"] = _normalize_members(enriched.get("members") or [], user_context, existing=enriched, strict=False)
    enriched["capabilities"] = _conversation_capabilities(user_context, enriched)
    return enriched


def _sort_key(conversation):
    return conversation.get("updatedAt") or conversation.get("createdAt") or ""


def _member_id_list(conversation):
    return sorted(_conversation_member_ids(conversation))


def list_conversations_for_user(user_context):
    with STORE_LOCK:
        store = _load_store_unlocked()

    conversations = [
        _enrich_conversation_for_user(user_context, conversation)
        for conversation in store.get("conversations", [])
        if can_access_conversation(user_context, conversation)
    ]

    conversations.sort(key=_sort_key, reverse=True)
    return conversations


def _prepare_conversation_for_store(user_context, payload, existing=None):
    now = _now_iso()
    payload = payload if isinstance(payload, dict) else {}
    existing = existing if isinstance(existing, dict) else {}
    owner = _owner_summary(existing, user_context)
    can_rename = not existing or _can_rename_conversation(user_context, existing)
    can_manage_members = not existing or _can_manage_conversation_members(user_context, existing)
    can_update_content = not existing or _can_update_conversation_content(user_context, existing)

    title_source = payload.get("title") if can_rename else existing.get("title")
    title = str(title_source or existing.get("title") or "New chat").strip() or "New chat"
    created_at = str(existing.get("createdAt") or payload.get("createdAt") or now)
    updated_at = str(payload.get("updatedAt") or now)
    members_source = payload.get("members") if can_manage_members else existing.get("members")
    chat_history_source = payload.get("chatHistory") if can_update_content else existing.get("chatHistory")
    fraud_category_source = payload.get("fraudCategory") if can_update_content else existing.get("fraudCategory")
    case_status_source = payload.get("caseStatus") if can_update_content else existing.get("caseStatus")
    notes_source = payload.get("investigatorNotes") if can_update_content else existing.get("investigatorNotes")
    conversation_state_source = payload.get("conversationState") if can_update_content else existing.get("conversationState")
    workflow_mode_source = payload.get("workflowMode") if can_update_content else existing.get("workflowMode")

    existing_case_status = _normalize_case_status(existing.get("caseStatus"))
    prepared_case_status = _normalize_case_status(case_status_source or existing.get("caseStatus"))
    existing_notes = _normalize_investigator_notes(existing.get("investigatorNotes"))
    prepared_notes = _normalize_investigator_notes(notes_source if notes_source is not None else existing.get("investigatorNotes"))

    case_status_updated_at = str(existing.get("caseStatusUpdatedAt") or payload.get("caseStatusUpdatedAt") or created_at or now)
    if not existing or prepared_case_status != existing_case_status:
        case_status_updated_at = now

    notes_updated_at = str(existing.get("notesUpdatedAt") or payload.get("notesUpdatedAt") or "")
    if prepared_notes:
        if not existing_notes or prepared_notes != existing_notes:
            notes_updated_at = now
        elif not notes_updated_at:
            notes_updated_at = created_at or now
    else:
        notes_updated_at = ""

    return {
        "id": str(payload.get("id") or existing.get("id") or "").strip(),
        "title": title[:80],
        "bankId": AXIS_BANK_ID,
        "ownerUserId": str(owner.get("userId") or user_context["userId"]).strip().lower(),
        "ownerDisplayName": str(owner.get("displayName") or user_context["displayName"]).strip() or user_context["displayName"],
        "ownerRole": str(owner.get("role") or user_context.get("role") or "").strip().lower(),
        "ownerRoleLabel": str(owner.get("roleLabel") or user_context.get("roleLabel") or "Investigator").strip() or "Investigator",
        "createdBy": str(existing.get("createdBy") or user_context["userId"]).strip().lower(),
        "createdAt": created_at,
        "updatedAt": updated_at,
        "members": _normalize_members(members_source or existing.get("members") or [], user_context, existing=existing),
        "chatHistory": _normalize_chat_history(chat_history_source or existing.get("chatHistory") or []),
        "fraudCategory": str(fraud_category_source or existing.get("fraudCategory") or ""),
        "caseStatus": prepared_case_status,
        "caseStatusUpdatedAt": case_status_updated_at,
        "investigatorNotes": prepared_notes,
        "notesUpdatedAt": notes_updated_at,
        "conversationState": _normalize_conversation_state(conversation_state_source or existing.get("conversationState") or {}),
        "workflowMode": str(workflow_mode_source or existing.get("workflowMode") or "blueprint").strip() or "blueprint",
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
            if not _can_update_conversation_content(user_context, existing):
                raise PermissionError("Your role cannot change this conversation.")

        prepared = _prepare_conversation_for_store(user_context, payload, existing=existing)

        if existing:
            index = conversations.index(existing)
            conversations[index] = prepared
        else:
            conversations.append(prepared)

        store["conversations"] = conversations
        _save_store_unlocked(store)

    if not existing:
        log_activity(
            "conversation_created",
            actor=user_context,
            target_type="conversation",
            target_id=prepared["id"],
            summary=f'{user_context.get("displayName") or user_context.get("userId")} created "{prepared["title"]}".',
            details={
                "title": prepared["title"],
                "ownerUserId": prepared.get("ownerUserId"),
                "relatedUserIds": _member_id_list(prepared),
            },
        )
    else:
        existing_title = str(existing.get("title") or "").strip()
        prepared_title = str(prepared.get("title") or "").strip()
        if existing_title != prepared_title:
            log_activity(
                "conversation_renamed",
                actor=user_context,
                target_type="conversation",
                target_id=prepared["id"],
                summary=f'{user_context.get("displayName") or user_context.get("userId")} renamed a chat to "{prepared_title}".',
                details={
                    "before": existing_title,
                    "after": prepared_title,
                    "ownerUserId": prepared.get("ownerUserId"),
                    "relatedUserIds": _member_id_list(prepared),
                },
            )

        existing_members = _member_id_list(existing)
        prepared_members = _member_id_list(prepared)
        added_members = [member for member in prepared_members if member not in existing_members]
        removed_members = [member for member in existing_members if member not in prepared_members]

        if added_members or removed_members:
            change_bits = []
            if added_members:
                change_bits.append(f"added {', '.join(added_members)}")
            if removed_members:
                change_bits.append(f"removed {', '.join(removed_members)}")
            log_activity(
                "conversation_members_updated",
                actor=user_context,
                target_type="conversation",
                target_id=prepared["id"],
                summary=f'{user_context.get("displayName") or user_context.get("userId")} {", ".join(change_bits)} on "{prepared_title}".',
                details={
                    "addedMembers": added_members,
                    "removedMembers": removed_members,
                    "ownerUserId": prepared.get("ownerUserId"),
                    "relatedUserIds": _member_id_list(prepared),
                },
            )

        existing_case_status = _normalize_case_status(existing.get("caseStatus"))
        prepared_case_status = _normalize_case_status(prepared.get("caseStatus"))
        if existing_case_status != prepared_case_status:
            log_activity(
                "conversation_case_status_updated",
                actor=user_context,
                target_type="conversation",
                target_id=prepared["id"],
                summary=f'{user_context.get("displayName") or user_context.get("userId")} changed case status to "{prepared_case_status}" on "{prepared_title}".',
                details={
                    "before": existing_case_status,
                    "after": prepared_case_status,
                    "ownerUserId": prepared.get("ownerUserId"),
                    "relatedUserIds": _member_id_list(prepared),
                },
            )

        existing_notes = _normalize_investigator_notes(existing.get("investigatorNotes"))
        prepared_notes = _normalize_investigator_notes(prepared.get("investigatorNotes"))
        if existing_notes != prepared_notes:
            note_summary = "saved investigator notes" if prepared_notes else "cleared investigator notes"
            log_activity(
                "conversation_notes_updated",
                actor=user_context,
                target_type="conversation",
                target_id=prepared["id"],
                summary=f'{user_context.get("displayName") or user_context.get("userId")} {note_summary} on "{prepared_title}".',
                details={
                    "notesLength": len(prepared_notes),
                    "ownerUserId": prepared.get("ownerUserId"),
                    "relatedUserIds": _member_id_list(prepared),
                },
            )

    return _enrich_conversation_for_user(user_context, prepared)


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

    log_activity(
        "conversation_deleted",
        actor=user_context,
        target_type="conversation",
        target_id=conversation_id,
        summary=f'{user_context.get("displayName") or user_context.get("userId")} deleted "{existing.get("title") or "chat"}".',
        details={
            "title": existing.get("title") or "",
            "ownerUserId": existing.get("ownerUserId") or "",
            "relatedUserIds": _member_id_list(existing),
        },
    )

    return True
