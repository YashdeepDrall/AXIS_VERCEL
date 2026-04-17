import datetime
import os
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.core.config import AXIS_BANK_DIR, AXIS_BANK_ID, AXIS_BANK_NAME, AXIS_BLUEPRINT_FILE, BASE_DIR
from app.db.mongodb import cases_collection, chat_logs_collection, documents_collection, fs
from app.services.audit_service import list_activity_events, log_activity
from app.services.conversation_service import (
    delete_conversation_for_user,
    list_conversations_for_user,
    upsert_conversation_for_user,
)
from app.services.auth_service import get_user_context, list_workspace_users, require_permission, verify_user_credentials
from app.services.fraud_service import detect_fraud, generate_investigation_report
from app.services.user_service import create_workspace_user, update_workspace_user


router = APIRouter()


class ConversationState(BaseModel):
    step: str | None = None
    analysis: dict[str, Any] = Field(default_factory=dict)
    case_query: str | None = None
    sessionId: str | None = None


class ChatTurnRequest(BaseModel):
    userId: str
    query: str
    state: ConversationState | None = None


class ConversationMemberPayload(BaseModel):
    id: str
    name: str | None = None
    color: str | None = None


class ConversationHistoryItemPayload(BaseModel):
    role: str
    content: str | None = None
    items: list[dict[str, Any]] = Field(default_factory=list)


class ConversationPayload(BaseModel):
    id: str
    title: str = "New chat"
    updatedAt: str | None = None
    createdAt: str | None = None
    ownerUserId: str | None = None
    createdBy: str | None = None
    members: list[ConversationMemberPayload] = Field(default_factory=list)
    chatHistory: list[ConversationHistoryItemPayload] = Field(default_factory=list)
    fraudCategory: str | None = None
    conversationState: dict[str, Any] = Field(default_factory=dict)


class ConversationSyncRequest(BaseModel):
    userId: str
    conversation: ConversationPayload


class WorkspaceUserCreateRequest(BaseModel):
    userId: str
    password: str
    displayName: str
    role: str


class WorkspaceUserUpdateRequest(BaseModel):
    displayName: str | None = None
    password: str | None = None
    role: str | None = None
    active: bool | None = None


def get_last_chat(user_id, session_id):
    return chat_logs_collection.find_one(
        {"userId": user_id, "sessionId": session_id},
        sort=[("timestamp", -1)],
    )


def save_chat_log(user_id, bank_id, role, session_id, user_input, bot_output, step, analysis, case_query):
    chat_logs_collection.insert_one(
        {
            "userId": user_id,
            "bankId": bank_id,
            "role": role,
            "sessionId": session_id,
            "user_input": user_input,
            "bot_output": bot_output,
            "step": step,
            "analysis": analysis,
            "case_query": case_query,
            "timestamp": datetime.datetime.utcnow(),
        }
    )


def format_analysis(analysis):
    if not isinstance(analysis, dict):
        return str(analysis)

    indicators = analysis.get("suspicious_indicators") or []
    indicators_text = ", ".join(item.strip() for item in indicators if str(item).strip()) or "N/A"

    return (
        "Fraud Detection Result:\n"
        f"- Fraud Category: {analysis.get('fraud_category', 'Unknown')}\n"
        f"- Fraud Classification: {analysis.get('fraud_classification', 'Manual review required')}\n"
        f"- Risk Level: {analysis.get('risk_level', 'Medium')}\n"
        f"- Suspicious Indicators: {indicators_text}\n"
        f"- Relevant Information: {analysis.get('relevant_information', 'N/A')}\n"
        f"- Recommended Action: {analysis.get('recommended_action', 'Review the SOP guidance manually.')}"
    )


def fetch_relevant_documents(bank_id):
    if bank_id != AXIS_BANK_ID:
        return []

    target_path = os.path.join(AXIS_BANK_DIR, AXIS_BLUEPRINT_FILE)
    docs = list(
        documents_collection.find(
            {
                "bankId": bank_id,
                "fileName": AXIS_BLUEPRINT_FILE,
                "$or": [
                    {"filePath": {"$exists": True}},
                    {"isPDF": True},
                    {"documentType": "SOP"},
                ],
            }
        )
    )

    if not docs and os.path.exists(target_path):
        grid_file = fs.find_one({"filename": AXIS_BLUEPRINT_FILE, "bankId": bank_id})
        return [
            {
                "name": AXIS_BLUEPRINT_FILE,
                "path": target_path,
                "fileId": str(grid_file._id) if grid_file else "",
                "downloadUrl": f"/documents/{grid_file._id}" if grid_file else "",
            }
        ]

    results = []

    for doc in docs:
        file_name = doc.get("fileName", "")
        file_path = doc.get("filePath", "")
        file_id = doc.get("fileId", "")

        if file_path and not os.path.isabs(file_path):
            file_path = os.path.abspath(os.path.join(BASE_DIR, file_path))

        if not file_path and file_name:
            candidate_path = os.path.join(AXIS_BANK_DIR, file_name)
            if os.path.exists(candidate_path):
                file_path = candidate_path

        if not file_id and file_name:
            grid_file = fs.find_one({"filename": file_name, "bankId": bank_id})
            if grid_file:
                file_id = str(grid_file._id)

        results.append(
            {
                "name": file_name,
                "path": file_path,
                "fileId": file_id,
                "downloadUrl": f"/documents/{file_id}" if file_id else "",
            }
        )

    return results


@router.get("/documents/{file_id}")
def download_document(file_id: str):
    try:
        grid_out = fs.get(file_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Document not found")

    filename = grid_out.filename or "document.pdf"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(grid_out, media_type="application/pdf", headers=headers)


def fetch_historical_docs():
    docs = list(cases_collection.find({}))

    return [
        {
            "name": doc.get("fileName"),
            "path": doc.get("filePath", ""),
        }
        for doc in docs
    ]


def _normalize_choice(text):
    text = (text or "").strip().lower()
    if not text:
        return None

    head = text.split()[0]
    yes_set = {"yes", "y", "yeah", "yep", "sure", "ok", "okay"}
    no_set = {"no", "n", "nope", "nah", "nothing"}

    if head in yes_set:
        return "yes"
    if head in no_set:
        return "no"
    return None


def _can_view_workspace_directory(user_context: dict) -> bool:
    role = str((user_context or {}).get("role") or "").lower()
    permissions = (user_context or {}).get("permissions") or {}
    return bool(role in {"supervisor", "admin", "auditor"} or permissions.get("canManageMembers") or permissions.get("canManageUsers"))


def _can_view_activity_log(user_context: dict) -> bool:
    return bool((user_context or {}).get("userId"))


def _activity_event_visible_to_user(user_context: dict, event: dict) -> bool:
    role = str((user_context or {}).get("role") or "").lower()
    user_id = str((user_context or {}).get("userId") or "").strip().lower()

    if role in {"supervisor", "admin", "auditor"}:
        return True

    if not user_id or not isinstance(event, dict):
        return False

    if str(event.get("actorUserId") or "").strip().lower() == user_id:
        return True

    if str(event.get("targetType") or "").strip().lower() == "user" and str(event.get("targetId") or "").strip().lower() == user_id:
        return True

    details = event.get("details") if isinstance(event.get("details"), dict) else {}
    if str(details.get("ownerUserId") or "").strip().lower() == user_id:
        return True

    related_users = {
        str(value or "").strip().lower()
        for value in (details.get("relatedUserIds") or [])
        if str(value or "").strip()
    }
    return user_id in related_users


def _activity_events_for_user(user_context: dict, limit: int | None = None) -> list[dict]:
    visible_events = [
        event
        for event in list_activity_events(limit=None)
        if _activity_event_visible_to_user(user_context, event)
    ]

    if limit is None:
        return visible_events

    return visible_events[: max(0, int(limit))]


@router.get("/conversations")
def list_conversations(userId: str):
    try:
        user_context = get_user_context(userId.strip())
    except Exception as exc:
        raise HTTPException(status_code=401, detail=str(exc))

    conversations = list_conversations_for_user(user_context)
    return {"conversations": conversations}


@router.get("/workspace-users")
def workspace_users(userId: str):
    try:
        user_context = get_user_context(userId.strip())
    except Exception as exc:
        raise HTTPException(status_code=401, detail=str(exc))

    if not _can_view_workspace_directory(user_context):
        raise HTTPException(status_code=403, detail="Your role cannot view the AXIS workspace directory.")

    return {"users": list_workspace_users()}


@router.post("/workspace-users")
def create_user(request: WorkspaceUserCreateRequest, userId: str):
    try:
        user_context = get_user_context(userId.strip())
    except Exception as exc:
        raise HTTPException(status_code=401, detail=str(exc))

    try:
        require_permission(user_context, "canManageUsers", "Only admins can create workspace users.")
    except Exception as exc:
        raise HTTPException(status_code=403, detail=str(exc))

    try:
        created_user = create_workspace_user(
            user_id=request.userId,
            password=request.password,
            display_name=request.displayName,
            role=request.role,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    log_activity(
        "user_created",
        actor=user_context,
        target_type="user",
        target_id=created_user["userId"],
        summary=f'{user_context.get("displayName") or user_context.get("userId")} created user {created_user["userId"]}.',
        details={
            "role": created_user.get("role"),
            "displayName": created_user.get("displayName"),
            "relatedUserIds": [created_user["userId"]],
        },
    )
    return {"user": created_user}


@router.patch("/workspace-users/{target_user_id}")
def patch_user(target_user_id: str, request: WorkspaceUserUpdateRequest, userId: str):
    try:
        user_context = get_user_context(userId.strip())
    except Exception as exc:
        raise HTTPException(status_code=401, detail=str(exc))

    try:
        require_permission(user_context, "canManageUsers", "Only admins can update workspace users.")
    except Exception as exc:
        raise HTTPException(status_code=403, detail=str(exc))

    updates = request.model_dump(exclude_none=True)

    try:
        updated_user = update_workspace_user(target_user_id, updates, actor_user_id=user_context.get("userId"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    summary_bits = []
    if "displayName" in updates:
        summary_bits.append("updated display name")
    if "role" in updates:
        summary_bits.append(f"set role to {updates['role']}")
    if "active" in updates:
        summary_bits.append("enabled user" if updates["active"] else "disabled user")
    if "password" in updates and str(updates.get("password") or "").strip():
        summary_bits.append("reset password")

    log_activity(
        "user_updated",
        actor=user_context,
        target_type="user",
        target_id=updated_user["userId"],
        summary=f'{user_context.get("displayName") or user_context.get("userId")} {", ".join(summary_bits) or "updated a user"} for {updated_user["userId"]}.',
        details={**updates, "relatedUserIds": [updated_user["userId"]]},
    )
    return {"user": updated_user}


@router.get("/activity")
def activity_log(userId: str, limit: int | None = None):
    try:
        user_context = get_user_context(userId.strip())
    except Exception as exc:
        raise HTTPException(status_code=401, detail=str(exc))

    if not _can_view_activity_log(user_context):
        raise HTTPException(status_code=403, detail="Your role cannot view the activity log.")

    return {"events": _activity_events_for_user(user_context, limit=limit)}


@router.post("/conversations/sync")
def sync_conversation(request: ConversationSyncRequest):
    user_id = request.userId.strip()

    if not user_id:
        raise HTTPException(status_code=400, detail="userId is required")

    try:
        user_context = get_user_context(user_id)
    except Exception as exc:
        raise HTTPException(status_code=401, detail=str(exc))

    try:
        saved_conversation = upsert_conversation_for_user(
            user_context,
            request.conversation.model_dump(),
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return {"conversation": saved_conversation}


@router.delete("/conversations/{conversation_id}")
def remove_conversation(conversation_id: str, userId: str):
    try:
        user_context = get_user_context(userId.strip())
    except Exception as exc:
        raise HTTPException(status_code=401, detail=str(exc))

    try:
        deleted = delete_conversation_for_user(user_context, conversation_id)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    if not deleted:
        raise HTTPException(status_code=404, detail="Conversation not found")

    return {"deleted": True, "conversationId": conversation_id}


def _run_chat_turn(user_context, query, step=None, analysis=None, case_query=None, session_id=None):
    user_id = user_context["userId"]
    bank_id = user_context["bankId"]
    permissions = user_context.get("permissions") or {}

    if not session_id:
        session_id = f"{user_id}_{uuid.uuid4().hex}"

    response = ""
    next_step = None
    analysis = analysis if isinstance(analysis, dict) else {}
    case_query = case_query if isinstance(case_query, str) and case_query.strip() else None
    documents = []

    choice = _normalize_choice(query)
    followup_steps = {"fetch_documentation", "generate_report", "historical_docs", "final_assistance"}

    if step in followup_steps and choice is None:
        step = None
        analysis = {}
        case_query = None

    if not step or step == "conversation_end":
        analysis = detect_fraud(query, bank_id)

        if not analysis.get("supported"):
            response = (
                analysis.get("reason")
                or f"The content you asked for is not available in the {AXIS_BANK_NAME} fraud blueprint knowledge base. Please ask a relevant fraud-related query."
            )
            next_step = "conversation_end"
            analysis = {}
            case_query = None
        else:
            case_query = query
            analysis_text = format_analysis(analysis)
            sop_summary = analysis.get("sop_summary", "").strip()
            references = analysis.get("references") or []
            sources_line = f"\n\nSources: {', '.join(references)}" if references else ""

            response = f"""
{analysis_text}

SOP Analysis:
{sop_summary}{sources_line}

Do you want the relevant documentation for this fraud case? (Yes/No)
"""
            next_step = "fetch_documentation"

    elif step == "fetch_documentation":
        if choice == "yes":
            if permissions.get("canDownloadDocuments"):
                docs = fetch_relevant_documents(bank_id)
                documents = docs
                doc_names = ", ".join([doc.get("name", "Document") for doc in docs]) or "None found"

                response = f"""
Relevant Blueprint Documentation:

{doc_names}

Do you want an automated investigation report generated? (Yes/No)
"""
            else:
                response = """
Document downloads are not available for your role.

Do you want an automated investigation report generated? (Yes/No)
"""
            next_step = "generate_report"

        elif choice == "no":
            response = """
Skipping documents.

Do you want an automated investigation report generated? (Yes/No)
"""
            next_step = "generate_report"
        else:
            response = """
I did not get a clear Yes/No. Please reply Yes or No.
"""
            next_step = "fetch_documentation"

    elif step == "generate_report":
        if choice == "yes":
            if permissions.get("canGenerateReport"):
                report = generate_investigation_report(case_query or query, bank_id, analysis or {})

                response = f"""
Generated Investigation Report:

{report}

Do you want historical fraud case references? (Yes/No)
"""
            else:
                response = """
Automated investigation reports are not available for your role.

Do you want historical fraud case references? (Yes/No)
"""
            next_step = "historical_docs"

        elif choice == "no":
            response = """
Skipping report generation.

Do you want historical fraud case references? (Yes/No)
"""
            next_step = "historical_docs"
        else:
            response = """
I did not get a clear Yes/No. Please reply Yes or No.
"""
            next_step = "generate_report"

    elif step == "historical_docs":
        if choice == "yes":
            if permissions.get("canViewHistoricalCases"):
                hist_docs = fetch_historical_docs()

                response = f"""
Historical Fraud Case References:

{hist_docs}

Is there anything else I can help you with?
"""
            else:
                response = """
Historical fraud case references are not available for your role.

Is there anything else I can help you with?
"""
            next_step = "final_assistance"

        elif choice == "no":
            response = """
Skipping historical documents.

Is there anything else I can help you with?
"""
            next_step = "final_assistance"
        else:
            response = """
I did not get a clear Yes/No. Please reply Yes or No.
"""
            next_step = "historical_docs"

    elif step == "final_assistance":
        if choice == "yes":
            response = "Okay. Please provide details about the case."
            next_step = "conversation_end"

        elif choice == "no":
            response = (
                "Thank you for using the AXIS Bank Fraud Investigation Assistant. "
                "If you need help again, just type the case details anytime and I will be ready to assist."
            )
            next_step = "conversation_end"

        else:
            response = "I did not get a clear Yes/No. Please reply Yes or No."
            next_step = "final_assistance"

    fraud_category = analysis.get("fraud_category") if isinstance(analysis, dict) else ""

    return {
        "user": user_id,
        "bank": bank_id,
        "query": query,
        "fraud_analysis": analysis,
        "chatbot_response": response,
        "next_step": next_step,
        "sessionId": session_id,
        "fraud_category": fraud_category,
        "documents": documents,
        "conversation_state": {
            "step": next_step,
            "analysis": analysis if isinstance(analysis, dict) else {},
            "case_query": case_query,
            "sessionId": session_id,
        },
    }


@router.get("/fraud")
def fraud_chat(userId: str, query: str, sessionId: str | None = None):
    try:
        user_context = get_user_context(userId)
    except Exception as exc:
        raise HTTPException(status_code=401, detail=str(exc))

    try:
        require_permission(user_context, "canChat", "Your role has read-only workspace access and cannot start chat analysis.")
    except Exception as exc:
        raise HTTPException(status_code=403, detail=str(exc))

    if not sessionId:
        sessionId = f"{userId}_{uuid.uuid4().hex}"

    last_chat = get_last_chat(userId, sessionId)
    step = last_chat["step"] if last_chat else None
    analysis = last_chat.get("analysis") if last_chat else None
    case_query = last_chat.get("case_query") if last_chat else None

    result = _run_chat_turn(
        user_context=user_context,
        query=query,
        step=step,
        analysis=analysis,
        case_query=case_query,
        session_id=sessionId,
    )

    save_chat_log(
        userId,
        user_context["bankId"],
        user_context["role"],
        result["sessionId"],
        query,
        result["chatbot_response"],
        result["next_step"],
        result["fraud_analysis"],
        result["conversation_state"]["case_query"],
    )

    return result


@router.post("/chat")
def fraud_chat_turn(request: ChatTurnRequest):
    user_id = request.userId.strip()
    query = request.query.strip()

    if not user_id or not query:
        raise HTTPException(status_code=400, detail="userId and query are required")

    try:
        user_context = get_user_context(user_id)
    except Exception as exc:
        raise HTTPException(status_code=401, detail=str(exc))

    try:
        require_permission(user_context, "canChat", "Your role has read-only workspace access and cannot send chat queries.")
    except Exception as exc:
        raise HTTPException(status_code=403, detail=str(exc))

    state = request.state or ConversationState()

    return _run_chat_turn(
        user_context=user_context,
        query=query,
        step=state.step,
        analysis=state.analysis,
        case_query=state.case_query,
        session_id=state.sessionId,
    )


@router.post("/login")
def login(request: dict):
    user_id = request.get("userId", "").strip()
    password = request.get("password", "").strip()

    if not user_id or not password:
        raise HTTPException(status_code=400, detail="userId and password are required")

    try:
        user_context = verify_user_credentials(user_id, password)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid userId or password")

    log_activity(
        "user_login",
        actor=user_context,
        target_type="user",
        target_id=user_context.get("userId") or "",
        summary=f'{user_context.get("displayName") or user_context.get("userId")} signed in.',
        details={"role": user_context.get("role") or "", "relatedUserIds": [user_context.get("userId") or ""]},
    )

    return user_context
