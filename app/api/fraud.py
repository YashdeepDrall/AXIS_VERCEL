import datetime
import os
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.core.config import AXIS_BANK_DIR, AXIS_BANK_ID, AXIS_BANK_NAME, BASE_DIR
from app.db.mongodb import cases_collection, chat_logs_collection, documents_collection, fs
from app.services.auth_service import verify_user, verify_user_credentials
from app.services.fraud_service import detect_fraud, generate_investigation_report


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


def get_last_chat(user_id, session_id):
    return chat_logs_collection.find_one(
        {"userId": user_id, "sessionId": session_id},
        sort=[("timestamp", -1)],
    )


def save_chat_log(user_id, bank_id, session_id, user_input, bot_output, step, analysis, case_query):
    chat_logs_collection.insert_one(
        {
            "userId": user_id,
            "bankId": bank_id,
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

    docs = list(
        documents_collection.find(
            {
                "bankId": bank_id,
                "$or": [
                    {"filePath": {"$exists": True}},
                    {"isPDF": True},
                    {"documentType": "SOP"},
                ],
            }
        )
    )

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


def _run_chat_turn(user_id, bank_id, query, step=None, analysis=None, case_query=None, session_id=None):
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
                or f"The content you asked for is not available in the {AXIS_BANK_NAME} SOP. Please ask a relevant fraud-related query."
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
            docs = fetch_relevant_documents(bank_id)
            documents = docs
            doc_names = ", ".join([doc.get("name", "Document") for doc in docs]) or "None found"

            response = f"""
Relevant SOP Documents:

{doc_names}

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
            report = generate_investigation_report(case_query or query, bank_id, analysis or {})

            response = f"""
Generated Investigation Report:

{report}

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
            hist_docs = fetch_historical_docs()

            response = f"""
Historical Fraud Case References:

{hist_docs}

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
        bank_id = verify_user(userId)
    except Exception as exc:
        raise HTTPException(status_code=401, detail=str(exc))

    if not sessionId:
        sessionId = f"{userId}_{uuid.uuid4().hex}"

    last_chat = get_last_chat(userId, sessionId)
    step = last_chat["step"] if last_chat else None
    analysis = last_chat.get("analysis") if last_chat else None
    case_query = last_chat.get("case_query") if last_chat else None

    result = _run_chat_turn(
        user_id=userId,
        bank_id=bank_id,
        query=query,
        step=step,
        analysis=analysis,
        case_query=case_query,
        session_id=sessionId,
    )

    save_chat_log(
        userId,
        bank_id,
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
        bank_id = verify_user(user_id)
    except Exception as exc:
        raise HTTPException(status_code=401, detail=str(exc))

    state = request.state or ConversationState()

    return _run_chat_turn(
        user_id=user_id,
        bank_id=bank_id,
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
        bank_id = verify_user_credentials(user_id, password)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid userId or password")

    return {"userId": user_id, "bankId": bank_id}
