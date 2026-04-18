from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field

from app.services.auth_service import get_user_context, require_permission
from app.services.customer_fraud_chat_service import run_customer_fraud_chat


router = APIRouter(prefix="/customer-fraud", tags=["Customer Fraud Chat"])


class CustomerFraudConversationState(BaseModel):
    step: str | None = None
    sessionId: str | None = None
    cif_id: str | None = None
    customer_name: str | None = None
    mobile: str | None = None
    start_datetime: str | None = None
    end_datetime: str | None = None
    resolved_customer: dict[str, Any] = Field(default_factory=dict)
    missing_fields: list[str] = Field(default_factory=list)
    latest_analysis: dict[str, Any] = Field(default_factory=dict)


class CustomerFraudChatRequest(BaseModel):
    userId: str
    query: str
    state: CustomerFraudConversationState | None = None

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "userId": "axis001",
                "query": "Check CIF1001 from 2026-04-17 00:00 to 2026-04-18 23:59 for fraud.",
                "state": None,
            }
        }
    )


class CustomerRecordPayload(BaseModel):
    cif_id: str
    name: str
    mobile: str
    accounts: list[str] = Field(default_factory=list)


class QueryWindowPayload(BaseModel):
    start_datetime: str
    end_datetime: str


class ReviewedTransactionPayload(BaseModel):
    txn_id: str
    timestamp: str
    amount: float
    type: str
    channel: str
    beneficiary: str
    is_new_beneficiary: bool
    status: str
    account_id: str


class FlaggedTransactionPayload(ReviewedTransactionPayload):
    reasons: list[str] = Field(default_factory=list)


class FraudPatternPayload(BaseModel):
    pattern: str
    severity: str
    details: str
    transaction_ids: list[str] = Field(default_factory=list)


class TransactionSummaryPayload(BaseModel):
    total_transactions: int
    debit_transactions: int
    credit_transactions: int
    total_debit_amount: float
    total_credit_amount: float
    channels: list[str] = Field(default_factory=list)
    new_beneficiary_transactions: int
    high_value_debits: int


class CustomerFraudAnalysisPayload(BaseModel):
    customer: CustomerRecordPayload
    query_window: QueryWindowPayload
    risk_level: str
    risk_score: int
    fraud_classification: str
    suspicious_patterns: list[FraudPatternPayload] = Field(default_factory=list)
    flagged_transactions: list[FlaggedTransactionPayload] = Field(default_factory=list)
    reviewed_transactions: list[ReviewedTransactionPayload] = Field(default_factory=list)
    transaction_summary: TransactionSummaryPayload
    reasoning_summary: str
    recommended_actions: list[str] = Field(default_factory=list)


class CustomerFraudChatResponse(BaseModel):
    user: str
    query: str
    chatbot_response: str
    next_step: str
    sessionId: str
    customer_identified: bool
    customer: CustomerRecordPayload | None = None
    fraud_analysis: CustomerFraudAnalysisPayload | None = None
    conversation_state: CustomerFraudConversationState

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "user": "axis001",
                "query": "Check CIF1001 from 2026-04-17 00:00 to 2026-04-18 23:59 for fraud.",
                "chatbot_response": "Customer Identified: Rahul Sharma (CIF1001) ...",
                "next_step": "analysis_complete",
                "sessionId": "axis001_demo_session",
                "customer_identified": True,
                "customer": {
                    "cif_id": "CIF1001",
                    "name": "Rahul Sharma",
                    "mobile": "9876543210",
                    "accounts": ["ACC1001"],
                },
                "fraud_analysis": {
                    "customer": {
                        "cif_id": "CIF1001",
                        "name": "Rahul Sharma",
                        "mobile": "9876543210",
                        "accounts": ["ACC1001"],
                    },
                    "query_window": {
                        "start_datetime": "2026-04-17 00:00:00",
                        "end_datetime": "2026-04-18 23:59:59",
                    },
                    "risk_level": "High",
                    "risk_score": 82,
                    "fraud_classification": "Likely mule-account payout or account takeover pattern",
                    "suspicious_patterns": [
                        {
                            "pattern": "Rapid debit velocity",
                            "severity": "high",
                            "details": "2 debit transactions occurred within 5 minutes with total outward value Rs. 99,998.00.",
                            "transaction_ids": ["TXN00001", "TXN00002"],
                        }
                    ],
                    "flagged_transactions": [],
                    "reviewed_transactions": [],
                    "transaction_summary": {
                        "total_transactions": 2,
                        "debit_transactions": 2,
                        "credit_transactions": 0,
                        "total_debit_amount": 99998.0,
                        "total_credit_amount": 0.0,
                        "channels": ["UPI"],
                        "new_beneficiary_transactions": 2,
                        "high_value_debits": 0,
                    },
                    "reasoning_summary": "I reviewed the selected transaction window...",
                    "recommended_actions": [
                        "Call the customer on the registered mobile number and trigger immediate fraud-operations review."
                    ],
                },
                "conversation_state": {
                    "step": "analysis_complete",
                    "sessionId": "axis001_demo_session",
                    "cif_id": "CIF1001",
                    "customer_name": "Rahul Sharma",
                    "mobile": "9876543210",
                    "start_datetime": "2026-04-17 00:00:00",
                    "end_datetime": "2026-04-18 23:59:59",
                    "resolved_customer": {
                        "cif_id": "CIF1001",
                        "name": "Rahul Sharma",
                        "mobile": "9876543210",
                        "accounts": ["ACC1001"],
                    },
                    "missing_fields": [],
                    "latest_analysis": {},
                },
            }
        }
    )


@router.post(
    "/chat",
    response_model=CustomerFraudChatResponse,
    status_code=status.HTTP_200_OK,
    summary="LLM-assisted customer transaction fraud chat",
    description=(
        "Collect missing customer and date-range inputs, identify the customer uniquely, "
        "fetch transactions from MongoDB, and return structured fraud analysis."
    ),
)
def customer_fraud_chat(request: CustomerFraudChatRequest) -> CustomerFraudChatResponse:
    user_id = request.userId.strip()
    query = request.query.strip()

    if not user_id or not query:
        raise HTTPException(status_code=400, detail="userId and query are required")

    try:
        user_context = get_user_context(user_id)
    except Exception as exc:
        raise HTTPException(status_code=401, detail=str(exc))

    try:
        require_permission(user_context, "canChat", "Your role cannot run the customer fraud chat workflow.")
    except Exception as exc:
        raise HTTPException(status_code=403, detail=str(exc))

    try:
        result = run_customer_fraud_chat(
            user_context=user_context,
            query=query,
            state=request.state.model_dump() if request.state else None,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Customer fraud chat failed: {exc}") from exc

    return CustomerFraudChatResponse(**result)
