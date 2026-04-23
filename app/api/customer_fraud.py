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
    case_query: str | None = None
    case_description: str | None = None
    case_family: str | None = None
    suspicion_direction: str | None = None
    investigation_basis: str | None = None
    transaction_relevance: str | None = None
    cif_id: str | None = None
    account_id: str | None = None
    pan: str | None = None
    customer_name: str | None = None
    mobile: str | None = None
    start_datetime: str | None = None
    end_datetime: str | None = None
    evidence_modules_used: list[str] = Field(default_factory=list)
    resolved_customer: dict[str, Any] = Field(default_factory=dict)
    missing_fields: list[str] = Field(default_factory=list)
    latest_analysis: dict[str, Any] = Field(default_factory=dict)
    sop_analysis: dict[str, Any] = Field(default_factory=dict)


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
    pan: str | None = None
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


class TransactionTimelineItemPayload(BaseModel):
    txn_id: str
    timestamp: str
    amount: float
    type: str
    channel: str
    beneficiary: str
    account_id: str
    status: str
    severity: str
    title: str
    details: str
    reasons: list[str] = Field(default_factory=list)


class TransactionSummaryPayload(BaseModel):
    total_transactions: int
    debit_transactions: int
    credit_transactions: int
    total_debit_amount: float
    total_credit_amount: float
    channels: list[str] = Field(default_factory=list)
    new_beneficiary_transactions: int
    high_value_debits: int


class CustomerBaselinePayload(BaseModel):
    available: bool
    lookback_days: int
    dominant_channel: str
    historical_debit_count: int
    same_channel_history_count: int
    typical_debit_amount: float
    average_debit_amount: float
    prior_max_debit: float
    average_daily_debit_count: float
    average_daily_debit_outflow: float
    review_window_debit_count: int
    review_window_debit_outflow: float
    review_window_max_debit: float
    comparison_summary: str
    anomalies: list[str] = Field(default_factory=list)


class FamilyEvidenceCardPayload(BaseModel):
    title: str
    summary: str
    items: list[str] = Field(default_factory=list)
    emphasis: str | None = None


class RelatedDataSummaryPayload(BaseModel):
    loan_accounts: int = 0
    collateral_records: int = 0
    document_verifications: int = 0
    case_events: int = 0


class LoanExposurePayload(BaseModel):
    available: bool = False
    loan_count: int = 0
    loan_id: str | None = None
    loan_account_number: str | None = None
    product_type: str | None = None
    sanction_amount: float = 0.0
    disbursed_amount: float = 0.0
    outstanding_amount: float = 0.0
    overdue_amount: float = 0.0
    emi_amount: float = 0.0
    sanctioned_at: str | None = None
    repayment_status: str | None = None
    days_past_due: int = 0
    last_repayment_at: str | None = None
    branch_name: str | None = None
    loan_status: str | None = None
    underwriting_flags: list[str] = Field(default_factory=list)
    summary: str | None = None


class CollateralReviewPayload(BaseModel):
    available: bool = False
    collateral_count: int = 0
    collateral_id: str | None = None
    loan_id: str | None = None
    collateral_type: str | None = None
    property_address: str | None = None
    declared_owner_name: str | None = None
    verified_owner_name: str | None = None
    declared_market_value: float = 0.0
    assessed_value: float = 0.0
    verification_status: str | None = None
    encumbrance_status: str | None = None
    registry_reference: str | None = None
    duplicate_collateral_hits: int = 0
    supporting_document_count: int = 0
    issues: list[str] = Field(default_factory=list)
    summary: str | None = None


class DocumentReviewPayload(BaseModel):
    available: bool = False
    documents_reviewed: int = 0
    failed_documents: int = 0
    primary_mismatch_types: list[str] = Field(default_factory=list)
    verification_statuses: list[str] = Field(default_factory=list)
    highlights: list[str] = Field(default_factory=list)
    latest_submitted_at: str | None = None
    summary: str | None = None


class CaseEventSummaryPayload(BaseModel):
    available: bool = False
    total_events: int = 0
    open_events: int = 0
    escalated_events: int = 0
    high_severity_events: int = 0
    recent_event_types: list[str] = Field(default_factory=list)
    latest_event_at: str | None = None
    latest_status: str | None = None
    latest_summary: str | None = None
    highlights: list[str] = Field(default_factory=list)
    summary: str | None = None


class CustomerFraudAnalysisPayload(BaseModel):
    customer: CustomerRecordPayload
    query_window: QueryWindowPayload
    risk_level: str
    risk_score: int
    case_family: str
    suspicion_direction: str
    investigation_basis: str
    transaction_relevance: str
    fraud_classification: str
    suspicious_patterns: list[FraudPatternPayload] = Field(default_factory=list)
    flagged_transactions: list[FlaggedTransactionPayload] = Field(default_factory=list)
    transaction_timeline: list[TransactionTimelineItemPayload] = Field(default_factory=list)
    reviewed_transactions: list[ReviewedTransactionPayload] = Field(default_factory=list)
    transaction_summary: TransactionSummaryPayload
    customer_baseline: CustomerBaselinePayload | dict[str, Any] = Field(default_factory=dict)
    related_data_summary: RelatedDataSummaryPayload | dict[str, Any] = Field(default_factory=dict)
    loan_exposure: LoanExposurePayload | dict[str, Any] = Field(default_factory=dict)
    collateral_review: CollateralReviewPayload | dict[str, Any] = Field(default_factory=dict)
    document_review: DocumentReviewPayload | dict[str, Any] = Field(default_factory=dict)
    case_event_summary: CaseEventSummaryPayload | dict[str, Any] = Field(default_factory=dict)
    evidence_modules_used: list[str] = Field(default_factory=list)
    case_summary: str
    family_cards: list[FamilyEvidenceCardPayload] = Field(default_factory=list)
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
                    "case_family": "Transaction Fraud",
                    "suspicion_direction": "Customer Victim",
                    "investigation_basis": "Transaction-Led",
                    "transaction_relevance": "primary",
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
                    "evidence_modules_used": [
                        "Universal Case Header",
                        "Transaction Review",
                        "Transaction Timeline",
                        "Customer Baseline",
                        "SOP Grounding"
                    ],
                    "case_summary": "Transaction Fraud is being reviewed as a transaction-led case with customer victim suspicion. Transaction activity is the primary evidence source for this investigation.",
                    "family_cards": [],
                    "reasoning_summary": "I reviewed the selected transaction window...",
                    "recommended_actions": [
                        "Call the customer on the registered mobile number and trigger immediate fraud-operations review."
                    ],
                },
                "conversation_state": {
                    "step": "analysis_complete",
                    "sessionId": "axis001_demo_session",
                    "case_description": "Repeated UPI debits reported by the customer.",
                    "case_family": "Transaction Fraud",
                    "suspicion_direction": "Customer Victim",
                    "investigation_basis": "Transaction-Led",
                    "transaction_relevance": "primary",
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
