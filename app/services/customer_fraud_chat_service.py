from __future__ import annotations

import datetime as dt
import os
import re
import uuid
from collections import Counter
from statistics import median
from typing import Any

from app.core.config import AXIS_BANK_DIR, AXIS_BANK_ID, AXIS_BLUEPRINT_FILE, BASE_DIR
from app.db.banking import (
    get_case_events_collection,
    get_collateral_records_collection,
    get_customers_collection,
    get_document_verifications_collection,
    get_loan_accounts_collection,
    get_transactions_collection,
)
from app.db.mongodb import cases_collection, documents_collection, fs
from app.services.fraud_service import detect_fraud, generate_investigation_report
from app.services.historical_reference_service import list_historical_reference_cards
from app.services.llm_service import GeminiServiceError, generate_text
from app.services.report_export_service import export_investigation_report_pdf


SUSPICIOUS_BENEFICIARY_KEYWORDS = (
    "scam",
    "fraud",
    "unknown",
    "mule",
    "flashloan",
    "new_acc",
    "urgenthelp",
    "prize",
)
HIGH_VALUE_THRESHOLD = 50000.0
RAPID_DEBIT_WINDOW_MINUTES = 5
BASELINE_LOOKBACK_DAYS = 30
TIMELINE_ITEM_LIMIT = 10
FOLLOWUP_STEPS = {"fetch_documentation", "generate_report", "historical_docs", "final_assistance"}
PRIMARY_TRANSACTION_MODULES = [
    "Universal Case Header",
    "Transaction Review",
    "Transaction Timeline",
    "Customer Baseline",
    "SOP Grounding",
]
CASE_FAMILY_METADATA: dict[str, dict[str, Any]] = {
    "Transaction Fraud": {
        "suspicion_direction": "Customer Victim",
        "investigation_basis": "Transaction-Led",
        "transaction_relevance": "primary",
        "default_risk_score": 40,
        "default_classification": "Potential transaction fraud requiring payment-channel review",
        "default_actions": [
            "Validate the customer-reported debit activity and review the linked digital payment trail.",
        ],
        "evidence_modules": PRIMARY_TRANSACTION_MODULES,
    },
    "Loan / Mortgage Fraud": {
        "suspicion_direction": "Customer-to-Bank",
        "investigation_basis": "Loan-Led",
        "transaction_relevance": "not_applicable",
        "default_risk_score": 72,
        "default_classification": "Potential loan or mortgage fraud requiring collateral and document verification",
        "default_actions": [
            "Validate submitted collateral ownership, legal enforceability, and any duplication across linked credit cases.",
            "Review loan sanction, repayment/default status, and supporting underwriting documents with credit-risk and legal teams.",
            "Escalate the case for coordinated fraud, recovery, and legal review before any closure decision.",
        ],
        "evidence_modules": [
            "Universal Case Header",
            "Loan Exposure",
            "Collateral Review",
            "Repayment Snapshot",
            "SOP Grounding",
        ],
    },
    "Document Fraud": {
        "suspicion_direction": "Customer-to-Bank",
        "investigation_basis": "Document-Led",
        "transaction_relevance": "not_applicable",
        "default_risk_score": 68,
        "default_classification": "Potential forged or manipulated document submission requiring verification",
        "default_actions": [
            "Verify submitted documents against source records and document-authenticity checkpoints.",
            "Document the mismatch type and escalate the case for manual document-review controls.",
        ],
        "evidence_modules": [
            "Universal Case Header",
            "Document Verification",
            "Mismatch Findings",
            "Linked Exposure",
            "SOP Grounding",
        ],
    },
    "KYC / Identity Fraud": {
        "suspicion_direction": "Customer-to-Bank",
        "investigation_basis": "Profile-Led",
        "transaction_relevance": "not_applicable",
        "default_risk_score": 58,
        "default_classification": "Potential KYC or identity misuse requiring profile verification",
        "default_actions": [
            "Validate onboarding or profile-update records, linked identities, and profile-change history.",
            "Check whether related accounts or identifiers indicate mule, synthetic, or impersonation risk.",
        ],
        "evidence_modules": [
            "Universal Case Header",
            "Identity / KYC Review",
            "Profile Events",
            "Linked Accounts",
            "SOP Grounding",
        ],
    },
    "Dispute / First-Party Abuse": {
        "suspicion_direction": "Customer-to-Bank",
        "investigation_basis": "Complaint-Led",
        "transaction_relevance": "supporting",
        "default_risk_score": 48,
        "default_classification": "Potential first-party or dispute-abuse pattern requiring complaint validation",
        "default_actions": [
            "Review complaint history, authorization evidence, and any repeat dispute pattern before deciding liability.",
            "Validate merchant, session, device, or channel evidence against the customer denial narrative.",
        ],
        "evidence_modules": [
            "Universal Case Header",
            "Complaint / Dispute Pattern",
            "Transaction Review",
            "Authorization Evidence",
            "SOP Grounding",
        ],
    },
    "Mule / Funnel Account": {
        "suspicion_direction": "Customer-to-Bank",
        "investigation_basis": "Transaction-Led",
        "transaction_relevance": "primary",
        "default_risk_score": 72,
        "default_classification": "Potential mule or funnel-account routing pattern",
        "default_actions": [
            "Trace linked inward and outward movement across connected beneficiaries, devices, and accounts.",
            "Escalate for linked-account, beneficiary, and downstream-funds review.",
        ],
        "evidence_modules": [
            "Universal Case Header",
            "Transaction Review",
            "Transaction Timeline",
            "Linked Accounts / Beneficiaries",
            "SOP Grounding",
        ],
    },
    "Mixed": {
        "suspicion_direction": "Mixed",
        "investigation_basis": "Mixed",
        "transaction_relevance": "supporting",
        "default_risk_score": 62,
        "default_classification": "Mixed fraud indicators requiring multi-source review",
        "default_actions": [
            "Review the case through both transaction evidence and non-transaction case documents before final classification.",
        ],
        "evidence_modules": [
            "Universal Case Header",
            "Primary Review Basis",
            "Transaction Review",
            "SOP Grounding",
        ],
    },
    "Manual Review": {
        "suspicion_direction": "Manual Review",
        "investigation_basis": "Mixed",
        "transaction_relevance": "supporting",
        "default_risk_score": 40,
        "default_classification": "Manual review required",
        "default_actions": [
            "Continue investigator review and gather the strongest available evidence before final categorization.",
        ],
        "evidence_modules": [
            "Universal Case Header",
            "Primary Review Basis",
            "SOP Grounding",
        ],
    },
}
CASE_FAMILY_KEYWORDS: dict[str, tuple[str, ...]] = {
    "Loan / Mortgage Fraud": (
        "loan",
        "home loan",
        "mortgage",
        "collateral",
        "property",
        "emi",
        "default",
        "repayment",
        "sanction",
        "borrower",
        "housing loan",
        "overdue",
    ),
    "Document Fraud": (
        "document",
        "documents",
        "forged",
        "forgery",
        "fake document",
        "fake documents",
        "fake papers",
        "salary slip",
        "salary slips",
        "bank statement",
        "bank statements",
        "itr",
        "gst",
        "registry",
        "ownership mismatch",
        "valuation",
        "title deed",
        "deed",
        "property papers",
    ),
    "KYC / Identity Fraud": (
        "kyc",
        "identity",
        "synthetic identity",
        "account opening",
        "onboarding",
        "aadhaar",
        "aadhar",
        "pan mismatch",
        "profile update",
        "mobile change",
        "email change",
    ),
    "Dispute / First-Party Abuse": (
        "chargeback",
        "merchant dispute",
        "friendly fraud",
        "false dispute",
        "customer denies",
        "denied transaction",
        "denies transaction",
        "self authorized",
        "self-authorized",
        "repeat dispute",
        "dispute abuse",
    ),
    "Mule / Funnel Account": (
        "mule",
        "money mule",
        "funnel account",
        "funnel",
        "pass through",
        "pass-through",
        "layering",
        "cash out",
        "cash-out",
        "rapid outward",
        "inward credits",
    ),
    "Transaction Fraud": (
        "upi",
        "imps",
        "atm",
        "netbanking",
        "pos",
        "beneficiary",
        "unauthorized debit",
        "unauthorised debit",
        "withdrawal",
        "withdrawn",
        "debit",
        "payment link",
        "fake link",
        "phishing",
        "otp",
        "collect request",
        "refund scam",
        "account takeover",
        "card fraud",
    ),
}
NON_TRANSACTION_FAMILIES = {
    "Loan / Mortgage Fraud",
    "Document Fraud",
    "KYC / Identity Fraud",
    "Dispute / First-Party Abuse",
}
GENERIC_CLASSIFICATIONS = {
    "",
    "manual review required",
    "no transactions found in the selected window",
    "no strong fraud signal in the selected window",
    "transaction pattern requires manual fraud review",
}
NON_PRIMARY_TRANSACTION_ACTION_SNIPPETS = (
    "continue monitoring the account",
    "customer-authorized",
    "expand the date range",
    "review digital channel logs for the flagged window",
    "flagged transaction pattern",
    "call the customer on the registered mobile number",
    "beneficiary registration details",
    "approved the new payee",
    "source channel, device, and approval trail",
    "flagged transaction ids",
    "beneficiary profiling",
)
LOAN_SIGNAL_KEYWORDS = ("loan", "mortgage", "property", "collateral", "emi", "default", "repayment", "sanction", "overdue")
DOCUMENT_SIGNAL_KEYWORDS = ("document", "documents", "forged", "forgery", "fake", "salary slip", "bank statement", "itr", "gst", "deed", "registry", "valuation")
IDENTITY_SIGNAL_KEYWORDS = ("kyc", "identity", "aadhaar", "aadhar", "pan", "mobile change", "email change", "onboarding", "profile")
DISPUTE_SIGNAL_KEYWORDS = ("chargeback", "merchant", "dispute", "friendly fraud", "false dispute", "customer denies", "denied transaction", "repeat dispute")
MULE_SIGNAL_KEYWORDS = ("mule", "funnel", "pass through", "layering", "cash out", "rapid outward", "inward credits")
HIGH_RISK_DOCUMENT_STATUSES = {"forged", "suspected_forged", "suspected_impersonation", "mismatch", "unverifiable"}
HIGH_RISK_COLLATERAL_STATUSES = {"suspected_forged", "ownership_mismatch", "duplicate_collateral"}


def _format_datetime(value: dt.datetime | None) -> str | None:
    if value is None:
        return None
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _cap_risk_score(value: Any) -> int:
    try:
        numeric = int(float(value or 0))
    except (TypeError, ValueError):
        numeric = 0
    return max(0, min(numeric, 100))


def _start_of_day(value: dt.datetime) -> dt.datetime:
    return value.replace(hour=0, minute=0, second=0, microsecond=0)


def _end_of_day(value: dt.datetime) -> dt.datetime:
    return value.replace(hour=23, minute=59, second=59, microsecond=0)


def _now() -> dt.datetime:
    return dt.datetime.now().replace(microsecond=0)


def _normalize_choice(text: str) -> str | None:
    cleaned = (text or "").strip().lower()
    if not cleaned:
        return None

    head = cleaned.split()[0]
    if head in {"yes", "y", "yeah", "yep", "sure", "ok", "okay"}:
        return "yes"
    if head in {"no", "n", "nope", "nah", "nothing"}:
        return "no"
    return None


def _fetch_relevant_documents(bank_id: str) -> list[dict[str, Any]]:
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
                "path": "",
                "fileId": str(grid_file._id) if grid_file else "",
                "downloadUrl": f"/documents/{grid_file._id}" if grid_file else "",
            }
        ]

    results: list[dict[str, Any]] = []

    for doc in docs:
        file_name = str(doc.get("fileName") or "").strip()
        file_id = str(doc.get("fileId") or "").strip()

        if not file_id and file_name:
            grid_file = fs.find_one({"filename": file_name, "bankId": bank_id})
            if grid_file:
                file_id = str(grid_file._id)

        results.append(
            {
                "name": file_name or "Document",
                "path": "",
                "fileId": file_id,
                "downloadUrl": f"/documents/{file_id}" if file_id else "",
            }
        )

    return results


def _fetch_historical_references(analysis: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    case_family = str((analysis or {}).get("case_family") or "").strip()
    transaction_relevance = str((analysis or {}).get("transaction_relevance") or "").strip().lower()
    if case_family in NON_TRANSACTION_FAMILIES and transaction_relevance == "not_applicable":
        return []
    return list_historical_reference_cards(limit=6)


def _empty_state(session_id: str | None = None) -> dict[str, Any]:
    return {
        "step": "collect_customer",
        "sessionId": session_id,
        "cif_id": None,
        "account_id": None,
        "pan": None,
        "customer_name": None,
        "mobile": None,
        "start_datetime": None,
        "end_datetime": None,
        "resolved_customer": {},
        "missing_fields": [],
        "latest_analysis": {},
    }


def _normalize_state(raw_state: dict[str, Any] | None) -> dict[str, Any]:
    state = _empty_state((raw_state or {}).get("sessionId"))

    if not isinstance(raw_state, dict):
        return state

    for key in [
        "step",
        "sessionId",
        "cif_id",
        "account_id",
        "pan",
        "customer_name",
        "mobile",
        "start_datetime",
        "end_datetime",
    ]:
        value = raw_state.get(key)
        if isinstance(value, str) and value.strip():
            state[key] = value.strip()
        elif value is not None and key in {"step", "sessionId"}:
            state[key] = str(value).strip() or state[key]

    resolved_customer = raw_state.get("resolved_customer")
    if isinstance(resolved_customer, dict):
        state["resolved_customer"] = _normalize_resolved_customer_payload(resolved_customer)

    missing_fields = raw_state.get("missing_fields")
    if isinstance(missing_fields, list):
        state["missing_fields"] = [str(item).strip() for item in missing_fields if str(item).strip()]

    latest_analysis = raw_state.get("latest_analysis")
    if isinstance(latest_analysis, dict):
        state["latest_analysis"] = latest_analysis

    return state


def _normalize_query(text: str) -> str:
    return " ".join((text or "").strip().split())


def _normalized_case_text(text: str) -> str:
    return f" {re.sub(r'[^a-z0-9]+', ' ', (text or '').lower()).strip()} "


def _keyword_score(text: str, keywords: tuple[str, ...]) -> tuple[int, list[str]]:
    score = 0
    matched: list[str] = []
    for keyword in keywords:
        normalized_keyword = keyword.lower().strip()
        if not normalized_keyword or normalized_keyword not in text:
            continue
        matched.append(normalized_keyword)
        score += 2 if " " in normalized_keyword else 1
    return score, matched


def _case_route(case_family: str, matched_keywords: list[str] | None = None, transaction_relevance: str | None = None) -> dict[str, Any]:
    metadata = CASE_FAMILY_METADATA.get(case_family, CASE_FAMILY_METADATA["Manual Review"])
    modules = [str(item).strip() for item in metadata.get("evidence_modules") or [] if str(item).strip()]
    route = {
        "case_family": case_family,
        "suspicion_direction": str(metadata.get("suspicion_direction") or "Manual Review"),
        "investigation_basis": str(metadata.get("investigation_basis") or "Mixed"),
        "transaction_relevance": transaction_relevance or str(metadata.get("transaction_relevance") or "supporting"),
        "evidence_modules_used": modules,
        "matched_keywords": [item for item in (matched_keywords or []) if item],
    }
    if route["transaction_relevance"] in {"primary", "supporting"} and "Transaction Review" not in route["evidence_modules_used"]:
        route["evidence_modules_used"].append("Transaction Review")
    return route


def _classify_case_family(case_description: str | None) -> dict[str, Any]:
    normalized = _normalized_case_text(case_description or "")
    if len(normalized.strip()) < 4:
        return _case_route("Manual Review")

    scored: list[tuple[str, int, list[str]]] = []
    family_order = list(CASE_FAMILY_KEYWORDS.keys())
    for family in family_order:
        score, matched = _keyword_score(normalized, CASE_FAMILY_KEYWORDS[family])
        if score > 0:
            scored.append((family, score, matched))

    if not scored:
        return _case_route("Manual Review")

    scored.sort(key=lambda item: (-item[1], family_order.index(item[0])))
    top_family, top_score, top_matches = scored[0]
    second_family = None
    second_score = 0
    second_matches: list[str] = []
    if len(scored) > 1:
        second_family, second_score, second_matches = scored[1]

    if second_family:
        if top_score == second_score and top_score >= 2:
            return _case_route("Mixed", top_matches + second_matches)
        if top_family == "Transaction Fraud" and second_family in NON_TRANSACTION_FAMILIES and second_score >= 2:
            return _case_route("Mixed", top_matches + second_matches)
        if top_family in NON_TRANSACTION_FAMILIES and second_family == "Transaction Fraud" and second_score >= 2 and top_score - second_score <= 1:
            route = _case_route(top_family, top_matches + second_matches, transaction_relevance="supporting")
            if "Transaction Timeline" not in route["evidence_modules_used"]:
                route["evidence_modules_used"].append("Transaction Timeline")
            return route
        if top_family in NON_TRANSACTION_FAMILIES and second_family in NON_TRANSACTION_FAMILIES and second_score >= 2 and top_score - second_score <= 0:
            return _case_route("Mixed", top_matches + second_matches)

    return _case_route(top_family, top_matches)


def _is_generic_classification(value: str | None) -> bool:
    return str(value or "").strip().lower() in GENERIC_CLASSIFICATIONS


def _looks_like_code(value: str | None) -> bool:
    cleaned = str(value or "").strip()
    return bool(cleaned) and bool(re.fullmatch(r"[A-Z0-9_-]{3,}", cleaned))


def _friendly_fraud_type_label(analysis: dict[str, Any], case_family: str) -> str:
    classification = str(analysis.get("fraud_classification") or "").strip()
    if classification and not _looks_like_code(classification):
        return classification

    details_text = " ".join(
        str(analysis.get(field) or "").strip()
        for field in ["sop_summary", "relevant_information", "sop_reason"]
    ).strip()
    if classification and details_text:
        pattern = re.escape(classification)
        match = re.search(pattern + r"\s*\(([^)]+)\)", details_text, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip().capitalize()

    return case_family or "Manual review required"


def _action_similarity_tokens(text: str) -> set[str]:
    stopwords = {
        "the",
        "and",
        "for",
        "with",
        "from",
        "into",
        "that",
        "this",
        "then",
        "than",
        "after",
        "before",
        "should",
        "would",
        "could",
        "their",
        "there",
        "them",
        "your",
        "have",
        "has",
        "had",
        "been",
        "being",
        "only",
        "just",
        "case",
        "customer",
        "account",
        "accounts",
        "linked",
        "immediate",
        "immediately",
    }
    tokens: set[str] = set()
    for raw in re.findall(r"[a-z0-9]+", str(text or "").lower()):
        token = raw[:-1] if len(raw) > 4 and raw.endswith("s") else raw
        if len(token) <= 2 or token in stopwords:
            continue
        tokens.add(token)
    return tokens


def _actions_are_near_duplicates(left: str, right: str) -> bool:
    left_tokens = _action_similarity_tokens(left)
    right_tokens = _action_similarity_tokens(right)
    if not left_tokens or not right_tokens:
        return False
    overlap = len(left_tokens & right_tokens)
    coverage = overlap / min(len(left_tokens), len(right_tokens))
    return coverage >= 0.6


def _merge_unique_actions(*action_groups: list[str] | tuple[str, ...]) -> list[str]:
    merged: list[str] = []
    for group in action_groups:
        for item in group or []:
            text = str(item or "").strip()
            if not text:
                continue
            if text in merged:
                continue
            if any(_actions_are_near_duplicates(text, existing) for existing in merged):
                continue
            merged.append(text)
    return merged[:6]


def _filter_actions_for_transaction_relevance(actions: list[str], transaction_relevance: str) -> list[str]:
    if transaction_relevance == "primary":
        return _merge_unique_actions(actions)

    filtered: list[str] = []
    for action in actions:
        text = str(action or "").strip()
        lowered = text.lower()
        if not text:
            continue
        if any(snippet in lowered for snippet in NON_PRIMARY_TRANSACTION_ACTION_SNIPPETS):
            continue
        filtered.append(text)
    return _merge_unique_actions(filtered)


def _build_non_primary_key_concern(analysis: dict[str, Any]) -> str:
    collateral_review = analysis.get("collateral_review") or {}
    document_review = analysis.get("document_review") or {}
    case_event_summary = analysis.get("case_event_summary") or {}
    loan_exposure = analysis.get("loan_exposure") or {}
    case_family = str(analysis.get("case_family") or "").strip()

    if case_family == "Loan / Mortgage Fraud":
        if collateral_review.get("summary"):
            return str(collateral_review.get("summary")).strip()
        issues = collateral_review.get("issues") or []
        if issues:
            return str(issues[0]).strip()
        if loan_exposure.get("summary"):
            return str(loan_exposure.get("summary")).strip()
        repayment_status = str(loan_exposure.get("repayment_status") or "").replace("_", " ").strip()
        if repayment_status:
            days_past_due = int(loan_exposure.get("days_past_due") or 0)
            if days_past_due:
                return f"Repayment status is {repayment_status.lower()} and the linked loan file is {days_past_due} day(s) past due."
            return f"Repayment status is {repayment_status.lower()} on the linked loan exposure."
    if case_family in {"Document Fraud", "KYC / Identity Fraud"}:
        if document_review.get("summary"):
            return str(document_review.get("summary")).strip()
    if case_event_summary.get("latest_summary"):
        return str(case_event_summary.get("latest_summary")).strip()
    if case_event_summary.get("summary"):
        return str(case_event_summary.get("summary")).strip()
    return "Transaction activity is not the primary evidence source for this case family."


def _build_non_primary_indicators(analysis: dict[str, Any], fallback_indicators: list[str] | None = None) -> list[str]:
    case_family = str(analysis.get("case_family") or "").strip()
    collateral_review = analysis.get("collateral_review") or {}
    document_review = analysis.get("document_review") or {}
    case_event_summary = analysis.get("case_event_summary") or {}
    loan_exposure = analysis.get("loan_exposure") or {}
    indicators: list[str] = []

    def add_indicator(value: str | None) -> None:
        text = str(value or "").strip()
        if text and text not in indicators:
            indicators.append(text)

    if case_family == "Loan / Mortgage Fraud":
        status = str(collateral_review.get("verification_status") or "").replace("_", " ").strip()
        if status:
            add_indicator(f"Collateral verification status: {status.title()}")
        for issue in (collateral_review.get("issues") or [])[:2]:
            add_indicator(issue)
        if loan_exposure.get("available"):
            dpd = int(loan_exposure.get("days_past_due") or 0)
            repayment_status = str(loan_exposure.get("repayment_status") or "").replace("_", " ").strip()
            if repayment_status:
                add_indicator(f"Repayment status: {repayment_status.title()}")
            if dpd:
                add_indicator(f"Loan delinquency reached {dpd} day(s) past due.")
        for item in (case_event_summary.get("highlights") or [])[:2]:
            add_indicator(item)

    elif case_family in {"Document Fraud", "KYC / Identity Fraud"}:
        for mismatch in (document_review.get("primary_mismatch_types") or [])[:3]:
            add_indicator(f"Mismatch identified: {mismatch}")
        for item in (document_review.get("highlights") or [])[:3]:
            add_indicator(item)
        for item in (case_event_summary.get("highlights") or [])[:2]:
            add_indicator(item)

    elif case_family == "Dispute / First-Party Abuse":
        for item in (case_event_summary.get("highlights") or [])[:3]:
            add_indicator(item)
        summary = str(case_event_summary.get("summary") or "").strip()
        if summary:
            add_indicator(summary)

    if not indicators:
        for item in fallback_indicators or []:
            add_indicator(item)

    return indicators[:5]


def _build_case_summary(case_description: str, route: dict[str, Any], analysis: dict[str, Any]) -> str:
    family = str(route.get("case_family") or "Manual Review").strip()
    direction = str(route.get("suspicion_direction") or "Manual Review").strip()
    basis = str(route.get("investigation_basis") or "Mixed").strip()
    transaction_relevance = str(route.get("transaction_relevance") or "supporting").strip().lower()
    total_transactions = int(((analysis.get("transaction_summary") or {}).get("total_transactions")) or 0)
    description_text = str(case_description or "").strip()

    if transaction_relevance == "primary":
        summary = (
            f"{family} is being reviewed as a {basis.lower()} case with {direction.lower()} suspicion. "
            f"Transaction activity is the primary evidence source for this investigation."
        )
        if total_transactions:
            summary = f"{summary} {total_transactions} reviewed transaction(s) are currently in scope."
        return summary

    if transaction_relevance == "supporting":
        summary = (
            f"{family} is being reviewed as a {basis.lower()} case with {direction.lower()} suspicion. "
            "Transaction activity is supporting evidence for this investigation."
        )
        if total_transactions:
            summary = f"{summary} {total_transactions} reviewed transaction(s) are available as supporting context."
        return summary

    summary = (
        f"{family} is being reviewed as a {basis.lower()} case with {direction.lower()} suspicion. "
        "Transaction activity is not the primary evidence source for this investigation."
    )
    if not total_transactions:
        summary = f"{summary} No meaningful transaction activity was retrieved in the selected window."
    if description_text:
        summary = f"{summary} Reported narrative: {description_text}"
    return summary


def _display_review_scope_label(analysis: dict[str, Any]) -> str:
    label = str(analysis.get("review_scope_label") or "").strip()
    if label:
        return label
    query_window = analysis.get("query_window") or {}
    return " to ".join(
        item
        for item in [
            query_window.get("start_datetime"),
            query_window.get("end_datetime"),
        ]
        if item
    ) or "The supplied review window"


def _build_non_primary_reasoning_summary(case_summary: str, sop_analysis: dict[str, Any], transaction_analysis: dict[str, Any]) -> str:
    supporting_text = (
        str((sop_analysis or {}).get("relevant_information") or "").strip()
        or str((sop_analysis or {}).get("sop_summary") or "").strip()
        or str((sop_analysis or {}).get("reason") or "").strip()
    )
    total_transactions = int((((transaction_analysis or {}).get("transaction_summary") or {}).get("total_transactions")) or 0)
    transaction_relevance = str((transaction_analysis or {}).get("transaction_relevance") or "supporting").strip().lower()
    case_family = str((transaction_analysis or {}).get("case_family") or "").strip()

    if transaction_relevance == "not_applicable" and supporting_text:
        filtered_sentences: list[str] = []
        transaction_noise_markers = [
            "rapid debit",
            "new beneficiary",
            "beneficiary usage",
            "suspicious post-disbursal",
            "post-disbursal transaction",
            "debit velocity",
            "collect request",
            "upi debit",
            "imps debit",
            "atm cash-out",
            "merchant transaction",
        ]
        for sentence in re.split(r"(?<=[.!?])\s+", supporting_text):
            cleaned = str(sentence or "").strip()
            lowered = cleaned.lower()
            if not cleaned:
                continue
            if case_family in {"Document Fraud", "Loan / Mortgage Fraud", "KYC / Identity Fraud"} and any(marker in lowered for marker in transaction_noise_markers):
                continue
            filtered_sentences.append(cleaned)
        supporting_text = " ".join(filtered_sentences).strip()

    if transaction_relevance == "supporting":
        transaction_note = (
            f"{total_transactions} reviewed transaction(s) are available as supporting context and do not override the primary {str((transaction_analysis or {}).get('investigation_basis') or 'case').strip().lower()} concern."
            if total_transactions
            else "Supporting transaction context is limited, so the conclusion rests mainly on the non-transaction case evidence."
        )
    else:
        transaction_note = (
            "Transaction activity is not the primary evidence source for this case."
            if total_transactions
            else "No meaningful transaction activity is available, so the conclusion rests on the non-transaction case evidence."
        )

    parts = [str(case_summary or "").strip(), supporting_text, transaction_note]
    return " ".join(part for part in parts if part).strip()


def _extract_signal_matches(case_description: str, keywords: tuple[str, ...]) -> list[str]:
    normalized = _normalized_case_text(case_description)
    matched: list[str] = []
    for keyword in keywords:
        cleaned = str(keyword or "").strip().lower()
        if cleaned and cleaned in normalized and cleaned not in matched:
            matched.append(cleaned)
    return matched


def _build_case_family_card(title: str, summary: str, items: list[str], emphasis: str | None = None) -> dict[str, Any]:
    normalized_items: list[str] = []
    seen_items: set[str] = set()
    for item in items:
        text = str(item or "").strip()
        if not text:
            continue
        key = text.lower()
        if key in seen_items:
            continue
        seen_items.add(key)
        normalized_items.append(text)
    return {
        "title": str(title or "").strip(),
        "summary": str(summary or "").strip(),
        "items": normalized_items,
        "emphasis": str(emphasis or "").strip(),
    }


def _transaction_context_line(total_transactions: int, transaction_relevance: str) -> str:
    if transaction_relevance == "not_applicable":
        if total_transactions:
            return f"{total_transactions} account-history transaction(s) are available in relationship history, but they should remain background context and not drive the primary conclusion."
        return "No meaningful transaction activity is available, so the case should be reviewed through non-transaction evidence."
    if transaction_relevance == "supporting":
        return f"{total_transactions} reviewed transaction(s) are available as supporting context for the case narrative." if total_transactions else "Supporting transaction context is currently limited."
    return f"{total_transactions} reviewed transaction(s) are in primary scope for this investigation." if total_transactions else "Transaction review remains primary, but no meaningful activity was retrieved in the selected window."


def _safe_datetime(value: Any) -> dt.datetime | None:
    if isinstance(value, dt.datetime):
        return value
    if isinstance(value, str):
        try:
            return dt.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return None


def _sort_records_by_datetime(records: list[dict[str, Any]], field_name: str) -> list[dict[str, Any]]:
    return sorted(
        records,
        key=lambda item: _safe_datetime(item.get(field_name)) or dt.datetime.min,
        reverse=True,
    )


def _fetch_loan_accounts_for_customer(cif_id: str) -> list[dict[str, Any]]:
    if not cif_id:
        return []
    return list(get_loan_accounts_collection().find({"cif_id": cif_id}, {"_id": 0}).sort("sanctioned_at", -1))


def _fetch_collateral_records_for_customer(cif_id: str, loan_ids: list[str] | None = None) -> list[dict[str, Any]]:
    if not cif_id:
        return []
    query: dict[str, Any] = {"cif_id": cif_id}
    if loan_ids:
        query["loan_id"] = {"$in": loan_ids}
    return list(get_collateral_records_collection().find(query, {"_id": 0}).sort("last_verified_at", -1))


def _fetch_document_verifications_for_customer(
    cif_id: str,
    loan_ids: list[str] | None = None,
    collateral_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    if not cif_id:
        return []
    query: dict[str, Any] = {"cif_id": cif_id}
    or_conditions: list[dict[str, Any]] = []
    if loan_ids:
        or_conditions.append({"loan_id": {"$in": loan_ids}})
    if collateral_ids:
        or_conditions.append({"collateral_id": {"$in": collateral_ids}})
    if or_conditions:
        query["$or"] = or_conditions + [{"loan_id": None, "collateral_id": None}]
    return list(get_document_verifications_collection().find(query, {"_id": 0}).sort("submitted_at", -1))


def _fetch_case_events_for_customer(cif_id: str, case_family: str) -> list[dict[str, Any]]:
    if not cif_id:
        return []
    collection = get_case_events_collection()
    if case_family not in {"Mixed", "Manual Review", "Transaction Fraud"}:
        targeted = list(
            collection.find({"cif_id": cif_id, "case_family": case_family}, {"_id": 0}).sort("occurred_at", -1)
        )
        if targeted:
            return targeted
    return list(collection.find({"cif_id": cif_id}, {"_id": 0}).sort("occurred_at", -1))


def _select_primary_loan_account(loan_accounts: list[dict[str, Any]]) -> dict[str, Any]:
    if not loan_accounts:
        return {}
    ranked = sorted(
        loan_accounts,
        key=lambda item: (
            str(item.get("loan_status") or "").lower() in {"under_fraud_review", "legal_hold"},
            str(item.get("repayment_status") or "").lower() == "defaulted",
            int(item.get("days_past_due") or 0),
            float(item.get("outstanding_amount") or 0.0),
            _safe_datetime(item.get("updated_at")) or dt.datetime.min,
        ),
        reverse=True,
    )
    return ranked[0]


def _select_primary_collateral(collateral_records: list[dict[str, Any]]) -> dict[str, Any]:
    if not collateral_records:
        return {}
    ranked = sorted(
        collateral_records,
        key=lambda item: (
            str(item.get("verification_status") or "").lower() in HIGH_RISK_COLLATERAL_STATUSES,
            int(item.get("duplicate_collateral_hits") or 0),
            len(item.get("issues") or []),
            _safe_datetime(item.get("last_verified_at")) or dt.datetime.min,
        ),
        reverse=True,
    )
    return ranked[0]


def _build_related_data_summary(
    loan_accounts: list[dict[str, Any]],
    collateral_records: list[dict[str, Any]],
    document_verifications: list[dict[str, Any]],
    case_events: list[dict[str, Any]],
) -> dict[str, int]:
    return {
        "loan_accounts": len(loan_accounts),
        "collateral_records": len(collateral_records),
        "document_verifications": len(document_verifications),
        "case_events": len(case_events),
    }


def _summarize_loan_exposure(loan_accounts: list[dict[str, Any]]) -> dict[str, Any]:
    primary_loan = _select_primary_loan_account(loan_accounts)
    if not primary_loan:
        return {
            "available": False,
            "loan_count": 0,
            "summary": "No linked loan exposure record is currently available for this customer.",
        }

    repayment_status = str(primary_loan.get("repayment_status") or "unknown").replace("_", " ").title()
    loan_status = str(primary_loan.get("loan_status") or "unknown").replace("_", " ").title()
    days_past_due = int(primary_loan.get("days_past_due") or 0)
    summary = (
        f"Primary linked loan file {primary_loan.get('loan_account_number')} ({primary_loan.get('product_type')}) "
        f"is currently marked {loan_status.lower()} with repayment status {repayment_status.lower()}."
    )
    if days_past_due:
        summary = f"{summary} The account is {days_past_due} day(s) past due."

    return {
        "available": True,
        "loan_count": len(loan_accounts),
        "loan_id": str(primary_loan.get("loan_id") or "").strip(),
        "loan_account_number": str(primary_loan.get("loan_account_number") or "").strip(),
        "product_type": str(primary_loan.get("product_type") or "").strip(),
        "sanction_amount": float(primary_loan.get("sanction_amount") or 0.0),
        "disbursed_amount": float(primary_loan.get("disbursed_amount") or 0.0),
        "outstanding_amount": float(primary_loan.get("outstanding_amount") or 0.0),
        "overdue_amount": float(primary_loan.get("overdue_amount") or 0.0),
        "emi_amount": float(primary_loan.get("emi_amount") or 0.0),
        "sanctioned_at": _format_datetime(_safe_datetime(primary_loan.get("sanctioned_at"))) or "",
        "repayment_status": str(primary_loan.get("repayment_status") or "").strip(),
        "days_past_due": days_past_due,
        "last_repayment_at": _format_datetime(_safe_datetime(primary_loan.get("last_repayment_at"))) or "",
        "branch_name": str(primary_loan.get("branch_name") or "").strip(),
        "loan_status": str(primary_loan.get("loan_status") or "").strip(),
        "underwriting_flags": [str(item).strip() for item in primary_loan.get("underwriting_flags") or [] if str(item).strip()],
        "summary": summary,
    }


def _summarize_collateral_review(collateral_records: list[dict[str, Any]]) -> dict[str, Any]:
    primary_collateral = _select_primary_collateral(collateral_records)
    if not primary_collateral:
        return {
            "available": False,
            "collateral_count": 0,
            "summary": "No linked collateral record is currently available for review.",
        }

    verification_status = str(primary_collateral.get("verification_status") or "unknown").replace("_", " ").title()
    encumbrance_status = str(primary_collateral.get("encumbrance_status") or "unknown").replace("_", " ").title()
    summary = (
        f"Primary collateral {primary_collateral.get('collateral_type')} is currently marked {verification_status.lower()} "
        f"with encumbrance status {encumbrance_status.lower()}."
    )
    issues = [str(item).strip() for item in primary_collateral.get("issues") or [] if str(item).strip()]
    if issues:
        summary = f"{summary} Top issue: {issues[0]}"

    return {
        "available": True,
        "collateral_count": len(collateral_records),
        "collateral_id": str(primary_collateral.get("collateral_id") or "").strip(),
        "loan_id": str(primary_collateral.get("loan_id") or "").strip(),
        "collateral_type": str(primary_collateral.get("collateral_type") or "").strip(),
        "property_address": str(primary_collateral.get("property_address") or "").strip(),
        "declared_owner_name": str(primary_collateral.get("declared_owner_name") or "").strip(),
        "verified_owner_name": str(primary_collateral.get("verified_owner_name") or "").strip(),
        "declared_market_value": float(primary_collateral.get("declared_market_value") or 0.0),
        "assessed_value": float(primary_collateral.get("assessed_value") or 0.0),
        "verification_status": str(primary_collateral.get("verification_status") or "").strip(),
        "encumbrance_status": str(primary_collateral.get("encumbrance_status") or "").strip(),
        "registry_reference": str(primary_collateral.get("registry_reference") or "").strip(),
        "duplicate_collateral_hits": int(primary_collateral.get("duplicate_collateral_hits") or 0),
        "supporting_document_count": len(primary_collateral.get("supporting_document_ids") or []),
        "issues": issues,
        "summary": summary,
    }


def _summarize_document_review(document_verifications: list[dict[str, Any]]) -> dict[str, Any]:
    sorted_documents = _sort_records_by_datetime(document_verifications, "submitted_at")
    if not sorted_documents:
        return {
            "available": False,
            "documents_reviewed": 0,
            "failed_documents": 0,
            "summary": "No document-verification records are currently available for this customer.",
        }

    failed_documents = [
        row
        for row in sorted_documents
        if str(row.get("verification_status") or "").strip().lower() in HIGH_RISK_DOCUMENT_STATUSES
    ]
    mismatch_types = [
        str(row.get("mismatch_type") or "").strip().replace("_", " ")
        for row in failed_documents
        if str(row.get("mismatch_type") or "").strip()
    ]
    highlights = [
        f"{row.get('document_type')}: {row.get('mismatch_reason')}"
        for row in failed_documents[:3]
        if row.get("document_type") and row.get("mismatch_reason")
    ]
    latest_document = sorted_documents[0]
    summary = (
        f"{len(sorted_documents)} document verification record(s) are available, including {len(failed_documents)} "
        "flagged finding(s)."
    )
    if highlights:
        summary = f"{summary} Latest material finding: {highlights[0]}"

    return {
        "available": True,
        "documents_reviewed": len(sorted_documents),
        "failed_documents": len(failed_documents),
        "primary_mismatch_types": list(dict.fromkeys(item.title() for item in mismatch_types))[:4],
        "verification_statuses": list(
            dict.fromkeys(
                str(row.get("verification_status") or "").strip().replace("_", " ").title()
                for row in sorted_documents
                if str(row.get("verification_status") or "").strip()
            )
        ),
        "highlights": highlights,
        "latest_submitted_at": _format_datetime(_safe_datetime(latest_document.get("submitted_at"))) or "",
        "summary": summary,
    }


def _summarize_case_events(case_events: list[dict[str, Any]]) -> dict[str, Any]:
    sorted_events = _sort_records_by_datetime(case_events, "occurred_at")
    if not sorted_events:
        return {
            "available": False,
            "total_events": 0,
            "summary": "No linked case-event history is currently available for this customer.",
        }

    open_events = [event for event in sorted_events if str(event.get("status") or "").strip().lower() in {"open", "under_review"}]
    escalated_events = [event for event in sorted_events if str(event.get("status") or "").strip().lower() == "escalated"]
    high_severity_events = [event for event in sorted_events if str(event.get("severity") or "").strip().lower() == "high"]
    latest_event = sorted_events[0]
    highlights = [str(event.get("summary") or "").strip() for event in sorted_events[:3] if str(event.get("summary") or "").strip()]
    summary = (
        f"{len(sorted_events)} linked case event(s) are available, including {len(open_events)} active review item(s) "
        f"and {len(escalated_events)} escalated item(s)."
    )
    if highlights:
        summary = f"{summary} Most recent event: {highlights[0]}"

    return {
        "available": True,
        "total_events": len(sorted_events),
        "open_events": len(open_events),
        "escalated_events": len(escalated_events),
        "high_severity_events": len(high_severity_events),
        "recent_event_types": list(
            dict.fromkeys(
                str(event.get("event_type") or "").strip().replace("_", " ").title()
                for event in sorted_events
                if str(event.get("event_type") or "").strip()
            )
        )[:4],
        "latest_event_at": _format_datetime(_safe_datetime(latest_event.get("occurred_at"))) or "",
        "latest_status": str(latest_event.get("status") or "").strip(),
        "latest_summary": str(latest_event.get("summary") or "").strip(),
        "highlights": highlights,
        "summary": summary,
    }


def _load_non_transaction_case_context(customer: dict[str, Any], route: dict[str, Any]) -> dict[str, Any]:
    cif_id = str(customer.get("cif_id") or "").strip()
    case_family = str(route.get("case_family") or "Manual Review").strip()
    if not cif_id:
        return {
            "related_data_summary": _build_related_data_summary([], [], [], []),
            "loan_exposure": {"available": False, "loan_count": 0, "summary": ""},
            "collateral_review": {"available": False, "collateral_count": 0, "summary": ""},
            "document_review": {"available": False, "documents_reviewed": 0, "failed_documents": 0, "summary": ""},
            "case_event_summary": {"available": False, "total_events": 0, "summary": ""},
        }

    loan_accounts = _fetch_loan_accounts_for_customer(cif_id) if case_family in {"Loan / Mortgage Fraud", "Document Fraud", "Mixed"} else []
    loan_ids = [str(item.get("loan_id") or "").strip() for item in loan_accounts if str(item.get("loan_id") or "").strip()]
    collateral_records = (
        _fetch_collateral_records_for_customer(cif_id, loan_ids)
        if case_family in {"Loan / Mortgage Fraud", "Document Fraud", "Mixed"}
        else []
    )
    collateral_ids = [str(item.get("collateral_id") or "").strip() for item in collateral_records if str(item.get("collateral_id") or "").strip()]
    document_verifications = (
        _fetch_document_verifications_for_customer(cif_id, loan_ids, collateral_ids)
        if case_family in {"Loan / Mortgage Fraud", "Document Fraud", "KYC / Identity Fraud", "Mixed"}
        else []
    )
    case_events = (
        _fetch_case_events_for_customer(cif_id, case_family)
        if case_family in {"Loan / Mortgage Fraud", "Document Fraud", "KYC / Identity Fraud", "Dispute / First-Party Abuse", "Mixed"}
        else []
    )

    return {
        "related_data_summary": _build_related_data_summary(loan_accounts, collateral_records, document_verifications, case_events),
        "loan_exposure": _summarize_loan_exposure(loan_accounts),
        "collateral_review": _summarize_collateral_review(collateral_records),
        "document_review": _summarize_document_review(document_verifications),
        "case_event_summary": _summarize_case_events(case_events),
    }


def _build_context_backed_case_summary(route: dict[str, Any], analysis: dict[str, Any]) -> str:
    case_family = str(route.get("case_family") or "Manual Review").strip()
    suspicion_direction = str(route.get("suspicion_direction") or "Manual Review").strip()
    investigation_basis = str(route.get("investigation_basis") or "Mixed").strip()
    transaction_relevance = str(route.get("transaction_relevance") or "supporting").strip().lower()
    total_transactions = int(((analysis.get("transaction_summary") or {}).get("total_transactions")) or 0)
    related_summary = analysis.get("related_data_summary") or {}
    loan_exposure = analysis.get("loan_exposure") or {}
    collateral_review = analysis.get("collateral_review") or {}
    document_review = analysis.get("document_review") or {}
    case_event_summary = analysis.get("case_event_summary") or {}

    if case_family == "Loan / Mortgage Fraud":
        parts = [
            f"{case_family} is being reviewed as a {investigation_basis.lower()} case with {suspicion_direction.lower()} suspicion.",
            f"{int(related_summary.get('loan_accounts') or 0)} linked loan file(s) and {int(related_summary.get('document_verifications') or 0)} document verification record(s) are in scope.",
        ]
        if loan_exposure.get("available"):
            parts.append(
                f"Primary exposure is {loan_exposure.get('product_type')} with repayment status {str(loan_exposure.get('repayment_status') or '').replace('_', ' ').lower()} and {int(loan_exposure.get('days_past_due') or 0)} day(s) past due."
            )
        if collateral_review.get("available"):
            parts.append(
                f"Collateral verification is currently marked {str(collateral_review.get('verification_status') or '').replace('_', ' ').lower()}."
            )
        if transaction_relevance == "supporting" and total_transactions:
            parts.append(f"{total_transactions} reviewed transaction(s) are available as supporting context only.")
        if transaction_relevance == "not_applicable":
            parts.append("Transaction activity is not the primary evidence source for this investigation.")
        return " ".join(part for part in parts if part)

    if case_family == "Document Fraud":
        parts = [
            f"{case_family} is being reviewed as a {investigation_basis.lower()} case with {suspicion_direction.lower()} suspicion.",
            f"{int(document_review.get('documents_reviewed') or 0)} document verification record(s) are in scope, including {int(document_review.get('failed_documents') or 0)} flagged finding(s).",
        ]
        if case_event_summary.get("available"):
            parts.append(case_event_summary.get("summary") or "")
        if transaction_relevance == "supporting":
            parts.append("Transaction activity remains secondary to the verified document findings.")
            if total_transactions:
                parts.append(f"{total_transactions} reviewed transaction(s) are available only as supporting context.")
        elif transaction_relevance == "not_applicable":
            parts.append("Document-authenticity findings remain primary, and transaction activity should stay in background context only.")
        return " ".join(part for part in parts if part)

    if case_family == "KYC / Identity Fraud":
        parts = [
            f"{case_family} is being reviewed as a {investigation_basis.lower()} case with {suspicion_direction.lower()} suspicion.",
            f"{int(document_review.get('documents_reviewed') or 0)} linked verification record(s) and {int(case_event_summary.get('total_events') or 0)} case-event record(s) are available for profile review.",
            "Identity, onboarding, and profile-change evidence should drive the primary conclusion.",
        ]
        if transaction_relevance == "supporting" and total_transactions:
            parts.append(f"{total_transactions} reviewed transaction(s) are available as secondary context.")
        return " ".join(part for part in parts if part)

    if case_family == "Dispute / First-Party Abuse":
        parts = [
            f"{case_family} is being reviewed as a {investigation_basis.lower()} case with {suspicion_direction.lower()} suspicion.",
            f"{int(case_event_summary.get('total_events') or 0)} complaint or case-event record(s) are currently in scope.",
        ]
        if transaction_relevance == "supporting":
            parts.append("Transaction activity is supporting evidence for this dispute-focused investigation.")
            if total_transactions:
                parts.append(f"{total_transactions} reviewed transaction(s) remain available for authorization cross-checks.")
        return " ".join(part for part in parts if part)

    return str(analysis.get("case_summary") or "").strip()


def _apply_case_context_to_analysis(route: dict[str, Any], analysis: dict[str, Any], customer: dict[str, Any], case_description: str) -> dict[str, Any]:
    enriched = dict(analysis)
    case_family = str(route.get("case_family") or "Manual Review").strip()
    context = _load_non_transaction_case_context(customer, route)
    enriched.update(context)

    related_summary = context.get("related_data_summary") or {}
    loan_exposure = context.get("loan_exposure") or {}
    collateral_review = context.get("collateral_review") or {}
    document_review = context.get("document_review") or {}
    case_event_summary = context.get("case_event_summary") or {}

    if case_family == "Loan / Mortgage Fraud":
        if collateral_review.get("available") and str(collateral_review.get("verification_status") or "").lower() in HIGH_RISK_COLLATERAL_STATUSES:
            enriched["risk_score"] = max(int(enriched.get("risk_score") or 0), 84)
            enriched["fraud_classification"] = "Fake or duplicated collateral"
        elif loan_exposure.get("available") and str(loan_exposure.get("repayment_status") or "").lower() == "defaulted":
            enriched["risk_score"] = max(int(enriched.get("risk_score") or 0), 78)
        if case_event_summary.get("high_severity_events"):
            enriched["risk_score"] = max(int(enriched.get("risk_score") or 0), 82)

        enriched["recommended_actions"] = _merge_unique_actions(
            [
                "Validate submitted collateral ownership, legal enforceability, and any duplication across linked credit cases.",
                "Review loan sanction, repayment/default status, and supporting underwriting documents with credit-risk and legal teams.",
                "Escalate the case for coordinated fraud, recovery, and legal review before any closure decision.",
            ],
            enriched.get("recommended_actions") or [],
        )

    if case_family == "Document Fraud" and document_review.get("failed_documents"):
        enriched["risk_score"] = max(int(enriched.get("risk_score") or 0), 76)
        enriched["fraud_classification"] = "Forged or mismatched submitted documents"
        enriched["recommended_actions"] = _merge_unique_actions(
            [
                "Verify the submitted document set against source records and preserve the exact mismatch trail.",
                "Escalate the case for manual document-review controls and linked exposure checks before any approval decision.",
            ],
            enriched.get("recommended_actions") or [],
        )

    if case_family == "KYC / Identity Fraud" and (document_review.get("failed_documents") or case_event_summary.get("high_severity_events")):
        enriched["risk_score"] = max(int(enriched.get("risk_score") or 0), 71)
        enriched["fraud_classification"] = "Identity or onboarding mismatch requiring enhanced due diligence"

    if case_family == "Dispute / First-Party Abuse" and case_event_summary.get("total_events"):
        enriched["risk_score"] = max(int(enriched.get("risk_score") or 0), 61)
        enriched["fraud_classification"] = "Repeat complaint or first-party abuse pattern requiring validation"
        enriched["recommended_actions"] = _merge_unique_actions(
            [
                "Review complaint history, authorization evidence, and repeat dispute behaviour before assigning liability.",
            ],
            enriched.get("recommended_actions") or [],
        )

    if str(route.get("transaction_relevance") or "").strip().lower() in {"supporting", "not_applicable"}:
        enriched["risk_level"] = _risk_level_from_score(int(enriched.get("risk_score") or 0))

    enriched["case_summary"] = _build_context_backed_case_summary(route, enriched)
    enriched["family_cards"] = _build_family_evidence_cards(case_description, route, enriched)
    return enriched


def _build_family_evidence_cards(case_description: str, route: dict[str, Any], analysis: dict[str, Any]) -> list[dict[str, Any]]:
    family = str(route.get("case_family") or "Manual Review").strip()
    transaction_relevance = str(route.get("transaction_relevance") or "supporting").strip().lower()
    total_transactions = int(((analysis.get("transaction_summary") or {}).get("total_transactions")) or 0)
    common_context = _transaction_context_line(total_transactions, transaction_relevance)
    loan_exposure = analysis.get("loan_exposure") or {}
    collateral_review = analysis.get("collateral_review") or {}
    document_review = analysis.get("document_review") or {}
    case_event_summary = analysis.get("case_event_summary") or {}

    if family == "Loan / Mortgage Fraud":
        loan_matches = _extract_signal_matches(case_description, LOAN_SIGNAL_KEYWORDS)
        document_matches = _extract_signal_matches(case_description, DOCUMENT_SIGNAL_KEYWORDS)
        loan_items = [
            f"Primary basis: {route.get('investigation_basis') or 'Loan-Led'} review with {route.get('suspicion_direction') or 'customer-to-bank'} suspicion.",
        ]
        if loan_exposure.get("available"):
            loan_items.extend(
                [
                    f"Primary linked file: {loan_exposure.get('loan_account_number')} | {loan_exposure.get('product_type')} | Branch: {loan_exposure.get('branch_name') or 'N/A'}",
                    f"Sanctioned {_format_rupees(float(loan_exposure.get('sanction_amount') or 0.0))} | Outstanding {_format_rupees(float(loan_exposure.get('outstanding_amount') or 0.0))} | Overdue {_format_rupees(float(loan_exposure.get('overdue_amount') or 0.0))}",
                    f"Repayment status: {str(loan_exposure.get('repayment_status') or 'unknown').replace('_', ' ').title()} | DPD: {int(loan_exposure.get('days_past_due') or 0)} | Loan status: {str(loan_exposure.get('loan_status') or 'unknown').replace('_', ' ').title()}",
                ]
            )
            if loan_exposure.get("underwriting_flags"):
                loan_items.append(f"Loan-file flags: {', '.join(loan_exposure.get('underwriting_flags') or [])}")
        else:
            loan_items.append("Validate sanction, repayment/default status, and whether the reported issue affects enforceability of the lending decision.")
        loan_items.append(common_context)

        collateral_items = [
            f"Reported signals from the narrative: {', '.join(loan_matches + document_matches) if loan_matches or document_matches else 'loan exposure, collateral, or document inconsistency'}",
        ]
        if collateral_review.get("available"):
            collateral_items.extend(
                [
                    f"Primary collateral: {collateral_review.get('collateral_type')} | Status: {str(collateral_review.get('verification_status') or 'unknown').replace('_', ' ').title()} | Encumbrance: {str(collateral_review.get('encumbrance_status') or 'unknown').replace('_', ' ').title()}",
                    f"Declared owner: {collateral_review.get('declared_owner_name') or 'N/A'} | Verified owner: {collateral_review.get('verified_owner_name') or 'N/A'}",
                    f"Property address: {collateral_review.get('property_address') or 'N/A'}",
                ]
            )
            if collateral_review.get("issues"):
                collateral_items.append(f"Top issues: {'; '.join((collateral_review.get('issues') or [])[:3])}")
        else:
            collateral_items.extend(
                [
                    "Verify title deed, registry extract, ownership match, valuation trail, and encumbrance status against source records.",
                    "If property or mortgage enforceability fails, mark the collateral trail as compromised and escalate for legal and recovery review.",
                ]
            )

        repayment_items: list[str] = []
        if case_event_summary.get("available"):
            repayment_items.append(case_event_summary.get("summary") or "")
            if case_event_summary.get("highlights"):
                repayment_items.extend((case_event_summary.get("highlights") or [])[:3])
        else:
            repayment_items.append("Credit, fraud, recovery, and legal stakeholders should align before any closure or recovery decision.")
        if document_review.get("available"):
            repayment_items.append(
                f"Document checks reviewed: {int(document_review.get('documents_reviewed') or 0)} | Flagged findings: {int(document_review.get('failed_documents') or 0)}"
            )
        return [
            _build_case_family_card(
                "Loan Exposure Review",
                "This case should be assessed as a customer-to-bank lending exposure review rather than a normal payment-fraud investigation.",
                loan_items,
                loan_exposure.get("summary") or "Credit, fraud, recovery, and legal stakeholders should align before any closure or recovery decision.",
            ),
            _build_case_family_card(
                "Collateral and Document Focus",
                "Collateral ownership, legal enforceability, and submitted property papers are the critical evidence sources for this case family.",
                collateral_items,
                collateral_review.get("summary") or "Transaction review should remain secondary unless linked fund diversion or payout activity is separately identified.",
            ),
            _build_case_family_card(
                "Repayment Snapshot",
                "Repayment status, delinquency progression, and linked escalations help settle whether the case reflects collateral fraud, default, or both.",
                repayment_items,
                case_event_summary.get("latest_summary") or "Document integrity and recovery escalations should be reviewed together before closure.",
            ),
        ]

    if family == "Document Fraud":
        document_matches = _extract_signal_matches(case_description, DOCUMENT_SIGNAL_KEYWORDS)
        document_items = [
            f"Reported document signals: {', '.join(document_matches) if document_matches else 'possible forged, fake, or mismatched submission'}",
        ]
        if document_review.get("available"):
            document_items.extend(
                [
                    f"Documents reviewed: {int(document_review.get('documents_reviewed') or 0)} | Flagged findings: {int(document_review.get('failed_documents') or 0)}",
                    f"Verification statuses: {', '.join(document_review.get('verification_statuses') or []) or 'No verification status recorded'}",
                ]
            )
            if document_review.get("primary_mismatch_types"):
                document_items.append(f"Primary mismatch types: {', '.join(document_review.get('primary_mismatch_types') or [])}")
        else:
            document_items.extend(
                [
                    "Capture the exact document set in dispute and compare each item against issuing-source records or approved verification checkpoints.",
                    common_context,
                ]
            )

        escalation_items = []
        if case_event_summary.get("available"):
            escalation_items.append(case_event_summary.get("summary") or "")
        if document_review.get("highlights"):
            escalation_items.extend((document_review.get("highlights") or [])[:3])
        if loan_exposure.get("available"):
            escalation_items.append(
                f"Linked exposure: {loan_exposure.get('product_type')} | Loan status: {str(loan_exposure.get('loan_status') or 'unknown').replace('_', ' ').title()} | Branch: {loan_exposure.get('branch_name') or 'N/A'}"
            )
        if not escalation_items:
            escalation_items = [
                "Record whether the mismatch is forged content, impersonation, altered ownership, fake income proof, or unsupported valuation evidence.",
                "Check whether the same document trail, PAN, address, or contact identity appears across linked accounts or applications.",
                "Escalate for manual document review and preserve the evidence chain for legal or disciplinary action if fraud is confirmed.",
            ]
        return [
            _build_case_family_card(
                "Document Verification Focus",
                "The investigator should prioritise source verification, mismatch documentation, and authenticity checks over payment-pattern analysis.",
                document_items,
                document_review.get("summary") or "Document-led cases should record the mismatch type clearly before final fraud categorisation.",
            ),
            _build_case_family_card(
                "Mismatch and Escalation Checks",
                "This case family is settled through document-authenticity findings, linked exposure review, and formal escalation rather than transaction-only screening.",
                escalation_items,
                case_event_summary.get("latest_summary") or "",
            ),
        ]

    if family == "KYC / Identity Fraud":
        identity_matches = _extract_signal_matches(case_description, IDENTITY_SIGNAL_KEYWORDS)
        identity_items = [
            f"Reported profile signals: {', '.join(identity_matches) if identity_matches else 'identity, KYC, or onboarding inconsistency'}",
        ]
        if document_review.get("available"):
            identity_items.extend(
                [
                    f"Verification records reviewed: {int(document_review.get('documents_reviewed') or 0)} | Flagged findings: {int(document_review.get('failed_documents') or 0)}",
                    f"Mismatch types: {', '.join(document_review.get('primary_mismatch_types') or []) or 'Not yet captured'}",
                ]
            )
        else:
            identity_items.extend(
                [
                    "Validate PAN, Aadhaar, registered mobile, email, and onboarding records against the current customer profile.",
                    "Check whether the same identifiers or profile changes appear across multiple linked customers or recently updated accounts.",
                ]
            )

        profile_items = []
        if case_event_summary.get("available"):
            profile_items.append(case_event_summary.get("summary") or "")
            profile_items.extend((case_event_summary.get("highlights") or [])[:2])
        if document_review.get("highlights"):
            profile_items.extend((document_review.get("highlights") or [])[:2])
        if not profile_items:
            profile_items = [
                "Review mobile/email changes, profile resets, KYC refresh events, and account-opening anomalies around the reported timeline.",
                common_context,
                "Escalate for linked-account review if the same identity trail appears across multiple suspicious records.",
            ]
        return [
            _build_case_family_card(
                "Identity and KYC Review",
                "The case should be reviewed through profile authenticity, onboarding integrity, and identity-linkage checks.",
                identity_items,
                document_review.get("summary") or "If identity authenticity is weak, treat transaction evidence as secondary until profile verification is completed.",
            ),
            _build_case_family_card(
                "Profile Event Checks",
                "Recent profile changes often explain whether the case reflects impersonation, synthetic identity, or manipulated onboarding.",
                profile_items,
                case_event_summary.get("latest_summary") or "",
            ),
        ]

    if family == "Dispute / First-Party Abuse":
        dispute_matches = _extract_signal_matches(case_description, DISPUTE_SIGNAL_KEYWORDS)
        dispute_items = [
            f"Reported complaint signals: {', '.join(dispute_matches) if dispute_matches else 'merchant dispute, denial, or repeat complaint behaviour'}",
        ]
        if case_event_summary.get("available"):
            dispute_items.append(case_event_summary.get("summary") or "")
        dispute_items.append(common_context)

        authorization_items = []
        if case_event_summary.get("highlights"):
            authorization_items.extend((case_event_summary.get("highlights") or [])[:3])
        if not authorization_items:
            authorization_items = [
                "Check prior complaint frequency, repeat dispute themes, and whether similar transactions were previously confirmed as valid.",
                "Review merchant descriptors, card-present indicators, approval flow, and any customer acknowledgement trail.",
                "Escalate only after complaint behaviour and supporting evidence are documented together.",
            ]
        return [
            _build_case_family_card(
                "Complaint and Dispute Review",
                "This case should be assessed through the customer complaint narrative, prior dispute behaviour, and authorization evidence.",
                dispute_items,
                "Do not treat the complaint itself as proof; validate whether the evidence supports or weakens the customer denial.",
            ),
            _build_case_family_card(
                "Authorization Evidence",
                "First-party abuse reviews become stronger when complaint history and authorization artefacts point away from genuine compromise.",
                authorization_items,
                case_event_summary.get("latest_summary") or "",
            ),
        ]

    if family == "Mule / Funnel Account":
        mule_matches = _extract_signal_matches(case_description, MULE_SIGNAL_KEYWORDS)
        return [
            _build_case_family_card(
                "Linked Account and Beneficiary Review",
                "This case remains transaction-led, but the investigator should explicitly test for layering, pass-through behaviour, and linked-account movement.",
                [
                    f"Reported mule indicators: {', '.join(mule_matches) if mule_matches else 'mule, funnel, or rapid pass-through behaviour'}",
                    "Trace whether inward credits are followed by rapid outward debits, cash-out, or repeated beneficiary routing.",
                    common_context,
                ],
                "Linked-account and beneficiary tracing should be treated as a core evidence stream for mule-like cases.",
            ),
        ]

    if family == "Mixed":
        return [
            _build_case_family_card(
                "Primary Review Basis",
                "The narrative contains mixed indicators, so the case should be settled through a combined review rather than one evidence source alone.",
                [
                    "Review both transaction evidence and non-transaction records before choosing the final fraud category.",
                    common_context,
                    "Use the SOP category that best fits the strongest verified evidence, not just the first reported signal.",
                ],
                "Mixed cases should remain open until the leading evidence source is clear.",
            ),
        ]

    if family == "Manual Review":
        return [
            _build_case_family_card(
                "Primary Review Basis",
                "The current narrative does not yet point strongly to one fraud family, so the investigator should gather stronger evidence before final categorisation.",
                [
                    "Confirm the case narrative, customer context, and date-range evidence before final review.",
                    common_context,
                    "Use the SOP only after the case family becomes clearer from the verified facts.",
                ],
            ),
        ]

    return []


def _refine_case_route_with_transactions(route: dict[str, Any], analysis: dict[str, Any], review_scope_mode: str | None = None) -> dict[str, Any]:
    total_transactions = int(((analysis.get("transaction_summary") or {}).get("total_transactions")) or 0)
    patterns = analysis.get("suspicious_patterns") or []
    classification = str(analysis.get("fraud_classification") or "").strip()
    normalized_scope_mode = str(review_scope_mode or "").strip().lower()
    refined = {
        **route,
        "evidence_modules_used": list(route.get("evidence_modules_used") or []),
        "matched_keywords": list(route.get("matched_keywords") or []),
    }

    if refined.get("case_family") == "Manual Review" and total_transactions and (patterns or not _is_generic_classification(classification)):
        return _case_route("Transaction Fraud", transaction_relevance="primary")

    if (
        refined.get("case_family") in {"Loan / Mortgage Fraud", "Document Fraud", "KYC / Identity Fraud"}
        and total_transactions
        and normalized_scope_mode
        and normalized_scope_mode != "relationship_history"
    ):
        refined["transaction_relevance"] = "supporting"
        for module in ["Transaction Review", "Transaction Timeline"]:
            if module not in refined["evidence_modules_used"]:
                refined["evidence_modules_used"].append(module)

    if refined.get("case_family") == "Mixed" and not total_transactions:
        refined["transaction_relevance"] = "not_applicable"

    return refined


def _apply_case_route_to_analysis(case_description: str, route: dict[str, Any], analysis: dict[str, Any]) -> dict[str, Any]:
    metadata = CASE_FAMILY_METADATA.get(str(route.get("case_family") or "").strip(), CASE_FAMILY_METADATA["Manual Review"])
    transaction_relevance = str(route.get("transaction_relevance") or metadata.get("transaction_relevance") or "supporting").strip().lower()
    adjusted = dict(analysis)

    adjusted["case_family"] = str(route.get("case_family") or "Manual Review").strip()
    adjusted["suspicion_direction"] = str(route.get("suspicion_direction") or metadata.get("suspicion_direction") or "Manual Review").strip()
    adjusted["investigation_basis"] = str(route.get("investigation_basis") or metadata.get("investigation_basis") or "Mixed").strip()
    adjusted["transaction_relevance"] = transaction_relevance
    adjusted["evidence_modules_used"] = list(route.get("evidence_modules_used") or metadata.get("evidence_modules") or [])
    adjusted["case_summary"] = _build_case_summary(case_description, route, adjusted)
    adjusted["family_cards"] = _build_family_evidence_cards(case_description, route, adjusted)

    if transaction_relevance == "not_applicable":
        adjusted["risk_score"] = int(metadata.get("default_risk_score") or 0)
        adjusted["risk_level"] = _risk_level_from_score(int(adjusted.get("risk_score") or 0))
    elif transaction_relevance == "supporting":
        adjusted["risk_score"] = max(int(adjusted.get("risk_score") or 0), int(metadata.get("default_risk_score") or 0))
        adjusted["risk_level"] = _risk_level_from_score(int(adjusted.get("risk_score") or 0))

    if _is_generic_classification(adjusted.get("fraud_classification")):
        adjusted["fraud_classification"] = str(metadata.get("default_classification") or adjusted.get("fraud_classification") or "Manual review required")

    if transaction_relevance == "primary":
        current_actions = adjusted.get("recommended_actions") if isinstance(adjusted.get("recommended_actions"), list) else []
        adjusted["recommended_actions"] = current_actions or list(metadata.get("default_actions") or [])
        if not str(adjusted.get("reasoning_summary") or "").strip():
            adjusted["reasoning_summary"] = adjusted["case_summary"]
        return adjusted

    if transaction_relevance == "supporting":
        existing_summary = str(adjusted.get("reasoning_summary") or "").strip()
        adjusted["reasoning_summary"] = (
            f"{adjusted['case_summary']} {existing_summary}".strip()
            if existing_summary and existing_summary != adjusted["case_summary"]
            else adjusted["case_summary"]
        )
        adjusted["recommended_actions"] = _merge_unique_actions(
            list(metadata.get("default_actions") or []),
            adjusted.get("recommended_actions") or [],
        )
        return adjusted

    adjusted["reasoning_summary"] = adjusted["case_summary"]
    adjusted["recommended_actions"] = _merge_unique_actions(
        list(metadata.get("default_actions") or []),
        adjusted.get("recommended_actions") or [],
    )
    return adjusted


def _is_reset_query(query: str) -> bool:
    normalized = _normalize_query(query).lower()
    return normalized in {
        "reset",
        "start over",
        "restart",
        "new case",
        "clear",
        "clear state",
    }


def _is_relationship_scope_query(query: str) -> bool:
    normalized = _normalize_query(query).lower()
    if not normalized:
        return False

    patterns = [
        r"\bsince (?:the )?account (?:was )?(?:created|opened|opening)\b",
        r"\bsince opening\b",
        r"\bsince inception\b",
        r"\bsince the beginning\b",
        r"\bfull relationship history\b",
        r"\bfull account history\b",
        r"\bentire account history\b",
        r"\ball transaction history\b",
        r"\bentire history\b",
        r"\bfrom account opening\b",
        r"\bfrom the start\b",
    ]
    return any(re.search(pattern, normalized) for pattern in patterns)


def _extract_cif_id(query: str) -> str | None:
    match = re.search(r"\bCIF(?:\s*ID)?[\s\-_/:#]*?(\d{4,6})\b", query or "", flags=re.IGNORECASE)
    if match:
        return f"CIF{match.group(1)}"

    shorthand = re.fullmatch(r"\s*(\d{4,6})\s*", query or "")
    if shorthand:
        return f"CIF{shorthand.group(1)}"
    return None


def _extract_mobile(query: str) -> str | None:
    match = re.search(r"(?<!\d)([6-9]\d{9})(?!\d)", query or "")
    if not match:
        return None
    return match.group(1)


def _extract_account_id(query: str) -> str | None:
    match = re.search(r"\b(?:ACC\d{4,}|[1-9]\d{11,17})\b", query or "", flags=re.IGNORECASE)
    if not match:
        return None
    value = match.group(0).strip()
    return value.upper() if value.lower().startswith("acc") else value


def _extract_pan(query: str) -> str | None:
    match = re.search(r"\b([A-Z]{5}\d{4}[A-Z])\b", query or "", flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(1).upper()


def _extract_name_fragment(query: str) -> str | None:
    if _is_relationship_scope_query(query):
        return None

    cleaned = re.sub(r"\bCIF\d{4,}\b", " ", query or "", flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(?:ACC\d{4,}|[1-9]\d{11,17})\b", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b[A-Z]{5}\d{4}[A-Z]\b", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"(?<!\d)([6-9]\d{9})(?!\d)", " ", cleaned)
    cleaned = re.sub(r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?\b", " ", cleaned)
    cleaned = re.sub(r"\b\d{1,2}[-/]\d{1,2}[-/]\d{4}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?\b", " ", cleaned)
    cleaned = re.sub(r"\b(?:today|yesterday|last|past|days?|debit|debits|upi|imps|atm|pos|netbanking|transaction|transactions|reported|report|review|check|analyse|analyze|inspect|fraud|scam|case|customer|from|to|between|range|without|approval|money|withdrawal|withdrawals|unauthorised|unauthorized|since|account|created|opened|opening|history|entire|full|relationship|start|beginning)\b", " ", cleaned, flags=re.IGNORECASE)
    tokens = [token for token in re.findall(r"[A-Za-z]+", cleaned) if len(token) >= 1]
    if 1 <= len(tokens) <= 4:
        candidate = " ".join(token.title() for token in tokens)
        if candidate.lower() not in {"since", "account", "created", "opened", "history", "full history"} and not _is_generic_name_fragment(candidate, query, "loose_tokens"):
            return candidate

    action_match = re.search(
        r"^\s*([A-Za-z]+(?:\s+[A-Za-z]+){0,2})\s+"
        r"(?:clicked|clicks|received|got|lost|shared|entered|used|opened|reported|faced|saw|noticed|sent|transferred|made|paid|withdrew|withdrawn|complained)\b",
        query or "",
        flags=re.IGNORECASE,
    )
    if action_match:
        candidate = " ".join(token.title() for token in re.findall(r"[A-Za-z]+", action_match.group(1)))
        if candidate.lower() not in {"customer", "user", "victim", "account", "he", "she", "they"} and not _is_generic_name_fragment(candidate, query, "action_subject"):
            return candidate

    leading_name = re.match(r"^\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})\b", query or "")
    if leading_name:
        candidate = " ".join(token.title() for token in re.findall(r"[A-Za-z]+", leading_name.group(1)))
        if candidate.lower() not in {"customer", "user", "victim", "account"} and not _is_generic_name_fragment(candidate, query, "leading_name"):
            return candidate

    return None


def _query_looks_like_case_narrative(query: str) -> bool:
    tokens = [token.lower() for token in re.findall(r"[A-Za-z]+", str(query or ""))]
    if len(tokens) < 4:
        return False

    narrative_terms = {
        "customer",
        "account",
        "loan",
        "mortgage",
        "property",
        "documents",
        "document",
        "aadhaar",
        "aadhar",
        "pan",
        "kyc",
        "identity",
        "onboarding",
        "fraud",
        "scam",
        "transaction",
        "transactions",
        "beneficiary",
        "debit",
        "debits",
        "credit",
        "credits",
        "repayment",
        "defaulted",
        "default",
        "submitted",
        "provided",
        "used",
        "shared",
        "clicked",
        "reported",
        "denied",
        "mismatched",
        "mismatch",
    }
    hits = sum(1 for token in tokens if token in narrative_terms)
    return hits >= 2


def _is_generic_name_fragment(value: str | None, query: str | None = None, source: str | None = None) -> bool:
    tokens = [token.lower() for token in re.findall(r"[A-Za-z]+", str(value or ""))]
    if not tokens:
        return True

    generic_leads = {"customer", "account", "user", "victim", "borrower", "client", "member", "holder"}
    non_name_context_terms = {
        "customer",
        "account",
        "user",
        "victim",
        "borrower",
        "client",
        "member",
        "holder",
        "reported",
        "submitted",
        "provided",
        "used",
        "shared",
        "clicked",
        "onboarding",
        "loan",
        "mortgage",
        "property",
        "documents",
        "document",
        "aadhaar",
        "aadhar",
        "pan",
        "kyc",
        "identity",
        "fraud",
        "details",
        "mismatched",
        "mismatch",
    }
    if tokens[0] in generic_leads:
        return True
    if all(token in non_name_context_terms for token in tokens):
        return True

    generic_ratio = sum(1 for token in tokens if token in non_name_context_terms) / max(len(tokens), 1)
    if generic_ratio >= 0.67:
        return True

    normalized = " ".join(tokens)
    if normalized in {"the customer", "the user", "the victim", "account holder", "bank customer"}:
        return True

    if _query_looks_like_case_narrative(query or "") and (source or "") != "action_subject":
        return True
    return False


def _name_tokens(value: str) -> list[str]:
    return [token for token in re.findall(r"[a-z]+", (value or "").lower()) if token]


def _matches_name_fragment(fragment: str, customer_name: str) -> bool:
    fragment_tokens = _name_tokens(fragment)
    customer_tokens = _name_tokens(customer_name)
    if not fragment_tokens or len(fragment_tokens) > len(customer_tokens):
        return False
    for index, token in enumerate(fragment_tokens):
        if not customer_tokens[index].startswith(token):
            return False
    return True


def _parse_explicit_datetime(raw_value: str) -> tuple[dt.datetime, bool] | None:
    cleaned = raw_value.strip().replace("T", " ")
    has_time = ":" in cleaned

    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
    ]

    for fmt in formats:
        try:
            return dt.datetime.strptime(cleaned, fmt), has_time
        except ValueError:
            continue

    return None


def _extract_explicit_datetime_candidates(query: str) -> list[dict[str, Any]]:
    patterns = [
        r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?\b",
        r"\b\d{1,2}[-/]\d{1,2}[-/]\d{4}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?\b",
    ]
    seen_spans: set[tuple[int, int]] = set()
    candidates: list[dict[str, Any]] = []

    for pattern in patterns:
        for match in re.finditer(pattern, query or ""):
            span = match.span()
            if span in seen_spans:
                continue
            parsed = _parse_explicit_datetime(match.group(0))
            if not parsed:
                continue
            seen_spans.add(span)
            value, has_time = parsed
            candidates.append(
                {
                    "value": value,
                    "has_time": has_time,
                    "start": span[0],
                    "raw": match.group(0),
                }
            )

    candidates.sort(key=lambda item: item["start"])
    return candidates


def _extract_date_window(query: str, now: dt.datetime) -> dict[str, Any]:
    normalized = _normalize_query(query).lower()

    relative_match = re.search(r"\b(?:last|past)\s+(\d{1,2})\s+days?\b", normalized)
    if relative_match:
        days = max(1, int(relative_match.group(1)))
        end = now
        start = now - dt.timedelta(days=days)
        return {
            "detected": True,
            "clear_start": False,
            "clear_end": False,
            "start_datetime": _format_datetime(start),
            "end_datetime": _format_datetime(end),
        }

    if "today" in normalized:
        return {
            "detected": True,
            "clear_start": False,
            "clear_end": False,
            "start_datetime": _format_datetime(_start_of_day(now)),
            "end_datetime": _format_datetime(now),
        }

    if "yesterday" in normalized:
        yesterday = now - dt.timedelta(days=1)
        return {
            "detected": True,
            "clear_start": False,
            "clear_end": False,
            "start_datetime": _format_datetime(_start_of_day(yesterday)),
            "end_datetime": _format_datetime(_end_of_day(yesterday)),
        }

    candidates = _extract_explicit_datetime_candidates(query)
    if len(candidates) >= 2:
        first = candidates[0]
        second = candidates[1]
        start = first["value"] if first["has_time"] else _start_of_day(first["value"])
        end = second["value"] if second["has_time"] else _end_of_day(second["value"])
        if end < start:
            start, end = end, start
        return {
            "detected": True,
            "clear_start": False,
            "clear_end": False,
            "start_datetime": _format_datetime(start),
            "end_datetime": _format_datetime(end),
        }

    if len(candidates) == 1:
        candidate = candidates[0]
        if not candidate["has_time"]:
            single_day = candidate["value"]
            return {
                "detected": True,
                "clear_start": False,
                "clear_end": False,
                "start_datetime": _format_datetime(_start_of_day(single_day)),
                "end_datetime": _format_datetime(_end_of_day(single_day)),
            }

        if any(token in normalized for token in ["until", "before", "to "]):
            return {
                "detected": True,
                "clear_start": True,
                "clear_end": False,
                "start_datetime": None,
                "end_datetime": _format_datetime(candidate["value"]),
            }

        return {
            "detected": True,
            "clear_start": False,
            "clear_end": True,
            "start_datetime": _format_datetime(candidate["value"]),
            "end_datetime": None,
        }

    return {
        "detected": False,
        "clear_start": False,
        "clear_end": False,
        "start_datetime": None,
        "end_datetime": None,
    }


def _all_customers() -> list[dict[str, Any]]:
    return list(get_customers_collection().find({}, {"_id": 0}))


def _serialize_customer(customer: dict[str, Any] | None) -> dict[str, Any]:
    if not customer:
        return {}

    return {
        "cif_id": str(customer.get("cif_id") or "").strip(),
        "name": str(customer.get("name") or "").strip(),
        "mobile": str(customer.get("mobile") or "").strip(),
        "pan": str(customer.get("pan") or "").strip(),
        "accounts": [str(item).strip() for item in customer.get("accounts") or [] if str(item).strip()],
    }


def _normalize_resolved_customer_payload(resolved_customer: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(resolved_customer, dict):
        return {}

    normalized = {
        "cif_id": str(resolved_customer.get("cif_id") or "").strip(),
        "name": str(resolved_customer.get("name") or "").strip(),
        "mobile": str(resolved_customer.get("mobile") or "").strip(),
        "pan": str(resolved_customer.get("pan") or "").strip(),
        "accounts": [str(item).strip() for item in resolved_customer.get("accounts") or [] if str(item).strip()],
    }
    if normalized["cif_id"] or normalized["name"] or normalized["mobile"] or normalized["pan"] or normalized["accounts"]:
        return normalized
    return {}


def _has_resolved_customer(state_or_customer: dict[str, Any] | None) -> bool:
    normalized = _normalize_resolved_customer_payload(state_or_customer)
    return bool(normalized.get("cif_id"))


def _resolve_customer(query: str, state: dict[str, Any]) -> tuple[dict[str, Any] | None, list[dict[str, Any]], str | None]:
    customers_collection = get_customers_collection()
    cif_id = _extract_cif_id(query) or state.get("cif_id")
    account_id = _extract_account_id(query) or state.get("account_id")
    pan = _extract_pan(query) or state.get("pan")
    mobile = _extract_mobile(query) or state.get("mobile")
    state_customer_name = state.get("customer_name")
    if _is_generic_name_fragment(state_customer_name, state.get("case_description"), "state"):
        state_customer_name = None
    name_fragment = _extract_name_fragment(query) or state_customer_name
    if _is_generic_name_fragment(name_fragment, query, "resolved"):
        name_fragment = None

    if cif_id:
        customer = customers_collection.find_one({"cif_id": cif_id}, {"_id": 0})
        return customer, [customer] if customer else [], cif_id

    if account_id:
        customer = customers_collection.find_one({"accounts": account_id}, {"_id": 0})
        return customer, [customer] if customer else [], account_id

    if pan:
        customer = customers_collection.find_one({"pan": pan}, {"_id": 0})
        return customer, [customer] if customer else [], pan

    if mobile:
        customer = customers_collection.find_one({"mobile": mobile}, {"_id": 0})
        return customer, [customer] if customer else [], mobile

    matches = []
    if name_fragment:
        for customer in _all_customers():
            name = str(customer.get("name") or "").strip()
            if name and _matches_name_fragment(name_fragment, name):
                matches.append(customer)

    return None, matches, name_fragment or None


def _parse_state_datetime(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        return dt.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _merge_state(state: dict[str, Any], customer: dict[str, Any] | None, query: str) -> dict[str, Any]:
    merged = dict(state)
    date_window = _extract_date_window(query, _now())

    if date_window.get("detected"):
        if date_window.get("clear_start"):
            merged["start_datetime"] = None
        if date_window.get("clear_end"):
            merged["end_datetime"] = None
        if date_window.get("start_datetime"):
            merged["start_datetime"] = date_window["start_datetime"]
        if date_window.get("end_datetime"):
            merged["end_datetime"] = date_window["end_datetime"]

    if customer:
        serialized_customer = _serialize_customer(customer)
        merged["cif_id"] = serialized_customer.get("cif_id")
        merged["account_id"] = (state.get("account_id") or (serialized_customer.get("accounts") or [None])[0])
        merged["pan"] = serialized_customer.get("pan")
        merged["customer_name"] = serialized_customer.get("name")
        merged["mobile"] = serialized_customer.get("mobile")
        merged["resolved_customer"] = serialized_customer

    start_datetime = _parse_state_datetime(merged.get("start_datetime"))
    end_datetime = _parse_state_datetime(merged.get("end_datetime"))

    if start_datetime and end_datetime and end_datetime < start_datetime:
        merged["start_datetime"] = _format_datetime(end_datetime)
        merged["end_datetime"] = _format_datetime(start_datetime)

    return merged


def _build_missing_fields(state: dict[str, Any], has_customer: bool) -> list[str]:
    missing_fields: list[str] = []

    if not has_customer:
        missing_fields.append("customer_id")

    if not state.get("start_datetime"):
        missing_fields.append("start_datetime")
    if not state.get("end_datetime"):
        missing_fields.append("end_datetime")

    return missing_fields


def _fallback_followup_question(state: dict[str, Any], customer_matches: list[dict[str, Any]]) -> str:
    missing = state.get("missing_fields") or []

    if customer_matches and len(customer_matches) > 1:
        options = ", ".join(
            f"{item.get('name')} ({item.get('cif_id')})"
            for item in customer_matches[:5]
        )
        return (
            "I found multiple customers matching that name. "
            f"Please share a unique identifier such as CIF ID, account number, PAN, or registered mobile number. Matches: {options}."
        )

    if "customer_id" in missing and {"start_datetime", "end_datetime"}.issubset(set(missing)):
        return (
            "Please share the customer identifier and the review window. "
            "You can send the CIF ID plus a range like `CIF1001 from 2026-04-17 00:00 to 2026-04-18 23:59`."
        )

    if "customer_id" in missing:
        return "Please share a unique customer identifier such as CIF ID, account number, PAN, or registered mobile number so I can identify the customer uniquely."

    if "start_datetime" in missing and "end_datetime" in missing:
        return (
            "Please share the date or date-time range to inspect. "
            "Example: `2026-04-17 00:00 to 2026-04-18 23:59` or `last 3 days`."
        )

    if "start_datetime" in missing:
        return "Please share the start date/time for the transaction review window in `YYYY-MM-DD HH:MM` format."

    if "end_datetime" in missing:
        return "Please share the end date/time for the transaction review window in `YYYY-MM-DD HH:MM` format."

    return "Please share the missing customer or date-range details so I can continue."


def _build_followup_question(state: dict[str, Any], customer_matches: list[dict[str, Any]]) -> str:
    if customer_matches and len(customer_matches) > 1:
        return _fallback_followup_question(state, customer_matches)

    prompt = f"""
You are an AXIS Bank fraud operations assistant collecting inputs for a transaction fraud review.
Ask one concise follow-up question only for the missing details.
If you need dates or times, mention the expected format `YYYY-MM-DD HH:MM`.
If you need both customer ID and date range, ask for both in one sentence.

Known state:
- Customer CIF ID: {state.get("cif_id") or "missing"}
- Customer name: {state.get("customer_name") or "missing"}
- Mobile: {state.get("mobile") or "missing"}
- Start datetime: {state.get("start_datetime") or "missing"}
- End datetime: {state.get("end_datetime") or "missing"}
- Missing fields: {", ".join(state.get("missing_fields") or []) or "none"}

Return plain text only.
""".strip()

    try:
        response = generate_text(prompt, temperature=0.15)
        cleaned = _normalize_query(response)
        if cleaned:
            return cleaned
    except Exception:
        pass

    return _fallback_followup_question(state, customer_matches)


def _serialize_transaction(transaction: dict[str, Any]) -> dict[str, Any]:
    timestamp = transaction.get("timestamp")
    return {
        "txn_id": str(transaction.get("txn_id") or "").strip(),
        "timestamp": _format_datetime(timestamp if isinstance(timestamp, dt.datetime) else None) or "",
        "amount": float(transaction.get("amount") or 0.0),
        "type": str(transaction.get("type") or "").strip(),
        "channel": str(transaction.get("channel") or "").strip(),
        "beneficiary": str(transaction.get("beneficiary") or "").strip(),
        "is_new_beneficiary": bool(transaction.get("is_new_beneficiary")),
        "status": str(transaction.get("status") or "").strip(),
        "account_id": str(transaction.get("account_id") or "").strip(),
    }


def _is_suspicious_beneficiary(beneficiary: str) -> bool:
    normalized = (beneficiary or "").strip().lower()
    if not normalized:
        return False
    return any(keyword in normalized for keyword in SUSPICIOUS_BENEFICIARY_KEYWORDS)


def _build_flagged_transactions(transactions: list[dict[str, Any]], reasons_by_txn: dict[str, set[str]]) -> list[dict[str, Any]]:
    flagged: list[dict[str, Any]] = []

    for transaction in transactions:
        txn_id = str(transaction.get("txn_id") or "").strip()
        reasons = sorted(reasons_by_txn.get(txn_id) or [])
        if not reasons:
            continue
        flagged.append({**_serialize_transaction(transaction), "reasons": reasons})

    return flagged


def _find_rapid_debit_clusters(debits: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    if len(debits) < 2:
        return []

    clusters: list[list[dict[str, Any]]] = []
    current_cluster = [debits[0]]

    for transaction in debits[1:]:
        previous = current_cluster[-1]
        gap = transaction["timestamp"] - previous["timestamp"]
        if gap <= dt.timedelta(minutes=RAPID_DEBIT_WINDOW_MINUTES):
            current_cluster.append(transaction)
            continue

        if len(current_cluster) >= 2:
            total = sum(float(item.get("amount") or 0.0) for item in current_cluster)
            if total >= HIGH_VALUE_THRESHOLD or any(item.get("is_new_beneficiary") for item in current_cluster):
                clusters.append(current_cluster)
        current_cluster = [transaction]

    if len(current_cluster) >= 2:
        total = sum(float(item.get("amount") or 0.0) for item in current_cluster)
        if total >= HIGH_VALUE_THRESHOLD or any(item.get("is_new_beneficiary") for item in current_cluster):
            clusters.append(current_cluster)

    return clusters


def _risk_level_from_score(score: int) -> str:
    if score >= 70:
        return "High"
    if score >= 35:
        return "Medium"
    return "Low"


def _format_rupees(value: float) -> str:
    return f"Rs. {float(value or 0.0):,.2f}"


def _distinct_transaction_days(transactions: list[dict[str, Any]]) -> int:
    days = {
        transaction["timestamp"].date()
        for transaction in transactions
        if isinstance(transaction.get("timestamp"), dt.datetime)
    }
    return max(len(days), 1)


def _fetch_baseline_transactions(cif_id: str, start_datetime: dt.datetime) -> list[dict[str, Any]]:
    if not cif_id:
        return []

    lookback_start = start_datetime - dt.timedelta(days=BASELINE_LOOKBACK_DAYS)
    query = {
        "cif_id": cif_id,
        "timestamp": {
            "$gte": lookback_start,
            "$lt": start_datetime,
        },
    }
    return list(get_transactions_collection().find(query, {"_id": 0}).sort("timestamp", 1))


def _build_customer_baseline(customer: dict[str, Any], transactions: list[dict[str, Any]], start_datetime: dt.datetime) -> dict[str, Any]:
    current_debits = [
        transaction
        for transaction in transactions
        if str(transaction.get("type") or "").lower() == "debit" and str(transaction.get("status") or "").lower() == "success"
    ]
    baseline_transactions = _fetch_baseline_transactions(str(customer.get("cif_id") or ""), start_datetime)
    historical_debits = [
        transaction
        for transaction in baseline_transactions
        if str(transaction.get("type") or "").lower() == "debit" and str(transaction.get("status") or "").lower() == "success"
    ]

    dominant_channel = ""
    if current_debits:
        dominant_channel = Counter(
            str(transaction.get("channel") or "").strip()
            for transaction in current_debits
            if str(transaction.get("channel") or "").strip()
        ).most_common(1)[0][0] if any(str(transaction.get("channel") or "").strip() for transaction in current_debits) else ""

    same_channel_history = [
        transaction
        for transaction in historical_debits
        if dominant_channel and str(transaction.get("channel") or "").strip() == dominant_channel
    ]

    reference_history = same_channel_history or historical_debits
    current_amounts = [float(transaction.get("amount") or 0.0) for transaction in current_debits]
    historical_amounts = [float(transaction.get("amount") or 0.0) for transaction in reference_history]
    review_window_debit_outflow = round(sum(current_amounts), 2)
    review_window_max_debit = round(max(current_amounts), 2) if current_amounts else 0.0
    current_has_new_beneficiary = any(bool(transaction.get("is_new_beneficiary")) for transaction in current_debits)

    if not current_debits:
        return {
            "available": bool(historical_debits),
            "lookback_days": BASELINE_LOOKBACK_DAYS,
            "dominant_channel": dominant_channel,
            "historical_debit_count": len(historical_debits),
            "same_channel_history_count": len(same_channel_history),
            "typical_debit_amount": round(median(historical_amounts), 2) if historical_amounts else 0.0,
            "average_debit_amount": round(sum(historical_amounts) / len(historical_amounts), 2) if historical_amounts else 0.0,
            "prior_max_debit": round(max(historical_amounts), 2) if historical_amounts else 0.0,
            "average_daily_debit_count": round(len(historical_debits) / _distinct_transaction_days(historical_debits), 2) if historical_debits else 0.0,
            "average_daily_debit_outflow": round(sum(float(item.get("amount") or 0.0) for item in historical_debits) / _distinct_transaction_days(historical_debits), 2) if historical_debits else 0.0,
            "review_window_debit_count": 0,
            "review_window_debit_outflow": 0.0,
            "review_window_max_debit": 0.0,
            "comparison_summary": "No reviewed debit activity was available to compare with the customer's recent transaction baseline.",
            "anomalies": [],
        }

    if not historical_debits:
        return {
            "available": False,
            "lookback_days": BASELINE_LOOKBACK_DAYS,
            "dominant_channel": dominant_channel,
            "historical_debit_count": 0,
            "same_channel_history_count": 0,
            "typical_debit_amount": 0.0,
            "average_debit_amount": 0.0,
            "prior_max_debit": 0.0,
            "average_daily_debit_count": 0.0,
            "average_daily_debit_outflow": 0.0,
            "review_window_debit_count": len(current_debits),
            "review_window_debit_outflow": review_window_debit_outflow,
            "review_window_max_debit": review_window_max_debit,
            "comparison_summary": (
                f"Recent baseline unavailable because no successful debit history was found in the {BASELINE_LOOKBACK_DAYS} days "
                "before the reviewed window."
            ),
            "anomalies": [],
        }

    typical_debit_amount = round(median(historical_amounts), 2) if historical_amounts else 0.0
    average_debit_amount = round(sum(historical_amounts) / len(historical_amounts), 2) if historical_amounts else 0.0
    prior_max_debit = round(max(historical_amounts), 2) if historical_amounts else 0.0
    average_daily_debit_count = round(len(historical_debits) / _distinct_transaction_days(historical_debits), 2)
    average_daily_debit_outflow = round(
        sum(float(item.get("amount") or 0.0) for item in historical_debits) / _distinct_transaction_days(historical_debits),
        2,
    )

    anomalies: list[str] = []
    if dominant_channel and not same_channel_history and (
        review_window_debit_outflow >= HIGH_VALUE_THRESHOLD
        or len(current_debits) >= 2
        or current_has_new_beneficiary
    ):
        anomalies.append(f"No successful {dominant_channel} debit history was seen in the recent baseline window.")
    if review_window_max_debit and prior_max_debit and review_window_max_debit > max(prior_max_debit * 1.6, typical_debit_amount * 3 if typical_debit_amount else 0.0):
        anomalies.append(
            f"The largest reviewed debit {_format_rupees(review_window_max_debit)} is materially above the prior baseline max of {_format_rupees(prior_max_debit)}."
        )
    if review_window_debit_outflow and average_daily_debit_outflow and review_window_debit_outflow > average_daily_debit_outflow * 2.5:
        anomalies.append(
            f"Reviewed debit outflow {_format_rupees(review_window_debit_outflow)} is sharply above the recent daily average of {_format_rupees(average_daily_debit_outflow)}."
        )
    if len(current_debits) >= 2 and average_daily_debit_count and len(current_debits) > max(average_daily_debit_count * 2, average_daily_debit_count + 1.5):
        anomalies.append(
            f"The review window shows {len(current_debits)} debit transactions versus a recent daily average of {average_daily_debit_count:.2f}."
        )

    baseline_prefix = (
        f"In the previous {BASELINE_LOOKBACK_DAYS} days, the customer's typical {dominant_channel} debit was {_format_rupees(typical_debit_amount)} "
        f"with a prior max of {_format_rupees(prior_max_debit)}."
        if dominant_channel and same_channel_history
        else f"In the previous {BASELINE_LOOKBACK_DAYS} days, the customer's typical debit was {_format_rupees(typical_debit_amount)} "
        f"with a prior max of {_format_rupees(prior_max_debit)}."
    )
    comparison_summary = (
        f"{baseline_prefix} The reviewed window shows {len(current_debits)} debit transaction(s) totaling "
        f"{_format_rupees(review_window_debit_outflow)} with a peak debit of {_format_rupees(review_window_max_debit)}."
    )
    if anomalies:
        comparison_summary = f"{comparison_summary} {' '.join(anomalies[:2])}"

    return {
        "available": True,
        "lookback_days": BASELINE_LOOKBACK_DAYS,
        "dominant_channel": dominant_channel,
        "historical_debit_count": len(historical_debits),
        "same_channel_history_count": len(same_channel_history),
        "typical_debit_amount": typical_debit_amount,
        "average_debit_amount": average_debit_amount,
        "prior_max_debit": prior_max_debit,
        "average_daily_debit_count": average_daily_debit_count,
        "average_daily_debit_outflow": average_daily_debit_outflow,
        "review_window_debit_count": len(current_debits),
        "review_window_debit_outflow": review_window_debit_outflow,
        "review_window_max_debit": review_window_max_debit,
        "comparison_summary": comparison_summary,
        "anomalies": anomalies,
    }


def _build_transaction_timeline(transactions: list[dict[str, Any]], reasons_by_txn: dict[str, set[str]]) -> list[dict[str, Any]]:
    timeline: list[dict[str, Any]] = []

    for transaction in transactions[:TIMELINE_ITEM_LIMIT]:
        txn_id = str(transaction.get("txn_id") or "").strip()
        transaction_type = str(transaction.get("type") or "").strip().lower() or "transaction"
        channel = str(transaction.get("channel") or "").strip() or "Unknown channel"
        beneficiary = str(transaction.get("beneficiary") or "").strip()
        status = str(transaction.get("status") or "").strip().lower() or "unknown"
        amount = float(transaction.get("amount") or 0.0)
        is_new_beneficiary = bool(transaction.get("is_new_beneficiary"))
        reasons = sorted(reasons_by_txn.get(txn_id) or [])

        severity = "info"
        if amount > HIGH_VALUE_THRESHOLD or any("Rapid" in reason or "Repeated" in reason for reason in reasons):
            severity = "high"
        elif reasons or is_new_beneficiary or _is_suspicious_beneficiary(beneficiary):
            severity = "medium"

        if transaction_type == "debit":
            title = f"{channel} debit"
            if beneficiary:
                title = f"{title} to {beneficiary}"
        elif transaction_type == "credit":
            title = f"{channel} credit received"
        else:
            title = f"{channel} {transaction_type}".strip()

        details_parts = [
            f"{transaction_type.title()} of {_format_rupees(amount)}",
            f"Status: {status.title()}",
        ]
        if transaction.get("account_id"):
            details_parts.append(f"Account: {transaction.get('account_id')}")
        if is_new_beneficiary:
            details_parts.append("New beneficiary")
        if beneficiary and transaction_type != "debit":
            details_parts.append(f"Beneficiary: {beneficiary}")

        timeline.append(
            {
                "txn_id": txn_id,
                "timestamp": _format_datetime(transaction.get("timestamp") if isinstance(transaction.get("timestamp"), dt.datetime) else None) or "",
                "amount": amount,
                "type": transaction_type,
                "channel": channel,
                "beneficiary": beneficiary,
                "account_id": str(transaction.get("account_id") or "").strip(),
                "status": status,
                "severity": severity,
                "title": title,
                "details": " | ".join(part for part in details_parts if part),
                "reasons": reasons,
            }
        )

    return timeline


def _classify_fraud(pattern_names: set[str], flagged_transactions: list[dict[str, Any]]) -> str:
    if not flagged_transactions:
        return "No strong fraud signal in the selected window"

    if {"Rapid debit velocity", "Repeated suspicious beneficiary"}.issubset(pattern_names):
        return "Likely mule-account payout or account takeover pattern"

    if "High-value debit activity" in pattern_names and "New beneficiary usage" in pattern_names:
        return "High-value transfer to a newly added beneficiary"

    if "Repeated suspicious beneficiary" in pattern_names:
        return "Repeated suspicious beneficiary transfer pattern"

    if "Rapid debit velocity" in pattern_names:
        return "Rapid debit burst requiring urgent review"

    return "Transaction pattern requires manual fraud review"


def _build_recommended_actions(risk_level: str, pattern_names: set[str], flagged_transactions: list[dict[str, Any]]) -> list[str]:
    actions: list[str] = []

    if risk_level == "High":
        actions.append("Call the customer on the registered mobile number and trigger immediate fraud-operations review.")
    elif risk_level == "Medium":
        actions.append("Validate the disputed activity with the customer and review digital channel logs for the flagged window.")
    else:
        actions.append("Continue monitoring the account and validate whether the reviewed activity is customer-authorized.")

    if "New beneficiary usage" in pattern_names:
        actions.append("Review beneficiary registration details and confirm whether the customer approved the new payee.")
    if "High-value debit activity" in pattern_names:
        actions.append("Verify source channel, device, and approval trail for the high-value outward transactions.")
    if "Repeated suspicious beneficiary" in pattern_names or "Rapid debit velocity" in pattern_names:
        actions.append("Escalate the flagged transaction IDs for manual fraud review and beneficiary profiling.")
    if not flagged_transactions:
        actions.append("No flagged transaction pattern was found; expand the date range if additional review is needed.")

    deduped_actions: list[str] = []
    for action in actions:
        if action not in deduped_actions:
            deduped_actions.append(action)

    return deduped_actions[:4]


def _fallback_reasoning_summary(customer: dict[str, Any], analysis: dict[str, Any]) -> str:
    patterns = analysis.get("suspicious_patterns") or []
    baseline_summary = str((analysis.get("customer_baseline") or {}).get("comparison_summary") or "").strip()
    if not patterns:
        summary = (
            f"I reviewed the selected transaction window for {customer.get('name')} ({customer.get('cif_id')}) "
            "and did not find a strong fraud pattern in the available transactions."
        )
        if baseline_summary:
            summary = f"{summary} {baseline_summary}"
        return summary

    pattern_text = "; ".join(item.get("details", "") for item in patterns[:3] if item.get("details"))
    summary = (
        f"I reviewed the selected transaction window for {customer.get('name')} ({customer.get('cif_id')}). "
        f"Risk is {analysis.get('risk_level', 'Medium')} because {pattern_text}"
    ).strip()
    if baseline_summary:
        summary = f"{summary} {baseline_summary}"
    return summary


def _build_reasoning_summary(customer: dict[str, Any], analysis: dict[str, Any]) -> str:
    prompt = f"""
You are an AXIS Bank fraud analyst assistant.
Use only the structured findings below and write a concise banker-facing summary in 3 to 5 sentences.
Do not invent facts.

Customer:
- Name: {customer.get("name")}
- CIF ID: {customer.get("cif_id")}
- Mobile: {customer.get("mobile")}

Fraud analysis:
- Risk level: {analysis.get("risk_level")}
- Fraud classification: {analysis.get("fraud_classification")}
- Transaction summary: {analysis.get("transaction_summary")}
- Customer baseline: {analysis.get("customer_baseline")}
- Transaction timeline: {analysis.get("transaction_timeline")}
- Suspicious patterns: {analysis.get("suspicious_patterns")}
- Flagged transactions: {analysis.get("flagged_transactions")}
- Recommended actions: {analysis.get("recommended_actions")}
""".strip()

    try:
        response = generate_text(prompt, temperature=0.15)
        cleaned = _normalize_query(response)
        if cleaned:
            return cleaned
    except Exception:
        pass

    return _fallback_reasoning_summary(customer, analysis)


def _clear_resolved_customer(state: dict[str, Any]) -> None:
    state["resolved_customer"] = {}
    state["cif_id"] = None
    state["account_id"] = None
    state["pan"] = None
    state["customer_name"] = None
    state["mobile"] = None


def _build_chat_response(customer: dict[str, Any], analysis: dict[str, Any]) -> str:
    summary = analysis.get("reasoning_summary") or _fallback_reasoning_summary(customer, analysis)
    patterns = analysis.get("suspicious_patterns") or []
    flagged_transactions = analysis.get("flagged_transactions") or []
    actions = analysis.get("recommended_actions") or []

    pattern_lines = "\n".join(
        f"- {item.get('pattern')}: {item.get('details')}"
        for item in patterns[:4]
    ) or "- No suspicious pattern triggered in the selected window."

    flagged_lines = "\n".join(
        f"- {item.get('txn_id')} | {item.get('timestamp')} | Rs. {item.get('amount'):,.2f} | {', '.join(item.get('reasons') or [])}"
        for item in flagged_transactions[:5]
    ) or "- No transactions were flagged."

    action_lines = "\n".join(f"- {item}" for item in actions) or "- No further action recommended."

    return (
        f"Customer Identified:\n"
        f"- {customer.get('name')} ({customer.get('cif_id')})\n"
        f"- Mobile: {customer.get('mobile')}\n"
        f"- PAN: {customer.get('pan') or 'Not available'}\n"
        f"- Accounts: {', '.join(customer.get('accounts') or [])}\n\n"
        f"Fraud Analysis Summary:\n"
        f"- Risk Level: {analysis.get('risk_level')}\n"
        f"- Risk Score: {analysis.get('risk_score')}\n"
        f"- Classification: {analysis.get('fraud_classification')}\n\n"
        f"{summary}\n\n"
        f"Suspicious Patterns:\n{pattern_lines}\n\n"
        f"Flagged Transactions:\n{flagged_lines}\n\n"
        f"Recommended Actions:\n{action_lines}"
    )


def _analyze_transactions(customer: dict[str, Any], transactions: list[dict[str, Any]], start_datetime: dt.datetime, end_datetime: dt.datetime) -> dict[str, Any]:
    reviewed_transactions = [_serialize_transaction(transaction) for transaction in transactions]

    if not transactions:
        analysis = {
            "customer": _serialize_customer(customer),
            "query_window": {
                "start_datetime": _format_datetime(start_datetime),
                "end_datetime": _format_datetime(end_datetime),
            },
            "risk_level": "Low",
            "risk_score": 0,
            "fraud_classification": "No transactions found in the selected window",
            "suspicious_patterns": [],
            "flagged_transactions": [],
            "transaction_timeline": [],
            "reviewed_transactions": [],
            "transaction_summary": {
                "total_transactions": 0,
                "debit_transactions": 0,
                "credit_transactions": 0,
                "total_debit_amount": 0.0,
                "total_credit_amount": 0.0,
                "channels": [],
                "new_beneficiary_transactions": 0,
                "high_value_debits": 0,
            },
            "customer_baseline": _build_customer_baseline(customer, transactions, start_datetime),
            "recommended_actions": [
                "No transactions were found for the selected range. Ask for a different date range if further review is needed."
            ],
            "reasoning_summary": (
                f"No transactions were found for {customer.get('name')} ({customer.get('cif_id')}) in the selected review window."
            ),
            "case_family": "Transaction Fraud",
            "suspicion_direction": "Customer Victim",
            "investigation_basis": "Transaction-Led",
            "transaction_relevance": "primary",
            "evidence_modules_used": list(PRIMARY_TRANSACTION_MODULES),
            "case_summary": "Transaction activity is the primary evidence source for this case review.",
            "family_cards": [],
        }
        return analysis

    successful_debits = [
        transaction
        for transaction in transactions
        if str(transaction.get("type") or "").lower() == "debit" and str(transaction.get("status") or "").lower() == "success"
    ]
    credits = [transaction for transaction in transactions if str(transaction.get("type") or "").lower() == "credit"]
    high_value_debits = [transaction for transaction in successful_debits if float(transaction.get("amount") or 0.0) > HIGH_VALUE_THRESHOLD]
    new_beneficiary_debits = [transaction for transaction in successful_debits if bool(transaction.get("is_new_beneficiary"))]
    suspicious_beneficiary_debits = [
        transaction for transaction in successful_debits if _is_suspicious_beneficiary(str(transaction.get("beneficiary") or ""))
    ]
    rapid_clusters = _find_rapid_debit_clusters(sorted(successful_debits, key=lambda item: item["timestamp"]))
    beneficiary_counter = Counter(
        str(transaction.get("beneficiary") or "").strip()
        for transaction in suspicious_beneficiary_debits
        if str(transaction.get("beneficiary") or "").strip()
    )
    repeated_suspicious_beneficiaries = {
        beneficiary: count
        for beneficiary, count in beneficiary_counter.items()
        if count >= 2
    }

    reasons_by_txn: dict[str, set[str]] = {}

    def mark(transaction: dict[str, Any], reason: str) -> None:
        txn_id = str(transaction.get("txn_id") or "").strip()
        reasons_by_txn.setdefault(txn_id, set()).add(reason)

    suspicious_patterns: list[dict[str, Any]] = []
    pattern_names: set[str] = set()
    risk_score = 0

    if rapid_clusters:
        for cluster in rapid_clusters:
            for transaction in cluster:
                mark(transaction, "Rapid successive debit within minutes")
        biggest_cluster = max(rapid_clusters, key=lambda item: sum(float(txn.get("amount") or 0.0) for txn in item))
        cluster_total = sum(float(txn.get("amount") or 0.0) for txn in biggest_cluster)
        suspicious_patterns.append(
            {
                "pattern": "Rapid debit velocity",
                "severity": "high",
                "details": (
                    f"{len(biggest_cluster)} debit transactions occurred within {RAPID_DEBIT_WINDOW_MINUTES} minutes "
                    f"with total outward value Rs. {cluster_total:,.2f}."
                ),
                "transaction_ids": [str(item.get("txn_id") or "").strip() for item in biggest_cluster],
            }
        )
        pattern_names.add("Rapid debit velocity")
        risk_score += 45

    if high_value_debits:
        for transaction in high_value_debits:
            mark(transaction, "High-value debit above Rs. 50,000")
        highest_value = max(float(transaction.get("amount") or 0.0) for transaction in high_value_debits)
        suspicious_patterns.append(
            {
                "pattern": "High-value debit activity",
                "severity": "high" if len(high_value_debits) >= 2 else "medium",
                "details": (
                    f"{len(high_value_debits)} debit transaction(s) above Rs. 50,000 were detected. "
                    f"The highest debit was Rs. {highest_value:,.2f}."
                ),
                "transaction_ids": [str(item.get("txn_id") or "").strip() for item in high_value_debits[:5]],
            }
        )
        pattern_names.add("High-value debit activity")
        risk_score += 25 + (10 if len(high_value_debits) >= 2 else 0)

    if new_beneficiary_debits:
        for transaction in new_beneficiary_debits:
            mark(transaction, "Transfer to newly added beneficiary")
        suspicious_patterns.append(
            {
                "pattern": "New beneficiary usage",
                "severity": "medium" if len(new_beneficiary_debits) == 1 else "high",
                "details": (
                    f"{len(new_beneficiary_debits)} successful debit transaction(s) involved a newly added beneficiary."
                ),
                "transaction_ids": [str(item.get("txn_id") or "").strip() for item in new_beneficiary_debits[:5]],
            }
        )
        pattern_names.add("New beneficiary usage")
        risk_score += 10 if len(new_beneficiary_debits) == 1 else 20

    if repeated_suspicious_beneficiaries:
        beneficiary, count = sorted(repeated_suspicious_beneficiaries.items(), key=lambda item: item[1], reverse=True)[0]
        for transaction in suspicious_beneficiary_debits:
            if str(transaction.get("beneficiary") or "").strip() == beneficiary:
                mark(transaction, "Repeated transfers to suspicious beneficiary")
        suspicious_patterns.append(
            {
                "pattern": "Repeated suspicious beneficiary",
                "severity": "high",
                "details": f"Beneficiary `{beneficiary}` received {count} suspicious transfer attempts in the selected window.",
                "transaction_ids": [
                    str(item.get("txn_id") or "").strip()
                    for item in suspicious_beneficiary_debits
                    if str(item.get("beneficiary") or "").strip() == beneficiary
                ][:5],
            }
        )
        pattern_names.add("Repeated suspicious beneficiary")
        risk_score += 25
    elif suspicious_beneficiary_debits:
        for transaction in suspicious_beneficiary_debits:
            mark(transaction, "Suspicious beneficiary naming pattern")
        suspicious_patterns.append(
            {
                "pattern": "Suspicious beneficiary naming pattern",
                "severity": "medium",
                "details": (
                    f"{len(suspicious_beneficiary_debits)} transaction(s) targeted beneficiaries such as "
                    "unknown, scam, fraud, or mule-linked labels."
                ),
                "transaction_ids": [str(item.get("txn_id") or "").strip() for item in suspicious_beneficiary_debits[:5]],
            }
        )
        pattern_names.add("Suspicious beneficiary naming pattern")
        risk_score += 20

    customer_baseline = _build_customer_baseline(customer, transactions, start_datetime)
    baseline_anomalies = customer_baseline.get("anomalies") or []
    if baseline_anomalies:
        baseline_candidates = sorted(
            successful_debits,
            key=lambda item: float(item.get("amount") or 0.0),
            reverse=True,
        )[: max(1, min(2, len(successful_debits)))]
        baseline_reason = "Amount materially above customer baseline"
        if customer_baseline.get("dominant_channel") and not customer_baseline.get("same_channel_history_count"):
            baseline_reason = f"No recent {customer_baseline.get('dominant_channel')} debit history in customer baseline"
        for transaction in baseline_candidates:
            mark(transaction, baseline_reason)
        suspicious_patterns.append(
            {
                "pattern": "Customer baseline deviation",
                "severity": "high" if len(baseline_anomalies) >= 2 else "medium",
                "details": str(customer_baseline.get("comparison_summary") or "").strip(),
                "transaction_ids": [str(item.get("txn_id") or "").strip() for item in baseline_candidates],
            }
        )
        pattern_names.add("Customer baseline deviation")
        risk_score += 15 if len(baseline_anomalies) == 1 else 20

    flagged_transactions = _build_flagged_transactions(transactions, reasons_by_txn)
    transaction_timeline = _build_transaction_timeline(transactions, reasons_by_txn)
    fraud_classification = _classify_fraud(pattern_names, flagged_transactions)
    risk_level = _risk_level_from_score(risk_score)
    recommended_actions = _build_recommended_actions(risk_level, pattern_names, flagged_transactions)

    analysis = {
        "customer": _serialize_customer(customer),
        "query_window": {
            "start_datetime": _format_datetime(start_datetime),
            "end_datetime": _format_datetime(end_datetime),
        },
        "risk_level": risk_level,
        "risk_score": risk_score,
        "fraud_classification": fraud_classification,
        "suspicious_patterns": suspicious_patterns,
        "flagged_transactions": flagged_transactions,
        "transaction_timeline": transaction_timeline,
        "reviewed_transactions": reviewed_transactions,
        "transaction_summary": {
            "total_transactions": len(transactions),
            "debit_transactions": len([item for item in transactions if str(item.get("type") or "").lower() == "debit"]),
            "credit_transactions": len(credits),
            "total_debit_amount": round(sum(float(item.get("amount") or 0.0) for item in successful_debits), 2),
            "total_credit_amount": round(sum(float(item.get("amount") or 0.0) for item in credits), 2),
            "channels": sorted({str(item.get("channel") or "").strip() for item in transactions if str(item.get("channel") or "").strip()}),
            "new_beneficiary_transactions": len(new_beneficiary_debits),
            "high_value_debits": len(high_value_debits),
        },
        "customer_baseline": customer_baseline,
        "recommended_actions": recommended_actions,
        "case_family": "Transaction Fraud",
        "suspicion_direction": "Customer Victim",
        "investigation_basis": "Transaction-Led",
        "transaction_relevance": "primary",
        "evidence_modules_used": list(PRIMARY_TRANSACTION_MODULES),
        "family_cards": [],
    }
    analysis["reasoning_summary"] = _build_reasoning_summary(customer, analysis)
    analysis["case_summary"] = "Transaction activity is the primary evidence source for this case review."
    return analysis


def _fetch_transactions(cif_id: str, start_datetime: dt.datetime, end_datetime: dt.datetime) -> list[dict[str, Any]]:
    query = {
        "cif_id": cif_id,
        "timestamp": {
            "$gte": start_datetime,
            "$lte": end_datetime,
        },
    }
    return list(get_transactions_collection().find(query, {"_id": 0}).sort("timestamp", 1))


def run_customer_fraud_chat(user_context: dict[str, Any], query: str, state: dict[str, Any] | None = None) -> dict[str, Any]:
    normalized_query = _normalize_query(query)
    normalized_state = _normalize_state(state)
    session_id = normalized_state.get("sessionId") or f"{user_context['userId']}_{uuid.uuid4().hex}"

    if _is_reset_query(normalized_query):
        reset_state = _empty_state(session_id)
        reset_state["missing_fields"] = ["customer_id", "start_datetime", "end_datetime"]
        response = _build_followup_question(reset_state, [])
        return {
            "user": user_context["userId"],
            "query": query,
            "chatbot_response": response,
            "next_step": "collect_customer",
            "sessionId": session_id,
            "customer_identified": False,
            "customer": None,
            "fraud_analysis": None,
            "conversation_state": reset_state,
        }

    resolved_customer, customer_matches, attempted_identifier = _resolve_customer(normalized_query, normalized_state)
    merged_state = _merge_state(normalized_state, resolved_customer, normalized_query)
    has_customer = _has_resolved_customer(merged_state.get("resolved_customer"))
    merged_state["missing_fields"] = _build_missing_fields(merged_state, has_customer)

    if attempted_identifier and not resolved_customer and customer_matches:
        merged_state["customer_name"] = merged_state.get("customer_name") or attempted_identifier
        merged_state["missing_fields"] = _build_missing_fields(merged_state, False)
        merged_state["step"] = "collect_customer"
        response = _build_identifier_requirement_message(attempted_identifier, customer_matches)
        return {
            "user": user_context["userId"],
            "query": query,
            "chatbot_response": response,
            "next_step": "collect_customer",
            "sessionId": session_id,
            "customer_identified": False,
            "customer": None,
            "fraud_analysis": None,
            "conversation_state": {**merged_state, "sessionId": session_id},
        }

    if attempted_identifier and not resolved_customer and not customer_matches:
        _clear_resolved_customer(merged_state)
        merged_state["missing_fields"] = _build_missing_fields(merged_state, False)
        merged_state["step"] = "collect_customer"
        response = (
            f"I could not find a customer for `{attempted_identifier}`. "
            "Please share a valid CIF ID, account number, PAN, or registered mobile number."
        )
        return {
            "user": user_context["userId"],
            "query": query,
            "chatbot_response": response,
            "next_step": "collect_customer",
            "sessionId": session_id,
            "customer_identified": False,
            "customer": None,
            "fraud_analysis": None,
            "conversation_state": {**merged_state, "sessionId": session_id},
        }

    if customer_matches and len(customer_matches) > 1 and not resolved_customer:
        _clear_resolved_customer(merged_state)
        merged_state["missing_fields"] = _build_missing_fields(merged_state, False)
        merged_state["step"] = "collect_customer"
        response = _build_followup_question(merged_state, customer_matches)
        return {
            "user": user_context["userId"],
            "query": query,
            "chatbot_response": response,
            "next_step": "collect_customer",
            "sessionId": session_id,
            "customer_identified": False,
            "customer": None,
            "fraud_analysis": None,
            "conversation_state": {**merged_state, "sessionId": session_id},
        }

    if merged_state["missing_fields"]:
        merged_state["step"] = "collect_inputs"
        response = _build_followup_question(merged_state, [])
        return {
            "user": user_context["userId"],
            "query": query,
            "chatbot_response": response,
            "next_step": "collect_inputs",
            "sessionId": session_id,
            "customer_identified": has_customer,
            "customer": merged_state.get("resolved_customer") or None,
            "fraud_analysis": None,
            "conversation_state": {**merged_state, "sessionId": session_id},
        }

    start_datetime = _parse_state_datetime(merged_state.get("start_datetime"))
    end_datetime = _parse_state_datetime(merged_state.get("end_datetime"))
    customer = _normalize_resolved_customer_payload(merged_state.get("resolved_customer"))

    if not start_datetime or not end_datetime:
        merged_state["missing_fields"] = _build_missing_fields(merged_state, _has_resolved_customer(customer))
        merged_state["step"] = "collect_inputs"
        response = _build_followup_question(merged_state, [])
        return {
            "user": user_context["userId"],
            "query": query,
            "chatbot_response": response,
            "next_step": "collect_inputs",
            "sessionId": session_id,
            "customer_identified": _has_resolved_customer(customer),
            "customer": customer or None,
            "fraud_analysis": None,
            "conversation_state": {**merged_state, "sessionId": session_id},
        }

    transactions = _fetch_transactions(str(customer.get("cif_id") or ""), start_datetime, end_datetime)
    analysis = _analyze_transactions(customer, transactions, start_datetime, end_datetime)
    merged_state["step"] = "analysis_complete"
    merged_state["missing_fields"] = []
    merged_state["latest_analysis"] = analysis

    return {
        "user": user_context["userId"],
        "query": query,
        "chatbot_response": _build_chat_response(customer, analysis),
        "next_step": "analysis_complete",
        "sessionId": session_id,
        "customer_identified": _has_resolved_customer(customer),
        "customer": customer,
        "fraud_analysis": analysis,
        "conversation_state": {**merged_state, "sessionId": session_id},
    }


def _empty_integrated_state(session_id: str | None = None) -> dict[str, Any]:
    return {
        "step": "collect_case_details",
        "sessionId": session_id,
        "case_query": None,
        "case_description": None,
        "case_family": None,
        "suspicion_direction": None,
        "investigation_basis": None,
        "transaction_relevance": None,
        "review_scope_mode": None,
        "review_scope_label": None,
        "cif_id": None,
        "account_id": None,
        "pan": None,
        "customer_name": None,
        "mobile": None,
        "start_datetime": None,
        "end_datetime": None,
        "evidence_modules_used": [],
        "resolved_customer": {},
        "missing_fields": [],
        "latest_analysis": {},
        "sop_analysis": {},
    }


def _normalize_integrated_state(raw_state: dict[str, Any] | None) -> dict[str, Any]:
    state = _empty_integrated_state((raw_state or {}).get("sessionId"))

    if not isinstance(raw_state, dict):
        return state

    for key in [
        "step",
        "sessionId",
        "case_query",
        "case_description",
        "case_family",
        "suspicion_direction",
        "investigation_basis",
        "transaction_relevance",
        "review_scope_mode",
        "review_scope_label",
        "cif_id",
        "account_id",
        "pan",
        "customer_name",
        "mobile",
        "start_datetime",
        "end_datetime",
    ]:
        value = raw_state.get(key)
        if isinstance(value, str) and value.strip():
            state[key] = value.strip()
        elif value is not None and key in {"step", "sessionId"}:
            state[key] = str(value).strip() or state[key]

    resolved_customer = raw_state.get("resolved_customer")
    if isinstance(resolved_customer, dict):
        state["resolved_customer"] = _normalize_resolved_customer_payload(resolved_customer)

    missing_fields = raw_state.get("missing_fields")
    if isinstance(missing_fields, list):
        state["missing_fields"] = [str(item).strip() for item in missing_fields if str(item).strip()]

    evidence_modules_used = raw_state.get("evidence_modules_used")
    if isinstance(evidence_modules_used, list):
        state["evidence_modules_used"] = [str(item).strip() for item in evidence_modules_used if str(item).strip()]

    latest_analysis = raw_state.get("latest_analysis")
    if isinstance(latest_analysis, dict):
        state["latest_analysis"] = latest_analysis

    sop_analysis = raw_state.get("sop_analysis")
    if isinstance(sop_analysis, dict):
        state["sop_analysis"] = sop_analysis

    return state


def _extract_case_description(query: str, existing_description: str | None = None) -> str | None:
    normalized = _normalize_query(query)
    if not normalized:
        return existing_description

    candidate = normalized
    candidate = re.sub(r"\b(?:customer\s+with\s+)?CIF(?:\s*ID)?[\s\-_/:#]*\d{4,6}\b", " ", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"\bCIF\d{4,}\b", " ", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"(?<!\d)([6-9]\d{9})(?!\d)", " ", candidate)
    candidate = re.sub(r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?\b", " ", candidate)
    candidate = re.sub(r"\b\d{1,2}[-/]\d{1,2}[-/]\d{4}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?\b", " ", candidate)
    candidate = re.sub(r"\b(?:last|past)\s+\d{1,2}\s+days?\b", " ", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"\b(?:today|yesterday)\b", " ", candidate, flags=re.IGNORECASE)
    candidate = re.sub(
        r"\b(?:check|review|analyse|analyze|inspect|see|look|please|cif|id|mobile|phone|number|registered|from|to|between|range|account|accounts)\b",
        " ",
        candidate,
        flags=re.IGNORECASE,
    )
    candidate = re.sub(
        r"\bcustomer\s+with\s+(?=(?:provided|submitted|reported|clicked|used|shared|entered|gave|defaulted|den(?:ies|ied)|onboarding|applied|made|faced|noticed|lost|saw|complained)\b)",
        "",
        candidate,
        flags=re.IGNORECASE,
    )
    candidate = " ".join(candidate.split())
    if candidate and normalized.lower().startswith("customer ") and re.match(
        r"^(?:provided|submitted|reported|clicked|used|shared|entered|gave|defaulted|den(?:ies|ied)|onboarding|applied|made|faced|noticed|lost|saw|complained)\b",
        candidate,
        flags=re.IGNORECASE,
    ):
        candidate = f"Customer {candidate}"

    if len(candidate.split()) >= 4:
        return candidate

    if existing_description:
        return existing_description

    lowered = normalized.lower()
    if len(normalized.split()) >= 5 and any(
        keyword in lowered
        for keyword in [
            "debit",
            "withdraw",
            "withdrawal",
            "upi",
            "imps",
            "atm",
            "netbanking",
            "beneficiary",
            "transaction",
            "suspicious",
            "scam",
            "fraud",
            "unauthorised",
            "unauthorized",
            "phishing",
            "money",
        ]
    ):
        return normalized

    return existing_description


def _merge_integrated_state(state: dict[str, Any], customer: dict[str, Any] | None, query: str) -> dict[str, Any]:
    merged = _merge_state(state, customer, query)
    extracted_account_id = _extract_account_id(query)
    extracted_pan = _extract_pan(query)
    extracted_name = _extract_name_fragment(query)

    if extracted_account_id:
        merged["account_id"] = extracted_account_id
    if extracted_pan:
        merged["pan"] = extracted_pan
    if extracted_name and not merged.get("customer_name"):
        merged["customer_name"] = extracted_name

    case_description = _extract_case_description(query, merged.get("case_description"))
    if case_description:
        merged["case_description"] = case_description
        route = _classify_case_family(case_description)
        merged["case_family"] = route.get("case_family")
        merged["suspicion_direction"] = route.get("suspicion_direction")
        merged["investigation_basis"] = route.get("investigation_basis")
        merged["transaction_relevance"] = route.get("transaction_relevance")
        merged["evidence_modules_used"] = list(route.get("evidence_modules_used") or [])
    return merged


def _build_integrated_missing_fields(state: dict[str, Any], has_customer: bool) -> list[str]:
    missing_fields: list[str] = []
    transaction_relevance = str(state.get("transaction_relevance") or "primary").strip().lower()

    if not state.get("case_description"):
        missing_fields.append("case_description")
    if not has_customer:
        missing_fields.append("customer_id")
    if transaction_relevance != "not_applicable":
        if not state.get("start_datetime"):
            missing_fields.append("start_datetime")
        if not state.get("end_datetime"):
            missing_fields.append("end_datetime")

    return missing_fields


def _build_integrated_followup_question(state: dict[str, Any], customer_matches: list[dict[str, Any]]) -> str:
    missing = state.get("missing_fields") or []
    transaction_relevance = str(state.get("transaction_relevance") or "primary").strip().lower()
    review_label = "transaction review window" if transaction_relevance in {"primary", "supporting"} else "review window"

    if customer_matches and len(customer_matches) > 1:
        options = ", ".join(
            f"{item.get('name')} ({item.get('cif_id')})"
            for item in customer_matches[:5]
        )
        return (
            "I found multiple customers with that name. "
            f"Please share a unique identifier such as CIF ID, account number, PAN, or registered mobile number. Matches: {options}."
        )

    if {"case_description", "customer_id", "start_datetime", "end_datetime"}.issubset(set(missing)):
        return (
            "Please describe what happened in the case, share a unique customer identifier such as CIF ID, account number, PAN, or registered mobile number, "
            "and mention the date-time range to review in `YYYY-MM-DD HH:MM` format."
        )

    if "case_description" in missing and "customer_id" in missing:
        return (
            "Please describe what happened in the case and share a unique customer identifier such as CIF ID, account number, PAN, or registered mobile number."
        )

    if "case_description" in missing:
        return (
            "Please describe what happened in the case, for example repeated debits, unusual withdrawals, "
            "new beneficiary transfer, or suspicious UPI activity."
        )

    if "customer_id" in missing:
        customer_name = str(state.get("customer_name") or "").strip()
        if customer_name:
            return (
                f"I noted the customer name `{customer_name}`, but before I continue I need one unique identifier "
                "such as CIF ID, account number, PAN, or registered mobile number."
            )
        return "Please share one unique customer identifier such as CIF ID, account number, PAN, or registered mobile number so I can confirm the customer."

    if "start_datetime" in missing and "end_datetime" in missing:
        return (
            "Please share the date-time range to review in `YYYY-MM-DD HH:MM` format. "
            "Example: `2026-04-17 00:00 to 2026-04-18 23:59` or `last 3 days`."
        )

    if "start_datetime" in missing:
        return f"Please share the start date/time for the {review_label} in `YYYY-MM-DD HH:MM` format."

    if "end_datetime" in missing:
        return f"Please share the end date/time for the {review_label} in `YYYY-MM-DD HH:MM` format."

    if not state.get("resolved_customer"):
        return "Please share one unique customer identifier such as CIF ID, account number, PAN, or registered mobile number so I can confirm the customer."

    if not state.get("case_description"):
        return "Please describe what happened in the case so I can continue."

    if transaction_relevance != "not_applicable" and (not state.get("start_datetime") or not state.get("end_datetime")):
        return (
            "Please share the date-time range to review in `YYYY-MM-DD HH:MM` format. "
            "Example: `2026-04-17 00:00 to 2026-04-18 23:59` or `last 3 days`."
        )

    return "Please share the missing case, customer, or date-range details so I can continue."


def _build_identifier_requirement_message(name_fragment: str | None, customer_matches: list[dict[str, Any]]) -> str:
    cleaned_name = str(name_fragment or "").strip()

    if customer_matches and len(customer_matches) > 1:
        options = ", ".join(
            f"{item.get('name')} ({item.get('cif_id')})"
            for item in customer_matches[:5]
        )
        return (
            f"I found multiple possible matches for `{cleaned_name}`. "
            "Before I continue, please share one unique identifier such as CIF ID, account number, PAN, or registered mobile number. "
            f"Possible matches: {options}."
        )

    if customer_matches and len(customer_matches) == 1:
        matched_customer = customer_matches[0]
        return (
            f"I found a likely customer match for `{cleaned_name}`: {matched_customer.get('name')} ({matched_customer.get('cif_id')}). "
            "Before I continue, please share one unique identifier such as CIF ID, account number, PAN, or registered mobile number."
        )

    if cleaned_name:
        return (
            f"`{cleaned_name}` alone is not enough to confirm the customer. "
            "Please share one unique identifier such as CIF ID, account number, PAN, or registered mobile number."
        )

    return "Before I continue, please share one unique identifier such as CIF ID, account number, PAN, or registered mobile number."


def _build_sop_grounding_query(case_description: str, customer: dict[str, Any], analysis: dict[str, Any]) -> str:
    suspicious_patterns = analysis.get("suspicious_patterns") or []
    flagged_transactions = analysis.get("flagged_transactions") or []
    transaction_summary = analysis.get("transaction_summary") or {}
    evidence_modules = ", ".join(analysis.get("evidence_modules_used") or []) or "Universal Case Header, SOP Grounding"
    case_family = str(analysis.get("case_family") or "Manual Review").strip()
    suspicion_direction = str(analysis.get("suspicion_direction") or "Manual Review").strip()
    investigation_basis = str(analysis.get("investigation_basis") or "Mixed").strip()
    transaction_relevance = str(analysis.get("transaction_relevance") or "supporting").strip()
    case_summary = str(analysis.get("case_summary") or "").strip() or "Case-family routing summary is not available."

    pattern_text = "; ".join(
        f"{item.get('pattern')}: {item.get('details')}"
        for item in suspicious_patterns[:4]
        if item.get("pattern")
    ) or "No suspicious pattern was triggered in the reviewed window."

    flagged_text = "; ".join(
        (
            f"{item.get('txn_id')} on {item.get('timestamp')} | {item.get('channel')} | "
            f"Rs. {float(item.get('amount') or 0.0):,.2f} | beneficiary {item.get('beneficiary')}"
        )
        for item in flagged_transactions[:5]
    ) or "No specific transaction IDs were flagged."

    channels = ", ".join(transaction_summary.get("channels") or []) or "No channels recorded"
    baseline_summary = str((analysis.get("customer_baseline") or {}).get("comparison_summary") or "").strip() or "Baseline comparison was not available."
    loan_exposure_summary = str((analysis.get("loan_exposure") or {}).get("summary") or "").strip() or "No linked loan exposure summary was available."
    collateral_summary = str((analysis.get("collateral_review") or {}).get("summary") or "").strip() or "No linked collateral summary was available."
    document_summary = str((analysis.get("document_review") or {}).get("summary") or "").strip() or "No document-verification summary was available."
    case_event_summary = str((analysis.get("case_event_summary") or {}).get("summary") or "").strip() or "No linked case-event summary was available."
    related_data_summary = analysis.get("related_data_summary") or {}

    return f"""
Customer-reported case:
{case_description}

Customer verified:
- Name: {customer.get("name")}
- CIF ID: {customer.get("cif_id")}
- PAN: {customer.get("pan") or "Not available"}
- Mobile: {customer.get("mobile")}
- Accounts: {", ".join(customer.get("accounts") or [])}

Reviewed transaction window:
- Start: {analysis.get("query_window", {}).get("start_datetime")}
- End: {analysis.get("query_window", {}).get("end_datetime")}
- Current risk level: {analysis.get("risk_level")}
- Case family: {case_family}
- Suspicion direction: {suspicion_direction}
- Investigation basis: {investigation_basis}
- Transaction relevance: {transaction_relevance}
- Current fraud classification: {analysis.get("fraud_classification")}
- Case routing summary: {case_summary}
- Current reasoning summary: {analysis.get("reasoning_summary")}
- Customer baseline comparison: {baseline_summary}
- Channels involved: {channels}
- Suspicious patterns: {pattern_text}
- Flagged transactions: {flagged_text}
- Evidence modules currently in scope: {evidence_modules}
- Related loan files: {int(related_data_summary.get("loan_accounts") or 0)}
- Related collateral records: {int(related_data_summary.get("collateral_records") or 0)}
- Related document verifications: {int(related_data_summary.get("document_verifications") or 0)}
- Related case events: {int(related_data_summary.get("case_events") or 0)}
- Loan exposure summary: {loan_exposure_summary}
- Collateral review summary: {collateral_summary}
- Document verification summary: {document_summary}
- Case-event summary: {case_event_summary}

Based on the AXIS Bank SOP, identify the closest case category, explain what likely happened, list the suspicious indicators, confirm whether the case looks customer-victim, customer-to-bank, or mixed, and provide the immediate SOP-grounded action.
""".strip()


def _risk_rank(value: str | None) -> int:
    normalized = str(value or "").strip().lower()
    return {"low": 1, "medium": 2, "high": 3}.get(normalized, 2)


def _combine_transaction_and_sop_analysis(
    case_description: str,
    transaction_analysis: dict[str, Any],
    sop_analysis: dict[str, Any],
) -> dict[str, Any]:
    combined = dict(transaction_analysis)
    sop_supported = bool(sop_analysis.get("supported")) if isinstance(sop_analysis, dict) else False
    tx_risk = str(transaction_analysis.get("risk_level") or "Medium").strip() or "Medium"
    sop_risk = str((sop_analysis or {}).get("risk_level") or tx_risk).strip() or tx_risk
    tx_classification = str(transaction_analysis.get("fraud_classification") or "").strip()
    sop_category = str((sop_analysis or {}).get("fraud_category") or "").strip()
    sop_classification = str((sop_analysis or {}).get("fraud_classification") or "").strip()
    case_family = str(transaction_analysis.get("case_family") or "Manual Review").strip() or "Manual Review"
    transaction_relevance = str(transaction_analysis.get("transaction_relevance") or "supporting").strip().lower()
    sop_has_specific_category = sop_supported and sop_category and sop_category.lower() not in {"unknown"}
    sop_has_specific_classification = sop_supported and sop_classification and sop_classification.lower() not in {
        "unknown",
        "manual review required",
        "no transactions found in the selected window",
    }

    combined["case_description"] = case_description
    combined["supported"] = True
    combined["transaction_classification"] = transaction_analysis.get("fraud_classification")
    combined["fraud_category"] = sop_category if sop_has_specific_category else case_family
    combined["fraud_classification"] = (
        sop_classification
        if sop_has_specific_classification
        else tx_classification
    )
    combined["risk_level"] = sop_risk if _risk_rank(sop_risk) > _risk_rank(tx_risk) else tx_risk
    combined["sop_supported"] = sop_supported
    combined["case_family"] = case_family
    combined["suspicion_direction"] = str(transaction_analysis.get("suspicion_direction") or "Manual Review").strip() or "Manual Review"
    combined["investigation_basis"] = str(transaction_analysis.get("investigation_basis") or "Mixed").strip() or "Mixed"
    combined["transaction_relevance"] = transaction_relevance or "supporting"
    combined["evidence_modules_used"] = list(transaction_analysis.get("evidence_modules_used") or [])
    combined["case_summary"] = str(transaction_analysis.get("case_summary") or "").strip()
    if transaction_relevance == "not_applicable":
        combined["suspicious_indicators"] = _build_non_primary_indicators(
            transaction_analysis,
            (sop_analysis or {}).get("suspicious_indicators") or [],
        )
        combined["relevant_information"] = _build_non_primary_reasoning_summary(
            str(transaction_analysis.get("case_summary") or "").strip(),
            sop_analysis if isinstance(sop_analysis, dict) else {},
            transaction_analysis,
        )
    else:
        combined["suspicious_indicators"] = (
            (sop_analysis or {}).get("suspicious_indicators")
            or [item.get("pattern") for item in transaction_analysis.get("suspicious_patterns") or [] if item.get("pattern")]
        )
        combined["relevant_information"] = (
            (
                str((sop_analysis or {}).get("relevant_information") or "").strip()
                if sop_has_specific_classification or sop_has_specific_category
                else ""
            )
            or (
                str(transaction_analysis.get("reasoning_summary") or "").strip()
                if transaction_relevance == "primary"
                else str(transaction_analysis.get("case_summary") or "").strip()
            )
        )
    combined["sop_summary"] = (
        str((sop_analysis or {}).get("sop_summary") or "").strip()
        or str((sop_analysis or {}).get("reason") or "").strip()
    )
    combined["recommended_action"] = (
        str((sop_analysis or {}).get("recommended_action") or "").strip()
        or (
            (transaction_analysis.get("recommended_actions") or [None])[0]
            if isinstance(transaction_analysis.get("recommended_actions"), list)
            else ""
        )
        or ""
    )
    combined["references"] = list((sop_analysis or {}).get("references") or [])
    combined["sop_reason"] = str((sop_analysis or {}).get("reason") or "").strip()
    combined["review_scope_label"] = str(transaction_analysis.get("review_scope_label") or "").strip()
    combined["review_scope_mode"] = str(transaction_analysis.get("review_scope_mode") or "").strip()

    if transaction_relevance != "primary":
        combined["reasoning_summary"] = _build_non_primary_reasoning_summary(
            str(transaction_analysis.get("case_summary") or "").strip(),
            sop_analysis if isinstance(sop_analysis, dict) else {},
            transaction_analysis,
        )

    combined["recommended_actions"] = _filter_actions_for_transaction_relevance(
        _merge_unique_actions(
        [combined.get("recommended_action")],
        (transaction_analysis.get("recommended_actions") or []) if isinstance(transaction_analysis.get("recommended_actions"), list) else [],
        ),
        transaction_relevance,
    )
    if combined["recommended_actions"]:
        combined["recommended_action"] = combined["recommended_actions"][0]
    combined["risk_score"] = _cap_risk_score(combined.get("risk_score"))
    combined["risk_level"] = _risk_level_from_score(int(combined.get("risk_score") or 0))

    return combined


def _build_combined_chat_response(customer: dict[str, Any], analysis: dict[str, Any]) -> str:
    patterns = analysis.get("suspicious_patterns") or []
    actions = analysis.get("recommended_actions") or []
    query_window = analysis.get("query_window") or {}
    case_family = str(analysis.get("case_family") or "Manual Review").strip() or "Manual Review"
    suspicion_direction = str(analysis.get("suspicion_direction") or "Manual Review").strip() or "Manual Review"
    investigation_basis = str(analysis.get("investigation_basis") or "Mixed").strip() or "Mixed"
    transaction_relevance = str(analysis.get("transaction_relevance") or "supporting").strip() or "supporting"
    case_summary = str(analysis.get("case_summary") or "").strip()
    transaction_relevance_label = transaction_relevance.replace("_", " ").title()
    display_fraud_type = _friendly_fraud_type_label(analysis, case_family)
    if transaction_relevance == "not_applicable":
        primary_pattern = _build_non_primary_key_concern(analysis)
    else:
        primary_pattern = next(
            (
                f"{item.get('pattern')}: {item.get('details')}"
                for item in patterns
                if item.get("pattern")
            ),
            (
                f"Supporting transaction history does not override the primary {investigation_basis.lower()} concern."
                if transaction_relevance == "supporting"
                else "No strong suspicious transaction pattern was triggered in the selected review window."
            ),
        )
    first_action = next((str(item).strip() for item in actions if str(item).strip()), "Proceed with manual review and safeguard the impacted customer account.")
    review_window = _display_review_scope_label(analysis)
    baseline_summary = str((analysis.get("customer_baseline") or {}).get("comparison_summary") or "").strip()
    review_scope_mode = str(analysis.get("review_scope_mode") or "").strip().lower()

    lines = [
        "Executive Summary:",
        f"- Customer confirmed: {customer.get('name')} ({customer.get('cif_id')})",
        f"- Case family: {case_family}",
        f"- Suspicion direction: {suspicion_direction}",
        f"- Review basis: {investigation_basis}",
        f"- Transaction relevance: {transaction_relevance_label}",
        f"- Review scope: {review_window}",
        f"- Risk level: {analysis.get('risk_level') or 'Medium'}",
        f"- Likely fraud type: {display_fraud_type}",
        f"- Key concern: {primary_pattern}",
    ]
    if case_summary:
        lines.append(f"- Routing summary: {case_summary}")
    if baseline_summary and transaction_relevance != "not_applicable" and review_scope_mode != "relationship_history":
        lines.append(f"- Customer baseline: {baseline_summary}")
    lines.append(f"- Immediate next action: {first_action}")

    return "\n".join(lines)


def _build_documents_followup_prompt() -> str:
    return "Do you want the relevant blueprint documents for this case? (Yes/No)"


def _build_report_followup_prompt() -> str:
    return "Do you want an automated investigation report generated? (Yes/No)"


def _build_history_followup_prompt() -> str:
    return "Do you want historical fraud case references? (Yes/No)"


def _build_final_assistance_prompt() -> str:
    return "Is there anything else I can help you with? (Yes/No)"


def _build_report_case_query(state: dict[str, Any], customer: dict[str, Any]) -> str:
    case_description = str(state.get("case_description") or "").strip()
    customer_name = str(customer.get("name") or "").strip()
    cif_id = str(customer.get("cif_id") or "").strip()
    start_datetime = str(state.get("start_datetime") or "").strip()
    end_datetime = str(state.get("end_datetime") or "").strip()
    review_scope_label = str(state.get("review_scope_label") or "").strip()
    case_family = str(state.get("case_family") or "").strip()
    suspicion_direction = str(state.get("suspicion_direction") or "").strip()
    investigation_basis = str(state.get("investigation_basis") or "").strip()

    parts = []
    if case_description:
        parts.append(f"Case summary: {case_description}")
    if case_family:
        parts.append(f"Case family: {case_family}")
    if suspicion_direction:
        parts.append(f"Suspicion direction: {suspicion_direction}")
    if investigation_basis:
        parts.append(f"Investigation basis: {investigation_basis}")
    if customer_name or cif_id:
        label = customer_name or "Customer"
        suffix = f" ({cif_id})" if cif_id else ""
        parts.append(f"Customer verified: {label}{suffix}")
    if review_scope_label:
        parts.append(f"Review scope: {review_scope_label}")
    elif start_datetime and end_datetime:
        parts.append(f"Reviewed window: {start_datetime} to {end_datetime}")

    return ". ".join(part for part in parts if part).strip() or "Customer fraud investigation case"


def _build_report_export_title(state: dict[str, Any], customer: dict[str, Any]) -> str:
    customer_name = re.sub(r"[^A-Za-z0-9]+", "_", str(customer.get("name") or "").strip()).strip("_")
    cif_id = re.sub(r"[^A-Za-z0-9]+", "_", str(customer.get("cif_id") or "").strip()).strip("_")
    timestamp = _now().strftime("%Y%m%d_%H%M%S")

    parts = ["AXIS_Investigation_Report"]
    if cif_id:
        parts.append(cif_id)
    elif customer_name:
        parts.append(customer_name)
    parts.append(timestamp)
    return "_".join(parts)


def run_integrated_fraud_chat(user_context: dict[str, Any], query: str, state: dict[str, Any] | None = None) -> dict[str, Any]:
    normalized_query = _normalize_query(query)
    normalized_state = _normalize_integrated_state(state)
    session_id = normalized_state.get("sessionId") or f"{user_context['userId']}_{uuid.uuid4().hex}"

    if normalized_state.get("step") == "analysis_complete" and normalized_state.get("latest_analysis"):
        normalized_state["step"] = "fetch_documentation"

    if _is_reset_query(normalized_query):
        reset_state = _empty_integrated_state(session_id)
        reset_state["missing_fields"] = ["case_description", "customer_id", "start_datetime", "end_datetime"]
        response = _build_integrated_followup_question(reset_state, [])
        return {
            "user": user_context["userId"],
            "bank": user_context["bankId"],
            "query": query,
            "fraud_analysis": None,
            "chatbot_response": response,
            "next_step": "collect_case_details",
            "sessionId": session_id,
            "fraud_category": "",
            "documents": [],
            "documents_title": "",
            "conversation_state": reset_state,
        }

    current_step = str(normalized_state.get("step") or "").strip()
    choice = _normalize_choice(normalized_query)

    if current_step in FOLLOWUP_STEPS and choice is None:
        normalized_state = _empty_integrated_state(session_id)
        current_step = str(normalized_state.get("step") or "").strip()

    if current_step == "fetch_documentation":
        documents: list[dict[str, Any]] = []
        documents_title = ""

        if choice == "yes":
            if (user_context.get("permissions") or {}).get("canDownloadDocuments"):
                documents = _fetch_relevant_documents(user_context["bankId"])
                documents_title = "Relevant Blueprint Documentation"
                if documents:
                    response = f"I have attached the relevant blueprint documents for this case.\n\n{_build_report_followup_prompt()}"
                else:
                    response = f"I could not find a matching blueprint document right now.\n\n{_build_report_followup_prompt()}"
            else:
                response = f"Document downloads are not available for your role.\n\n{_build_report_followup_prompt()}"
            normalized_state["step"] = "generate_report"
        elif choice == "no":
            response = f"Skipping blueprint documents.\n\n{_build_report_followup_prompt()}"
            normalized_state["step"] = "generate_report"
        else:
            response = "Please reply Yes or No so I know whether to fetch the relevant blueprint documents."

        latest_analysis = normalized_state.get("latest_analysis") if isinstance(normalized_state.get("latest_analysis"), dict) else {}
        return {
            "user": user_context["userId"],
            "bank": user_context["bankId"],
            "query": query,
            "fraud_analysis": None,
            "chatbot_response": response,
            "next_step": normalized_state.get("step"),
            "sessionId": session_id,
            "fraud_category": latest_analysis.get("fraud_category") or "",
            "documents": documents,
            "documents_title": documents_title,
            "conversation_state": {**normalized_state, "sessionId": session_id},
        }

    if current_step == "generate_report":
        documents: list[dict[str, Any]] = []
        documents_title = ""

        if choice == "yes":
            if (user_context.get("permissions") or {}).get("canGenerateReport"):
                analysis = normalized_state.get("latest_analysis") if isinstance(normalized_state.get("latest_analysis"), dict) else {}
                report_query = (
                    str(normalized_state.get("case_query") or "").strip()
                    or str(normalized_state.get("case_description") or "").strip()
                    or query
                )
                report = generate_investigation_report(report_query, user_context["bankId"], analysis)
                export_note = ""
                try:
                    report_export = export_investigation_report_pdf(
                        report,
                        report_title=_build_report_export_title(normalized_state, normalized_state.get("resolved_customer") or {}),
                    )
                    documents = [
                        {
                            "name": report_export["fileName"],
                            "path": "",
                            "fileId": "",
                            "downloadUrl": report_export["downloadUrl"],
                            "kind": "report_export",
                            "buttonLabel": "Download Investigation Report PDF",
                        }
                    ]
                    documents_title = "Investigation Report Export"
                except Exception:
                    export_note = "\n\nNote: The report was generated, but the downloadable PDF could not be prepared right now."

                response = f"Generated Investigation Report:\n\n{report}{export_note}\n\n{_build_history_followup_prompt()}"
            else:
                response = f"Automated investigation reports are not available for your role.\n\n{_build_history_followup_prompt()}"
            normalized_state["step"] = "historical_docs"
        elif choice == "no":
            response = f"Skipping report generation.\n\n{_build_history_followup_prompt()}"
            normalized_state["step"] = "historical_docs"
        else:
            response = "Please reply Yes or No so I know whether to generate the investigation report."

        latest_analysis = normalized_state.get("latest_analysis") if isinstance(normalized_state.get("latest_analysis"), dict) else {}
        return {
            "user": user_context["userId"],
            "bank": user_context["bankId"],
            "query": query,
            "fraud_analysis": None,
            "chatbot_response": response,
            "next_step": normalized_state.get("step"),
            "sessionId": session_id,
            "fraud_category": latest_analysis.get("fraud_category") or "",
            "documents": documents,
            "documents_title": documents_title,
            "conversation_state": {**normalized_state, "sessionId": session_id},
        }

    if current_step == "historical_docs":
        documents = []
        documents_title = ""
        latest_analysis = normalized_state.get("latest_analysis") if isinstance(normalized_state.get("latest_analysis"), dict) else {}
        case_family = str(latest_analysis.get("case_family") or "").strip()
        transaction_relevance = str(latest_analysis.get("transaction_relevance") or "").strip().lower()

        if choice == "yes":
            if (user_context.get("permissions") or {}).get("canViewHistoricalCases"):
                documents = _fetch_historical_references(latest_analysis)
                documents_title = "Historical Fraud Case References"
                if documents:
                    response = f"I have added comparable historical fraud case references below.\n\n{_build_final_assistance_prompt()}"
                elif case_family in NON_TRANSACTION_FAMILIES and transaction_relevance == "not_applicable":
                    response = (
                        "Comparable historical fraud case references are not available yet for this case family. "
                        f"I can continue based on the {case_family.lower()} evidence already reviewed.\n\n{_build_final_assistance_prompt()}"
                    )
                else:
                    response = f"I could not find historical case references right now.\n\n{_build_final_assistance_prompt()}"
            else:
                response = f"Historical fraud case references are not available for your role.\n\n{_build_final_assistance_prompt()}"
            normalized_state["step"] = "final_assistance"
        elif choice == "no":
            response = f"Skipping historical case references.\n\n{_build_final_assistance_prompt()}"
            normalized_state["step"] = "final_assistance"
        else:
            response = "Please reply Yes or No so I know whether to fetch the historical fraud case references."

        latest_analysis = normalized_state.get("latest_analysis") if isinstance(normalized_state.get("latest_analysis"), dict) else {}
        return {
            "user": user_context["userId"],
            "bank": user_context["bankId"],
            "query": query,
            "fraud_analysis": None,
            "chatbot_response": response,
            "next_step": normalized_state.get("step"),
            "sessionId": session_id,
            "fraud_category": latest_analysis.get("fraud_category") or "",
            "documents": documents,
            "documents_title": documents_title,
            "conversation_state": {**normalized_state, "sessionId": session_id},
        }

    if current_step == "final_assistance":
        if choice == "yes":
            reset_state = _empty_integrated_state(session_id)
            reset_state["missing_fields"] = ["case_description", "customer_id", "start_datetime", "end_datetime"]
            response = "Okay. Please describe the next case, then share one unique customer identifier and the review date-time range."
            return {
                "user": user_context["userId"],
                "bank": user_context["bankId"],
                "query": query,
                "fraud_analysis": None,
                "chatbot_response": response,
                "next_step": "collect_case_details",
                "sessionId": session_id,
                "fraud_category": "",
                "documents": [],
                "documents_title": "",
                "conversation_state": reset_state,
            }

        if choice == "no":
            reset_state = _empty_integrated_state(session_id)
            response = "Thank you. When you have another case, just describe it and I will continue from there."
            return {
                "user": user_context["userId"],
                "bank": user_context["bankId"],
                "query": query,
                "fraud_analysis": None,
                "chatbot_response": response,
                "next_step": "conversation_end",
                "sessionId": session_id,
                "fraud_category": "",
                "documents": [],
                "documents_title": "",
                "conversation_state": {**reset_state, "step": "conversation_end"},
            }

        latest_analysis = normalized_state.get("latest_analysis") if isinstance(normalized_state.get("latest_analysis"), dict) else {}
        return {
            "user": user_context["userId"],
            "bank": user_context["bankId"],
            "query": query,
            "fraud_analysis": None,
            "chatbot_response": "Please reply Yes or No so I know whether you want to continue with another case.",
            "next_step": "final_assistance",
            "sessionId": session_id,
            "fraud_category": latest_analysis.get("fraud_category") or "",
            "documents": [],
            "documents_title": "",
            "conversation_state": {**normalized_state, "sessionId": session_id},
        }

    resolved_customer, customer_matches, attempted_identifier = _resolve_customer(normalized_query, normalized_state)
    merged_state = _merge_integrated_state(normalized_state, resolved_customer, normalized_query)
    has_customer = _has_resolved_customer(merged_state.get("resolved_customer"))
    merged_state["missing_fields"] = _build_integrated_missing_fields(merged_state, has_customer)

    if attempted_identifier and not resolved_customer and customer_matches:
        merged_state["customer_name"] = merged_state.get("customer_name") or attempted_identifier
        merged_state["missing_fields"] = _build_integrated_missing_fields(merged_state, False)
        merged_state["step"] = "collect_customer"
        response = _build_identifier_requirement_message(attempted_identifier, customer_matches)
        return {
            "user": user_context["userId"],
            "bank": user_context["bankId"],
            "query": query,
            "fraud_analysis": None,
            "chatbot_response": response,
            "next_step": "collect_customer",
            "sessionId": session_id,
            "fraud_category": "",
            "documents": [],
            "documents_title": "",
            "conversation_state": {**merged_state, "sessionId": session_id},
        }

    if attempted_identifier and not resolved_customer and not customer_matches:
        _clear_resolved_customer(merged_state)
        merged_state["missing_fields"] = _build_integrated_missing_fields(merged_state, False)
        merged_state["step"] = "collect_customer"
        response = (
            f"I could not find a customer for `{attempted_identifier}`. "
            "Please share a valid CIF ID, account number, PAN, or registered mobile number."
        )
        return {
            "user": user_context["userId"],
            "bank": user_context["bankId"],
            "query": query,
            "fraud_analysis": None,
            "chatbot_response": response,
            "next_step": "collect_customer",
            "sessionId": session_id,
            "fraud_category": "",
            "documents": [],
            "documents_title": "",
            "conversation_state": {**merged_state, "sessionId": session_id},
        }

    if customer_matches and len(customer_matches) > 1 and not resolved_customer:
        _clear_resolved_customer(merged_state)
        merged_state["missing_fields"] = _build_integrated_missing_fields(merged_state, False)
        merged_state["step"] = "collect_customer"
        response = _build_integrated_followup_question(merged_state, customer_matches)
        return {
            "user": user_context["userId"],
            "bank": user_context["bankId"],
            "query": query,
            "fraud_analysis": None,
            "chatbot_response": response,
            "next_step": "collect_customer",
            "sessionId": session_id,
            "fraud_category": "",
            "documents": [],
            "documents_title": "",
            "conversation_state": {**merged_state, "sessionId": session_id},
        }

    if merged_state["missing_fields"]:
        merged_state["step"] = "collect_inputs"
        response = _build_integrated_followup_question(merged_state, [])
        return {
            "user": user_context["userId"],
            "bank": user_context["bankId"],
            "query": query,
            "fraud_analysis": None,
            "chatbot_response": response,
            "next_step": "collect_inputs",
            "sessionId": session_id,
            "fraud_category": "",
            "documents": [],
            "documents_title": "",
            "conversation_state": {**merged_state, "sessionId": session_id},
        }

    start_datetime = _parse_state_datetime(merged_state.get("start_datetime"))
    end_datetime = _parse_state_datetime(merged_state.get("end_datetime"))
    customer = _normalize_resolved_customer_payload(merged_state.get("resolved_customer"))
    transaction_relevance = str(merged_state.get("transaction_relevance") or "primary").strip().lower()

    if transaction_relevance == "not_applicable" and (not start_datetime or not end_datetime):
        merged_state["review_scope_mode"] = "relationship_history"
        merged_state["review_scope_label"] = "Full relationship history"
        if not start_datetime:
            start_datetime = dt.datetime(2000, 1, 1, 0, 0, 0)
            merged_state["start_datetime"] = _format_datetime(start_datetime)
        if not end_datetime:
            end_datetime = _now()
            merged_state["end_datetime"] = _format_datetime(end_datetime)

    if not start_datetime or not end_datetime:
        merged_state["missing_fields"] = _build_integrated_missing_fields(merged_state, True)
        merged_state["step"] = "collect_inputs"
        response = _build_integrated_followup_question(merged_state, [])
        return {
            "user": user_context["userId"],
            "bank": user_context["bankId"],
            "query": query,
            "fraud_analysis": None,
            "chatbot_response": response,
            "next_step": "collect_inputs",
            "sessionId": session_id,
            "fraud_category": "",
            "documents": [],
            "documents_title": "",
            "conversation_state": {**merged_state, "sessionId": session_id},
        }

    case_description = str(merged_state.get("case_description") or "").strip()
    route = _classify_case_family(case_description)
    transactions = _fetch_transactions(str(customer.get("cif_id") or ""), start_datetime, end_datetime)
    transaction_analysis = _analyze_transactions(customer, transactions, start_datetime, end_datetime)
    route = _refine_case_route_with_transactions(route, transaction_analysis, merged_state.get("review_scope_mode"))
    routed_analysis = _apply_case_route_to_analysis(case_description, route, transaction_analysis)
    routed_analysis = _apply_case_context_to_analysis(route, routed_analysis, customer, case_description)
    scope_mode = str(merged_state.get("review_scope_mode") or "").strip()
    scope_label = str(merged_state.get("review_scope_label") or "").strip()
    if scope_mode == "relationship_history":
        routed_analysis["review_scope_mode"] = scope_mode
        routed_analysis["review_scope_label"] = (
            "Supporting transaction context: full relationship history"
            if str(routed_analysis.get("transaction_relevance") or "").strip().lower() == "supporting"
            else scope_label or "Full relationship history"
        )
    sop_query = _build_sop_grounding_query(case_description, customer, routed_analysis)
    sop_analysis = detect_fraud(sop_query, user_context["bankId"])
    combined_analysis = _combine_transaction_and_sop_analysis(
        case_description,
        routed_analysis,
        sop_analysis if isinstance(sop_analysis, dict) else {},
    )

    merged_state["step"] = "fetch_documentation"
    merged_state["case_family"] = route.get("case_family")
    merged_state["suspicion_direction"] = route.get("suspicion_direction")
    merged_state["investigation_basis"] = route.get("investigation_basis")
    merged_state["transaction_relevance"] = route.get("transaction_relevance")
    merged_state["evidence_modules_used"] = list(route.get("evidence_modules_used") or [])
    merged_state["review_scope_mode"] = str(combined_analysis.get("review_scope_mode") or scope_mode or "").strip()
    merged_state["review_scope_label"] = str(combined_analysis.get("review_scope_label") or scope_label or "").strip()
    merged_state["case_query"] = _build_report_case_query(merged_state, customer)
    merged_state["missing_fields"] = []
    merged_state["latest_analysis"] = combined_analysis
    merged_state["sop_analysis"] = sop_analysis if isinstance(sop_analysis, dict) else {}

    return {
        "user": user_context["userId"],
        "bank": user_context["bankId"],
        "query": query,
        "fraud_analysis": combined_analysis,
        "chatbot_response": f"{_build_combined_chat_response(customer, combined_analysis)}\n\n{_build_documents_followup_prompt()}",
        "next_step": "fetch_documentation",
        "sessionId": session_id,
        "fraud_category": combined_analysis.get("fraud_category") or "",
        "documents": [],
        "documents_title": "",
        "conversation_state": {**merged_state, "sessionId": session_id},
    }
