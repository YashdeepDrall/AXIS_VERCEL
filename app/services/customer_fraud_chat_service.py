from __future__ import annotations

import datetime as dt
import re
import uuid
from collections import Counter
from typing import Any

from app.db.banking import get_customers_collection, get_transactions_collection
from app.services.llm_service import GeminiServiceError, generate_text


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


def _format_datetime(value: dt.datetime | None) -> str | None:
    if value is None:
        return None
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _start_of_day(value: dt.datetime) -> dt.datetime:
    return value.replace(hour=0, minute=0, second=0, microsecond=0)


def _end_of_day(value: dt.datetime) -> dt.datetime:
    return value.replace(hour=23, minute=59, second=59, microsecond=0)


def _now() -> dt.datetime:
    return dt.datetime.now().replace(microsecond=0)


def _empty_state(session_id: str | None = None) -> dict[str, Any]:
    return {
        "step": "collect_customer",
        "sessionId": session_id,
        "cif_id": None,
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
        state["resolved_customer"] = {
            "cif_id": str(resolved_customer.get("cif_id") or "").strip(),
            "name": str(resolved_customer.get("name") or "").strip(),
            "mobile": str(resolved_customer.get("mobile") or "").strip(),
            "accounts": [str(item).strip() for item in resolved_customer.get("accounts") or [] if str(item).strip()],
        }

    missing_fields = raw_state.get("missing_fields")
    if isinstance(missing_fields, list):
        state["missing_fields"] = [str(item).strip() for item in missing_fields if str(item).strip()]

    latest_analysis = raw_state.get("latest_analysis")
    if isinstance(latest_analysis, dict):
        state["latest_analysis"] = latest_analysis

    return state


def _normalize_query(text: str) -> str:
    return " ".join((text or "").strip().split())


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


def _extract_cif_id(query: str) -> str | None:
    match = re.search(r"\bCIF\d{4,}\b", query or "", flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(0).upper()


def _extract_mobile(query: str) -> str | None:
    match = re.search(r"(?<!\d)([6-9]\d{9})(?!\d)", query or "")
    if not match:
        return None
    return match.group(1)


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
        "accounts": [str(item).strip() for item in customer.get("accounts") or [] if str(item).strip()],
    }


def _resolve_customer(query: str, state: dict[str, Any]) -> tuple[dict[str, Any] | None, list[dict[str, Any]], str | None]:
    customers_collection = get_customers_collection()
    cif_id = _extract_cif_id(query) or state.get("cif_id")
    mobile = _extract_mobile(query) or state.get("mobile")
    query_lower = (query or "").lower()

    if cif_id:
        customer = customers_collection.find_one({"cif_id": cif_id}, {"_id": 0})
        return customer, [customer] if customer else [], cif_id

    if mobile:
        customer = customers_collection.find_one({"mobile": mobile}, {"_id": 0})
        return customer, [customer] if customer else [], mobile

    existing_name = str(state.get("customer_name") or "").strip().lower()
    matches = []
    for customer in _all_customers():
        name = str(customer.get("name") or "").strip()
        if not name:
            continue
        normalized_name = name.lower()
        if normalized_name and (normalized_name in query_lower or (existing_name and normalized_name == existing_name)):
            matches.append(customer)

    if len(matches) == 1:
        return matches[0], matches, matches[0].get("name")

    return None, matches, existing_name or None


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
            "I found multiple customers matching that input. "
            f"Please share the exact CIF ID or mobile number. Matches: {options}."
        )

    if "customer_id" in missing and {"start_datetime", "end_datetime"}.issubset(set(missing)):
        return (
            "Please share the customer identifier and the review window. "
            "You can send the CIF ID plus a range like `CIF1001 from 2026-04-17 00:00 to 2026-04-18 23:59`."
        )

    if "customer_id" in missing:
        return "Please share the customer CIF ID or registered mobile number so I can identify the customer uniquely."

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
    if not patterns:
        return (
            f"I reviewed the selected transaction window for {customer.get('name')} ({customer.get('cif_id')}) "
            "and did not find a strong fraud pattern in the available transactions."
        )

    pattern_text = "; ".join(item.get("details", "") for item in patterns[:3] if item.get("details"))
    return (
        f"I reviewed the selected transaction window for {customer.get('name')} ({customer.get('cif_id')}). "
        f"Risk is {analysis.get('risk_level', 'Medium')} because {pattern_text}"
    ).strip()


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
            "recommended_actions": [
                "No transactions were found for the selected range. Ask for a different date range if further review is needed."
            ],
            "reasoning_summary": (
                f"No transactions were found for {customer.get('name')} ({customer.get('cif_id')}) in the selected review window."
            ),
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

    flagged_transactions = _build_flagged_transactions(transactions, reasons_by_txn)
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
        "recommended_actions": recommended_actions,
    }
    analysis["reasoning_summary"] = _build_reasoning_summary(customer, analysis)
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
    has_customer = bool(merged_state.get("resolved_customer"))
    merged_state["missing_fields"] = _build_missing_fields(merged_state, has_customer)

    if attempted_identifier and not resolved_customer and not customer_matches:
        _clear_resolved_customer(merged_state)
        merged_state["missing_fields"] = _build_missing_fields(merged_state, False)
        merged_state["step"] = "collect_customer"
        response = (
            f"I could not find a customer for `{attempted_identifier}`. "
            "Please share a valid CIF ID or registered mobile number."
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
    customer = merged_state.get("resolved_customer") or {}

    if not start_datetime or not end_datetime:
        merged_state["missing_fields"] = _build_missing_fields(merged_state, True)
        merged_state["step"] = "collect_inputs"
        response = _build_followup_question(merged_state, [])
        return {
            "user": user_context["userId"],
            "query": query,
            "chatbot_response": response,
            "next_step": "collect_inputs",
            "sessionId": session_id,
            "customer_identified": True,
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
        "customer_identified": True,
        "customer": customer,
        "fraud_analysis": analysis,
        "conversation_state": {**merged_state, "sessionId": session_id},
    }
