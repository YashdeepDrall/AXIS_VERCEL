from __future__ import annotations

import datetime as dt
import os
import re
import uuid
from collections import Counter
from typing import Any

from app.core.config import AXIS_BANK_DIR, AXIS_BANK_ID, AXIS_BLUEPRINT_FILE, BASE_DIR
from app.db.banking import get_customers_collection, get_transactions_collection
from app.db.mongodb import cases_collection, documents_collection, fs
from app.services.fraud_service import detect_fraud, generate_investigation_report
from app.services.historical_reference_service import list_historical_reference_cards
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
FOLLOWUP_STEPS = {"fetch_documentation", "generate_report", "historical_docs", "final_assistance"}


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


def _fetch_historical_references() -> list[dict[str, Any]]:
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
        state["resolved_customer"] = {
            "cif_id": str(resolved_customer.get("cif_id") or "").strip(),
            "name": str(resolved_customer.get("name") or "").strip(),
            "mobile": str(resolved_customer.get("mobile") or "").strip(),
            "pan": str(resolved_customer.get("pan") or "").strip(),
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
    cleaned = re.sub(r"\bCIF\d{4,}\b", " ", query or "", flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(?:ACC\d{4,}|[1-9]\d{11,17})\b", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b[A-Z]{5}\d{4}[A-Z]\b", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"(?<!\d)([6-9]\d{9})(?!\d)", " ", cleaned)
    cleaned = re.sub(r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?\b", " ", cleaned)
    cleaned = re.sub(r"\b\d{1,2}[-/]\d{1,2}[-/]\d{4}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?\b", " ", cleaned)
    cleaned = re.sub(r"\b(?:today|yesterday|last|past|days?|debit|debits|upi|imps|atm|pos|netbanking|transaction|transactions|reported|report|review|check|analyse|analyze|inspect|fraud|scam|case|customer|from|to|between|range|without|approval|money|withdrawal|withdrawals|unauthorised|unauthorized)\b", " ", cleaned, flags=re.IGNORECASE)
    tokens = [token for token in re.findall(r"[A-Za-z]+", cleaned) if len(token) >= 1]
    if 1 <= len(tokens) <= 4:
        return " ".join(token.title() for token in tokens)

    action_match = re.search(
        r"^\s*([A-Za-z]+(?:\s+[A-Za-z]+){0,2})\s+"
        r"(?:clicked|clicks|received|got|lost|shared|entered|used|opened|reported|faced|saw|noticed|sent|transferred|made|paid|withdrew|withdrawn|complained)\b",
        query or "",
        flags=re.IGNORECASE,
    )
    if action_match:
        candidate = " ".join(token.title() for token in re.findall(r"[A-Za-z]+", action_match.group(1)))
        if candidate.lower() not in {"customer", "user", "victim", "account", "he", "she", "they"}:
            return candidate

    leading_name = re.match(r"^\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})\b", query or "")
    if leading_name:
        candidate = " ".join(token.title() for token in re.findall(r"[A-Za-z]+", leading_name.group(1)))
        if candidate.lower() not in {"customer", "user", "victim", "account"}:
            return candidate

    return None


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


def _resolve_customer(query: str, state: dict[str, Any]) -> tuple[dict[str, Any] | None, list[dict[str, Any]], str | None]:
    customers_collection = get_customers_collection()
    cif_id = _extract_cif_id(query) or state.get("cif_id")
    account_id = _extract_account_id(query) or state.get("account_id")
    pan = _extract_pan(query) or state.get("pan")
    mobile = _extract_mobile(query) or state.get("mobile")
    name_fragment = _extract_name_fragment(query) or state.get("customer_name")

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


def _empty_integrated_state(session_id: str | None = None) -> dict[str, Any]:
    return {
        "step": "collect_case_details",
        "sessionId": session_id,
        "case_query": None,
        "case_description": None,
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
        state["resolved_customer"] = {
            "cif_id": str(resolved_customer.get("cif_id") or "").strip(),
            "name": str(resolved_customer.get("name") or "").strip(),
            "mobile": str(resolved_customer.get("mobile") or "").strip(),
            "pan": str(resolved_customer.get("pan") or "").strip(),
            "accounts": [str(item).strip() for item in resolved_customer.get("accounts") or [] if str(item).strip()],
        }

    missing_fields = raw_state.get("missing_fields")
    if isinstance(missing_fields, list):
        state["missing_fields"] = [str(item).strip() for item in missing_fields if str(item).strip()]

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
    candidate = re.sub(r"\bCIF\d{4,}\b", " ", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"(?<!\d)([6-9]\d{9})(?!\d)", " ", candidate)
    candidate = re.sub(r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?\b", " ", candidate)
    candidate = re.sub(r"\b\d{1,2}[-/]\d{1,2}[-/]\d{4}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?\b", " ", candidate)
    candidate = re.sub(r"\b(?:last|past)\s+\d{1,2}\s+days?\b", " ", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"\b(?:today|yesterday)\b", " ", candidate, flags=re.IGNORECASE)
    candidate = re.sub(
        r"\b(?:check|review|analyse|analyze|inspect|see|look|please|customer|cif|id|mobile|phone|number|registered|for|from|to|between|and|range|account|accounts)\b",
        " ",
        candidate,
        flags=re.IGNORECASE,
    )
    candidate = " ".join(candidate.split())

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
    return merged


def _build_integrated_missing_fields(state: dict[str, Any], has_customer: bool) -> list[str]:
    missing_fields: list[str] = []

    if not state.get("case_description"):
        missing_fields.append("case_description")
    if not has_customer:
        missing_fields.append("customer_id")
    if not state.get("start_datetime"):
        missing_fields.append("start_datetime")
    if not state.get("end_datetime"):
        missing_fields.append("end_datetime")

    return missing_fields


def _build_integrated_followup_question(state: dict[str, Any], customer_matches: list[dict[str, Any]]) -> str:
    missing = state.get("missing_fields") or []

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
        return "Please share the start date/time for the transaction review window in `YYYY-MM-DD HH:MM` format."

    if "end_datetime" in missing:
        return "Please share the end date/time for the transaction review window in `YYYY-MM-DD HH:MM` format."

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
- Risk level from transaction review: {analysis.get("risk_level")}
- Transaction-led classification: {analysis.get("fraud_classification")}
- Transaction reasoning summary: {analysis.get("reasoning_summary")}
- Channels involved: {channels}
- Suspicious patterns: {pattern_text}
- Flagged transactions: {flagged_text}

Based on the AXIS Bank SOP, identify the closest fraud category, explain what likely happened, list the suspicious indicators, and provide the immediate SOP-grounded action.
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
    sop_has_specific_category = sop_supported and sop_category and sop_category.lower() not in {"unknown"}
    sop_has_specific_classification = sop_supported and sop_classification and sop_classification.lower() not in {
        "unknown",
        "manual review required",
        "no transactions found in the selected window",
    }

    combined["case_description"] = case_description
    combined["supported"] = True
    combined["transaction_classification"] = transaction_analysis.get("fraud_classification")
    combined["fraud_category"] = sop_category if sop_has_specific_category else "Transaction-Led Review"
    combined["fraud_classification"] = (
        sop_classification
        if sop_has_specific_classification
        else tx_classification
    )
    combined["risk_level"] = sop_risk if _risk_rank(sop_risk) > _risk_rank(tx_risk) else tx_risk
    combined["sop_supported"] = sop_supported
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
        or str(transaction_analysis.get("reasoning_summary") or "").strip()
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

    recommended_actions: list[str] = []
    for candidate in [
        combined.get("recommended_action"),
        *((transaction_analysis.get("recommended_actions") or []) if isinstance(transaction_analysis.get("recommended_actions"), list) else []),
    ]:
        text = str(candidate or "").strip()
        if text and text not in recommended_actions:
            recommended_actions.append(text)
    combined["recommended_actions"] = recommended_actions[:5]

    return combined


def _build_combined_chat_response(customer: dict[str, Any], analysis: dict[str, Any]) -> str:
    patterns = analysis.get("suspicious_patterns") or []
    actions = analysis.get("recommended_actions") or []
    query_window = analysis.get("query_window") or {}
    primary_pattern = next(
        (
            f"{item.get('pattern')}: {item.get('details')}"
            for item in patterns
            if item.get("pattern")
        ),
        "No strong suspicious transaction pattern was triggered in the selected review window.",
    )
    first_action = next((str(item).strip() for item in actions if str(item).strip()), "Proceed with manual review and safeguard the impacted customer account.")
    review_window = " to ".join(
        item
        for item in [
            query_window.get("start_datetime"),
            query_window.get("end_datetime"),
        ]
        if item
    ) or "The supplied transaction review window"

    return (
        "Executive Summary:\n"
        f"- Customer confirmed: {customer.get('name')} ({customer.get('cif_id')})\n"
        f"- Review window: {review_window}\n"
        f"- Risk level: {analysis.get('risk_level') or 'Medium'}\n"
        f"- Likely fraud type: {analysis.get('fraud_classification') or 'Manual review required'}\n"
        f"- Key concern: {primary_pattern}\n"
        f"- Immediate next action: {first_action}"
    )


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

    parts = []
    if case_description:
        parts.append(f"Case summary: {case_description}")
    if customer_name or cif_id:
        label = customer_name or "Customer"
        suffix = f" ({cif_id})" if cif_id else ""
        parts.append(f"Customer verified: {label}{suffix}")
    if start_datetime and end_datetime:
        parts.append(f"Reviewed window: {start_datetime} to {end_datetime}")

    return ". ".join(part for part in parts if part).strip() or "Customer fraud investigation case"


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
        if choice == "yes":
            if (user_context.get("permissions") or {}).get("canGenerateReport"):
                analysis = normalized_state.get("latest_analysis") if isinstance(normalized_state.get("latest_analysis"), dict) else {}
                report_query = (
                    str(normalized_state.get("case_query") or "").strip()
                    or str(normalized_state.get("case_description") or "").strip()
                    or query
                )
                report = generate_investigation_report(report_query, user_context["bankId"], analysis)
                response = f"Generated Investigation Report:\n\n{report}\n\n{_build_history_followup_prompt()}"
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
            "documents": [],
            "documents_title": "",
            "conversation_state": {**normalized_state, "sessionId": session_id},
        }

    if current_step == "historical_docs":
        documents = []
        documents_title = ""

        if choice == "yes":
            if (user_context.get("permissions") or {}).get("canViewHistoricalCases"):
                documents = _fetch_historical_references()
                documents_title = "Historical Fraud Case References"
                if documents:
                    response = f"I have added comparable historical fraud case references below.\n\n{_build_final_assistance_prompt()}"
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
    has_customer = bool(merged_state.get("resolved_customer"))
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
    customer = merged_state.get("resolved_customer") or {}

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

    transactions = _fetch_transactions(str(customer.get("cif_id") or ""), start_datetime, end_datetime)
    transaction_analysis = _analyze_transactions(customer, transactions, start_datetime, end_datetime)
    sop_query = _build_sop_grounding_query(str(merged_state.get("case_description") or "").strip(), customer, transaction_analysis)
    sop_analysis = detect_fraud(sop_query, user_context["bankId"])
    combined_analysis = _combine_transaction_and_sop_analysis(
        str(merged_state.get("case_description") or "").strip(),
        transaction_analysis,
        sop_analysis if isinstance(sop_analysis, dict) else {},
    )

    merged_state["step"] = "fetch_documentation"
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
