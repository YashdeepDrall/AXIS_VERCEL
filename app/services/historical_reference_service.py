from __future__ import annotations

import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any

from app.core.config import AXIS_BANK_ID
from app.db.mongodb import cases_collection


HISTORICAL_CASES_FILE = Path(__file__).resolve().parents[2] / "seed" / "historical_cases.json"


def _clean_text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip() or default


def _clean_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _slugify(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", _clean_text(value).lower())
    return normalized.strip("-") or "historical-case"


def _load_seed_payload() -> list[dict[str, Any]]:
    if not HISTORICAL_CASES_FILE.exists():
        return []

    try:
        payload = json.loads(HISTORICAL_CASES_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []

    return payload if isinstance(payload, list) else []


def _normalize_case_record(raw_case: dict[str, Any]) -> dict[str, Any]:
    title = _clean_text(raw_case.get("title") or raw_case.get("fileName"), "Historical Fraud Reference")
    case_id = _clean_text(raw_case.get("caseId") or raw_case.get("case_id"), _slugify(title).upper())

    return {
        "bankId": AXIS_BANK_ID,
        "caseId": case_id,
        "fileName": title,
        "title": title,
        "referenceTag": _clean_text(raw_case.get("referenceTag"), case_id),
        "category": _clean_text(raw_case.get("category"), "General Fraud Reference"),
        "channel": _clean_text(raw_case.get("channel"), "Multiple"),
        "caseWindow": _clean_text(raw_case.get("caseWindow")),
        "customerProfile": _clean_text(raw_case.get("customerProfile")),
        "incidentSummary": _clean_text(raw_case.get("incidentSummary")),
        "transactionPattern": _clean_text(raw_case.get("transactionPattern")),
        "suspiciousIndicators": _clean_list(raw_case.get("suspiciousIndicators")),
        "investigationHighlights": _clean_list(raw_case.get("investigationHighlights")),
        "actionTaken": _clean_text(raw_case.get("actionTaken")),
        "outcome": _clean_text(raw_case.get("outcome")),
        "sopRelevance": _clean_text(raw_case.get("sopRelevance")),
        "learning": _clean_text(raw_case.get("learning")),
    }


def seed_historical_reference_cases() -> int:
    seeded_count = 0

    for raw_case in _load_seed_payload():
        if not isinstance(raw_case, dict):
            continue

        record = _normalize_case_record(raw_case)
        cases_collection.update_one(
            {
                "bankId": record["bankId"],
                "caseId": record["caseId"],
            },
            {
                "$set": deepcopy(record),
            },
            upsert=True,
        )
        seeded_count += 1

    return seeded_count


def list_historical_reference_cards(limit: int | None = None) -> list[dict[str, Any]]:
    if not cases_collection.find_one({"bankId": AXIS_BANK_ID}):
        seed_historical_reference_cases()

    cards: list[dict[str, Any]] = []

    for raw_case in cases_collection.find({"bankId": AXIS_BANK_ID}):
        cards.append(
            {
                "name": _clean_text(raw_case.get("title") or raw_case.get("fileName"), "Historical Fraud Reference"),
                "path": "",
                "fileId": "",
                "downloadUrl": "",
                "referenceTag": _clean_text(raw_case.get("referenceTag")),
                "category": _clean_text(raw_case.get("category"), "General Fraud Reference"),
                "channel": _clean_text(raw_case.get("channel"), "Multiple"),
                "caseWindow": _clean_text(raw_case.get("caseWindow")),
                "customerProfile": _clean_text(raw_case.get("customerProfile")),
                "incidentSummary": _clean_text(raw_case.get("incidentSummary")),
                "transactionPattern": _clean_text(raw_case.get("transactionPattern")),
                "suspiciousIndicators": _clean_list(raw_case.get("suspiciousIndicators")),
                "investigationHighlights": _clean_list(raw_case.get("investigationHighlights")),
                "actionTaken": _clean_text(raw_case.get("actionTaken")),
                "outcome": _clean_text(raw_case.get("outcome")),
                "sopRelevance": _clean_text(raw_case.get("sopRelevance")),
                "learning": _clean_text(raw_case.get("learning")),
            }
        )

    cards.sort(key=lambda item: (_clean_text(item.get("referenceTag")), _clean_text(item.get("name"))))
    return cards[:limit] if isinstance(limit, int) and limit > 0 else cards
