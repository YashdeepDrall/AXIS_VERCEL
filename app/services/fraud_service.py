import re
from textwrap import dedent

from app.services.llm_service import GeminiServiceError, generate_text, parse_json_response
from app.services.rag_service import retrieve_context


def _normalize_text(value, default=""):
    if value is None:
        return default
    return str(value).strip() or default


def _normalize_risk_level(value):
    normalized = _normalize_text(value, "Medium").lower()

    if normalized == "high":
        return "High"
    if normalized == "low":
        return "Low"
    return "Medium"


def _normalize_list(value):
    if isinstance(value, list):
        items = [str(item).strip() for item in value if str(item).strip()]
        return items[:5]

    if isinstance(value, str) and value.strip():
        return [value.strip()]

    return []


def _excerpt(text, limit=420):
    compact = " ".join((text or "").split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3].rstrip() + "..."


def _friendly_service_note(error, stage="analysis"):
    message = _normalize_text(error, "The AXIS reasoning service is temporarily unavailable.")
    lowered = message.lower()
    is_capacity_issue = isinstance(error, GeminiServiceError) and error.status_code == 503
    is_capacity_issue = is_capacity_issue or any(
        marker in lowered
        for marker in [
            "503",
            "high demand",
            "temporarily unavailable",
            "service unavailable",
            "overloaded",
            "try again later",
        ]
    )

    if stage == "retrieval":
        if is_capacity_issue:
            return (
                "The live AXIS SOP retrieval service is temporarily busy. "
                "Please retry shortly, or continue with manual SOP review if the case is urgent."
            )
        return "The AXIS SOP retrieval service could not prepare context for this case right now. Please retry shortly."

    if stage == "report":
        if is_capacity_issue:
            return (
                "The live report-generation service is temporarily busy. "
                "A structured fallback report has been prepared from the grounded case analysis."
            )
        return "The live report-generation service was unavailable, so a structured fallback report has been prepared."

    if is_capacity_issue:
        return (
            "The live AXIS SOP reasoning service is temporarily busy. "
            "A grounded fallback response has been prepared from the retrieved blueprint context."
        )

    return "The live AXIS SOP reasoning service was unavailable, so a grounded fallback response has been prepared."


def _normalize_action_lines(analysis):
    candidates = []

    for value in analysis.get("recommended_actions") or []:
        text = _normalize_text(value)
        if text and text not in candidates:
            candidates.append(text)

    single_action = _normalize_text(analysis.get("recommended_action"))
    if single_action and single_action not in candidates:
        candidates.append(single_action)

    if not candidates:
        candidates.append("Continue investigator review based on the retrieved SOP context and apply immediate account safeguards.")

    return candidates[:5]


def _report_fraud_category(analysis):
    category = _normalize_text(analysis.get("fraud_category"))
    if category and category.lower() not in {"unknown"}:
        return category
    return "Transaction-Led Review"


def _report_fraud_type(analysis):
    primary = _normalize_text(analysis.get("fraud_classification"))
    if primary and primary.lower() not in {"unknown", "manual review required"}:
        return primary

    secondary = _normalize_text(analysis.get("transaction_classification"))
    if secondary:
        return secondary

    return "Manual review required"


def _strip_code_fences(text):
    cleaned = _normalize_text(text)
    cleaned = re.sub(r"^```(?:text)?\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def _build_prompt(query, ranked_chunks):
    formatted_chunks = _format_ranked_chunks(ranked_chunks)
    joined_context = "\n\n".join(formatted_chunks)

    return f"""
You are an AXIS Bank fraud investigation assistant.
Use only the retrieved SOP context below. Do not use outside knowledge.

If the retrieved SOP context is not relevant enough to answer the user query, respond with JSON where:
- "supported" is false
- "reason" briefly explains why

If the context is relevant, respond with JSON only using this schema:
{{
  "supported": true,
  "fraud_category": "short category code or Unknown",
  "fraud_classification": "short classification",
  "risk_level": "Low | Medium | High",
  "suspicious_indicators": ["2 to 5 short items"],
  "relevant_information": "short grounded explanation",
  "recommended_action": "clear SOP-grounded next step",
  "sop_summary": "2 to 4 sentence answer grounded in the retrieved SOP context",
  "reason": ""
}}

User query:
{query}

Retrieved SOP context:
{joined_context}
""".strip()


def _format_ranked_chunks(ranked_chunks):
    formatted_chunks = []

    for index, chunk in enumerate(ranked_chunks, start=1):
        formatted_chunks.append(
            (
                f"[Chunk {index} | Source: {chunk['file_name']} | Similarity: {chunk['score']:.3f}]\n"
                f"{chunk['text']}"
            )
        )

    return formatted_chunks

def _build_report_prompt(query, ranked_chunks, analysis):
    formatted_chunks = _format_ranked_chunks(ranked_chunks)
    joined_context = "\n\n".join(formatted_chunks)
    indicators = analysis.get("suspicious_indicators") or []
    indicator_text = ", ".join(indicators) if indicators else "None explicitly identified yet"

    return dedent(f"""
    You are preparing an AXIS Bank fraud investigation report.
    Use only the retrieved SOP context and grounded analysis below. Do not invent facts.

    Write a concise but operationally useful report in plain text using exactly these section headings:
    INVESTIGATION REPORT
    Case Overview:
    Customer and Exposure Snapshot:
    Fraud Assessment:
    Observed Red Flags:
    SOP-Grounded Findings:
    Immediate Containment Actions:
    Evidence and Documentation Required:
    Escalation and Reporting:
    Source References:

    Requirements:
    - Keep every point grounded in the retrieved SOP context.
    - Make the output look like a banker-ready investigation note.
    - Make the actions specific, sequenced, and investigator-friendly.
    - Mention IFMS, ALP, logs, device or document review only if supported by the context.
    - If something is uncertain, say "Based on retrieved SOP context" instead of assuming.
    - Under Source References, list the retrieved source file names.
    - Use flat bullet points under each heading.

    Grounded analysis:
    - Fraud category: {analysis.get('fraud_category', 'Unknown')}
    - Fraud classification: {analysis.get('fraud_classification', 'Manual review required')}
    - Risk level: {analysis.get('risk_level', 'Medium')}
- Indicators: {indicator_text}
- Relevant information: {analysis.get('relevant_information', '')}
- Recommended action: {analysis.get('recommended_action', '')}

    User query:
    {query}

    Retrieved SOP context:
    {joined_context}
    """).strip()


def _fallback_analysis(raw_response, reference_files, context, service_note=""):
    relevant_excerpt = _excerpt(context)
    summary_body = _normalize_text(raw_response, relevant_excerpt)

    return {
        "supported": True,
        "fraud_category": "Unknown",
        "fraud_classification": "Manual review required",
        "risk_level": "Medium",
        "suspicious_indicators": [],
        "relevant_information": relevant_excerpt,
        "recommended_action": "Review the retrieved SOP context manually and continue with investigator validation.",
        "sop_summary": summary_body,
        "reason": _normalize_text(service_note),
        "references": reference_files,
    }


def detect_fraud(query, bank_id):
    try:
        context, reference_files, ranked_chunks = retrieve_context(query, bank_id)
    except GeminiServiceError as exc:
        return {
            "supported": False,
            "reason": _friendly_service_note(exc, stage="retrieval"),
        }

    if not context.strip():
        return {
            "supported": False,
            "reason": "No relevant SOP context was retrieved for this query yet.",
        }

    prompt = _build_prompt(query, ranked_chunks)

    try:
        raw_response = generate_text(prompt)
    except GeminiServiceError as exc:
        return _fallback_analysis(
            raw_response="",
            reference_files=reference_files,
            context=context,
            service_note=_friendly_service_note(exc, stage="analysis"),
        )

    try:
        parsed = parse_json_response(raw_response)
    except Exception:
        return _fallback_analysis(raw_response, reference_files, context)

    supported = bool(parsed.get("supported", True))

    analysis = {
        "supported": supported,
        "fraud_category": _normalize_text(parsed.get("fraud_category"), "Unknown"),
        "fraud_classification": _normalize_text(parsed.get("fraud_classification"), "Manual review required"),
        "risk_level": _normalize_risk_level(parsed.get("risk_level")),
        "suspicious_indicators": _normalize_list(parsed.get("suspicious_indicators")),
        "relevant_information": _normalize_text(parsed.get("relevant_information"), _excerpt(context)),
        "recommended_action": _normalize_text(
            parsed.get("recommended_action"),
            "Review the retrieved SOP context and continue with investigator validation.",
        ),
        "sop_summary": _normalize_text(parsed.get("sop_summary"), _excerpt(context)),
        "reason": _normalize_text(parsed.get("reason")),
        "references": reference_files,
    }

    if not analysis["supported"] and not analysis["reason"]:
        analysis["reason"] = "The retrieved SOP context does not clearly cover this query."

    return analysis


def _fallback_report(query, analysis, service_note=""):
    references = analysis.get("references") or []
    indicators = analysis.get("suspicious_indicators") or []
    indicator_lines = [f"- {item}" for item in indicators] if indicators else ["- No specific indicators extracted"]
    source_lines = [f"- {item}" for item in references] if references else ["- No source references available"]
    action_lines = [f"- {item}" for item in _normalize_action_lines(analysis)]
    sop_summary = _normalize_text(
        analysis.get("sop_summary") or analysis.get("relevant_information"),
        "Based on retrieved SOP context, further investigator validation is required.",
    )
    sections = [
        "INVESTIGATION REPORT",
        "Case Overview:",
        f"- Reported case: {query}",
        "- Report basis: Grounded AXIS SOP case summary",
        "- Prepared status: Draft for investigator validation",
        "",
        "Customer and Exposure Snapshot:",
        f"- Risk level: {analysis.get('risk_level', 'Medium')}",
        f"- Likely fraud type: {_report_fraud_type(analysis)}",
        f"- Fraud category: {_report_fraud_category(analysis)}",
        "",
        "Fraud Assessment:",
        f"- Fraud Category: {_report_fraud_category(analysis)}",
        f"- Fraud Classification: {_report_fraud_type(analysis)}",
        f"- Risk Level: {analysis.get('risk_level', 'Medium')}",
        "",
        "Observed Red Flags:",
        *indicator_lines,
        "",
        "SOP-Grounded Findings:",
        f"- {sop_summary}",
        "",
        "Immediate Containment Actions:",
        *action_lines,
        "",
        "Evidence and Documentation Required:",
        "- Obtain customer confirmation and dispute narration for the reviewed transactions.",
        "- Capture account statement trail, beneficiary/VPA details, and relevant channel/session evidence as supported by the SOP.",
        "- Preserve screening notes, containment actions, and recovery/escalation checkpoints in the case record.",
        "",
        "Escalation and Reporting:",
        "- Record the case in the appropriate AXIS fraud workflow and continue escalation in line with the retrieved SOP guidance.",
        "- Document all containment, customer-contact, and beneficiary-review actions before closure or handoff.",
        "",
        "Source References:",
        *source_lines,
    ]
    return "\n".join(sections).strip()


def _polish_generated_report(report_text, reference_files, service_note=""):
    cleaned_report = _strip_code_fences(report_text)
    cleaned_report = re.sub(r"\n{3,}", "\n\n", cleaned_report).strip()

    if not cleaned_report:
        return ""

    if not cleaned_report.startswith("INVESTIGATION REPORT"):
        cleaned_report = f"INVESTIGATION REPORT\n{cleaned_report}"

    if reference_files and "Source References:" not in cleaned_report:
        source_lines = "\n".join(f"- {item}" for item in reference_files)
        cleaned_report = f"{cleaned_report}\n\nSource References:\n{source_lines}"

    if service_note and "Report Preparation Note:" not in cleaned_report:
        cleaned_report = f"{cleaned_report}\n\nReport Preparation Note:\n- {service_note}"

    return cleaned_report.strip()


def generate_investigation_report(query, bank_id, analysis):
    if not isinstance(analysis, dict) or not analysis.get("supported"):
        reason = ""
        if isinstance(analysis, dict):
            reason = _normalize_text(analysis.get("reason"))
        return reason or "A grounded investigation report could not be generated for this case."

    try:
        context, reference_files, ranked_chunks = retrieve_context(query, bank_id)
    except GeminiServiceError as exc:
        return _fallback_report(query, analysis, service_note=_friendly_service_note(exc, stage="report"))

    if not context.strip() or not ranked_chunks:
        return _fallback_report(query, analysis)

    report_prompt = _build_report_prompt(query, ranked_chunks, analysis)

    try:
        report = generate_text(report_prompt, temperature=0.15)
    except GeminiServiceError as exc:
        return _fallback_report(query, analysis, service_note=_friendly_service_note(exc, stage="report"))

    cleaned_report = _polish_generated_report(report, reference_files)

    if not cleaned_report:
        return _fallback_report(query, analysis)

    return cleaned_report
