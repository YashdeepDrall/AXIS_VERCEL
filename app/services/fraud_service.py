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


def _build_prompt(query, ranked_chunks):
    formatted_chunks = _format_ranked_chunks(ranked_chunks)
    joined_context = "\n\n".join(formatted_chunks)

    return f"""
You are an SBI fraud investigation assistant.
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

    return f"""
You are preparing an SBI fraud investigation report.
Use only the retrieved SOP context and grounded analysis below. Do not invent facts.

Write a concise but operationally useful report in plain text using exactly these section headings:
INVESTIGATION REPORT
Case Query:
SOP Classification:
Risk Assessment:
Observed Indicators:
SOP-Grounded Findings:
Immediate Investigation Actions:
Required Documentation and Evidence:
Escalation and Reporting:
Source References:

Requirements:
- Keep every point grounded in the retrieved SOP context.
- Make the actions specific and investigator-friendly.
- Mention IFMS, ALP, logs, device or document review only if supported by the context.
- If something is uncertain, say "Based on retrieved SOP context" instead of assuming.
- Under Source References, list the retrieved source file names.

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
""".strip()


def _fallback_analysis(raw_response, reference_files, context):
    relevant_excerpt = _excerpt(context)

    return {
        "supported": True,
        "fraud_category": "Unknown",
        "fraud_classification": "Manual review required",
        "risk_level": "Medium",
        "suspicious_indicators": [],
        "relevant_information": relevant_excerpt,
        "recommended_action": "Review the retrieved SOP context manually and continue with investigator validation.",
        "sop_summary": _normalize_text(raw_response, relevant_excerpt),
        "reason": "",
        "references": reference_files,
    }


def detect_fraud(query, bank_id):
    try:
        context, reference_files, ranked_chunks = retrieve_context(query, bank_id)
    except GeminiServiceError as exc:
        return {
            "supported": False,
            "reason": str(exc),
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
        return {
            "supported": False,
            "reason": str(exc),
        }

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


def _fallback_report(query, analysis):
    references = analysis.get("references") or []
    indicators = analysis.get("suspicious_indicators") or []
    indicator_lines = "\n".join(f"- {item}" for item in indicators) if indicators else "- No specific indicators extracted"
    source_lines = "\n".join(f"- {item}" for item in references) if references else "- No source references available"

    return f"""
INVESTIGATION REPORT
Case Query:
{query}

SOP Classification:
- Fraud Category: {analysis.get('fraud_category', 'Unknown')}
- Fraud Classification: {analysis.get('fraud_classification', 'Manual review required')}

Risk Assessment:
- Risk Level: {analysis.get('risk_level', 'Medium')}

Observed Indicators:
{indicator_lines}

SOP-Grounded Findings:
- {analysis.get('relevant_information', 'Review retrieved SOP context manually.')}

Immediate Investigation Actions:
- {analysis.get('recommended_action', 'Continue investigator review based on the SOP context.')}

Required Documentation and Evidence:
- Based on retrieved SOP context, collect the documents and system evidence referenced in the matched SOP sections.

Escalation and Reporting:
- Record the case in the appropriate SBI fraud workflow and continue escalation based on the retrieved SOP guidance.

Source References:
{source_lines}
""".strip()


def generate_investigation_report(query, bank_id, analysis):
    if not isinstance(analysis, dict) or not analysis.get("supported"):
        reason = ""
        if isinstance(analysis, dict):
            reason = _normalize_text(analysis.get("reason"))
        return reason or "A grounded investigation report could not be generated for this case."

    try:
        context, reference_files, ranked_chunks = retrieve_context(query, bank_id)
    except GeminiServiceError as exc:
        return f"Report generation failed: {exc}"

    if not context.strip() or not ranked_chunks:
        return _fallback_report(query, analysis)

    report_prompt = _build_report_prompt(query, ranked_chunks, analysis)

    try:
        report = generate_text(report_prompt, temperature=0.15)
    except GeminiServiceError as exc:
        return f"{_fallback_report(query, analysis)}\n\nReport note: {exc}"

    cleaned_report = _normalize_text(report)

    if not cleaned_report:
        return _fallback_report(query, analysis)

    if reference_files and "Source References:" not in cleaned_report:
        source_lines = "\n".join(f"- {item}" for item in reference_files)
        cleaned_report = f"{cleaned_report}\n\nSource References:\n{source_lines}"

    return cleaned_report
