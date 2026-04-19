import json
import math
import re
import time
from copy import deepcopy

import requests

from app.core.config import (
    GEMINI_API_BASE,
    GEMINI_API_KEY,
    GEMINI_EMBEDDING_DIMENSION,
    GEMINI_EMBEDDING_FALLBACK_MODELS,
    GEMINI_EMBEDDING_MODEL,
    GEMINI_GENERATION_FALLBACK_MODELS,
    GEMINI_GENERATION_MODEL,
    GEMINI_REQUEST_TIMEOUT_SECONDS,
)


class GeminiServiceError(RuntimeError):
    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.status_code = status_code


_resolved_models = {}
TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}
TRANSIENT_RETRY_DELAYS_SECONDS = [1.2, 2.4]


def is_gemini_configured():
    return bool(GEMINI_API_KEY)


def _build_model_url(model_name, action):
    return f"{GEMINI_API_BASE}/models/{model_name}:{action}"


def _post_to_gemini(model_name, action, payload):
    if not GEMINI_API_KEY:
        raise GeminiServiceError(
            "GEMINI_API_KEY is not set. Add it to your environment or .env before using Gemini RAG."
        )

    last_error = None

    for attempt_index in range(len(TRANSIENT_RETRY_DELAYS_SECONDS) + 1):
        try:
            response = requests.post(
                _build_model_url(model_name, action),
                headers={
                    "Content-Type": "application/json",
                    "x-goog-api-key": GEMINI_API_KEY,
                },
                json=payload,
                timeout=GEMINI_REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            last_error = GeminiServiceError(f"Gemini API request failed before a response was received: {exc}")
            if attempt_index < len(TRANSIENT_RETRY_DELAYS_SECONDS):
                time.sleep(TRANSIENT_RETRY_DELAYS_SECONDS[attempt_index])
                continue
            raise last_error from exc

        if response.ok:
            return response.json()

        try:
            error_payload = response.json()
            error_message = error_payload.get("error", {}).get("message") or response.text
        except Exception:
            error_message = response.text

        last_error = GeminiServiceError(
            f"Gemini API request failed ({response.status_code}): {error_message.strip()}",
            status_code=response.status_code,
        )

        if response.status_code in TRANSIENT_STATUS_CODES and attempt_index < len(TRANSIENT_RETRY_DELAYS_SECONDS):
            time.sleep(TRANSIENT_RETRY_DELAYS_SECONDS[attempt_index])
            continue

        raise last_error

    if last_error:
        raise last_error

    raise GeminiServiceError("Gemini request failed unexpectedly.")


def _dedupe_models(primary_model, fallback_models):
    candidates = []

    for model_name in [primary_model, *fallback_models]:
        normalized = (model_name or "").strip()
        if normalized and normalized not in candidates:
            candidates.append(normalized)

    return candidates


def _should_try_next_model(error):
    message = str(error).lower()

    if error.status_code not in {400, 404}:
        return False

    return any(
        marker in message
        for marker in [
            "not found",
            "not supported",
            "unsupported",
            "unknown model",
            "is not found",
        ]
    )


def _request_with_model_fallback(cache_key, action, payload, primary_model, fallback_models):
    if cache_key in _resolved_models:
        resolved_model = _resolved_models[cache_key]
        resolved_payload = deepcopy(payload)
        if "model" in resolved_payload:
            resolved_payload["model"] = f"models/{resolved_model}"
        return _post_to_gemini(resolved_model, action, resolved_payload), resolved_model

    last_error = None

    for model_name in _dedupe_models(primary_model, fallback_models):
        try:
            candidate_payload = deepcopy(payload)
            if "model" in candidate_payload:
                candidate_payload["model"] = f"models/{model_name}"
            response = _post_to_gemini(model_name, action, candidate_payload)
            _resolved_models[cache_key] = model_name
            return response, model_name
        except GeminiServiceError as exc:
            last_error = exc
            if not _should_try_next_model(exc):
                raise

    if last_error:
        raise last_error

    raise GeminiServiceError("No Gemini model candidates were available for this request.")


def _normalize_embedding(values):
    norm = math.sqrt(sum(value * value for value in values))

    if not norm:
        return values

    return [value / norm for value in values]


def embed_text(text, task_type="RETRIEVAL_QUERY"):
    payload = {
        "model": f"models/{GEMINI_EMBEDDING_MODEL}",
        "content": {
            "parts": [
                {
                    "text": text,
                }
            ]
        },
        "taskType": task_type,
        "output_dimensionality": GEMINI_EMBEDDING_DIMENSION,
    }

    response, resolved_model = _request_with_model_fallback(
        "embedding",
        "embedContent",
        payload,
        GEMINI_EMBEDDING_MODEL,
        GEMINI_EMBEDDING_FALLBACK_MODELS,
    )

    embedding = response.get("embedding", {})
    values = embedding.get("values")

    if not values:
        embeddings = response.get("embeddings") or []
        if embeddings:
            values = embeddings[0].get("values")

    if not values:
        raise GeminiServiceError("Gemini embedding response did not include embedding values.")

    return _normalize_embedding([float(value) for value in values])


def generate_text(prompt, temperature=0.2):
    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {
                        "text": prompt,
                    }
                ],
            }
        ],
        "generationConfig": {
            "temperature": temperature,
        },
    }

    response, resolved_model = _request_with_model_fallback(
        "generation",
        "generateContent",
        payload,
        GEMINI_GENERATION_MODEL,
        GEMINI_GENERATION_FALLBACK_MODELS,
    )

    text_parts = []

    for candidate in response.get("candidates", []):
        content = candidate.get("content", {})
        for part in content.get("parts", []):
            if isinstance(part, dict) and part.get("text"):
                text_parts.append(part["text"])

    output_text = "".join(text_parts).strip()

    if not output_text:
        raise GeminiServiceError("Gemini generation response did not include text output.")

    return output_text


def get_effective_model_name(model_type):
    if model_type == "embedding":
        return _resolved_models.get("embedding") or GEMINI_EMBEDDING_MODEL

    if model_type == "generation":
        return _resolved_models.get("generation") or GEMINI_GENERATION_MODEL

    return ""


def parse_json_response(text):
    cleaned = text.strip()
    cleaned = re.sub(r"^```json\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^```\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        match = re.search(r"\{[\s\S]*\}", cleaned)
        if not match:
            raise ValueError("Model response did not contain a JSON object.")
        return json.loads(match.group(0))
