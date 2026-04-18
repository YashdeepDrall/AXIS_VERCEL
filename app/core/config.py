import os
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
ENV_FILE = ROOT_DIR / ".env"


def _load_env_file():
    if not ENV_FILE.exists():
        return

    for line in ENV_FILE.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if key and key not in os.environ:
            os.environ[key] = value


_load_env_file()

AXIS_BANK_ID = "axis"
AXIS_BANK_NAME = "AXIS Bank"
AXIS_BLUEPRINT_FILE = "Axis_Bank_Fraud_SOP_Blueprint.pdf"

BASE_DIR = str(ROOT_DIR)
BANKS_DIR = os.path.join(BASE_DIR, "banks")
AXIS_BANK_DIR = os.path.join(BANKS_DIR, "Axis")

GEMINI_API_BASE = os.getenv("GEMINI_API_BASE", "https://generativelanguage.googleapis.com/v1beta")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_GENERATION_MODEL = os.getenv("GEMINI_GENERATION_MODEL", "gemini-3.1-flash-lite-preview").strip()
GEMINI_GENERATION_FALLBACK_MODELS = [
    model.strip()
    for model in os.getenv(
        "GEMINI_GENERATION_FALLBACK_MODELS",
        "gemini-2.5-flash-lite-preview-09-2025,gemini-2.5-flash-lite,gemini-3-flash-preview",
    ).split(",")
    if model.strip()
]
GEMINI_EMBEDDING_MODEL = os.getenv("GEMINI_EMBEDDING_MODEL", "gemini-embedding-2-preview").strip()
GEMINI_EMBEDDING_FALLBACK_MODELS = [
    model.strip()
    for model in os.getenv(
        "GEMINI_EMBEDDING_FALLBACK_MODELS",
        "gemini-embedding-001",
    ).split(",")
    if model.strip()
]
GEMINI_EMBEDDING_DIMENSION = int(os.getenv("GEMINI_EMBEDDING_DIMENSION", "768"))
GEMINI_REQUEST_TIMEOUT_SECONDS = float(os.getenv("GEMINI_REQUEST_TIMEOUT_SECONDS", "45"))

RAG_TOP_K = int(os.getenv("RAG_TOP_K", "4"))
RAG_MIN_SIMILARITY = float(os.getenv("RAG_MIN_SIMILARITY", "0.2"))
RAG_CHUNK_SIZE = int(os.getenv("RAG_CHUNK_SIZE", "1400"))
RAG_CHUNK_OVERLAP = int(os.getenv("RAG_CHUNK_OVERLAP", "220"))

LOCAL_VECTOR_CACHE_DIR = os.getenv("LOCAL_VECTOR_CACHE_DIR", os.path.join(BASE_DIR, "local_cache"))
LOCAL_VECTOR_CACHE_FILE = os.getenv(
    "LOCAL_VECTOR_CACHE_FILE",
    os.path.join(LOCAL_VECTOR_CACHE_DIR, "axis_vectors.json"),
)
LOCAL_CONVERSATION_STORE_FILE = os.getenv(
    "LOCAL_CONVERSATION_STORE_FILE",
    os.path.join(LOCAL_VECTOR_CACHE_DIR, "axis_conversations.json"),
)
LOCAL_USER_STORE_FILE = os.getenv(
    "LOCAL_USER_STORE_FILE",
    os.path.join(LOCAL_VECTOR_CACHE_DIR, "axis_users.json"),
)
LOCAL_ACTIVITY_LOG_FILE = os.getenv(
    "LOCAL_ACTIVITY_LOG_FILE",
    os.path.join(LOCAL_VECTOR_CACHE_DIR, "axis_activity.json"),
)

MONGO_URI = os.getenv("MONGO_URI", os.getenv("MONGO_URL", "")).strip()
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "axis_fraud_chatbot").strip()
