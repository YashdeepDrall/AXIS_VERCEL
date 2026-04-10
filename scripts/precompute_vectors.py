import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.core.config import LOCAL_VECTOR_CACHE_FILE  # noqa: E402
from app.ml.vector_store import load_sbi_documents  # noqa: E402
from app.services.llm_service import is_gemini_configured  # noqa: E402


def main():
    if not is_gemini_configured():
        raise SystemExit(
            "GEMINI_API_KEY is not set. The Vercel build needs this so the SOP vector cache can be precomputed."
        )

    print("Precomputing SBI SOP vector cache...")
    load_sbi_documents(force_rebuild=True)
    print(f"Vector cache ready at: {LOCAL_VECTOR_CACHE_FILE}")


if __name__ == "__main__":
    main()
