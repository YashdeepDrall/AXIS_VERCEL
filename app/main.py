from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse

from app.api import customer_fraud, fraud, seed
from app.db.banking import ensure_banking_indexes
from app.ml.vector_store import load_axis_documents, rebuild_vector_index


app = FastAPI(title="AXIS Bank Fraud Investigation Assistant")
UI_FILE = Path(__file__).resolve().parent / "ui" / "index.html"


@app.on_event("startup")
def startup_event():
    print("Starting up system...")

    try:
        rebuild_vector_index()
        load_axis_documents()
        print("System ready. AXIS fraud blueprint knowledge loaded and Gemini RAG index initialized when available.")
    except Exception as exc:
        print(f"Startup warning: {exc}")

    try:
        ensure_banking_indexes()
        print("MongoDB banking indexes ready.")
    except Exception as exc:
        print(f"Banking seed setup warning: {exc}")


app.include_router(fraud.router)
app.include_router(customer_fraud.router)
app.include_router(seed.router)


@app.get("/")
def home():
    if UI_FILE.exists():
        return FileResponse(UI_FILE)
    return {"message": "AXIS Bank Fraud Investigation Assistant running"}


@app.get("/health")
def health():
    return {"message": "AXIS Bank Fraud Investigation Assistant running"}
