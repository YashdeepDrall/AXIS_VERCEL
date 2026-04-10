from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse

from app.api import fraud
from app.ml.vector_store import load_sbi_documents, rebuild_vector_index


app = FastAPI(title="SBI Fraud Investigation Assistant")
UI_FILE = Path(__file__).resolve().parent / "ui" / "index.html"


@app.on_event("startup")
def startup_event():
    print("Starting up system...")

    try:
        rebuild_vector_index()
        load_sbi_documents()
        print("System ready. SBI SOP metadata loaded and Gemini RAG index initialized when available.")
    except Exception as exc:
        print(f"Startup warning: {exc}")


app.include_router(fraud.router)


@app.get("/")
def home():
    if UI_FILE.exists():
        return FileResponse(UI_FILE)
    return {"message": "SBI Fraud Investigation Assistant running"}


@app.get("/health")
def health():
    return {"message": "SBI Fraud Investigation Assistant running"}
