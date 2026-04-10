import hashlib
import json
import os
import re
from copy import deepcopy

from pypdf import PdfReader

from app.core.config import (
    GEMINI_EMBEDDING_DIMENSION,
    GEMINI_EMBEDDING_MODEL,
    RAG_CHUNK_OVERLAP,
    RAG_CHUNK_SIZE,
    LOCAL_VECTOR_CACHE_FILE,
    SBI_BANK_DIR,
    SBI_BANK_ID,
)
from app.db.mongodb import documents_collection, fs
from app.ml.embeddings import generate_embedding as build_embedding
from app.services.llm_service import get_effective_model_name, is_gemini_configured


vector_store = []
VECTOR_CACHE_VERSION = 1


def generate_embedding(text, task_type="RETRIEVAL_QUERY"):
    return build_embedding(text, task_type=task_type)


def extract_text_from_pdf(file_path):
    reader = PdfReader(file_path)
    text = ""

    for page in reader.pages:
        extracted = page.extract_text()
        if extracted:
            text += extracted + "\n"

    return text


def split_by_category(text):
    pattern = r"([A-Z]{1,3}-\d{2}[\s\S]*?)(?=[A-Z]{1,3}-\d{2}|$)"
    matches = re.findall(pattern, text)
    return [match.strip() for match in matches if match.strip()]


def split_text(text, chunk_size=RAG_CHUNK_SIZE, overlap=RAG_CHUNK_OVERLAP):
    normalized = re.sub(r"\s+", " ", text or "").strip()

    if not normalized:
        return []

    chunks = []
    start = 0
    text_length = len(normalized)

    while start < text_length:
        end = min(text_length, start + chunk_size)

        if end < text_length:
            sentence_break = normalized.rfind(". ", start + chunk_size // 2, end)
            if sentence_break > start:
                end = sentence_break + 1

        chunk = normalized[start:end].strip()
        if chunk:
            chunks.append(chunk)

        if end >= text_length:
            break

        start = max(end - overlap, start + 1)

    return chunks


def build_document_chunks(text):
    category_sections = split_by_category(text) or [text]
    chunks = []

    for section in category_sections:
        chunks.extend(split_text(section))

    return [chunk for chunk in chunks if chunk.strip()]


def _stable_file_id(bank_id, file_name):
    digest = hashlib.sha256(f"{bank_id}:{file_name}".encode("utf-8")).hexdigest()
    return digest[:32]


def store_pdf(file_path, bank_id=SBI_BANK_ID):
    file_name = os.path.basename(file_path)
    file_id = _stable_file_id(bank_id, file_name)

    existing = documents_collection.find_one(
        {
            "bankId": bank_id,
            "fileName": file_name,
            "isPDF": True,
        }
    )

    if existing and existing.get("fileId"):
        return existing["fileId"]

    with open(file_path, "rb") as file_obj:
        file_id = str(fs.put(file_obj, filename=file_name, bankId=bank_id, file_id=file_id))

    documents_collection.update_one(
        {
            "bankId": bank_id,
            "fileName": file_name,
            "isPDF": True,
        },
        {
            "$set": {
                "bankId": bank_id,
                "fileName": file_name,
                "fileId": file_id,
                "filePath": file_path,
                "isPDF": True,
                "documentType": "SOP",
            }
        },
        upsert=True,
    )

    return file_id


def add_vector(embedding, text, bank_id, file_name, file_id, source_file=None):
    exists = documents_collection.find_one(
        {
            "bankId": bank_id,
            "fileName": file_name,
            "text": text,
        }
    )

    if exists:
        return

    doc = {
        "bankId": bank_id,
        "fileName": file_name,
        "text": text,
        "embedding": embedding,
        "fileId": file_id,
        "sourceFile": source_file or file_name,
    }

    vector_store.append(doc)
    documents_collection.insert_one(doc)


def _load_vector_cache():
    if not os.path.exists(LOCAL_VECTOR_CACHE_FILE):
        return {"version": VECTOR_CACHE_VERSION, "documents": {}}

    try:
        with open(LOCAL_VECTOR_CACHE_FILE, "r", encoding="utf-8") as cache_file:
            cache = json.load(cache_file)
    except Exception:
        return {"version": VECTOR_CACHE_VERSION, "documents": {}}

    if not isinstance(cache, dict):
        return {"version": VECTOR_CACHE_VERSION, "documents": {}}

    cache.setdefault("version", VECTOR_CACHE_VERSION)
    cache.setdefault("documents", {})
    return cache


def _save_vector_cache(cache):
    cache_dir = os.path.dirname(LOCAL_VECTOR_CACHE_FILE)
    if cache_dir:
        os.makedirs(cache_dir, exist_ok=True)

    with open(LOCAL_VECTOR_CACHE_FILE, "w", encoding="utf-8") as cache_file:
        json.dump(cache, cache_file, ensure_ascii=True, indent=2)


def _document_fingerprint(file_path):
    stat = os.stat(file_path)
    return {
        "size": int(stat.st_size),
        "mtime_ns": int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))),
    }


def _cache_settings():
    return {
        "embedding_model_requested": GEMINI_EMBEDDING_MODEL,
        "embedding_dimension": GEMINI_EMBEDDING_DIMENSION,
        "chunk_size": RAG_CHUNK_SIZE,
        "chunk_overlap": RAG_CHUNK_OVERLAP,
    }


def _get_cached_document(cache, file_name, fingerprint):
    cache_entry = (cache.get("documents") or {}).get(file_name)

    if not cache_entry:
        return None

    if cache_entry.get("fingerprint") != fingerprint:
        return None

    if cache_entry.get("settings") != _cache_settings():
        return None

    return cache_entry


def _load_cached_chunks(cache_entry, bank_id, file_id):
    for chunk in cache_entry.get("chunks", []):
        add_vector(
            chunk.get("embedding", []),
            chunk.get("text", ""),
            bank_id,
            chunk.get("fileName", ""),
            file_id,
            source_file=chunk.get("sourceFile"),
        )


def _build_cache_entry(file_path, resolved_embedding_model, chunks):
    return {
        "fingerprint": _document_fingerprint(file_path),
        "settings": _cache_settings(),
        "embedding_model_used": resolved_embedding_model,
        "chunks": chunks,
    }


def rebuild_vector_index():
    global vector_store

    vector_store = list(
        documents_collection.find(
            {
                "bankId": SBI_BANK_ID,
                "embedding": {"$exists": True},
            }
        )
    )


def _register_sbi_pdfs():
    if not os.path.exists(SBI_BANK_DIR):
        return []

    registered_files = []

    for file_name in os.listdir(SBI_BANK_DIR):
        if not file_name.lower().endswith(".pdf"):
            continue

        file_path = os.path.join(SBI_BANK_DIR, file_name)
        file_id = store_pdf(file_path, SBI_BANK_ID)
        registered_files.append((file_name, file_path, file_id))

    return registered_files


def load_sbi_documents(force_rebuild=False):
    registered_files = _register_sbi_pdfs()

    if not registered_files:
        return

    if vector_store and not force_rebuild:
        return

    cache = _load_vector_cache()
    cached_documents = deepcopy(cache.get("documents", {}))
    updated_cache = {"version": VECTOR_CACHE_VERSION, "documents": {}}

    if not is_gemini_configured():
        print("GEMINI_API_KEY not set. Skipping Gemini embedding index build.")
        return

    documents_collection.delete_many(
        {
            "bankId": SBI_BANK_ID,
            "embedding": {"$exists": True},
        }
    )
    rebuild_vector_index()

    if force_rebuild:
        cached_documents = {}

    for file_name, file_path, file_id in registered_files:
        fingerprint = _document_fingerprint(file_path)
        cache_entry = _get_cached_document({"documents": cached_documents}, file_name, fingerprint)

        if cache_entry:
            _load_cached_chunks(cache_entry, SBI_BANK_ID, file_id)
            updated_cache["documents"][file_name] = cache_entry
            continue

        text = extract_text_from_pdf(file_path)

        if not text.strip():
            continue

        chunks = build_document_chunks(text)
        cached_chunks = []

        for index_number, chunk in enumerate(chunks, start=1):
            chunk_file_name = f"{file_name}_chunk{index_number}"
            embedding = generate_embedding(chunk, task_type="RETRIEVAL_DOCUMENT")

            add_vector(
                embedding,
                chunk,
                SBI_BANK_ID,
                chunk_file_name,
                file_id,
                source_file=file_name,
            )
            cached_chunks.append(
                {
                    "fileName": chunk_file_name,
                    "text": chunk,
                    "embedding": embedding,
                    "sourceFile": file_name,
                }
            )

        updated_cache["documents"][file_name] = _build_cache_entry(
            file_path,
            get_effective_model_name("embedding"),
            cached_chunks,
        )

    if updated_cache != cache:
        _save_vector_cache(updated_cache)
    rebuild_vector_index()


def _cosine_similarity(query_embedding, doc_embedding):
    if not query_embedding or not doc_embedding:
        return 0.0

    return sum(query_value * doc_value for query_value, doc_value in zip(query_embedding, doc_embedding))


def search_vector(query_embedding, bank_id, top_k=3):
    if not vector_store:
        load_sbi_documents()

    results = []

    for doc in vector_store:
        if doc.get("bankId") != bank_id:
            continue

        similarity = _cosine_similarity(query_embedding, doc.get("embedding", []))
        results.append((similarity, doc))

    results.sort(key=lambda item: item[0], reverse=True)
    return results[:top_k]
