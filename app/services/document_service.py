import os
import re

from pypdf import PdfReader

from app.core.config import SBI_BANK_DIR, SBI_BANK_ID
from app.db.mongodb import documents_collection, fs
from app.ml.embeddings import generate_embedding
from app.ml.vector_store import add_vector


def extract_text_from_pdf(file_path):
    reader = PdfReader(file_path)
    text = ""

    for page in reader.pages:
        page_text = page.extract_text()
        if page_text:
            text += page_text + "\n"

    return text


def split_by_category(text):
    pattern = r"([A-Z]{1,3}-\d{2}[\s\S]*?)(?=[A-Z]{1,3}-\d{2}|$)"
    matches = re.findall(pattern, text)
    return [match.strip() for match in matches if match.strip()]


def split_text(text, chunk_size=500):
    chunks = []

    for start in range(0, len(text), chunk_size):
        chunks.append(text[start:start + chunk_size])

    return chunks


def store_pdf_in_db(file_path, bank_id=SBI_BANK_ID):
    file_name = os.path.basename(file_path)

    existing = documents_collection.find_one({
        "bankId": bank_id,
        "fileName": file_name
    })

    if existing and "fileId" in existing:
        return existing["fileId"]

    with open(file_path, "rb") as file_obj:
        file_id = fs.put(
            file_obj,
            filename=file_name,
            bankId=bank_id
        )

    documents_collection.update_one(
        {
            "bankId": bank_id,
            "fileName": file_name
        },
        {
            "$set": {
                "fileId": str(file_id),
                "filePath": file_path
            }
        },
        upsert=True
    )

    return str(file_id)


def process_document(file_path, bank_id=SBI_BANK_ID):
    text = extract_text_from_pdf(file_path)
    chunks = split_text(text)
    file_name = os.path.basename(file_path)
    file_id = store_pdf_in_db(file_path, bank_id)

    for chunk in chunks:
        embedding = generate_embedding(chunk)

        add_vector(
            embedding,
            chunk,
            bank_id,
            file_name,
            file_id,
            source_file=file_name
        )


def load_sbi_documents():
    if not os.path.isdir(SBI_BANK_DIR):
        print("SBI documents folder not found")
        return

    for file_name in os.listdir(SBI_BANK_DIR):
        if not file_name.lower().endswith(".pdf"):
            continue

        file_path = os.path.join(SBI_BANK_DIR, file_name)

        exists = documents_collection.find_one({
            "bankId": SBI_BANK_ID,
            "fileName": file_name
        })

        if not exists:
            documents_collection.insert_one({
                "bankId": SBI_BANK_ID,
                "documentType": "SOP",
                "fileName": file_name,
                "filePath": file_path
            })

        process_document(file_path, SBI_BANK_ID)

    print("SBI documents loaded and indexed successfully")
