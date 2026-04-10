from datetime import datetime
from app.db.mongodb import chat_logs_collection


def save_chat(user_id, bank_id, query, answer):

    chat_logs_collection.insert_one({
        "userId": user_id,
        "bankId": bank_id,
        "query": query,
        "answer": answer,
        "timestamp": datetime.utcnow()
    })