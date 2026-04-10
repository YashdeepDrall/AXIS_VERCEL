from app.services.llm_service import embed_text


def generate_embedding(text, task_type="RETRIEVAL_QUERY"):
    return embed_text(text, task_type=task_type)
