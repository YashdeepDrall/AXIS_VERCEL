from app.core.config import RAG_MIN_SIMILARITY, RAG_TOP_K
from app.ml.embeddings import generate_embedding
from app.ml.vector_store import search_vector


def retrieve_context(query, bank_id, top_k=RAG_TOP_K):
    query_embedding = generate_embedding(query, task_type="RETRIEVAL_QUERY")
    results = search_vector(query_embedding, bank_id, top_k=top_k)

    if not results:
        return "", [], []

    filtered_results = [
        (similarity, doc)
        for similarity, doc in results
        if isinstance(similarity, (int, float)) and similarity >= RAG_MIN_SIMILARITY
    ]

    if not filtered_results:
        best_similarity, best_doc = results[0]
        if isinstance(best_similarity, (int, float)) and best_similarity >= max(RAG_MIN_SIMILARITY * 0.75, 0.05):
            filtered_results = [(best_similarity, best_doc)]

    context_blocks = []
    reference_files = []
    ranked_chunks = []

    for similarity, doc in filtered_results:
        text = doc.get("text", "")
        source_file = doc.get("sourceFile") or doc.get("fileName", "")

        if text:
            context_blocks.append(text)

        if source_file and source_file not in reference_files:
            reference_files.append(source_file)

        ranked_chunks.append(
            {
                "file_name": source_file,
                "score": similarity,
                "text": text,
            }
        )

    context = "\n\n".join(context_blocks)
    return context, reference_files, ranked_chunks
