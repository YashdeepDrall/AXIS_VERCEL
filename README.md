# AXIS Bank Chatbot

This project is scoped for AXIS Bank fraud investigation workflows and runs as a single FastAPI service with a browser-based chat UI.

- Backend: FastAPI APIs for login, fraud analysis, SOP retrieval, and document download
- Frontend: same-origin web UI served from the FastAPI app root
- Knowledge base: `banks/Axis/Axis_Bank_Fraud_SOP_Blueprint.pdf`
- Application state: existing chat/workspace flows use local runtime and local cache files
- Demo seed database: dedicated MongoDB collections in the separate `axis_fraud_chatbot` database
- RAG: Gemini embeddings plus Gemini answer generation through API calls
- Vector storage: document chunk embeddings are cached locally on disk and reused for unchanged PDFs
- Conversation history: persisted locally in `local_cache/axis_conversations.json` with role-based visibility

## Environment

Set these values in `.env` or your deployment environment:

- `MONGO_URL=your_mongodb_atlas_uri`
- `MONGO_DB_NAME=axis_fraud_chatbot`
- `GEMINI_API_KEY=your_api_key`
- `GEMINI_GENERATION_MODEL=gemini-3.1-flash-lite-preview`
- `GEMINI_EMBEDDING_MODEL=gemini-embedding-2-preview`

Optional tuning:

- `GEMINI_EMBEDDING_DIMENSION=768`
- `RAG_TOP_K=4`
- `RAG_MIN_SIMILARITY=0.2`

The app writes its reusable local vector cache to `local_cache/axis_vectors.json`.

## Local Run

1. Install packages from `requirements.txt`
2. Set `GEMINI_API_KEY`
3. Start the FastAPI app:

```bash
uvicorn app.main:app --reload
```

4. Open `http://127.0.0.1:8000`

Useful demo endpoints:

- `GET /seed/status` to confirm the live Mongo host, database name, collections, and record counts
- `POST /seed/run` to rebuild the `customers` and `transactions` demo data in the separate `axis_fraud_chatbot` database
- `POST /customer-fraud/chat` to collect customer/date inputs, fetch Mongo transactions, and return structured fraud analysis

Default local login:

- Investigator: `axis001 / 0000`
- Supervisor: `axis_supervisor / 0000`
- Admin: `axis_admin / 0000`
- Auditor: `axis_auditor / 0000`

## Vercel Deploy

This repo is prepared for a single Vercel project and a single public URL.

How it works on Vercel:

- `index.py` exposes the FastAPI app as the Vercel Python entrypoint
- `app/ui/index.html` is served from the FastAPI root so the chatbot opens directly on the same domain
- the browser keeps the conversation step state and sends it back with each request, which avoids relying on server memory across Vercel function invocations
- `scripts/precompute_vectors.py` runs during the Vercel build so the SOP vector cache is ready in the deployed bundle

Files used for Vercel:

- `index.py`
- `vercel.json`
- `scripts/precompute_vectors.py`
- `app/ui/index.html`

How to deploy on Vercel:

1. Push this repo to GitHub.
2. Import the repo into Vercel as one project.
3. Add the environment variable `GEMINI_API_KEY`.
4. Deploy.
5. Open the generated Vercel URL and log in with:
   - User ID: `axis001`
   - Password: `0000`

## Notes

- This repo is using the Vercel single-service path only.
