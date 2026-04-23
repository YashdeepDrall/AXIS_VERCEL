from __future__ import annotations

from functools import lru_cache
from urllib.parse import urlparse

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from app.core.config import MONGO_DB_NAME, MONGO_URI


CUSTOMERS_COLLECTION_NAME = "customers"
TRANSACTIONS_COLLECTION_NAME = "transactions"
LOAN_ACCOUNTS_COLLECTION_NAME = "loan_accounts"
COLLATERAL_RECORDS_COLLECTION_NAME = "collateral_records"
DOCUMENT_VERIFICATIONS_COLLECTION_NAME = "document_verifications"
CASE_EVENTS_COLLECTION_NAME = "case_events"


def get_mongo_host() -> str:
    if not MONGO_URI:
        return ""

    parsed = urlparse(MONGO_URI)
    return parsed.hostname or ""


@lru_cache(maxsize=1)
def get_mongo_client() -> MongoClient:
    if not MONGO_URI:
        raise RuntimeError("MongoDB URI is not configured. Set MONGO_URI or MONGO_URL in your environment.")

    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)


def get_banking_database() -> Database:
    return get_mongo_client()[MONGO_DB_NAME]


def get_customers_collection(database: Database | None = None) -> Collection:
    resolved_database = database if database is not None else get_banking_database()
    return resolved_database[CUSTOMERS_COLLECTION_NAME]


def get_transactions_collection(database: Database | None = None) -> Collection:
    resolved_database = database if database is not None else get_banking_database()
    return resolved_database[TRANSACTIONS_COLLECTION_NAME]


def get_loan_accounts_collection(database: Database | None = None) -> Collection:
    resolved_database = database if database is not None else get_banking_database()
    return resolved_database[LOAN_ACCOUNTS_COLLECTION_NAME]


def get_collateral_records_collection(database: Database | None = None) -> Collection:
    resolved_database = database if database is not None else get_banking_database()
    return resolved_database[COLLATERAL_RECORDS_COLLECTION_NAME]


def get_document_verifications_collection(database: Database | None = None) -> Collection:
    resolved_database = database if database is not None else get_banking_database()
    return resolved_database[DOCUMENT_VERIFICATIONS_COLLECTION_NAME]


def get_case_events_collection(database: Database | None = None) -> Collection:
    resolved_database = database if database is not None else get_banking_database()
    return resolved_database[CASE_EVENTS_COLLECTION_NAME]


def ensure_banking_indexes(database: Database | None = None) -> None:
    customers_collection = get_customers_collection(database)
    transactions_collection = get_transactions_collection(database)
    loan_accounts_collection = get_loan_accounts_collection(database)
    collateral_records_collection = get_collateral_records_collection(database)
    document_verifications_collection = get_document_verifications_collection(database)
    case_events_collection = get_case_events_collection(database)

    customers_collection.create_index(
        [("cif_id", ASCENDING)],
        name="uq_customers_cif_id",
        unique=True,
    )
    customers_collection.create_index(
        [("mobile", ASCENDING)],
        name="idx_customers_mobile",
    )
    customers_collection.create_index(
        [("pan", ASCENDING)],
        name="uq_customers_pan",
        unique=True,
        sparse=True,
    )
    customers_collection.create_index(
        [("accounts", ASCENDING)],
        name="idx_customers_accounts",
    )

    transactions_collection.create_index(
        [("txn_id", ASCENDING)],
        name="uq_transactions_txn_id",
        unique=True,
    )
    transactions_collection.create_index(
        [("cif_id", ASCENDING)],
        name="idx_transactions_cif_id",
    )
    transactions_collection.create_index(
        [("account_id", ASCENDING), ("timestamp", DESCENDING)],
        name="idx_transactions_account_timestamp",
    )
    transactions_collection.create_index(
        [("timestamp", DESCENDING)],
        name="idx_transactions_timestamp",
    )

    loan_accounts_collection.create_index(
        [("loan_id", ASCENDING)],
        name="uq_loan_accounts_loan_id",
        unique=True,
    )
    loan_accounts_collection.create_index(
        [("loan_account_number", ASCENDING)],
        name="uq_loan_accounts_number",
        unique=True,
    )
    loan_accounts_collection.create_index(
        [("cif_id", ASCENDING), ("loan_status", ASCENDING)],
        name="idx_loan_accounts_cif_status",
    )
    loan_accounts_collection.create_index(
        [("collateral_id", ASCENDING)],
        name="idx_loan_accounts_collateral_id",
        sparse=True,
    )

    collateral_records_collection.create_index(
        [("collateral_id", ASCENDING)],
        name="uq_collateral_records_collateral_id",
        unique=True,
    )
    collateral_records_collection.create_index(
        [("loan_id", ASCENDING)],
        name="idx_collateral_records_loan_id",
    )
    collateral_records_collection.create_index(
        [("cif_id", ASCENDING), ("verification_status", ASCENDING)],
        name="idx_collateral_records_cif_verification",
    )

    document_verifications_collection.create_index(
        [("verification_id", ASCENDING)],
        name="uq_document_verifications_verification_id",
        unique=True,
    )
    document_verifications_collection.create_index(
        [("cif_id", ASCENDING), ("case_family", ASCENDING), ("submitted_at", DESCENDING)],
        name="idx_document_verifications_cif_family_submitted",
    )
    document_verifications_collection.create_index(
        [("loan_id", ASCENDING), ("document_type", ASCENDING)],
        name="idx_document_verifications_loan_document_type",
        sparse=True,
    )
    document_verifications_collection.create_index(
        [("collateral_id", ASCENDING)],
        name="idx_document_verifications_collateral_id",
        sparse=True,
    )

    case_events_collection.create_index(
        [("event_id", ASCENDING)],
        name="uq_case_events_event_id",
        unique=True,
    )
    case_events_collection.create_index(
        [("cif_id", ASCENDING), ("occurred_at", DESCENDING)],
        name="idx_case_events_cif_occurred",
    )
    case_events_collection.create_index(
        [("case_family", ASCENDING), ("occurred_at", DESCENDING)],
        name="idx_case_events_family_occurred",
    )
    case_events_collection.create_index(
        [("loan_id", ASCENDING)],
        name="idx_case_events_loan_id",
        sparse=True,
    )


def get_banking_snapshot(database: Database | None = None) -> dict[str, object]:
    resolved_database = database if database is not None else get_banking_database()
    customers_collection = get_customers_collection(resolved_database)
    transactions_collection = get_transactions_collection(resolved_database)
    loan_accounts_collection = get_loan_accounts_collection(resolved_database)
    collateral_records_collection = get_collateral_records_collection(resolved_database)
    document_verifications_collection = get_document_verifications_collection(resolved_database)
    case_events_collection = get_case_events_collection(resolved_database)

    return {
        "mongo_host": get_mongo_host(),
        "database_name": resolved_database.name,
        "collections": [
            CUSTOMERS_COLLECTION_NAME,
            TRANSACTIONS_COLLECTION_NAME,
            LOAN_ACCOUNTS_COLLECTION_NAME,
            COLLATERAL_RECORDS_COLLECTION_NAME,
            DOCUMENT_VERIFICATIONS_COLLECTION_NAME,
            CASE_EVENTS_COLLECTION_NAME,
        ],
        "customers_count": customers_collection.count_documents({}),
        "transactions_count": transactions_collection.count_documents({}),
        "loan_accounts_count": loan_accounts_collection.count_documents({}),
        "collateral_records_count": collateral_records_collection.count_documents({}),
        "document_verifications_count": document_verifications_collection.count_documents({}),
        "case_events_count": case_events_collection.count_documents({}),
    }
