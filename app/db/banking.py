from __future__ import annotations

from functools import lru_cache
from urllib.parse import urlparse

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from app.core.config import MONGO_DB_NAME, MONGO_URI


CUSTOMERS_COLLECTION_NAME = "customers"
TRANSACTIONS_COLLECTION_NAME = "transactions"


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


def ensure_banking_indexes(database: Database | None = None) -> None:
    customers_collection = get_customers_collection(database)
    transactions_collection = get_transactions_collection(database)

    customers_collection.create_index(
        [("cif_id", ASCENDING)],
        name="uq_customers_cif_id",
        unique=True,
    )
    customers_collection.create_index(
        [("mobile", ASCENDING)],
        name="idx_customers_mobile",
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


def get_banking_snapshot(database: Database | None = None) -> dict[str, object]:
    resolved_database = database if database is not None else get_banking_database()
    customers_collection = get_customers_collection(resolved_database)
    transactions_collection = get_transactions_collection(resolved_database)

    return {
        "mongo_host": get_mongo_host(),
        "database_name": resolved_database.name,
        "collections": [CUSTOMERS_COLLECTION_NAME, TRANSACTIONS_COLLECTION_NAME],
        "customers_count": customers_collection.count_documents({}),
        "transactions_count": transactions_collection.count_documents({}),
    }
