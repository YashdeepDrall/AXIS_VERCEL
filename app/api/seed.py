from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field

from app.db.banking import get_banking_snapshot
from seed.seed_runner import run_seed


router = APIRouter(prefix="/seed", tags=["Seed Data"])


class SeedRunResponse(BaseModel):
    status: Literal["success"] = Field(
        description="Seed execution result.",
        examples=["success"],
    )
    mongo_host: str = Field(
        description="MongoDB cluster host used for the seed run.",
        examples=["cluster0.jbhyihm.mongodb.net"],
    )
    database_name: str = Field(
        description="MongoDB database where the seed data was written.",
        examples=["axis_fraud_chatbot"],
    )
    collections: list[str] = Field(
        description="Collections refreshed during the seed run.",
        examples=[["customers", "transactions", "loan_accounts", "collateral_records", "document_verifications", "case_events"]],
    )
    customers_inserted: int = Field(
        description="Number of customer records inserted into MongoDB.",
        examples=[80],
    )
    transactions_inserted: int = Field(
        description="Number of transaction records inserted into MongoDB.",
        examples=[721],
    )
    loan_accounts_inserted: int = Field(
        description="Number of loan-account records inserted into MongoDB.",
        examples=[12],
    )
    collateral_records_inserted: int = Field(
        description="Number of collateral-review records inserted into MongoDB.",
        examples=[6],
    )
    document_verifications_inserted: int = Field(
        description="Number of document-verification records inserted into MongoDB.",
        examples=[24],
    )
    case_events_inserted: int = Field(
        description="Number of case-event records inserted into MongoDB.",
        examples=[10],
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "success",
                "mongo_host": "cluster0.jbhyihm.mongodb.net",
                "database_name": "axis_fraud_chatbot",
                "collections": ["customers", "transactions", "loan_accounts", "collateral_records", "document_verifications", "case_events"],
                "customers_inserted": 80,
                "transactions_inserted": 721,
                "loan_accounts_inserted": 12,
                "collateral_records_inserted": 6,
                "document_verifications_inserted": 24,
                "case_events_inserted": 10,
            }
        }
    )


@router.post(
    "/run",
    response_model=SeedRunResponse,
    status_code=status.HTTP_200_OK,
    summary="Seed demo banking data",
    description="Seed database with demo banking data",
    responses={
        200: {
            "description": "Database seeded successfully.",
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "mongo_host": "cluster0.jbhyihm.mongodb.net",
                        "database_name": "axis_fraud_chatbot",
                        "collections": ["customers", "transactions", "loan_accounts", "collateral_records", "document_verifications", "case_events"],
                        "customers_inserted": 80,
                        "transactions_inserted": 721,
                        "loan_accounts_inserted": 12,
                        "collateral_records_inserted": 6,
                        "document_verifications_inserted": 24,
                        "case_events_inserted": 10,
                    }
                }
            },
        }
    },
)
def run_seed_endpoint() -> SeedRunResponse:
    try:
        result = run_seed()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Seed run failed: {exc}") from exc

    return SeedRunResponse(
        status="success",
        mongo_host=result["mongo_host"],
        database_name=result["database_name"],
        collections=result["collections"],
        customers_inserted=result["customers_inserted"],
        transactions_inserted=result["transactions_inserted"],
        loan_accounts_inserted=result["loan_accounts_inserted"],
        collateral_records_inserted=result["collateral_records_inserted"],
        document_verifications_inserted=result["document_verifications_inserted"],
        case_events_inserted=result["case_events_inserted"],
    )


@router.get(
    "/status",
    response_model=SeedRunResponse,
    status_code=status.HTTP_200_OK,
    summary="Check current seed data status",
    description="Show the current MongoDB demo banking collections and their record counts",
)
def get_seed_status() -> SeedRunResponse:
    try:
        snapshot = get_banking_snapshot()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Seed status check failed: {exc}") from exc

    return SeedRunResponse(
        status="success",
        mongo_host=str(snapshot["mongo_host"]),
        database_name=str(snapshot["database_name"]),
        collections=list(snapshot["collections"]),
        customers_inserted=int(snapshot["customers_count"]),
        transactions_inserted=int(snapshot["transactions_count"]),
        loan_accounts_inserted=int(snapshot["loan_accounts_count"]),
        collateral_records_inserted=int(snapshot["collateral_records_count"]),
        document_verifications_inserted=int(snapshot["document_verifications_count"]),
        case_events_inserted=int(snapshot["case_events_count"]),
    )
