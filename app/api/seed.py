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
        examples=[["customers", "transactions"]],
    )
    customers_inserted: int = Field(
        description="Number of customer records inserted into MongoDB.",
        examples=[80],
    )
    transactions_inserted: int = Field(
        description="Number of transaction records inserted into MongoDB.",
        examples=[721],
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "success",
                "mongo_host": "cluster0.jbhyihm.mongodb.net",
                "database_name": "axis_fraud_chatbot",
                "collections": ["customers", "transactions"],
                "customers_inserted": 80,
                "transactions_inserted": 721,
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
                        "collections": ["customers", "transactions"],
                        "customers_inserted": 80,
                        "transactions_inserted": 721,
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
    )
