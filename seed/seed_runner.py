from __future__ import annotations

import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from app.db.banking import (
    CASE_EVENTS_COLLECTION_NAME,
    COLLATERAL_RECORDS_COLLECTION_NAME,
    CUSTOMERS_COLLECTION_NAME,
    DOCUMENT_VERIFICATIONS_COLLECTION_NAME,
    LOAN_ACCOUNTS_COLLECTION_NAME,
    TRANSACTIONS_COLLECTION_NAME,
    ensure_banking_indexes,
    get_banking_database,
    get_banking_snapshot,
)


SEED_DIR = Path(__file__).resolve().parent
CUSTOMERS_FILE = SEED_DIR / "customers.json"
TRANSACTIONS_FILE = SEED_DIR / "transactions.json"
LOAN_ACCOUNTS_FILE = SEED_DIR / "loan_accounts.json"
COLLATERAL_RECORDS_FILE = SEED_DIR / "collateral_records.json"
DOCUMENT_VERIFICATIONS_FILE = SEED_DIR / "document_verifications.json"
CASE_EVENTS_FILE = SEED_DIR / "case_events.json"

TOTAL_CUSTOMERS = 80
MIN_TRANSACTIONS_PER_CUSTOMER = 7
MAX_TRANSACTIONS_PER_CUSTOMER = 12
LOOKBACK_DAYS = 10

SHOWCASE_CUSTOMER_NAMES = [
    "Rahul Sharma",
    "Aman Verma",
    "Neha Singh",
    "Rohit Gupta",
    "Priya Mehta",
    "Ankit Yadav",
    "Simran Kaur",
    "Karan Malhotra",
    "Meena Devi",
    "Suresh Kumar",
]

CUSTOMER_NAME_POOL = [
    "Aditi Nair",
    "Vivek Iyer",
    "Ritika Kapoor",
    "Arjun Menon",
    "Sneha Patil",
    "Harsh Vardhan",
    "Pooja Bansal",
    "Nitin Joshi",
    "Tanvi Deshmukh",
    "Abhishek Jain",
    "Kavya Reddy",
    "Manish Tiwari",
    "Isha Arora",
    "Sanjay Mishra",
    "Rhea Chatterjee",
    "Yuvraj Gill",
    "Divya Narang",
    "Mukesh Soni",
    "Nidhi Saxena",
    "Deepak Chauhan",
    "Ishita Das",
    "Mohit Bhatia",
    "Ananya Bose",
    "Rakesh Solanki",
    "Komal Thakur",
    "Siddharth Goyal",
    "Varsha Pillai",
    "Ajay Kulkarni",
    "Palak Sethi",
    "Hemant Rawat",
    "Bhavna Kohli",
    "Rohan Purohit",
    "Payal Anand",
    "Tarun Bedi",
    "Nikita Jaiswal",
    "Saurabh Dubey",
    "Shreya Dutta",
    "Pranav Shetty",
    "Madhuri Rao",
    "Akash Thakur",
    "Garima Luthra",
    "Vikas Pandey",
    "Mitali Sen",
    "Aditya Chopra",
    "Preeti Narayan",
    "Kunal Saxena",
    "Sakshi Batra",
    "Navin Pillai",
    "Rupali Jha",
    "Chirag Mahajan",
    "Ayesha Khan",
    "Devansh Bedi",
    "Monika Sood",
    "Yash Kulshreshtha",
    "Sonal Dhingra",
    "Parth Trivedi",
    "Tanmay Nagpal",
    "Bhavya Saran",
    "Jatin Kalra",
    "Rashi Kulkarni",
    "Naveen Rao",
    "Pallavi Sinha",
    "Anirudh Bhalla",
    "Sanya Khanna",
    "Gaurav Ahuja",
    "Niharika Anand",
    "Harleen Bedi",
    "Lakshay Arora",
    "Mansi Chawla",
    "Pratik Ghosh",
]

ATM_BENEFICIARIES = ["ATM-WDL", "ATM-CASH", "ATM-SVC"]
POS_BENEFICIARIES = [
    "Amazon",
    "Flipkart",
    "Swiggy",
    "Zomato",
    "DMart",
    "BigBasket",
    "Nykaa",
    "Myntra",
    "Reliance Smart",
    "Apollo Pharmacy",
]
UPI_CONTACTS = [
    "friend@upi",
    "kirana@upi",
    "rent@upi",
    "milkshop@upi",
    "taxi@upi",
    "cabdriver@upi",
    "grocery@upi",
    "maid@upi",
    "electricity@upi",
    "society@upi",
]
KNOWN_ACCOUNT_BENEFICIARIES = [
    "family_savings",
    "school_fees",
    "rd_account",
    "insurance_premium",
    "property_rent",
]
UNKNOWN_ACCOUNT_BENEFICIARIES = [
    "unknown_acc",
    "new_acc",
    "mule_account",
    "fraud@upi",
    "scam@upi",
    "scam2@upi",
    "flashloan@upi",
]
CREDIT_BENEFICIARIES = [
    "Salary Credit",
    "Refund Credit",
    "UPI Collect",
    "Interest Credit",
    "Cash Deposit",
]
CHANNELS = ["UPI", "ATM", "IMPS", "POS", "NETBANKING"]

LOAN_PRODUCTS = [
    "Home Loan",
    "Loan Against Property",
    "Personal Loan",
    "Business Loan",
    "Auto Loan",
]
COLLATERAL_REQUIRED_PRODUCTS = {"Home Loan", "Loan Against Property"}
BRANCH_NAMES = [
    "Axis Bank Indore Home Loan Centre",
    "Axis Bank Jaipur Retail Assets Hub",
    "Axis Bank Delhi South Credit Desk",
    "Axis Bank Pune Lending Operations",
    "Axis Bank Bengaluru Prime Branch",
    "Axis Bank Lucknow Recovery Cell",
]
PROPERTY_TYPES = [
    "Residential Flat",
    "Residential Plot",
    "Independent House",
    "Commercial Unit",
]
PROPERTY_AREAS = [
    "Vijay Nagar, Indore",
    "Vaishali Nagar, Jaipur",
    "Dwarka, New Delhi",
    "Baner, Pune",
    "Whitefield, Bengaluru",
    "Gomti Nagar, Lucknow",
]


def _format_datetime(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _format_optional_datetime(value: datetime | None) -> str | None:
    return _format_datetime(value) if isinstance(value, datetime) else None


def _parse_datetime(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def _parse_optional_datetime(value: str | None) -> datetime | None:
    return _parse_datetime(value) if value else None


def _write_json(path: Path, payload: list[dict[str, Any]]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _generate_mobile_number(index: int) -> str:
    prefix = str(9 - (index % 4))
    suffix = f"{854320000 + (index * 7919):09d}"[-9:]
    return f"{prefix}{suffix}"


def _generate_accounts(index: int, rng: random.Random) -> list[str]:
    primary = f"{918020000000000 + index:015d}"
    if rng.random() < 0.34:
        secondary = f"{918021000000000 + index:015d}"
        return [primary, secondary]
    return [primary]


def _generate_pan_number(name: str, index: int, rng: random.Random) -> str:
    clean_name = "".join(char for char in name.upper() if char.isalpha()) or "AXISX"
    letters = (clean_name + "AXISX")[:5]
    serial = f"{1000 + index:04d}"[-4:]
    suffix = chr(ord("A") + ((index + rng.randint(0, 25)) % 26))
    return f"{letters}{serial}{suffix}"


def _generate_customer_records(rng: random.Random, now: datetime) -> list[dict[str, Any]]:
    shuffled_names = CUSTOMER_NAME_POOL[:]
    rng.shuffle(shuffled_names)
    selected_names = SHOWCASE_CUSTOMER_NAMES + shuffled_names[: TOTAL_CUSTOMERS - len(SHOWCASE_CUSTOMER_NAMES)]
    customers: list[dict[str, Any]] = []

    for index, name in enumerate(selected_names, start=1):
        created_at = now - timedelta(days=rng.randint(60, 540), hours=rng.randint(0, 23), minutes=rng.randint(0, 59))
        updated_at = created_at + timedelta(days=rng.randint(1, 45), hours=rng.randint(0, 6), minutes=rng.randint(0, 59))
        if updated_at > now:
            updated_at = now - timedelta(minutes=rng.randint(5, 180))

        customers.append(
            {
                "cif_id": f"CIF{1000 + index}",
                "name": name,
                "mobile": _generate_mobile_number(index),
                "pan": _generate_pan_number(name, index, rng),
                "accounts": _generate_accounts(index, rng),
                "created_at": _format_datetime(created_at),
                "updated_at": _format_datetime(updated_at),
            }
        )

    return customers


def _normal_debit(channel: str, account_id: str, event_time: datetime, rng: random.Random) -> dict[str, Any]:
    if channel == "UPI":
        amount = rng.choice([149, 299, 450, 799, 1200, 1800, 2400, 3500, 5200, 7500])
        beneficiary = rng.choice(UPI_CONTACTS)
        is_new = rng.random() < 0.12
    elif channel == "ATM":
        amount = rng.choice([500, 1000, 2000, 3000, 5000, 8000, 10000])
        beneficiary = rng.choice(ATM_BENEFICIARIES)
        is_new = False
    elif channel == "POS":
        amount = rng.choice([245, 499, 899, 1499, 2300, 3500, 5800, 9200, 14600])
        beneficiary = rng.choice(POS_BENEFICIARIES)
        is_new = False
    elif channel == "IMPS":
        amount = rng.choice([3500, 4800, 7500, 12500, 18000, 24000, 42000])
        beneficiary = rng.choice(KNOWN_ACCOUNT_BENEFICIARIES)
        is_new = rng.random() < 0.2
    else:
        amount = rng.choice([5000, 9000, 15000, 25000, 32000, 45000])
        beneficiary = rng.choice(KNOWN_ACCOUNT_BENEFICIARIES)
        is_new = rng.random() < 0.18

    return {
        "account_id": account_id,
        "timestamp": event_time,
        "amount": float(amount),
        "type": "debit",
        "channel": channel,
        "beneficiary": beneficiary,
        "is_new_beneficiary": is_new,
        "status": "failed" if rng.random() < 0.06 else "success",
    }


def _credit_transaction(account_id: str, event_time: datetime, rng: random.Random) -> dict[str, Any]:
    amount = rng.choice([1500, 2400, 5000, 12000, 25000, 45000, 65000])
    return {
        "account_id": account_id,
        "timestamp": event_time,
        "amount": float(amount),
        "type": "credit",
        "channel": rng.choice(["IMPS", "NETBANKING", "UPI"]),
        "beneficiary": rng.choice(CREDIT_BENEFICIARIES),
        "is_new_beneficiary": False,
        "status": "success",
    }


def _fraud_pattern_transactions(customer: dict[str, Any], now: datetime, rng: random.Random) -> list[dict[str, Any]]:
    primary_account = customer["accounts"][0]
    suspicious_window = now - timedelta(days=rng.randint(0, 4), hours=rng.randint(1, 10), minutes=rng.randint(0, 45))
    pattern_type = rng.choice(["rapid_upi", "high_value_imps", "unknown_netbanking", "repeated_unknown"])

    if pattern_type == "rapid_upi":
        base_amount = rng.choice([49999.0, 74999.0, 99999.0])
        return [
            {
                "account_id": primary_account,
                "timestamp": suspicious_window,
                "amount": base_amount,
                "type": "debit",
                "channel": "UPI",
                "beneficiary": rng.choice(["scam@upi", "fraud@upi", "flashloan@upi"]),
                "is_new_beneficiary": True,
                "status": "success",
            },
            {
                "account_id": primary_account,
                "timestamp": suspicious_window + timedelta(minutes=rng.randint(2, 5)),
                "amount": base_amount,
                "type": "debit",
                "channel": "UPI",
                "beneficiary": rng.choice(["scam2@upi", "urgenthelp@upi", "prizeclaim@upi"]),
                "is_new_beneficiary": True,
                "status": "success",
            },
        ]

    if pattern_type == "high_value_imps":
        return [
            {
                "account_id": primary_account,
                "timestamp": suspicious_window,
                "amount": float(rng.choice([85000, 120000, 150000])),
                "type": "debit",
                "channel": "IMPS",
                "beneficiary": "unknown_acc",
                "is_new_beneficiary": True,
                "status": "success",
            },
            {
                "account_id": primary_account,
                "timestamp": suspicious_window + timedelta(minutes=rng.randint(3, 7)),
                "amount": float(rng.choice([50000, 65000, 70000])),
                "type": "debit",
                "channel": "IMPS",
                "beneficiary": "unknown_acc",
                "is_new_beneficiary": True,
                "status": "success",
            },
        ]

    if pattern_type == "unknown_netbanking":
        return [
            {
                "account_id": primary_account,
                "timestamp": suspicious_window,
                "amount": float(rng.choice([30000, 45000, 60000])),
                "type": "debit",
                "channel": "NETBANKING",
                "beneficiary": "new_acc",
                "is_new_beneficiary": True,
                "status": "success",
            },
            {
                "account_id": primary_account,
                "timestamp": suspicious_window + timedelta(minutes=rng.randint(4, 8)),
                "amount": float(rng.choice([20000, 35000, 50000])),
                "type": "debit",
                "channel": "NETBANKING",
                "beneficiary": "new_acc",
                "is_new_beneficiary": True,
                "status": "success",
            },
        ]

    return [
        {
            "account_id": primary_account,
            "timestamp": suspicious_window,
            "amount": float(rng.choice([52000, 68000, 99000])),
            "type": "debit",
            "channel": rng.choice(["UPI", "IMPS"]),
            "beneficiary": rng.choice(UNKNOWN_ACCOUNT_BENEFICIARIES),
            "is_new_beneficiary": True,
            "status": "success",
        },
        {
            "account_id": primary_account,
            "timestamp": suspicious_window + timedelta(minutes=rng.randint(1, 3)),
            "amount": float(rng.choice([52000, 68000, 99000])),
            "type": "debit",
            "channel": rng.choice(["UPI", "IMPS"]),
            "beneficiary": rng.choice(UNKNOWN_ACCOUNT_BENEFICIARIES),
            "is_new_beneficiary": True,
            "status": "success",
        },
        {
            "account_id": primary_account,
            "timestamp": suspicious_window + timedelta(minutes=rng.randint(4, 8)),
            "amount": float(rng.choice([25000, 40000, 51000])),
            "type": "debit",
            "channel": "NETBANKING",
            "beneficiary": "mule_account",
            "is_new_beneficiary": True,
            "status": "success",
        },
    ]


def _manual_showcase_transactions(customers: list[dict[str, Any]], now: datetime) -> dict[str, list[dict[str, Any]]]:
    showcases: dict[str, list[dict[str, Any]]] = {}

    if len(customers) < 10:
        return showcases

    featured_windows = [
        now - timedelta(days=1, hours=1, minutes=18),
        now - timedelta(days=3, hours=8, minutes=10),
        now - timedelta(days=3, hours=5, minutes=20),
        now - timedelta(days=4, hours=12),
        now - timedelta(days=5, hours=5, minutes=30),
        now - timedelta(days=6, hours=4),
        now - timedelta(days=6, hours=3, minutes=55),
        now - timedelta(days=7, hours=8, minutes=45),
        now - timedelta(days=8, hours=11, minutes=30),
        now - timedelta(days=9, hours=1, minutes=45),
    ]

    showcases[customers[0]["cif_id"]] = [
        {
            "account_id": customers[0]["accounts"][0],
            "timestamp": featured_windows[0],
            "amount": 49999.0,
            "type": "debit",
            "channel": "UPI",
            "beneficiary": "scam@upi",
            "is_new_beneficiary": True,
            "status": "success",
        },
        {
            "account_id": customers[0]["accounts"][0],
            "timestamp": featured_windows[0] + timedelta(minutes=5),
            "amount": 49999.0,
            "type": "debit",
            "channel": "UPI",
            "beneficiary": "fraud@upi",
            "is_new_beneficiary": True,
            "status": "success",
        },
    ]
    showcases[customers[1]["cif_id"]] = [
        {
            "account_id": customers[1]["accounts"][0],
            "timestamp": featured_windows[1],
            "amount": 1500.0,
            "type": "debit",
            "channel": "POS",
            "beneficiary": "Amazon",
            "is_new_beneficiary": False,
            "status": "success",
        },
        {
            "account_id": customers[1]["accounts"][0],
            "timestamp": featured_windows[1] + timedelta(minutes=2),
            "amount": 2500.0,
            "type": "debit",
            "channel": "POS",
            "beneficiary": "Flipkart",
            "is_new_beneficiary": False,
            "status": "success",
        },
    ]
    showcases[customers[2]["cif_id"]] = [
        {
            "account_id": customers[2]["accounts"][0],
            "timestamp": featured_windows[2],
            "amount": 150000.0,
            "type": "debit",
            "channel": "IMPS",
            "beneficiary": "unknown_acc",
            "is_new_beneficiary": True,
            "status": "success",
        },
        {
            "account_id": customers[2]["accounts"][0],
            "timestamp": featured_windows[2] + timedelta(minutes=5),
            "amount": 50000.0,
            "type": "debit",
            "channel": "IMPS",
            "beneficiary": "unknown_acc",
            "is_new_beneficiary": True,
            "status": "success",
        },
    ]
    showcases[customers[3]["cif_id"]] = [
        {
            "account_id": customers[3]["accounts"][0],
            "timestamp": featured_windows[3],
            "amount": 10000.0,
            "type": "debit",
            "channel": "UPI",
            "beneficiary": "friend@upi",
            "is_new_beneficiary": False,
            "status": "success",
        }
    ]
    showcases[customers[4]["cif_id"]] = [
        {
            "account_id": customers[4]["accounts"][0],
            "timestamp": featured_windows[4],
            "amount": 750.0,
            "type": "debit",
            "channel": "ATM",
            "beneficiary": "ATM-WDL",
            "is_new_beneficiary": False,
            "status": "success",
        }
    ]
    showcases[customers[5]["cif_id"]] = [
        {
            "account_id": customers[5]["accounts"][0],
            "timestamp": featured_windows[5],
            "amount": 99999.0,
            "type": "debit",
            "channel": "UPI",
            "beneficiary": "scam@upi",
            "is_new_beneficiary": True,
            "status": "success",
        }
    ]
    showcases[customers[6]["cif_id"]] = [
        {
            "account_id": customers[6]["accounts"][0],
            "timestamp": featured_windows[6],
            "amount": 99999.0,
            "type": "debit",
            "channel": "UPI",
            "beneficiary": "scam2@upi",
            "is_new_beneficiary": True,
            "status": "success",
        }
    ]
    showcases[customers[7]["cif_id"]] = [
        {
            "account_id": customers[7]["accounts"][0],
            "timestamp": featured_windows[7],
            "amount": 2000.0,
            "type": "debit",
            "channel": "POS",
            "beneficiary": "Swiggy",
            "is_new_beneficiary": False,
            "status": "success",
        }
    ]
    showcases[customers[8]["cif_id"]] = [
        {
            "account_id": customers[8]["accounts"][0],
            "timestamp": featured_windows[8],
            "amount": 500.0,
            "type": "debit",
            "channel": "ATM",
            "beneficiary": "ATM-WDL",
            "is_new_beneficiary": False,
            "status": "success",
        }
    ]
    showcases[customers[9]["cif_id"]] = [
        {
            "account_id": customers[9]["accounts"][0],
            "timestamp": featured_windows[9],
            "amount": 30000.0,
            "type": "debit",
            "channel": "NETBANKING",
            "beneficiary": "new_acc",
            "is_new_beneficiary": True,
            "status": "success",
        },
        {
            "account_id": customers[9]["accounts"][0],
            "timestamp": featured_windows[9] + timedelta(minutes=5),
            "amount": 20000.0,
            "type": "debit",
            "channel": "NETBANKING",
            "beneficiary": "new_acc",
            "is_new_beneficiary": True,
            "status": "success",
        },
    ]

    return showcases


def _build_customer_transactions(
    customer: dict[str, Any],
    now: datetime,
    rng: random.Random,
    featured: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    transactions: list[dict[str, Any]] = list(featured or [])
    target_count = rng.randint(MIN_TRANSACTIONS_PER_CUSTOMER, MAX_TRANSACTIONS_PER_CUSTOMER)
    showcase_cif_ids = {f"CIF{1000 + index}" for index in range(1, len(SHOWCASE_CUSTOMER_NAMES) + 1)}
    should_inject_fraud = customer["cif_id"] not in showcase_cif_ids and rng.random() < 0.28

    if should_inject_fraud:
        transactions.extend(_fraud_pattern_transactions(customer, now, rng))

    while len(transactions) < target_count:
        lookback = timedelta(
            days=rng.randint(0, LOOKBACK_DAYS - 1),
            hours=rng.randint(0, 23),
            minutes=rng.randint(0, 59),
        )
        event_time = now - lookback
        account_id = rng.choice(customer["accounts"])

        if rng.random() < 0.18:
            transactions.append(_credit_transaction(account_id, event_time, rng))
            continue

        channel = rng.choice(CHANNELS)
        transactions.append(_normal_debit(channel, account_id, event_time, rng))

    return transactions


def _generate_transaction_records(customers: list[dict[str, Any]], rng: random.Random, now: datetime) -> list[dict[str, Any]]:
    showcases = _manual_showcase_transactions(customers, now)
    transaction_rows: list[dict[str, Any]] = []
    txn_sequence = 1

    for customer in customers:
        featured = showcases.get(customer["cif_id"], [])
        customer_transactions = _build_customer_transactions(customer, now, rng, featured=featured)
        customer_transactions.sort(key=lambda item: item["timestamp"], reverse=True)

        for transaction in customer_transactions:
            transaction_rows.append(
                {
                    "txn_id": f"TXN{txn_sequence:05d}",
                    "cif_id": customer["cif_id"],
                    "account_id": transaction["account_id"],
                    "timestamp": _format_datetime(transaction["timestamp"]),
                    "amount": float(transaction["amount"]),
                    "type": transaction["type"],
                    "channel": transaction["channel"],
                    "beneficiary": transaction["beneficiary"],
                    "is_new_beneficiary": bool(transaction["is_new_beneficiary"]),
                    "status": transaction["status"],
                }
            )
            txn_sequence += 1

    transaction_rows.sort(key=lambda item: item["timestamp"], reverse=True)
    return transaction_rows


def _pick_customer_transactions(
    transactions_by_cif: dict[str, list[dict[str, Any]]],
    cif_id: str,
    *,
    debit_only: bool = True,
    limit: int = 3,
) -> list[dict[str, Any]]:
    rows = transactions_by_cif.get(cif_id, [])
    if debit_only:
        rows = [row for row in rows if str(row.get("type") or "").lower() == "debit"]
    return rows[:limit]


def _property_address(index: int) -> str:
    locality = PROPERTY_AREAS[index % len(PROPERTY_AREAS)]
    return f"Plot {300 + index}, {locality}"


def _registry_reference(prefix: str, index: int) -> str:
    return f"{prefix}-{2024 + (index % 2)}-{9000 + index}"


def _generate_case_data_records(
    customers: list[dict[str, Any]],
    transactions: list[dict[str, Any]],
    rng: random.Random,
    now: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    customer_map = {customer["cif_id"]: customer for customer in customers}
    transactions_by_cif: dict[str, list[dict[str, Any]]] = {}
    for transaction in transactions:
        transactions_by_cif.setdefault(str(transaction["cif_id"]), []).append(transaction)
    for rows in transactions_by_cif.values():
        rows.sort(key=lambda item: item["timestamp"], reverse=True)

    loan_accounts: list[dict[str, Any]] = []
    collateral_records: list[dict[str, Any]] = []
    document_verifications: list[dict[str, Any]] = []
    case_events: list[dict[str, Any]] = []

    loan_sequence = 1
    collateral_sequence = 1
    verification_sequence = 1
    event_sequence = 1

    def next_loan_id() -> str:
        nonlocal loan_sequence
        value = f"LOAN{loan_sequence:05d}"
        loan_sequence += 1
        return value

    def next_collateral_id() -> str:
        nonlocal collateral_sequence
        value = f"COLL{collateral_sequence:05d}"
        collateral_sequence += 1
        return value

    def next_verification_id() -> str:
        nonlocal verification_sequence
        value = f"DOCV{verification_sequence:05d}"
        verification_sequence += 1
        return value

    def next_event_id() -> str:
        nonlocal event_sequence
        value = f"CASEEV{event_sequence:05d}"
        event_sequence += 1
        return value

    def add_case_event(
        *,
        cif_id: str,
        case_family: str,
        event_type: str,
        severity: str,
        status: str,
        summary: str,
        occurred_at: datetime,
        loan_id: str | None = None,
        linked_document_ids: list[str] | None = None,
        linked_txn_ids: list[str] | None = None,
        outcome_note: str = "",
    ) -> None:
        case_events.append(
            {
                "event_id": next_event_id(),
                "cif_id": cif_id,
                "loan_id": loan_id,
                "case_family": case_family,
                "event_type": event_type,
                "severity": severity,
                "status": status,
                "summary": summary,
                "occurred_at": _format_datetime(occurred_at),
                "linked_txn_ids": list(linked_txn_ids or []),
                "linked_document_ids": list(linked_document_ids or []),
                "outcome_note": outcome_note,
                "created_at": _format_datetime(occurred_at),
                "updated_at": _format_datetime(min(now, occurred_at + timedelta(hours=4))),
            }
        )

    def add_document_verification(
        *,
        cif_id: str,
        case_family: str,
        document_type: str,
        document_reference: str,
        submitted_at: datetime,
        verification_status: str,
        mismatch_type: str,
        mismatch_reason: str,
        source_checked_with: str,
        verifier_remark: str,
        linked_reference: str,
        loan_id: str | None = None,
        collateral_id: str | None = None,
    ) -> str:
        verification_id = next_verification_id()
        updated_at = min(now, submitted_at + timedelta(hours=8))
        document_verifications.append(
            {
                "verification_id": verification_id,
                "cif_id": cif_id,
                "loan_id": loan_id,
                "collateral_id": collateral_id,
                "case_family": case_family,
                "document_type": document_type,
                "document_reference": document_reference,
                "submitted_at": _format_datetime(submitted_at),
                "verification_status": verification_status,
                "mismatch_type": mismatch_type,
                "mismatch_reason": mismatch_reason,
                "source_checked_with": source_checked_with,
                "verifier_remark": verifier_remark,
                "linked_reference": linked_reference,
                "created_at": _format_datetime(submitted_at),
                "updated_at": _format_datetime(updated_at),
            }
        )
        return verification_id

    def add_collateral_record(
        *,
        cif_id: str,
        loan_id: str,
        collateral_type: str,
        property_address: str,
        declared_owner_name: str,
        verified_owner_name: str,
        declared_market_value: float,
        assessed_value: float,
        verification_status: str,
        encumbrance_status: str,
        registry_reference: str,
        duplicate_collateral_hits: int,
        issues: list[str],
        supporting_document_ids: list[str],
        last_verified_at: datetime,
    ) -> str:
        collateral_id = next_collateral_id()
        collateral_records.append(
            {
                "collateral_id": collateral_id,
                "loan_id": loan_id,
                "cif_id": cif_id,
                "collateral_type": collateral_type,
                "property_address": property_address,
                "declared_owner_name": declared_owner_name,
                "verified_owner_name": verified_owner_name,
                "declared_market_value": float(declared_market_value),
                "assessed_value": float(assessed_value),
                "verification_status": verification_status,
                "encumbrance_status": encumbrance_status,
                "registry_reference": registry_reference,
                "duplicate_collateral_hits": int(duplicate_collateral_hits),
                "issues": list(issues),
                "supporting_document_ids": list(supporting_document_ids),
                "last_verified_at": _format_datetime(last_verified_at),
                "created_at": _format_datetime(last_verified_at - timedelta(days=45)),
                "updated_at": _format_datetime(last_verified_at),
            }
        )
        return collateral_id

    def add_loan_account(
        *,
        cif_id: str,
        linked_account_id: str | None,
        product_type: str,
        sanction_amount: float,
        disbursed_amount: float,
        outstanding_amount: float,
        overdue_amount: float,
        emi_amount: float,
        sanctioned_at: datetime,
        repayment_status: str,
        days_past_due: int,
        branch_name: str,
        loan_status: str,
        underwriting_flags: list[str],
        collateral_id: str | None = None,
        last_repayment_at: datetime | None = None,
    ) -> str:
        loan_id = next_loan_id()
        product_prefix = {
            "Home Loan": "HL",
            "Loan Against Property": "LAP",
            "Personal Loan": "PL",
            "Business Loan": "BL",
            "Auto Loan": "AL",
        }.get(product_type, "LN")
        loan_accounts.append(
            {
                "loan_id": loan_id,
                "cif_id": cif_id,
                "loan_account_number": f"{product_prefix}{918035000000000 + len(loan_accounts) + 1:015d}",
                "linked_account_id": linked_account_id,
                "product_type": product_type,
                "sanction_amount": float(sanction_amount),
                "disbursed_amount": float(disbursed_amount),
                "outstanding_amount": float(outstanding_amount),
                "overdue_amount": float(overdue_amount),
                "emi_amount": float(emi_amount),
                "sanctioned_at": _format_datetime(sanctioned_at),
                "repayment_status": repayment_status,
                "days_past_due": int(days_past_due),
                "last_repayment_at": _format_optional_datetime(last_repayment_at),
                "branch_name": branch_name,
                "collateral_id": collateral_id,
                "loan_status": loan_status,
                "underwriting_flags": list(underwriting_flags),
                "created_at": _format_datetime(sanctioned_at),
                "updated_at": _format_datetime(min(now, sanctioned_at + timedelta(days=120))),
            }
        )
        return loan_id

    # Showcase: CIF1002 loan and fake mortgage collateral fraud.
    aman = customer_map["CIF1002"]
    aman_sanctioned_at = now - timedelta(days=420)
    aman_last_repayment = now - timedelta(days=132)
    aman_loan_id = add_loan_account(
        cif_id=aman["cif_id"],
        linked_account_id=aman["accounts"][0],
        product_type="Home Loan",
        sanction_amount=4200000.0,
        disbursed_amount=4200000.0,
        outstanding_amount=3895000.0,
        overdue_amount=215000.0,
        emi_amount=46250.0,
        sanctioned_at=aman_sanctioned_at,
        repayment_status="defaulted",
        days_past_due=128,
        branch_name="Axis Bank Indore Home Loan Centre",
        loan_status="under_fraud_review",
        underwriting_flags=["Collateral authenticity concern", "Ownership trail mismatch"],
        last_repayment_at=aman_last_repayment,
    )
    aman_docs = [
        add_document_verification(
            cif_id=aman["cif_id"],
            case_family="Loan / Mortgage Fraud",
            loan_id=aman_loan_id,
            document_type="Title Deed",
            document_reference="TD-MP-2024-1002",
            submitted_at=aman_sanctioned_at - timedelta(days=14),
            verification_status="forged",
            mismatch_type="ownership_mismatch",
            mismatch_reason="Registry extract shows a different owner than the submitted deed chain.",
            source_checked_with="State land registry extract",
            verifier_remark="Manual registry pull does not support the submitted owner transition.",
            linked_reference=_registry_reference("REG-MP-IND", 1002),
        ),
        add_document_verification(
            cif_id=aman["cif_id"],
            case_family="Loan / Mortgage Fraud",
            loan_id=aman_loan_id,
            document_type="Encumbrance Certificate",
            document_reference="ENC-MP-2024-1002",
            submitted_at=aman_sanctioned_at - timedelta(days=12),
            verification_status="mismatch",
            mismatch_type="registry_mismatch",
            mismatch_reason="Submitted encumbrance trail does not align with land-registry charge history.",
            source_checked_with="Registrar of assurances portal",
            verifier_remark="Charge details differ from the submitted certificate.",
            linked_reference=_registry_reference("ENC-CHK", 1002),
        ),
        add_document_verification(
            cif_id=aman["cif_id"],
            case_family="Loan / Mortgage Fraud",
            loan_id=aman_loan_id,
            document_type="Valuation Report",
            document_reference="VAL-MP-2024-1002",
            submitted_at=aman_sanctioned_at - timedelta(days=10),
            verification_status="unverifiable",
            mismatch_type="valuer_source_gap",
            mismatch_reason="The submitted valuer credentials and report reference could not be validated.",
            source_checked_with="Approved valuer roster",
            verifier_remark="Valuation source check failed and report numbering is inconsistent.",
            linked_reference="VAL-CHECK-1002",
        ),
    ]
    aman_collateral_verified_at = now - timedelta(days=5, hours=3)
    aman_collateral_id = add_collateral_record(
        cif_id=aman["cif_id"],
        loan_id=aman_loan_id,
        collateral_type="Residential Flat",
        property_address="Flat 804, Plot 348, Vijay Nagar, Indore",
        declared_owner_name="Aman Verma",
        verified_owner_name="Sushila Devi",
        declared_market_value=6200000.0,
        assessed_value=0.0,
        verification_status="suspected_forged",
        encumbrance_status="registry_mismatch",
        registry_reference=_registry_reference("INDORE-REG", 1002),
        duplicate_collateral_hits=1,
        issues=[
            "Submitted deed number does not match the land-registry extract.",
            "Declared owner name differs from the verified registry owner.",
            "The same property reference appears in a prior third-party mortgage search hit.",
        ],
        supporting_document_ids=aman_docs,
        last_verified_at=aman_collateral_verified_at,
    )
    loan_accounts[-1]["collateral_id"] = aman_collateral_id
    for document in document_verifications[-3:]:
        document["collateral_id"] = aman_collateral_id
    add_case_event(
        cif_id=aman["cif_id"],
        loan_id=aman_loan_id,
        case_family="Loan / Mortgage Fraud",
        event_type="repayment_default",
        severity="high",
        status="open",
        summary="EMI obligations remained unpaid for over 120 days after the initial repayment cycle.",
        occurred_at=now - timedelta(days=128),
        outcome_note="Recovery queue opened after prolonged delinquency.",
    )
    add_case_event(
        cif_id=aman["cif_id"],
        loan_id=aman_loan_id,
        case_family="Loan / Mortgage Fraud",
        event_type="collateral_registry_mismatch",
        severity="high",
        status="escalated",
        summary="Collateral re-verification found a registry-owner mismatch against the submitted property papers.",
        occurred_at=aman_collateral_verified_at,
        linked_document_ids=aman_docs,
        outcome_note="Legal and fraud review escalated after registry mismatch confirmation.",
    )
    add_case_event(
        cif_id=aman["cif_id"],
        loan_id=aman_loan_id,
        case_family="Loan / Mortgage Fraud",
        event_type="legal_recovery_review",
        severity="medium",
        status="under_review",
        summary="Legal and recovery teams were asked to review enforceability before recovery action.",
        occurred_at=now - timedelta(days=3, hours=4),
        linked_document_ids=aman_docs[:2],
        outcome_note="Awaiting legal enforceability opinion on the submitted collateral set.",
    )

    # Showcase: CIF1003 document fraud during underwriting.
    neha = customer_map["CIF1003"]
    neha_sanctioned_at = now - timedelta(days=54)
    neha_loan_id = add_loan_account(
        cif_id=neha["cif_id"],
        linked_account_id=neha["accounts"][0],
        product_type="Business Loan",
        sanction_amount=1800000.0,
        disbursed_amount=0.0,
        outstanding_amount=0.0,
        overdue_amount=0.0,
        emi_amount=39250.0,
        sanctioned_at=neha_sanctioned_at,
        repayment_status="on_hold",
        days_past_due=0,
        branch_name="Axis Bank Jaipur Retail Assets Hub",
        loan_status="under_fraud_review",
        underwriting_flags=["Income proof inconsistency", "Bank statement mismatch"],
        last_repayment_at=None,
    )
    neha_docs = [
        add_document_verification(
            cif_id=neha["cif_id"],
            case_family="Document Fraud",
            loan_id=neha_loan_id,
            document_type="Salary Slip",
            document_reference="SAL-2026-NS-01",
            submitted_at=neha_sanctioned_at - timedelta(days=5),
            verification_status="forged",
            mismatch_type="issuer_mismatch",
            mismatch_reason="Employer format and payroll reference do not match the issuing company's standard output.",
            source_checked_with="Employer payroll confirmation",
            verifier_remark="Payroll team did not confirm the submitted salary slip number.",
            linked_reference="HR-EMAIL-1003",
        ),
        add_document_verification(
            cif_id=neha["cif_id"],
            case_family="Document Fraud",
            loan_id=neha_loan_id,
            document_type="Bank Statement",
            document_reference="BST-2026-NS-01",
            submitted_at=neha_sanctioned_at - timedelta(days=4),
            verification_status="mismatch",
            mismatch_type="statement_alteration",
            mismatch_reason="PDF metadata and running-balance sequence indicate likely statement alteration.",
            source_checked_with="Source bank statement pull",
            verifier_remark="Source statement values do not match the submitted version.",
            linked_reference="BANK-PULL-1003",
        ),
        add_document_verification(
            cif_id=neha["cif_id"],
            case_family="Document Fraud",
            loan_id=neha_loan_id,
            document_type="ITR Ack",
            document_reference="ITR-2025-NS-01",
            submitted_at=neha_sanctioned_at - timedelta(days=3),
            verification_status="unverifiable",
            mismatch_type="tax_record_gap",
            mismatch_reason="Submitted tax acknowledgement could not be reconciled with the taxpayer portal status.",
            source_checked_with="Income tax acknowledgement portal",
            verifier_remark="Portal lookup did not validate the submitted acknowledgement reference.",
            linked_reference="ITR-CHECK-1003",
        ),
    ]
    add_case_event(
        cif_id=neha["cif_id"],
        loan_id=neha_loan_id,
        case_family="Document Fraud",
        event_type="underwriting_hold",
        severity="high",
        status="under_review",
        summary="Loan underwriting was paused after income-proof verification failed.",
        occurred_at=now - timedelta(days=48),
        linked_document_ids=neha_docs,
        outcome_note="Credit approval is on hold pending manual document review.",
    )
    add_case_event(
        cif_id=neha["cif_id"],
        loan_id=neha_loan_id,
        case_family="Document Fraud",
        event_type="income_document_mismatch",
        severity="high",
        status="open",
        summary="Salary and bank-statement checks produced multiple mismatches against source records.",
        occurred_at=now - timedelta(days=47, hours=6),
        linked_document_ids=neha_docs[:2],
        outcome_note="Fraud team requested a forensic review of the submitted income pack.",
    )

    # Showcase: CIF1004 KYC and identity anomalies.
    rohit = customer_map["CIF1004"]
    rohit_docs = [
        add_document_verification(
            cif_id=rohit["cif_id"],
            case_family="KYC / Identity Fraud",
            document_type="PAN",
            document_reference=f"{rohit['pan']}-KYC",
            submitted_at=now - timedelta(days=22),
            verification_status="mismatch",
            mismatch_type="demographic_mismatch",
            mismatch_reason="PAN demographic pull does not align with the submitted profile date of birth.",
            source_checked_with="NSDL PAN verification",
            verifier_remark="PAN returned a demographic mismatch against the onboarding profile.",
            linked_reference="NSDL-VERIFY-1004",
        ),
        add_document_verification(
            cif_id=rohit["cif_id"],
            case_family="KYC / Identity Fraud",
            document_type="Aadhaar",
            document_reference="AAD-1004-KYC",
            submitted_at=now - timedelta(days=21, hours=5),
            verification_status="suspected_impersonation",
            mismatch_type="face_mismatch",
            mismatch_reason="Face and liveness check did not align with the submitted identity image.",
            source_checked_with="Video KYC review",
            verifier_remark="The captured face image appears inconsistent with the document portrait.",
            linked_reference="VKYC-1004",
        ),
        add_document_verification(
            cif_id=rohit["cif_id"],
            case_family="KYC / Identity Fraud",
            document_type="Address Proof",
            document_reference="ADDR-1004-KYC",
            submitted_at=now - timedelta(days=20, hours=10),
            verification_status="mismatch",
            mismatch_type="address_inconsistency",
            mismatch_reason="Submitted address proof did not align with the declared correspondence address.",
            source_checked_with="Manual proof review",
            verifier_remark="Address proof review found a mismatch across submitted KYC records.",
            linked_reference="ADDR-CHK-1004",
        ),
    ]
    add_case_event(
        cif_id=rohit["cif_id"],
        case_family="KYC / Identity Fraud",
        event_type="onboarding_flag",
        severity="high",
        status="under_review",
        summary="Onboarding review flagged identity mismatch indicators across PAN, Aadhaar, and address proof.",
        occurred_at=now - timedelta(days=20),
        linked_document_ids=rohit_docs,
        outcome_note="Relationship remains on enhanced due-diligence hold.",
    )
    add_case_event(
        cif_id=rohit["cif_id"],
        case_family="KYC / Identity Fraud",
        event_type="profile_change_alert",
        severity="medium",
        status="open",
        summary="Recent mobile and email changes were recorded shortly before the KYC mismatch escalation.",
        occurred_at=now - timedelta(days=18, hours=9),
        outcome_note="Profile changes require linked-account review before closure.",
    )

    # Showcase: CIF1008 dispute / first-party abuse.
    karan = customer_map["CIF1008"]
    karan_linked_txns = [row["txn_id"] for row in _pick_customer_transactions(transactions_by_cif, karan["cif_id"], debit_only=True, limit=2)]
    add_case_event(
        cif_id=karan["cif_id"],
        case_family="Dispute / First-Party Abuse",
        event_type="dispute_received",
        severity="medium",
        status="open",
        summary="Customer denied recent merchant debits and requested reversal despite a normal authorization trail.",
        occurred_at=now - timedelta(days=6, hours=3),
        linked_txn_ids=karan_linked_txns[:1],
        outcome_note="Initial dispute logged pending merchant and channel evidence review.",
    )
    add_case_event(
        cif_id=karan["cif_id"],
        case_family="Dispute / First-Party Abuse",
        event_type="repeat_dispute_pattern",
        severity="high",
        status="under_review",
        summary="Repeat complaint behaviour matches prior disputes raised against similar merchant spends.",
        occurred_at=now - timedelta(days=2, hours=11),
        linked_txn_ids=karan_linked_txns,
        outcome_note="Complaint history suggests potential first-party fraud and needs supervisor review.",
    )
    add_case_event(
        cif_id=karan["cif_id"],
        case_family="Dispute / First-Party Abuse",
        event_type="authorization_evidence_found",
        severity="medium",
        status="confirmed",
        summary="Merchant and channel records indicate a normal authorization path for the disputed merchant transaction.",
        occurred_at=now - timedelta(days=1, hours=8),
        linked_txn_ids=karan_linked_txns[:1],
        outcome_note="Liability decision deferred until the repeat-dispute review is completed.",
    )

    # Additional normal loan records to make the lending dataset realistic.
    standard_loan_profiles = [
        ("CIF1005", "Home Loan"),
        ("CIF1006", "Loan Against Property"),
        ("CIF1007", "Personal Loan"),
        ("CIF1010", "Auto Loan"),
        ("CIF1011", "Home Loan"),
        ("CIF1013", "Business Loan"),
        ("CIF1015", "Loan Against Property"),
        ("CIF1018", "Personal Loan"),
    ]
    for offset, (cif_id, product_type) in enumerate(standard_loan_profiles, start=1):
        customer = customer_map.get(cif_id)
        if not customer:
            continue

        sanctioned_at = now - timedelta(days=rng.randint(180, 900))
        sanction_amount = float(rng.choice([350000, 650000, 950000, 1800000, 3200000]))
        repayment_status = rng.choice(["active", "active", "overdue", "closed"])
        days_past_due = 0 if repayment_status in {"active", "closed"} else rng.randint(5, 34)
        overdue_amount = 0.0 if days_past_due == 0 else float(rng.choice([18000, 32000, 54000]))
        outstanding_amount = 0.0 if repayment_status == "closed" else round(sanction_amount * rng.uniform(0.18, 0.82), 2)
        disbursed_amount = sanction_amount
        emi_amount = round(max(6200.0, sanction_amount / rng.randint(48, 180)), 2)
        branch_name = rng.choice(BRANCH_NAMES)
        last_repayment_at = None if repayment_status == "closed" else now - timedelta(days=rng.randint(7, 45))
        underwriting_flags = ["Standard control checks cleared"]
        loan_status = "active" if repayment_status != "closed" else "closed"

        loan_id = add_loan_account(
            cif_id=cif_id,
            linked_account_id=customer["accounts"][0],
            product_type=product_type,
            sanction_amount=sanction_amount,
            disbursed_amount=disbursed_amount,
            outstanding_amount=outstanding_amount,
            overdue_amount=overdue_amount,
            emi_amount=emi_amount,
            sanctioned_at=sanctioned_at,
            repayment_status=repayment_status,
            days_past_due=days_past_due,
            branch_name=branch_name,
            loan_status=loan_status,
            underwriting_flags=underwriting_flags,
            last_repayment_at=last_repayment_at,
        )

        supporting_document_ids: list[str] = []
        if product_type in COLLATERAL_REQUIRED_PRODUCTS:
            supporting_document_ids.append(
                add_document_verification(
                    cif_id=cif_id,
                    case_family="Loan / Mortgage Fraud",
                    loan_id=loan_id,
                    document_type="Title Deed",
                    document_reference=f"TD-STD-{offset:03d}",
                    submitted_at=sanctioned_at - timedelta(days=12),
                    verification_status="verified",
                    mismatch_type="none",
                    mismatch_reason="Ownership documents aligned with registry checks.",
                    source_checked_with="State land registry extract",
                    verifier_remark="Ownership and registry trail verified successfully.",
                    linked_reference=_registry_reference("STD-REG", offset),
                )
            )
            supporting_document_ids.append(
                add_document_verification(
                    cif_id=cif_id,
                    case_family="Loan / Mortgage Fraud",
                    loan_id=loan_id,
                    document_type="Encumbrance Certificate",
                    document_reference=f"ENC-STD-{offset:03d}",
                    submitted_at=sanctioned_at - timedelta(days=10),
                    verification_status="verified",
                    mismatch_type="none",
                    mismatch_reason="Charge history matched submitted encumbrance status.",
                    source_checked_with="Registrar of assurances portal",
                    verifier_remark="No mismatch was observed in the submitted encumbrance trail.",
                    linked_reference=_registry_reference("STD-ENC", offset),
                )
            )
            collateral_id = add_collateral_record(
                cif_id=cif_id,
                loan_id=loan_id,
                collateral_type=rng.choice(PROPERTY_TYPES),
                property_address=_property_address(offset),
                declared_owner_name=customer["name"],
                verified_owner_name=customer["name"],
                declared_market_value=round(sanction_amount * rng.uniform(1.4, 2.2), 2),
                assessed_value=round(sanction_amount * rng.uniform(1.3, 2.0), 2),
                verification_status="verified",
                encumbrance_status="clear",
                registry_reference=_registry_reference("CLR-REG", offset),
                duplicate_collateral_hits=0,
                issues=["No critical ownership or enforceability issue detected during verification."],
                supporting_document_ids=supporting_document_ids,
                last_verified_at=sanctioned_at - timedelta(days=3),
            )
            loan_accounts[-1]["collateral_id"] = collateral_id
            for document in document_verifications[-2:]:
                document["collateral_id"] = collateral_id
        else:
            add_document_verification(
                cif_id=cif_id,
                case_family="Document Fraud",
                loan_id=loan_id,
                document_type="Income Proof",
                document_reference=f"INC-STD-{offset:03d}",
                submitted_at=sanctioned_at - timedelta(days=6),
                verification_status="verified",
                mismatch_type="none",
                mismatch_reason="Income proof aligned with source verification checks.",
                source_checked_with="Employer or source validation",
                verifier_remark="Income proof review completed without exception.",
                linked_reference=f"INC-REF-{offset:03d}",
            )
            add_document_verification(
                cif_id=cif_id,
                case_family="Document Fraud",
                loan_id=loan_id,
                document_type="Bank Statement",
                document_reference=f"BST-STD-{offset:03d}",
                submitted_at=sanctioned_at - timedelta(days=5),
                verification_status="verified",
                mismatch_type="none",
                mismatch_reason="Submitted bank statement aligned with source values.",
                source_checked_with="Statement pull verification",
                verifier_remark="Statement authenticity and balances were verified.",
                linked_reference=f"BST-REF-{offset:03d}",
            )

    loan_accounts.sort(key=lambda item: item["sanctioned_at"], reverse=True)
    collateral_records.sort(key=lambda item: item["last_verified_at"], reverse=True)
    document_verifications.sort(key=lambda item: item["submitted_at"], reverse=True)
    case_events.sort(key=lambda item: item["occurred_at"], reverse=True)
    return loan_accounts, collateral_records, document_verifications, case_events


def _validate_seed_payload(
    customers: list[dict[str, Any]],
    transactions: list[dict[str, Any]],
    loan_accounts: list[dict[str, Any]],
    collateral_records: list[dict[str, Any]],
    document_verifications: list[dict[str, Any]],
    case_events: list[dict[str, Any]],
) -> None:
    cif_ids = {customer["cif_id"] for customer in customers}
    pan_values = {customer["pan"] for customer in customers}
    txn_ids = {transaction["txn_id"] for transaction in transactions}

    if len(cif_ids) != len(customers):
        raise ValueError("Duplicate CIF IDs generated in customer seed data.")
    if len(pan_values) != len(customers):
        raise ValueError("Duplicate PAN values generated in customer seed data.")
    if len(txn_ids) != len(transactions):
        raise ValueError("Duplicate transaction IDs generated in transaction seed data.")
    if not all(customer.get("accounts") for customer in customers):
        raise ValueError("Every customer must have at least one account.")
    if not 50 <= len(customers) <= 100:
        raise ValueError("Generated customer count is outside the required 50-100 range.")
    if not 500 <= len(transactions) <= 1000:
        raise ValueError("Generated transaction count is outside the required 500-1000 range.")

    loan_ids = {row["loan_id"] for row in loan_accounts}
    loan_numbers = {row["loan_account_number"] for row in loan_accounts}
    collateral_ids = {row["collateral_id"] for row in collateral_records}
    verification_ids = {row["verification_id"] for row in document_verifications}
    event_ids = {row["event_id"] for row in case_events}

    if len(loan_ids) != len(loan_accounts):
        raise ValueError("Duplicate loan IDs generated in loan seed data.")
    if len(loan_numbers) != len(loan_accounts):
        raise ValueError("Duplicate loan account numbers generated in loan seed data.")
    if len(collateral_ids) != len(collateral_records):
        raise ValueError("Duplicate collateral IDs generated in collateral seed data.")
    if len(verification_ids) != len(document_verifications):
        raise ValueError("Duplicate document verification IDs generated in document seed data.")
    if len(event_ids) != len(case_events):
        raise ValueError("Duplicate case-event IDs generated in case-event seed data.")

    customer_accounts = {customer["cif_id"]: set(customer["accounts"]) for customer in customers}
    for row in loan_accounts:
        if row["cif_id"] not in cif_ids:
            raise ValueError(f"Loan record references unknown CIF {row['cif_id']}.")
        linked_account_id = row.get("linked_account_id")
        if linked_account_id and linked_account_id not in customer_accounts.get(row["cif_id"], set()):
            raise ValueError(f"Loan record for {row['cif_id']} references an unknown linked account.")

    for row in collateral_records:
        if row["cif_id"] not in cif_ids:
            raise ValueError(f"Collateral record references unknown CIF {row['cif_id']}.")
        if row["loan_id"] not in loan_ids:
            raise ValueError(f"Collateral record references unknown loan ID {row['loan_id']}.")
        for document_id in row.get("supporting_document_ids") or []:
            if document_id not in verification_ids:
                raise ValueError(f"Collateral record references unknown document verification ID {document_id}.")

    for row in document_verifications:
        if row["cif_id"] not in cif_ids:
            raise ValueError(f"Document verification references unknown CIF {row['cif_id']}.")
        if row.get("loan_id") and row["loan_id"] not in loan_ids:
            raise ValueError(f"Document verification references unknown loan ID {row['loan_id']}.")
        if row.get("collateral_id") and row["collateral_id"] not in collateral_ids:
            raise ValueError(f"Document verification references unknown collateral ID {row['collateral_id']}.")

    for row in case_events:
        if row["cif_id"] not in cif_ids:
            raise ValueError(f"Case event references unknown CIF {row['cif_id']}.")
        if row.get("loan_id") and row["loan_id"] not in loan_ids:
            raise ValueError(f"Case event references unknown loan ID {row['loan_id']}.")
        for document_id in row.get("linked_document_ids") or []:
            if document_id not in verification_ids:
                raise ValueError(f"Case event references unknown document verification ID {document_id}.")
        for txn_id in row.get("linked_txn_ids") or []:
            if txn_id not in txn_ids:
                raise ValueError(f"Case event references unknown transaction ID {txn_id}.")

    showcase_cifs = {"CIF1002", "CIF1003", "CIF1004", "CIF1008"}
    if not showcase_cifs.issubset({row["cif_id"] for row in case_events}):
        raise ValueError("Expected showcase case-event records were not generated.")


def generate_seed_files() -> dict[str, list[dict[str, Any]]]:
    now = datetime.now().replace(second=0, microsecond=0)
    rng = random.Random(now.strftime("%Y%m%d%H%M"))

    customers = _generate_customer_records(rng, now)
    transactions = _generate_transaction_records(customers, rng, now)
    loan_accounts, collateral_records, document_verifications, case_events = _generate_case_data_records(customers, transactions, rng, now)
    _validate_seed_payload(customers, transactions, loan_accounts, collateral_records, document_verifications, case_events)

    payload = {
        "customers": customers,
        "transactions": transactions,
        "loan_accounts": loan_accounts,
        "collateral_records": collateral_records,
        "document_verifications": document_verifications,
        "case_events": case_events,
    }
    _write_json(CUSTOMERS_FILE, customers)
    _write_json(TRANSACTIONS_FILE, transactions)
    _write_json(LOAN_ACCOUNTS_FILE, loan_accounts)
    _write_json(COLLATERAL_RECORDS_FILE, collateral_records)
    _write_json(DOCUMENT_VERIFICATIONS_FILE, document_verifications)
    _write_json(CASE_EVENTS_FILE, case_events)
    return payload


def _load_seed_file(path: Path) -> list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def _prepare_customer_documents(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "cif_id": row["cif_id"],
            "name": row["name"],
            "mobile": row["mobile"],
            "pan": row["pan"],
            "accounts": row["accounts"],
            "created_at": _parse_datetime(row["created_at"]),
            "updated_at": _parse_datetime(row["updated_at"]),
        }
        for row in rows
    ]


def _prepare_transaction_documents(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "txn_id": row["txn_id"],
            "cif_id": row["cif_id"],
            "account_id": row["account_id"],
            "timestamp": _parse_datetime(row["timestamp"]),
            "amount": float(row["amount"]),
            "type": row["type"],
            "channel": row["channel"],
            "beneficiary": row["beneficiary"],
            "is_new_beneficiary": bool(row["is_new_beneficiary"]),
            "status": row["status"],
        }
        for row in rows
    ]


def _prepare_loan_account_documents(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "loan_id": row["loan_id"],
            "cif_id": row["cif_id"],
            "loan_account_number": row["loan_account_number"],
            "linked_account_id": row.get("linked_account_id"),
            "product_type": row["product_type"],
            "sanction_amount": float(row["sanction_amount"]),
            "disbursed_amount": float(row["disbursed_amount"]),
            "outstanding_amount": float(row["outstanding_amount"]),
            "overdue_amount": float(row["overdue_amount"]),
            "emi_amount": float(row["emi_amount"]),
            "sanctioned_at": _parse_datetime(row["sanctioned_at"]),
            "repayment_status": row["repayment_status"],
            "days_past_due": int(row["days_past_due"]),
            "last_repayment_at": _parse_optional_datetime(row.get("last_repayment_at")),
            "branch_name": row["branch_name"],
            "collateral_id": row.get("collateral_id"),
            "loan_status": row["loan_status"],
            "underwriting_flags": list(row.get("underwriting_flags") or []),
            "created_at": _parse_datetime(row["created_at"]),
            "updated_at": _parse_datetime(row["updated_at"]),
        }
        for row in rows
    ]


def _prepare_collateral_documents(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "collateral_id": row["collateral_id"],
            "loan_id": row["loan_id"],
            "cif_id": row["cif_id"],
            "collateral_type": row["collateral_type"],
            "property_address": row["property_address"],
            "declared_owner_name": row["declared_owner_name"],
            "verified_owner_name": row["verified_owner_name"],
            "declared_market_value": float(row["declared_market_value"]),
            "assessed_value": float(row["assessed_value"]),
            "verification_status": row["verification_status"],
            "encumbrance_status": row["encumbrance_status"],
            "registry_reference": row["registry_reference"],
            "duplicate_collateral_hits": int(row["duplicate_collateral_hits"]),
            "issues": list(row.get("issues") or []),
            "supporting_document_ids": list(row.get("supporting_document_ids") or []),
            "last_verified_at": _parse_datetime(row["last_verified_at"]),
            "created_at": _parse_datetime(row["created_at"]),
            "updated_at": _parse_datetime(row["updated_at"]),
        }
        for row in rows
    ]


def _prepare_document_verification_documents(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "verification_id": row["verification_id"],
            "cif_id": row["cif_id"],
            "loan_id": row.get("loan_id"),
            "collateral_id": row.get("collateral_id"),
            "case_family": row["case_family"],
            "document_type": row["document_type"],
            "document_reference": row["document_reference"],
            "submitted_at": _parse_datetime(row["submitted_at"]),
            "verification_status": row["verification_status"],
            "mismatch_type": row["mismatch_type"],
            "mismatch_reason": row["mismatch_reason"],
            "source_checked_with": row["source_checked_with"],
            "verifier_remark": row["verifier_remark"],
            "linked_reference": row["linked_reference"],
            "created_at": _parse_datetime(row["created_at"]),
            "updated_at": _parse_datetime(row["updated_at"]),
        }
        for row in rows
    ]


def _prepare_case_event_documents(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "event_id": row["event_id"],
            "cif_id": row["cif_id"],
            "loan_id": row.get("loan_id"),
            "case_family": row["case_family"],
            "event_type": row["event_type"],
            "severity": row["severity"],
            "status": row["status"],
            "summary": row["summary"],
            "occurred_at": _parse_datetime(row["occurred_at"]),
            "linked_txn_ids": list(row.get("linked_txn_ids") or []),
            "linked_document_ids": list(row.get("linked_document_ids") or []),
            "outcome_note": row.get("outcome_note") or "",
            "created_at": _parse_datetime(row["created_at"]),
            "updated_at": _parse_datetime(row["updated_at"]),
        }
        for row in rows
    ]


def run_seed() -> dict[str, Any]:
    generate_seed_files()

    customer_rows = _load_seed_file(CUSTOMERS_FILE)
    transaction_rows = _load_seed_file(TRANSACTIONS_FILE)
    loan_account_rows = _load_seed_file(LOAN_ACCOUNTS_FILE)
    collateral_rows = _load_seed_file(COLLATERAL_RECORDS_FILE)
    document_verification_rows = _load_seed_file(DOCUMENT_VERIFICATIONS_FILE)
    case_event_rows = _load_seed_file(CASE_EVENTS_FILE)

    customer_documents = _prepare_customer_documents(customer_rows)
    transaction_documents = _prepare_transaction_documents(transaction_rows)
    loan_account_documents = _prepare_loan_account_documents(loan_account_rows)
    collateral_documents = _prepare_collateral_documents(collateral_rows)
    document_verification_documents = _prepare_document_verification_documents(document_verification_rows)
    case_event_documents = _prepare_case_event_documents(case_event_rows)

    database = get_banking_database()
    for collection_name in [
        CUSTOMERS_COLLECTION_NAME,
        TRANSACTIONS_COLLECTION_NAME,
        LOAN_ACCOUNTS_COLLECTION_NAME,
        COLLATERAL_RECORDS_COLLECTION_NAME,
        DOCUMENT_VERIFICATIONS_COLLECTION_NAME,
        CASE_EVENTS_COLLECTION_NAME,
    ]:
        database.drop_collection(collection_name)

    ensure_banking_indexes(database)

    customer_result = database[CUSTOMERS_COLLECTION_NAME].insert_many(customer_documents, ordered=True)
    transaction_result = database[TRANSACTIONS_COLLECTION_NAME].insert_many(transaction_documents, ordered=True)
    loan_account_result = database[LOAN_ACCOUNTS_COLLECTION_NAME].insert_many(loan_account_documents, ordered=True)
    collateral_result = database[COLLATERAL_RECORDS_COLLECTION_NAME].insert_many(collateral_documents, ordered=True)
    document_verification_result = database[DOCUMENT_VERIFICATIONS_COLLECTION_NAME].insert_many(
        document_verification_documents,
        ordered=True,
    )
    case_event_result = database[CASE_EVENTS_COLLECTION_NAME].insert_many(case_event_documents, ordered=True)

    snapshot = get_banking_snapshot(database)
    return {
        "mongo_host": str(snapshot["mongo_host"]),
        "database_name": str(snapshot["database_name"]),
        "collections": list(snapshot["collections"]),
        "customers_inserted": len(customer_result.inserted_ids),
        "transactions_inserted": len(transaction_result.inserted_ids),
        "loan_accounts_inserted": len(loan_account_result.inserted_ids),
        "collateral_records_inserted": len(collateral_result.inserted_ids),
        "document_verifications_inserted": len(document_verification_result.inserted_ids),
        "case_events_inserted": len(case_event_result.inserted_ids),
    }


if __name__ == "__main__":
    print(json.dumps(run_seed(), indent=2))
