from __future__ import annotations

import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from app.db.banking import (
    CUSTOMERS_COLLECTION_NAME,
    TRANSACTIONS_COLLECTION_NAME,
    ensure_banking_indexes,
    get_banking_database,
    get_banking_snapshot,
)


SEED_DIR = Path(__file__).resolve().parent
CUSTOMERS_FILE = SEED_DIR / "customers.json"
TRANSACTIONS_FILE = SEED_DIR / "transactions.json"

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


def _format_datetime(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _parse_datetime(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


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


def _validate_seed_payload(customers: list[dict[str, Any]], transactions: list[dict[str, Any]]) -> None:
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


def generate_seed_files() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    now = datetime.now().replace(second=0, microsecond=0)
    rng = random.Random(now.strftime("%Y%m%d%H%M"))

    customers = _generate_customer_records(rng, now)
    transactions = _generate_transaction_records(customers, rng, now)
    _validate_seed_payload(customers, transactions)

    _write_json(CUSTOMERS_FILE, customers)
    _write_json(TRANSACTIONS_FILE, transactions)

    return customers, transactions


def _load_seed_file(path: Path) -> list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def _prepare_customer_documents(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []

    for row in rows:
        documents.append(
            {
                "cif_id": row["cif_id"],
                "name": row["name"],
                "mobile": row["mobile"],
                "pan": row["pan"],
                "accounts": row["accounts"],
                "created_at": _parse_datetime(row["created_at"]),
                "updated_at": _parse_datetime(row["updated_at"]),
            }
        )

    return documents


def _prepare_transaction_documents(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []

    for row in rows:
        documents.append(
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
        )

    return documents


def run_seed() -> dict[str, int]:
    generate_seed_files()

    customer_rows = _load_seed_file(CUSTOMERS_FILE)
    transaction_rows = _load_seed_file(TRANSACTIONS_FILE)

    customer_documents = _prepare_customer_documents(customer_rows)
    transaction_documents = _prepare_transaction_documents(transaction_rows)

    database = get_banking_database()
    database.drop_collection(CUSTOMERS_COLLECTION_NAME)
    database.drop_collection(TRANSACTIONS_COLLECTION_NAME)
    ensure_banking_indexes(database)

    customer_result = database[CUSTOMERS_COLLECTION_NAME].insert_many(customer_documents, ordered=True)
    transaction_result = database[TRANSACTIONS_COLLECTION_NAME].insert_many(transaction_documents, ordered=True)

    snapshot = get_banking_snapshot(database)

    return {
        "mongo_host": str(snapshot["mongo_host"]),
        "database_name": str(snapshot["database_name"]),
        "collections": list(snapshot["collections"]),
        "customers_inserted": len(customer_result.inserted_ids),
        "transactions_inserted": len(transaction_result.inserted_ids),
    }


if __name__ == "__main__":
    print(json.dumps(run_seed(), indent=2))
