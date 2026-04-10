import io
import os
import uuid
from copy import deepcopy

from app.core.config import AXIS_BANK_ID, AXIS_BANK_NAME


def _matches(document, query):
    query = query or {}

    for key, value in query.items():
        if key == "$or":
            if not any(_matches(document, clause) for clause in value):
                return False
            continue

        current = document.get(key)

        if isinstance(value, dict):
            if "$exists" in value:
                exists = key in document and document.get(key) not in (None, "")
                if exists != bool(value["$exists"]):
                    return False
                continue

            if "$ne" in value:
                if current == value["$ne"]:
                    return False
                continue

            if "$regex" in value:
                import re

                pattern = value["$regex"]
                if current is None or not re.search(pattern, str(current)):
                    return False
                continue

        if current != value:
            return False

    return True


class InMemoryCollection:
    def __init__(self):
        self._documents = []

    def create_index(self, *_args, **_kwargs):
        return None

    def insert_one(self, document):
        stored = deepcopy(document)
        stored.setdefault("_id", uuid.uuid4().hex)
        self._documents.append(stored)
        return stored

    def find_one(self, query=None, sort=None):
        matches = [doc for doc in self._documents if _matches(doc, query or {})]

        if sort:
            for field, direction in reversed(sort):
                matches.sort(key=lambda item: item.get(field), reverse=direction == -1)

        return deepcopy(matches[0]) if matches else None

    def find(self, query=None):
        for document in self._documents:
            if _matches(document, query or {}):
                yield deepcopy(document)

    def update_one(self, query, update, upsert=False):
        for index, document in enumerate(self._documents):
            if _matches(document, query):
                updated = deepcopy(document)
                updated.update(deepcopy(update.get("$set", {})))
                self._documents[index] = updated
                return updated

        if upsert:
            inserted = deepcopy(query)
            inserted.update(deepcopy(update.get("$setOnInsert", {})))
            inserted.update(deepcopy(update.get("$set", {})))
            return self.insert_one(inserted)

        return None

    def delete_many(self, query):
        before_count = len(self._documents)
        self._documents = [doc for doc in self._documents if not _matches(doc, query)]
        return before_count - len(self._documents)


class InMemoryGridFile(io.BytesIO):
    def __init__(self, content, filename):
        super().__init__(content)
        self.filename = filename


class InMemoryGridFS:
    def __init__(self):
        self._files = {}

    def put(self, file_obj, filename, bankId, file_id=None):
        resolved_file_id = str(file_id or uuid.uuid4().hex)
        self._files[resolved_file_id] = {
            "_id": resolved_file_id,
            "filename": filename,
            "bankId": bankId,
            "content": file_obj.read(),
        }
        return resolved_file_id

    def find_one(self, query):
        for file_data in self._files.values():
            if _matches(file_data, query):
                return type("GridFSMatch", (), deepcopy(file_data))
        return None

    def get(self, file_id):
        file_data = self._files.get(str(file_id))
        if not file_data:
            raise KeyError(file_id)
        return InMemoryGridFile(file_data["content"], file_data["filename"])


users_collection = InMemoryCollection()
documents_collection = InMemoryCollection()
cases_collection = InMemoryCollection()
chat_logs_collection = InMemoryCollection()
fs = InMemoryGridFS()


def _seed_default_user():
    default_user_id = os.getenv("AXIS_DEFAULT_USER_ID", "axis001")
    default_password = os.getenv("AXIS_DEFAULT_PASSWORD", "0000")

    if users_collection.find_one({"userId": default_user_id}):
        return

    users_collection.insert_one(
        {
            "userId": default_user_id,
            "password": default_password,
            "bankId": AXIS_BANK_ID,
            "bankName": AXIS_BANK_NAME,
            "role": "investigator",
        }
    )


_seed_default_user()
