from pymongo import MongoClient
from app.models.vectorStoreItem import VectorStoreItem
from checker.config import MONGO_DB_DATABASE_NAME, MONGO_DB_URI, MONGO_DB_COLLECTION_NAME
import os

client = MongoClient(MONGO_DB_URI)
db = client[MONGO_DB_DATABASE_NAME]
collection = db[MONGO_DB_COLLECTION_NAME]

def check_if_document_name_exists(document_name: str) -> bool:
    result = collection.find_one({"document_name": document_name})
    return result is not None

async def upload_item_to_mongodb(item: VectorStoreItem):
    item_dict = item.dict()
    collection.insert_one(item_dict)

def delete_all_items_with_name_or_id(input_str: str) -> int:
    delete_result = collection.delete_many({
        "$or": [
            {"document_name": input_str},
            {"document_id": input_str}
        ]
    })
    return delete_result.deleted_count
