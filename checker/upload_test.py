import os
import requests
import time
import boto3
from pymongo import MongoClient
from checker.config import MONGO_DB_DATABASE_NAME, MONGO_DB_URI, MONGO_DB_COLLECTION_NAME
from functools import lru_cache

# Initialize MongoDB and S3 clients
client = MongoClient(MONGO_DB_URI)
db = client[MONGO_DB_DATABASE_NAME]
collection = db[MONGO_DB_COLLECTION_NAME]
s3 = boto3.client('s3')
bucket_name = 'niagara-docs-folder'
directory_path = "./niagara-docs-folder"
os.makedirs(directory_path, exist_ok=True)

# Cache MongoDB document names with a TTL of 5 minutes
@lru_cache(maxsize=1)
def get_mongo_document_names():
    mongo_docs = set(item["document_name"] for item in collection.find({}, {"document_name": 1}))
    return mongo_docs

# Refresh MongoDB document names every 5 minutes
def refresh_mongo_document_names():
    get_mongo_document_names.cache_clear()
    return get_mongo_document_names()

# Get mapping of base filenames to full S3 keys
def get_s3_document_names():
    response = s3.list_objects_v2(Bucket=bucket_name)
    if 'Contents' in response:
        return {os.path.basename(obj['Key']): obj['Key'] for obj in response['Contents'] if (obj['Key'].endswith(".xlsx") or obj['Key'].endswith(".pdf"))}
    return {}

# Bulk delete MongoDB documents missing in S3
def delete_mongo_documents_not_in_s3(mongo_docs, s3_doc_names_set):
    documents_to_delete = [{"document_name": doc_name} for doc_name in mongo_docs - s3_doc_names_set]
    if documents_to_delete:
        delete_count = collection.delete_many({"$or": documents_to_delete}).deleted_count
        print(f"Deleted {delete_count} MongoDB entries for documents not found in S3.")

# Download missing S3 documents and upload to FastAPI
def upload_to_fastapi(file_path, filename):
    url = "http://localhost:8888/api/upload-file/"
    if filename.endswith(".xlsx"):
        mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    elif filename.endswith(".pdf"):
        mime_type = "application/pdf"
    with open(file_path, "rb") as file:
        files = {"file": (filename, file, mime_type)}
        response = requests.post(url, files=files)
        if response.status_code == 200:
            print(f"Uploaded {filename} successfully.")
        else:
            print(f"Failed to upload {filename}: Status {response.status_code}")

def download_and_upload_missing_s3_documents(mongo_docs, s3_docs):
    for doc_name in s3_docs.keys() - mongo_docs:
        local_file_path = os.path.join(directory_path, doc_name)
        s3_key = s3_docs[doc_name]  # Get the full S3 key
        # Download file from S3 if it doesn't exist locally
        if not os.path.exists(local_file_path):
            print(f"Downloading {doc_name} from S3...")
            s3.download_file(bucket_name, s3_key, local_file_path)
        # Upload to FastAPI
        upload_to_fastapi(local_file_path, doc_name)

# Cleanup function to clear local directory files
def clear_local_directory():
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"Cleared local file: {filename}")
        except Exception as e:
            print(f"Error deleting file {filename}: {e}")

# Main loop with optimized MongoDB calls
def main_sync_loop():
    while True:
        print("Starting MongoDB-S3-FastAPI sync...\n")
        # Step 0: Clear local directory
        clear_local_directory()
        # Refresh cached MongoDB document names every 5 minutes
        mongo_docs = refresh_mongo_document_names()
        s3_docs = get_s3_document_names()
        s3_doc_names_set = set(s3_docs.keys())
        # Delete MongoDB entries not in S3
        delete_mongo_documents_not_in_s3(mongo_docs, s3_doc_names_set)
        # Download and upload missing S3 documents
        download_and_upload_missing_s3_documents(mongo_docs, s3_docs)
        print("Sync cycle complete. Waiting 10 seconds before next cycle.\n")
        time.sleep(10)

if __name__ == "__main__":
    main_sync_loop()
