import io
import pandas as pd
from fastapi import APIRouter, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from app.services.pdf_processor import process_pdf
from app.services.csv_processor import process_csv
from app.services.xlsx_processor import process_xlsx
from app.services.mongo_helpers import check_if_document_name_exists
from fastapi import File

router = APIRouter()

@router.post("/upload-file/")
async def upload_file(file: UploadFile = File(...)):
    """
    This endpoint handles PDF, CSV, and XLSX files and processes them accordingly.
    """
    print("Received a file upload request")

    # Determine file type from content type or extension
    file_extension = file.filename.split('.')[-1].lower()

    # Check for supported file types
    if file_extension not in ['pdf', 'csv', 'xlsx']:
        raise HTTPException(status_code=400, detail="Only PDF, CSV, or XLSX files are supported")

    try:
        # Extract document name and check for duplicates
        document_name = file.filename if file.filename else "unknown"
        if check_if_document_name_exists(document_name):
            raise HTTPException(status_code=400, detail=f"Document with name '{document_name}' already exists")

        # Read the file content
        file_stream = io.BytesIO(await file.read())

        # Process the file based on its type
        if file_extension == "pdf":
            print(f"Processing PDF: {document_name}")
            await process_pdf(file_stream, document_name)

        elif file_extension == "csv":
            print(f"Processing CSV: {document_name}")
            await process_csv(file_stream, document_name)

        elif file_extension == "xlsx":
            print(f"Processing XLSX: {document_name}")
            await process_xlsx(file_stream, document_name)

        return JSONResponse(status_code=200, content={"message": f"{file_extension.upper()} file was uploaded and processed successfully"})

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing {file_extension.upper()} file: {str(e)}")
