from fastapi import APIRouter, HTTPException
from app.models.deleteRequest import DeleteDocumentRequest
from app.services.mongo_helpers import delete_all_items_with_name_or_id

router = APIRouter()

@router.post("/delete-document/")
async def delete_document(request: DeleteDocumentRequest):
    try:
        delete_count = delete_all_items_with_name_or_id(request.input_str)

        if delete_count > 0:
            return {"message": f"Successfully deleted {delete_count} documents with document_name or document_id: {request.input_str}"}
        else:
            return {"message": f"No documents found with document_name or document_id: {request.input_str}"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting document: {str(e)}")
