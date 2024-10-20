from pydantic import BaseModel
class DeleteDocumentRequest(BaseModel):
    input_str: str  # This can be either document_name or document_id
