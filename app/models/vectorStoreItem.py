from pydantic import BaseModel
from typing import List

class VectorStoreItem(BaseModel):
    id : str
    original_text : str
    contextual_text : str
    document_name : str
    document_id : str
    page_number : str
    vector_embeddings : List[float]

