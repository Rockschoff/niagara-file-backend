import PyPDF2
from uuid import uuid4
import asyncio
from app.services.mongo_helpers import upload_item_to_mongodb
from app.models.vectorStoreItem import VectorStoreItem
from app.services.ai_helpers import get_contextual_chunk, get_embeddings

async def process_pdf(file_stream, document_name=""):
    print(document_name)
    try:
        reader = PyPDF2.PdfReader(file_stream)
        document_id = uuid4()
        total_pages = len(reader.pages)

        tasks = []

        for page_num in range(total_pages):
            page = reader.pages[page_num]
            text = page.extract_text()

            if not text:
                continue

            chunks = split_into_chunks(text)
            context = get_page_context(reader, page_num)

            contextual_chunks_task = asyncio.gather(*[get_contextual_chunk(context=context, chunk=chunk) for chunk in chunks])
            embeddings_task = asyncio.gather(*[get_embeddings(chunk=chunk) for chunk in chunks])

            tasks.append((contextual_chunks_task, embeddings_task, page_num, chunks, document_id, document_name))

        for contextual_chunks_task, embeddings_task, page_num, chunks, document_id, document_name in tasks:
            contextual_chunks = await contextual_chunks_task
            embeddings = await embeddings_task

            vector_store_items = [
                VectorStoreItem(
                    id=str(uuid4()),
                    original_text=chunk,
                    contextual_text=contextual_chunk,
                    document_id=str(document_id),
                    page_number=str(page_num),
                    vector_embeddings=embedding,
                    document_name=document_name
                )
                for chunk, contextual_chunk, embedding in zip(chunks, contextual_chunks, embeddings)
            ]

            for item in vector_store_items:
                await upload_item_to_mongodb(item)

    except Exception as e:
        raise Exception(f"Error processing PDF: {str(e)}")

def split_into_chunks(text, chunk_size=5000):
    return [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]

def get_page_context(reader, page_num, window=2):
    start = max(0, page_num - window)
    end = min(len(reader.pages), page_num + window + 1)
    return "".join(reader.pages[i].extract_text() for i in range(start, end))
