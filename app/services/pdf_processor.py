import PyPDF2
from uuid import uuid4
import asyncio
from app.services.mongo_helpers import upload_item_to_mongodb
from app.models.vectorStoreItem import VectorStoreItem
from app.services.ai_helpers import get_contextual_chunk, get_embeddings

async def process_pdf(file_stream, document_name=""):
    print(f"Processing document: {document_name}")
    try:
        reader = PyPDF2.PdfReader(file_stream)
        print("PDF successfully read.")
        
        document_id = uuid4()
        print(f"Generated document ID: {document_id}")
        
        total_pages = len(reader.pages)
        print(f"Total pages in document: {total_pages}")

        tasks = []

        for page_num in range(total_pages):
            print(f"Processing page: {page_num + 1}/{total_pages}")
            page = reader.pages[page_num]
            
            text = page.extract_text()
            if not text:
                print(f"No text found on page {page_num + 1}. Skipping.")
                continue

            print(f"Text extracted from page {page_num + 1}: {text[:100]}...")

            chunks = split_into_chunks(text)
            print(f"Text split into {len(chunks)} chunks.")

            context = get_page_context(reader, page_num)
            print(f"Context for page {page_num + 1} obtained.")

            contextual_chunks_task = asyncio.gather(*[get_contextual_chunk(context=context, chunk=chunk) for chunk in chunks])
            embeddings_task = asyncio.gather(*[get_embeddings(chunk=chunk) for chunk in chunks])

            tasks.append((contextual_chunks_task, embeddings_task, page_num, chunks, document_id, document_name))

        for contextual_chunks_task, embeddings_task, page_num, chunks, document_id, document_name in tasks:
            print(f"Awaiting contextual chunks and embeddings for page {page_num + 1}.")
            contextual_chunks = await contextual_chunks_task
            embeddings = await embeddings_task

            print(f"Contextual chunks received for page {page_num + 1}.")
            print(f"Embeddings received for page {page_num + 1}.")

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

            print(f"Generated {len(vector_store_items)} vector store items for page {page_num + 1}. Uploading to MongoDB.")

            for item in vector_store_items:
                await upload_item_to_mongodb(item)
                print(f"Uploaded item {item.id} to MongoDB.")

    except Exception as e:
        print(f"Error processing PDF: {str(e)}")
        raise Exception(f"Error processing PDF: {str(e)}")

def split_into_chunks(text, chunk_size=5000):
    print(f"Splitting text into chunks of size {chunk_size}.")
    chunks = [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]
    print(f"Generated {len(chunks)} chunks.")
    return chunks

def get_page_context(reader, page_num, window=2):
    print(f"Generating context for page {page_num} with window size {window}.")
    start = max(0, page_num - window)
    end = min(len(reader.pages), page_num + window + 1)
    context = "".join(reader.pages[i].extract_text() for i in range(start, end))
    print(f"Context for page {page_num} generated.")
    return context
