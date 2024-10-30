import csv
from uuid import uuid4
import asyncio
from app.services.mongo_helpers import upload_item_to_mongodb
from app.models.vectorStoreItem import VectorStoreItem
from app.services.ai_helpers import get_contextual_chunk, get_embeddings, get_sheet_description


async def process_csv(file_stream, document_name=""):
    try:
        reader = csv.reader(file_stream.decode('utf-8').splitlines())
        document_id = uuid4()
        rows = list(reader)
        header = rows[0]  # The header row
        total_rows = len(rows)
        
        # Chunks are created by taking 25 consecutive rows, including the header each time.
        chunks = [rows[i:i + 25] for i in range(0, total_rows, 25)]
        tasks = []

        # Context is constant for the entire CSV, function is left empty as per the requirement.
        context = get_context(rows)

        for chunk_num, chunk in enumerate(chunks):
            # Re-adding the header for each chunk
            chunk_with_header = [header] + chunk[1:]

            # Create tasks for contextual chunks and embeddings
            contextual_chunks_task = asyncio.gather(*[get_contextual_chunk(context=context, chunk=row) for row in chunk_with_header])
            embeddings_task = asyncio.gather(*[get_embeddings(chunk=row) for row in chunk_with_header])

            tasks.append((contextual_chunks_task, embeddings_task, chunk_num, chunk_with_header, document_id, document_name))

        for contextual_chunks_task, embeddings_task, chunk_num, chunk_with_header, document_id, document_name in tasks:
            contextual_chunks = await contextual_chunks_task
            embeddings = await embeddings_task

            vector_store_items = [
                VectorStoreItem(
                    id=str(uuid4()),
                    original_text=str(row),
                    contextual_text=contextual_chunk,
                    document_id=str(document_id),
                    page_number=str(chunk_num),  # Using chunk number as page equivalent
                    vector_embeddings=embedding,
                    document_name=document_name
                )
                for row, contextual_chunk, embedding in zip(chunk_with_header, contextual_chunks, embeddings)
            ]

            for item in vector_store_items:
                await upload_item_to_mongodb(item)

    except Exception as e:
        raise Exception(f"Error processing CSV: {str(e)}")

def get_context(rows):
    """
    Generates a description of the CSV by extracting the first 5 rows and column names.
    """
    # Extract the first 5 rows from the CSV (including the header)
    header = rows[0]
    first_5_rows = rows[1:6]  # Skipping the header and getting the first 5 rows

    # Convert the header and the first 5 rows to a string
    first_5_row_with_col_names_str_format = "\n".join([",".join(header)] + [",".join(row) for row in first_5_rows])

    # Get context by passing the string format to `get_sheet_description`
    return get_sheet_description(first_5_row=first_5_row_with_col_names_str_format)
