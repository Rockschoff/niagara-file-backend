import pandas as pd
from uuid import uuid4
import asyncio
from app.services.mongo_helpers import upload_item_to_mongodb
from app.models.vectorStoreItem import VectorStoreItem
from app.services.ai_helpers import get_contextual_chunk, get_embeddings, get_sheet_description

async def process_xlsx(file_stream, document_name=""):
    try:
        xls = pd.ExcelFile(file_stream)
        document_id = uuid4()

        tasks = []

        # Iterate through each sheet in the Excel file
        for sheet_name in xls.sheet_names:
            print(f"Processing sheet: {sheet_name}")
            sheet_df = xls.parse(sheet_name)
            rows = sheet_df.values.tolist()  # Convert DataFrame rows to list
            header = sheet_df.columns.tolist()  # Extract column names

            total_rows = len(rows)
            chunks = [rows[i:i + 25] for i in range(0, total_rows, 25)]

            # Context per sheet, function is kept empty as per the specification.
            context = get_context(sheet_name , sheet_df)  

            for chunk_num, chunk in enumerate(chunks):
                # Add the header back for each chunk
                chunk_with_header = [header] + chunk

                # Create tasks for contextual chunks and embeddings
                contextual_chunks_task = asyncio.gather(*[get_contextual_chunk(context=context, chunk=row) for row in chunk_with_header])
                embeddings_task = asyncio.gather(*[get_embeddings(chunk=row) for row in chunk_with_header])

                tasks.append((contextual_chunks_task, embeddings_task, chunk_num, chunk_with_header, document_id, document_name, sheet_name))

        for contextual_chunks_task, embeddings_task, chunk_num, chunk_with_header, document_id, document_name, sheet_name in tasks:
            contextual_chunks = await contextual_chunks_task
            embeddings = await embeddings_task

            vector_store_items = [
                VectorStoreItem(
                    id=str(uuid4()),
                    original_text=str(row),
                    contextual_text=contextual_chunk,
                    document_id=str(document_id),
                    page_number=f"{sheet_name}_{chunk_num}",  # Use sheet_name and chunk_num for page equivalent
                    vector_embeddings=embedding,
                    document_name=document_name
                )
                for row, contextual_chunk, embedding in zip(chunk_with_header, contextual_chunks, embeddings)
            ]

            for item in vector_store_items:
                await upload_item_to_mongodb(item)

    except Exception as e:
        raise Exception(f"Error processing XLSX: {str(e)}")

def get_context(sheet_name, sheet_df):
    """
    Generates a description of the sheet by extracting the first 5 rows and column names.
    """
    # Extract the first 5 rows from the DataFrame
    first_5_rows = sheet_df.head(5)
    
    # Convert the DataFrame (with headers) to a string
    first_5_rows_with_headers = pd.concat([pd.DataFrame([sheet_df.columns], columns=sheet_df.columns), first_5_rows])
    first_5_row_with_col_names_str_format = first_5_rows_with_headers.to_string(index=False)
    
    # Get context by passing the string format to `get_sheet_description`
    return get_sheet_description(first_5_row=first_5_row_with_col_names_str_format)
