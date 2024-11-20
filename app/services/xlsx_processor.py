import pandas as pd
from uuid import uuid4
import asyncio
from app.services.mongo_helpers import upload_item_to_mongodb
from app.models.vectorStoreItem import VectorStoreItem
from app.services.ai_helpers import get_contextual_chunk, get_embeddings, get_sheet_description

CHUNK_SIZE=10

async def process_chunk(context, chunk_text, document_id, sheet_name, chunk_num, document_name, progress):
    try:
        # Process contextual chunk and embeddings in parallel
        contextual_chunk, embedding = await asyncio.gather(
            get_contextual_chunk(context=context, chunk=chunk_text),
            get_embeddings(chunk=chunk_text)
        )

        # Create and upload VectorStoreItem
        vector_store_item = VectorStoreItem(
            id=str(uuid4()),
            original_text=chunk_text,
            contextual_text=contextual_chunk,
            document_id=str(document_id),
            page_number=f"{sheet_name}_{chunk_num}",
            vector_embeddings=embedding,
            document_name=document_name
        )
        await upload_item_to_mongodb(vector_store_item)

        # Update progress and print
        progress["completed"] += 1
        print(f"Completed {progress['completed']} out of {progress['total']} tasks.")

    except Exception as e:
        print(f"Error processing chunk {sheet_name}_{chunk_num}: {e}")


async def process_xlsx(file_stream, document_name=""):
    try:
        # Load the Excel file
        xls = pd.ExcelFile(file_stream)
        document_id = uuid4()

        tasks = []
        progress = {"completed": 0, "total": 0}  # Track progress

        # Iterate through each sheet in the Excel file
        for sheet_name in xls.sheet_names:
            print(f"Processing sheet: {sheet_name}")
            sheet_df = xls.parse(sheet_name)

            # Extract header and rows
            header = sheet_df.columns.tolist()
            rows = sheet_df.values.tolist()
            total_rows = len(rows)

            # Divide rows into chunks of 25 rows each
            chunks = [rows[i:i + CHUNK_SIZE] for i in range(0, total_rows, CHUNK_SIZE)]

            # Generate context for the sheet
            context = get_context(sheet_name, sheet_df)

            # Create tasks for each chunk
            for chunk_num, chunk in enumerate(chunks):
                # Combine the header and chunk into a single piece of text
                chunk_with_header = [header] + chunk
                chunk_text = "\n".join([", ".join(map(str, row)) for row in chunk_with_header])

                # Add a task for processing the chunk
                tasks.append(process_chunk(
                    context=context,
                    chunk_text=chunk_text,
                    document_id=document_id,
                    sheet_name=sheet_name,
                    chunk_num=chunk_num,
                    document_name=document_name,
                    progress=progress
                ))

        # Update total number of tasks
        progress["total"] = len(tasks)

        # Run all tasks concurrently
        await asyncio.gather(*tasks)

        print("All tasks completed.")

    except Exception as e:
        raise Exception(f"Error processing XLSX: {str(e)}")


def get_context(sheet_name, sheet_df):
    """
    Generates a description of the sheet by extracting the first 5 rows and column names.
    """
    # Extract the first 5 rows from the DataFrame
    first_5_rows = sheet_df.head(5)
    
    # Combine headers and first 5 rows into a single DataFrame
    first_5_rows_with_headers = pd.concat([pd.DataFrame([sheet_df.columns], columns=sheet_df.columns), first_5_rows])
    first_5_row_with_col_names_str_format = first_5_rows_with_headers.to_string(index=False)
    
    # Get context by passing the string format to `get_sheet_description`
    return get_sheet_description(first_5_row=first_5_row_with_col_names_str_format)
