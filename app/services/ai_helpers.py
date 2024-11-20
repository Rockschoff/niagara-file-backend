from openai import OpenAI
from typing import List
from checker.config import OPENAI_API_KEY
client = OpenAI(api_key=OPENAI_API_KEY)



async def get_sheet_description(first_5_row : str):

    prompt = f"Please describe this table, here the first 5 rows : {first_5_row}"

    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    return completion.choices[0].message.content



async def get_contextual_chunk(context , chunk)->str:

    prompt = f"""<context>{context}</context>
    <chunk>{chunk}</chunk>
    Please give a short succinct context to situate this chunk within the overall document for the purposes of improving search retrieval of the chunk. Answer only with the succinct context and nothing else.Inlcude key words that will help the Food Safety and Quality Professional Search for the chunk efficiently"""

    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    return completion.choices[0].message.content

async def get_embeddings(chunk)->List[float]:

    response = client.embeddings.create(
        model="text-embedding-ada-002",
        input=chunk,
        encoding_format="float"
        )
    return response.data[0].embedding