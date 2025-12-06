# utils_embeddings.py
import openai
import numpy as np

# Initialize OpenAI
openai_api_key = "YOUR_OPENAI_API_KEY"

def get_text_embedding(text: str):
    """
    Generates a 3072-dim embedding vector using OpenAI 'text-embedding-3-large'.
    """
    if text is None or len(text.strip()) == 0:
        return np.zeros(3072)

    client = openai.OpenAI(api_key=openai_api_key)

    embedding = client.embeddings.create(
        model="text-embedding-3-large",
        input=text
    ).data[0].embedding

    return np.array(embedding, dtype=float)
