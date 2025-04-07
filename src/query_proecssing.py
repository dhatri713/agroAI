import os
import requests
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

ENV_PATH = "../.env"
load_dotenv(ENV_PATH)

class QueryProcessor:
    def __init__(self):
        self.groq_api_key = os.getenv("GROQ_API_KEY")
        if not self.groq_api_key:
            raise ValueError("Groq API key not found in .env")

        self.groq_url = "https://api.groq.com/openai/v1/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {self.groq_api_key}",
            "Content-Type": "application/json",
        }
        self.embedding_model = SentenceTransformer("sentence-transformers/paraphrase-mpnet-base-v2")

    def expand_query(self, query: str) -> str:
        system_prompt = "You are an AI assistant that expands short user queries from farmers with related words along with existing words so that it is optimised for better retrieval for my QA system respond in a clear single sentence only. Dont write anything extra. Just the expanded query that will directly be passed for embedding. Please remember that important words in the query need to be emphasised. In some queries, they might mention symptoms, and could or could not ask for tratment methods. In those cases, Symptoms and treatment methods should be emphasised."

        user_prompt = f"Expand this query in detail: {query}"

        payload = {
            "model": "llama3-8b-8192",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.2,
        }

        response = requests.post(self.groq_url, headers=self.headers, json=payload)

        if response.status_code == 200:
            return response.json()["choices"][0]["message"]["content"].strip()
        else:
            raise Exception(f"Groq API Error: {response.status_code} - {response.text}")

    def get_query_embedding(self, text: str) -> list:
        return self.embedding_model.encode(text).tolist()

# if __name__ == "__main__":
#     qp = QueryProcessor()
#     user_query = "leaves turning yellow with brown spots"

#     expanded_query = qp.expand_query(user_query)
#     embedding = qp.get_query_embedding(expanded_query)

#     print("\nOriginal Query:", user_query)
#     print("\nExpanded Query:", expanded_query)
#     print("\nEmbedding:", embedding[:5], "...")