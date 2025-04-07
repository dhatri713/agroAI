import os
from typing import List, Dict
from groq import Groq
from dotenv import load_dotenv

ENV_PATH = "../.env"
load_dotenv(ENV_PATH)

class AnswerGenerator:
    def __init__(self, model_name: str = "llama3-8b-8192"):
        self.client = Groq(api_key=os.getenv("GROQ_API_KEY"))
        self.model_name = model_name
    
    def build_prompt(self, query: str, retrieved_docs: List[Dict]) -> str:
        context = "\n\n".join([doc['text'] for doc in retrieved_docs])
        prompt = f"""You are an expert agricultural assistant.

        Use the following information to answer the user's query in a simple and accurate manner. Try to use as much as the information possible based on the user query. This is for the betterment of farming practices.

        Context:
        {context}

        User Query:
        {query}

        Answer:"""
        return prompt

    def generate_answer(self, query: str, retrieved_docs: List[Dict]) -> str:
        prompt = self.build_prompt(query, retrieved_docs)
        response = self.client.chat.completions.create(
            model=self.model_name,
            messages=[{"role": "user", "content": prompt}]
        )

        return response.choices[0].message.content.strip()

# if __name__ == "__main__":
#     import json
#     from vector_store import ChromaVectorStore
#     from data_processing import DataProcessor
#     from query_proecssing import   QueryProcessor
#     from retriever_and_ranker import RetrieverAndRanker
#     from sentence_transformers import SentenceTransformer

#     processor = DataProcessor()
#     data_file = "../data/bacterial_wilt_chilli.json"
#     with open(data_file, "r") as f:
#         raw_data = json.load(f)
#     chunks = processor.process_files(data_file)
    
#     # Step 1: Initialize the embedding model
#     print("Initializing sentence transformer model...")
#     embedder = SentenceTransformer("paraphrase-mpnet-base-v2")
    
#     # Step 2: Load vector store
#     print("Loading vector store...")
#     vector_store = ChromaVectorStore(
#         persist_directory="../chroma_db",
#         collection_name="test_chilli",
#         embedding_function=embedder
#     )
#     vector_store.add_documents(chunks)
    
#     # Check if collection is empty
#     if vector_store.collection.count() == 0:
#         print("WARNING: Collection is empty! No documents to retrieve.")
#         exit(1)

#     # Step 3: Instantiate retriever and ranker
#     print("Initializing retriever...")
#     retriever = RetrieverAndRanker(vector_store)

#     # Step 4: Example user query
#     qp = QueryProcessor()
#     user_query = "leaves turning yellow with brown spots give treatment methods"

#     expanded_query = qp.expand_query(user_query)
#     embedding = qp.get_query_embedding(expanded_query)

#     # Step 6: Retrieve & rerank
#     print("Retrieving and re-ranking results...")
#     try:
#         results = retriever.retrieve_and_rerank(query_embedding=embedding, top_k=5)
        
#         print("\nFinal Retrieved Results:")
#         if not results:
#             print("No results found.")
#         else:
#             for idx, item in enumerate(results, 1):
#                 print(f"{idx}. Score: {item['score']:.4f} - {item['text'][:100]}...")

#             # Print metadata for the best result
#             print("\nBest match metadata:")
#             print(results[0]['metadata'])
            
#     except Exception as e:
#         print(f"Error during retrieval: {e}")
#         import traceback
#         traceback.print_exc()

#     generator = AnswerGenerator()
#     answer = generator.generate_answer(user_query, results)
#     print("Generated Answer:\n", answer)
