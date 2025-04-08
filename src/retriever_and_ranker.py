import numpy as np
from typing import List, Dict, Any
import warnings
warnings.filterwarnings("ignore")

class RetrieverAndRanker:
    def __init__(self, vector_store):
        # vector_store: instance of your ChromaVectorStore class
        self.collection = vector_store.collection
        # print(f"Connected to collection with {self.collection.count()} documents")

    def cosine_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))
    
    def retrieve_and_rerank(self, query_embedding: List[float], top_k: int = 5) -> List[Dict[str, Any]]:
        # print(f"Retrieving top {top_k} documents...")
        
        # Query the collection - add include parameter to get embeddings
        results = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=min(top_k, self.collection.count()),  # Make sure we don't request more than available
            include=["embeddings", "documents", "metadatas", "distances"]
        )
        
        # print(f"Query returned {len(results['ids'][0])} documents")
        
        # If no results returned, return empty list
        if not results['ids'][0]:
            print("No documents found matching the query")
            return []
            
        # Get embeddings from retrieved results
        retrieved_embeddings = results['embeddings'][0]
        documents = results['documents'][0]
        metadatas = results['metadatas'][0]
        ids = results['ids'][0]
        
        # print(f"Re-ranking {len(documents)} documents")
        
        # Re-rank using cosine similarity
        ranked = []
        for idx, emb in enumerate(retrieved_embeddings):
            score = self.cosine_similarity(np.array(query_embedding), np.array(emb))
            ranked.append({
                "text": documents[idx],
                "metadata": metadatas[idx],
                "id": ids[idx],
                "score": score
            })

        # Sort by score descending
        ranked = sorted(ranked, key=lambda x: x['score'], reverse=True)
        # print(f"Re-ranking complete. Best score: {ranked[0]['score']:.4f}")
        
        return ranked
    
# if __name__ == "__main__":
#     import os
#     import json
#     from vector_store import ChromaVectorStore
#     from data_processing import DataProcessor
#     from query_proecssing import   QueryProcessor
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
#     user_query = "leaves turning yellow with brown spots"

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
    #         print(results[0]['metadata'])
            
    # except Exception as e:
    #     print(f"Error during retrieval: {e}")
    #     import traceback
    #     traceback.print_exc()