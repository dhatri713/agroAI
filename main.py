import os
import json
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict, List, Any, Optional

from src.data_processing import DataProcessor
from src.sentence_embedder import SentenceEmbedder
from src.vector_store import ChromaVectorStore
from src.query_proecssing import QueryProcessor
from src.retriever_and_ranker import RetrieverAndRanker
from src.answer_generation import AnswerGenerator

load_dotenv()

class AgricultureQAPipeline:
    def __init__(self, data_dir: str = "data", 
                 persist_directory: str = "chroma_db",
                 collection_name: str = "agriculture_data",
                 embedding_model_name: str = "paraphrase-mpnet-base-v2",
                 llm_model_name: str = "llama3-8b-8192"):
        
        self.data_dir = data_dir
        self.persist_directory = persist_directory
        self.collection_name = collection_name
        
        # ensure directories exist
        Path(data_dir).mkdir(exist_ok=True)
        Path(persist_directory).mkdir(exist_ok=True)
        
        # initialize components
        self.data_processor = DataProcessor(data_dir=data_dir)
        self.embedder = SentenceEmbedder(model_name=embedding_model_name)
        self.vector_store = ChromaVectorStore(
            collection_name=collection_name,
            persist_directory=persist_directory,
            embedding_function=self.embedder.model
        )
        self.query_processor = QueryProcessor()
        self.retriever = None  # will be initialized after data is loaded
        self.answer_generator = AnswerGenerator(model_name=llm_model_name)
        
    def process_data(self):
        all_chunks = self.data_processor.process_all_files()
        self.vector_store.add_documents(all_chunks)

        # initialize retriever after data is loaded
        self.retriever = RetrieverAndRanker(self.vector_store)
        
    def answer_query(self, query: str, top_k: int = 5) -> Dict[str, Any]:

        expanded_query = self.query_processor.expand_query(query)
        query_embedding = self.query_processor.get_query_embedding(expanded_query)
        retrieved_docs = self.retriever.retrieve_and_rerank(query_embedding=query_embedding, top_k=top_k)
        
        if not retrieved_docs:
            return {
                "original_query": query,
                "expanded_query": expanded_query,
                "answer": "I couldn't find relevant information to answer your question.",
                "sources": []
            }
        
        # generate answer
        answer = self.answer_generator.generate_answer(query, retrieved_docs)
        
        # Prepare sources for the frontend
        sources = []
        for doc in retrieved_docs:
            sources.append({
                "text": doc["text"],
                "metadata": doc["metadata"],
                "score": doc["score"]
            })
        
        return {
            "original_query": query,
            "expanded_query": expanded_query,
            "answer": answer,
            "sources": sources
        }


def main():
    # Initialize the pipeline
    pipeline = AgricultureQAPipeline(
        data_dir="data",
        persist_directory="chroma_db",
        collection_name="agriculture_data"
    )

    pipeline.process_data()
    query = "what are the diseases commonly found in cotton?"
    answer = pipeline.answer_query(query=query)
    print(answer["answer"])
    

if __name__ == "__main__":
    main()