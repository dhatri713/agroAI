"""
Continues building the Chroma DB with embeddings to improve the knowledge base.
"""

import os
import sys
import json
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict, List, Any, Optional

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from data_processing import DataProcessor
from sentence_embedder import SentenceEmbedder
from vector_store import ChromaVectorStore

ENV_PATH = "../.env"
load_dotenv(ENV_PATH)

class BuildKnowledgeBasePipeline():
    def __init__(self, data_dir: str = "data", 
                 persist_directory: str = "chroma_db",
                 collection_name: str = "agriculture_data",
                 embedding_model_name: str = "paraphrase-mpnet-base-v2"):
        
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

    def process_data(self):# process all files and add documents to the vector store
        all_chunks = self.data_processor.process_all_files()
        self.vector_store.add_documents(all_chunks)
    
    def update_data(self, new_data_dir: str):
        # process new data and add it to the existing database
        new_chunks = self.data_processor.process_files(new_data_dir)
        self.vector_store.add_documents(new_chunks)

def main():
    # initialize the pipeline
    pipeline = BuildKnowledgeBasePipeline(
        data_dir="../data",
        persist_directory="../chroma_db",
        collection_name="agriculture_data"
    )
    
    pipeline.process_data()

if __name__ == "__main__":
    main()