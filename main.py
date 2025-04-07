import os
import json
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict, List, Any, Optional

# Import all the necessary modules
from src.data_processing import DataProcessor
from src.sentence_embedder import SentenceEmbedder
from src.vector_store import ChromaVectorStore
from src.query_proecssing import QueryProcessor
from src.retriever_and_ranker import RetrieverAndRanker
from src.answer_generation import AnswerGenerator

# Load environment variables
load_dotenv()

class AgricultureQAPipeline:
    def __init__(self, data_dir: str = "data", 
                 persist_directory: str = "chroma_db",
                 collection_name: str = "agriculture_data",
                 embedding_model_name: str = "paraphrase-mpnet-base-v2",
                 llm_model_name: str = "llama3-8b-8192"):
        """
        Initialize the Agriculture QA Pipeline
        
        Args:
            data_dir: Directory containing the data files
            persist_directory: Directory to persist the vector database
            collection_name: Name of the collection in the vector database
            embedding_model_name: Name of the embedding model to use
            llm_model_name: Name of the LLM model to use for answer generation
        """
        self.data_dir = data_dir
        self.persist_directory = persist_directory
        self.collection_name = collection_name
        
        # Ensure directories exist
        Path(data_dir).mkdir(exist_ok=True)
        Path(persist_directory).mkdir(exist_ok=True)
        
        # Initialize components
        print("Initializing pipeline components...")
        self.data_processor = DataProcessor(data_dir=data_dir)
        self.embedder = SentenceEmbedder(model_name=embedding_model_name)
        self.vector_store = ChromaVectorStore(
            collection_name=collection_name,
            persist_directory=persist_directory,
            embedding_function=self.embedder.model
        )
        self.query_processor = QueryProcessor()
        self.retriever = None  # Will be initialized after data is loaded
        self.answer_generator = AnswerGenerator(model_name=llm_model_name)
        
    def process_data(self, file_path: Optional[str] = None):
        """
        Process data from a specific file or all files in the data directory
        
        Args:
            file_path: Path to a specific file to process (if None, all files are processed)
        """
        print("Processing data...")
        if file_path:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File {file_path} not found")
            chunks = self.data_processor.process_files(file_path)
            print(f"Generated {len(chunks)} chunks from {file_path}")
        else:
            chunks = self.data_processor.process_all_files()
            print(f"Generated {len(chunks)} chunks from all files")
            
        print("Adding documents to vector store...")
        self.vector_store.add_documents(chunks)
        print(f"Documents stored in collection '{self.collection_name}'")
        
        # Initialize retriever after data is loaded
        self.retriever = RetrieverAndRanker(self.vector_store)
        
    def answer_query(self, query: str, top_k: int = 5) -> Dict[str, Any]:
        """
        Process a user query and generate an answer
        
        Args:
            query: User's question
            top_k: Number of top documents to retrieve
            
        Returns:
            Dictionary containing the original query, expanded query, and answer
        """
        if not self.retriever:
            raise ValueError("Pipeline not fully initialized. Please process data first.")
            
        print(f"Processing query: '{query}'")
        
        # Step 1: Process the query
        expanded_query = self.query_processor.expand_query(query)
        print(f"Expanded query: '{expanded_query}'")
        
        # Step 2: Get query embedding
        query_embedding = self.query_processor.get_query_embedding(expanded_query)
        
        # Step 3: Retrieve relevant documents
        retrieved_docs = self.retriever.retrieve_and_rerank(query_embedding=query_embedding, top_k=top_k)
        
        if not retrieved_docs:
            return {
                "original_query": query,
                "expanded_query": expanded_query,
                "answer": "I couldn't find relevant information to answer your question.",
                "sources": []
            }
        
        # Step 4: Generate answer
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

def load_demo_data(pipeline, file_path):
    """Helper function to load demo data"""
    if not os.path.exists(file_path):
        print(f"Warning: File {file_path} not found")
        return
    
    try:
        pipeline.process_data(file_path)
        print(f"Successfully loaded data from {file_path}")
    except Exception as e:
        print(f"Error loading data: {e}")

def main():
    """Main function to initialize the pipeline and run a demo query"""
    # Initialize the pipeline
    pipeline = AgricultureQAPipeline(
        data_dir="data",
        persist_directory="chroma_db",
        collection_name="agriculture_data"
    )
    
    # Demo data loading
    demo_file = "data/bacterial_wilt_chilli.json" 
    load_demo_data(pipeline, demo_file)
    
    # Example query
    query = "What are the symptoms of bacterial wilt in chilli plants?"
    result = pipeline.answer_query(query)
    
    print("\n----- Query Results -----")
    print(f"Query: {result['original_query']}")
    print(f"Expanded: {result['expanded_query']}")
    print(f"\nAnswer: {result['answer']}")
    print("\nTop Sources:")
    for i, source in enumerate(result['sources'][:2], 1):
        print(f"{i}. {source['text'][:150]}...")
        print(f"   Metadata: {source['metadata']}")
        print()

# For web integration
def create_pipeline(data_dir="data", persist_directory="chroma_db", collection_name="agriculture_data"):
    """
    Create and return a pipeline instance for web integration
    
    Args:
        data_dir: Directory containing the data files
        persist_directory: Directory to persist the vector database
        collection_name: Name of the collection in the vector database
        
    Returns:
        An initialized AgricultureQAPipeline instance
    """
    return AgricultureQAPipeline(
        data_dir=data_dir,
        persist_directory=persist_directory,
        collection_name=collection_name
    )

if __name__ == "__main__":
    main()