import logging
from chromadb import PersistentClient
from typing import List, Dict, Any
import numpy as np
import uuid

class ChromaVectorStore:
    def __init__(self, collection_name: str, persist_directory: str, embedding_function):
        self.client = PersistentClient(path=persist_directory)
        self.collection = self.client.get_or_create_collection(name=collection_name)
        self.embedding_function = embedding_function
        self.logger = logging.getLogger(__name__)  # Initialize logger
        
        # Configure the specific ChromaDB logger to suppress warnings
        logging.getLogger('chromadb.segment.impl.vector.local_persistent_hnsw').setLevel(logging.ERROR)

    def add_documents(self, chunks: List[Dict[str, Any]]):
        if not chunks:
            self.logger.info("No documents to add.")
            return
            
        texts = [chunk['text'] for chunk in chunks]
        embeddings = self.embedding_function.encode(texts)
        
        # Generate IDs first - now using UUID for uniqueness
        ids = [f"{chunk['metadata']['crop']}_{chunk['metadata']['disease']}_{uuid.uuid4().hex[:8]}" for chunk in chunks]

        processed_metadatas = []
        for chunk in chunks:
            processed_metadata = {}
            for key, value in chunk['metadata'].items():
                if isinstance(value, list):
                    processed_metadata[key] = ", ".join(str(item) for item in value)
                else:
                    processed_metadata[key] = value
            processed_metadatas.append(processed_metadata)
        
        try:
            self.collection.add(
                ids=ids,
                embeddings=embeddings.tolist(),
                metadatas=processed_metadatas,
                documents=texts
            )
            self.logger.info(f"Successfully added {len(chunks)} documents to the vector store.")
        except Exception as e:
            self.logger.error(f"Error adding documents: {e}")
    
    def get_documents(self, query_embedding, top_k=5):
        """
        Retrieve documents based on vector similarity
        """
        try:
            results = self.collection.query(
                query_embeddings=[query_embedding],
                n_results=top_k
            )
            
            documents = []
            if results and 'documents' in results and results['documents']:
                for i, doc in enumerate(results['documents'][0]):
                    documents.append({
                        'text': doc,
                        'metadata': results['metadatas'][0][i] if 'metadatas' in results and results['metadatas'] else {},
                        'score': results['distances'][0][i] if 'distances' in results and results['distances'] else 0.0
                    })
            
            return documents
        except Exception as e:
            self.logger.error(f"Error retrieving documents: {e}")
            return []
    
    def delete_collection(self):
        """
        Delete the current collection
        """
        try:
            self.client.delete_collection(self.collection.name)
            self.logger.info(f"Collection {self.collection.name} deleted.")
        except Exception as e:
            self.logger.error(f"Error deleting collection: {e}")
    
    def create_collection(self):
        """
        Create a new collection
        """
        try:
            self.collection = self.client.create_collection(self.collection.name)
            self.logger.info(f"Collection {self.collection.name} created.")
        except Exception as e:
            self.logger.error(f"Error creating collection: {e}")
    
    def count_documents(self):
        """
        Return the count of documents in the collection
        """
        try:
            return self.collection.count()
        except Exception as e:
            self.logger.error(f"Error counting documents: {e}")
            return 0