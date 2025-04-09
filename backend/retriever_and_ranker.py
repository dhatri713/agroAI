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
    
    def retrieve_and_rerank(self, query_embedding: List[float], top_k: int = 5, relevance_threshold: float = 0.65) -> List[Dict[str, Any]]:
    # query the collection - add include parameter to get embeddings
        initial_k = min(top_k * 3, self.collection.count())  # Retrieve more initially to allow for filtering
        
        results = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=initial_k,
            include=["embeddings", "documents", "metadatas", "distances"]
        )
        
        # If no results returned, return empty list
        if not results['ids'][0]:
            print("No documents found matching the query")
            return []
            
        # Get embeddings from retrieved results
        retrieved_embeddings = results['embeddings'][0]
        documents = results['documents'][0]
        metadatas = results['metadatas'][0]
        ids = results['ids'][0]
        
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
        
        # Filter out documents with low relevance scores
        filtered_ranked = [doc for doc in ranked if doc['score'] >= relevance_threshold]
        
        # For cross-crop pest detection, check for disease/pest name similarities
        # across different documents with different crop metadata
        potential_pest_disease = self._detect_cross_crop_entries(filtered_ranked)
        
        # If pest/disease appears in multiple crops, mark it in metadata
        if potential_pest_disease:
            # Add this information to each document's metadata
            for doc in filtered_ranked:
                if any(pest in doc['text'] for pest in potential_pest_disease):
                    # Create cross_crop key if it doesn't exist
                    if 'cross_crop' not in doc['metadata']:
                        doc['metadata']['cross_crop'] = True
                        doc['metadata']['related_crops'] = self._find_related_crops(potential_pest_disease, filtered_ranked)
        
        # If all documents were filtered out, return top results anyway but limited
        if not filtered_ranked:
            return ranked[:max(1, min(3, len(ranked)))]  # Return at least one but no more than 3
        
        # Only return top_k documents
        return filtered_ranked[:top_k]

    def _detect_cross_crop_entries(self, ranked_docs: List[Dict]) -> List[str]:
        """Detect if the same pest/disease appears across different crop documents"""
        import re
        
        # Extract potential disease/pest names based on common patterns
        pest_disease_names = []
        
        # Check for disease or pest names in document text and metadata
        for doc in ranked_docs:
            # Look for JSON structures in text that might contain disease/pest names
            if '"disease"' in doc['text'] or '"pest"' in doc['text']:
                # Extract the disease/pest name if present
                name_match = re.search(r'"name"\s*:\s*"([^"]+)"', doc['text'])
                if name_match:
                    pest_disease_names.append(name_match.group(1))
            
            # Check metadata for disease or pest names
            if 'pathogen' in doc['metadata']:
                pest_disease_names.append(doc['metadata']['pathogen'])
            if 'disease' in doc['metadata']:
                pest_disease_names.append(doc['metadata']['disease'])
            if 'pest' in doc['metadata']:
                pest_disease_names.append(doc['metadata']['pest'])
        
        # Count occurrences of each pest/disease name
        from collections import Counter
        name_counts = Counter(pest_disease_names)
        
        # Return names that appear in multiple documents (likely cross-crop)
        return [name for name, count in name_counts.items() if count > 1]

    def _find_related_crops(self, pest_disease_names: List[str], ranked_docs: List[Dict]) -> List[str]:
        """Find crops affected by the given pest/disease"""
        crops = set()
        
        for doc in ranked_docs:
            # Check if this document mentions any of our cross-crop pests/diseases
            if any(pest in doc['text'] for pest in pest_disease_names):
                # Extract crop information
                if 'crop' in doc['metadata']:
                    crops.add(doc['metadata']['crop'])
                
                # Try to extract crop from text
                import re
                crop_match = re.search(r'"crop"\s*:\s*"([^"]+)"', doc['text'])
                if crop_match:
                    crops.add(crop_match.group(1))
        
        return list(crops)