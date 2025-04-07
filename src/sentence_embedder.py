from typing import List, Union, Dict, Any
from sentence_transformers import SentenceTransformer


class SentenceEmbedder:
    def __init__(self, model_name: str = "paraphrase-mpnet-base-v2"):
        self.model = SentenceTransformer(model_name)
        self.model_name = model_name
        self.embedding_dimension = self.model.get_sentence_embedding_dimension()

    def embed_text(self, text: Union[str, List[str]]) -> List[List[float]]:
        # Ensure text is a list for batch processing
        if isinstance(text, str):
            text = [text]
        embeddings = self.model.encode(text, convert_to_numpy=True).tolist()
        return embeddings

    def embed_chunks(self, chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # Create embeddings for a list of text chunks and add them to the chunks
        texts = [chunk['text'] for chunk in chunks]
        embeddings = self.embed_text(texts)

        for i, chunk in enumerate(chunks):
            chunk['embedding'] = embeddings[i]

        return chunks

    def embed_query(self, query: str) -> List[float]:
        # Generate embedding for a single query
        return self.embed_text(query)[0]

# if __name__ == "__main__":
#     embedder = SentenceEmbedder()
#     sample_text = "Symptoms of bacterial wilt in chilli include wilting of leaves."
    
#     print("Query Embedding:", embedder.embed_query(sample_text))
