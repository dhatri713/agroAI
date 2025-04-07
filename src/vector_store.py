from chromadb import PersistentClient
from typing import List, Dict, Any

class ChromaVectorStore:
    def __init__(self, collection_name: str, persist_directory: str, embedding_function):
        self.client = PersistentClient(path=persist_directory)
        self.collection = self.client.get_or_create_collection(name=collection_name)
        self.embedding_function = embedding_function

    def add_documents(self, chunks: List[Dict[str, Any]]):
        texts = [chunk['text'] for chunk in chunks]
        embeddings = self.embedding_function.embed_text(texts)
        ids = [f"{chunk['metadata']['crop']}_{chunk['metadata']['disease']}_{i}" for i, chunk in enumerate(chunks)]
        
        # Fix: Convert any list values in metadata to strings
        processed_metadatas = []
        for chunk in chunks:
            processed_metadata = {}
            for key, value in chunk['metadata'].items():
                if isinstance(value, list):
                    # Convert list to comma-separated string
                    processed_metadata[key] = ", ".join(str(item) for item in value)
                else:
                    processed_metadata[key] = value
            processed_metadatas.append(processed_metadata)

        self.collection.add(
            ids=ids,
            embeddings=embeddings,
            metadatas=processed_metadatas,  # Use processed metadata
            documents=texts
        )

if __name__ == "__main__":
    import os
    import json
    from data_processing import DataProcessor        # your chunker
    from sentence_embedder import SentenceEmbedder  # your embedder
    from vector_store import ChromaVectorStore      # your vector store

    # 1. Load data
    data_file = "../data/bacterial_wilt_chilli.json"
    with open(data_file, "r") as f:
        raw_data = json.load(f)

    print(f"Loaded {data_file}")

    # 2. Chunk the data
    processor = DataProcessor()
    chunks = processor.process_files(data_file)
    print(f"Generated {len(chunks)} chunks")

    # 3. Embeddings
    embedder = SentenceEmbedder(model_name="paraphrase-mpnet-base-v2")
    chunks_with_embeddings = embedder.embed_chunks(chunks)
    print("Embeddings generated")

    # 4. Store in Chroma
    vector_store = ChromaVectorStore(
        persist_directory="./chroma_db",
        collection_name="test_chilli",
        embedding_function=embedder
    )

    vector_store.add_documents(chunks_with_embeddings)
    print("Data stored in Chroma")