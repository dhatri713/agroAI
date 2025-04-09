import os
import sys
import base64
from typing import Dict, Any, List, Optional, Union
from fastapi import FastAPI, File, UploadFile, Form, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import json
from pathlib import Path
import logging

logging.getLogger('chromadb.segment.impl.vector.local_persistent_hnsw').setLevel(logging.ERROR)

# import your existing modules
from vector_store import ChromaVectorStore
from query_proecssing import QueryProcessor
from retriever_and_ranker import RetrieverAndRanker
from answer_generation import AnswerGenerator
from sentence_embedder import SentenceEmbedder
from data_processing import DataProcessor

# import new modules
from language_service import LanguageService, InputType

# setup logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ConversationManager:
    def __init__(self):
        self.conversations = {}
        
    def add_message(self, conversation_id: str, message: Dict[str, Any]):
        if conversation_id not in self.conversations:
            self.conversations[conversation_id] = []
        self.conversations[conversation_id].append(message)
        
    def get_conversation(self, conversation_id: str) -> List[Dict[str, Any]]:
        return self.conversations.get(conversation_id, [])

class MultilingualRAGService:
    def __init__(self, 
                 data_dir: str = "../data", 
                 persist_directory: str = "../chroma_db",
                 collection_name: str = "agriculture_data",
                 embedding_model_name: str = "paraphrase-mpnet-base-v2",
                 llm_model_name: str = "llama3-8b-8192"):
        
        # initialize directories
        self.data_dir = data_dir
        self.persist_directory = persist_directory
        Path(data_dir).mkdir(exist_ok=True)
        Path(persist_directory).mkdir(exist_ok=True)
        
        # initialize core RAG components
        self.embedder = SentenceEmbedder(model_name=embedding_model_name)
        self.vector_store = ChromaVectorStore(
            collection_name=collection_name,
            persist_directory=persist_directory,
            embedding_function=self.embedder.model
        )
        self.query_processor = QueryProcessor()
        self.retriever = RetrieverAndRanker(self.vector_store)
        self.answer_generator = AnswerGenerator(model_name=llm_model_name)
        
        # initialize new components
        self.language_service = LanguageService()
        self.data_processor = DataProcessor(data_dir=data_dir)
        self.conversation_manager = ConversationManager()
    
    def process_query(self, 
                  query_content: Union[str, bytes], 
                  is_audio: bool = False,
                  conversation_id: str = None,
                  top_k: int = 5) -> Dict[str, Any]:
        # process the input and detect language
        try:
            processed_input = self.language_service.process_input(query_content, is_audio)
            
            # get the English query text for processing
            english_query = processed_input["processed_text"]
            input_language = processed_input["input_language"]
            
            # Check if query mentions a specific crop
            import re
            from collections import Counter
            
            # Common crops in Telugu agriculture
            crops = ["rice", "paddy", "wheat", "maize", "corn", "jowar", "bajra", "groundnut", 
                    "peanut", "cotton", "sugarcane", "chilli", "pepper", "turmeric"]
            
            # Check if any crop is mentioned in the query
            has_crop_mention = any(crop in english_query.lower() for crop in crops)
            
            # store user message in conversation history
            if conversation_id:
                self.conversation_manager.add_message(
                    conversation_id,
                    {"role": "user", "content": processed_input["original_text"]}
                )
            
            # use existing RAG pipeline with the English query
            expanded_query = self.query_processor.expand_query(english_query)
            query_embedding = self.query_processor.get_query_embedding(expanded_query)
            
            # Adjust retrieval based on crop detection
            if not has_crop_mention:
                # If no crop mentioned, retrieve more documents to find cross-crop information
                retrieved_docs = self.retriever.retrieve_and_rerank(query_embedding=query_embedding, 
                                                                top_k=top_k * 2,
                                                                relevance_threshold=0.6)  # Lower threshold to catch more
            else:
                # Normal retrieval for crop-specific queries
                retrieved_docs = self.retriever.retrieve_and_rerank(query_embedding=query_embedding, 
                                                                top_k=top_k,
                                                                relevance_threshold=0.65)
            
            if not retrieved_docs:
                answer = "I couldn't find relevant information to answer your question."
            else:
                # add conversation context for better answers
                conversation_context = ""
                if conversation_id:
                    previous_messages = self.conversation_manager.get_conversation(conversation_id)
                    if len(previous_messages) > 1:  # at least one exchange
                        conversation_context = "Previous conversation:\n"
                        # get last few messages (limited to keep context relevant)
                        for msg in previous_messages[-4:]:  # Last 4 messages max
                            conversation_context += f"{msg['role']}: {msg['content']}\n"
                
                # Check for cross-crop pests/diseases in retrieved documents
                crops_mentioned = set()
                cross_crop_detected = False
                
                for doc in retrieved_docs:
                    if 'crop' in doc.get('metadata', {}):
                        crops_mentioned.add(doc['metadata']['crop'])
                    # Also try to extract crop from text for JSON documents
                    crop_match = re.search(r'"crop"\s*:\s*"([^"]+)"', doc['text'])
                    if crop_match:
                        crops_mentioned.add(crop_match.group(1))
                    
                    # Check if this document has cross-crop flag
                    if 'cross_crop' in doc.get('metadata', {}) and doc['metadata']['cross_crop']:
                        cross_crop_detected = True
                
                # If we detected multiple crops but user didn't specify one, enhance the context
                if len(crops_mentioned) > 1 and not has_crop_mention and cross_crop_detected:
                    # Add this context to the conversation for the LLM
                    crop_context = f"Note: The issue mentioned may affect multiple crops: {', '.join(crops_mentioned)}. "
                    conversation_context += crop_context
                
                answer = self.answer_generator.generate_answer(
                    english_query, 
                    retrieved_docs,
                    conversation_context=conversation_context
                )
            
            # store assistant response in conversation history
            if conversation_id:
                self.conversation_manager.add_message(
                    conversation_id,
                    {"role": "assistant", "content": answer}
                )
            
            # format the output based on input language
            output = self.language_service.format_output(answer, input_language)
            
            # add additional context to the response
            response = {
                "original_query": processed_input["original_text"],
                "processed_query": english_query,
                "answer": output["text"],
                "language": output["language"],
            }
            
            # add speech data if present (for Telugu)
            if "speech" in output:
                response["speech_base64"] = base64.b64encode(output["speech"]).decode("utf-8")
            
            # add sources for context
            sources = []
            for doc in retrieved_docs:
                source_entry = {
                    "text": doc["text"],
                    "metadata": doc["metadata"],
                    "score": doc["score"]
                }
                
                # Add cross-crop information if available
                if 'cross_crop' in doc.get('metadata', {}) and 'related_crops' in doc.get('metadata', {}):
                    source_entry["related_crops"] = doc['metadata']['related_crops']
                    
                sources.append(source_entry)
                
            response["sources"] = sources
            
            return response
            
        except Exception as e:
            logger.error(f"Error processing query: {str(e)}", exc_info=True)
            return {
                "answer": "I encountered an error while processing your query. Please try again.",
                "error": str(e)
            }


# Create FastAPI application - moved outside the class definition
app = FastAPI(title="Multilingual Agricultural RAG API")

# add CORS middleware to allow cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust this in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# initialize the service
rag_service = MultilingualRAGService()

@app.post("/query/text")
async def text_query(request: Dict):
    """
    Process a text query (English or Telugu)
    """
    query = request.get("query", "")
    language = request.get("language", "english")
    conversation_id = request.get("conversation_id")
    
    if not query:
        return JSONResponse(content={"error": "Query cannot be empty"}, status_code=400)
    
    logger.info(f"Processing text query: '{query}' in {language}, conversation_id: {conversation_id}")
    result = rag_service.process_query(query, conversation_id=conversation_id)
    return JSONResponse(content=result)

@app.post("/query/audio")
async def audio_query(
    file: UploadFile = File(...),
    language: str = Form("telugu"),
    conversation_id: str = Form(None)
):
    """
    Process a Telugu speech query
    """
    try:
        logger.info(f"Processing audio query in {language}, conversation_id: {conversation_id}")
        audio_content = await file.read()
        
        if not audio_content:
            return JSONResponse(content={"error": "Empty audio file"}, status_code=400)
            
        # log the audio file size for debugging
        logger.info(f"Audio file size: {len(audio_content)} bytes")
        
        result = rag_service.process_query(
            audio_content, 
            is_audio=True,
            conversation_id=conversation_id
        )
        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"Error processing audio query: {str(e)}", exc_info=True)
        return JSONResponse(
            content={"error": f"Failed to process audio: {str(e)}"},
            status_code=500
        )

@app.get("/conversation/{conversation_id}")
async def get_conversation(conversation_id: str):
    """
    Get the conversation history for a specific conversation ID
    """
    conversation = rag_service.conversation_manager.get_conversation(conversation_id)
    return JSONResponse(content={"conversation": conversation})

# run the API with: uvicorn app:app --reload
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)