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
    
    def build_prompt(self, query: str, retrieved_docs: List[Dict], conversation_context: str = "") -> str:
        # check if any retrieved docs have cross-crop pests/diseases
        has_cross_crop = any('cross_crop' in doc.get('metadata', {}) for doc in retrieved_docs)
        cross_crop_info = ""
        
        # include specific crop context for cross-crop pests/diseases
        if has_cross_crop:
            # extract related crops from metadata
            crops = set()
            for doc in retrieved_docs:
                if 'cross_crop' in doc.get('metadata', {}) and 'related_crops' in doc.get('metadata', {}):
                    crops.update(doc['metadata']['related_crops'])
            
            if crops:
                cross_crop_info = f"""
                IMPORTANT: The pest/disease in the query may affect multiple crops: {', '.join(crops)}. 
                If the user hasn't specified which crop they're asking about, acknowledge this in your response and ask which crop they're referring to.
                """
        
        context = "\n\n".join([doc['text'] for doc in retrieved_docs])
        
        # include conversation context if available
        conversation_part = ""
        if conversation_context:
            conversation_part = f"""
            Recent conversation history:
            {conversation_context}
            
            Please take into account the conversation history above when answering the current query.
            """
        
        prompt = f"""You are an expert agricultural assistant specialized in helping farmers.

        Use the following information to answer the user's query in a simple, accurate, and concise manner. 
        
        IMPORTANT GUIDELINES:
        1. Do NOT use asterisks (*) or bullet points (•) in your response.
        2. Keep your response short, focused and to the point - preferably under 200 words.
        3. Use simple language that will translate well to Telugu.
        4. Focus only on the most relevant information for the farmer's query.
        5. Include specific measurements, treatments, or solutions when available.
        6. Avoid conversational fillers like "I hope this helps" or "Let me know if you have more questions".
        {cross_crop_info}
        
        {conversation_part}

        Context information (agricultural knowledge):
        {context}

        Current User Query:
        {query}"""
        
        return prompt

    def generate_answer(self, query: str, retrieved_docs: List[Dict], conversation_context: str = "") -> str:
        prompt = self.build_prompt(query, retrieved_docs, conversation_context)
        
        response = self.client.chat.completions.create(
            model=self.model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.35,  # slightly increased temperature for more natural conversation
            max_tokens=400,   # limiting response length
        )

        answer = response.choices[0].message.content.strip()
        
        # post-process to remove asterisks and bullet points
        answer = answer.replace('*', '')  # remove asterisks
        answer = answer.replace('•', '')  # remove bullet points if present
        
        # clean up extra whitespace that might result from removing bullets
        import re
        answer = re.sub(r'\n\s*\n', '\n\n', answer)  # replace multiple empty lines with just one
        answer = re.sub(r'^\s+', '', answer, flags=re.MULTILINE)  # remove leading whitespace from each line
        
        return answer