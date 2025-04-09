import os
import requests
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

ENV_PATH = "../.env"
load_dotenv(ENV_PATH)

class QueryProcessor:
    def __init__(self):
        self.groq_api_key = os.getenv("GROQ_API_KEY")
        if not self.groq_api_key:
            raise ValueError("Groq API key not found in .env")

        self.groq_url = "https://api.groq.com/openai/v1/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {self.groq_api_key}",
            "Content-Type": "application/json",
        }
        self.embedding_model = SentenceTransformer("sentence-transformers/paraphrase-mpnet-base-v2")
        
        # common agricultural terms for better vocabulary expansion
        self.agricultural_terms = [
            "crop", "pest", "disease", "fertilizer", "irrigation", "harvest", "yield", 
            "soil", "seed", "cultivation", "organic", "inorganic", "chemical", "biological",
            "treatment", "prevention", "control", "management", "symptoms", "lifecycle",
            "fungicide", "insecticide", "pesticide", "herbicide", "application", "dosage",
            "rotation", "intercropping", "nutrient", "deficiency", "excess"
        ]
        
        # common crops in Andhra Pradesh and Telangana
        self.common_crops = [
            "rice", "paddy", "wheat", "maize", "corn", "jowar", "bajra", "ragi", 
            "groundnut", "peanut", "cotton", "sugarcane", "chilli", "pepper", 
            "turmeric", "tobacco", "mango", "banana", "papaya", "guava", "sapota", 
            "tomato", "brinjal", "eggplant", "okra", "bhendi", "onion", "leafy vegetables"
        ]

    def expand_query(self, query: str) -> str:
        # enhanced query expansion with cross-crop pest/disease awareness
        system_prompt = """You are an agricultural query expansion AI. Your task is to expand short queries from farmers into detailed queries optimized for a retrieval system.

    Follow these strict guidelines:
    1. Include all original keywords from the user query.
    2. Add relevant agricultural terms related to the query.
    3. If the query mentions symptoms, add keywords like "symptoms, causes, identification".
    4. If the query implies treatment or management, add keywords like "control, prevention, treatment, chemical control, biological control, cultural practices".
    5. If the query mentions a pest or disease but no specific crop, add keywords to help identify the relevant crop.
    6. If the query mentions a location or region, include "Andhra Pradesh, Telangana" and relevant environmental conditions.
    7. Add terms for different growth stages if applicable.
    8. Include both scientific and common/local names of pests and diseases when possible.
    9. Format output as a single, clear sentence - no explanations, just the expanded query.
    10. Never use asterisks (*) in your expansion.
    11. When units of measurement are mentioned (like grams or milliliters), maintain them in the expansion.
    12. If a specific crop is mentioned, include related pest and disease issues common for that crop.

    Your expanded query should be comprehensive but focused on retrieving the most relevant agricultural information."""

        # add relevant terms to the query based on content
        query_terms = query.lower().split()
        additional_terms = []
        
        # check for crop mentions and add relevant terms
        crop_mentioned = False
        for crop in self.common_crops:
            if crop in query.lower():
                crop_mentioned = True
                additional_terms.extend(["cultivation", "farming", "practices"])
                break
        
        # if talking about pests/diseases but no crop mentioned, add general crop terms
        if not crop_mentioned:
            pest_disease_indicators = ["pest", "disease", "infection", "infestation", "attack", 
                                    "white grub", "aphid", "thrips", "borer", "wilt", "rot", "blight"]
            
            if any(indicator in query.lower() for indicator in pest_disease_indicators):
                additional_terms.extend(["common crops", "affected crops", "host plants"])
        
        # check for symptom mentions
        symptom_indicators = ["yellow", "brown", "spots", "wilting", "dying", "rot", "damage", "holes", 
                            "stunted", "infection", "infected", "lesions"]
        if any(term in query.lower() for term in symptom_indicators):
            additional_terms.extend(["symptoms", "signs", "identification", "diagnosis"])
        
        # check for treatment requests
        treatment_indicators = ["treat", "control", "prevent", "manage", "solution", "spray", "apply", "cure"]
        if any(term in query.lower() for term in treatment_indicators):
            additional_terms.extend(["treatment", "control measures", "prevention", "management"])
            additional_terms.extend(["chemical control", "biological control", "cultural practices"])
        
        # enhanced query for the LLM
        user_prompt = f"""Expand this agricultural query in detail: {query}

    I want you to consider these additional relevant terms if appropriate: {', '.join(additional_terms)}"""
        payload = {
            "model": "llama3-8b-8192",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.2,
        }

        try:
            response = requests.post(self.groq_url, headers=self.headers, json=payload)
            
            if response.status_code == 200:
                expanded_query = response.json()["choices"][0]["message"]["content"].strip()
                # remove any asterisks that might have been added
                expanded_query = expanded_query.replace("*", "")
                return expanded_query
            else:
                print(f"Groq API Error: {response.status_code} - {response.text}")
                # fall back to basic expansion if API fails
                return self._basic_expansion(query, additional_terms)
        except Exception as e:
            print(f"Query expansion error: {e}")
            # fall back to basic expansion if API call fails
            return self._basic_expansion(query, additional_terms)
    
    def _basic_expansion(self, query: str, additional_terms: list) -> str:
        # fallback method for basic query expansion without LLM
        expanded_query = query
        
        # add additional terms with minimal duplication
        for term in additional_terms:
            if term.lower() not in query.lower():
                expanded_query += f" {term}"
                
        return expanded_query
    
    def get_query_embedding(self, text: str) -> list:
        return self.embedding_model.encode(text).tolist()