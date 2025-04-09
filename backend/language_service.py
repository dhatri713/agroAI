import os
import json
from google.cloud import speech
from google.cloud import texttospeech
from google.cloud import translate_v2 as translate
from enum import Enum
import io
from dotenv import load_dotenv

ENV_PATH = "../.env"
load_dotenv(ENV_PATH)

class InputType(Enum):
    ENGLISH_TEXT = "english_text"
    TELUGU_TEXT = "telugu_text"
    TELUGU_SPEECH = "telgu_speech"

class LanguageService:
    def __init__(self):
        # initialise google cloud clients
        self.speech_client = speech.SpeechClient()
        self.tts_client = texttospeech.TextToSpeechClient()
        self.translate_client = translate.Client()

        # langauge codes
        self.ENGLISH = "en"
        self.TELUGU = "te"

    def detect_input_type(self, input_content, is_audio=False):
        # detect if input is english text, telugu text or telugu speech
        if is_audio:
            return InputType.TELUGU_SPEECH
    
        # detect language from text
        detection = self.translate_client.detect_language(input_content)
        if detection["language"] == self.TELUGU:
            return InputType.TELUGU_TEXT
        else:
            return InputType.ENGLISH_TEXT
        
    def speech_to_text(self, audio_content):
        audio = speech.RecognitionAudio(content=audio_content)
        
        # Add speech adaptation for agricultural terms
        speech_contexts = [
            speech.SpeechContext(
                phrases=["మిల్లీలీటర్", "లీటర్", "హెక్టారు", "పురుగుమందు", "ఎరువు"],
                boost=10.0
            )
        ]
        
        config = speech.RecognitionConfig(
            encoding=speech.RecognitionConfig.AudioEncoding.LINEAR16,
            language_code=self.TELUGU,
            speech_contexts=speech_contexts
        )

        response = self.speech_client.recognize(config=config, audio=audio)
        text = ""
        for result in response.results:
            text += result.alternatives[0].transcript
        
        return text
    
    def text_to_speech(self, text, language_code):
    # pre-process text to improve pronunciation
        if language_code == self.TELUGU:
            # replace technical terms with phonetic equivalents
            text = text.replace("మి.లీ", "మిల్లీలీటర్")
            text = text.replace("మి.లీ/లీ", "మిల్లీలీటర్ పర్ లీటర్")
            
            # add more common agricultural measurements and abbreviations
            text = text.replace("గ్రా", "గ్రాము")  # Fix for "gra" issue
            text = text.replace("గ్రాం", "గ్రాము")
            text = text.replace("కి.గ్రా", "కిలో గ్రాము")
            text = text.replace("కి.గ్రా/హె", "కిలో గ్రాము పర్ హెక్టారు")
            text = text.replace("సెం.మీ", "సెంటీమీటర్")
            text = text.replace("సెం", "సెంటీమీటర్")
            text = text.replace("హెక్", "హెక్టారు")
            text = text.replace("ఎ.", "ఎకరము")
            text = text.replace("%", "శాతం")
            
            # common pest/disease terms
            text = text.replace("ఇన్ సెక్టిసైడ్", "కీటక నాశిని")
            text = text.replace("ఫంగిసైడ్", "శిలీంధ్ర నాశిని")
            text = text.replace("హెర్బిసైడ్", "కలుపు నాశిని")
            
            # chemical names
            text = text.replace("N-P-K", "నత్రజని ఫాస్పరస్ పొటాషియం")
            
            # numbers, ratios and ranges
            import re
            text = re.sub(r'(\d+)-(\d+)', r'\1 నుండి \2', text)  # Replace 5-10 with "5 నుండి 10"
            text = re.sub(r'(\d+):(\d+):(\d+)', r'\1 \2 \3 నిష్పత్తిలో', text)  # Replace 5:10:10 with ratio
            
            # ensure proper spacing after punctuation for better speech flow
            text = re.sub(r'([.,;:!?])(\S)', r'\1 \2', text)
        
        synthesis_input = texttospeech.SynthesisInput(text=text)
        
        # voice and audio config same as before
        voice = texttospeech.VoiceSelectionParams(
            language_code=language_code,
            ssml_gender=texttospeech.SsmlVoiceGender.NEUTRAL
        )
        
        audio_config = texttospeech.AudioConfig(
            audio_encoding=texttospeech.AudioEncoding.MP3
        )
        
        response = self.tts_client.synthesize_speech(
            input=synthesis_input, voice=voice, audio_config=audio_config
        )
        
        return response.audio_content
    
    def translate_text(self, text, target_language):
        result = self.translate_client.translate(
            text, target_language=target_language
        )
        
        return result["translatedText"]
    
    def process_input(self, input_content, is_audio=False):
        # process input and convert to English for RAG pipeline
        input_type = self.detect_input_type(input_content, is_audio)
        
        if input_type == InputType.ENGLISH_TEXT:
            return {
                "input_type": InputType.ENGLISH_TEXT,
                "original_text": input_content,
                "processed_text": input_content,  # no processing needed
                "input_language": self.ENGLISH
            }
        
        elif input_type == InputType.TELUGU_TEXT:
            # translate Telugu text to English for processing
            english_text = self.translate_text(input_content, self.ENGLISH)
            return {
                "input_type": InputType.TELUGU_TEXT,
                "original_text": input_content,
                "processed_text": english_text,
                "input_language": self.TELUGU
            }
        
        elif input_type == InputType.TELUGU_SPEECH:
            # convert speech to text and then translate
            telugu_text = self.speech_to_text(input_content)
            english_text = self.translate_text(telugu_text, self.ENGLISH)
            return {
                "input_type": InputType.TELUGU_SPEECH,
                "original_text": telugu_text,
                "processed_text": english_text,
                "input_language": self.TELUGU
            }
        
    def format_output(self, answer, input_language):
        # format output based on input language
        if input_language == self.ENGLISH:
            return {
                "text": answer,
                "language": self.ENGLISH
            }
        else:  # telugu
            # translate answer to Telugu
            telugu_answer = self.translate_text(answer, self.TELUGU)
            # generate speech from Telugu text
            speech_content = self.text_to_speech(telugu_answer, self.TELUGU)
            
            return {
                "text": telugu_answer,
                "language": self.TELUGU,
                "speech": speech_content
            }
        
