
import logging
import base64
import numpy as np
import os
import tempfile
from typing import Dict, Any, Optional, Union
import asyncio

from ..config import settings

logger = logging.getLogger("mcp_server.stt")

class WhisperSTT:
    """
    Speech-to-Text service using OpenAI's Whisper model.
    
    This class uses a local instance of the Whisper model to transcribe
    audio data to text. It's designed to work with various audio formats.
    """
    
    def __init__(self, model_name: str = None):
        """
        Initialize the Whisper STT service.
        
        Args:
            model_name: Name/size of Whisper model to use
        """
        self.model_name = model_name or settings.STT_MODEL
        self.model = None
        self.processor = None
        logger.info(f"Initializing Whisper STT with model: {self.model_name}")
        
        # Lazy loading - we'll load the model on first use
    
    async def _load_model(self):
        """
        Load the Whisper model if not already loaded.
        This is done asynchronously to avoid blocking the server startup.
        """
        if self.model is not None:
            return
        
        # Run model loading in a separate thread to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._load_model_sync)
    
    def _load_model_sync(self):
        """Synchronous model loading function to be run in executor."""
        try:
            # We're using transformers implementation of Whisper
            from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor
            import torch
            
            # Set device
            device = "cuda" if torch.cuda.is_available() else "cpu"
            torch_dtype = torch.float16 if torch.cuda.is_available() else torch.float32
            
            # Load model and processor
            self.model = AutoModelForSpeechSeq2Seq.from_pretrained(
                self.model_name,
                torch_dtype=torch_dtype,
                low_cpu_mem_usage=True,
                use_safetensors=True
            )
            self.model.to(device)
            
            self.processor = AutoProcessor.from_pretrained(self.model_name)
            
            logger.info(f"Whisper model loaded successfully on {device}")
            
        except Exception as e:
            logger.error(f"Error loading Whisper model: {str(e)}")
            raise
    
    async def transcribe(self, audio_data: Union[str, bytes], format: str = "wav") -> str:
        """
        Transcribe audio data to text.
        
        Args:
            audio_data: Base64 encoded audio string or raw bytes
            format: Audio format (wav, mp3, etc.)
            
        Returns:
            Transcribed text
        """
        # Ensure model is loaded
        await self._load_model()
        
        # Prepare audio data
        if isinstance(audio_data, str):
            # Handle base64 encoded data
            if audio_data.startswith('data:audio'):
                # Extract the base64 part from data URL
                audio_data = audio_data.split(',')[1]
            
            # Decode base64
            audio_bytes = base64.b64decode(audio_data)
        else:
            audio_bytes = audio_data
        
        # Process audio
        try:
            # We need to save the audio to a temporary file
            # as the Whisper model expects a file path
            with tempfile.NamedTemporaryFile(suffix=f".{format}", delete=False) as temp_file:
                temp_path = temp_file.name
                temp_file.write(audio_bytes)
            
            # Run transcription in a separate thread
            loop = asyncio.get_event_loop()
            text = await loop.run_in_executor(
                None, self._transcribe_sync, temp_path
            )
            
            # Clean up temp file
            os.unlink(temp_path)
            
            return text
            
        except Exception as e:
            logger.error(f"Error transcribing audio: {str(e)}")
            return f"Transcription error: {str(e)}"
    
    def _transcribe_sync(self, audio_path: str) -> str:
        """
        Synchronous transcription function to be run in executor.
        
        Args:
            audio_path: Path to audio file
            
        Returns:
            Transcribed text
        """
        try:
            import torch
            import librosa
            
            # Load audio using librosa instead of datasets
            audio_array, sampling_rate = librosa.load(audio_path, sr=16000)
            
            # Process audio with Whisper
            input_features = self.processor(
                audio_array, 
                sampling_rate=16000, 
                return_tensors="pt"
            ).input_features
            
            # Generate tokens
            with torch.no_grad():
                predicted_ids = self.model.generate(
                    input_features.to(self.model.device)
                )
            
            # Decode tokens to text
            transcription = self.processor.batch_decode(
                predicted_ids, 
                skip_special_tokens=True
            )[0]
            
            return transcription.strip()
            
        except Exception as e:
            logger.error(f"Error in transcribe_sync: {str(e)}")
            raise
