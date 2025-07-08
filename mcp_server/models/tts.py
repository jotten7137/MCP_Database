# mcp_server/models/tts.py - Updated version
import logging
import base64
import os
import tempfile
from typing import Dict, Any, Optional, Union
import asyncio
from ..config import settings  # Note the relative import

logger = logging.getLogger("mcp_server.tts")

class TTSService:
    """
    Text-to-Speech service for generating audio from text.
    
    This class uses open source TTS models to convert text responses
    to audio for multimodal interactions.
    """
    
    def __init__(self, model_name: str = None):
        """
        Initialize the TTS service.
        
        Args:
            model_name: Name of the TTS model to use
        """
        self.model_name = model_name or settings.TTS_MODEL
        self.model = None
        self.vocoder = None
        logger.info(f"Initializing TTS service with model: {self.model_name}")
        
        # Lazy loading - we'll load the model on first use
    
    async def _load_model(self):
        """
        Load the TTS model if not already loaded.
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
            # First try Mozilla TTS method
            try:
                from TTS.api import TTS as MozillaTTS
                
                # Load model (with more specific error handling)
                try:
                    self.model = MozillaTTS(model_name=self.model_name)
                    logger.info("Mozilla TTS model loaded successfully")
                    return
                except Exception as tts_error:
                    logger.warning(f"Could not load Mozilla TTS model: {str(tts_error)}")
                    logger.warning("Trying alternative approach...")
            except ImportError:
                logger.warning("Mozilla TTS not available. Trying alternative...")
            
            # Try alternative approach with TTS directly
            try:
                # If we reach here, we'll try the lower-level TTS approach
                from TTS.utils.manage import ModelManager
                from TTS.utils.synthesizer import Synthesizer
                
                # Find available models
                manager = ModelManager()
                model_path, config_path, model_item = manager.download_model(self.model_name)
                vocoder_name = model_item.get("default_vocoder", "")
                
                if vocoder_name:
                    vocoder_path, vocoder_config_path, _ = manager.download_model(vocoder_name)
                else:
                    vocoder_path, vocoder_config_path = None, None
                
                # Load synthesizer
                self.model = Synthesizer(
                    tts_checkpoint=model_path,
                    tts_config_path=config_path,
                    vocoder_checkpoint=vocoder_path,
                    vocoder_config=vocoder_config_path
                )
                logger.info("TTS Synthesizer loaded successfully")
                return
            except Exception as synth_error:
                logger.warning(f"Could not load TTS Synthesizer: {str(synth_error)}")
            
            # Fall back to gTTS as last resort
            try:
                from gtts import gTTS
                self.model = "gtts"  # Just a marker for using gTTS
                logger.info("Using gTTS as fallback TTS engine")
                return
            except ImportError:
                logger.error("Could not load any TTS engine. Audio generation will be unavailable.")
                
        except Exception as e:
            logger.error(f"Error loading TTS model: {str(e)}")
            raise
    
    # Update the generate_speech method to accept a model parameter
    async def generate_speech(self, text: str, model_name: str = None) -> str:
        """
        Generate speech audio from text.
        
        Args:
            text: Text to convert to speech
            model_name: Override the default TTS model
            
        Returns:
            Base64 encoded audio string
        """
        # If a different model is requested, create a temporary TTS service
        if model_name and model_name != self.model_name:
            temp_service = TTSService(model_name=model_name)
            return await temp_service.generate_speech(text)
        
        # Otherwise, use the default model
        # Ensure model is loaded
        await self._load_model()
        
        # Process text
        try:
            # Run generation in a separate thread
            loop = asyncio.get_event_loop()
            audio_base64 = await loop.run_in_executor(
                None, lambda: self._generate_speech_sync(text)
            )
            
            return audio_base64
            
        except Exception as e:
            logger.error(f"Error generating speech: {str(e)}")
            return ""
    
    def _generate_speech_sync(self, text: str) -> str:
        """
        Synchronous speech generation function to be run in executor.
        
        Args:
            text: Text to convert to speech
            
        Returns:
            Base64 encoded audio string
        """
        try:
            # Create a temporary file for the audio
            with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as temp_file:
                temp_path = temp_file.name
            
            # Generate speech based on what's available
            if isinstance(self.model, str) and self.model == "gtts":
                # Use gTTS as fallback
                from gtts import gTTS
                tts = gTTS(text=text, lang="en", slow=False)
                tts.save(temp_path)
            else:
                # Use the loaded model
                try:
                    # Try Mozilla TTS API approach first
                    if hasattr(self.model, "tts_to_file"):
                        self.model.tts_to_file(text=text, file_path=temp_path)
                    # Try synthesizer approach
                    elif hasattr(self.model, "tts"):
                        wav = self.model.tts(text)
                        self.model.save_wav(wav, temp_path)
                    else:
                        raise ValueError("Unknown TTS model type")
                except Exception as tts_error:
                    logger.error(f"TTS synthesis failed: {str(tts_error)}")
                    # Emergency fallback to gTTS
                    from gtts import gTTS
                    tts = gTTS(text=text, lang="en", slow=False)
                    tts.save(temp_path)
            
            # Read the file and encode to base64
            with open(temp_path, "rb") as audio_file:
                audio_bytes = audio_file.read()
            
            # Clean up temp file
            os.unlink(temp_path)
            
            # Encode to base64
            audio_base64 = base64.b64encode(audio_bytes).decode('utf-8')
            
            # Add data URL prefix
            audio_base64 = f"data:audio/wav;base64,{audio_base64}"
            
            return audio_base64
            
        except Exception as e:
            logger.error(f"Error in generate_speech_sync: {str(e)}")
            raise