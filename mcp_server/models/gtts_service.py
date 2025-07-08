# mcp_server/models/gtts_service.py - Full corrected implementation
import logging
import tempfile
import base64
import os
from typing import Optional, Union
import asyncio

logger = logging.getLogger("mcp_server.gtts")

class GTTSService:
    """
    Text-to-Speech service using Google's TTS API (gTTS).
    
    This is a simpler and more reliable alternative to Mozilla TTS.
    """
    
    def __init__(self, language: str = "en", slow: bool = False):
        """
        Initialize the TTS service.
        
        Args:
            language: Language code for speech
            slow: Whether to speak slowly
        """
        self.language = language
        self.slow = slow
        logger.info(f"Initialized gTTS service with language: {language}")
        
        # Check if gtts is installed
        try:
            import gtts
            self.gtts_available = True
            logger.info("gTTS library found and available")
        except ImportError:
            logger.warning("gtts package not found. Please install with: pip install gtts")
            self.gtts_available = False
    
    async def generate_speech(self, text: str, **kwargs) -> Optional[bytes]:
        """
        Generate speech audio from text.
        
        Args:
            text: Text to convert to speech
            **kwargs: Additional parameters (ignored for compatibility)
            
        Returns:
            Raw audio bytes or None if failed
        """
        # Log and ignore any extra parameters
        if kwargs:
            logger.debug(f"Ignoring extra parameters for compatibility: {kwargs}")
            
        if not self.gtts_available:
            logger.error("Cannot generate speech: gtts package not available")
            return None
            
        # Run generation in a separate thread
        loop = asyncio.get_event_loop()
        audio_bytes = await loop.run_in_executor(
            None, lambda: self._generate_speech_sync(text)
        )
        
        return audio_bytes
    
    def _generate_speech_sync(self, text: str) -> Optional[bytes]:
        """
        Synchronous speech generation function.
        
        Args:
            text: Text to convert to speech
            
        Returns:
            Raw audio bytes or None if failed
        """
        try:
            # Import here to avoid issues if package is missing
            from gtts import gTTS
            
            # Create a BytesIO buffer to avoid file system I/O
            import io
            mp3_io = io.BytesIO()
            
            # Generate speech directly to the buffer
            tts = gTTS(text=text, lang=self.language, slow=self.slow)
            tts.write_to_fp(mp3_io)
            mp3_io.seek(0)
            
            # Get the raw bytes
            audio_bytes = mp3_io.read()
            
            logger.info(f"Generated MP3 audio: {len(audio_bytes)} bytes")
            return audio_bytes
            
        except Exception as e:
            logger.error(f"Error in _generate_speech_sync: {str(e)}")
            return None