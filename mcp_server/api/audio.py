# api/audio.py - Audio Processing API Endpoints

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Dict, Optional, Any
import logging
import base64

from ..config import settings
from ..core.session import SessionManager
from ..models.stt import WhisperSTT
from ..utils.auth import verify_token

# Get instances from main application
from ..main import session_manager, stt_service

# Setup logging
logger = logging.getLogger("mcp_server.api.audio")

# Create router
audio_router = APIRouter()

# Models for request/response
class AudioTranscriptionRequest(BaseModel):
    audio_data: str  # Base64 encoded audio
    session_id: Optional[str] = None
    format: str = "wav"  # Audio format

class AudioTranscriptionResponse(BaseModel):
    text: str
    session_id: str

@audio_router.post("/transcribe", response_model=AudioTranscriptionResponse)
async def transcribe_audio(request: AudioTranscriptionRequest, token: str = Depends(verify_token)):
    """
    Transcribe audio to text using the STT service.
    """
    session_id = request.session_id or session_manager.create_session()
    
    # Decode and process the audio
    text = await stt_service.transcribe(
        audio_data=request.audio_data,
        format=request.format
    )
    
    # Add transcribed message to conversation history
    if text:
        session_manager.add_message(session_id, "user", text)
    
    return AudioTranscriptionResponse(
        text=text,
        session_id=session_id
    )

@audio_router.get("/audio/{audio_id}")
async def get_audio_response(audio_id: str, token: str = Depends(verify_token)):
    """
    Retrieve an audio response by ID.
    """
    audio_data = session_manager.get_audio(audio_id)
    if not audio_data:
        raise HTTPException(status_code=404, detail="Audio response not found")
    
    # Return audio file as binary response
    from fastapi.responses import Response
    return Response(content=audio_data, media_type="audio/wav")