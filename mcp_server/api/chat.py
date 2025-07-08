# api/chat.py - Text Chat API Endpoints

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Dict, List, Optional, Any, Union
import logging

from ..config import settings
from ..core.router import RequestRouter
from ..core.session import SessionManager
from ..models.llm import LLMService
from ..models.tts import TTSService
from ..utils.auth import verify_token

# Get instances from main application
from ..main import session_manager, router, llm_service, tts_service

# Setup logging
logger = logging.getLogger("mcp_server.api.chat")

# Create router
chat_router = APIRouter()

# Models for request/response
class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    tool_calls: Optional[List[Dict[str, Any]]] = None

class ChatResponse(BaseModel):
    message: str
    session_id: str
    tool_calls: Optional[List[Dict[str, Any]]] = None
    audio_response_id: Optional[str] = None

@chat_router.post("/chat", response_model=ChatResponse)
async def chat_endpoint(request: ChatRequest, token: str = Depends(verify_token)):
    """
    Process a text chat message and return a response.
    Optionally executes tool calls if specified.
    """
    session_id = request.session_id or session_manager.create_session()
    
    # Add user message to conversation history
    session_manager.add_message(session_id, "user", request.message)
    
    # Process any tool calls from the request
    tool_results = None
    if request.tool_calls:
        tool_results = await router.process_tool_calls(request.tool_calls)
    
    # Get conversation history
    conversation = session_manager.get_conversation(session_id)
    
    # Get response from LLM
    response = await llm_service.generate_response(
        message=request.message,
        session_id=session_id,
        conversation=conversation,
        tool_results=tool_results
    )
    
    # Add assistant response to conversation history
    session_manager.add_message(session_id, "assistant", response["message"])
    
    # Check if LLM wants to call any tools
    tool_calls = router.extract_tool_calls(response)
    
    # Generate audio if needed
    audio_response_id = None
    if settings.GENERATE_AUDIO_RESPONSE:
        audio = await tts_service.generate_speech(response["message"])
        audio_response_id = session_manager.store_audio(audio, session_id)
    
    return ChatResponse(
        message=response["message"],
        session_id=session_id,
        tool_calls=tool_calls,
        audio_response_id=audio_response_id
    )