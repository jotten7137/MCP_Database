# main.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import asyncio
import json
from pydantic import BaseModel
from typing import Dict, List, Optional, Any, Union
import logging

# Import our modules - corrected import paths
from .config import settings
from .core.router import RequestRouter
from .core.session import SessionManager
from .models.stt import WhisperSTT
from .models.llm import LLMService
from .models.tts import TTSService
from .models.ollama_llm import OllamaLLMService
from .models.gtts_service import GTTSService
from .utils.auth import verify_token

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp_server")

app = FastAPI(title="MCP Server")

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Modify in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
session_manager = SessionManager()
router = RequestRouter()
stt_service = WhisperSTT()
#llm_service = LLMService()
llm_service = OllamaLLMService(
    model_name=settings.LLM_MODEL,
    ollama_url=settings.OLLAMA_URL
)
# tts_service = TTSService()
tts_service = GTTSService(language="en")

# Register tools with the router
from .tools.weather import WeatherTool
from .tools.calculator import CalculatorTool
from .tools.document_rag import DocumentRAGTool
from .tools.knowledge_base import KnowledgeBaseTool
from .tools import DATABASE_AVAILABLE

router.register_tool("weather", WeatherTool())
router.register_tool("calculator", CalculatorTool())
router.register_tool("document_search", DocumentRAGTool())
router.register_tool("knowledge_search", KnowledgeBaseTool())

# Register database tool if available
if DATABASE_AVAILABLE:
    from .tools import DatabaseTool
    # Initialize database tool with config file
    database_tool = DatabaseTool(config_file=settings.DATABASE_CONFIG_FILE)
    router.register_tool("database", database_tool)
    logger.info("Database tool registered successfully")
else:
    logger.warning("Database tool not available - missing dependencies")

# Models for API request/response
class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    tool_calls: Optional[List[Dict[str, Any]]] = None

class ChatResponse(BaseModel):
    message: str
    session_id: str
    tool_calls: Optional[List[Dict[str, Any]]] = None
    audio_response_id: Optional[str] = None

class AudioTranscriptionRequest(BaseModel):
    audio_data: str  # Base64 encoded audio
    session_id: Optional[str] = None
    format: str = "wav"  # Audio format

@app.get("/")
async def root():
    return {"status": "running", "message": "MCP Server is operational"}

@app.post("/api/chat", response_model=ChatResponse)
async def chat_endpoint(request: ChatRequest, token: str = Depends(verify_token)):
    """
    Process a text chat message and return a response.
    Optionally executes tool calls if specified.
    """
    logger.info(f"Received chat request: {request}")
    
    session_id = request.session_id or session_manager.create_session()
    logger.info(f"Using session ID: {session_id}")
    
    # Process any tool calls from the request
    tool_results = None
    if request.tool_calls:
        logger.info(f"Processing tool calls: {request.tool_calls}")
        tool_results = await router.process_tool_calls(request.tool_calls)
        logger.info(f"Tool results: {tool_results}")
    
    # Get response from LLM
    logger.info("Calling LLM for response")
    response = await llm_service.generate_response(
        message=request.message,
        session_id=session_id,
        tool_results=tool_results
    )
    logger.info(f"LLM response received: {response['message'][:100]}")
    
    # Check if LLM wants to call any tools
    tool_calls = router.extract_tool_calls(response)
    logger.info(f"Extracted tool calls: {tool_calls}")
    
    # If tools were called, process them and regenerate response
    if tool_calls:
        logger.info(f"Processing {len(tool_calls)} tool calls")
        new_tool_results = await router.process_tool_calls(tool_calls)
        logger.info(f"Tool execution results: {new_tool_results}")
        
        # Generate a follow-up response with tool results
        follow_up_response = await llm_service.generate_response(
            message=request.message,
            session_id=session_id,
            tool_results=new_tool_results
        )
        logger.info(f"Follow-up response: {follow_up_response['message'][:100]}")
        
        # Use the follow-up response as the main response
        response = follow_up_response
        tool_calls = []  # Clear since we've processed them
    
    # Generate audio if needed
    audio_response_id = None
    if settings.GENERATE_AUDIO_RESPONSE:
        logger.info("Generating audio response")
        try:
            # Don't pass model_name parameter to GTTSService
            audio = await tts_service.generate_speech(response["message"])
            audio_response_id = session_manager.store_audio(audio, session_id)
            logger.info(f"Audio response generated with ID: {audio_response_id}")
        except Exception as e:
            logger.error(f"Error generating audio: {str(e)}")
    
    return ChatResponse(
        message=response["message"],
        session_id=session_id,
        tool_calls=tool_calls,
        audio_response_id=audio_response_id
    )

@app.get("/api/test-tts")
async def test_tts_endpoint(text: str = "This is a test of the text to speech system.", token: str = Depends(verify_token)):
    """
    Test the TTS service with a simple text input.
    """
    try:
        # Generate audio
        audio_data = await tts_service.generate_speech(text)
        
        if audio_data and audio_data.startswith('data:audio/'):
            # Extract the audio data from base64
            content_type = "audio/mp3" if "audio/mp3" in audio_data else "audio/wav"
            audio_base64 = audio_data.split(',')[1]
            audio_bytes = base64.b64decode(audio_base64)
            
            # Return the audio
            from fastapi.responses import Response
            return Response(
                content=audio_bytes,
                media_type=content_type,
                headers={"Cache-Control": "no-cache"}
            )
        else:
            raise HTTPException(status_code=500, detail="TTS failed to generate audio")
            
    except Exception as e:
        logger.error(f"Error in test TTS endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=f"TTS error: {str(e)}")


@app.post("/api/transcribe", response_model=Dict[str, str])
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
    
    return {
        "text": text,
        "session_id": session_id
    }


@app.get("/api/audio/{audio_id}")
async def get_audio_response(audio_id: str, token: str = Depends(verify_token)):
    """
    Retrieve an audio response by ID.
    """
    from fastapi.responses import Response, JSONResponse
    
    # Get the audio data
    audio_data = session_manager.get_audio(audio_id)
    
    if not audio_data:
        logger.warning(f"Audio not found: {audio_id}")
        raise HTTPException(status_code=404, detail="Audio not found")
    
    # Log info about the audio
    logger.info(f"Serving audio {audio_id}: {len(audio_data)} bytes")
    
    # Return as MP3
    return Response(
        content=audio_data,
        media_type="audio/mp3",
        headers={
            "Content-Disposition": "inline; filename=audio.mp3",
            "Cache-Control": "no-cache"
        }
    )
    
@app.get("/api/test-mp3")
async def test_mp3_endpoint():
    """
    Test endpoint that returns a simple MP3 file.
    """
    from fastapi.responses import Response
    
    try:
        # Generate a simple MP3 using gTTS
        from gtts import gTTS
        import io
        
        # Create a BytesIO buffer
        mp3_io = io.BytesIO()
        
        # Generate MP3
        tts = gTTS("This is a test audio file from the MCP server.", lang="en")
        tts.write_to_fp(mp3_io)
        mp3_io.seek(0)
        
        # Get the data
        mp3_data = mp3_io.read()
        
        # Log
        logger.info(f"Generated test MP3, size: {len(mp3_data)} bytes")
        
        # Return the MP3
        return Response(
            content=mp3_data,
            media_type="audio/mp3",
            headers={
                "Cache-Control": "no-cache",
                "Content-Disposition": "inline; filename=test.mp3"
            }
        )
        
    except Exception as e:
        logger.error(f"Error in test MP3 endpoint: {str(e)}")
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=500,
            content={"detail": f"Error generating test MP3: {str(e)}"}
        )

# WebSocket endpoint for real-time audio/chat
@app.websocket("/ws/session")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    session_id = None
    
    try:
        # First message should contain authentication
        auth_message = await websocket.receive_text()
        auth_data = json.loads(auth_message)
        
        if not verify_token(auth_data.get("token")):
            await websocket.close(code=1008, reason="Invalid authentication")
            return
        
        session_id = auth_data.get("session_id") or session_manager.create_session()
        await websocket.send_json({"status": "connected", "session_id": session_id})
        
        # Main communication loop
        while True:
            message = await websocket.receive_text()
            data = json.loads(message)
            
            if data["type"] == "chat":
                # Process text message
                response = await llm_service.generate_response(
                    message=data["message"],
                    session_id=session_id
                )
                
                # Check for tool calls
                tool_calls = router.extract_tool_calls(response)
                if tool_calls:
                    # Process tools and get results
                    tool_results = await router.process_tool_calls(tool_calls)
                    # Generate final response with tool results
                    response = await llm_service.generate_response(
                        message=data["message"],
                        session_id=session_id,
                        tool_results=tool_results
                    )
                
                await websocket.send_json({
                    "type": "chat_response",
                    "message": response["message"]
                })
                
                # Generate and send audio if needed
                if settings.GENERATE_AUDIO_RESPONSE:
                    audio = await tts_service.generate_speech(response["message"])
                    await websocket.send_json({
                        "type": "audio_response",
                        "audio_data": audio  # Base64 encoded audio
                    })
                
            elif data["type"] == "audio":
                # Process audio message
                text = await stt_service.transcribe(
                    audio_data=data["audio_data"],
                    format=data.get("format", "wav")
                )
                
                await websocket.send_json({
                    "type": "transcription",
                    "text": text
                })
                
                # Automatically process the transcribed text with LLM
                response = await llm_service.generate_response(
                    message=text,
                    session_id=session_id
                )
                
                await websocket.send_json({
                    "type": "chat_response",
                    "message": response["message"]
                })
                
                # Generate and send audio if needed
                if settings.GENERATE_AUDIO_RESPONSE:
                    audio = await tts_service.generate_speech(response["message"])
                    await websocket.send_json({
                        "type": "audio_response",
                        "audio_data": audio  # Base64 encoded audio
                    })
            
    except WebSocketDisconnect:
        logger.info(f"Client disconnected, session: {session_id}")
    except Exception as e:
        logger.error(f"Error in WebSocket: {str(e)}")
        if websocket.client_state.CONNECTED:
            await websocket.close(code=1011, reason="Server error")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)