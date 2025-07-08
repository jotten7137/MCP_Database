"""API endpoints for the MCP server."""

from fastapi import APIRouter
from .chat import chat_router
from .audio import audio_router

# Create a combined router for all API endpoints
api_router = APIRouter(prefix="/api")

# Include the sub-routers
api_router.include_router(chat_router)
api_router.include_router(audio_router)

# Export the routers
__all__ = ["api_router", "chat_router", "audio_router"]