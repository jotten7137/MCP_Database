# core/session.py - Session Management for MCP Server

import uuid
import time
import json
import os
from typing import Dict, Any, Optional, List, Union
import base64
import logging

from ..config import settings 

logger = logging.getLogger("mcp_server.session")

class SessionManager:
    """
    Manages user sessions and temporary data storage.
    
    This class:
    1. Creates and tracks user sessions
    2. Stores session state (conversation history, etc.)
    3. Handles temporary storage for things like audio files
    """
    
    def __init__(self):
        """Initialize the session manager."""
        self.sessions = {}
        self.audio_storage = {}
        self.cache_dir = settings.CACHE_DIR
        
        # Ensure cache directory exists
        os.makedirs(self.cache_dir, exist_ok=True)
    
    def create_session(self) -> str:
        """
        Create a new session.
        
        Returns:
            Session ID string
        """
        session_id = str(uuid.uuid4())
        self.sessions[session_id] = {
            "created_at": time.time(),
            "last_activity": time.time(),
            "conversation": [],
            "metadata": {}
        }
        return session_id
    
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a session by ID.
        
        Args:
            session_id: Session ID to retrieve
            
        Returns:
            Session data or None if not found
        """
        return self.sessions.get(session_id)
    
    def update_session(self, session_id: str, data: Dict[str, Any]) -> bool:
        """
        Update session data.
        
        Args:
            session_id: Session ID to update
            data: New data to merge with existing session
            
        Returns:
            True if successful, False if session not found
        """
        if session_id not in self.sessions:
            return False
            
        self.sessions[session_id].update(data)
        self.sessions[session_id]["last_activity"] = time.time()
        return True
    
    def add_message(self, session_id: str, role: str, content: str) -> bool:
        """
        Add a message to the conversation history.
        
        Args:
            session_id: Session ID to update
            role: Message role (user, assistant, system)
            content: Message content
            
        Returns:
            True if successful, False if session not found
        """
        if session_id not in self.sessions:
            return False
            
        self.sessions[session_id]["conversation"].append({
            "role": role,
            "content": content,
            "timestamp": time.time()
        })
        self.sessions[session_id]["last_activity"] = time.time()
        return True
    
    def get_conversation(self, session_id: str) -> List[Dict[str, Any]]:
        """
        Get the conversation history for a session.
        
        Args:
            session_id: Session ID to retrieve
            
        Returns:
            List of conversation messages or empty list if session not found
        """
        if session_id not in self.sessions:
            return []
            
        return self.sessions[session_id]["conversation"]
    
    def store_audio(self, audio_data: Union[str, bytes, None], session_id: str) -> Optional[str]:
        """
        Store audio data and return an ID for retrieval.
        
        Args:
            audio_data: Base64 encoded audio string, raw bytes, or None
            session_id: Associated session ID
            
        Returns:
            Audio data ID for retrieval or None if audio_data is None
        """
        if audio_data is None:
            logger.warning("Attempted to store None audio data")
            return None
        
        audio_id = str(uuid.uuid4())
        
        # Convert to bytes if needed
        if isinstance(audio_data, str):
            # Assume it's base64 encoded
            if audio_data.startswith('data:audio/'):
                # Extract the base64 part from data URL
                audio_data = audio_data.split(',')[1]
                audio_bytes = base64.b64decode(audio_data)
            else:
                # Try to decode as base64
                try:
                    audio_bytes = base64.b64decode(audio_data)
                except Exception as e:
                    logger.warning(f"Failed to decode audio as base64: {e}")
                    audio_bytes = audio_data.encode('utf-8')
        else:
            audio_bytes = audio_data
        
        # Log size for debugging
        logger.info(f"Storing audio {audio_id}: {len(audio_bytes)} bytes")
    
        # Store in memory (for simplicity)
        self.audio_storage[audio_id] = audio_bytes
        
        return audio_id

    def get_audio(self, audio_id: str) -> Optional[bytes]:
        """
        Retrieve stored audio data.
        
        Args:
            audio_id: ID of the audio data to retrieve
            
        Returns:
            Audio bytes or None if not found
        """
        if audio_id not in self.audio_storage:
            logger.warning(f"Audio ID not found: {audio_id}")
            return None
        
        audio_data = self.audio_storage[audio_id]
        
        # Check if it's a dictionary (old format) or bytes (new format)
        if isinstance(audio_data, dict):
            audio_bytes = audio_data.get("data")
            logger.info(f"Retrieved audio {audio_id} from dictionary: {len(audio_bytes) if audio_bytes else 0} bytes")
            return audio_bytes
        else:
            logger.info(f"Retrieved audio {audio_id}: {len(audio_data)} bytes")
            return audio_data
    
    def cleanup_old_sessions(self, max_age_seconds: int = 3600) -> int:
        """
        Clean up sessions older than the specified age.
        
        Args:
            max_age_seconds: Maximum session age in seconds
            
        Returns:
            Number of sessions removed
        """
        current_time = time.time()
        sessions_to_remove = [
            sid for sid, data in self.sessions.items()
            if current_time - data["last_activity"] > max_age_seconds
        ]
        
        for sid in sessions_to_remove:
            del self.sessions[sid]
            
        return len(sessions_to_remove)
    
    def cleanup_old_audio(self, max_age_seconds: int = 3600) -> int:
        """
        Clean up audio data older than the specified age.
        
        Args:
            max_age_seconds: Maximum audio age in seconds
            
        Returns:
            Number of audio files removed
        """
        current_time = time.time()
        audio_to_remove = [
            aid for aid, data in self.audio_storage.items()
            if current_time - data["created_at"] > max_age_seconds
        ]
        
        for aid in audio_to_remove:
            del self.audio_storage[aid]
            
        return len(audio_to_remove)