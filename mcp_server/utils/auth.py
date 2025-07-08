import logging
import time
import jwt
from typing import Optional, Dict, Any
from fastapi import HTTPException, Depends, Request
from fastapi.security import APIKeyHeader

from ..config import settings

logger = logging.getLogger("mcp_server.auth")

# API key header for simple authentication
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def verify_token(
    api_key: str = Depends(api_key_header),
    request: Request = None
) -> str:
    """
    Verify API key or JWT token for authentication.
    
    This function acts as a FastAPI dependency for endpoint protection.
    It checks if the provided API key or JWT token is valid.
    
    Args:
        api_key: API key from header
        request: FastAPI request object
        
    Returns:
        The validated token
        
    Raises:
        HTTPException: If authentication fails
    """
    # Skip auth if not required
    if not settings.AUTH_REQUIRED:
        return "no_auth_required"
    
    # Check for API key in header
    if api_key:
        if api_key == settings.API_KEY_SECRET:
            return api_key
    
    # Check for JWT token in Authorization header
    if request:
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header.replace("Bearer ", "")
            try:
                # Verify JWT token
                payload = jwt.decode(
                    token,
                    settings.API_KEY_SECRET,
                    algorithms=["HS256"]
                )
                
                # Check if token is expired
                if payload.get("exp", 0) < time.time():
                    raise HTTPException(
                        status_code=401,
                        detail="Token expired"
                    )
                
                return token
            except jwt.PyJWTError:
                pass
    
    # Authentication failed
    raise HTTPException(
        status_code=401,
        detail="Invalid authentication credentials"
    )

def create_token(user_id: str, expiration_seconds: int = 3600) -> str:
    """
    Create a JWT token for a user.
    
    Args:
        user_id: User identifier
        expiration_seconds: Token validity in seconds
        
    Returns:
        JWT token string
    """
    expire_time = time.time() + expiration_seconds
    
    payload = {
        "sub": user_id,
        "exp": expire_time,
        "iat": time.time()
    }
    
    token = jwt.encode(
        payload,
        settings.API_KEY_SECRET,
        algorithm="HS256"
    )
    
    return token

def decode_token(token: str) -> Optional[Dict[str, Any]]:
    """
    Decode and validate a JWT token.
    
    Args:
        token: JWT token to decode
        
    Returns:
        Token payload or None if invalid
    """
    try:
        payload = jwt.decode(
            token,
            settings.API_KEY_SECRET,
            algorithms=["HS256"]
        )
        return payload
    except jwt.PyJWTError as e:
        logger.warning(f"Error decoding token: {str(e)}")
        return None
