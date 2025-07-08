import requests
import json
import base64
import os
import time
import argparse
import websocket
import threading
import sounddevice as sd
import soundfile as sf
import numpy as np
from io import BytesIO
import wave
import pyaudio
import tempfile
from typing import Dict, Any, Optional, Callable, List

class MCPClient:
    """
    Client for interacting with the MCP Server.
    
    This client supports both REST API and WebSocket connections
    for text and audio interactions with the MCP server.
    """
    
    def __init__(self, server_url: str, api_key: str = None):
        """
        Initialize the MCP client.
        
        Args:
            server_url: URL of the MCP server (e.g., "http://localhost:8000")
            api_key: API key for authentication
        """
        self.server_url = server_url
        self.api_key = api_key
        self.session_id = None
        self.ws = None
        self.ws_thread = None
        self.running = False
        
        # Callback functions
        self.on_message = None
        self.on_audio = None
        self.on_transcription = None
        self.on_error = None
        
        # Audio settings
        self.audio_format = pyaudio.paInt16
        self.channels = 1
        self.rate = 16000
        self.chunk = 1024
        self.audio = pyaudio.PyAudio()
        self.recording = False
        self.frames = []
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with authentication."""
        headers = {
            "Content-Type": "application/json"
        }
        
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        
        return headers
    
    def connect(self) -> bool:
        """
        Connect to the server and get a session ID.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Try to get a session via REST API
            response = requests.post(
                f"{self.server_url}/api/chat",
                headers=self._get_headers(),
                json={"message": "Hello"}
            )
            
            if response.status_code == 200:
                data = response.json()
                self.session_id = data.get("session_id")
                print(f"Connected to MCP server with session ID: {self.session_id}")
                return True
            else:
                print(f"Failed to connect: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"Error connecting to server: {str(e)}")
            return False
    
    def send_message(self, message: str) -> Optional[Dict[str, Any]]:
        """
        Send a text message to the server.
        
        Args:
            message: Text message to send
            
        Returns:
            Response data or None if failed
        """
        try:
            response = requests.post(
                f"{self.server_url}/api/chat",
                headers=self._get_headers(),
                json={
                    "message": message,
                    "session_id": self.session_id
                }
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Check for audio response
                if data.get("audio_response_id"):
                    self._fetch_audio(data["audio_response_id"])
                
                return data
            else:
                print(f"Error sending message: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            print(f"Error sending message: {str(e)}")
            return None
    
    def _fetch_audio(self, audio_id: str) -> Optional[bytes]:
        """
        Fetch audio response from the server.
        
        Args:
            audio_id: ID of the audio response
            
        Returns:
            Audio bytes or None if failed
        """
        try:
            response = requests.get(
                f"{self.server_url}/api/audio/{audio_id}",
                headers=self._get_headers()
            )
            
            if response.status_code == 200:
                audio_data = response.content
                
                # Play audio if callback is set
                if self.on_audio:
                    self.on_audio(audio_data)
                else:
                    self._play_audio(audio_data)
                
                return audio_data
            else:
                print(f"Error fetching audio: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            print(f"Error fetching audio: {str(e)}")
            return None
    
    def send_audio(self, audio_data: bytes, format: str = "wav") -> Optional[Dict[str, Any]]:
        """
        Send audio data to the server for transcription.
        
        Args:
            audio_data: Audio bytes
            format: Audio format
            
        Returns:
            Response data or None if failed
        """
        try:
            # Convert to base64
            audio_base64 = base64.b64encode(audio_data).decode('utf-8')
            
            response = requests.post(
                f"{self.server_url}/api/transcribe",
                headers=self._get_headers(),
                json={
                    "audio_data": audio_base64,
                    "session_id": self.session_id,
                    "format": format
                }
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # If transcription successful, automatically send as message
                if data.get("text"):
                    print(f"Transcription: {data['text']}")
                    
                    # Call transcription callback if set
                    if self.on_transcription:
                        self.on_transcription(data["text"])
                    
                    # Send the transcribed text as a message
                    return self.send_message(data["text"])
                
                return data
            else:
                print(f"Error sending audio: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            print(f"Error sending audio: {str(e)}")
            return None
    
    def _play_audio(self, audio_data: bytes) -> None:
        """
        Play audio data using sounddevice.
        
        Args:
            audio_data: Audio bytes
        """
        try:
            # Create in-memory file-like object
            audio_file = BytesIO(audio_data)
            
            # Read audio data using soundfile
            data, samplerate = sf.read(audio_file)
            
            # Play audio
            sd.play(data, samplerate)
            sd.wait()
            
        except Exception as e:
            print(f"Error playing audio: {str(e)}")
    
    def start_recording(self) -> None:
        """Start recording audio from the microphone."""
        if self.recording:
            return
            
        self.recording = True
        self.frames = []
        
        # Open microphone stream
        self.stream = self.audio.open(
            format=self.audio_format,
            channels=self.channels,
            rate=self.rate,
            input=True,
            frames_per_buffer=self.chunk
        )
        
        print("Recording started...")
        
        # Start recording thread
        self.record_thread = threading.Thread(target=self._record_audio)
        self.record_thread.daemon = True
        self.record_thread.start()
    
    def stop_recording(self) -> Optional[bytes]:
        """
        Stop recording and return the recorded audio.
        
        Returns:
            Recorded audio bytes or None if failed
        """
        if not self.recording:
            return None
            
        self.recording = False
        
        # Wait for recording thread to finish
        if hasattr(self, 'record_thread') and self.record_thread.is_alive():
            self.record_thread.join()
        
        # Close stream
        if hasattr(self, 'stream'):
            self.stream.stop_stream()
            self.stream.close()
        
        print("Recording stopped.")
        
        # Convert frames to WAV
        with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as tmp_file:
            wf = wave.open(tmp_file.name, 'wb')
            wf.setnchannels(self.channels)
            wf.setsampwidth(self.audio.get_sample_size(self.audio_format))
            wf.setframerate(self.rate)
            wf.writeframes(b''.join(self.frames))
            wf.close()
            
            # Read the WAV file
            with open(tmp_file.name, 'rb') as f:
                audio_data = f.read()
            
            # Delete the temporary file
            os.unlink(tmp_file.name)
            
            return audio_data
    
    def _record_audio(self) -> None:
        """Record audio from the microphone."""
        while self.recording:
            try:
                data = self.stream.read(self.chunk)
                self.frames.append(data)
            except Exception as e:
                print(f"Error recording audio: {str(e)}")
                self.recording = False
                break
    
    def connect_websocket(self) -> bool:
        """
        Connect to the server via WebSocket.
        
        Returns:
            True if successful, False otherwise
        """
        if self.ws:
            return True
            
        try:
            # Convert HTTP URL to WebSocket URL
            ws_url = self.server_url.replace('http://', 'ws://').replace('https://', 'wss://')
            ws_url = f"{ws_url}/ws/session"
            
            # Connect to WebSocket
            self.ws = websocket.WebSocketApp(
                ws_url,
                on_open=self._on_ws_open,
                on_message=self._on_ws_message,
                on_error=self._on_ws_error,
                on_close=self._on_ws_close
            )
            
            # Start WebSocket thread
            self.running = True
            self.ws_thread = threading.Thread(target=self._run_websocket)
            self.ws_thread.daemon = True
            self.ws_thread.start()
            
            # Wait for connection
            time.sleep(1)
            
            return True
            
        except Exception as e:
            print(f"Error connecting to WebSocket: {str(e)}")
            return False
    
    def _run_websocket(self) -> None:
        """Run the WebSocket connection."""
        while self.running:
            try:
                self.ws.run_forever()
                if not self.running:
                    break
                print("WebSocket disconnected. Reconnecting...")
                time.sleep(2)
            except Exception as e:
                print(f"WebSocket error: {str(e)}")
                if not self.running:
                    break
                time.sleep(2)
    
    def _on_ws_open(self, ws) -> None:
        """Callback when WebSocket connection is established."""
        print("WebSocket connection opened")
        
        # Send authentication
        auth_message = {
            "token": self.api_key,
            "session_id": self.session_id
        }
        ws.send(json.dumps(auth_message))
    
    def _on_ws_message(self, ws, message) -> None:
        """Callback when WebSocket message is received."""
        try:
            data = json.loads(message)
            
            # Handle different message types
            if data.get("status") == "connected":
                self.session_id = data.get("session_id")
                print(f"WebSocket connected with session ID: {self.session_id}")
            
            elif data.get("type") == "chat_response":
                print(f"Response: {data.get('message')}")
                
                # Call message callback if set
                if self.on_message:
                    self.on_message(data.get('message'))
            
            elif data.get("type") == "audio_response":
                audio_base64 = data.get("audio_data")
                
                if audio_base64:
                    # Remove data URL prefix if present
                    if audio_base64.startswith('data:audio'):
                        audio_base64 = audio_base64.split(',')[1]
                    
                    # Decode base64
                    audio_data = base64.b64decode(audio_base64)
                    
                    # Play audio if callback is set
                    if self.on_audio:
                        self.on_audio(audio_data)
                    else:
                        self._play_audio(audio_data)
            
            elif data.get("type") == "transcription":
                print(f"Transcription: {data.get('text')}")
                
                # Call transcription callback if set
                if self.on_transcription:
                    self.on_transcription(data.get('text'))
            
        except Exception as e:
            print(f"Error handling WebSocket message: {str(e)}")
    
    def _on_ws_error(self, ws, error) -> None:
        """Callback when WebSocket error occurs."""
        print(f"WebSocket error: {str(error)}")
        
        # Call error callback if set
        if self.on_error:
            self.on_error(error)
    
    def _on_ws_close(self, ws, close_status_code, close_reason) -> None:
        """Callback when WebSocket connection is closed."""
        print(f"WebSocket closed: {close_status_code} - {close_reason}")
    
    def send_ws_message(self, message: str) -> bool:
        """
        Send a text message via WebSocket.
        
        Args:
            message: Text message to send
            
        Returns:
            True if successful, False otherwise
        """
        if not self.ws:
            print("WebSocket not connected")
            return False
            
        try:
            data = {
                "type": "chat",
                "message": message
            }
            self.ws.send(json.dumps(data))
            return True
            
        except Exception as e:
            print(f"Error sending WebSocket message: {str(e)}")
            return False
    
    def send_ws_audio(self, audio_data: bytes, format: str = "wav") -> bool:
        """
        Send audio data via WebSocket.
        
        Args:
            audio_data: Audio bytes
            format: Audio format
            
        Returns:
            True if successful, False otherwise
        """
        if not self.ws:
            print("WebSocket not connected")
            return False
            
        try:
            # Convert to base64
            audio_base64 = base64.b64encode(audio_data).decode('utf-8')
            
            data = {
                "type": "audio",
                "audio_data": audio_base64,
                "format": format
            }
            self.ws.send(json.dumps(data))
            return True
            
        except Exception as e:
            print(f"Error sending WebSocket audio: {str(e)}")
            return False
    
    def close(self) -> None:
        """Close the client and release resources."""
        self.running = False
        
        # Close WebSocket
        if self.ws:
            self.ws.close()
            
        # Close audio
        if hasattr(self, 'stream') and self.stream:
            self.stream.stop_stream()
            self.stream.close()
            
        self.audio.terminate()
        
        print("MCP client closed")

def main():
    """Main function to run the MCP client CLI."""
    parser = argparse.ArgumentParser(description="MCP Client")
    parser.add_argument("--server", type=str, default="http://localhost:8000", help="MCP server URL")
    parser.add_argument("--key", type=str, default=None, help="API key for authentication")
    parser.add_argument("--websocket", action="store_true", help="Use WebSocket connection")
    args = parser.parse_args()
    
    client = MCPClient(args.server, args.key)
    
    # Connect to server
    if not client.connect():
        print("Failed to connect to server")
        return
    
    # Connect WebSocket if requested
    if args.websocket:
        if not client.connect_websocket():
            print("Failed to connect WebSocket")
            return
    
    print("MCP Client CLI")
    print("Commands:")
    print("  /exit - Exit the client")
    print("  /record - Start recording audio")
    print("  /stop - Stop recording and send audio")
    print("  /ws - Toggle WebSocket mode")
    print("  Any other text will be sent as a message")
    print("")
    
    websocket_mode = args.websocket
    
    try:
        while True:
            try:
                user_input = input("> ")
                
                if user_input.lower() == "/exit":
                    break
                    
                elif user_input.lower() == "/record":
                    client.start_recording()
                    
                elif user_input.lower() == "/stop":
                    audio_data = client.stop_recording()
                    if audio_data:
                        print("Sending audio...")
                        if websocket_mode:
                            client.send_ws_audio(audio_data)
                        else:
                            client.send_audio(audio_data)
                    
                elif user_input.lower() == "/ws":
                    websocket_mode = not websocket_mode
                    print(f"WebSocket mode: {'On' if websocket_mode else 'Off'}")
                    
                    if websocket_mode and not client.ws:
                        client.connect_websocket()
                    
                else:
                    if websocket_mode:
                        client.send_ws_message(user_input)
                    else:
                        response = client.send_message(user_input)
                        if response:
                            print(f"Response: {response.get('message')}")
                
            except KeyboardInterrupt:
                break
                
    finally:
        client.close()

if __name__ == "__main__":
    main()
