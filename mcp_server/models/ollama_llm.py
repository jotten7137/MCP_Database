# mcp_server/models/ollama_llm.py
import logging
import json
import aiohttp
from typing import Dict, List, Any, Optional, Union

logger = logging.getLogger("mcp_server.ollama_llm")

class OllamaLLMService:
    """
    Language Model service that uses a local Ollama instance for generating responses.
    """
    
    def __init__(self, model_name: str = "llama2", ollama_url: str = "http://localhost:11434"):
        """
        Initialize the Ollama LLM service.
        
        Args:
            model_name: Name of the model to use in Ollama
            ollama_url: URL of the Ollama API server
        """
        self.model_name = model_name
        self.ollama_url = ollama_url
        logger.info(f"Initialized Ollama LLM service with model: {model_name}")
    
    async def generate_response(self, message: str, 
                              session_id: str = None,
                              conversation: List[Dict[str, Any]] = None,
                              tool_results: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """
        Generate a response using the Ollama API.
        
        Args:
            message: User message
            session_id: Session ID
            conversation: Conversation history (if not provided, will use empty history)
            tool_results: Results from tool calls
            
        Returns:
            Dict with the generated response
        """
        try:
            # Prepare prompt with conversation context and tool results
            prompt = self._format_prompt(message, conversation, tool_results)
            
            # Call Ollama API
            async with aiohttp.ClientSession() as session:
                # Try using system message parameter for better instruction following
                system_message = """You are an AI assistant with tool access.

For weather questions: respond with @weather({"location": "CITY"})
For math questions: respond with @calculator({"expression": "MATH"})
For document searches: respond with @document_search({"query": "SEARCH_QUERY"})
For knowledge base questions: respond with @knowledge_search({"question": "QUESTION"})
For database queries about Titanic data: respond with @database({"action": "query", "connection_name": "titanic_db", "query": "SQL_QUERY", "format": "table"})
For database queries about Iris data: respond with @database({"action": "query", "connection_name": "iris_db", "query": "SQL_QUERY", "format": "table"})

IMPORTANT DATABASE EXAMPLES:
TITANIC DATA:
- "How many passengers were on the Titanic?" → @database({"action": "query", "connection_name": "titanic_db", "query": "SELECT COUNT(*) as passenger_count FROM test_data.titanic_test", "format": "table"})
- "What's the average age of passengers?" → @database({"action": "query", "connection_name": "titanic_db", "query": "SELECT ROUND(AVG(age), 2) as average_age FROM test_data.titanic_test WHERE age IS NOT NULL", "format": "table"})
- "Show me the first 10 passengers" → @database({"action": "query", "connection_name": "titanic_db", "query": "SELECT * FROM test_data.titanic_test LIMIT 10", "format": "table"})

IRIS DATA:
- "How many iris samples are there?" → @database({"action": "query", "connection_name": "iris_db", "query": "SELECT COUNT(*) as sample_count FROM iris", "format": "table"})
- "Show me all iris species" → @database({"action": "query", "connection_name": "iris_db", "query": "SELECT DISTINCT species FROM iris", "format": "table"})
- "What's the average petal length?" → @database({"action": "query", "connection_name": "iris_db", "query": "SELECT ROUND(AVG(petal_length), 2) as avg_petal_length FROM iris", "format": "table"}))

No explanations needed, just the tool call."""
                
                # Simplified prompt for the user message
                user_prompt = self._format_user_prompt(message, conversation, tool_results)
                
                # Debug logging
                logger.info(f"System message: {system_message[:200]}...")
                logger.info(f"User prompt: {user_prompt[:200]}...")
                
                async with session.post(
                    f"{self.ollama_url}/api/generate",
                    json={
                        "model": self.model_name,
                        "prompt": user_prompt,
                        "system": system_message,
                        "stream": False,
                        "options": {
                            "temperature": 0.7,
                            "top_p": 0.9,
                            "max_tokens": 1024
                        }
                    }
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Ollama API error: {error_text}")
                        raise Exception(f"Ollama API error: {response.status}")
                    
                    data = await response.json()
                    
                    # Extract the generated text
                    response_text = data.get("response", "")
                    
                    logger.info(f"Full LLM response: {response_text}")
                    logger.info(f"Generated response: {response_text[:50]}...")
                    
                    # If conversation history provided, add this exchange
                    if conversation is None:
                        conversation = []
                    
                    # Add user message
                    conversation.append({
                        "role": "user",
                        "content": message
                    })
                    
                    # Add assistant message
                    conversation.append({
                        "role": "assistant",
                        "content": response_text
                    })
                    
                    return {
                        "message": response_text,
                        "conversation": conversation,
                        "raw_response": response_text  # Keep raw for tool extraction
                    }
                    
        except Exception as e:
            logger.error(f"Error generating response: {str(e)}")
            return {
                "message": f"Error generating response: {str(e)}",
                "conversation": conversation or []
            }
    
    def _format_prompt(self, message: str, 
                     conversation: Optional[List[Dict[str, Any]]] = None,
                     tool_results: Optional[List[Dict[str, Any]]] = None) -> str:
        """
        Format the prompt for the Ollama API.
        
        Args:
            message: User message
            conversation: Conversation history
            tool_results: Results from tool calls
            
        Returns:
            Formatted prompt string
        """
        # Start with a system prompt that includes tool instructions
        prompt = """You are an AI assistant with access to tools. You MUST use tools when appropriate.

IMPORTANT: When users ask about weather, calculations, or Titanic data, you MUST call the appropriate tool using this EXACT format:
@tool_name({"param": "value"})

Available tools:
- @calculator({"expression": "math expression"}) - REQUIRED for ANY math questions
- @weather({"location": "city name"}) - REQUIRED for ANY weather questions
- @database({"action": "query", "connection_name": "titanic_db", "query": "SQL", "format": "table"}) - REQUIRED for Titanic data questions
- @database({"action": "query", "connection_name": "iris_db", "query": "SQL", "format": "table"}) - REQUIRED for Iris data questions

EXAMPLES:
User: "What's the weather in London?"
You: "I'll check the weather for you. @weather({\"location\": \"London\"})"

User: "What's 15 + 25?"
You: "I'll calculate that. @calculator({\"expression\": \"15 + 25\"})"

User: "How many passengers were on the Titanic?"
You: "I'll query the database. @database({\"action\": \"query\", \"connection_name\": \"titanic_db\", \"query\": \"SELECT COUNT(*) as passenger_count FROM test_data.titanic_test\", \"format\": \"table\"})"

User: "What's the average age of passengers?"
You: "Let me check the database. @database({\"action\": \"query\", \"connection_name\": \"titanic_db\", \"query\": \"SELECT ROUND(AVG(age), 2) as average_age FROM test_data.titanic_test WHERE age IS NOT NULL\", \"format\": \"table\"})"

You MUST use tools for weather, math, and Titanic data. Do NOT give generic responses.

"""
        
        # Add conversation history
        if conversation:
            for msg in conversation:
                role = msg.get("role", "user")
                content = msg.get("content", "")
                
                if role == "user":
                    prompt += f"User: {content}\n\n"
                elif role == "assistant":
                    prompt += f"Assistant: {content}\n\n"
        else:
            # Add few-shot examples if no conversation history
            prompt += """Here are examples of how to respond:

User: What's the weather in Paris?
Assistant: I'll check the weather for you. @weather({"location": "Paris"})

User: What's 10 + 15?
Assistant: I'll calculate that for you. @calculator({"expression": "10 + 15"})

User: How's the weather in Tokyo?
Assistant: Let me get the current weather. @weather({"location": "Tokyo"})

User: How many passengers were on the Titanic?
Assistant: I'll query the database. @database({"action": "query", "connection_name": "titanic_db", "query": "SELECT COUNT(*) as passenger_count FROM test_data.titanic_test", "format": "table"})

User: What's the average age of passengers?
Assistant: Let me check the database. @database({"action": "query", "connection_name": "titanic_db", "query": "SELECT ROUND(AVG(age), 2) as average_age FROM test_data.titanic_test WHERE age IS NOT NULL", "format": "table"})

User: Show me the first 10 passengers
Assistant: I'll get the first 10 passengers from the database. @database({"action": "query", "connection_name": "titanic_db", "query": "SELECT * FROM test_data.titanic_test LIMIT 10", "format": "table"})

User: How many iris samples are there?
Assistant: I'll count the iris samples. @database({"action": "query", "connection_name": "iris_db", "query": "SELECT COUNT(*) as sample_count FROM iris", "format": "table"})

User: Show me all iris species
Assistant: I'll get all the iris species. @database({"action": "query", "connection_name": "iris_db", "query": "SELECT DISTINCT species FROM iris", "format": "table"})

"""
        
        # Add tool results if provided
        if tool_results:
            prompt += "Tool Results:\n"
            for result in tool_results:
                tool_name = result.get("tool_name", "unknown")
                formatted = result.get("formatted", json.dumps(result.get("result", {}), indent=2))
                prompt += f"{tool_name} result:\n{formatted}\n\n"
        
        # Add the current message
        prompt += f"User: {message}\n\nAssistant:"
        
        # Debug: Log the prompt being sent
        logger.info(f"Sending prompt to Ollama: {prompt[:500]}...")
        
        return prompt
    
    def _format_user_prompt(self, message: str, 
                           conversation: Optional[List[Dict[str, Any]]] = None,
                           tool_results: Optional[List[Dict[str, Any]]] = None) -> str:
        """
        Format just the user prompt without system instructions (for use with system parameter).
        """
        prompt = ""
        
        # Add conversation history
        if conversation:
            for msg in conversation:
                role = msg.get("role", "user")
                content = msg.get("content", "")
                
                if role == "user":
                    prompt += f"User: {content}\n\n"
                elif role == "assistant":
                    prompt += f"Assistant: {content}\n\n"
        
        # Add tool results if provided
        if tool_results:
            prompt += "Tool Results:\n"
            for result in tool_results:
                tool_name = result.get("tool_name", "unknown")
                formatted = result.get("formatted", json.dumps(result.get("result", {}), indent=2))
                prompt += f"{tool_name} result:\n{formatted}\n\n"
        
        # Add the current message
        prompt += f"User: {message}\n\nAssistant:"
        
        return prompt