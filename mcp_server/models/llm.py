import logging
import json
import time
from typing import Dict, Any, List, Optional, Union
import asyncio

from ..config import settings

logger = logging.getLogger("mcp_server.llm")

class LLMService:
    """
    Language Model service for generating responses using LLMs.
    
    This class integrates with open source LLMs (like Llama) to generate
    responses based on conversation history and tool results.
    """
    
    def __init__(self, model_name: str = None):
        """
        Initialize the LLM service.
        
        Args:
            model_name: Name of the language model to use
        """
        self.model_name = model_name or settings.LLM_MODEL
        self.model = None
        self.tokenizer = None
        logger.info(f"Initializing LLM service with model: {self.model_name}")
        
        # System prompt template
        self.system_template = """
        You are an AI assistant in an MCP (Message Control Program) system. You have access to various tools
        that help you accomplish tasks. When a tool might be useful, use it by calling it with @tool_name({"param": "value"}).
        
        Available tools:
        {tool_descriptions}
        
        If you need to use a tool, format the call like:
        @weather({"location": "New York", "units": "metric"})
        
        If multiple tools are needed, you can make multiple calls in your response.
        Only use tools when necessary and be concise in your responses.
        """
    
    async def _load_model(self):
        """
        Load the LLM if not already loaded.
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
            # We're using transformers implementation
            from transformers import AutoModelForCausalLM, AutoTokenizer
            import torch
            
            # Set device
            device = "cuda" if torch.cuda.is_available() else "cpu"
            
            # Load tokenizer
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            
            # Load model
            self.model = AutoModelForCausalLM.from_pretrained(
                self.model_name,
                torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
                low_cpu_mem_usage=True,
                device_map="auto"
            )
            
            logger.info(f"LLM loaded successfully on {device}")
            
        except Exception as e:
            logger.error(f"Error loading LLM: {str(e)}")
            raise
    
    def _format_conversation(self, messages: List[Dict[str, Any]], 
                            tool_descriptions: str = "",
                            tool_results: Optional[List[Dict[str, Any]]] = None) -> str:
        """
        Format the conversation history for the LLM.
        
        Args:
            messages: List of conversation messages
            tool_descriptions: String describing available tools
            tool_results: Results from tool calls
            
        Returns:
            Formatted conversation string
        """
        # Create system prompt with tool descriptions
        system_prompt = self.system_template.format(
            tool_descriptions=tool_descriptions
        )
        
        # Format conversation
        formatted = f"<s>[SYSTEM]\n{system_prompt}\n[/SYSTEM]\n\n"
        
        # Add conversation history
        for msg in messages:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            
            if role == "user":
                formatted += f"[USER]: {content}\n\n"
            elif role == "assistant":
                formatted += f"[ASSISTANT]: {content}\n\n"
        
        # Add tool results if provided
        if tool_results:
            formatted += "[TOOL RESULTS]:\n"
            for result in tool_results:
                tool_name = result.get("tool_name", "unknown")
                if "formatted" in result:
                    formatted += f"{tool_name} result:\n{result['formatted']}\n\n"
                elif "result" in result:
                    formatted += f"{tool_name} result:\n{json.dumps(result['result'], indent=2)}\n\n"
            formatted += "[/TOOL RESULTS]\n\n"
        
        # Add assistant prompt
        formatted += "[ASSISTANT]: "
        
        return formatted
    
    async def generate_response(self, message: str, 
                              session_id: str = None,
                              conversation: List[Dict[str, Any]] = None,
                              tool_results: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """
        Generate a response using the LLM.
        
        Args:
            message: User message
            session_id: Session ID
            conversation: Conversation history (if not provided, will use empty history)
            tool_results: Results from tool calls
            
        Returns:
            Dict with the generated response
        """
        # Ensure model is loaded
        await self._load_model()
        
        # Prepare conversation history
        if conversation is None:
            conversation = []
        
        # Add current message to history
        conversation.append({
            "role": "user",
            "content": message
        })
        
        # Get tool descriptions
        tool_descriptions = ""
        if settings.ALLOW_TOOL_CALLS:
            from core.router import RequestRouter
            router = RequestRouter()
            tool_manifests = router.get_tool_manifests()
            
            for tool in tool_manifests:
                tool_descriptions += f"- {tool['name']}: {tool['description']}\n"
                tool_descriptions += f"  Parameters: {json.dumps(tool['parameters'], indent=2)}\n\n"
        
        # Format conversation for the model
        formatted_prompt = self._format_conversation(
            conversation,
            tool_descriptions=tool_descriptions,
            tool_results=tool_results
        )
        
        # Generate response
        try:
            # Run generation in a separate thread
            loop = asyncio.get_event_loop()
            response_text = await loop.run_in_executor(
                None, 
                lambda: self._generate_sync(formatted_prompt)
            )
            
            # Add response to conversation history
            conversation.append({
                "role": "assistant",
                "content": response_text
            })
            
            return {
                "message": response_text,
                "conversation": conversation
            }
            
        except Exception as e:
            logger.error(f"Error generating response: {str(e)}")
            return {
                "message": f"Error generating response: {str(e)}",
                "conversation": conversation
            }
    
    def _generate_sync(self, prompt: str) -> str:
        """
        Synchronous text generation function to be run in executor.
        
        Args:
            prompt: The formatted prompt for the model
            
        Returns:
            Generated text
        """
        try:
            import torch
            
            # Tokenize input
            inputs = self.tokenizer(prompt, return_tensors="pt")
            inputs = {k: v.to(self.model.device) for k, v in inputs.items()}
            
            # Generate
            with torch.no_grad():
                output = self.model.generate(
                    **inputs,
                    max_new_tokens=settings.MAX_TOKENS,
                    temperature=settings.TEMPERATURE,
                    top_p=settings.TOP_P,
                    do_sample=True
                )
            
            # Decode output
            generated_text = self.tokenizer.decode(
                output[0][inputs["input_ids"].shape[1]:],
                skip_special_tokens=True
            )
            
            return generated_text.strip()
            
        except Exception as e:
            logger.error(f"Error in generate_sync: {str(e)}")
            raise
