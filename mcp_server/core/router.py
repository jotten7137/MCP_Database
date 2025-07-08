# core/router.py - Request Router for MCP Server

import logging
import json
import re
from typing import Dict, List, Any, Optional, Union
import asyncio

# You might need to import BaseTool when validating tool instances
from ..tools.base import BaseTool

logger = logging.getLogger("mcp_server.router")

class RequestRouter:
    """
    Routes requests and manages tool integrations in the MCP server.
    
    This class:
    1. Maintains a registry of available tools
    2. Handles extracting tool calls from LLM responses
    3. Processes tool calls and returns results
    """
    
    def __init__(self):
        """Initialize the request router with an empty tool registry."""
        self.tools = {}
        logger.info("Request Router initialized")
    
    def register_tool(self, name: str, tool_instance: Any) -> None:
        """
        Register a tool with the router.
        
        Args:
            name: Name to register the tool under
            tool_instance: Instance of a tool class (must inherit from BaseTool)
        """
        # Optionally add type checking
        # if not isinstance(tool_instance, BaseTool):
        #     raise TypeError(f"Tool must be an instance of BaseTool, got {type(tool_instance)}")
        
        self.tools[name] = tool_instance
        logger.info(f"Tool registered: {name}")
    
    def get_tool_manifests(self) -> List[Dict[str, Any]]:
        """
        Get all tool manifests for providing to the LLM.
        
        Returns:
            List of tool manifests
        """
        return [tool.manifest for tool in self.tools.values()]
    
    def extract_tool_calls(self, llm_response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract tool calls from an LLM response.
        
        This function parses the LLM output to identify tool call requests.
        Different LLMs may format tool calls differently, so this might need adaptation.
        
        Args:
            llm_response: Response dictionary from the LLM
            
        Returns:
            List of parsed tool calls
        """
        # Method 1: Check if the LLM has a dedicated tool_calls field (like OpenAI)
        if "tool_calls" in llm_response:
            return llm_response["tool_calls"]
        
        # Method 2: Parse from message text using regex patterns
        # This is a fallback for LLMs that don't have structured tool calling
        message = llm_response.get("message", "")
        if not message:
            # Try alternate keys
            message = llm_response.get("raw_response", "")
        
        logger.info(f"Extracting tool calls from message: {message[:200]}...")
        
        # Look for patterns like: @tool_name({"param": "value"})
        # Make the pattern more flexible to handle variations
        tool_call_pattern = r'@(\w+)\s*\(\s*({[^}]*})\s*\)'
        matches = re.findall(tool_call_pattern, message, re.DOTALL)
        
        tool_calls = []
        for tool_name, params_str in matches:
            try:
                # Clean up the JSON string
                params_str = params_str.strip()
                params = json.loads(params_str)
                tool_calls.append({
                    "tool_name": tool_name,
                    "parameters": params
                })
                logger.info(f"Successfully extracted tool call: {tool_name} with params {params}")
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse tool call parameters: {params_str}, error: {e}")
        
        logger.info(f"Total tool calls extracted: {len(tool_calls)}")
        return tool_calls
    
    async def process_tool_calls(self, tool_calls: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process a list of tool calls and return the results.
        
        Args:
            tool_calls: List of tool call specifications
            
        Returns:
            List of tool call results
        """
        results = []
        tasks = []
        
        for call in tool_calls:
            tool_name = call.get("tool_name")
            parameters = call.get("parameters", {})
            
            if tool_name not in self.tools:
                results.append({
                    "status": "error",
                    "tool_name": tool_name,
                    "error": f"Tool '{tool_name}' not found"
                })
                continue
            
            # Create task for each tool call
            tool = self.tools[tool_name]
            tasks.append(tool.run(parameters))
        
        # Execute all tool calls concurrently
        if tasks:
            tool_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for i, result in enumerate(tool_results):
                if isinstance(result, Exception):
                    # Handle exceptions from tools
                    results.append({
                        "status": "error",
                        "tool_name": tool_calls[i].get("tool_name"),
                        "error": str(result)
                    })
                else:
                    results.append(result)
        
        # Format results for LLM consumption
        for i, result in enumerate(results):
            if result.get("status") == "success" and "tool_name" in result:
                tool_name = result["tool_name"]
                if tool_name in self.tools:
                    formatted = self.tools[tool_name].format_for_llm(result)
                    results[i]["formatted"] = formatted
        
        return results