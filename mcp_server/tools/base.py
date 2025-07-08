import abc
from typing import Dict, Any, Optional, List, Union
import json
import logging

logger = logging.getLogger("mcp_server.tools")

class BaseTool(abc.ABC):
    """
    Abstract base class for all MCP tools.
    
    Tools are integrations that allow the LLM to interact with external services
    or perform specialized operations like calculations, data retrieval, etc.
    """
    
    def __init__(self, name: str = None, description: str = None):
        """
        Initialize a tool with a name and description.
        
        Args:
            name: The name of the tool (defaults to class name if None)
            description: Human-readable description of what the tool does
        """
        self.name = name or self.__class__.__name__.lower().replace("tool", "")
        self.description = description or "No description provided"
    
    @property
    def manifest(self) -> Dict[str, Any]:
        """
        Return the tool manifest to be provided to the LLM.
        This follows a format similar to OpenAI's function calling.
        
        Returns:
            Dict with tool schema information
        """
        return {
            "name": self.name,
            "description": self.description,
            "parameters": self.parameters,
        }
    
    @property
    @abc.abstractmethod
    def parameters(self) -> Dict[str, Any]:
        """
        Define the parameters schema for this tool.
        Must be implemented by subclasses.
        
        Returns:
            Dict with JSON schema describing the parameters
        """
        pass
    
    @abc.abstractmethod
    async def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Execute the tool with the provided parameters.
        Must be implemented by subclasses.
        
        Args:
            **kwargs: Parameters passed to the tool
        
        Returns:
            Dict containing the result of the tool execution
        """
        pass
    
    def format_for_llm(self, result: Dict[str, Any]) -> str:
        """
        Format tool results into a string representation for the LLM.
        
        Args:
            result: The result from execute()
            
        Returns:
            String representation of results
        """
        return json.dumps(result, indent=2)
    
    async def run(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run the tool with error handling.
        
        Args:
            params: Parameters to pass to execute()
            
        Returns:
            Dict with execution results or error information
        """
        try:
            logger.info(f"Executing tool: {self.name} with params: {params}")
            result = await self.execute(**params)
            return {
                "status": "success",
                "tool_name": self.name,
                "result": result
            }
        except Exception as e:
            logger.error(f"Error executing tool {self.name}: {str(e)}")
            return {
                "status": "error",
                "tool_name": self.name,
                "error": str(e)
            }
