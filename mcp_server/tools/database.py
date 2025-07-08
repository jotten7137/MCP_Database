"""
Main Database Tool for MCP Server
Integrates all database functionality into a single tool interface.
"""

from typing import Dict, Any, List, Optional
import logging

from .base import BaseTool
from .db_tools.config import DatabaseConfigTool
from .db_tools.manager import DatabaseManagerTool

logger = logging.getLogger("mcp_server.tools.database")

class DatabaseTool(BaseTool):
    """
    Main database tool that provides unified access to all database functionality.
    Combines configuration management and query execution.
    """
    
    def __init__(self, config_file: str = None, database_configs: Dict[str, Dict[str, Any]] = None):
        """
        Initialize main database tool.
        
        Args:
            config_file: Path to JSON configuration file (optional)
            database_configs: Direct database configurations (optional)
        """
        super().__init__(
            name="database",
            description="Execute SQL queries and manage database connections across multiple database types"
        )
        
        self.config_tool = None
        self.manager = None
        
        # Try to initialize configuration and manager tools
        try:
            # Initialize configuration tool
            if database_configs:
                # Use provided configurations directly
                self.manager = DatabaseManagerTool(database_configs)
            else:
                # Use configuration tool for loading from files/env
                self.config_tool = DatabaseConfigTool(config_file)
                self.manager = self.config_tool.get_manager()
                
            logger.info("Database tool initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database tool: {str(e)}")
            # Set manager to None so we can still provide basic functionality
            self.manager = None
            self.config_tool = None
    
    @property
    def parameters(self) -> Dict[str, Any]:
        """Define parameters for the main database tool."""
        if not self.manager:
            # If no manager, only allow config operations
            return {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list_connections", "test_connections", "add_connection"],
                        "description": "Database action to perform"
                    },
                    "connection_name": {
                        "type": "string",
                        "description": "Name of database connection"
                    },
                    "connection_config": {
                        "type": "object",
                        "description": "Database connection configuration"
                    }
                },
                "required": ["action"]
            }
        
        # Full parameters when manager is available
        connection_names = self.manager.get_available_connections() if self.manager else []
        
        return {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": [
                        "query", "list_connections", "test_connections", 
                        "get_schema_info", "get_table_info", "cross_database_query"
                    ],
                    "description": "Database action to perform"
                },
                "connection_name": {
                    "type": "string",
                    "enum": connection_names,
                    "description": f"Database connection to use. Available: {', '.join(connection_names)}"
                },
                "query": {
                    "type": "string",
                    "description": "SQL query to execute (for query action)"
                },
                "table_name": {
                    "type": "string",
                    "description": "Table name (for get_table_info action)"
                },
                "schema_name": {
                    "type": "string",
                    "description": "Schema name (optional)"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of rows to return",
                    "default": 100,
                    "minimum": 1,
                    "maximum": 10000
                },
                "format": {
                    "type": "string",
                    "enum": ["json", "table", "csv"],
                    "description": "Output format for query results",
                    "default": "table"
                },
                "timeout": {
                    "type": "integer",
                    "description": "Query timeout in seconds",
                    "default": 30,
                    "minimum": 1,
                    "maximum": 300
                },
                "queries": {
                    "type": "array",
                    "description": "Multiple queries for cross-database action",
                    "items": {
                        "type": "object",
                        "properties": {
                            "connection_name": {"type": "string"},
                            "query": {"type": "string"},
                            "limit": {"type": "integer"},
                            "format": {"type": "string"}
                        },
                        "required": ["connection_name", "query"]
                    }
                },
                "combine_results": {
                    "type": "boolean",
                    "description": "Whether to combine results from multiple queries",
                    "default": False
                }
            },
            "required": ["action"]
        }
    
    async def execute(self, action: str, **kwargs) -> Dict[str, Any]:
        """
        Execute database action.
        
        Args:
            action: Action to perform
            **kwargs: Action-specific parameters
            
        Returns:
            Dict with action results
        """
        try:
            if action == "query":
                return await self._execute_query(**kwargs)
            
            elif action == "list_connections":
                return await self._list_connections()
            
            elif action == "test_connections":
                return await self._test_connections()
            
            elif action == "get_schema_info":
                return await self._get_schema_info(**kwargs)
            
            elif action == "get_table_info":
                return await self._get_table_info(**kwargs)
            
            elif action == "cross_database_query":
                return await self._cross_database_query(**kwargs)
            
            else:
                raise ValueError(f"Unknown action: {action}")
                
        except Exception as e:
            logger.error(f"Database action '{action}' failed: {str(e)}")
            return {
                "status": "error",
                "action": action,
                "error": str(e)
            }
    
    async def _execute_query(self, connection_name: str, query: str, 
                           limit: int = 100, format: str = "table", 
                           timeout: int = 30, **kwargs) -> Dict[str, Any]:
        """Execute a single database query."""
        if not self.manager:
            raise ValueError("No database manager available. Check configurations.")
        
        return await self.manager.execute(
            connection_name, query, limit, format, timeout, **kwargs
        )
    
    async def _list_connections(self) -> Dict[str, Any]:
        """List all available database connections."""
        if self.config_tool:
            return self.config_tool._list_connections()
        elif self.manager:
            return await self.manager.get_connection_info()
        else:
            return {
                "status": "error",
                "message": "No database configurations available"
            }
    
    async def _test_connections(self) -> Dict[str, Any]:
        """Test all database connections."""
        if not self.manager:
            return {
                "status": "error",
                "message": "No database manager available. Check configurations."
            }
        
        return await self.manager.test_all_connections()
    
    async def _get_schema_info(self, connection_name: str = None) -> Dict[str, Any]:
        """Get schema information for a database connection."""
        if not self.manager:
            raise ValueError("No database manager available. Check configurations.")
        
        return await self.manager.get_connection_info(connection_name)
    
    async def _get_table_info(self, connection_name: str, table_name: str, 
                            schema_name: str = None) -> Dict[str, Any]:
        """Get detailed information about a table."""
        if not self.manager:
            raise ValueError("No database manager available. Check configurations.")
        
        return await self.manager.get_table_info(connection_name, table_name, schema_name)
    
    async def _cross_database_query(self, queries: List[Dict[str, Any]], 
                                  combine_results: bool = False) -> Dict[str, Any]:
        """Execute queries across multiple databases."""
        if not self.manager:
            raise ValueError("No database manager available. Check configurations.")
        
        return await self.manager.execute_cross_database_query(queries, combine_results)
    
    def format_for_llm(self, result: Dict[str, Any]) -> str:
        """Format database results for LLM consumption."""
        if result.get("status") == "error":
            return f"Database error: {result.get('error', 'Unknown error')}"
        
        # Use manager's formatting if available
        if self.manager and "connection_name" in result:
            return self.manager.format_for_llm(result)
        
        # Use config tool's formatting if available
        elif self.config_tool:
            return self.config_tool.format_for_llm(result)
        
        # Default formatting
        else:
            import json
            return json.dumps(result, indent=2)
    
    def get_available_connections(self) -> List[str]:
        """Get list of available database connections."""
        if self.manager:
            return self.manager.get_available_connections()
        else:
            return []
    
    def get_connection_types(self) -> Dict[str, str]:
        """Get mapping of connection names to their database types."""
        if self.manager:
            return self.manager.get_connection_types()
        else:
            return {}
    
    async def disconnect_all(self) -> None:
        """Disconnect all database connections."""
        if self.manager:
            await self.manager.disconnect_all()
