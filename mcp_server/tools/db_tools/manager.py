"""
Database Manager Tool for MCP Server
Manages multiple database connections and provides unified interface.
"""

from typing import Dict, Any, List, Optional, Union
import logging
import asyncio
from datetime import datetime

from ..base import BaseTool
from .base import BaseDatabaseTool, DatabaseConnectionError, DatabaseQueryError
from .sql import SQLDatabaseTool, PostgreSQLTool, MySQLTool, SQLiteTool
from .snowflake import SnowflakeTool

logger = logging.getLogger("mcp_server.tools.database.manager")

class DatabaseManagerTool(BaseTool):
    """
    Unified database manager that handles multiple database connections.
    Provides a single interface to query different database types.
    """
    
    def __init__(self, database_configs: Dict[str, Dict[str, Any]]):
        """
        Initialize database manager.
        
        Args:
            database_configs: Dict mapping connection names to their configs
                Example:
                {
                    "prod_postgres": {
                        "database_type": "postgresql",
                        "host": "localhost",
                        "port": 5432,
                        "database": "mydb",
                        "username": "user",
                        "password": "pass"
                    },
                    "analytics_snowflake": {
                        "database_type": "snowflake",
                        "account": "myaccount",
                        "username": "user",
                        "password": "pass",
                        "warehouse": "COMPUTE_WH",
                        "database": "ANALYTICS",
                        "schema": "PUBLIC"
                    }
                }
        """
        super().__init__(
            name="database_manager",
            description="Execute SQL queries against multiple database types (PostgreSQL, MySQL, SQLite, Snowflake)"
        )
        
        self.database_configs = database_configs
        self.connections: Dict[str, BaseDatabaseTool] = {}
        self._initialize_connections()
    
    @property
    def parameters(self) -> Dict[str, Any]:
        """Define parameters for the database manager tool."""
        connection_names = list(self.database_configs.keys())
        
        return {
            "type": "object",
            "properties": {
                "connection_name": {
                    "type": "string",
                    "enum": connection_names,
                    "description": f"Database connection to use. Available: {', '.join(connection_names)}"
                },
                "query": {
                    "type": "string",
                    "description": "SQL query to execute"
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
                }
            },
            "required": ["connection_name", "query"]
        }
    
    def _initialize_connections(self) -> None:
        """Initialize database connection objects."""
        for name, config in self.database_configs.items():
            try:
                db_type = config.get('database_type', '').lower()
                
                if db_type == 'postgresql':
                    self.connections[name] = PostgreSQLTool(config)
                elif db_type == 'mysql':
                    self.connections[name] = MySQLTool(config)
                elif db_type == 'sqlite':
                    self.connections[name] = SQLiteTool(config)
                elif db_type == 'snowflake':
                    self.connections[name] = SnowflakeTool(config)
                else:
                    # Generic SQL tool
                    self.connections[name] = SQLDatabaseTool(config)
                    
                logger.info(f"Initialized {db_type} connection: {name}")
                
            except Exception as e:
                logger.error(f"Failed to initialize connection {name}: {str(e)}")
                # Continue with other connections
    
    async def execute(self, connection_name: str, query: str, limit: int = 100, 
                     format: str = "table", timeout: int = 30, **kwargs) -> Dict[str, Any]:
        """
        Execute query against specified database connection.
        
        Args:
            connection_name: Name of the database connection to use
            query: SQL query to execute
            limit: Maximum rows to return
            format: Output format (json, table, csv)
            timeout: Query timeout in seconds
            **kwargs: Additional database-specific parameters
            
        Returns:
            Dict containing query results and metadata
        """
        if connection_name not in self.connections:
            raise DatabaseConnectionError(f"Unknown connection: {connection_name}")
        
        db_tool = self.connections[connection_name]
        
        try:
            # Execute query using the specific database tool
            if isinstance(db_tool, SnowflakeTool):
                # Pass Snowflake-specific parameters
                warehouse = kwargs.get('warehouse')
                use_cached_result = kwargs.get('use_cached_result', True)
                result = await db_tool.execute(
                    query, limit, format, timeout, warehouse, use_cached_result
                )
            else:
                # Standard SQL execution
                result = await db_tool.execute(query, limit, format, timeout)
            
            # Add connection metadata
            result["connection_name"] = connection_name
            result["connection_type"] = db_tool.name
            
            return result
            
        except Exception as e:
            logger.error(f"Query failed on {connection_name}: {str(e)}")
            raise DatabaseQueryError(f"Query failed on {connection_name}: {str(e)}")
    
    async def get_connection_info(self, connection_name: str = None) -> Dict[str, Any]:
        """
        Get information about database connections.
        
        Args:
            connection_name: Specific connection name (optional)
            
        Returns:
            Dict with connection information
        """
        if connection_name:
            if connection_name not in self.connections:
                raise DatabaseConnectionError(f"Unknown connection: {connection_name}")
            
            db_tool = self.connections[connection_name]
            schema_info = await db_tool.get_schema_info()
            
            return {
                "connection_name": connection_name,
                "connection_type": db_tool.name,
                "schema_info": schema_info
            }
        else:
            # Return info for all connections
            connections_info = {}
            
            for name, db_tool in self.connections.items():
                try:
                    schema_info = await db_tool.get_schema_info()
                    connections_info[name] = {
                        "connection_type": db_tool.name,
                        "schema_info": schema_info
                    }
                except Exception as e:
                    connections_info[name] = {
                        "connection_type": db_tool.name,
                        "error": str(e)
                    }
            
            return {"connections": connections_info}
    
    async def test_connection(self, connection_name: str) -> Dict[str, Any]:
        """
        Test a specific database connection.
        
        Args:
            connection_name: Name of connection to test
            
        Returns:
            Dict with test results
        """
        if connection_name not in self.connections:
            return {
                "connection_name": connection_name,
                "status": "error",
                "message": f"Unknown connection: {connection_name}"
            }
        
        db_tool = self.connections[connection_name]
        
        try:
            result = await db_tool.test_connection()
            result["connection_name"] = connection_name
            result["connection_type"] = db_tool.name
            return result
            
        except Exception as e:
            return {
                "connection_name": connection_name,
                "connection_type": db_tool.name,
                "status": "error",
                "message": str(e)
            }
    
    async def test_all_connections(self) -> Dict[str, Any]:
        """
        Test all configured database connections.
        
        Returns:
            Dict with test results for all connections
        """
        results = {}
        
        for name in self.connections.keys():
            results[name] = await self.test_connection(name)
        
        return {"connection_tests": results}
    
    async def get_table_info(self, connection_name: str, table_name: str, 
                           schema_name: str = None) -> Dict[str, Any]:
        """
        Get detailed information about a table from specific connection.
        
        Args:
            connection_name: Database connection name
            table_name: Name of the table
            schema_name: Schema name (optional)
            
        Returns:
            Dict with table information
        """
        if connection_name not in self.connections:
            raise DatabaseConnectionError(f"Unknown connection: {connection_name}")
        
        db_tool = self.connections[connection_name]
        
        if hasattr(db_tool, 'get_table_info'):
            table_info = await db_tool.get_table_info(table_name, schema_name)
            table_info["connection_name"] = connection_name
            table_info["connection_type"] = db_tool.name
            return table_info
        else:
            raise DatabaseQueryError(f"Table info not supported for {db_tool.name}")
    
    async def execute_cross_database_query(self, queries: List[Dict[str, Any]], 
                                         combine_results: bool = False) -> Dict[str, Any]:
        """
        Execute queries across multiple databases.
        
        Args:
            queries: List of query specifications, each containing:
                - connection_name: Database connection to use
                - query: SQL query to execute
                - limit: Optional row limit
                - format: Optional output format
            combine_results: Whether to combine results from all queries
            
        Returns:
            Dict with results from all queries
        """
        results = {}
        combined_data = []
        
        for i, query_spec in enumerate(queries):
            connection_name = query_spec['connection_name']
            query = query_spec['query']
            limit = query_spec.get('limit', 100)
            format_type = query_spec.get('format', 'json')  # Use json for combining
            
            try:
                result = await self.execute(connection_name, query, limit, format_type)
                results[f"query_{i+1}_{connection_name}"] = result
                
                if combine_results and format_type == 'json':
                    # Add connection info to each row
                    query_results = result.get('results', [])
                    for row in query_results:
                        row['_source_connection'] = connection_name
                        row['_source_query'] = i + 1
                    combined_data.extend(query_results)
                    
            except Exception as e:
                results[f"query_{i+1}_{connection_name}"] = {
                    "status": "error",
                    "error": str(e)
                }
        
        if combine_results:
            results["combined_results"] = {
                "total_rows": len(combined_data),
                "data": combined_data
            }
        
        return results
    
    async def disconnect_all(self) -> None:
        """Disconnect all database connections."""
        for name, db_tool in self.connections.items():
            try:
                await db_tool.disconnect()
                logger.info(f"Disconnected from {name}")
            except Exception as e:
                logger.error(f"Error disconnecting from {name}: {str(e)}")
    
    def format_for_llm(self, result: Dict[str, Any]) -> str:
        """Format database manager results for LLM consumption."""
        if result.get("status") == "error":
            return f"Database Manager error: {result.get('error')}"
        
        # Check if this is a connection test result
        if "connection_tests" in result:
            output = "Database Connection Test Results:\n"
            for name, test_result in result["connection_tests"].items():
                status = test_result.get("status", "unknown")
                output += f"- {name}: {status.upper()}\n"
                if status == "error":
                    output += f"  Error: {test_result.get('message', 'Unknown error')}\n"
            return output
        
        # Check if this is connection info
        if "connections" in result:
            output = "Database Connections Overview:\n"
            for name, info in result["connections"].items():
                conn_type = info.get("connection_type", "unknown")
                output += f"- {name} ({conn_type})\n"
                if "error" in info:
                    output += f"  Error: {info['error']}\n"
                elif "schema_info" in info:
                    schema_info = info["schema_info"]
                    if "tables" in schema_info:
                        table_count = sum(len(tables) for tables in schema_info["tables"].values())
                        output += f"  Tables: {table_count}\n"
            return output
        
        # Regular query result
        connection_name = result.get("connection_name", "unknown")
        connection_type = result.get("connection_type", "unknown")
        
        data = result.get("result", result)
        row_count = data.get("row_count", 0)
        execution_time = data.get("execution_time_seconds", 0)
        
        output = f"Database Query Results ({connection_name} - {connection_type}):\n"
        output += f"Rows returned: {row_count}\n"
        output += f"Execution time: {execution_time:.2f} seconds\n\n"
        
        results = data.get("results", "")
        if isinstance(results, str):
            output += results
        else:
            import json
            output += json.dumps(results, indent=2)
        
        return output
    
    def get_available_connections(self) -> List[str]:
        """Get list of available connection names."""
        return list(self.connections.keys())
    
    def get_connection_types(self) -> Dict[str, str]:
        """Get mapping of connection names to their types."""
        return {name: tool.name for name, tool in self.connections.items()}
