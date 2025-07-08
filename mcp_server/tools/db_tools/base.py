"""
Database Base Tool for MCP Server
Provides a foundation for various database connection types.
"""

import abc
import asyncio
from typing import Dict, Any, List, Optional, Union, Tuple
import logging
import json
from datetime import datetime
import pandas as pd

from ..base import BaseTool

logger = logging.getLogger("mcp_server.tools.database")

class DatabaseConnectionError(Exception):
    """Raised when database connection fails"""
    pass

class DatabaseQueryError(Exception):
    """Raised when database query fails"""
    pass

class BaseDatabaseTool(BaseTool):
    """
    Abstract base class for database tools.
    Provides common functionality for database connections and queries.
    """
    
    def __init__(self, name: str, description: str, connection_params: Dict[str, Any]):
        """
        Initialize database tool.
        
        Args:
            name: Tool name
            description: Tool description
            connection_params: Database connection parameters
        """
        super().__init__(name, description)
        self.connection_params = connection_params
        self.connection = None
        self._is_connected = False
        
    @property
    def parameters(self) -> Dict[str, Any]:
        """Define common parameters for database tools."""
        return {
            "type": "object",
            "properties": {
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
            "required": ["query"]
        }
    
    @abc.abstractmethod
    async def connect(self) -> None:
        """Establish database connection. Must be implemented by subclasses."""
        pass
    
    @abc.abstractmethod
    async def disconnect(self) -> None:
        """Close database connection. Must be implemented by subclasses."""
        pass
    
    @abc.abstractmethod
    async def execute_query(self, query: str, timeout: int = 30) -> Tuple[List[Dict], List[str]]:
        """
        Execute SQL query and return results.
        Must be implemented by subclasses.
        
        Args:
            query: SQL query to execute
            timeout: Query timeout in seconds
            
        Returns:
            Tuple of (rows as list of dicts, column names)
        """
        pass
    
    @abc.abstractmethod
    async def get_schema_info(self) -> Dict[str, Any]:
        """Get database schema information. Must be implemented by subclasses."""
        pass
    
    async def execute(self, query: str, limit: int = 100, format: str = "table", timeout: int = 30) -> Dict[str, Any]:
        """
        Execute database query with standardized output.
        
        Args:
            query: SQL query to execute
            limit: Maximum rows to return
            format: Output format (json, table, csv)
            timeout: Query timeout in seconds
            
        Returns:
            Dict containing query results and metadata
        """
        try:
            # Ensure connection
            if not self._is_connected:
                await self.connect()
            
            # Validate query
            self._validate_query(query)
            
            # Add limit if not present
            limited_query = self._apply_limit(query, limit)
            
            # Execute query
            start_time = datetime.now()
            rows, columns = await self.execute_query(limited_query, timeout)
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # Format results
            formatted_results = self._format_results(rows, columns, format)
            
            return {
                "query": limited_query,
                "row_count": len(rows),
                "columns": columns,
                "execution_time_seconds": execution_time,
                "results": formatted_results,
                "metadata": {
                    "database_type": self.name,
                    "limit_applied": limit,
                    "format": format
                }
            }
            
        except Exception as e:
            logger.error(f"Database query failed: {str(e)}")
            raise DatabaseQueryError(f"Query execution failed: {str(e)}")
    
    def _validate_query(self, query: str) -> None:
        """
        Basic SQL query validation to prevent dangerous operations.
        
        Args:
            query: SQL query to validate
            
        Raises:
            DatabaseQueryError: If query contains dangerous patterns
        """
        query_upper = query.upper().strip()
        
        # Block dangerous operations
        dangerous_patterns = [
            'DROP', 'DELETE', 'TRUNCATE', 'INSERT', 'UPDATE', 
            'CREATE', 'ALTER', 'GRANT', 'REVOKE', 'EXEC',
            'EXECUTE', 'CALL', 'MERGE', 'UPSERT'
        ]
        
        for pattern in dangerous_patterns:
            if f" {pattern} " in f" {query_upper} ":
                raise DatabaseQueryError(f"Query contains forbidden operation: {pattern}")
        
        # Ensure it's a SELECT query
        if not query_upper.startswith('SELECT') and not query_upper.startswith('WITH'):
            raise DatabaseQueryError("Only SELECT and WITH queries are allowed")
    
    def _apply_limit(self, query: str, limit: int) -> str:
        """
        Apply LIMIT clause to query if not already present.
        
        Args:
            query: Original SQL query
            limit: Maximum rows to return
            
        Returns:
            Query with LIMIT clause applied
        """
        query_upper = query.upper()
        
        # Check if LIMIT already exists
        if 'LIMIT' in query_upper:
            return query
        
        # Add LIMIT clause
        return f"{query.rstrip(';')} LIMIT {limit}"
    
    def _format_results(self, rows: List[Dict], columns: List[str], format: str) -> Union[List[Dict], str]:
        """
        Format query results according to specified format.
        
        Args:
            rows: Query result rows
            columns: Column names
            format: Output format (json, table, csv)
            
        Returns:
            Formatted results
        """
        if format == "json":
            return rows
        
        elif format == "csv":
            if not rows:
                return ""
            
            # Convert to CSV format
            import io
            import csv
            
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
            return output.getvalue()
        
        elif format == "table":
            if not rows:
                return "No results found."
            
            # Create ASCII table
            return self._create_ascii_table(rows, columns)
        
        else:
            return rows
    
    def _create_ascii_table(self, rows: List[Dict], columns: List[str]) -> str:
        """
        Create ASCII table representation of query results.
        
        Args:
            rows: Query result rows
            columns: Column names
            
        Returns:
            ASCII table string
        """
        if not rows:
            return "No results found."
        
        # Calculate column widths
        widths = {}
        for col in columns:
            widths[col] = len(col)
            for row in rows:
                value = str(row.get(col, ''))
                widths[col] = max(widths[col], len(value))
        
        # Create table
        lines = []
        
        # Header
        header = "| " + " | ".join(col.ljust(widths[col]) for col in columns) + " |"
        separator = "+-" + "-+-".join("-" * widths[col] for col in columns) + "-+"
        
        lines.append(separator)
        lines.append(header)
        lines.append(separator)
        
        # Rows
        for row in rows:
            row_str = "| " + " | ".join(str(row.get(col, '')).ljust(widths[col]) for col in columns) + " |"
            lines.append(row_str)
        
        lines.append(separator)
        
        return "\n".join(lines)
    
    async def test_connection(self) -> Dict[str, Any]:
        """
        Test database connection and return status.
        
        Returns:
            Dict with connection test results
        """
        try:
            await self.connect()
            schema = await self.get_schema_info()
            await self.disconnect()
            
            return {
                "status": "success",
                "message": "Connection successful",
                "schema_info": schema
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Connection failed: {str(e)}"
            }
    
    def format_for_llm(self, result: Dict[str, Any]) -> str:
        """Format database results for LLM consumption."""
        if result.get("status") == "error":
            return f"Database error: {result.get('error')}"
        
        data = result.get("result", {})
        row_count = data.get("row_count", 0)
        execution_time = data.get("execution_time_seconds", 0)
        
        output = f"Database Query Results:\n"
        output += f"Rows returned: {row_count}\n"
        output += f"Execution time: {execution_time:.2f} seconds\n\n"
        
        results = data.get("results", "")
        if isinstance(results, str):
            output += results
        else:
            output += json.dumps(results, indent=2)
        
        return output
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
