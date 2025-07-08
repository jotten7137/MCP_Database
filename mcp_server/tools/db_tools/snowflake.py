"""
Snowflake Database Tool for MCP Server
Provides connectivity to Snowflake data warehouse.
"""

import asyncio
from typing import Dict, Any, List, Tuple, Optional
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

try:
    import snowflake.connector
    from snowflake.connector import DictCursor
    import snowflake.connector.pandas_tools as pd_tools
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False

from .base import BaseDatabaseTool, DatabaseConnectionError, DatabaseQueryError

logger = logging.getLogger("mcp_server.tools.database.snowflake")

class SnowflakeTool(BaseDatabaseTool):
    """
    Tool for connecting to Snowflake data warehouse.
    """
    
    def __init__(self, connection_params: Dict[str, Any]):
        """
        Initialize Snowflake database tool.
        
        Args:
            connection_params: Snowflake connection parameters
                Required keys:
                - account: Snowflake account identifier
                - username: Snowflake username
                - password: Snowflake password (or use key_pair authentication)
                - warehouse: Snowflake warehouse name
                - database: Snowflake database name
                - schema: Snowflake schema name
                Optional keys:
                - role: Snowflake role
                - region: Snowflake region (if not in account identifier)
                - private_key: Private key for key pair authentication
                - private_key_passphrase: Passphrase for private key
                - authenticator: Authentication method ('snowflake', 'oauth', etc.)
                - session_parameters: Additional session parameters
        """
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError(
                "Snowflake connector not available. "
                "Install with: pip install snowflake-connector-python"
            )
        
        super().__init__(
            name="snowflake",
            description="Execute SQL queries against Snowflake data warehouse",
            connection_params=connection_params
        )
        
        self.executor = ThreadPoolExecutor(max_workers=2)
        
    @property
    def parameters(self) -> Dict[str, Any]:
        """Override to include Snowflake-specific parameters."""
        base_params = super().parameters
        
        # Add Snowflake-specific options
        base_params["properties"]["warehouse"] = {
            "type": "string",
            "description": "Snowflake warehouse to use for query execution"
        }
        base_params["properties"]["use_cached_result"] = {
            "type": "boolean",
            "description": "Whether to use cached query results",
            "default": True
        }
        
        return base_params
        
    async def connect(self) -> None:
        """Establish Snowflake connection."""
        if self._is_connected and self.connection:
            return
        
        try:
            # Build connection parameters
            conn_params = self._build_connection_params()
            
            # Create connection in thread pool (Snowflake connector is not async)
            loop = asyncio.get_event_loop()
            self.connection = await loop.run_in_executor(
                self.executor, 
                lambda: snowflake.connector.connect(**conn_params)
            )
            
            # Test connection
            await loop.run_in_executor(
                self.executor,
                lambda: self.connection.cursor().execute("SELECT 1").fetchone()
            )
            
            self._is_connected = True
            logger.info("Connected to Snowflake")
            
        except Exception as e:
            self._is_connected = False
            raise DatabaseConnectionError(f"Failed to connect to Snowflake: {str(e)}")
    
    async def disconnect(self) -> None:
        """Close Snowflake connection."""
        if self.connection:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                self.executor,
                self.connection.close
            )
            self.connection = None
        self._is_connected = False
        logger.info("Disconnected from Snowflake")
    
    async def execute_query(self, query: str, timeout: int = 30, warehouse: str = None, 
                          use_cached_result: bool = True) -> Tuple[List[Dict], List[str]]:
        """
        Execute SQL query against Snowflake.
        
        Args:
            query: SQL query to execute
            timeout: Query timeout in seconds
            warehouse: Warehouse to use (optional)
            use_cached_result: Whether to use cached results
            
        Returns:
            Tuple of (rows as list of dicts, column names)
        """
        if not self._is_connected or not self.connection:
            await self.connect()
        
        try:
            loop = asyncio.get_event_loop()
            
            def _execute():
                cursor = self.connection.cursor(DictCursor)
                
                try:
                    # Set warehouse if specified
                    if warehouse:
                        cursor.execute(f"USE WAREHOUSE {warehouse}")
                    
                    # Set query timeout
                    cursor.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {timeout}")
                    
                    # Set result cache setting
                    cursor.execute(f"ALTER SESSION SET USE_CACHED_RESULT = {str(use_cached_result).upper()}")
                    
                    # Execute the main query
                    cursor.execute(query)
                    
                    # Fetch results
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description] if cursor.description else []
                    
                    return rows, columns
                    
                finally:
                    cursor.close()
            
            # Execute in thread pool
            rows, columns = await loop.run_in_executor(self.executor, _execute)
            
            return rows, columns
            
        except Exception as e:
            raise DatabaseQueryError(f"Snowflake query execution failed: {str(e)}")
    
    async def execute(self, query: str, limit: int = 100, format: str = "table", 
                     timeout: int = 30, warehouse: str = None, 
                     use_cached_result: bool = True) -> Dict[str, Any]:
        """
        Execute database query with Snowflake-specific parameters.
        
        Args:
            query: SQL query to execute
            limit: Maximum rows to return
            format: Output format (json, table, csv)
            timeout: Query timeout in seconds
            warehouse: Warehouse to use (optional)
            use_cached_result: Whether to use cached results
            
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
            start_time = pd.Timestamp.now()
            rows, columns = await self.execute_query(
                limited_query, timeout, warehouse, use_cached_result
            )
            execution_time = (pd.Timestamp.now() - start_time).total_seconds()
            
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
                    "format": format,
                    "warehouse": warehouse,
                    "cached_result": use_cached_result
                }
            }
            
        except Exception as e:
            logger.error(f"Snowflake query failed: {str(e)}")
            raise DatabaseQueryError(f"Query execution failed: {str(e)}")
    
    async def get_schema_info(self) -> Dict[str, Any]:
        """Get Snowflake database schema information."""
        if not self._is_connected or not self.connection:
            await self.connect()
        
        try:
            loop = asyncio.get_event_loop()
            
            def _get_schema_info():
                cursor = self.connection.cursor(DictCursor)
                
                try:
                    schema_info = {
                        "database_type": "snowflake",
                        "databases": [],
                        "schemas": {},
                        "tables": {},
                        "views": {}
                    }
                    
                    # Get databases
                    cursor.execute("SHOW DATABASES")
                    databases = cursor.fetchall()
                    schema_info["databases"] = [db["name"] for db in databases]
                    
                    # Get schemas for current database
                    cursor.execute("SHOW SCHEMAS")
                    schemas = cursor.fetchall()
                    current_db = self.connection_params.get('database')
                    schema_info["schemas"][current_db] = [schema["name"] for schema in schemas]
                    
                    # Get tables and views
                    cursor.execute("SHOW TABLES")
                    tables = cursor.fetchall()
                    
                    cursor.execute("SHOW VIEWS")
                    views = cursor.fetchall()
                    
                    # Organize by schema
                    for table in tables:
                        schema_name = table["schema_name"]
                        if schema_name not in schema_info["tables"]:
                            schema_info["tables"][schema_name] = []
                        schema_info["tables"][schema_name].append(table["name"])
                    
                    for view in views:
                        schema_name = view["schema_name"]
                        if schema_name not in schema_info["views"]:
                            schema_info["views"][schema_name] = []
                        schema_info["views"][schema_name].append(view["name"])
                    
                    return schema_info
                    
                finally:
                    cursor.close()
            
            return await loop.run_in_executor(self.executor, _get_schema_info)
            
        except Exception as e:
            raise DatabaseQueryError(f"Failed to retrieve Snowflake schema info: {str(e)}")
    
    def _build_connection_params(self) -> Dict[str, Any]:
        """Build Snowflake connection parameters."""
        params = self.connection_params.copy()
        
        # Required parameters
        conn_params = {
            'account': params['account'],
            'user': params['username'],
            'warehouse': params['warehouse'],
            'database': params['database'],
            'schema': params['schema']
        }
        
        # Authentication
        if 'password' in params:
            conn_params['password'] = params['password']
        elif 'private_key' in params:
            conn_params['private_key'] = params['private_key']
            if 'private_key_passphrase' in params:
                conn_params['private_key_passphrase'] = params['private_key_passphrase']
        
        # Optional parameters
        if 'role' in params:
            conn_params['role'] = params['role']
        if 'region' in params:
            conn_params['region'] = params['region']
        if 'authenticator' in params:
            conn_params['authenticator'] = params['authenticator']
        
        # Session parameters
        session_params = params.get('session_parameters', {})
        if session_params:
            conn_params['session_parameters'] = session_params
        
        return conn_params
    
    def _apply_limit(self, query: str, limit: int) -> str:
        """
        Apply LIMIT clause to Snowflake query if not already present.
        Snowflake uses LIMIT syntax similar to other SQL databases.
        """
        query_upper = query.upper()
        
        # Check if LIMIT already exists
        if 'LIMIT' in query_upper:
            return query
        
        # Add LIMIT clause
        return f"{query.rstrip(';')} LIMIT {limit}"
    
    async def get_warehouse_info(self) -> Dict[str, Any]:
        """
        Get information about available Snowflake warehouses.
        
        Returns:
            Dict with warehouse information
        """
        if not self._is_connected or not self.connection:
            await self.connect()
        
        try:
            loop = asyncio.get_event_loop()
            
            def _get_warehouses():
                cursor = self.connection.cursor(DictCursor)
                
                try:
                    cursor.execute("SHOW WAREHOUSES")
                    warehouses = cursor.fetchall()
                    
                    warehouse_info = []
                    for wh in warehouses:
                        warehouse_info.append({
                            "name": wh["name"],
                            "state": wh["state"],
                            "type": wh["type"],
                            "size": wh["size"],
                            "auto_suspend": wh["auto_suspend"],
                            "auto_resume": wh["auto_resume"],
                            "comment": wh.get("comment", "")
                        })
                    
                    return {"warehouses": warehouse_info}
                    
                finally:
                    cursor.close()
            
            return await loop.run_in_executor(self.executor, _get_warehouses)
            
        except Exception as e:
            raise DatabaseQueryError(f"Failed to get warehouse info: {str(e)}")
    
    async def get_table_info(self, table_name: str, schema_name: str = None) -> Dict[str, Any]:
        """
        Get detailed information about a Snowflake table.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (optional, uses current schema if not provided)
            
        Returns:
            Dict with table information
        """
        if not self._is_connected or not self.connection:
            await self.connect()
        
        try:
            loop = asyncio.get_event_loop()
            
            def _get_table_info():
                cursor = self.connection.cursor(DictCursor)
                
                try:
                    # Use specified schema or current schema
                    if schema_name:
                        cursor.execute(f"USE SCHEMA {schema_name}")
                    
                    # Get table details
                    cursor.execute(f"DESCRIBE TABLE {table_name}")
                    columns = cursor.fetchall()
                    
                    # Get table stats
                    try:
                        cursor.execute(f"SELECT COUNT(*) as row_count FROM {table_name}")
                        row_count_result = cursor.fetchone()
                        row_count = row_count_result["ROW_COUNT"] if row_count_result else None
                    except:
                        row_count = None
                    
                    table_info = {
                        "table_name": table_name,
                        "schema_name": schema_name or self.connection_params.get('schema'),
                        "columns": [],
                        "row_count": row_count
                    }
                    
                    for col in columns:
                        table_info["columns"].append({
                            "name": col["name"],
                            "type": col["type"],
                            "nullable": col["null?"] == "Y",
                            "default": col.get("default"),
                            "primary_key": col.get("primary key") == "Y",
                            "unique_key": col.get("unique key") == "Y"
                        })
                    
                    return table_info
                    
                finally:
                    cursor.close()
            
            return await loop.run_in_executor(self.executor, _get_table_info)
            
        except Exception as e:
            raise DatabaseQueryError(f"Failed to get table info: {str(e)}")
    
    async def execute_dataframe_query(self, query: str, limit: int = 100) -> pd.DataFrame:
        """
        Execute query and return results as pandas DataFrame.
        
        Args:
            query: SQL query to execute
            limit: Maximum rows to return
            
        Returns:
            pandas DataFrame with query results
        """
        if not self._is_connected or not self.connection:
            await self.connect()
        
        try:
            loop = asyncio.get_event_loop()
            
            def _execute_df():
                # Apply limit
                limited_query = self._apply_limit(query, limit)
                
                # Use pandas integration
                df = pd.read_sql(limited_query, self.connection)
                return df
            
            return await loop.run_in_executor(self.executor, _execute_df)
            
        except Exception as e:
            raise DatabaseQueryError(f"DataFrame query execution failed: {str(e)}")
    
    def __del__(self):
        """Cleanup executor on deletion."""
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=False)
