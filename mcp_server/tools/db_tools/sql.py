"""
SQL Database Tool for MCP Server
Supports PostgreSQL, MySQL, SQLite, and other SQL databases via SQLAlchemy.
"""

import asyncio
from typing import Dict, Any, List, Tuple, Optional
import logging
from urllib.parse import quote_plus

try:
    import asyncpg
    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False

try:
    import aiomysql
    AIOMYSQL_AVAILABLE = True
except ImportError:
    AIOMYSQL_AVAILABLE = False

try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

try:
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.sql import text
    from sqlalchemy import MetaData, inspect
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

from .base import BaseDatabaseTool, DatabaseConnectionError, DatabaseQueryError

logger = logging.getLogger("mcp_server.tools.database.sql")

class SQLDatabaseTool(BaseDatabaseTool):
    """
    Tool for connecting to SQL databases (PostgreSQL, MySQL, SQLite, etc.)
    """
    
    def __init__(self, connection_params: Dict[str, Any]):
        """
        Initialize SQL database tool.
        
        Args:
            connection_params: Database connection parameters
                Required keys:
                - database_type: 'postgresql', 'mysql', 'sqlite'
                - host: Database host (not needed for SQLite)
                - port: Database port (not needed for SQLite) 
                - database: Database name or file path (for SQLite)
                - username: Database username (not needed for SQLite)
                - password: Database password (not needed for SQLite)
                Optional keys:
                - schema: Default schema name
                - ssl_mode: SSL mode for connection
                - connection_timeout: Connection timeout in seconds
        """
        if not SQLALCHEMY_AVAILABLE:
            raise ImportError(
                "SQLAlchemy not available. "
                "Install with: pip install sqlalchemy"
            )
        
        db_type = connection_params.get('database_type', 'postgresql').lower()
        
        # Check specific database driver availability
        if db_type == 'postgresql' and not ASYNCPG_AVAILABLE:
            raise ImportError(
                "PostgreSQL driver not available. "
                "Install with: pip install asyncpg"
            )
        elif db_type == 'mysql' and not AIOMYSQL_AVAILABLE:
            raise ImportError(
                "MySQL driver not available. "
                "Install with: pip install aiomysql"
            )
        elif db_type == 'sqlite' and not AIOSQLITE_AVAILABLE:
            raise ImportError(
                "SQLite driver not available. "
                "Install with: pip install aiosqlite"
            )
        super().__init__(
            name=f"sql_database_{db_type}",
            description=f"Execute SQL queries against {db_type.title()} database",
            connection_params=connection_params
        )
        
        self.database_type = db_type
        self.engine = None
        self.metadata = None
        
    async def connect(self) -> None:
        """Establish database connection."""
        if self._is_connected and self.engine:
            return
        
        try:
            connection_string = self._build_connection_string()
            
            # Create async engine
            self.engine = create_async_engine(
                connection_string,
                echo=False,
                pool_pre_ping=True,
                pool_recycle=3600,
                connect_args=self._get_connect_args()
            )
            
            # Test connection
            async with self.engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
            
            self._is_connected = True
            logger.info(f"Connected to {self.database_type} database")
            
        except Exception as e:
            self._is_connected = False
            raise DatabaseConnectionError(f"Failed to connect to {self.database_type}: {str(e)}")
    
    async def disconnect(self) -> None:
        """Close database connection."""
        if self.engine:
            await self.engine.dispose()
            self.engine = None
        self._is_connected = False
        logger.info(f"Disconnected from {self.database_type} database")
    
    async def execute_query(self, query: str, timeout: int = 30) -> Tuple[List[Dict], List[str]]:
        """
        Execute SQL query and return results.
        
        Args:
            query: SQL query to execute
            timeout: Query timeout in seconds
            
        Returns:
            Tuple of (rows as list of dicts, column names)
        """
        if not self._is_connected or not self.engine:
            await self.connect()
        
        try:
            async with self.engine.begin() as conn:
                # Set query timeout if supported
                if self.database_type == 'postgresql':
                    await conn.execute(text(f"SET statement_timeout = {timeout * 1000}"))
                elif self.database_type == 'mysql':
                    await conn.execute(text(f"SET SESSION max_execution_time = {timeout * 1000}"))
                
                # Execute query
                result = await conn.execute(text(query))
                
                # Fetch results
                rows = result.fetchall()
                columns = list(result.keys()) if rows else []
                
                # Convert to list of dicts
                rows_as_dicts = [dict(row._mapping) for row in rows]
                
                return rows_as_dicts, columns
                
        except Exception as e:
            raise DatabaseQueryError(f"Query execution failed: {str(e)}")
    
    async def get_schema_info(self) -> Dict[str, Any]:
        """Get database schema information."""
        if not self._is_connected or not self.engine:
            await self.connect()
        
        try:
            schema_info = {
                "database_type": self.database_type,
                "schemas": [],
                "tables": {},
                "views": {}
            }
            
            async with self.engine.begin() as conn:
                # Get schemas/databases
                if self.database_type == 'postgresql':
                    result = await conn.execute(text("""
                        SELECT schema_name 
                        FROM information_schema.schemata 
                        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                    """))
                    schema_info["schemas"] = [row[0] for row in result.fetchall()]
                    
                    # Get tables and views
                    result = await conn.execute(text("""
                        SELECT table_schema, table_name, table_type
                        FROM information_schema.tables
                        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                        ORDER BY table_schema, table_name
                    """))
                    
                elif self.database_type == 'mysql':
                    result = await conn.execute(text("SHOW DATABASES"))
                    schema_info["schemas"] = [row[0] for row in result.fetchall()]
                    
                    # Get tables and views
                    result = await conn.execute(text("""
                        SELECT table_schema, table_name, table_type
                        FROM information_schema.tables
                        WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                        ORDER BY table_schema, table_name
                    """))
                    
                elif self.database_type == 'sqlite':
                    # SQLite doesn't have schemas, just tables
                    result = await conn.execute(text("""
                        SELECT name, type 
                        FROM sqlite_master 
                        WHERE type IN ('table', 'view')
                        ORDER BY name
                    """))
                    
                    for row in result.fetchall():
                        table_name, table_type = row
                        if table_type == 'table':
                            schema_info["tables"][table_name] = {"schema": "main"}
                        else:
                            schema_info["views"][table_name] = {"schema": "main"}
                    
                    return schema_info
                
                # Process tables and views for PostgreSQL/MySQL
                for row in result.fetchall():
                    schema_name, table_name, table_type = row
                    table_type = table_type.upper()
                    
                    if table_type in ('BASE TABLE', 'TABLE'):
                        if schema_name not in schema_info["tables"]:
                            schema_info["tables"][schema_name] = []
                        schema_info["tables"][schema_name].append(table_name)
                    elif table_type == 'VIEW':
                        if schema_name not in schema_info["views"]:
                            schema_info["views"][schema_name] = []
                        schema_info["views"][schema_name].append(table_name)
                
                return schema_info
                
        except Exception as e:
            raise DatabaseQueryError(f"Failed to retrieve schema info: {str(e)}")
    
    def _build_connection_string(self) -> str:
        """Build database connection string."""
        params = self.connection_params
        db_type = self.database_type
        
        if db_type == 'postgresql':
            host = params.get('host', 'localhost')
            port = params.get('port', 5432)
            database = params['database']
            username = params.get('username', 'postgres')
            password = params.get('password', '')
            
            # URL encode password to handle special characters
            encoded_password = quote_plus(password) if password else ''
            auth = f"{username}:{encoded_password}@" if username else ""
            
            return f"postgresql+asyncpg://{auth}{host}:{port}/{database}"
            
        elif db_type == 'mysql':
            host = params.get('host', 'localhost')
            port = params.get('port', 3306)
            database = params['database']
            username = params.get('username', 'root')
            password = params.get('password', '')
            
            # URL encode password to handle special characters
            encoded_password = quote_plus(password) if password else ''
            auth = f"{username}:{encoded_password}@" if username else ""
            
            return f"mysql+aiomysql://{auth}{host}:{port}/{database}"
            
        elif db_type == 'sqlite':
            database_path = params['database']
            return f"sqlite+aiosqlite:///{database_path}"
            
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    def _get_connect_args(self) -> Dict[str, Any]:
        """Get database-specific connection arguments."""
        params = self.connection_params
        connect_args = {}
        
        if self.database_type == 'postgresql':
            if params.get('ssl_mode'):
                connect_args['sslmode'] = params['ssl_mode']
            if params.get('connection_timeout'):
                connect_args['command_timeout'] = params['connection_timeout']
                
        elif self.database_type == 'mysql':
            if params.get('connection_timeout'):
                connect_args['connect_timeout'] = params['connection_timeout']
            if params.get('charset'):
                connect_args['charset'] = params['charset']
            else:
                connect_args['charset'] = 'utf8mb4'
                
        elif self.database_type == 'sqlite':
            if params.get('connection_timeout'):
                connect_args['timeout'] = params['connection_timeout']
        
        return connect_args
    
    async def get_table_info(self, table_name: str, schema_name: str = None) -> Dict[str, Any]:
        """
        Get detailed information about a specific table.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (optional)
            
        Returns:
            Dict with table information including columns, types, etc.
        """
        if not self._is_connected or not self.engine:
            await self.connect()
        
        try:
            table_info = {
                "table_name": table_name,
                "schema_name": schema_name,
                "columns": [],
                "row_count": None
            }
            
            async with self.engine.begin() as conn:
                # Get column information
                if self.database_type == 'postgresql':
                    schema_filter = f"AND table_schema = '{schema_name}'" if schema_name else ""
                    query = f"""
                        SELECT column_name, data_type, is_nullable, column_default
                        FROM information_schema.columns
                        WHERE table_name = '{table_name}' {schema_filter}
                        ORDER BY ordinal_position
                    """
                    
                elif self.database_type == 'mysql':
                    schema_filter = f"AND table_schema = '{schema_name}'" if schema_name else ""
                    query = f"""
                        SELECT column_name, data_type, is_nullable, column_default
                        FROM information_schema.columns
                        WHERE table_name = '{table_name}' {schema_filter}
                        ORDER BY ordinal_position
                    """
                    
                elif self.database_type == 'sqlite':
                    query = f"PRAGMA table_info({table_name})"
                
                result = await conn.execute(text(query))
                
                if self.database_type == 'sqlite':
                    # SQLite PRAGMA returns different format
                    for row in result.fetchall():
                        table_info["columns"].append({
                            "name": row[1],
                            "type": row[2],
                            "nullable": not bool(row[3]),
                            "default": row[4]
                        })
                else:
                    # PostgreSQL/MySQL format
                    for row in result.fetchall():
                        table_info["columns"].append({
                            "name": row[0],
                            "type": row[1],
                            "nullable": row[2] == 'YES',
                            "default": row[3]
                        })
                
                # Get row count (approximate for large tables)
                try:
                    if schema_name and self.database_type != 'sqlite':
                        full_table_name = f"{schema_name}.{table_name}"
                    else:
                        full_table_name = table_name
                    
                    count_result = await conn.execute(text(f"SELECT COUNT(*) FROM {full_table_name}"))
                    table_info["row_count"] = count_result.fetchone()[0]
                except:
                    # If count fails (permissions, etc.), leave as None
                    pass
                
                return table_info
                
        except Exception as e:
            raise DatabaseQueryError(f"Failed to get table info: {str(e)}")


class PostgreSQLTool(SQLDatabaseTool):
    """Specialized PostgreSQL database tool."""
    
    def __init__(self, connection_params: Dict[str, Any]):
        connection_params['database_type'] = 'postgresql'
        super().__init__(connection_params)


class MySQLTool(SQLDatabaseTool):
    """Specialized MySQL database tool."""
    
    def __init__(self, connection_params: Dict[str, Any]):
        connection_params['database_type'] = 'mysql'
        super().__init__(connection_params)


class SQLiteTool(SQLDatabaseTool):
    """Specialized SQLite database tool."""
    
    def __init__(self, connection_params: Dict[str, Any]):
        connection_params['database_type'] = 'sqlite'
        super().__init__(connection_params)
