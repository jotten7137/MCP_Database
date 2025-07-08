"""
Database Configuration Tool for MCP Server
Creates and manages database configurations from environment variables and config files.
"""

import os
import json
from typing import Dict, Any, List, Optional
import logging
from pathlib import Path

from ..base import BaseTool
from .manager import DatabaseManagerTool

logger = logging.getLogger("mcp_server.tools.database.config")

class DatabaseConfigTool(BaseTool):
    """
    Tool for managing database configurations and creating database manager instances.
    """
    
    def __init__(self, config_file: str = None):
        """
        Initialize database configuration tool.
        
        Args:
            config_file: Path to JSON configuration file (optional)
        """
        super().__init__(
            name="database_config",
            description="Manage database configurations and connections"
        )
        
        self.config_file = config_file
        self.database_configs = {}
        self.manager = None
        
        # Load configurations
        self._load_configurations()
    
    @property
    def parameters(self) -> Dict[str, Any]:
        """Define parameters for database configuration tool."""
        return {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["list_connections", "test_connections", "add_connection", "remove_connection", "update_connection"],
                    "description": "Action to perform on database configurations"
                },
                "connection_name": {
                    "type": "string",
                    "description": "Name of the database connection (for add/remove/update actions)"
                },
                "connection_config": {
                    "type": "object",
                    "description": "Database connection configuration (for add/update actions)",
                    "properties": {
                        "database_type": {
                            "type": "string",
                            "enum": ["postgresql", "mysql", "sqlite", "snowflake"],
                            "description": "Type of database"
                        },
                        "host": {"type": "string"},
                        "port": {"type": "integer"},
                        "database": {"type": "string"},
                        "username": {"type": "string"},
                        "password": {"type": "string"},
                        "schema": {"type": "string"},
                        "warehouse": {"type": "string"},
                        "account": {"type": "string"},
                        "role": {"type": "string"}
                    }
                }
            },
            "required": ["action"]
        }
    
    def _load_configurations(self) -> None:
        """Load database configurations from environment variables and config file."""
        # Load from environment variables first
        self._load_from_environment()
        
        # Load from config file if specified
        if self.config_file and os.path.exists(self.config_file):
            self._load_from_file()
        
        # Create database manager if we have configurations
        if self.database_configs:
            try:
                self.manager = DatabaseManagerTool(self.database_configs)
                logger.info(f"Loaded {len(self.database_configs)} database configurations")
            except Exception as e:
                logger.error(f"Failed to create database manager: {str(e)}")
    
    def _load_from_environment(self) -> None:
        """Load database configurations from environment variables."""
        # Look for environment variables with pattern: DB_<n>_<setting>
        db_vars = {}
        
        for key, value in os.environ.items():
            if key.startswith('DB_'):
                parts = key.split('_')
                if len(parts) >= 3:  # DB_<n>_<setting>
                    db_name = parts[1].lower()
                    setting = '_'.join(parts[2:]).lower()
                    
                    if db_name not in db_vars:
                        db_vars[db_name] = {}
                    
                    # Convert numeric values
                    if setting == 'port':
                        try:
                            value = int(value)
                        except ValueError:
                            pass
                    
                    db_vars[db_name][setting] = value
        
        # Convert to proper connection configs
        for db_name, config in db_vars.items():
            if 'database_type' in config:
                self.database_configs[db_name] = config
                logger.info(f"Loaded {config['database_type']} config for {db_name} from environment")
    
    def _load_from_file(self) -> None:
        """Load database configurations from JSON file."""
        try:
            with open(self.config_file, 'r') as f:
                file_configs = json.load(f)
            
            # Merge with existing configs (file takes precedence)
            self.database_configs.update(file_configs)
            logger.info(f"Loaded database configurations from {self.config_file}")
            
        except Exception as e:
            logger.error(f"Failed to load config file {self.config_file}: {str(e)}")
    
    def _save_to_file(self) -> None:
        """Save current configurations to file."""
        if not self.config_file:
            return
        
        try:
            # Create directory if it doesn't exist
            Path(self.config_file).parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.config_file, 'w') as f:
                json.dump(self.database_configs, f, indent=2)
            
            logger.info(f"Saved database configurations to {self.config_file}")
            
        except Exception as e:
            logger.error(f"Failed to save config file {self.config_file}: {str(e)}")
    
    async def execute(self, action: str, connection_name: str = None, 
                     connection_config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Execute database configuration action.
        
        Args:
            action: Action to perform
            connection_name: Name of connection (for specific actions)
            connection_config: Connection configuration (for add/update)
            
        Returns:
            Dict with action results
        """
        if action == "list_connections":
            return self._list_connections()
        
        elif action == "test_connections":
            return await self._test_connections()
        
        elif action == "add_connection":
            if not connection_name or not connection_config:
                raise ValueError("connection_name and connection_config required for add_connection")
            return self._add_connection(connection_name, connection_config)
        
        elif action == "remove_connection":
            if not connection_name:
                raise ValueError("connection_name required for remove_connection")
            return self._remove_connection(connection_name)
        
        elif action == "update_connection":
            if not connection_name or not connection_config:
                raise ValueError("connection_name and connection_config required for update_connection")
            return self._update_connection(connection_name, connection_config)
        
        else:
            raise ValueError(f"Unknown action: {action}")
    
    def _list_connections(self) -> Dict[str, Any]:
        """List all configured database connections."""
        connections = {}
        
        for name, config in self.database_configs.items():
            # Don't expose sensitive information like passwords
            safe_config = config.copy()
            for sensitive_key in ['password', 'private_key', 'private_key_passphrase']:
                if sensitive_key in safe_config:
                    safe_config[sensitive_key] = "***hidden***"
            
            connections[name] = safe_config
        
        return {
            "total_connections": len(connections),
            "connections": connections
        }
    
    async def _test_connections(self) -> Dict[str, Any]:
        """Test all configured database connections."""
        if not self.manager:
            return {
                "status": "error",
                "message": "No database manager available. Check configurations."
            }
        
        return await self.manager.test_all_connections()
    
    def _add_connection(self, name: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Add a new database connection configuration."""
        if name in self.database_configs:
            return {
                "status": "error",
                "message": f"Connection {name} already exists. Use update_connection to modify it."
            }
        
        # Validate required fields
        required_fields = ['database_type']
        for field in required_fields:
            if field not in config:
                return {
                    "status": "error",
                    "message": f"Missing required field: {field}"
                }
        
        # Add the configuration
        self.database_configs[name] = config
        
        # Recreate manager with new configs
        try:
            self.manager = DatabaseManagerTool(self.database_configs)
        except Exception as e:
            # Rollback on error
            del self.database_configs[name]
            return {
                "status": "error",
                "message": f"Failed to create connection: {str(e)}"
            }
        
        # Save to file
        self._save_to_file()
        
        return {
            "status": "success",
            "message": f"Connection {name} added successfully"
        }
    
    def _remove_connection(self, name: str) -> Dict[str, Any]:
        """Remove a database connection configuration."""
        if name not in self.database_configs:
            return {
                "status": "error",
                "message": f"Connection {name} not found"
            }
        
        # Remove the configuration
        del self.database_configs[name]
        
        # Recreate manager
        if self.database_configs:
            try:
                self.manager = DatabaseManagerTool(self.database_configs)
            except Exception as e:
                logger.error(f"Failed to recreate manager after removal: {str(e)}")
        else:
            self.manager = None
        
        # Save to file
        self._save_to_file()
        
        return {
            "status": "success",
            "message": f"Connection {name} removed successfully"
        }
    
    def _update_connection(self, name: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing database connection configuration."""
        if name not in self.database_configs:
            return {
                "status": "error",
                "message": f"Connection {name} not found. Use add_connection to create it."
            }
        
        # Store old config for rollback
        old_config = self.database_configs[name].copy()
        
        # Update the configuration
        self.database_configs[name].update(config)
        
        # Recreate manager with updated configs
        try:
            self.manager = DatabaseManagerTool(self.database_configs)
        except Exception as e:
            # Rollback on error
            self.database_configs[name] = old_config
            return {
                "status": "error",
                "message": f"Failed to update connection: {str(e)}"
            }
        
        # Save to file
        self._save_to_file()
        
        return {
            "status": "success",
            "message": f"Connection {name} updated successfully"
        }
    
    def get_manager(self) -> Optional[DatabaseManagerTool]:
        """Get the database manager instance."""
        return self.manager
    
    def format_for_llm(self, result: Dict[str, Any]) -> str:
        """Format configuration results for LLM consumption."""
        if result.get("status") == "error":
            return f"Database Config error: {result.get('message', 'Unknown error')}"
        
        if "connections" in result:
            output = f"Database Connections ({result.get('total_connections', 0)} total):\n"
            
            for name, config in result["connections"].items():
                db_type = config.get('database_type', 'unknown')
                host = config.get('host', 'N/A')
                database = config.get('database', 'N/A')
                
                output += f"- {name} ({db_type}): {host}/{database}\n"
            
            return output
        
        elif "connection_tests" in result:
            output = "Database Connection Test Results:\n"
            
            for name, test_result in result["connection_tests"].items():
                status = test_result.get("status", "unknown")
                output += f"- {name}: {status.upper()}\n"
                
                if status == "error":
                    output += f"  Error: {test_result.get('message', 'Unknown error')}\n"
            
            return output
        
        else:
            return result.get("message", str(result))
    
    @staticmethod
    def create_sample_config() -> Dict[str, Dict[str, Any]]:
        """Create a sample database configuration for reference."""
        return {
            "local_postgres": {
                "database_type": "postgresql",
                "host": "localhost",
                "port": 5432,
                "database": "myapp",
                "username": "postgres",
                "password": "password",
                "schema": "public"
            },
            "analytics_snowflake": {
                "database_type": "snowflake",
                "account": "mycompany.snowflakecomputing.com",
                "username": "analyst",
                "password": "password",
                "warehouse": "COMPUTE_WH",
                "database": "ANALYTICS",
                "schema": "PUBLIC",
                "role": "ANALYST_ROLE"
            },
            "staging_mysql": {
                "database_type": "mysql",
                "host": "staging-db.company.com",
                "port": 3306,
                "database": "staging",
                "username": "staging_user",
                "password": "password"
            },
            "local_sqlite": {
                "database_type": "sqlite",
                "database": "/path/to/database.db"
            }
        }
    
    @staticmethod
    def generate_env_template() -> str:
        """Generate environment variable template for database configurations."""
        template = """
# Database Configuration Environment Variables
# Format: DB_<connection_name>_<setting>=<value>

# PostgreSQL Example
DB_PROD_DATABASE_TYPE=postgresql
DB_PROD_HOST=prod-db.company.com
DB_PROD_PORT=5432
DB_PROD_DATABASE=production
DB_PROD_USERNAME=prod_user
DB_PROD_PASSWORD=secure_password
DB_PROD_SCHEMA=public

# Snowflake Example
DB_ANALYTICS_DATABASE_TYPE=snowflake
DB_ANALYTICS_ACCOUNT=mycompany.snowflakecomputing.com
DB_ANALYTICS_USERNAME=analyst
DB_ANALYTICS_PASSWORD=secure_password
DB_ANALYTICS_WAREHOUSE=COMPUTE_WH
DB_ANALYTICS_DATABASE=ANALYTICS
DB_ANALYTICS_SCHEMA=PUBLIC
DB_ANALYTICS_ROLE=ANALYST_ROLE

# MySQL Example
DB_STAGING_DATABASE_TYPE=mysql
DB_STAGING_HOST=staging-mysql.company.com
DB_STAGING_PORT=3306
DB_STAGING_DATABASE=staging
DB_STAGING_USERNAME=staging_user
DB_STAGING_PASSWORD=secure_password

# SQLite Example
DB_LOCAL_DATABASE_TYPE=sqlite
DB_LOCAL_DATABASE=/path/to/local.db
"""
        return template.strip()
