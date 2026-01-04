# MCP Database - Multi-Database AI Assistant

A sophisticated MCP (Model Context Protocol) starter project with voice, chat, and multi-database connectivity. Features intelligent tool calling with automatic natural language to SQL conversion across PostgreSQL and SQLite databases.

## Features

### Core Functionality
-  **Voice Input & Audio Responses** - Full speech-to-text and text-to-speech
-  **Text Chat Interface** - Clean web interface for queries
-  **Multi-Database Support** - PostgreSQL, SQLite with automatic query routing
-  **Intelligent Tool Calling** - Weather, calculations, database queries
-  **Docker Containerization** - Easy deployment and scaling

### Database Capabilities
-  **Natural Language to SQL** - Ask questions in plain English
-  **Cross-Database Queries** - Query multiple databases seamlessly
-  **Security First** - Read-only access, query validation, credential protection
-  **High Performance** - Optimized queries with automatic limits and timeouts

### Current Datasets
- **Titanic Dataset** (PostgreSQL) - 418 passenger records with demographics and survival data
- **Iris Dataset** (SQLite) - 150 flower samples with measurements and species classification

## Dependencies

### Required
- **Ollama**: Download from [https://ollama.com/download](https://ollama.com/download)
- **Docker**: Download from [https://www.docker.com/products/docker-desktop/](https://www.docker.com/products/docker-desktop/)
- **Weather API**: API key from [https://openweathermap.org/](https://openweathermap.org/)
- **PostgreSQL**: For Titanic database (or use existing instance)

### Database Dependencies (Included)
- pandas, sqlalchemy, asyncpg, aiomysql, aiosqlite

## Setup Instructions

### 1. Install and Configure Ollama
```bash
# Install Ollama, then pull the reasoning model
ollama pull qwq:32b

# Alternative models that work well:
# ollama pull deepseek-coder:6.7b
# ollama pull llama3.2:latest
```

### 2. Environment Configuration (.env)
```bash
LLM_MODEL=qwq:32b
WEATHER_API_KEY=your_weather_api_key_here

# Database Configuration
DATABASE_CONFIG_FILE=./database_config.json
```

### 3. Database Configuration (database_config.json)
```json
{
  "local_sqlite": {
    "database_type": "sqlite",
    "database": "./sample_database.db"
  },
  "titanic_db": {
    "database_type": "postgresql",
    "host": "host.docker.internal",
    "port": 5432,
    "database": "titanic_db",
    "username": "postgres",
    "password": "your_postgres_password",
    "schema": "test_data"
  },
  "iris_db": {
    "database_type": "sqlite",
    "database": "./iris.sqlite"
  }
}
```

### 4. Build and Run with Docker
```bash
# Build the container
docker build -t mcp-rag .

# Run the container
docker run -d --name mcp-rag-container -p 8000:8000 mcp-rag
```

### 5. Access the Interface
- Open `index.html` in your browser, or
- Navigate to `http://localhost:8000`

## Usage Examples

### Database Queries (Natural Language)
**Titanic Dataset:**
- \"How many passengers were on the Titanic?\"
- \"What's the average age of passengers?\"
- \"Show me the first 10 passengers\"
- \"How many passengers were in first class?\"
- \"What was the survival rate by passenger class?\"

**Iris Dataset:**
- \"How many iris samples are there?\"
- \"Show me all iris species\"
- \"What's the average petal length?\"
- \"Compare petal vs sepal measurements\"

**Cross-Database:**
- \"Compare the number of records in Titanic vs Iris datasets\"

### Other Tools
- **Weather**: \"What's the weather in Denver?\"
- **Math**: \"Calculate 25 * 17 + 33\"
- **Voice**: Click \"Voice\" tab for speech input

## Architecture

### System Overview
```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Web Frontend  │────│   MCP Server     │────│   Databases     │
│   (HTML/JS)     │    │   (FastAPI)      │    │   (PG/SQLite)   │
│                 │    │                  │    │                 │
│ • Chat Interface│    │ • LLM Integration│    │ • Titanic (PG)  │
│ • Voice Input   │    │ • Tool Routing   │    │ • Iris (SQLite) │
│ • Audio Output  │    │ • Query Parsing  │    │ • Future DBs... │
└─────────────────┘    └──────────────────┘    └─────────────────┘
          │                       │                       │
          └───────────────────────┼───────────────────────┘
                                  │
                           ┌──────▼──────┐
                           │   Ollama    │
                           │   LLM API   │
                           │ (qwq:32b)   │
                           └─────────────┘
```

### Key Components
- **FastAPI Server** - Main application with WebSocket support
- **Ollama Integration** - Local LLM with advanced reasoning
- **Tool Router** - Intelligent query classification and routing
- **Database Manager** - Multi-database connection and query execution
- **Audio Services** - Speech-to-text and text-to-speech

## Database Features

### Supported Query Types
- **Count Queries**: \"How many records...\"
- **Aggregations**: \"What's the average/sum/max...\"
- **Filtering**: \"Show me passengers from first class\"
- **Sorting**: \"Order by age/fare/name\"
- **Schema Exploration**: \"What tables are available?\"

### Security Features
- **Read-Only Access**: Only SELECT and WITH queries allowed
- **Query Validation**: Dangerous operations automatically blocked
- **Credential Protection**: Passwords hidden in logs
- **Timeout Controls**: Prevent long-running queries
- **Schema Isolation**: Proper schema-qualified table access

### Performance Optimizations
- **Automatic LIMIT**: Large result sets automatically limited
- **Connection Pooling**: Efficient database connection management
- **Query Caching**: Smart caching for repeated queries
- **Execution Timing**: Performance monitoring and logging

## Project Structure
```
MCP_RAG/
├── Core System
│   ├── .env                     # Environment configuration
│   ├── Dockerfile              # Container setup
│   ├── mcp-requirements.txt    # Python dependencies
│   ├── database-requirements.txt # Database dependencies
│   └── database_config.json    # Database connections
├── Data
│   └── iris.sqlite            # Iris dataset
├── Frontend
│   ├── index.html             # Main interface
│   ├── styles.css             # Styling
│   └── app.js                 # JavaScript logic
├── Backend
│   └── mcp_server/
│       ├── main.py            # FastAPI application
│       ├── config.py          # Configuration management
│       ├── core/              # Core routing and session
│       ├── models/            # LLM and audio services
│       └── tools/             # Tool implementations
│           ├── weather.py     # Weather API integration
│           ├── calculator.py  # Math calculations
│           ├── database.py    # Main database tool
│           └── db_tools/      # Database implementations
└── Documentation
    └── README.md     # This file
```

## Current Configuration
- **Model**: qwq:32b (Advanced reasoning model with excellent tool calling)
- **Tools**: Weather API, Calculator, Multi-Database
- **Audio**: Google Text-to-Speech (gTTS)
- **Databases**: PostgreSQL (Titanic), SQLite (Iris)

## Troubleshooting

### Common Issues
1. **Ollama connection**: Ensure Ollama is running (`ollama serve`)
2. **Docker networking**: Container uses `host.docker.internal:11434`
3. **Model not found**: Pull the model with `ollama pull qwq:32b`
4. **Database connection**: Check credentials and network connectivity
5. **Permission errors**: Ensure database user has read permissions

### Database Troubleshooting
```bash
# Test database connections
\"Test all database connections\"

# Check specific connection  
\"Test the titanic_db database connection\"

# List available connections
\"List all database connections\"
```

### Logs and Debugging
```bash
# View container logs
docker logs -f mcp-rag-container

# Check Ollama accessibility
curl http://localhost:11434/api/tags

# Test database dependencies
docker exec mcp-rag-container python -c \"import asyncpg, sqlalchemy; print('Database deps OK')\"
```

## Future Enhancements

### Planned Features
- **Additional Databases**: MySQL, MongoDB, Snowflake support
- **Advanced Analytics**: Statistical analysis and visualization
- **Data Export**: CSV, JSON, Excel export capabilities
- **Query History**: Save and replay previous queries
- **User Authentication**: Multi-user support with permissions
- **API Endpoints**: RESTful API for external integrations

### Dataset Expansion
- **Financial Data**: Stock prices, economic indicators
- **Scientific Data**: Research datasets, experimental results
- **Business Intelligence**: Sales, marketing, operational metrics
- **IoT Data**: Sensor readings, device telemetry

## Contributing

Contributions welcome! Areas for improvement:
- Additional database drivers
- Query optimization techniques
- Security enhancements
- UI/UX improvements
- Documentation and examples

## License

MIT License - See LICENSE file for details.

---
