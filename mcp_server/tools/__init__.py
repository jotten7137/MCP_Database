"""Tool integration framework for extending LLM capabilities."""

from .base import BaseTool
from .weather import WeatherTool
from .calculator import CalculatorTool

# Try to import DatabaseTool, but handle gracefully if dependencies are missing
try:
    from .database import DatabaseTool
    DATABASE_AVAILABLE = True
except ImportError as e:
    print(f"Database functionality not available: {e}")
    DATABASE_AVAILABLE = False
    DatabaseTool = None
except Exception as e:
    print(f"Database functionality not available: {e}")
    DATABASE_AVAILABLE = False
    DatabaseTool = None

if DATABASE_AVAILABLE:
    __all__ = ["BaseTool", "WeatherTool", "CalculatorTool", "DatabaseTool"]
else:
    __all__ = ["BaseTool", "WeatherTool", "CalculatorTool"]