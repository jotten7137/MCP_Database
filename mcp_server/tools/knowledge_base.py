import aiohttp
from typing import Dict, Any, Optional, List

from ..config import settings
from .base import BaseTool

class KnowledgeBaseTool(BaseTool):
    """
    Tool for querying a specialized knowledge base using RAG.
    """
    
    def __init__(self):
        super().__init__(
            name="knowledge_search",
            description="Search company knowledge base, FAQs, and internal documentation"
        )
        self.kb_endpoint = settings.TOOL_CONFIGS.get("knowledge_base", {}).get("endpoint", "http://localhost:8000")
        self.api_key = settings.TOOL_CONFIGS.get("knowledge_base", {}).get("api_key", "")
    
    @property
    def parameters(self) -> Dict[str, Any]:
        """Define the parameters for the knowledge base tool."""
        return {
            "type": "object",
            "properties": {
                "question": {
                    "type": "string",
                    "description": "Question to search in the knowledge base"
                },
                "category": {
                    "type": "string",
                    "enum": ["general", "technical", "policy", "faq", "procedures"],
                    "description": "Category of knowledge to search",
                    "default": "general"
                }
            },
            "required": ["question"]
        }
    
    async def execute(self, question: str, category: str = "general") -> Dict[str, Any]:
        """
        Query the knowledge base for relevant information.
        
        Args:
            question: Question to search for
            category: Category of knowledge to search
            
        Returns:
            Dict with knowledge base response
        """
        
        # Mock knowledge base for demonstration
        # Replace with actual RAG system integration
        mock_kb = {
            "technical": {
                "How do I deploy the application?": "Use Docker: `docker build -t app . && docker run -p 8000:8000 app`",
                "What's the API rate limit?": "100 requests per minute for authenticated users, 10 for anonymous users.",
                "How to setup OAuth?": "Configure OAuth in settings.py with your provider credentials."
            },
            "policy": {
                "What's the vacation policy?": "Employees get 15 days PTO annually, plus company holidays.",
                "Remote work policy?": "Hybrid work allowed 3 days remote per week with manager approval.",
                "Expense reimbursement?": "Submit expenses via portal within 30 days with receipts."
            },
            "faq": {
                "How to reset password?": "Click 'Forgot Password' on login page or contact IT support.",
                "Who to contact for support?": "Technical: tech@company.com, HR: hr@company.com, General: support@company.com",
                "Office hours?": "Monday-Friday 9AM-6PM EST, lunch break 12-1PM."
            }
        }
        
        # Simple keyword matching (replace with actual RAG search)
        category_kb = mock_kb.get(category, {})
        best_match = None
        best_score = 0.0
        
        for kb_question, answer in category_kb.items():
            # Simple similarity (replace with proper vector search)
            words_overlap = len(set(question.lower().split()) & set(kb_question.lower().split()))
            score = words_overlap / max(len(question.split()), len(kb_question.split()))
            
            if score > best_score:
                best_score = score
                best_match = (kb_question, answer)
        
        if best_match and best_score > 0.2:  # Threshold for relevance
            return {
                "question": question,
                "answer": best_match[1],
                "matched_question": best_match[0],
                "category": category,
                "confidence": best_score
            }
        else:
            return {
                "question": question,
                "answer": f"No relevant information found in {category} knowledge base for: {question}",
                "matched_question": None,
                "category": category,
                "confidence": 0.0
            }
    
    def format_for_llm(self, result: Dict[str, Any]) -> str:
        """Format the knowledge base results for the LLM."""
        if result.get("status") == "error":
            return f"Knowledge base error: {result.get('error')}"
        
        data = result.get("result", {})
        answer = data.get("answer", "No answer found")
        category = data.get("category", "general")
        confidence = data.get("confidence", 0.0)
        matched = data.get("matched_question")
        
        formatted = f"Knowledge Base ({category}):\n{answer}"
        
        if matched and confidence > 0.3:
            formatted += f"\n\n(Based on: {matched})"
        
        return formatted
