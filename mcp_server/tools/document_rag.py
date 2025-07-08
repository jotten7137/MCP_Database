import aiohttp
from typing import Dict, Any, Optional, List

from ..config import settings
from .base import BaseTool

class DocumentRAGTool(BaseTool):
    """
    Tool for querying a RAG system to retrieve and generate responses from documents.
    """
    
    def __init__(self):
        super().__init__(
            name="document_search",
            description="Search through uploaded documents and knowledge base using RAG"
        )
        self.rag_endpoint = settings.TOOL_CONFIGS.get("document_rag", {}).get("endpoint", "http://localhost:7000")
        self.api_key = settings.TOOL_CONFIGS.get("document_rag", {}).get("api_key", "")
    
    @property
    def parameters(self) -> Dict[str, Any]:
        """Define the parameters for the document RAG tool."""
        return {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Question or search query for the document knowledge base"
                },
                "collection": {
                    "type": "string",
                    "description": "Specific document collection to search (optional)",
                    "default": "general"
                },
                "max_results": {
                    "type": "integer",
                    "description": "Maximum number of relevant documents to retrieve",
                    "default": 5
                }
            },
            "required": ["query"]
        }
    
    async def execute(self, query: str, collection: str = "general", max_results: int = 5) -> Dict[str, Any]:
        """
        Query the RAG system for relevant documents and generated response.
        
        Args:
            query: Search query or question
            collection: Document collection to search
            max_results: Number of results to retrieve
            
        Returns:
            Dict with RAG response and source documents
        """
        
        # Example RAG API call (adjust for your RAG system)
        url = f"{self.rag_endpoint}/api/query"
        headers = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        
        payload = {
            "query": query,
            "collection": collection,
            "max_results": max_results,
            "include_sources": True
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=headers) as response:
                    if response.status != 200:
                        # Fallback response if RAG system unavailable
                        return self._fallback_response(query)
                    
                    data = await response.json()
                    
                    return {
                        "query": query,
                        "answer": data.get("answer", "No answer found"),
                        "sources": data.get("sources", []),
                        "confidence": data.get("confidence", 0.0),
                        "collection": collection
                    }
                    
        except Exception as e:
            return self._fallback_response(query, error=str(e))
    
    def _fallback_response(self, query: str, error: str = None) -> Dict[str, Any]:
        """Fallback when RAG system is unavailable."""
        return {
            "query": query,
            "answer": f"RAG system unavailable for query: '{query}'" + (f" (Error: {error})" if error else ""),
            "sources": [],
            "confidence": 0.0,
            "collection": "error"
        }
    
    def format_for_llm(self, result: Dict[str, Any]) -> str:
        """Format the RAG results for the LLM."""
        if result.get("status") == "error":
            return f"Document search error: {result.get('error')}"
        
        data = result.get("result", {})
        answer = data.get("answer", "No answer found")
        sources = data.get("sources", [])
        confidence = data.get("confidence", 0.0)
        
        formatted = f"Answer: {answer}"
        
        if confidence > 0:
            formatted += f"\nConfidence: {confidence:.1%}"
        
        if sources:
            formatted += "\n\nSources:"
            for i, source in enumerate(sources[:3], 1):  # Limit to top 3 sources
                doc_name = source.get("document", "Unknown")
                score = source.get("score", 0.0)
                formatted += f"\n{i}. {doc_name} (relevance: {score:.2f})"
        
        return formatted
