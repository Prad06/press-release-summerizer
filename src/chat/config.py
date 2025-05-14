# src/chat/config.py

import os
from typing import Dict, Any, Optional

class ChatConfig:
    """Configuration class for Chat module"""
    
    def __init__(
        self,
        model,
        temperature,
        max_tokens,
        timeout: int = 30,
        **kwargs
    ):
        """
        Initialize configuration with parameters or environment variables
        
        Args:
            api_key: API key for LLM service. If None, uses OPENAI_API_KEY env var
            model: Model name to use (default: gpt-3.5-turbo)
            temperature: Sampling temperature (0-1), lower is more deterministic
            max_tokens: Maximum tokens to generate
            top_p: Nucleus sampling parameter
            frequency_penalty: Penalty for token frequency
            presence_penalty: Penalty for token presence
            timeout: Request timeout in seconds
            **kwargs: Additional parameters passed to the LLM API
        """
        # API key handling
        self.api_key = os.environ.get("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("API key must be provided via OPENAI_API_KEY environment variable")
        
        # Core parameters
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens

        # Request handling
        self.timeout = timeout
        
        # Store any additional kwargs
        self.extra_params = kwargs
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to a dictionary for API calls
        
        Returns:
            Dictionary of parameters to pass to LLM API
        """
        result = {
            "model": self.model,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            **self.extra_params
        }
        return result
    
    def __getitem__(self, key: str) -> Any:
        """Enable dictionary-like access to config attributes"""
        return getattr(self, key, None)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get attribute value with optional default"""
        return getattr(self, key, default)