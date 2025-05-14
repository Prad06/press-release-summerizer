import time
import logging
import openai
from typing import Dict, Any, List, Optional, Union
from .config import ChatConfig

# Configure module logger
logger = logging.getLogger(__name__)

class Chat:
    """Wrapper for LLM chat APIs with configurable parameters"""
    
    def __init__(self, config: Union[ChatConfig, Dict[str, Any]]):
        """
        Initialize Chat instance with configuration
        
        Args:
            config: ChatConfig instance or dictionary with configuration parameters
        """
        # Convert dict to ChatConfig if needed
        if isinstance(config, dict):
            self.config = ChatConfig(**config)
        else:
            self.config = config
            
        # Initialize OpenAI client
        self.client = openai.OpenAI(api_key=self.config.api_key)
        
        logger.info(f"Initialized Chat with model: {self.config.model}")
    
    def ask(
        self, 
        system_msg: str, 
        user_msg: str, 
        model: Optional[str] = None, 
        **kwargs
    ) -> str:
        """
        Send a chat completion request and return the response text
        
        Args:
            system_msg: System message setting context
            user_msg: User message with the actual query
            model: Optional model override
            **kwargs: Additional parameters to pass to the API
            
        Returns:
            Response text from the LLM
            
        Raises:
            Exception: If request fails
        """
        try:
            # Prepare message payload
            messages = [
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg}
            ]
            
            # Prepare request parameters
            params = self.config.to_dict()
            params.update(kwargs)
            
            # Override model if specified
            if model:
                params["model"] = model
                
            # Log request information at debug level
            logger.debug(f"Sending request to model: {params['model']}")
            
            # Make the API call
            start_time = time.time()
            response = self.client.chat.completions.create(
                messages=messages,
                **params
            )
            elapsed_time = time.time() - start_time
            
            # Extract response text
            if not response.choices:
                raise ValueError("Empty response from LLM API")
                
            response_text = response.choices[0].message.content
            
            # Log success
            logger.debug(f"Request completed in {elapsed_time:.2f}s")
            return response_text.strip()
            
        except openai.RateLimitError as e:
            logger.warning(f"Rate limit exceeded: {e}")
            raise
        except openai.APITimeoutError as e:
            logger.warning(f"API timeout: {e}")
            raise
        except openai.APIConnectionError as e:
            logger.warning(f"API connection error: {e}")
            raise
        except Exception as e:
            logger.error(f"LLM API error: {e}")
            raise

    def ask_with_context(
        self, 
        system_msg: str, 
        user_msg: str, 
        conversation_history: List[Dict[str, str]],
        model: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Send a chat completion request with conversation history
        
        Args:
            system_msg: System message setting context
            user_msg: User message with the actual query
            conversation_history: List of previous messages in format [{"role": "user", "content": "..."}, ...]
            model: Optional model override
            **kwargs: Additional parameters to pass to the API
            
        Returns:
            Response text from the LLM
        """
        try:
            # Construct full messages array with history
            messages = [{"role": "system", "content": system_msg}]
            messages.extend(conversation_history)
            messages.append({"role": "user", "content": user_msg})
            
            # Prepare request parameters
            params = self.config.to_dict()
            params.update(kwargs)
            
            # Override model if specified
            if model:
                params["model"] = model
                
            # Make the API call
            start_time = time.time()
            response = self.client.chat.completions.create(
                messages=messages,
                **params
            )
            elapsed_time = time.time() - start_time
            
            # Extract response text
            if not response.choices:
                raise ValueError("Empty response from LLM API")
                
            response_text = response.choices[0].message.content
            
            # Log success
            logger.debug(f"Request with context completed in {elapsed_time:.2f}s")
            return response_text.strip()
            
        except Exception as e:
            logger.error(f"LLM API error in ask_with_context: {e}")
            raise