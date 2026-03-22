"""
HTTP REST session wrapper with error handling.

Provides simple HTTP client for REST API communication with
automatic error handling and response parsing.
"""
import requests
from typing import Dict, Any, Optional
import logging


logger = logging.getLogger(__name__)


class RestSession:
    """
    Simple HTTP REST client wrapper.

    Wraps the requests library with automatic error handling,
    JSON serialization, and structured logging.

    Features:
        - Automatic JSON request/response handling
        - HTTP error detection via raise_for_status()
        - Structured logging for debugging
        - Base URL normalization

    Example:
        >>> session = RestSession("https://api.example.com")
        >>> result = session.post("/orders", json={"symbol": "AAPL"})
        >>> print(result)  # Parsed JSON response
        {'orderId': '123', 'status': 'accepted'}

    Thread Safety:
        Requests library sessions are thread-safe for basic usage.
        Each RestSession instance can be used from multiple threads.

    Note:
        For async operations, consider migrating to httpx.AsyncClient.
    """

    def __init__(self, base_url: str):
        """
        Initialize REST session.

        Args:
            base_url: Base URL for all requests
                     (e.g., "https://api.example.com").
                     Trailing slashes are automatically stripped.

        Example:
            >>> session = RestSession("https://api.trading.com/v1/")
            >>> # base_url stored as "https://api.trading.com/v1"
        """
        self.base_url = base_url.rstrip("/")
        logger.debug(f"RestSession initialized with base_url: {self.base_url}")

    def post(
        self,
        path: str,
        json: Optional[Dict[str, Any]] = None,
        timeout: float = 30.0
    ) -> Any:
        """
        Send POST request with JSON payload.

        Args:
            path: URL path relative to base_url (e.g., "/orders").
            json: JSON-serializable dict to send as request body.
                 Optional, defaults to None.
            timeout: Request timeout in seconds. Defaults to 30.0.

        Returns:
            Parsed JSON response as dict, list, or primitive type.

        Raises:
            requests.HTTPError: If response status is 4xx or 5xx.
            requests.Timeout: If request exceeds timeout.
            requests.RequestException: For other request failures.
            ValueError: If response is not valid JSON.

        Example:
            >>> session.post("/orders", json={"symbol": "AAPL", "qty": 100})
            {'orderId': '123', 'status': 'accepted'}

        Thread Safety:
            Safe to call from multiple threads with same instance.
        """
        url = f"{self.base_url}{path}"
        
        logger.debug(
            f"POST {url}",
            extra={"payload_keys": list(json.keys()) if json else []}
        )
        
        try:
            response = requests.post(url, json=json, timeout=timeout)
            response.raise_for_status()
            
            result = response.json()
            logger.debug(
                f"POST {url} succeeded",
                extra={"status_code": response.status_code}
            )
            return result
            
        except requests.HTTPError as e:
            logger.error(
                f"POST {url} HTTP error",
                extra={
                    "status_code": e.response.status_code,
                    "response_text": e.response.text[:200]
                },
                exc_info=True
            )
            raise
        except requests.Timeout:
            logger.error(f"POST {url} timed out after {timeout}s")
            raise
        except Exception as e:
            logger.error(
                f"POST {url} failed: {e}",
                exc_info=True
            )
            raise

    def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: float = 30.0
    ) -> Any:
        """
        Send GET request with query parameters.

        Args:
            path: URL path relative to base_url (e.g., "/orders/123").
            params: Query parameters as dict. Optional, defaults to None.
            timeout: Request timeout in seconds. Defaults to 30.0.

        Returns:
            Parsed JSON response as dict, list, or primitive type.

        Raises:
            requests.HTTPError: If response status is 4xx or 5xx.
            requests.Timeout: If request exceeds timeout.
            requests.RequestException: For other request failures.
            ValueError: If response is not valid JSON.

        Example:
            >>> session.get("/orders", params={"symbol": "AAPL"})
            [{'orderId': '123', 'symbol': 'AAPL', 'qty': 100}]

        Thread Safety:
            Safe to call from multiple threads with same instance.
        """
        url = f"{self.base_url}{path}"
        
        logger.debug(
            f"GET {url}",
            extra={"params": params or {}}
        )
        
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            
            result = response.json()
            logger.debug(
                f"GET {url} succeeded",
                extra={"status_code": response.status_code}
            )
            return result
            
        except requests.HTTPError as e:
            logger.error(
                f"GET {url} HTTP error",
                extra={
                    "status_code": e.response.status_code,
                    "response_text": e.response.text[:200]
                },
                exc_info=True
            )
            raise
        except requests.Timeout:
            logger.error(f"GET {url} timed out after {timeout}s")
            raise
        except Exception as e:
            logger.error(
                f"GET {url} failed: {e}",
                exc_info=True
            )
            raise
