# paperbroker/events.py
"""
Event bus implementation for asynchronous, decoupled communication.

This module provides a thread-safe, observable event bus that supports:
- Namespaced events (e.g., 'fix:logon', 'order:filled')
- Synchronous and asynchronous handlers
- Error isolation (handler exceptions won't crash emitters)
- Structured logging and error reporting
"""
from collections import defaultdict
from typing import Callable, Dict, List, Any, Optional
import threading
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor


logger = logging.getLogger(__name__)


class EventBus:
    """
    Thread-safe event bus for publish-subscribe pattern.

    The EventBus enables decoupled communication between components through
    named events. Handlers can be synchronous or asynchronous, and failures
    in one handler won't affect others.

    Features:
        - Thread-safe registration and emission
        - Automatic async handler detection and execution
        - Error isolation with structured logging
        - Error event emission for monitoring
        - Optional executor for handler isolation

    Example:
        >>> bus = EventBus()
        >>> bus.on('order:filled', lambda order_id, price: \
        ...     print(f"Filled {order_id} @ {price}"))
        >>> bus.emit('order:filled', order_id='ABC123', price=100.5)

    Thread Safety:
        All methods are thread-safe. Multiple threads can register handlers
        and emit events concurrently without data corruption.

    Error Handling:
        Handler exceptions are caught, logged, and emitted as
        'event:handler_error' events. This prevents one misbehaving
        handler from affecting others.
    """

    def __init__(
        self,
        executor: Optional[ThreadPoolExecutor] = None,
        enable_async: bool = True,
    ) -> None:
        """
        Initialize the event bus.

        Args:
            executor: Optional ThreadPoolExecutor for running sync handlers
                     in separate threads. If None, handlers run inline.
            enable_async: Whether to support async handlers. If True,
                         async handlers will be scheduled on the event loop.

        Note:
            When enable_async=True and an event loop is available,
            async handlers will be executed via asyncio.create_task().
        """
        self._handlers: Dict[str, List[Callable[..., None]]] = (
            defaultdict(list)
        )
        self._lock = threading.RLock()
        self._executor = executor
        self._enable_async = enable_async
        self._handler_count = 0  # For metrics/debugging

    def on(self, event: str, handler: Callable[..., Any]) -> None:
        """
        Register a handler for the given event.

        Args:
            event: Event name (e.g., 'fix:logon', 'order:filled').
                  Convention: use namespaces with ':' separator.
            handler: Callable that accepts keyword arguments matching
                    the event payload. Can be sync or async function.

        Example:
            >>> def on_order(order_id, status):
            ...     print(f"Order {order_id}: {status}")
            >>> bus.on('order:update', on_order)

            >>> async def on_async_order(order_id, status):
            ...     await asyncio.sleep(0.1)
            ...     print(f"Order {order_id}: {status}")
            >>> bus.on('order:update', on_async_order)

        Thread Safety:
            This method is thread-safe and can be called from any thread.
        """
        with self._lock:
            self._handlers[event].append(handler)
            self._handler_count += 1
            logger.debug(
                f"Registered handler for event '{event}'. "
                f"Total handlers: {self._handler_count}"
            )

    def off(self, event: str, handler: Callable[..., Any]) -> None:
        """
        Unregister a handler from the given event.

        Args:
            event: Event name to unregister from.
            handler: The exact handler function to remove.

        Note:
            Uses identity comparison (is), not equality.
            If handler is not registered, this is a no-op.

        Thread Safety:
            This method is thread-safe and can be called from any thread.
        """
        with self._lock:
            if event in self._handlers:
                original_len = len(self._handlers[event])
                self._handlers[event] = [
                    h for h in self._handlers[event] if h is not handler
                ]
                removed = original_len - len(self._handlers[event])
                if removed > 0:
                    self._handler_count -= removed
                    logger.debug(
                        f"Unregistered handler from event '{event}'. "
                        f"Removed: {removed}"
                    )

    def emit(self, event: str, **payload: Any) -> None:
        """
        Emit an event to all registered handlers.

        This method dispatches the event to all handlers registered for
        the event name. Handlers are executed based on their type:
        - Sync handlers: executed inline or in executor if provided
        - Async handlers: scheduled on the event loop if available

        Args:
            event: Event name to emit.
            **payload: Keyword arguments passed to all handlers.

        Error Handling:
            If a handler raises an exception:
            1. Exception is logged with full traceback
            2. 'event:handler_error' event is emitted with error details
            3. Other handlers continue execution normally

        Example:
            >>> bus.emit('order:filled', order_id='ABC', price=100.5, qty=10)

        Thread Safety:
            This method is thread-safe. The handler list is copied before
            iteration to prevent modification during emission.

        Performance:
            Handlers are executed sequentially. For CPU-intensive handlers,
            consider providing an executor at initialization.
        """
        # Get handlers snapshot to avoid holding lock during execution
        with self._lock:
            handlers = list(self._handlers.get(event, []))

        if not handlers:
            logger.debug(f"No handlers registered for event '{event}'")
            return

        logger.debug(
            f"Emitting event '{event}' to {len(handlers)} handler(s)"
        )

        for handler in handlers:
            try:
                # Detect if handler is async
                if self._enable_async and asyncio.iscoroutinefunction(handler):
                    self._emit_async_handler(event, handler, payload)
                elif self._executor is not None:
                    # Run sync handler in executor
                    self._executor.submit(
                        self._safe_call_handler, event, handler, payload
                    )
                else:
                    # Run sync handler inline
                    self._safe_call_handler(event, handler, payload)

            except Exception as e:
                # This catches errors in scheduling/submission,
                # not handler errors
                logger.error(
                    f"Failed to dispatch handler for event '{event}': {e}",
                    exc_info=True
                )
                self._emit_error_event(event, handler, e, payload)

    def _safe_call_handler(
        self,
        event: str,
        handler: Callable[..., Any],
        payload: Dict[str, Any]
    ) -> None:
        """
        Safely invoke a synchronous handler with error isolation.

        Args:
            event: Event name (for logging/error reporting).
            handler: The handler function to invoke.
            payload: Keyword arguments to pass to handler.
        """
        try:
            handler(**payload)
        except Exception as e:
            logger.error(
                f"Handler {handler.__name__} failed for event '{event}': {e}",
                exc_info=True
            )
            self._emit_error_event(event, handler, e, payload)

    def _emit_async_handler(
        self,
        event: str,
        handler: Callable[..., Any],
        payload: Dict[str, Any]
    ) -> None:
        """
        Schedule an async handler on the event loop.

        Args:
            event: Event name (for logging/error reporting).
            handler: The async handler function.
            payload: Keyword arguments to pass to handler.
        """
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Create task that wraps handler with error handling
                loop.create_task(
                    self._async_handler_wrapper(event, handler, payload)
                )
            else:
                logger.warning(
                    f"Event loop not running; cannot execute async handler "
                    f"for event '{event}'"
                )
        except RuntimeError:
            # No event loop in this thread
            logger.warning(
                f"No event loop available; cannot execute async handler "
                f"for event '{event}'"
            )

    async def _async_handler_wrapper(
        self,
        event: str,
        handler: Callable[..., Any],
        payload: Dict[str, Any]
    ) -> None:
        """
        Wrapper for async handlers with error handling.

        Args:
            event: Event name (for logging/error reporting).
            handler: The async handler function.
            payload: Keyword arguments to pass to handler.
        """
        try:
            await handler(**payload)
        except Exception as e:
            logger.error(
                f"Async handler {handler.__name__} failed for "
                f"event '{event}': {e}",
                exc_info=True
            )
            # Emit error event synchronously to avoid nested async issues
            try:
                self._emit_error_event(event, handler, e, payload)
            except Exception as emit_err:
                logger.error(
                    f"Failed to emit error event: {emit_err}",
                    exc_info=True
                )

    def _emit_error_event(
        self,
        original_event: str,
        handler: Callable[..., Any],
        error: Exception,
        payload: Dict[str, Any]
    ) -> None:
        """
        Emit an error event when a handler fails.

        This allows monitoring systems to track handler failures.

        Args:
            original_event: The event that was being processed.
            handler: The handler that failed.
            error: The exception raised.
            payload: The original event payload.
        """
        try:
            # Avoid recursive error handling
            with self._lock:
                error_handlers = list(
                    self._handlers.get("event:handler_error", [])
                )

            error_payload = {
                "original_event": original_event,
                "handler_name": getattr(
                    handler, "__name__", str(handler)
                ),
                "error_type": type(error).__name__,
                "error_message": str(error),
                "original_payload": payload,
            }

            for error_handler in error_handlers:
                try:
                    error_handler(**error_payload)
                except Exception:
                    # Silently ignore errors in error handlers to prevent loops
                    pass

        except Exception:
            # Last resort: silently ignore to prevent infinite loops
            pass

    def handler_count(self, event: Optional[str] = None) -> int:
        """
        Get the number of registered handlers.

        Args:
            event: If provided, return count for specific event.
                  If None, return total handler count.

        Returns:
            Number of registered handlers.

        Example:
            >>> bus.handler_count('order:filled')
            3
            >>> bus.handler_count()
            15
        """
        with self._lock:
            if event is None:
                return self._handler_count
            return len(self._handlers.get(event, []))

    def clear(self, event: Optional[str] = None) -> None:
        """
        Clear registered handlers.

        Args:
            event: If provided, clear handlers for specific event only.
                  If None, clear all handlers.

        Warning:
            Use with caution. Clearing handlers may break existing
            event listeners that expect notifications.

        Thread Safety:
            This method is thread-safe.
        """
        with self._lock:
            if event is None:
                self._handlers.clear()
                self._handler_count = 0
                logger.info("Cleared all event handlers")
            elif event in self._handlers:
                removed = len(self._handlers[event])
                del self._handlers[event]
                self._handler_count -= removed
                logger.info(
                    f"Cleared {removed} handler(s) for event '{event}'"
                )
