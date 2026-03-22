"""
Sub-account context management with thread-safe switching.

Provides context-aware sub-account selection using contextvars for proper
thread/async isolation. Supports event listeners for account switch monitoring.
"""
import contextvars
from contextlib import contextmanager
from threading import RLock
from typing import Callable, Set, Optional
import logging


logger = logging.getLogger(__name__)


class SubAccountProvider:
    """
    Thread-safe sub-account context manager.

    Uses contextvars for proper thread and async task isolation. Each
    context (thread/task) can have its own active sub-account without
    affecting others.

    Features:
        - Thread-safe default account management
        - Context-aware account switching with contextvars
        - Event listeners for account switch notifications
        - Proper cleanup with context managers
        - Support for nested contexts

    Example:
        >>> provider = SubAccountProvider(default_sub='main_account')
        >>>
        >>> # Global default
        >>> print(provider.current())  # 'main_account'
        >>>
        >>> # Context-specific override
        >>> with provider.use('sub_account_1'):
        ...     print(provider.current())  # 'sub_account_1'
        ...     # Operations here use sub_account_1
        >>>
        >>> print(provider.current())  # 'main_account' (restored)

    Thread Safety:
        All operations are thread-safe. Multiple threads can use
        different sub-accounts concurrently without interference.
    """

    def __init__(self, default_sub: str):
        """
        Initialize sub-account provider.

        Args:
            default_sub: Default sub-account ID to use when no
                        context-specific account is set.

        Note:
            The default can be changed later with set_default().
        """
        self._default = default_sub
        # ContextVar provides thread and async task isolation
        # Each context gets its own value
        self._current: contextvars.ContextVar[Optional[str]] = (
            contextvars.ContextVar("pb_current_sub", default=None)
        )
        self._lock = RLock()
        self._listeners: Set[Callable[..., None]] = set()
        
        logger.debug(
            f"SubAccountProvider initialized with default: {default_sub}"
        )

    def current(self) -> str:
        """
        Get the current active sub-account ID.

        Returns the context-specific account if set, otherwise the
        global default.

        Returns:
            Active sub-account ID.

        Thread Safety:
            Each thread/task sees its own current value without
            needing locks.

        Example:
            >>> provider.current()
            'main_account'
            >>> with provider.use('temp_account'):
            ...     assert provider.current() == 'temp_account'
        """
        v = self._current.get()
        return v if v is not None else self._default

    def set_default(self, sub_id: str) -> None:
        """
        Change the global default sub-account.

        This affects all contexts that don't have a context-specific
        override. Does not affect contexts currently using an override.

        Args:
            sub_id: New default sub-account ID.

        Events:
            Emits switch event if current effective account changes.

        Thread Safety:
            Thread-safe via internal locking.

        Example:
            >>> provider.set_default('new_default')
            >>> provider.current()  # 'new_default'
        """
        with self._lock:
            old = self.current()
            self._default = sub_id
            logger.info(
                f"Default sub-account changed: {old} -> {sub_id}"
            )
        
        # Emit event outside lock to prevent deadlock if listener
        # calls back into provider
        self._emit(old, self.current(), scope="default")

    @contextmanager
    def use(self, sub_id: str):
        """
        Context manager for temporary sub-account switching.

        Within the context, all operations use the specified sub-account.
        Original account is restored on exit, even if exception occurs.

        Args:
            sub_id: Sub-account ID to use within context.

        Yields:
            None

        Events:
            - Emits 'context-enter' event on entry
            - Emits 'context-exit' event on exit

        Thread Safety:
            Safe to use in multiple threads. Each thread maintains
            its own context stack.

        Example:
            >>> with provider.use('trading_account'):
            ...     # All operations here use trading_account
            ...     place_order(...)
            ...
            ...     with provider.use('hedging_account'):
            ...         # Nested context uses hedging_account
            ...         place_hedge_order(...)
            ...
            ...     # Back to trading_account
            ...     check_positions()
            >>>
            >>> # Restored to original account

        Raises:
            Exception: Any exception from within the context is
                      propagated after cleanup.
        """
        old = self.current()
        token = self._current.set(sub_id)
        
        logger.debug(
            f"Entering sub-account context: {old} -> {sub_id}"
        )
        self._emit(old, sub_id, scope="context-enter")
        
        try:
            yield
        finally:
            # Always restore, even on exception
            self._current.reset(token)
            restored = self.current()
            
            logger.debug(
                f"Exiting sub-account context: {sub_id} -> {restored}"
            )
            self._emit(sub_id, restored, scope="context-exit")

    def on_switch(self, fn: Callable[..., None]) -> None:
        """
        Register a listener for sub-account switch events.

        Listener is called whenever the active sub-account changes,
        including context enter/exit and default changes.

        Args:
            fn: Callback function accepting keyword arguments:
                - old: Previous sub-account ID
                - new: New sub-account ID
                - scope: Switch scope ('default', 'context-enter',
                         'context-exit')

        Thread Safety:
            Thread-safe registration via internal locking.

        Example:
            >>> def log_switches(old, new, scope):
            ...     print(f"[{scope}] {old} -> {new}")
            >>>
            >>> provider.on_switch(log_switches)
            >>>
            >>> with provider.use('temp'):
            ...     pass  # Triggers enter and exit events

        Note:
            Listeners are called synchronously. Avoid long-running
            operations in listeners to prevent blocking switches.
        """
        with self._lock:
            self._listeners.add(fn)
            logger.debug(
                f"Registered sub-account switch listener: {fn.__name__}"
            )

    def off_switch(self, fn: Callable[..., None]) -> None:
        """
        Unregister a sub-account switch listener.

        Args:
            fn: Previously registered callback function.

        Thread Safety:
            Thread-safe via internal locking.

        Example:
            >>> provider.off_switch(log_switches)
        """
        with self._lock:
            self._listeners.discard(fn)
            logger.debug(
                f"Unregistered sub-account switch listener: {fn.__name__}"
            )

    def get_default(self) -> str:
        """
        Get the global default sub-account ID.

        Returns:
            Default sub-account ID.

        Thread Safety:
            Thread-safe read via internal locking.
        """
        with self._lock:
            return self._default

    def _emit(self, old: str, new: str, scope: str) -> None:
        """
        Emit switch event to all listeners.

        Internal method called when sub-account changes. Catches
        and logs listener exceptions to prevent disruption.

        Args:
            old: Previous sub-account ID.
            new: New sub-account ID.
            scope: Event scope ('default', 'context-enter',
                   'context-exit').

        Thread Safety:
            Uses snapshot of listeners to allow safe iteration
            without holding lock.
        """
        # No-op if no actual change
        if old == new:
            return
        
        # Get snapshot to iterate without holding lock
        with self._lock:
            listeners = list(self._listeners)
        
        # Call listeners outside lock to prevent deadlock
        for fn in listeners:
            try:
                fn(old=old, new=new, scope=scope)
            except Exception as e:
                # Log but don't propagate to prevent disrupting switch
                logger.error(
                    f"Sub-account switch listener {fn.__name__} failed: {e}",
                    exc_info=True
                )
