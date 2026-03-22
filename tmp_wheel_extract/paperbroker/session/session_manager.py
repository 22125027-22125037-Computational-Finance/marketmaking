"""
FIX session lifecycle manager.

Manages QuickFIX session initialization, connection lifecycle,
and event bus integration for FIX protocol communication.
"""
import quickfix as fix
from typing import Optional, TYPE_CHECKING
import logging
from paperbroker.logger import get_logger
from paperbroker.events import EventBus
from .app import FIXApp

if TYPE_CHECKING:
    from paperbroker.sub_account import SubAccountProvider


logger = logging.getLogger(__name__)


class FIXSessionManager:
    """
    High-level FIX session lifecycle manager.

    Manages QuickFIX engine initialization, session configuration,
    connection lifecycle, and event bus integration for system-wide
    event coordination.

    Features:
        - QuickFIX session initialization from config file
        - File-based message store for recovery
        - Event bus integration for FIX events
        - Logon state management
        - Graceful shutdown support

    Example:
        >>> from paperbroker.events import EventBus
        >>> from paperbroker.sub_account import SubAccountProvider
        >>>
        >>> bus = EventBus()
        >>> sub = SubAccountProvider(default_sub="main")
        >>>
        >>> manager = FIXSessionManager(
        ...     cfg_path="config.cfg",
        ...     sub_provider=sub,
        ...     username="trader",
        ...     password="secret",
        ...     bus=bus
        ... )
        >>>
        >>> manager.start()
        >>> if manager.wait_until_logged_on(timeout=10):
        ...     print("Connected!")
        ... else:
        ...     print(f"Failed: {manager.last_logon_error()}")
        >>>
        >>> manager.stop()

    Thread Safety:
        All public methods are thread-safe. QuickFIX engine
        manages its own threads for I/O.

    Note:
        This manager wraps QuickFIX SocketInitiator and provides
        a cleaner interface with event bus integration.
    """

    def __init__(
        self,
        *,
        cfg_path: str,
        sub_provider: "SubAccountProvider",
        username: str,
        password: str,
        bus: EventBus,
        log_dir: str = "logs",
        console: bool = False,
    ):
        """
        Initialize FIX session manager.

        Args:
            cfg_path: Path to QuickFIX configuration file (.cfg).
            sub_provider: Sub-account context provider for
                         multi-account support.
            username: FIX username for authentication.
            password: FIX password for authentication.
            bus: EventBus instance for system-wide event coordination.
            log_dir: Directory for log files. Defaults to "logs".
            console: Whether to also log to console. Defaults to False.

        Raises:
            quickfix.ConfigError: If config file is invalid.
            quickfix.RuntimeError: If initialization fails.

        Note:
            Session is not started automatically. Call start() to
            initiate connection.
        """
        self.logger = get_logger(log_dir, console)
        self.bus = bus

        # Load QuickFIX configuration
        self.settings = fix.SessionSettings(cfg_path)
        self.store_factory = fix.FileStoreFactory(self.settings)

        # Create FIX application
        self.app = FIXApp(
            sub_provider=sub_provider,
            username=username,
            password=password,
            logger=self.logger,
            emit=self._emit_fix,  # Bridge local events to global bus
        )
        
        # Create socket initiator
        self.initiator = fix.SocketInitiator(
            self.app, self.store_factory, self.settings
        )
        
        self.logger.debug(
            "FIXSessionManager initialized",
            extra={"config": cfg_path, "username": username}
        )

    def _emit_fix(self, event: str, **kw) -> None:
        """
        Bridge FIX events to global event bus.

        Internal method that prefixes FIX events with "fix:" namespace
        and emits them to the global event bus.

        Args:
            event: Event name (e.g., "logon", "logout").
            **kw: Event payload.

        Thread Safety:
            Safe to call from any thread. EventBus handles
            synchronization.
        """
        self.bus.emit(f"fix:{event}", **kw)

    def start(self) -> None:
        """
        Start FIX session and initiate connection.

        Starts QuickFIX engine which creates background threads
        for socket I/O and message processing. Connection to
        FIX server is attempted automatically.

        Raises:
            quickfix.RuntimeError: If engine fails to start.

        Thread Safety:
            Safe to call from any thread. Idempotent - multiple
            calls have no additional effect.

        Example:
            >>> manager.start()
            >>> # Engine running in background threads
            >>> if manager.wait_until_logged_on(timeout=10):
            ...     print("Ready to trade")
        """
        self.logger.info("Starting FIX session...")
        self.initiator.start()

    def stop(self) -> None:
        """
        Stop FIX session and disconnect.

        Stops QuickFIX engine, closes connections, and waits for
        background threads to terminate gracefully.

        Thread Safety:
            Safe to call from any thread. Blocks until engine
            fully stops.

        Example:
            >>> manager.stop()
            >>> # All connections closed, threads terminated
        """
        self.logger.info("Stopping FIX session...")
        self.initiator.stop()

    def is_logged_on(self) -> bool:
        """
        Check if currently logged on to FIX server.

        Returns:
            True if logon handshake completed and session active,
            False otherwise.

        Thread Safety:
            Safe to call from any thread.

        Example:
            >>> if manager.is_logged_on():
            ...     manager.app.place_order(...)
        """
        return self.app.is_logged_on()

    def wait_until_logged_on(
        self, timeout: Optional[float] = None
    ) -> bool:
        """
        Block until logged on or timeout expires.

        Waits for FIX logon handshake to complete. Useful for
        ensuring connection is ready before sending orders.

        Args:
            timeout: Maximum seconds to wait. None means wait forever.
                    Defaults to None.

        Returns:
            True if logged on within timeout, False if timeout expired.

        Thread Safety:
            Safe to call from multiple threads. Each thread blocks
            independently.

        Example:
            >>> manager.start()
            >>> if not manager.wait_until_logged_on(timeout=10):
            ...     error = manager.last_logon_error()
            ...     print(f"Logon failed: {error}")
            ...     sys.exit(1)
        """
        return self.app.wait_until_logged_on(timeout)

    def last_logon_error(self) -> Optional[str]:
        """
        Get last logon error message.

        Returns the error reason if last logon attempt failed,
        or None if no error or if currently logged on.

        Returns:
            Error message string, or None if no error.

        Thread Safety:
            Safe to call from any thread.

        Example:
            >>> if not manager.is_logged_on():
            ...     error = manager.last_logon_error()
            ...     if error:
            ...         print(f"Failed: {error}")
        """
        return self.app.last_logon_error()
