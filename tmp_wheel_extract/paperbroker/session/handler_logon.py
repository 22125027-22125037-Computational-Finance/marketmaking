"""
FIX session logon event handler.

Handles FIX session lifecycle events with structured logging.
"""
import logging


logger = logging.getLogger(__name__)


class LogonHandler:
    """
    Handler for FIX session lifecycle events.

    Provides callback hooks for session creation, logon success,
    and logout events with structured logging for monitoring.

    Features:
        - Session lifecycle tracking
        - Structured logging for all events
        - Simple callback interface

    Example:
        >>> handler = LogonHandler(logger)
        >>> handler.on_create(session_id)
        >>> handler.on_logon(session_id)

    Thread Safety:
        All methods are thread-safe as they only perform logging.

    Note:
        This is a lightweight handler primarily for logging.
        Business logic should be handled in FIXApp.
    """

    def __init__(self, logger: logging.Logger):
        """
        Initialize logon handler.

        Args:
            logger: Logger instance for event recording.
        """
        self.logger = logger

    def on_create(self, sessionID) -> None:
        """
        Handle session creation event.

        Called when QuickFIX creates a new session instance,
        before any connection is established.

        Args:
            sessionID: QuickFIX SessionID instance.

        Thread Safety:
            Safe to call from any thread.
        """
        self.logger.info(f"Session created: {sessionID}")

    def on_logon(self, sessionID) -> None:
        """
        Handle successful logon event.

        Called when FIX logon handshake completes successfully
        and session is ready for trading.

        Args:
            sessionID: QuickFIX SessionID instance.

        Thread Safety:
            Safe to call from any thread.
        """
        self.logger.info(f"Logon success: {sessionID}")

    def on_logout(self, sessionID) -> None:
        """
        Handle logout event.

        Called when session is disconnected, either by client
        request or server termination.

        Args:
            sessionID: QuickFIX SessionID instance.

        Thread Safety:
            Safe to call from any thread.
        """
        self.logger.info(f"Logged out: {sessionID}")
