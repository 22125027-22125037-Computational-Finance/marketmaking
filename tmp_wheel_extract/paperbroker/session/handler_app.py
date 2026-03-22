"""
FIX application message handler.

Handles FIX application-level messages (orders, executions, etc.)
with credential and sub-account context management.
"""
import quickfix as fix
from typing import TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from paperbroker.sub_account import SubAccountProvider


logger = logging.getLogger(__name__)


class AppHandler:
    """
    Handler for FIX application messages.

    Manages outgoing and incoming application-level messages,
    including order submissions, execution reports, and
    market data with sub-account context.

    Features:
        - Automatic credential and account injection
        - Sub-account context awareness
        - Message logging for audit trail
        - Thread-safe operation

    Example:
        >>> handler = AppHandler(sub_provider, "user", "pass", logger)
        >>> handler.to_app(order_message, session_id)
        # Username and account automatically added

    Thread Safety:
        All methods are thread-safe. Sub-account context is
        thread-local via SubAccountProvider.

    Note:
        This handler is called by QuickFIX for all application
        messages (not admin/session messages).
    """

    def __init__(
        self,
        sub_provider: "SubAccountProvider",
        username: str,
        password: str,
        logger: logging.Logger,
    ):
        """
        Initialize app message handler.

        Args:
            sub_provider: Sub-account context provider.
            username: FIX username for message tagging.
            password: FIX password (stored but not used in app msgs).
            logger: Logger instance for message tracking.
        """
        self.sub_provider = sub_provider
        self.username = username
        self.password = password
        self.logger = logger

    def to_app(self, message, sessionID) -> None:
        """
        Process outgoing application messages.

        Called by QuickFIX before sending app messages to server.
        Injects username and current sub-account context.

        Args:
            message: QuickFIX Message to process (order, cancel, etc.).
            sessionID: QuickFIX SessionID for the connection.

        Thread Safety:
            Safe to call from any thread. Each thread's sub-account
            context is properly isolated.

        Example:
            When placing an order in sub-account "trading_1":
            >>> with sub_provider.use("trading_1"):
            ...     # Order message automatically tagged with
            ...     # Account=trading_1
        """
        message.setField(fix.Username(self.username))
        message.setField(fix.Account(self.sub_provider.current()))
        self.logger.debug(f"[TO APP] {message}")

    def from_app(self, message, sessionID) -> None:
        """
        Process incoming application messages.

        Called by QuickFIX when app messages arrive from server.
        Logs messages for monitoring and debugging.

        Args:
            message: QuickFIX Message received (execution report, etc.).
            sessionID: QuickFIX SessionID for the connection.

        Thread Safety:
            Safe to call from any thread.

        Note:
            Execution reports are forwarded to OrderManager by
            FIXApp after this method completes.
        """
        self.logger.info(f"[FROM APP] {message}")
