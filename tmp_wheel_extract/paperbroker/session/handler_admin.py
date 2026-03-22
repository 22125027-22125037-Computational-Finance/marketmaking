"""
FIX administrative message handler.

Handles FIX admin messages including authentication credential
injection and sub-account context management.
"""
import quickfix as fix
from typing import TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from paperbroker.sub_account import SubAccountProvider


logger = logging.getLogger(__name__)


class AdminHandler:
    """
    Handler for FIX administrative messages.

    Manages outgoing and incoming administrative messages,
    including logon credential injection and sub-account context.

    Features:
        - Automatic credential injection on logon
        - Sub-account context awareness
        - Heartbeat interval configuration
        - Structured logging for debugging

    Example:
        >>> handler = AdminHandler(sub_provider, "user", "pass", logger)
        >>> handler.to_admin(logon_message, session_id)
        # Credentials automatically injected

    Thread Safety:
        All methods are thread-safe. Sub-account context is
        properly isolated per thread via SubAccountProvider.

    Note:
        This handler is called by QuickFIX engine for all
        administrative messages (logon, logout, heartbeat, etc.).
    """

    def __init__(
        self,
        sub_provider: "SubAccountProvider",
        username: str,
        password: str,
        logger: logging.Logger,
    ):
        """
        Initialize admin handler.

        Args:
            sub_provider: Sub-account context provider for
                         account field injection.
            username: FIX username for authentication.
            password: FIX password for authentication.
            logger: Logger instance for message debugging.
        """
        self.sub_provider = sub_provider
        self.username = username
        self.password = password
        self.logger = logger

    def to_admin(self, message, sessionID) -> None:
        """
        Process outgoing administrative messages.

        Called by QuickFIX before sending admin messages to server.
        Injects credentials for logon messages and adds sub-account
        context to all messages.

        Args:
            message: QuickFIX Message instance to process.
            sessionID: QuickFIX SessionID for the connection.

        Thread Safety:
            Safe to call from any thread. Sub-account context
            is thread-local via SubAccountProvider.

        Note:
            - Logon messages (MsgType=A) get password, encryption,
              and heartbeat fields
            - All messages get username and current sub-account
        """
        msg_type = message.getHeader().getField(fix.MsgType()).getString()
        
        # Inject logon-specific fields
        if msg_type == "A":  # Logon message
            message.setField(fix.Password(self.password))
            message.setField(fix.EncryptMethod(0))  # No encryption
            message.setField(fix.HeartBtInt(30))  # 30 second heartbeat
        
        # Add common fields to all admin messages
        message.setField(fix.Username(self.username))
        message.setField(fix.Account(self.sub_provider.current()))
        
        self.logger.debug(f"[TO ADMIN] {message}")

    def from_admin(self, message, sessionID) -> None:
        """
        Process incoming administrative messages.

        Called by QuickFIX when admin messages arrive from server.
        Primarily used for logging and monitoring.

        Args:
            message: QuickFIX Message instance received.
            sessionID: QuickFIX SessionID for the connection.

        Thread Safety:
            Safe to call from any thread.

        Note:
            Logout messages during logon are handled in FIXApp
            for logon error detection.
        """
        self.logger.debug(f"[FROM ADMIN] {message}")
