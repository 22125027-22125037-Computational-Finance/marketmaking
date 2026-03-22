"""
QuickFIX Application implementation with event-driven architecture.

Provides FIX protocol integration with logon state management,
order routing, and event bus coordination.
"""
import quickfix as fix
import threading
from typing import Optional, Callable, TYPE_CHECKING
import logging
from paperbroker.logger import get_logger
from .OrderManager import OrderManager
from .handler_logon import LogonHandler
from .handler_admin import AdminHandler
from .handler_app import AppHandler

if TYPE_CHECKING:
    from paperbroker.sub_account import SubAccountProvider


logger = logging.getLogger(__name__)


class FIXApp(fix.Application):
    """
    QuickFIX Application implementation with event-driven design.

    Implements QuickFIX Application interface to handle FIX protocol
    lifecycle, message routing, and order management with event
    emission for system-wide coordination.

    Features:
        - Thread-safe logon state management
        - Event emission for logon/logout/errors
        - Delegation to specialized handlers
        - Order routing via OrderManager
        - Automatic error isolation
        - Sub-account context awareness

    Example:
        >>> from paperbroker.sub_account import SubAccountProvider
        >>> sub = SubAccountProvider(default_sub="main")
        >>>
        >>> def on_event(event, **kw):
        ...     print(f"Event: {event}, {kw}")
        >>>
        >>> app = FIXApp(
        ...     sub_provider=sub,
        ...     username="trader",
        ...     password="secret",
        ...     emit=on_event
        ... )
        >>>
        >>> # Wait for logon
        >>> app.wait_until_logged_on(timeout=10)
        >>>
        >>> # Place order
        >>> app.place_order("AAPL", "BUY", 100, 150.0)

    Thread Safety:
        All public methods are thread-safe. QuickFIX callbacks
        are executed from engine threads and properly synchronized.

    Note:
        This is a low-level class typically used via FIXSessionManager.
        Direct usage requires QuickFIX engine setup.
    """

    def __init__(
        self,
        sub_provider: "SubAccountProvider",
        username: str,
        password: str,
        logger: Optional[logging.Logger] = None,
        console: bool = False,
        emit: Optional[Callable[[str], None]] = None,
    ):
        """
        Initialize FIX application.

        Args:
            sub_provider: Sub-account context provider for
                         multi-account support.
            username: FIX username for authentication.
            password: FIX password for authentication.
            logger: Logger instance. If None, creates default logger.
            console: Whether to log to console. Defaults to False.
            emit: Event emitter callback. Called with (event, **payload).
                 If None, events are not emitted.

        Note:
            Event emitter is called for: logon, logout, logon_error.
        """
        super().__init__()
        
        # Logger
        self.logger = logger or get_logger(console=console)

        # External event emitter (namespaced at upper layer if needed)
        self._emit = emit or (lambda *args, **kwargs: None)

        # State
        self._session_id: Optional[fix.SessionID] = None
        self._logon_event = threading.Event()
        self._last_logon_error: Optional[str] = None

        # Handlers
        self.logon_handler = LogonHandler(logger=self.logger)
        self.admin_handler = AdminHandler(
            sub_provider=sub_provider,
            username=username,
            password=password,
            logger=self.logger,
        )
        self.app_handler = AppHandler(
            sub_provider=sub_provider,
            username=username,
            password=password,
            logger=self.logger,
        )

        # Order manager (with event emitter)
        self.order_manager = OrderManager(logger=self.logger, emit=self._emit)

    # -------- Public helpers (sync) --------
    def is_logged_on(self) -> bool:
        """
        Check if currently logged on.

        Returns:
            True if logon event is set, False otherwise.

        Thread Safety:
            Safe to call from any thread.
        """
        return self._logon_event.is_set()

    def wait_until_logged_on(
        self, timeout: Optional[float] = None
    ) -> bool:
        """
        Block until logged on or timeout.

        Args:
            timeout: Maximum seconds to wait. None means forever.

        Returns:
            True if logged on within timeout, False if timed out.

        Thread Safety:
            Safe to call from multiple threads.

        Example:
            >>> if app.wait_until_logged_on(timeout=10):
            ...     print("Ready!")
        """
        return self._logon_event.wait(timeout=timeout)

    def last_logon_error(self) -> Optional[str]:
        """
        Get last logon error message.

        Returns:
            Error message if last logon failed, None otherwise.

        Thread Safety:
            Safe to call from any thread.
        """
        return self._last_logon_error

    def get_session_id(self) -> Optional[fix.SessionID]:
        """
        Get current QuickFIX session ID.

        Returns:
            SessionID if session created, None otherwise.

        Thread Safety:
            Safe to call from any thread.
        """
        return self._session_id

    # -------- QuickFIX lifecycle --------
    def onCreate(self, sessionID) -> None:  # noqa: N802
        """
        QuickFIX callback: Session created.

        Called when QuickFIX creates a session instance. This happens
        before any connection attempt.

        Args:
            sessionID: QuickFIX SessionID instance.

        Thread Safety:
            Called from QuickFIX engine thread.

        Note:
            Exceptions are caught and logged to prevent engine crashes.
        """
        self._session_id = sessionID
        try:
            self.logon_handler.on_create(sessionID)
        except Exception as e:
            self.logger.error(
                "event=onCreate_handler_failed",
                extra={"error": str(e)}
            )

    def onLogon(self, sessionID) -> None:  # noqa: N802
        """
        QuickFIX callback: Logon successful.

        Called when FIX logon handshake completes successfully.
        Sets logon event and emits logon event to listeners.

        Args:
            sessionID: QuickFIX SessionID instance.

        Events:
            Emits "logon" event with session_id payload.

        Thread Safety:
            Called from QuickFIX engine thread.
        """
        self._session_id = sessionID
        self._last_logon_error = None
        self._logon_event.set()
        try:
            self.logon_handler.on_logon(sessionID)
            self.order_manager.set_session(sessionID)
        finally:
            # Always emit after internal state is consistent
            self._safe_emit("logon", session_id=sessionID)

    def onLogout(self, sessionID) -> None:  # noqa: N802
        """
        QuickFIX callback: Session logout.

        Called when session disconnects, either by client request
        or server termination.

        Args:
            sessionID: QuickFIX SessionID instance.

        Events:
            Emits "logout" event with session_id payload.

        Thread Safety:
            Called from QuickFIX engine thread.
        """
        self._logon_event.clear()
        try:
            self.logon_handler.on_logout(sessionID)
        finally:
            self._safe_emit("logout", session_id=sessionID)

    # -------- Admin/Application messages --------
    def toAdmin(self, message, sessionID) -> None:  # noqa: N802
        """
        QuickFIX callback: Outgoing admin message.

        Called before sending administrative messages (logon,
        heartbeat, etc.). Delegates to AdminHandler for credential
        injection.

        Args:
            message: QuickFIX Message to process.
            sessionID: QuickFIX SessionID instance.

        Thread Safety:
            Called from QuickFIX engine thread.

        Note:
            AdminHandler injects credentials for logon messages.
        """
        try:
            self.admin_handler.to_admin(message, sessionID)
        except Exception as e:
            self.logger.error(
                "event=toAdmin_handler_failed",
                extra={"error": str(e)}
            )

    def fromAdmin(self, message, sessionID) -> None:  # noqa: N802
        """
        QuickFIX callback: Incoming admin message.

        Called when administrative messages arrive. Handles logon
        error detection by checking for Logout messages before
        logon completion.

        Args:
            message: QuickFIX Message received.
            sessionID: QuickFIX SessionID instance.

        Events:
            Emits "logon_error" if Logout arrives before logon.

        Thread Safety:
            Called from QuickFIX engine thread.
        """
        try:
            self.admin_handler.from_admin(message, sessionID)
        except Exception as e:
            self.logger.error(
                "event=fromAdmin_handler_failed",
                extra={"error": str(e)}
            )

        # Emit logon_error if a Logout arrives before we are logged on
        try:
            msg_type = fix.MsgType()
            message.getHeader().getField(msg_type)
            if (msg_type.getValue() == fix.MsgType_Logout
                    and not self.is_logged_on()):
                text = (
                    message.getField(58)
                    if message.isSetField(58)
                    else "Logout during logon"
                )
                self._last_logon_error = text
                self._logon_event.clear()
                self._safe_emit(
                    "logon_error",
                    session_id=sessionID,
                    reason=text
                )
        except Exception as e:
            self.logger.error(
                "event=fromAdmin_logon_error_detection_failed",
                extra={"error": str(e)}
            )

    def toApp(self, message, sessionID) -> None:  # noqa: N802
        """
        QuickFIX callback: Outgoing application message.

        Called before sending application messages (orders, etc.).
        Delegates to AppHandler for username/account injection.

        Args:
            message: QuickFIX Message to send.
            sessionID: QuickFIX SessionID instance.

        Thread Safety:
            Called from QuickFIX engine thread.
        """
        try:
            self.app_handler.to_app(message, sessionID)
        except Exception as e:
            self.logger.error(
                "event=toApp_handler_failed",
                extra={"error": str(e)}
            )

    def fromApp(self, message, sessionID) -> None:  # noqa: N802
        """
        QuickFIX callback: Incoming application message.

        Called when application messages arrive. Routes execution
        reports to OrderManager for order state tracking.

        Args:
            message: QuickFIX Message received.
            sessionID: QuickFIX SessionID instance.

        Thread Safety:
            Called from QuickFIX engine thread.

        Note:
            Execution reports (35=8) are forwarded to OrderManager.
        """
        # Route to AppHandler first
        try:
            self.app_handler.from_app(message, sessionID)
        except Exception as e:
            self.logger.error(
                "event=fromApp_handler_failed",
                extra={"error": str(e)}
            )

        # Forward ExecutionReport (35=8) to OrderManager
        try:
            msg_type = fix.MsgType()
            message.getHeader().getField(msg_type)
            if msg_type.getValue() == fix.MsgType_ExecutionReport:
                self.order_manager.on_execution_report(message)
        except Exception as e:
            self.logger.error(
                "event=order_manager_execution_report_failed",
                extra={"error": str(e)}
            )

    # -------- Order façade --------
    def place_order(
        self,
        full_symbol: str,
        side: str,
        qty: int,
        price: float,
        ord_type: str = "LIMIT",
        tif: str = "GTC",
    ) -> str:
        """
        Place a new order.

        Delegates to OrderManager for FIX order submission.

        Args:
            full_symbol: Full symbol identifier (e.g., "AAPL.US").
            side: Order side ("BUY" or "SELL").
            qty: Order quantity (shares/contracts).
            price: Limit price for order.
            ord_type: Order type. Defaults to "LIMIT".
            tif: Time in force. Defaults to "GTC" (Good Till Cancel).

        Returns:
            Client order ID (ClOrdID) for tracking.

        Example:
            >>> cl_ord_id = app.place_order("AAPL.US", "BUY", 100, 150.0)
            >>> print(f"Order placed: {cl_ord_id}")

        Thread Safety:
            Safe to call from any thread.

        Note:
            Requires active logon. Check is_logged_on() first.
        """
        return self.order_manager.place_order(
            full_symbol=full_symbol,
            side=side,
            qty=qty,
            price=price,
            ord_type=ord_type,
            tif=tif,
        )

    def cancel_order(
        self, cl_ord_id: str, timeout: float = 2.0
    ) -> None:
        """
        Cancel an existing order.

        Delegates to OrderManager for FIX cancel request submission.

        Args:
            cl_ord_id: Client order ID from place_order().
            timeout: Seconds to wait for OrderID resolution before
                    sending cancel. Defaults to 2.0.

        Example:
            >>> app.cancel_order(cl_ord_id, timeout=5.0)
            >>> # Cancel request sent (check status for confirmation)

        Thread Safety:
            Safe to call from any thread.

        Note:
            Waits for server OrderID assignment before cancelling.
            May log warning if OrderID not received within timeout.
        """
        return self.order_manager.cancel_order(
            cl_ord_id=cl_ord_id, timeout=timeout
        )

    def get_order_status(self, cl_ord_id: str) -> str:
        """
        Get current order status.

        Queries OrderManager for cached order state.

        Args:
            cl_ord_id: Client order ID to query.

        Returns:
            Dict with order details (status, fills, etc.),
            or None if order not found.

        Example:
            >>> status = app.get_order_status(cl_ord_id)
            >>> if status:
            ...     print(f"Status: {status['ord_status']}")

        Thread Safety:
            Safe to call from any thread.
        """
        return self.order_manager.get_order_status(cl_ord_id)

    # -------- Internal --------
    def _safe_emit(self, event: str, **payload) -> None:
        """
        Emit event with exception isolation.

        Internal method that calls external event emitter with
        error catching to prevent callback exceptions from
        crashing QuickFIX engine.

        Args:
            event: Event name (e.g., "logon", "logout").
            **payload: Event payload.

        Thread Safety:
            Safe to call from any thread.

        Note:
            Exceptions in event callbacks are logged but not propagated.
        """
        try:
            self._emit(event, **payload)
        except Exception as e:
            # Do not let user callbacks crash FIX I/O
            self.logger.error(
                "event=emit_failed",
                extra={"event": event, "error": str(e)}
            )
