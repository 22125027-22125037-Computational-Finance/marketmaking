"""
Order lifecycle management with event-driven state tracking.

This module handles FIX order submission, cancellation, and state management
with thread-safe operations and non-blocking waits using futures.
"""
from datetime import datetime, timezone
import uuid
import threading
import quickfix as fix
import quickfix44 as fix44
from typing import Callable, Optional, Iterable, Dict
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
import logging


class OrderManager:
    """
    Thread-safe order manager with event-driven state tracking.

    Manages the complete lifecycle of orders from submission through fills
    or cancels. Uses futures for non-blocking waits and emits events for
    all state changes.

    Features:
        - Thread-safe order state management
        - Non-blocking waits using futures (no sleep polling)
        - Event emission for all lifecycle changes
        - Automatic OrderID tracking for cancel operations
        - Structured logging with contextual information

    Thread Safety:
        All public methods are thread-safe and can be called from multiple
        threads concurrently. Internal state is protected by locks.

    Example:
        >>> manager = OrderManager(logger, emit_fn)
        >>> manager.set_session(session_id)
        >>> cl_ord_id = manager.place_order('HSX:VNM', 'BUY', 100, 75000)
        >>> # Wait for acceptance
        >>> manager.wait_for(cl_ord_id, ['New', 'Rejected'], timeout=5.0)
        >>> # Cancel if needed
        >>> manager.cancel_order(cl_ord_id, timeout=3.0)
    """

    def __init__(
        self,
        logger: logging.Logger,
        emit: Optional[Callable[..., None]] = None
    ):
        """
        Initialize the order manager.

        Args:
            logger: Logger instance for structured logging.
            emit: Optional event emitter function. Called as
                 emit(event_name, **payload). If None, events are
                 not emitted.

        Note:
            The emit function should be thread-safe as it may be
            called from FIX I/O threads.
        """
        self.logger = logger
        self.session_id: Optional[fix.SessionID] = None

        # State maps (protected by _state_lock)
        self._state_lock = threading.RLock()
        self.clord_map: Dict[str, str] = {}  # OrderID -> ClOrdID
        self.order_info: Dict[str, dict] = {}  # ClOrdID -> metadata
        self.status_map: Dict[str, dict] = {}  # ClOrdID -> status info
        self.order_id_map: Dict[str, str] = {}  # ClOrdID -> OrderID

        # Futures for async waits (protected by _futures_lock)
        self._futures_lock = threading.Lock()
        # ClOrdID -> Future[str] for OrderID
        self._order_id_futures: Dict[str, Future] = {}
        # ClOrdID -> Event for status changes
        self._status_events: Dict[str, threading.Event] = {}

        # Event emitter (can be None)
        self._emit: Callable[..., None] = emit or (lambda *a, **k: None)

    def set_session(self, session_id: fix.SessionID) -> None:
        """
        Set the FIX session ID for sending orders.

        Args:
            session_id: QuickFIX SessionID object.

        Note:
            Must be called before placing any orders. Typically called
            during FIX session establishment (onCreate callback).
        """
        self.session_id = session_id
        self.logger.info(
            f"Order manager session set: {session_id}"
        )

    def generate_ord_id(self) -> str:
        """
        Generate a unique client order ID.

        Returns:
            8-character UUID-based order ID.

        Note:
            Uses UUID4 for uniqueness. Truncated to 8 chars for brevity
            but collision probability is acceptably low for most use cases.
        """
        return str(uuid.uuid4())[:8]

    def extract_exchange_and_symbol(self, full_symbol: str) -> tuple:
        """
        Parse exchange and symbol from full symbol string.

        Args:
            full_symbol: Symbol in format 'EXCHANGE:SYMBOL'
                        (e.g., 'HSX:VNM', 'HNXDS:VN30F2509')

        Returns:
            Tuple of (exchange, symbol)

        Raises:
            ValueError: If full_symbol doesn't contain ':'

        Example:
            >>> extract_exchange_and_symbol('HSX:VNM')
            ('HSX', 'VNM')
        """
        if ":" not in full_symbol:
            raise ValueError(
                f"Invalid symbol format: {full_symbol}, "
                f"expected EXCHANGE:SYMBOL"
            )
        exchange, symbol = full_symbol.split(":", 1)
        return exchange, symbol

    # ---------- Public waits ----------
    def wait_for(
        self,
        cl_ord_id: str,
        statuses: Iterable[str],
        timeout: Optional[float] = None
    ) -> bool:
        """
        Block until order reaches one of the target statuses.

        Uses event-based waiting (no polling/sleeping). Returns immediately
        if already in target status.

        Args:
            cl_ord_id: Client order ID to wait for.
            statuses: Collection of acceptable status strings
                     (e.g., ['New', 'Rejected', 'Filled'])
            timeout: Maximum seconds to wait. None = wait forever.

        Returns:
            True if target status reached within timeout, False otherwise.

        Example:
            >>> # Wait up to 5s for order acceptance or rejection
            >>> if manager.wait_for(cl_ord_id, ['New', 'Rejected'], 5.0):
            ...     print(f"Order status: {manager.get_order_status(cl_ord_id)}")
            ... else:
            ...     print("Timeout waiting for order response")

        Thread Safety:
            Safe to call from any thread.
        """
        with self._futures_lock:
            ev = self._status_events.setdefault(
                cl_ord_id, threading.Event()
            )

        # Fast path: already at target status?
        with self._state_lock:
            cur = self.status_map.get(cl_ord_id, {}).get("status")
            if cur in set(statuses):
                return True

        # Wait for state change
        return ev.wait(timeout=timeout)

    def wait_for_order_id(
        self,
        cl_ord_id: str,
        timeout: Optional[float] = None
    ) -> Optional[str]:
        """
        Wait for OrderID assignment from exchange.

        Non-blocking wait using futures. Required before canceling orders.

        Args:
            cl_ord_id: Client order ID.
            timeout: Maximum seconds to wait. None = wait forever.

        Returns:
            OrderID string if received, None if timeout.

        Raises:
            TimeoutError: If timeout expires before OrderID arrives.

        Example:
            >>> cl_ord_id = manager.place_order(...)
            >>> order_id = manager.wait_for_order_id(cl_ord_id, timeout=3.0)
            >>> if order_id:
            ...     print(f"Exchange assigned OrderID: {order_id}")

        Thread Safety:
            Safe to call from any thread.
        """
        with self._futures_lock:
            # Check if already have OrderID
            with self._state_lock:
                if cl_ord_id in self.order_id_map:
                    return self.order_id_map[cl_ord_id]

            # Create or get future for this order
            if cl_ord_id not in self._order_id_futures:
                self._order_id_futures[cl_ord_id] = Future()

            future = self._order_id_futures[cl_ord_id]

        try:
            return future.result(timeout=timeout)
        except FutureTimeoutError:
            self.logger.warning(
                f"Timeout waiting for OrderID for {cl_ord_id}"
            )
            return None

    # ---------- Place / Cancel ----------
    def place_order(
        self,
        full_symbol: str,
        side: str,
        qty: int,
        price: float,
        ord_type: str = "LIMIT",
        tif: str = "GTC"
    ) -> str:
        """
        Submit a new order to the exchange.

        Args:
            full_symbol: Symbol in 'EXCHANGE:SYMBOL' format.
            side: 'BUY' or 'SELL' (case-insensitive).
            qty: Order quantity.
            price: Order price.
            ord_type: Order type - 'LIMIT' or 'MARKET' (default: 'LIMIT').
            tif: Time in force - 'GTC' or 'IOC' (default: 'GTC').

        Returns:
            Client order ID (cl_ord_id) for tracking.

        Raises:
            RuntimeError: If FIX session not established.
            ValueError: If full_symbol format invalid.

        Events Emitted:
            order:submit - When order is sent (or send fails)
                Payload: cl_ord_id, info, sent (bool), at (datetime)

        Example:
            >>> cl_ord_id = manager.place_order(
            ...     'HSX:VNM',
            ...     side='BUY',
            ...     qty=100,
            ...     price=75000
            ... )
            >>> print(f"Submitted order: {cl_ord_id}")

        Thread Safety:
            Safe to call from any thread.
        """
        if not self.session_id:
            raise RuntimeError("FIX session is not established.")

        exchange, symbol = self.extract_exchange_and_symbol(full_symbol)
        cl_ord_id = self.generate_ord_id()
        side = side.upper()

        # Build FIX message
        order = fix44.NewOrderSingle()
        order.setField(fix.ClOrdID(cl_ord_id))
        order.setField(fix.Symbol(symbol))
        order.setField(fix.SecurityExchange(exchange))
        order.setField(
            fix.Side(fix.Side_BUY if side == "BUY" else fix.Side_SELL)
        )
        order.setField(fix.OrderQty(qty))
        order.setField(fix.Price(price))
        order.setField(
            fix.OrdType(
                fix.OrdType_LIMIT if ord_type == "LIMIT"
                else fix.OrdType_MARKET
            )
        )
        order.setField(
            fix.TimeInForce(
                fix.TimeInForce_GOOD_TILL_CANCEL if tif == "GTC"
                else fix.TimeInForce_IMMEDIATE_OR_CANCEL
            )
        )
        order.setField(fix.TransactTime())

        # Send order
        sent = fix.Session.sendToTarget(order, self.session_id)
        now = datetime.now(timezone.utc)

        if sent:
            self.logger.info(f"[ORDER] Sent new order: {cl_ord_id}")
        else:
            self.logger.error(
                f"[ORDER] Failed to send order {cl_ord_id}"
            )

        # Update state
        with self._state_lock:
            self.order_info[cl_ord_id] = {
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "exchange": exchange,
                "price": price,
            }
            self.status_map[cl_ord_id] = {
                "status": "PendingNew",
                "time": now
            }

        # Initialize synchronization primitives
        with self._futures_lock:
            self._status_events.setdefault(
                cl_ord_id, threading.Event()
            ).clear()

        # Emit event (optimistic - reflects submission attempt)
        self._emit(
            "order:submit",
            cl_ord_id=cl_ord_id,
            info=self.order_info[cl_ord_id],
            sent=bool(sent),
            at=now,
        )

        return cl_ord_id

    def cancel_order(
        self,
        cl_ord_id: str,
        timeout: float = 2.0
    ) -> None:
        """
        Cancel an existing order.

        Waits for OrderID using future (non-blocking), then sends cancel.

        Args:
            cl_ord_id: Client order ID to cancel.
            timeout: Max seconds to wait for OrderID before giving up.

        Raises:
            RuntimeError: If FIX session not established.
            TimeoutError: If OrderID not received within timeout.
            KeyError: If order not found in local state.

        Events Emitted:
            order:cancel_submit - When cancel request is sent
                Payload: cl_ord_id, cancel_cl_ord_id, at, sent (bool)

        Example:
            >>> try:
            ...     manager.cancel_order(cl_ord_id, timeout=3.0)
            ...     print(f"Cancel request sent for {cl_ord_id}")
            ... except TimeoutError:
            ...     print(f"OrderID not yet available for {cl_ord_id}")

        Thread Safety:
            Safe to call from any thread.
        """
        # Wait for OrderID without busy-waiting
        order_id = self.wait_for_order_id(cl_ord_id, timeout=timeout)
        if order_id is None:
            raise TimeoutError(
                f"Timeout waiting for OrderID for {cl_ord_id}"
            )

        if not self.session_id:
            raise RuntimeError("FIX session is not established.")

        cancel_cl_ord_id = f"{cl_ord_id}-CXL"

        with self._state_lock:
            info = self.order_info.get(cl_ord_id)
            if not info:
                raise KeyError(f"Order {cl_ord_id} not found")

        # Build cancel message
        cancel = fix44.OrderCancelRequest()
        cancel.setField(fix.OrigClOrdID(cl_ord_id))
        cancel.setField(fix.ClOrdID(cancel_cl_ord_id))
        cancel.setField(fix.OrderID(order_id))
        cancel.setField(fix.Symbol(info["symbol"]))
        cancel.setField(fix.SecurityExchange(info.get("exchange", "")))
        cancel.setField(
            fix.Side(
                fix.Side_BUY if info["side"] == "BUY"
                else fix.Side_SELL
            )
        )
        cancel.setField(fix.OrderQty(info["qty"]))
        cancel.setField(fix.TransactTime())

        # Send cancel
        sent = fix.Session.sendToTarget(cancel, self.session_id)
        now = datetime.now(timezone.utc)

        if sent:
            self.logger.debug(
                f"[ORDER] Sent cancel request: {cancel_cl_ord_id}"
            )
        else:
            self.logger.error(
                f"[ORDER] Failed to send cancel for {cl_ord_id}"
            )

        self._update_status(cl_ord_id, "PendingCancel", now)
        self._emit(
            "order:cancel_submit",
            cl_ord_id=cl_ord_id,
            cancel_cl_ord_id=cancel_cl_ord_id,
            at=now,
            sent=bool(sent),
        )

    # ---------- ExecReport / OCR handling ----------
    def on_execution_report(self, message) -> None:
        """
        Process incoming FIX execution reports.

        Called by FIXApp when execution reports arrive. Updates order state,
        resolves futures, and emits appropriate events.

        Args:
            message: QuickFIX message object (ExecutionReport or
                    OrderCancelReject).

        Events Emitted:
            - order:cancel_reject: Cancel was rejected by exchange
            - order:rejected: Order was rejected
            - order:partial_fill: Partial fill occurred
            - order:filled: Order fully filled
            - order:trade: Generic trade event
            - order:cancel_pending: Cancel request acknowledged
            - order:canceled: Order successfully canceled
            - order:accepted: Order accepted by exchange
            - order:pending_new: Order pending acceptance
            - order:update: Generic status update

        Thread Safety:
            Safe to call from FIX I/O threads.

        Note:
            This method catches all exceptions to prevent FIX I/O
            thread crashes. Errors are logged but don't propagate.
        """
        try:
            msg_type = message.getHeader().getField(fix.MsgType().getTag())

            # Handle OrderCancelReject separately
            # (not an ExecutionReport but processed here)
            if msg_type == fix.MsgType_OrderCancelReject:
                clx = message.getField(fix.ClOrdID().getTag())
                ord_id = (
                    message.getField(fix.OrderID().getTag())
                    if message.isSetField(fix.OrderID().getTag())
                    else "?"
                )
                text = (
                    message.getField(fix.Text().getTag())
                    if message.isSetField(fix.Text().getTag())
                    else "Unknown reason"
                )
                rej_reason = (
                    message.getField(fix.CxlRejReason().getTag())
                    if message.isSetField(fix.CxlRejReason().getTag())
                    else "?"
                )
                self.logger.warning(
                    f"[CANCEL_REJECTED] Cancel for {clx} "
                    f"(orderID={ord_id}) rejected ({rej_reason}): {text}"
                )
                self._emit(
                    "order:cancel_reject",
                    cl_ord_id=clx,
                    order_id=ord_id,
                    reason=text,
                    code=rej_reason,
                )
                return

            if msg_type != fix.MsgType_ExecutionReport:
                return

            # Extract core fields
            cl_ord_id = message.getField(fix.ClOrdID().getTag())
            ord_status = message.getField(fix.OrdStatus().getTag())
            exec_type = message.getField(fix.ExecType().getTag())
            ord_id = message.getField(fix.OrderID().getTag())
            transact_time_str = message.getField(fix.TransactTime().getTag())
            transact_time = datetime.strptime(
                transact_time_str, "%Y%m%d-%H:%M:%S.%f"
            ).replace(tzinfo=timezone.utc)

            # Update OrderID mapping and resolve future
            with self._state_lock:
                self.order_id_map[cl_ord_id] = ord_id
                if ord_id:
                    self.clord_map[ord_id] = cl_ord_id

            # Resolve OrderID future for cancel operations
            with self._futures_lock:
                if cl_ord_id in self._order_id_futures:
                    future = self._order_id_futures[cl_ord_id]
                    if not future.done():
                        future.set_result(ord_id)

            status = self.map_status(ord_status)

            payload = {
                "cl_ord_id": cl_ord_id,
                "order_id": ord_id,
                "status": status,
                "exec_type": exec_type,
                "at": transact_time,
            }

            # Handle different execution types
            if exec_type == fix.ExecType_REJECTED:
                text = (
                    message.getField(fix.Text().getTag())
                    if message.isSetField(fix.Text().getTag())
                    else "No reason"
                )
                self.logger.warning(
                    f"[REJECTED] Order {cl_ord_id} was rejected: {text}"
                )
                payload["reason"] = text
                self._update_status(cl_ord_id, "Rejected", transact_time)
                self._emit("order:rejected", **payload)

            elif exec_type == fix.ExecType_TRADE:
                last_px = float(message.getField(fix.LastPx().getTag()))
                last_qty = float(message.getField(fix.LastQty().getTag()))
                cum_qty = float(message.getField(fix.CumQty().getTag())) if message.isSetField(fix.CumQty().getTag()) else 0.0
                leaves_qty = float(message.getField(fix.LeavesQty().getTag())) if message.isSetField(fix.LeavesQty().getTag()) else 0.0
                self.logger.info(
                    f"[TRADE] {cl_ord_id}: Traded {last_qty} @ {last_px} "
                    f"at {transact_time.isoformat()} "
                    f"(OrdStatus={ord_status})"
                )
                payload.update({
                    "last_px": last_px,
                    "last_qty": last_qty,
                    "cum_qty": cum_qty,
                    "leaves_qty": leaves_qty
                })
                self._update_status(cl_ord_id, status, transact_time)

                if ord_status == "1":  # Partially filled
                    self._emit("order:partial_fill", **payload)
                elif ord_status == "2":  # Fully filled
                    self._emit("order:filled", **payload)
                else:
                    self._emit("order:trade", **payload)

            elif exec_type == fix.ExecType_PENDING_CANCEL:
                orig = message.getField(fix.OrigClOrdID().getTag())
                self.logger.debug(
                    f"[PENDING_CANCEL] {orig} pending cancel "
                    f"at {transact_time.isoformat()}"
                )
                self._update_status(orig, "PendingCancel", transact_time)
                payload["orig_cl_ord_id"] = orig
                self._emit("order:cancel_pending", **payload)

            elif exec_type == fix.ExecType_CANCELED:
                orig = message.getField(fix.OrigClOrdID().getTag())
                self.logger.info(
                    f"[CANCELED] {orig} was canceled "
                    f"at {transact_time.isoformat()}"
                )
                self._update_status(orig, "Canceled", transact_time)
                payload["orig_cl_ord_id"] = orig
                self._emit("order:canceled", **payload)

            elif exec_type == fix.ExecType_NEW:
                self.logger.info(
                    f"[NEW] Order {cl_ord_id} accepted "
                    f"at {transact_time.isoformat()}"
                )
                self._update_status(cl_ord_id, "New", transact_time)
                self._emit("order:accepted", **payload)

            elif exec_type == fix.ExecType_PENDING_NEW:  # 'A'
                self.logger.info(
                    f"[PENDING_NEW] {cl_ord_id} "
                    f"at {transact_time.isoformat()}"
                )
                self._update_status(cl_ord_id, "PendingNew", transact_time)
                self._emit("order:pending_new", **payload)

            else:
                # Generic update for unhandled exec types
                self._update_status(cl_ord_id, status, transact_time)
                self._emit("order:update", **payload)

        except Exception as e:
            self.logger.error(
                f"Failed to process execution report: {e}",
                exc_info=True
            )

    # ---------- Queries ----------
    def get_order_status(self, cl_ord_id: str) -> str:
        """
        Get current status of an order.

        Args:
            cl_ord_id: Client order ID.

        Returns:
            Status string (e.g., 'New', 'Filled', 'Rejected')
            or 'Unknown' if order not found.

        Thread Safety:
            Safe to call from any thread.

        Example:
            >>> status = manager.get_order_status(cl_ord_id)
            >>> if status == 'Filled':
            ...     print("Order executed successfully")
        """
        with self._state_lock:
            entry = self.status_map.get(cl_ord_id)
            return entry["status"] if entry else "Unknown"

    def map_status(self, fix_status: str) -> str:
        """
        Convert FIX status code to human-readable string.

        Args:
            fix_status: Single-character FIX OrdStatus value.

        Returns:
            Human-readable status string.

        Example:
            >>> manager.map_status('0')
            'NEW'
            >>> manager.map_status('2')
            'FILLED'
        """
        return {
            "0": "NEW",
            "1": "PARTIALLY_FILLED",
            "2": "FILLED",
            "4": "CANCELED",
            "6": "PENDING_CANCEL",
            "8": "REJECTED",
            "A": "PENDING_NEW",
            "B": "CALCULATED",
            "C": "EXPIRED",
            "3": "DONE_FOR_DAY",
            "5": "REPLACED",
            "9": "SUSPENDED",
        }.get(fix_status, "UNKNOWN")

    # ---------- Internals ----------
    def _update_status(
        self,
        cl_ord_id: str,
        status: str,
        at: datetime
    ) -> None:
        """
        Update order status and wake waiting threads.

        Internal method called when status changes. Updates status map,
        signals waiting threads, and emits generic update event.

        Args:
            cl_ord_id: Client order ID.
            status: New status string.
            at: Timestamp of status change.

        Thread Safety:
            Thread-safe via internal locking.
        """
        with self._state_lock:
            prev = self.status_map.get(cl_ord_id)
            # Only update if newer timestamp (prevent out-of-order updates)
            if prev is None or at > prev["time"]:
                self.status_map[cl_ord_id] = {"status": status, "time": at}

        # Wake threads waiting for status changes
        with self._futures_lock:
            ev = self._status_events.setdefault(
                cl_ord_id, threading.Event()
            )
            ev.set()

        # Emit generic update event
        self._emit(
            "order:update",
            cl_ord_id=cl_ord_id,
            status=status,
            at=at
        )
