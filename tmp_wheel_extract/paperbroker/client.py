"""
Unified PaperBroker client for FIX and REST trading operations.

Provides high-level interface combining FIX order execution,
REST account queries, event-driven architecture, and multi-account
context management.
"""
from typing import Callable, Optional
from paperbroker.events import EventBus
from paperbroker.rest.rest_session import RestSession
from paperbroker.rest.account_client import AccountClient
from paperbroker.session.session_manager import FIXSessionManager
from paperbroker.sub_account import SubAccountProvider
from paperbroker.config import render_quickfix_cfg


class PaperBrokerClient:
    """
    Unified trading client with FIX and REST integration.

    Main entry point for paperbroker-client package. Coordinates
    FIX session management, REST API access, event bus, and
    sub-account context switching.

    Features:
        - FIX 4.4 protocol for order execution
        - REST API for account queries and analytics
        - Event-driven architecture with global EventBus
        - Multi-account support via SubAccountProvider
        - Context manager support for clean resource management
        - Comprehensive logging and error handling

    Example:
        >>> # Basic usage
        >>> client = PaperBrokerClient(
        ...     default_sub_account="main",
        ...     username="trader",
        ...     password="secret",
        ...     cfg_path="config.cfg",
        ...     rest_base_url="https://api.example.com"
        ... )
        >>>
        >>> # Connect and wait for logon
        >>> client.connect()
        >>> if client.wait_until_logged_on(timeout=10):
        ...     print("Connected!")
        ...
        ...     # Place order
        ...     cl_ord_id = client.place_order(
        ...         "AAPL.US", "BUY", 100, 150.0
        ...     )
        ...
        ...     # Query balance
        ...     balance = client.get_cash_balance()
        ...     print(f"Cash: {balance['remainCash']}")
        ...
        >>> client.disconnect()

        >>> # Context manager (auto-connect/disconnect)
        >>> with PaperBrokerClient(...) as client:
        ...     client.wait_until_logged_on()
        ...     client.place_order(...)

        >>> # Event handling
        >>> def on_logon(**kw):
        ...     print("FIX session established!")
        >>>
        >>> client.on("fix:logon", on_logon)

    Thread Safety:
        All public methods are thread-safe. Event handlers are
        called synchronously from event emitter threads.

    Note:
        - Call connect() before trading operations
        - Use wait_until_logged_on() to ensure FIX session ready
        - Always disconnect() when done to close connections cleanly
    """

    def __init__(
        self,
        default_sub_account: str,
        username: str,
        password: str,
        rest_base_url: str = "http://localhost:8000",
        # FIX connection parameters
        socket_connect_host: str = "localhost",
        socket_connect_port: int = 5001,
        sender_comp_id: str = "CLIENT",
        target_comp_id: str = "SERVER",
        # Config file handling
        cfg_path: Optional[str] = None,
        auto_generate_cfg: bool = True,
        save_cfg_to: Optional[str] = None,
        # Logging
        log_dir: str = "logs",
        console: bool = False,
    ):
        """
        Initialize PaperBroker client.

        Args:
            default_sub_account: Default sub-account ID to use.
            username: Trading account username for authentication.
            password: Trading account password for authentication.
            rest_base_url: Base URL for REST API.
                          Defaults to "http://localhost:8000".
            socket_connect_host: FIX server hostname or IP.
                                Defaults to "localhost".
            socket_connect_port: FIX server port. Defaults to 5001.
            sender_comp_id: FIX SenderCompID. Defaults to "CLIENT".
            target_comp_id: FIX TargetCompID. Defaults to "SERVER".
            cfg_path: Path to QuickFIX config file. If None and
                     auto_generate_cfg=True, config will be generated
                     automatically. Defaults to None.
            auto_generate_cfg: Auto-generate QuickFIX config if
                              cfg_path is None. Defaults to True.
            save_cfg_to: If provided, save generated config to this path
                        instead of temp file. Only used when auto_generate_cfg=True.
                        Useful for inspection or reuse. Defaults to None.
            log_dir: Directory for log files. Defaults to "logs".
            console: Whether to also log to console. Defaults to False.

        Note:
            Client is not connected automatically. Call connect() to
            initiate FIX session and REST authentication.
        """
        # Auto-generate config if needed
        if cfg_path is None and auto_generate_cfg:
            cfg_path = render_quickfix_cfg(
                socket_host=socket_connect_host,
                socket_port=socket_connect_port,
                sender_comp_id=sender_comp_id,
                target_comp_id=target_comp_id,
                file_store_path=f"{log_dir}/client_fix_messages/",
                file_log_path=f"{log_dir}/",
                save_as=save_cfg_to,  # Use provided path or temp file
            )
        elif cfg_path is None:
            raise ValueError(
                "Either provide cfg_path or set auto_generate_cfg=True"
            )
        
        # Global event bus for FIX (and future REST/price events)
        self.bus = EventBus()
        
        # Sub-account provider with event integration
        self.sub_provider = SubAccountProvider(default_sub_account)
        # Emit account switch events to global bus
        self.sub_provider.on_switch(
            lambda **kw: self.bus.emit("account:switch", **kw)
        )

        # FIX session manager (emits 'fix:*' events onto the bus)
        self.session = FIXSessionManager(
            cfg_path=cfg_path,
            sub_provider=self.sub_provider,
            username=username,
            password=password,
            bus=self.bus,  # <-- inject bus
            log_dir=log_dir,
            console=console,
        )

        # REST clients
        self.rest_session = RestSession(rest_base_url)
        self.account_client = AccountClient(
            rest_session=self.rest_session,
            sub_provider=self.sub_provider,
            username=username,
            password=password,
            log_dir=log_dir,
            console=console,
        )

    # -------- Lifecycle --------
    def connect(self) -> None:
        """
        Connect to trading services.

        Starts FIX session and resolves account ID via REST API.
        FIX logon and REST auth happen concurrently.

        Raises:
            quickfix.RuntimeError: If FIX engine fails to start.
            requests.RequestException: If REST auth fails.

        Example:
            >>> client.connect()
            >>> client.wait_until_logged_on(timeout=10)

        Thread Safety:
            Safe to call from any thread. Idempotent.

        Note:
            Does not wait for FIX logon. Use wait_until_logged_on()
            to block until session is ready.
        """
        self.session.start()
        # Resolve accountID immediately (non-blocking w.r.t. FIX logon)
        self.account_client.resolve_on_connect()

    def disconnect(self) -> None:
        """
        Disconnect from trading services.

        Stops FIX session and closes all connections gracefully.

        Thread Safety:
            Safe to call from any thread. Blocks until disconnect
            completes.

        Example:
            >>> client.disconnect()
            >>> # All connections closed
        """
        self.session.stop()

    # Optional: context manager for safety
    def __enter__(self):
        """Context manager entry: connect automatically."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb):
        """Context manager exit: disconnect automatically."""
        self.disconnect()

    def _ensure_account_id(self) -> bool:
        """
        Internal method to ensure account ID is resolved.

        Returns:
            True if account ID available, False otherwise.
        """
        return self.account_client._ensure_account_id()

    # -------- Unified Event API --------
    def on(self, event: str, fn: Callable[..., None]) -> None:
        """
        Register event handler on global bus.

        Subscribes to system-wide events for monitoring and
        coordination.

        Args:
            event: Event name to subscribe to. Common events:
                  - 'fix:logon': FIX session established
                  - 'fix:logout': FIX session disconnected
                  - 'fix:logon_error': FIX authentication failed
                  - 'account:switch': Sub-account changed
            fn: Callback function called with (**kwargs) when
               event occurs.

        Example:
            >>> def on_logon(session_id, **kw):
            ...     print(f"Connected: {session_id}")
            >>>
            >>> client.on("fix:logon", on_logon)

        Thread Safety:
            Safe to call from any thread.

        Note:
            Handlers are called synchronously. Avoid blocking
            operations in handlers.
        """
        self.bus.on(event, fn)

    def off(self, event: str, fn: Callable[..., None]) -> None:
        """
        Unregister event handler from global bus.

        Args:
            event: Event name to unsubscribe from.
            fn: Previously registered callback function.

        Example:
            >>> client.off("fix:logon", on_logon)

        Thread Safety:
            Safe to call from any thread.
        """
        self.bus.off(event, fn)

    # -------- FIX synchronous helpers --------
    def is_logged_on(self) -> bool:
        """
        Check if FIX session is logged on.

        Returns:
            True if logged on, False otherwise.

        Thread Safety:
            Safe to call from any thread.
        """
        return self.session.is_logged_on()

    def wait_until_logged_on(self, timeout: float | None = None) -> bool:
        """
        Block until FIX logon completes or timeout.

        Args:
            timeout: Maximum seconds to wait. None means forever.

        Returns:
            True if logged on within timeout, False if timed out.

        Example:
            >>> client.connect()
            >>> if client.wait_until_logged_on(timeout=10):
            ...     print("Ready to trade!")

        Thread Safety:
            Safe to call from multiple threads.
        """
        return self.session.wait_until_logged_on(timeout)

    def last_logon_error(self) -> str | None:
        """
        Get last FIX logon error message.

        Returns:
            Error message if last logon failed, None otherwise.

        Thread Safety:
            Safe to call from any thread.
        """
        return self.session.last_logon_error()

    def get_session_id(self):
        """
        Get QuickFIX session ID.

        Returns:
            SessionID instance if session created, None otherwise.

        Thread Safety:
            Safe to call from any thread.
        """
        return self.session.app.get_session_id()

    # -------- FIX order façade --------
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
        Place order via FIX.

        Submits new order to exchange via FIX protocol.

        Args:
            full_symbol: Full symbol (e.g., "AAPL.US").
            side: "BUY" or "SELL".
            qty: Order quantity.
            price: Limit price.
            ord_type: Order type. Defaults to "LIMIT".
            tif: Time in force. Defaults to "GTC".

        Returns:
            Client order ID for tracking.

        Example:
            >>> cl_ord_id = client.place_order(
            ...     "AAPL.US", "BUY", 100, 150.0
            ... )

        Thread Safety:
            Safe to call from any thread.
        """
        return self.session.app.place_order(
            full_symbol=full_symbol,
            side=side,
            qty=qty,
            price=price,
            ord_type=ord_type,
            tif=tif,
        )

    def cancel_order(self, cl_ord_id: str, timeout: float = 2.0) -> None:
        """
        Cancel existing order via FIX.

        Args:
            cl_ord_id: Client order ID from place_order().
            timeout: Seconds to wait for OrderID. Defaults to 2.0.

        Thread Safety:
            Safe to call from any thread.
        """
        return self.session.app.cancel_order(
            cl_ord_id=cl_ord_id, timeout=timeout
        )

    def get_order_status(self, cl_ord_id: str) -> str:
        """
        Get order status string.

        Args:
            cl_ord_id: Client order ID to query.

        Returns:
            Status string (e.g., "NEW", "FILLED").

        Thread Safety:
            Safe to call from any thread.
        """
        return self.session.app.get_order_status(cl_ord_id)

    # -------- REST façade --------
    def get_cash_balance(self):
        """
        Query available cash balance via REST.
        
        Returns cash available for trading (not including asset values).
        
        Returns:
            dict: Response containing 'remainCash' field.
        """
        return self.account_client.get_remain_balance()

    def get_account_balance(self):
        """
        Query total account balance via REST.
        
        Returns total balance including cash and asset values.
        
        Returns:
            dict: Response containing 'totalBalance' field.
        """
        return self.account_client.get_total_balance()

    def get_stock_orders(self):
        """Query stock orders via REST."""
        return self.account_client.get_stock_orders()

    def get_derivative_orders(self):
        """Query derivative orders via REST."""
        return self.account_client.get_derivative_orders()

    # ---- Sub-account facade ----
    def set_default_sub_account(self, sub_id: str) -> None:
        """
        Change default sub-account.

        Args:
            sub_id: New default sub-account ID.

        Thread Safety:
            Safe to call from any thread.
        """
        self.sub_provider.set_default(sub_id)

    def current_sub_account(self) -> str:
        """
        Get current active sub-account ID.

        Returns:
            Current sub-account ID.

        Thread Safety:
            Thread-local via ContextVar.
        """
        return self.sub_provider.current()

    def use_sub_account(self, sub_id: str):
        """
        Context manager for temporary sub-account switch.

        Args:
            sub_id: Sub-account ID to use within context.

        Example:
            >>> with client.use_sub_account('trading_1'):
            ...     client.place_order(...)  # Uses trading_1

        Thread Safety:
            Safe to use in multiple threads.
        """
        return self.sub_provider.use(sub_id)

    # =========================================================================
    # REST API Wrapper Methods
    # =========================================================================

    def get_portfolio_by_sub(self, sub_account_id: Optional[str] = None) -> dict:
        """
        Get portfolio (positions + pending orders) for a sub-account.

        Wrapper around AccountClient.get_portfolio_by_sub().

        Args:
            sub_account_id: Sub-account ID. If None, uses current/default.

        Returns:
            Dictionary with:
                - success (bool): Whether request succeeded
                - items (list): Portfolio items with positions, costs, PnL
                - error (str): Error message if success=False

        Example:
            >>> portfolio = client.get_portfolio_by_sub("D1")
            >>> if portfolio['success']:
            ...     for item in portfolio['items']:
            ...         print(f"{item['instrument']}: {item['quantity']}")
        """
        # Use context manager to switch sub-account if specified
        if sub_account_id:
            with self.use_sub_account(sub_account_id):
                return self.account_client.get_portfolio_by_sub()
        else:
            return self.account_client.get_portfolio_by_sub()

    def get_orders(self, start_date: str, end_date: str, sub_account_id: Optional[str] = None) -> dict:
        """
        Get orders within a date range.

        Wrapper around AccountClient.get_orders().

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            sub_account_id: Sub-account ID. If None, uses current/default.

        Returns:
            Dictionary with:
                - success (bool): Whether request succeeded
                - items (list): Order details with status, prices, quantities
                - error (str): Error message if success=False

        Example:
            >>> orders = client.get_orders("2025-11-01", "2025-11-21")
            >>> if orders['success']:
            ...     for order in orders['items']:
            ...         print(f"{order['symbol']}: {order['ordStatus']}")
        """
        # Use context manager to switch sub-account if specified
        if sub_account_id:
            with self.use_sub_account(sub_account_id):
                return self.account_client.get_orders(start_date, end_date)
        else:
            return self.account_client.get_orders(start_date, end_date)

    def get_transactions_by_date(self, start_date: str, end_date: str, sub_account_id: Optional[str] = None) -> dict:
        """
        Get transactions within a date range.

        Wrapper around AccountClient.get_transactions_by_date().

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            sub_account_id: Sub-account ID. If None, uses current/default.

        Returns:
            Dictionary with:
                - success (bool): Whether request succeeded
                - items (list): Transaction details, each with:
                    - transactionId: Transaction ID
                    - orderId: Order ID (for mapping back to orders)
                    - symbol: Symbol (e.g., "HNXDS:VN30F2512")
                    - type: BUY/SELL
                    - price: Execution price
                    - quantity: Filled quantity
                    - totalCost: price * quantity
                    - totalFee: Trading fees
                    - pnl: Profit/loss
                    - envTimestamp: Environment timestamp
                    - sysTimestamp: System timestamp
                - error (str): Error message if success=False

        Example:
            >>> txns = client.get_transactions_by_date("2025-11-01", "2025-11-21")
            >>> if txns['success']:
            ...     for txn in txns['items']:
            ...         print(f"{txn['symbol']}: {txn['quantity']} @ {txn['price']}")
            ...         print(f"  Order ID: {txn['orderId']}")
        """
        # Use context manager to switch sub-account if specified
        if sub_account_id:
            with self.use_sub_account(sub_account_id):
                return self.account_client.get_transactions_by_date(start_date, end_date)
        else:
            return self.account_client.get_transactions_by_date(start_date, end_date)

    def get_max_placeable(
        self,
        symbol: str,
        price: float,
        side: str,
        sub_account_id: Optional[str] = None
    ) -> dict:
        """
        Calculate maximum placeable quantity for an order.

        Wrapper around AccountClient.get_max_placeable().

        Args:
            symbol: Trading symbol (e.g., "MWG", "VN30F1M", "HNXDS:VN30F2512")
            price: Order price
            side: Order side - "BUY" or "SELL"
            sub_account_id: Sub-account ID. If None, uses current/default.

        Returns:
            Dictionary with:
                - success (bool): Whether request succeeded
                - maxQty (Decimal): Maximum placeable quantity (floored)
                - perUnitCost (Decimal): Cost per unit including margin and fees
                - remainCash (Decimal): Current free cash available
                - unlimited (bool): Whether account can place unlimited orders
                - error (str): Error message if success=False

        Example:
            >>> result = client.get_max_placeable("MWG", 73000, "BUY")
            >>> if result['success']:
            ...     print(f"Can buy up to {result['maxQty']} shares")
            ...     print(f"Cost per share: {result['perUnitCost']:,.0f} VND")
            ...     print(f"Available cash: {result['remainCash']:,.0f} VND")
        """
        # Use context manager to switch sub-account if specified
        if sub_account_id:
            with self.use_sub_account(sub_account_id):
                return self.account_client.get_max_placeable(symbol, price, side)
        else:
            return self.account_client.get_max_placeable(symbol, price, side)


