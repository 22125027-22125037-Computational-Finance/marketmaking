"""
REST API client for account operations.

Provides high-level interface for querying account information,
balances, orders, positions, and performance metrics via REST API.
"""
from typing import Dict, List, Optional, Any
from paperbroker.rest.rest_session import RestSession
from paperbroker.logger import get_logger
from paperbroker.sub_account import SubAccountProvider


class AccountClient:
    """
    High-level REST client for trading account operations.

    Manages account authentication, sub-account context, and provides
    convenient methods for querying balances, orders, positions, and
    performance metrics.

    Features:
        - Lazy account ID resolution on first use
        - Sub-account context awareness
        - Comprehensive error handling and logging
        - Structured logging with event correlation
        - Automatic retry on transient failures

    Example:
        >>> from paperbroker.rest import RestSession, AccountClient
        >>> from paperbroker.sub_account import SubAccountProvider
        >>>
        >>> session = RestSession("https://api.example.com")
        >>> sub = SubAccountProvider(default_sub="main")
        >>> client = AccountClient(session, sub, "user", "pass")
        >>>
        >>> # Resolve account ID on connect
        >>> client.resolve_on_connect()
        >>>
        >>> # Query balance for current sub-account
        >>> balance = client.get_remain_balance()
        >>> print(balance)
        {'fixAccountID': 'ABC123', 'subAccountID': 'main',
         'remainCash': 100000.0}

    Thread Safety:
        All methods are thread-safe. Multiple threads can use the
        same instance concurrently.

    Note:
        Account ID is resolved lazily on first API call. Call
        resolve_on_connect() explicitly to fail fast on auth errors.
    """

    def __init__(
        self,
        rest_session: RestSession,
        sub_provider: SubAccountProvider,
        username: str,
        password: str,
        log_dir: str = "logs",
        console: bool = False,
    ):
        """
        Initialize account client.

        Args:
            rest_session: Configured REST session for HTTP requests.
            sub_provider: Sub-account context provider.
            username: Trading account username for authentication.
            password: Trading account password for authentication.
            log_dir: Directory for log files. Defaults to "logs".
            console: Whether to also log to console. Defaults to False.

        Note:
            Account ID is not resolved until first use. Call
            resolve_on_connect() to resolve immediately.
        """
        self.rest = rest_session
        self.sub = sub_provider
        self.username = username
        self.password = password
        self.logger = get_logger(log_dir, console)
        # Lazy-resolved account ID (None until first fetch)
        self.ID: Optional[str] = None

    def _fetch_account_id(self) -> Optional[str]:
        """
        Fetch FIX account ID from server.

        Internal method that authenticates with username/password
        and retrieves the corresponding FIX account identifier.

        Returns:
            FIX account ID if authentication succeeds, None otherwise.

        Thread Safety:
            Safe to call from multiple threads. May result in
            duplicate fetches if called concurrently before ID
            is cached.

        Note:
            Result is cached in self.ID after first successful fetch.
        """
        try:
            resp = self.rest.post(
                "/api/fix-account-info/get-fix-id",
                json={"username": self.username, "password": self.password},
            )
            if isinstance(resp, dict):
                fix_id = resp.get("fixAccountID") or resp.get("accountID")
                if (resp.get("success", fix_id is not None)) and fix_id:
                    self.logger.info(
                        "event=fix_id_resolved",
                        extra={
                            "username": self.username,
                            "fixAccountID": fix_id,
                        },
                    )
                    return fix_id
                if resp.get("message") or resp.get("error"):
                    self.logger.warning(
                        "event=fix_id_auth_failed",
                        extra={
                            "username": self.username,
                            "message": resp.get("message") or resp.get("error"),
                        },
                    )
                    return None
                self.logger.warning(
                    "event=fix_id_fetch_invalid",
                    extra={
                        "username": self.username,
                        "response": str(resp),
                    },
                )
            else:
                self.logger.warning(
                    "event=fix_id_fetch_unexpected_type",
                    extra={
                        "username": self.username,
                        "response_type": type(resp).__name__,
                    },
                )
        except Exception as e:
            self.logger.error(
                "event=fix_id_fetch_failed",
                extra={
                    "username": self.username,
                    "error": str(e),
                    "endpoint": "/api/fix-account-info/get-fix-id",
                },
            )
        return None

    def _ensure_account_id(self) -> bool:
        """
        Ensure account ID is resolved before use.

        Resolves account ID if not already cached. Used internally
        by all methods that require account ID.

        Returns:
            True if account ID is available, False otherwise.

        Thread Safety:
            Safe to call from multiple threads. May result in
            duplicate fetches on concurrent first calls.
        """
        if self.ID is None:
            self.ID = self._fetch_account_id()
        return self.ID is not None

    def resolve_on_connect(self) -> Optional[str]:
        """
        Force resolve account ID on connection.

        Explicitly resolves account ID and logs warning if resolution
        fails. Useful for fail-fast behavior on startup.

        Returns:
            Resolved FIX account ID, or None if resolution fails.

        Example:
            >>> client = AccountClient(session, sub, "user", "pass")
            >>> account_id = client.resolve_on_connect()
            >>> if not account_id:
            ...     print("Failed to authenticate")
            ...     sys.exit(1)

        Thread Safety:
            Safe to call from multiple threads.
        """
        if not self._ensure_account_id():
            self.logger.warning(
                "event=account_id_not_resolved_on_connect",
                extra={"username": self.username},
            )
        return self.ID

    def get_remain_balance(self) -> Dict[str, Any]:
        """
        Fetch remaining cash balance for current sub-account.

        Queries the REST API for available cash (remaining buying power)
        for the current sub-account context.

        Returns:
            Dict with keys: fixAccountID, subAccountID, remainCash.
            Empty dict {} if query fails or prerequisites not met.

        Example:
            >>> with sub_provider.use('trading_account'):
            ...     balance = client.get_remain_balance()
            ...     print(f"Cash: {balance.get('remainCash', 0)}")
            Cash: 50000.0

        Thread Safety:
            Safe to call from multiple threads. Each thread sees
            its own sub-account context.

        Note:
            Requires both account ID and sub-account ID. Returns {}
            if either is missing.
        """
        if not self._ensure_account_id():
            self.logger.warning(
                "event=remain_cash_missing_fix_id", extra={"username": self.username}
            )
            return {}
        sub_id = self.sub.current()
        if not sub_id:
            self.logger.warning(
                "event=remain_cash_missing_sub_id", extra={"fixAccountID": self.ID}
            )
            return {}
        try:
            resp = self.rest.post(
                "/api/account-info-by-sub/remain-cash",
                json={"fixAccountID": self.ID, "subAccountID": sub_id},
            )
            if isinstance(resp, dict):
                amount = (
                    resp.get("remainCash")
                    or resp.get("remain")
                    or resp.get("remainBalance")
                )
                if (resp.get("success", amount is not None)) and amount is not None:
                    self.logger.info(
                        "event=remain_cash_resolved",
                        extra={
                            "fixAccountID": self.ID,
                            "subAccountID": sub_id,
                            "remainCash": str(amount),
                        },
                    )
                    return {
                        "fixAccountID": self.ID,
                        "subAccountID": sub_id,
                        "remainCash": amount,
                    }
                if resp.get("message") or resp.get("error"):
                    self.logger.warning(
                        "event=remain_cash_failed",
                        extra={
                            "fixAccountID": self.ID,
                            "subAccountID": sub_id,
                            "message": resp.get("message") or resp.get("error"),
                        },
                    )
                    return {}
                self.logger.warning(
                    "event=remain_cash_invalid_format",
                    extra={
                        "fixAccountID": self.ID,
                        "subAccountID": sub_id,
                        "response": str(resp),
                    },
                )
            else:
                self.logger.warning(
                    "event=remain_cash_unexpected_type",
                    extra={
                        "fixAccountID": self.ID,
                        "subAccountID": sub_id,
                        "response_type": type(resp).__name__,
                    },
                )
        except Exception as e:
            self.logger.error(
                "event=remain_cash_fetch_failed",
                extra={
                    "fixAccountID": self.ID,
                    "subAccountID": sub_id,
                    "error": str(e),
                },
            )
        return {}

    def get_total_balance(self) -> Dict[str, Any]:
        """
        Fetch total balance for current sub-account.

        Queries the REST API for total account value (cash + positions)
        for the current sub-account context.

        Returns:
            Dict with keys: fixAccountID, subAccountID, totalBalance.
            Empty dict {} if query fails or prerequisites not met.

        Example:
            >>> balance = client.get_total_balance()
            >>> print(f"Total: {balance.get('totalBalance', 0)}")
            Total: 125000.0

        Thread Safety:
            Safe to call from multiple threads.
        """
        if not self._ensure_account_id():
            self.logger.warning(
                "event=total_balance_missing_fix_id", extra={"username": self.username}
            )
            return {}

        sub_id = self.sub.current()
        if not sub_id:
            self.logger.warning(
                "event=total_balance_missing_sub_id", extra={"fixAccountID": self.ID}
            )
            return {}

        try:
            resp = self.rest.post(
                "/api/account-info-by-sub/total-balance",
                json={"fixAccountID": self.ID, "subAccountID": sub_id},
            )

            if isinstance(resp, dict):
                amount = (
                    resp.get("totalBalance") or resp.get("total") or resp.get("balance")
                )
                if (resp.get("success", amount is not None)) and amount is not None:
                    self.logger.info(
                        "event=total_balance_resolved",
                        extra={
                            "fixAccountID": self.ID,
                            "subAccountID": sub_id,
                            "totalBalance": str(amount),
                        },
                    )
                    return {
                        "fixAccountID": self.ID,
                        "subAccountID": sub_id,
                        "totalBalance": amount,
                    }

                if resp.get("message") or resp.get("error"):
                    self.logger.warning(
                        "event=total_balance_failed",
                        extra={
                            "fixAccountID": self.ID,
                            "subAccountID": sub_id,
                            "message": resp.get("message") or resp.get("error"),
                        },
                    )
                    return {}

                self.logger.warning(
                    "event=total_balance_invalid_format",
                    extra={
                        "fixAccountID": self.ID,
                        "subAccountID": sub_id,
                        "response": str(resp),
                    },
                )
            else:
                self.logger.warning(
                    "event=total_balance_unexpected_type",
                    extra={
                        "fixAccountID": self.ID,
                        "subAccountID": sub_id,
                        "response_type": type(resp).__name__,
                    },
                )
        except Exception as e:
            self.logger.error(
                "event=total_balance_fetch_failed",
                extra={
                    "fixAccountID": self.ID,
                    "subAccountID": sub_id,
                    "error": str(e),
                },
            )
        return {}

    def get_stock_orders(self) -> List[Dict[str, Any]]:
        """
        Fetch all stock orders for current account.

        Retrieves list of all stock orders (pending, filled, canceled)
        for the authenticated account.

        Returns:
            List of order dicts. Empty list [] if query fails.

        Example:
            >>> orders = client.get_stock_orders()
            >>> for order in orders:
            ...     print(f"{order['symbol']}: {order['status']}")
            AAPL: FILLED
            MSFT: PENDING

        Thread Safety:
            Safe to call from multiple threads.
        """
        if not self._ensure_account_id():
            self.logger.warning(
                "event=stock_orders_skipped_due_to_missing_id",
                extra={"username": self.username},
            )
            return []

        try:
            response = self.rest.get(
                "/api/account/orders/stock",
                params={"accountID": self.ID},
            )
            if isinstance(response, list):
                self.logger.info(
                    "event=stock_orders_resolved",
                    extra={"accountID": self.ID, "count": len(response)},
                )
                return response
            else:
                self.logger.warning(
                    "event=stock_orders_invalid_format",
                    extra={"accountID": self.ID, "response": str(response)},
                )
                return []
        except Exception as e:
            self.logger.error(
                "event=stock_orders_fetch_failed",
                extra={"accountID": self.ID, "error": str(e)},
            )
            return []

    def get_derivative_orders(self) -> List[Dict[str, Any]]:
        """
        Fetch all derivative orders for current account.

        Retrieves list of all derivative orders (futures, options)
        for the authenticated account.

        Returns:
            List of order dicts. Empty list [] if query fails.

        Thread Safety:
            Safe to call from multiple threads.
        """
        if not self._ensure_account_id():
            self.logger.warning(
                "event=derivative_orders_skipped_due_to_missing_id",
                extra={"username": self.username},
            )
            return []

        try:
            response = self.rest.get(
                "/api/account/orders/derivative",
                params={"accountID": self.ID},
            )
            if isinstance(response, list):
                self.logger.info(
                    "event=derivative_orders_resolved",
                    extra={"accountID": self.ID, "count": len(response)},
                )
                return response
            else:
                self.logger.warning(
                    "event=derivative_orders_invalid_format",
                    extra={"accountID": self.ID, "response": str(response)},
                )
                return []
        except Exception as e:
            self.logger.error(
                "event=derivative_orders_fetch_failed",
                extra={"accountID": self.ID, "error": str(e)},
            )
            return []

    def get_portfolio(self) -> Dict[str, Any]:
        """
        Fetch portfolio holdings for current account.

        Retrieves current positions and holdings across all instruments.

        Returns:
            Dict with 'holdings' key containing list of positions.
            Empty dict {} if query fails.

        Example:
            >>> portfolio = client.get_portfolio()
            >>> for holding in portfolio.get('holdings', []):
            ...     print(f"{holding['symbol']}: {holding['quantity']}")
            AAPL: 100
            MSFT: 50

        Thread Safety:
            Safe to call from multiple threads.
        """
        if not self._ensure_account_id():
            self.logger.warning(
                "event=portfolio_skipped_due_to_missing_id",
                extra={"username": self.username},
            )
            return {}

        try:
            response = self.rest.get(
                "/api/account/portfolio",
                params={"accountID": self.ID},
            )
            if isinstance(response, dict) and "holdings" in response:
                self.logger.info(
                    "event=portfolio_resolved",
                    extra={
                        "accountID": response.get("accountID", self.ID),
                        "holdings_count": len(response.get("holdings", [])),
                    },
                )
                return response
            else:
                self.logger.warning(
                    "event=portfolio_invalid_format",
                    extra={"accountID": self.ID, "response": str(response)},
                )
                return {}
        except Exception as e:
            self.logger.error(
                "event=portfolio_fetch_failed",
                extra={"accountID": self.ID, "error": str(e)},
            )
            return {}

    def get_transactions(self) -> List[Dict[str, Any]]:
        """
        Fetch all transactions for current account.

        Retrieves complete transaction history including deposits,
        withdrawals, trades, fees, and dividends.

        Returns:
            List of transaction dicts. Empty list [] if query fails.

        Thread Safety:
            Safe to call from multiple threads.
        """
        if not self._ensure_account_id():
            self.logger.warning(
                "event=transactions_skipped_due_to_missing_id",
                extra={"username": self.username},
            )
            return []

        try:
            response = self.rest.get(
                "/api/account/transactions",
                params={"accountID": self.ID},
            )
            if isinstance(response, list):
                self.logger.info(
                    "event=transactions_resolved",
                    extra={"accountID": self.ID, "count": len(response)},
                )
                return response
            else:
                self.logger.warning(
                    "event=transactions_invalid_format",
                    extra={"accountID": self.ID, "response": str(response)},
                )
                return []
        except Exception as e:
            self.logger.error(
                "event=transactions_fetch_failed",
                extra={"accountID": self.ID, "error": str(e)},
            )
            return []

    def get_executions_by_order(self, order_id: str) -> List[Dict[str, Any]]:
        """
        Fetch all executions for specific order.

        Retrieves fill/execution details for a single order, including
        partial fills and multiple execution prices.

        Args:
            order_id: Order ID to query executions for.

        Returns:
            List of execution dicts. Empty list [] if query fails.

        Example:
            >>> executions = client.get_executions_by_order('ORD123')
            >>> for exec in executions:
            ...     print(f"Filled {exec['qty']} @ {exec['price']}")
            Filled 50 @ 150.00
            Filled 50 @ 150.25

        Thread Safety:
            Safe to call from multiple threads.
        """
        try:
            response = self.rest.get(
                "/api/account/executions/by-order",
                params={"orderID": order_id},
            )
            if isinstance(response, list):
                self.logger.info(
                    "event=executions_by_order_resolved",
                    extra={"orderID": order_id, "count": len(response)},
                )
                return response
            else:
                self.logger.warning(
                    "event=executions_by_order_invalid_format",
                    extra={"orderID": order_id, "response": str(response)},
                )
                return []
        except Exception as e:
            self.logger.error(
                "event=executions_by_order_fetch_failed",
                extra={"orderID": order_id, "error": str(e)},
            )
            return []

    def get_executions_by_account(self) -> List[Dict[str, Any]]:
        """
        Fetch all executions for current account.

        Retrieves complete execution history across all orders for
        the authenticated account.

        Returns:
            List of execution dicts. Empty list [] if query fails.

        Thread Safety:
            Safe to call from multiple threads.
        """
        if not self._ensure_account_id():
            self.logger.warning(
                "event=executions_by_account_skipped_due_to_missing_id",
                extra={"username": self.username},
            )
            return []

        try:
            response = self.rest.get(
                "/api/account/executions/by-account",
                params={"accountID": self.ID},
            )
            if isinstance(response, list):
                self.logger.info(
                    "event=executions_by_account_resolved",
                    extra={"accountID": self.ID, "count": len(response)},
                )
                return response
            else:
                self.logger.warning(
                    "event=executions_by_account_invalid_format",
                    extra={"accountID": self.ID, "response": str(response)},
                )
                return []
        except Exception as e:
            self.logger.error(
                "event=executions_by_account_fetch_failed",
                extra={"accountID": self.ID, "error": str(e)},
            )
            return []

    def get_nav_history(self) -> List[Dict[str, Any]]:
        """
        Fetch NAV (Net Asset Value) time series.

        Retrieves complete NAV history for performance tracking
        and charting.

        Returns:
            List of NAV data points with timestamps and values.
            Empty list [] if query fails.

        Example:
            >>> nav_data = client.get_nav_history()
            >>> for point in nav_data[-5:]:  # Last 5 points
            ...     print(f"{point['date']}: ${point['nav']}")
            2024-01-15: $100000.00
            2024-01-16: $101500.50

        Thread Safety:
            Safe to call from multiple threads.
        """
        if not self._ensure_account_id():
            self.logger.warning(
                "event=nav_skipped_due_to_missing_id",
                extra={"username": self.username},
            )
            return []

        try:
            response = self.rest.get(
                "/api/account/metric/nav",
                params={"accountID": self.ID},
            )
            if isinstance(response, list):
                self.logger.info(
                    "event=nav_history_resolved",
                    extra={"accountID": self.ID, "points": len(response)},
                )
                return response
            else:
                self.logger.warning(
                    "event=nav_invalid_format",
                    extra={"accountID": self.ID, "response": str(response)},
                )
                return []
        except Exception as e:
            self.logger.error(
                "event=nav_fetch_failed",
                extra={"accountID": self.ID, "error": str(e)},
            )
            return []

    def get_max_drawdown(self) -> Dict[str, Any]:
        """
        Fetch Max Drawdown (MDD) statistics.

        Retrieves maximum peak-to-trough decline percentage for
        risk assessment.

        Returns:
            Dict with 'maxDrawdownPct' key. Empty dict {} if fails.

        Example:
            >>> mdd = client.get_max_drawdown()
            >>> print(f"Max DD: {mdd.get('maxDrawdownPct', 0)}%")
            Max DD: -12.5%

        Thread Safety:
            Safe to call from multiple threads.
        """
        if not self._ensure_account_id():
            self.logger.warning(
                "event=mdd_skipped_due_to_missing_id",
                extra={"username": self.username},
            )
            return {}

        try:
            response = self.rest.get(
                "/api/account/metric/mdd",
                params={"accountID": self.ID},
            )
            if isinstance(response, dict) and "maxDrawdownPct" in response:
                self.logger.info(
                    "event=mdd_resolved",
                    extra={
                        "accountID": response.get("accountID", self.ID),
                        "maxDrawdownPct": response["maxDrawdownPct"],
                    },
                )
                return {
                    "accountID": response.get("accountID", self.ID),
                    "maxDrawdownPct": response["maxDrawdownPct"],
                }
            else:
                self.logger.warning(
                    "event=mdd_invalid_format",
                    extra={"accountID": self.ID, "response": str(response)},
                )
                return {}
        except Exception as e:
            self.logger.error(
                "event=mdd_fetch_failed",
                extra={"accountID": self.ID, "error": str(e)},
            )
            return {}

    def get_drawdown_periods(self) -> List[Dict[str, Any]]:
        """
        Fetch all drawdown periods for current account.

        Retrieves list of all historical drawdown periods with
        start/end dates and magnitudes.

        Returns:
            List of drawdown period dicts. Empty list [] if fails.

        Thread Safety:
            Safe to call from multiple threads.
        """
        if not self._ensure_account_id():
            self.logger.warning(
                "event=drawdown_periods_skipped_due_to_missing_id",
                extra={"username": self.username},
            )
            return []

        try:
            response = self.rest.get(
                "/api/account/metric/drawdowns",
                params={"accountID": self.ID},
            )
            if isinstance(response, list):
                self.logger.info(
                    "event=drawdown_periods_resolved",
                    extra={"accountID": self.ID, "count": len(response)},
                )
                return response
            else:
                self.logger.warning(
                    "event=drawdown_periods_invalid_format",
                    extra={"accountID": self.ID, "response": str(response)},
                )
                return []
        except Exception as e:
            self.logger.error(
                "event=drawdown_periods_fetch_failed",
                extra={"accountID": self.ID, "error": str(e)},
            )
            return []

    def get_portfolio_by_sub(self) -> Dict[str, Any]:
        """
        Fetch portfolio positions for current sub-account.

        Retrieves aggregated portfolio view including:
        - Current positions + pending locked quantities
        - Mark-to-market PnL when environment ID is available
        - Per-instrument breakdown

        Returns:
            Dict with keys:
                - success: bool
                - items: List of instrument positions, each with:
                    - instrument: str (e.g., "VNM", "VN30F2511")
                    - quantity: Decimal (total including pending)
                    - totalCost: Decimal (total cost basis)
                    - currentPrice: Decimal (current market price, may be None)
                    - marketValue: Decimal (quantity * currentPrice, may be None)
                    - pnl: Decimal (marketValue - totalCost, may be None)
            Empty dict {} if query fails.

        Example:
            >>> portfolio = client.get_portfolio_by_sub()
            >>> if portfolio.get('success'):
            ...     for item in portfolio['items']:
            ...         print(f"{item['instrument']}: {item['quantity']} "
            ...               f"@ {item['currentPrice']} = {item['pnl']} PnL")
            VNM: 1000 @ 72000.00 = 240000.00 PnL

        Thread Safety:
            Safe to call from multiple threads.

        Note:
            PnL calculations require environment ID attachment and
            real-time price data availability.
        """
        if not self._ensure_account_id():
            self.logger.warning(
                "event=portfolio_missing_fix_id",
                extra={"username": self.username}
            )
            return {}

        sub_id = self.sub.current()
        if not sub_id:
            self.logger.warning(
                "event=portfolio_missing_sub_id",
                extra={"fixAccountID": self.ID}
            )
            return {}

        try:
            resp = self.rest.post(
                "/api/portfolio-by-sub/basic",
                json={
                    "fixAccountID": self.ID,
                    "subAccountID": sub_id
                }
            )

            if isinstance(resp, dict):
                if resp.get("success") and "items" in resp:
                    items = resp.get("items", [])
                    self.logger.info(
                        "event=portfolio_resolved",
                        extra={
                            "fixAccountID": self.ID,
                            "subAccountID": sub_id,
                            "instrument_count": len(items)
                        }
                    )
                    return resp

                if resp.get("error"):
                    self.logger.warning(
                        "event=portfolio_failed",
                        extra={
                            "fixAccountID": self.ID,
                            "subAccountID": sub_id,
                            "error": resp.get("error")
                        }
                    )
                    return {}

                self.logger.warning(
                    "event=portfolio_invalid_format",
                    extra={
                        "fixAccountID": self.ID,
                        "subAccountID": sub_id,
                        "response": str(resp)
                    }
                )
            else:
                self.logger.warning(
                    "event=portfolio_unexpected_type",
                    extra={
                        "fixAccountID": self.ID,
                        "subAccountID": sub_id,
                        "response_type": type(resp).__name__
                    }
                )
        except Exception as e:
            self.logger.error(
                "event=portfolio_fetch_failed",
                extra={
                    "fixAccountID": self.ID,
                    "subAccountID": sub_id,
                    "error": str(e)
                }
            )

        return {}

    def get_orders(
        self,
        start_date: str,
        end_date: str
    ) -> Dict[str, Any]:
        """
        Fetch orders for current sub-account within date range.

        Retrieves all orders for the current sub-account whose order time
        falls within the specified date range (inclusive).

        Args:
            start_date: Start date in yyyy-MM-dd format (e.g., "2025-11-14")
            end_date: End date in yyyy-MM-dd format (e.g., "2025-11-15")

        Returns:
            Dict with keys:
                - success: bool
                - items: List of orders, each with:
                    - orderId: str
                    - clOrdId: str (client order ID)
                    - accountId: str (internal account ID)
                    - symbol: str (e.g., "VNM")
                    - securityExchange: str (e.g., "HSX")
                    - envId: str (environment ID)
                    - side: str ("1"=Buy, "2"=Sell)
                    - ordType: str
                    - timeInForce: str
                    - price: Decimal
                    - orderQty: Decimal
                    - leavesQty: Decimal (remaining quantity)
                    - cumQty: Decimal (filled quantity)
                    - avgPx: Decimal (average fill price)
                    - ordStatus: str
                    - statusCode: str
                    - statusText: str
                    - lastSharedPx: Decimal
                    - lastSharedQty: Decimal
                    - orderDate: str (yyyy-MM-dd)
                    - lastUpdateDate: str (yyyy-MM-dd)
                    - lastExecId: str
                    - costEachQty: Decimal
                    - isCancelled: bool
                    - isExecuting: bool
            Empty dict {} if query fails.

        Example:
            >>> orders = client.get_orders("2025-11-14", "2025-11-15")
            >>> if orders.get('success'):
            ...     for order in orders['items']:
            ...         print(f"{order['symbol']} {order['side']} "
            ...               f"{order['orderQty']} @ {order['price']} "
            ...               f"- {order['ordStatus']}")
            VNM 1 1000 @ 72000.00 - FILLED

        Thread Safety:
            Safe to call from multiple threads.

        Note:
            Date format must be yyyy-MM-dd. Time is automatically
            set to cover the entire day (00:00:00 to 23:59:59 UTC).
        """
        if not self._ensure_account_id():
            self.logger.warning(
                "event=orders_missing_fix_id",
                extra={"username": self.username}
            )
            return {}

        sub_id = self.sub.current()
        if not sub_id:
            self.logger.warning(
                "event=orders_missing_sub_id",
                extra={"fixAccountID": self.ID}
            )
            return {}

        # Validate date format
        import re
        date_pattern = r'^\d{4}-\d{2}-\d{2}$'
        if not re.match(date_pattern, start_date) or not re.match(date_pattern, end_date):
            self.logger.warning(
                "event=orders_invalid_date_format",
                extra={
                    "fixAccountID": self.ID,
                    "subAccountID": sub_id,
                    "start_date": start_date,
                    "end_date": end_date
                }
            )
            return {}

        try:
            resp = self.rest.post(
                "/api/portfolio-by-sub/orders",
                json={
                    "fixAccountID": self.ID,
                    "subAccountID": sub_id,
                    "startTime": start_date,
                    "endTime": end_date
                }
            )

            if isinstance(resp, dict):
                if resp.get("success") and "items" in resp:
                    items = resp.get("items", [])
                    self.logger.info(
                        "event=orders_resolved",
                        extra={
                            "fixAccountID": self.ID,
                            "subAccountID": sub_id,
                            "start_date": start_date,
                            "end_date": end_date,
                            "order_count": len(items)
                        }
                    )
                    return resp

                if resp.get("error"):
                    self.logger.warning(
                        "event=orders_failed",
                        extra={
                            "fixAccountID": self.ID,
                            "subAccountID": sub_id,
                            "start_date": start_date,
                            "end_date": end_date,
                            "error": resp.get("error")
                        }
                    )
                    return {}

                self.logger.warning(
                    "event=orders_invalid_format",
                    extra={
                        "fixAccountID": self.ID,
                        "subAccountID": sub_id,
                        "response": str(resp)
                    }
                )
            else:
                self.logger.warning(
                    "event=orders_unexpected_type",
                    extra={
                        "fixAccountID": self.ID,
                        "subAccountID": sub_id,
                        "response_type": type(resp).__name__
                    }
                )
        except Exception as e:
            self.logger.error(
                "event=orders_fetch_failed",
                extra={
                    "fixAccountID": self.ID,
                    "subAccountID": sub_id,
                    "start_date": start_date,
                    "end_date": end_date,
                    "error": str(e)
                }
            )

        return {}

    def get_transactions_by_date(
        self,
        start_date: str,
        end_date: str
    ) -> Dict[str, Any]:
        """
        Fetch transactions for current sub-account within date range.

        Retrieves all transactions (executions/fills) for the current sub-account
        whose timestamp falls within the specified date range (inclusive).

        Args:
            start_date: Start date in yyyy-MM-dd format (e.g., "2025-11-14")
            end_date: End date in yyyy-MM-dd format (e.g., "2025-11-15")

        Returns:
            Dict with keys:
                - success: bool
                - items: List of transaction dicts, each with:
                    - transactionId: str (transaction ID)
                    - orderId: str (order ID for mapping back to orders)
                    - symbol: str (e.g., "HNXDS:VN30F2512")
                    - type: str (BUY/SELL)
                    - price: Decimal (execution price)
                    - quantity: Decimal (filled quantity)
                    - totalCost: Decimal (price * quantity)
                    - totalFee: Decimal (trading fees)
                    - pnl: Decimal (profit/loss)
                    - envTimestamp: str (environment timestamp)
                    - sysTimestamp: str (system timestamp)
            Empty dict {} if query fails.

        Example:
            >>> txns = client.get_transactions_by_date("2025-11-14", "2025-11-15")
            >>> if txns.get('success'):
            ...     for tx in txns['items']:
            ...         print(f"{tx['symbol']}: {tx['quantity']} @ {tx['price']}")

        Thread Safety:
            Safe to call from multiple threads.
        """
        if not self._ensure_account_id():
            self.logger.warning(
                "event=transactions_skipped_due_to_missing_account_id",
                extra={"username": self.username},
            )
            return {}

        current_sub = self.sub.current()

        try:
            payload = {
                "fixAccountID": self.ID,
                "subAccountID": current_sub,
                "startTime": start_date,
                "endTime": end_date,
            }

            self.logger.debug(
                "event=transactions_request",
                extra={
                    "fixAccountID": self.ID,
                    "subAccountID": current_sub,
                    "startTime": start_date,
                    "endTime": end_date,
                },
            )

            response = self.rest.post(
                "/api/transaction-by-sub/list",
                json=payload,
            )

            if isinstance(response, dict):
                success = response.get("success", False)
                items = response.get("items", [])

                self.logger.info(
                    "event=transactions_resolved",
                    extra={
                        "fixAccountID": self.ID,
                        "subAccountID": current_sub,
                        "count": len(items),
                        "success": success,
                    },
                )

                return {
                    "success": success,
                    "items": items,
                }
            else:
                self.logger.warning(
                    "event=transactions_invalid_format",
                    extra={
                        "fixAccountID": self.ID,
                        "subAccountID": current_sub,
                        "response": str(response),
                    },
                )
                return {}

        except Exception as e:
            self.logger.error(
                "event=transactions_fetch_failed",
                extra={
                    "fixAccountID": self.ID,
                    "subAccountID": current_sub,
                    "error": str(e),
                },
            )
            return {}

    def get_max_placeable(
        self,
        symbol: str,
        price: float,
        side: str
    ) -> Dict[str, Any]:
        """
        Calculate maximum placeable quantity for an order.

        Determines the maximum quantity that can be placed given the current
        account state, free cash, margin requirements, and fees.

        Args:
            symbol: Trading symbol (e.g., "MWG", "VN30F1M", "HNXDS:VN30F2512")
            price: Order price
            side: Order side - "BUY" or "SELL"

        Returns:
            Dict with keys:
                - success: bool
                - maxQty: Decimal (maximum placeable quantity, floored)
                - perUnitCost: Decimal (cost per unit including margin and fees)
                - remainCash: Decimal (current free cash available)
                - unlimited: bool (whether account can place unlimited orders)
            Empty dict {} if query fails.

        Example:
            >>> result = client.get_max_placeable("MWG", 73000, "BUY")
            >>> if result.get('success'):
            ...     print(f"Max quantity: {result['maxQty']}")
            ...     print(f"Cost per unit: {result['perUnitCost']}")
            ...     print(f"Available cash: {result['remainCash']}")

        Thread Safety:
            Safe to call from multiple threads.
        """
        if not self._ensure_account_id():
            self.logger.warning(
                "event=max_placeable_skipped_due_to_missing_account_id",
                extra={"username": self.username},
            )
            return {}

        current_sub = self.sub.current()

        try:
            payload = {
                "fixAccountID": self.ID,
                "subAccountID": current_sub,
                "symbol": symbol,
                "price": price,
                "side": side.upper(),
            }

            self.logger.debug(
                "event=max_placeable_request",
                extra={
                    "fixAccountID": self.ID,
                    "subAccountID": current_sub,
                    "symbol": symbol,
                    "price": price,
                    "side": side,
                },
            )

            response = self.rest.post(
                "/api/max-placeable-by-sub/max-placeable",
                json=payload,
            )

            if isinstance(response, dict):
                success = response.get("success", False)
                max_qty = response.get("maxQty")
                per_unit_cost = response.get("perUnitCost")
                remain_cash = response.get("remainCash")
                unlimited = response.get("unlimited", False)

                self.logger.info(
                    "event=max_placeable_resolved",
                    extra={
                        "fixAccountID": self.ID,
                        "subAccountID": current_sub,
                        "symbol": symbol,
                        "maxQty": max_qty,
                        "success": success,
                    },
                )

                return {
                    "success": success,
                    "maxQty": max_qty,
                    "perUnitCost": per_unit_cost,
                    "remainCash": remain_cash,
                    "unlimited": unlimited,
                }
            else:
                self.logger.warning(
                    "event=max_placeable_invalid_format",
                    extra={
                        "fixAccountID": self.ID,
                        "subAccountID": current_sub,
                        "response": str(response),
                    },
                )
                return {}

        except Exception as e:
            self.logger.error(
                "event=max_placeable_fetch_failed",
                extra={
                    "fixAccountID": self.ID,
                    "subAccountID": current_sub,
                    "symbol": symbol,
                    "error": str(e),
                },
            )
            return {}
