"""Live paper trading engine for PredictiveRSIMarketMaker.

This module wires three pieces together:
- PredictiveRSIMarketMaker: quote generation logic.
- PaperBrokerClient: FIX order management and execution reports.
- KafkaMarketDataClient: live quote stream.

Environment variables used:
- PAPERBROKER_USERNAME
- PAPERBROKER_PASSWORD
- PAPERBROKER_SUB_ACCOUNT
- PAPERBROKER_SOCKET_CONNECT_HOST
- PAPERBROKER_SOCKET_CONNECT_PORT
- PAPERBROKER_SENDER_COMP_ID
- PAPERBROKER_KAFKA_BOOTSTRAP_SERVERS
- PAPERBROKER_KAFKA_TOPIC
- PAPERBROKER_KAFKA_GROUP_ID
- PAPERBROKER_KAFKA_CLIENT_ID
- PAPERBROKER_SYMBOL
- PAPERBROKER_ORDER_QTY
- PAPERBROKER_CONTRACT_MULTIPLIER
- PAPERBROKER_TICK_SIZE
- PAPERBROKER_MAX_INVENTORY
- PAPERBROKER_SPREAD_MULTIPLIER
- PAPERBROKER_PORTFOLIO_REFRESH_SECONDS
"""

from __future__ import annotations

import asyncio
from contextlib import nullcontext
from datetime import datetime, time as dt_time
import inspect
import os
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv

from backtesting import PredictiveRSIMarketMaker

try:
    # Adjust this import path if your SDK exposes classes from a different module.
    from paperbroker.client import PaperBrokerClient
    from paperbroker.market_data import KafkaMarketDataClient, QuoteSnapshot
except ImportError as exc:  # pragma: no cover - runtime dependency
    raise ImportError(
        "Could not import PaperBrokerClient/KafkaMarketDataClient. "
        "Install or expose your paper broker SDK before running paper_trading.py."
    ) from exc


@dataclass
class ActiveOrder:
    cl_ord_id: str
    side: str
    price: float
    qty: int
    timestamp: float = 0.0


class LiveTradingEngine:
    """Live paper trading orchestrator.

    The engine consumes tick updates, computes indicators on a rolling window,
    asks PredictiveRSIMarketMaker for quotes, and manages active FIX orders.
    It also enforces a global kill switch to flatten and halt trading.
    """

    def __init__(self) -> None:
        # Override stale process-level variables with values from .env.
        load_dotenv(override=True)

        self.username = self._require_env_any("PAPER_USERNAME", "PAPERBROKER_USERNAME").strip()
        self.password = self._require_env_any("PAPER_PASSWORD", "PAPERBROKER_PASSWORD").strip()
        self.sub_account = self._require_env_any(
            "PAPER_ACCOUNT_ID_D1", "PAPERBROKER_SUB_ACCOUNT"
        ).strip()
        self.socket_connect_host = self._require_env_any(
            "PAPERBROKER_SOCKET_CONNECT_HOST", "SOCKET_HOST"
        )
        self.socket_connect_port = int(
            self._require_env_any("PAPERBROKER_SOCKET_CONNECT_PORT", "SOCKET_PORT")
        )
        self.sender_comp_id = self._require_env_any("PAPERBROKER_SENDER_COMP_ID", "SENDER_COMP_ID")

        self.symbol = self._get_env_any("TARGET_SYMBOL", "PAPERBROKER_SYMBOL") or "VN30F1M"
        self.order_qty = int(os.getenv("PAPERBROKER_ORDER_QTY", "1"))
        self.contract_multiplier = float(os.getenv("PAPERBROKER_CONTRACT_MULTIPLIER", "100"))
        self.tick_size = float(os.getenv("PAPERBROKER_TICK_SIZE", "0.1"))
        self.max_inventory = int(os.getenv("PAPERBROKER_MAX_INVENTORY", "5"))
        spread_multiplier = float(os.getenv("PAPERBROKER_SPREAD_MULTIPLIER", "1.0"))
        self.spread_multiplier = spread_multiplier

        self.market_maker = PredictiveRSIMarketMaker(spread_multiplier=spread_multiplier)

        self.initial_capital = 500_000_000.0
        self.current_equity = self.initial_capital
        self.kill_switch_equity = 400_000_000.0
        self.stop_loss_points = float(os.getenv("PAPERBROKER_STOP_LOSS_POINTS", "10.0"))
        self.trading_halted = False

        #self.fee_per_contract = 40_000.0  # 0.4 points * 100,000 multiplier
        #temporary disable fee to better visualize PnL in paper trading. Remember to re-enable for realistic backtesting.
        self.fee_per_contract = 0.0


        self.inventory = 0
        self.avg_entry_price: Optional[float] = None
        self.realized_pnl = 0.0
        self.last_price: Optional[float] = None
        self.total_trades_executed = 0
        self.state_heal_count = 0
        self.latest_indicators: Dict[str, Optional[float]] = {
            "rsi": None,
            "adx": None,
            "atr": None,
            "dynamic_spread": None,
        }

        self.prices: Deque[float] = deque(maxlen=300)
        self._last_price_append_time = 0.0
        self.min_points_for_signals = 100

        self.active_orders: Dict[str, Optional[ActiveOrder]] = {"BUY": None, "SELL": None}
        # Track in-flight non-quote orders (marketable and passive flatten orders).
        self.inflight_market_orders: Dict[str, Dict[str, Any]] = {}
        self._inflight_lock = threading.RLock()
        # Hold fills that arrive before REST placement state catches up.
        self.unresolved_fills: list[dict[str, Any]] = []
        self._unresolved_fills_lock = threading.RLock()

        self.portfolio_refresh_seconds = float(
            os.getenv("PAPERBROKER_PORTFOLIO_REFRESH_SECONDS", "3")
        )
        self._last_portfolio_pull = 0.0
        self._startup_equity_initialized = False

        self.reversal_flatten_cooldown_seconds = float(
            os.getenv("PAPERBROKER_REVERSAL_FLATTEN_COOLDOWN_SECONDS", "2")
        )
        self.reversal_flatten_max_retries = int(
            os.getenv("PAPERBROKER_REVERSAL_FLATTEN_MAX_RETRIES", "3")
        )
        self.reversal_flatten_lockout_seconds = float(
            os.getenv("PAPERBROKER_REVERSAL_FLATTEN_LOCKOUT_SECONDS", "20")
        )
        self.reversal_flatten_state: Dict[str, Dict[str, float]] = {
            "BUY": {"failures": 0.0, "next_retry_ts": 0.0},
            "SELL": {"failures": 0.0, "next_retry_ts": 0.0},
        }
        self.reversal_flatten_escalation_ticks: Dict[str, int] = {"BUY": 0, "SELL": 0}
        self.reversal_flatten_timeout_seconds = float(
            os.getenv("PAPERBROKER_REVERSAL_FLATTEN_TIMEOUT_SECONDS", "3")
        )

        self.requote_min_interval_seconds = float(
            os.getenv("PAPERBROKER_REQUOTE_MIN_INTERVAL_SECONDS", "1.0")
        )
        self.order_modify_cooldown_seconds = float(
            os.getenv("PAPERBROKER_ORDER_MODIFY_COOLDOWN_SECONDS", "5.0")
        )
        self.cancel_retry_backoff_seconds = float(
            os.getenv("PAPERBROKER_CANCEL_RETRY_BACKOFF_SECONDS", "5.0")
        )
        self._last_requote_ts: Dict[str, float] = {"BUY": 0.0, "SELL": 0.0}
        self._cancel_retry_block_until: Dict[str, float] = {"BUY": 0.0, "SELL": 0.0}

        self.startup_recovery_enabled = (
            os.getenv("PAPERBROKER_STARTUP_RECOVERY_ENABLED", "1").strip().lower()
            not in {"0", "false", "no"}
        )
        self.startup_recovery_max_cancel_orders = int(
            os.getenv("PAPERBROKER_STARTUP_RECOVERY_MAX_CANCEL_ORDERS", "6")
        )
        self.startup_recovery_cancel_timeout_seconds = float(
            os.getenv("PAPERBROKER_STARTUP_RECOVERY_CANCEL_TIMEOUT_SECONDS", "0.5")
        )

        self.reconciliation_interval_seconds = float(
            os.getenv("PAPERBROKER_RECONCILIATION_SECONDS", "600")
        )
        self.analytics_refresh_seconds = float(
            os.getenv("PAPERBROKER_ANALYTICS_SECONDS", "10")
        )
        self.analytics_chart_save_every = max(
            1,
            int(os.getenv("PAPERBROKER_ANALYTICS_CHART_SAVE_EVERY", "6")),
        )
        self._pending_snapshots_since_chart = 0
        self.reconciliation_probe_trade_count = (
            os.getenv("PAPERBROKER_RECON_PROBE_TRADE_COUNT", "1").strip().lower()
            not in {"0", "false", "no"}
        )
        self._printed_trade_api_capabilities = False

        self.analytics_timestamps: List[datetime] = []
        self.analytics_equity: List[float] = []
        self.analytics_inventory: List[int] = []
        self.analytics_price: List[float] = []
        self.analytics_elapsed_seconds: List[float] = []
        self.analytics_rsi: List[Optional[float]] = []
        self.analytics_adx: List[Optional[float]] = []
        self._analytics_last_wall_ts: Optional[float] = None
        self.analytics_history_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "result",
            "papertrading",
            "analytics_history.csv",
        )
        self._load_analytics_history()

        self.client = self._build_fix_client()
        self.market_data_client = self._build_kafka_client()

        self._register_fix_events()
        self._register_market_data_events()

    @staticmethod
    def _require_env(name: str) -> str:
        value = os.getenv(name)
        if not value:
            raise ValueError(f"Missing required environment variable: {name}")
        return value

    @staticmethod
    def _get_env_any(*names: str) -> Optional[str]:
        for name in names:
            value = os.getenv(name)
            if value:
                return value
        return None

    def _require_env_any(self, *names: str) -> str:
        value = self._get_env_any(*names)
        if value is None:
            joined = ", ".join(names)
            raise ValueError(f"Missing required environment variable. Any of: {joined}")
        return value

    @staticmethod
    def _to_dict(obj: Any) -> Dict[str, Any]:
        if obj is None:
            return {}
        if isinstance(obj, dict):
            return obj
        if hasattr(obj, "__dict__"):
            return vars(obj)
        return {}

    def _build_fix_client(self) -> Any:
        project_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(project_dir, "logs")
        os.makedirs(os.path.join(log_dir, "client_fix_messages"), exist_ok=True)

        kwargs = {
            "username": self.username,
            "password": self.password,
            "default_sub_account": self.sub_account,
            "socket_connect_host": self.socket_connect_host,
            "socket_connect_port": self.socket_connect_port,
            "sender_comp_id": self.sender_comp_id,
            "target_comp_id": self._get_env_any("PAPERBROKER_TARGET_COMP_ID", "TARGET_COMP_ID")
            or "SERVER",
            "rest_base_url": self._get_env_any(
                "PAPERBROKER_REST_BASE_URL", "PAPER_REST_BASE_URL"
            )
            or "https://papertrade.algotrade.vn/accounting",
            "log_dir": log_dir,
        }

        # Newer SDK versions support disabling persistent order store.
        try:
            if "order_store_path" in inspect.signature(PaperBrokerClient.__init__).parameters:
                kwargs["order_store_path"] = None
        except (TypeError, ValueError):
            pass

        return PaperBrokerClient(**kwargs)

    def _build_kafka_client(self) -> Any:
        kwargs = {
            "bootstrap_servers": self._require_env("PAPERBROKER_KAFKA_BOOTSTRAP_SERVERS"),
            "env_id": os.getenv("PAPERBROKER_ENV_ID", "real"),
            "merge_updates": True,
        }
        kafka_username = os.getenv("PAPERBROKER_KAFKA_USERNAME")
        kafka_password = os.getenv("PAPERBROKER_KAFKA_PASSWORD")
        if kafka_username and kafka_username.strip().lower() != "username":
            kwargs["username"] = kafka_username.strip()
        if kafka_password and kafka_password.strip().lower() != "password":
            kwargs["password"] = kafka_password.strip()
        return KafkaMarketDataClient(**kwargs)

    def _register_fix_events(self) -> None:
        self.client.on("fix:logon", self.on_logon)
        self.client.on("fix:logout", self.on_logout)
        self.client.on("fix:order:filled", self.on_order_filled)
        self.client.on("fix:order:partial_fill", self.on_order_filled)
        self.client.on("fix:order:canceled", self.on_order_canceled)
        self.client.on("fix:order:rejected", self.on_order_rejected)

    def _register_market_data_events(self) -> None:
        pass  # Handled asynchronously in _run_kafka_consumer

    def start(self) -> None:
        print("Starting LiveTradingEngine...")

        if hasattr(self.client, "connect"):
            self.client.connect()
        elif hasattr(self.client, "start"):
            self.client.start()

        if hasattr(self.market_data_client, "connect"):
            self.market_data_client.connect()
        elif hasattr(self.market_data_client, "start"):
            self.market_data_client.start()

    async def start_async(self) -> None:
        """Run FIX + Kafka + dashboard in asyncio mode."""
        print(f"Run started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("Starting LiveTradingEngine (async)...")

        await self._call_client_method_async(self.client, ("connect", "start"))

        print("Waiting for FIX and REST authentication...")
        for _ in range(100):  # 10-second timeout
            if hasattr(self.client, "is_logged_on") and self.client.is_logged_on():
                break
            await asyncio.sleep(0.1)

        if hasattr(self.client, "is_logged_on") and not self.client.is_logged_on():
            print("Authentication timeout. Check your credentials.")
            await self.stop_async()
            return

        # Use broker portfolio only once to seed startup capital.
        self._initialize_startup_equity()
        if self.trading_halted:
            print("Startup checks failed. Trading engine halted before loop startup.")
            await self.stop_async()
            return

        # Sync actual broker inventory to avoid startup desynchronization.
        self._initialize_startup_position()

        await self._recover_pending_orders_startup()

        dashboard_task = asyncio.create_task(self.print_dashboard_loop(), name="dashboard-loop")
        kafka_task = asyncio.create_task(self._run_kafka_consumer(), name="kafka-consumer")
        trading_task = asyncio.create_task(self._trading_logic_loop(), name="trading-logic")
        recon_task = asyncio.create_task(self._reconciliation_loop(), name="recon-loop")
        analytics_task = asyncio.create_task(self._analytics_loop(), name="analytics-loop")
        eod_task = asyncio.create_task(self._eod_liquidation_loop(), name="eod-loop")

        try:
            await asyncio.gather(
                kafka_task,
                dashboard_task,
                trading_task,
                recon_task,
                analytics_task,
                eod_task,
            )
        finally:
            if self.analytics_timestamps:
                self._save_analytics_charts()
            await self.stop_async()

    async def stop_async(self) -> None:
        print("Stopping LiveTradingEngine...")

        if hasattr(self.market_data_client, "stop"):
            result = self.market_data_client.stop()
            if inspect.isawaitable(result):
                await result
        if hasattr(self.client, "disconnect"):
            self.client.disconnect()
        elif hasattr(self.client, "stop"):
            self.client.stop()

    def stop(self) -> None:
        """Synchronous wrapper for compatibility with non-async callers."""
        try:
            asyncio.run(self.stop_async())
        except RuntimeError:
            # If already in an event loop, schedule async teardown best-effort.
            loop = asyncio.get_event_loop()
            loop.create_task(self.stop_async())

    def on_logon(self, session_id: str, **kwargs) -> None:
        _ = kwargs
        print(f"FIX logon successful. Session: {session_id}")

    def on_logout(self, session_id: str, reason: Optional[str] = None, **kwargs) -> None:
        _ = kwargs
        print(f"FIX logout received. Session: {session_id} | Reason: {reason or 'Normal logout'}")

    def on_order_canceled(
        self,
        orig_cl_ord_id: str,
        status: str,
        cum_qty: int = 0,
        **kwargs,
    ) -> None:
        _ = (status, cum_qty, kwargs)
        for side in ("BUY", "SELL"):
            if self.active_orders.get(side) and self.active_orders[side].cl_ord_id == orig_cl_ord_id:
                self.active_orders[side] = None
                print(f"Order canceled: side={side}, cl_ord_id={orig_cl_ord_id}")

        with self._inflight_lock:
            canceled_non_quote = self.inflight_market_orders.pop(orig_cl_ord_id, None)
        if canceled_non_quote and str(canceled_non_quote.get("tag", "")) == "reversal_flatten":
            canceled_side = str(canceled_non_quote.get("side", "")).upper()
            if canceled_non_quote.get("escalate_after_cancel") and canceled_side in {"BUY", "SELL"}:
                self.reversal_flatten_escalation_ticks[canceled_side] = min(
                    self.reversal_flatten_escalation_ticks.get(canceled_side, 0) + 1,
                    10,
                )
                print(
                    f"Reversal flatten for {canceled_side} timed out; "
                    f"escalating to {self.reversal_flatten_escalation_ticks[canceled_side]} tick(s)."
                )

    def on_order_rejected(self, cl_ord_id: str, reason: str, status: str, **kwargs) -> None:
        _ = (status, kwargs)
        print(f"Order rejected: cl_ord_id={cl_ord_id}, reason={reason}")
        for side in ("BUY", "SELL"):
            if self.active_orders.get(side) and self.active_orders[side].cl_ord_id == cl_ord_id:
                self.active_orders[side] = None
        with self._inflight_lock:
            market_order = self.inflight_market_orders.pop(cl_ord_id, None)

        if market_order and str(market_order.get("tag", "")) == "reversal_flatten":
            rejected_side = str(market_order.get("side", "")).upper()
            if rejected_side in {"BUY", "SELL"}:
                self._mark_reversal_flatten_failure(rejected_side, reason)

    def on_order_filled(
        self,
        cl_ord_id: str,
        status: str,
        last_px: float,
        last_qty: int,
        cum_qty: Optional[int] = None,
        avg_px: Optional[float] = None,
        **kwargs,
    ) -> None:
        _ = (cum_qty, avg_px, kwargs)

        side = None
        if self.active_orders.get("BUY") and self.active_orders["BUY"].cl_ord_id == cl_ord_id:
            side = "BUY"
        elif self.active_orders.get("SELL") and self.active_orders["SELL"].cl_ord_id == cl_ord_id:
            side = "SELL"
        else:
            with self._inflight_lock:
                market_order = self.inflight_market_orders.get(cl_ord_id)
            if market_order is not None:
                side = str(market_order.get("side", "")).upper()

        if not side:
            print(f"Fill arrived early for {cl_ord_id}. Queuing to pending fills.")
            with self._unresolved_fills_lock:
                self.unresolved_fills.append(
                    {
                        "cl_ord_id": cl_ord_id,
                        "status": status,
                        "last_px": last_px,
                        "last_qty": last_qty,
                    }
                )
            return

        fill_qty = int(last_qty)
        with self._inflight_lock:
            tracked = self.inflight_market_orders.get(cl_ord_id)
            if tracked is not None:
                tracked["filled_qty"] = int(tracked.get("filled_qty", 0)) + fill_qty

        self._apply_fill(side=side, qty=fill_qty, price=last_px)
        self.total_trades_executed += fill_qty

        if status == "FILLED":
            if self.active_orders.get(side) and self.active_orders[side].cl_ord_id == cl_ord_id:
                self.active_orders[side] = None
            with self._inflight_lock:
                completed_market_order = self.inflight_market_orders.pop(cl_ord_id, None)

            if (
                completed_market_order
                and str(completed_market_order.get("tag", "")) == "reversal_flatten"
            ):
                completed_side = str(completed_market_order.get("side", "")).upper()
                if completed_side in {"BUY", "SELL"}:
                    self._reset_reversal_flatten_state(completed_side)

        self._refresh_equity(force_pull=False)
        self._evaluate_kill_switch()

    def on_quote(self, instrument: str, quote_snapshot: Any) -> None:
        """Lightweight callback just to append the latest price."""
        _ = instrument
        latest_price = self._extract_latest_price(quote_snapshot)
        if latest_price is None:
            return

        self.last_price = latest_price
        now = time.time()
        if now - self._last_price_append_time >= 1.0:
            self.prices.append(latest_price)
            self._last_price_append_time = now

    async def _trading_logic_loop(self) -> None:
        """Background task to calculate indicators and place orders."""
        last_processed_price = None

        while not self.trading_halted:
            await asyncio.sleep(0.1)
            self._manage_reversal_flatten_timeouts()

            with self._unresolved_fills_lock:
                unresolved_snapshot = list(self.unresolved_fills)

            resolved_fill_tokens: set[int] = set()
            for fill in unresolved_snapshot:
                cl_ord_id = str(fill.get("cl_ord_id", ""))
                side = None
                if self.active_orders.get("BUY") and self.active_orders["BUY"].cl_ord_id == cl_ord_id:
                    side = "BUY"
                elif self.active_orders.get("SELL") and self.active_orders["SELL"].cl_ord_id == cl_ord_id:
                    side = "SELL"
                else:
                    with self._inflight_lock:
                        market_order = self.inflight_market_orders.get(cl_ord_id)
                    if market_order:
                        side = str(market_order.get("side", "")).upper()

                if not side:
                    continue

                print(f"Resolving delayed fill for {cl_ord_id} on side {side}")
                fill_qty = int(fill.get("last_qty", 0))
                fill_px = float(fill.get("last_px", 0.0))
                fill_status = str(fill.get("status", ""))

                self._apply_fill(side=side, qty=fill_qty, price=fill_px)
                self.total_trades_executed += fill_qty

                if fill_status == "FILLED":
                    if self.active_orders.get(side) and self.active_orders[side].cl_ord_id == cl_ord_id:
                        self.active_orders[side] = None
                    with self._inflight_lock:
                        completed = self.inflight_market_orders.pop(cl_ord_id, None)
                    if completed and str(completed.get("tag", "")) == "reversal_flatten":
                        completed_side = str(completed.get("side", "")).upper()
                        if completed_side in {"BUY", "SELL"}:
                            self._reset_reversal_flatten_state(completed_side)

                resolved_fill_tokens.add(id(fill))

            if resolved_fill_tokens:
                with self._unresolved_fills_lock:
                    self.unresolved_fills = [
                        fill
                        for fill in self.unresolved_fills
                        if id(fill) not in resolved_fill_tokens
                    ]
                self._refresh_equity(force_pull=False)
                self._evaluate_kill_switch()
                self._evaluate_stop_loss()

            if self.last_price is None or self.last_price == last_processed_price:
                continue

            last_processed_price = self.last_price

            self._refresh_equity(force_pull=False)
            self._evaluate_kill_switch()
            self._evaluate_stop_loss()

            if self.trading_halted:
                break

            if len(self.prices) < self.min_points_for_signals:
                continue

            indicators = self._calculate_indicators()
            if indicators is None:
                continue

            self.latest_indicators["rsi"] = indicators["rsi"]
            self.latest_indicators["adx"] = indicators["adx"]
            self.latest_indicators["atr"] = indicators["atr"]
            min_profitable_spread = 0.9
            floored_dynamic_spread = max(
                indicators["atr"] * self.spread_multiplier,
                min_profitable_spread,
            )
            self.latest_indicators["dynamic_spread"] = floored_dynamic_spread

            # Signal-driven reversal flattening: only flush when RSI says regime flipped.
            rsi_value = indicators["rsi"]
            long_signal = rsi_value < 30
            short_signal = rsi_value > 70

            should_flatten_short = self.inventory < 0 and long_signal
            should_flatten_long = self.inventory > 0 and short_signal

            if should_flatten_short:
                if self._has_pending_reversal_flatten("BUY"):
                    continue

                print(
                    f"Signal reversal flush: covering short inventory due to RSI {rsi_value:.2f} < 20."
                )
                self._cancel_all_resting_orders()
                self._place_market_order(
                    side="BUY",
                    qty=abs(self.inventory),
                    tag="reversal_flatten",
                )
                continue

            if should_flatten_long:
                if self._has_pending_reversal_flatten("SELL"):
                    continue

                print(
                    f"Signal reversal flush: dumping long inventory due to RSI {rsi_value:.2f} > 80."
                )
                self._cancel_all_resting_orders()
                self._place_market_order(
                    side="SELL",
                    qty=abs(self.inventory),
                    tag="reversal_flatten",
                )
                continue

            bid_price, ask_price = self.market_maker.calculate_quotes(
                mid_price=self.last_price,
                current_inventory=self.inventory,
                rsi=indicators["rsi"],
                atr=indicators["atr"],
                dynamic_spread=floored_dynamic_spread,
                trend_signal=indicators["trend_signal"],
                adx=indicators["adx"],
                max_inventory=self.max_inventory,
            )

            # Prevent crossed/washable quotes when spread collapses below one tick.
            if (
                self.last_price is not None
                and bid_price is not None
                and ask_price is not None
                and self._normalize_price(bid_price) >= self._normalize_price(ask_price)
            ):
                bid_price = self.last_price - self.tick_size
                ask_price = self.last_price + self.tick_size

            self._replace_order_if_needed("BUY", bid_price)
            self._replace_order_if_needed("SELL", ask_price)

    async def _reconciliation_loop(self) -> None:
        """Periodic state reconciliation via REST to fix FIX drop-copy drift."""
        while not self.trading_halted:
            await asyncio.sleep(self.reconciliation_interval_seconds)

            if self.trading_halted:
                break

            print("\n" + "-" * 40)
            print("10-Minute REST State Reconciliation")
            print("-" * 40)

            self._initialize_startup_position()

            if self.reconciliation_probe_trade_count:
                method_name, broker_trade_count = self._probe_broker_trade_count()
                if broker_trade_count is not None:
                    print(
                        f"Broker Trade Count:     {broker_trade_count} "
                        f"(via {method_name})"
                    )
                    print(f"Local Trade Count:      {self.total_trades_executed}")
                else:
                    print("Broker Trade Count:     NOT AVAILABLE")

            true_equity = self._pull_equity_from_portfolio()
            if true_equity is not None:
                self._rebase_equity(true_equity)
                print(f"Reconciled Equity:       {self.current_equity:,.0f} VND")
                self._record_analytics_snapshot()
                self._save_analytics_charts()
                self._pending_snapshots_since_chart = 0
            else:
                print("Reconciled Equity:       FAILED TO PULL")
            print("-" * 40 + "\n")

    def _rebase_equity(self, true_equity: float) -> None:
        """Align internal PnL baseline to the latest broker-reported equity."""
        unrealized = 0.0
        if self.inventory != 0 and self.avg_entry_price is not None and self.last_price is not None:
            if self.inventory > 0:
                unrealized = (
                    (self.last_price - self.avg_entry_price)
                    * abs(self.inventory)
                    * self.contract_multiplier
                )
            else:
                unrealized = (
                    (self.avg_entry_price - self.last_price)
                    * abs(self.inventory)
                    * self.contract_multiplier
                )

        self.initial_capital = float(true_equity) - unrealized
        self.realized_pnl = 0.0
        self.current_equity = float(true_equity)

    def _record_analytics_snapshot(self) -> None:
        if self.last_price is None:
            return

        now = datetime.now()
        self.analytics_timestamps.append(now)
        self.analytics_equity.append(float(self.current_equity))
        self.analytics_inventory.append(int(self.inventory))
        self.analytics_price.append(float(self.last_price))
        rsi_value = self.latest_indicators.get("rsi")
        adx_value = self.latest_indicators.get("adx")
        self.analytics_rsi.append(float(rsi_value) if rsi_value is not None else None)
        self.analytics_adx.append(float(adx_value) if adx_value is not None else None)

        if self._analytics_last_wall_ts is None:
            elapsed = (
                self.analytics_elapsed_seconds[-1] if self.analytics_elapsed_seconds else 0.0
            )
        else:
            delta = max(0.0, time.time() - self._analytics_last_wall_ts)
            elapsed = (
                (self.analytics_elapsed_seconds[-1] if self.analytics_elapsed_seconds else 0.0)
                + delta
            )

        self.analytics_elapsed_seconds.append(float(elapsed))
        self._analytics_last_wall_ts = time.time()
        self._append_analytics_snapshot(
            ts=now,
            elapsed_seconds=elapsed,
            rsi=self.analytics_rsi[-1],
            adx=self.analytics_adx[-1],
        )

    def _save_analytics_charts(self) -> None:
        if not self.analytics_timestamps:
            return

        result_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "result",
            "papertrading",
        )
        os.makedirs(result_dir, exist_ok=True)

        elapsed_index = pd.Series(self.analytics_elapsed_seconds, dtype="float64")

        equity_series = pd.Series(self.analytics_equity, index=elapsed_index)
        price_series = pd.Series(self.analytics_price, index=elapsed_index)
        inventory_series = pd.Series(self.analytics_inventory, index=elapsed_index)

        hpr = equity_series / float(self.initial_capital) if self.initial_capital else equity_series
        rolling_max = equity_series.cummax()
        drawdown = (equity_series - rolling_max) / rolling_max.replace(0.0, pd.NA)
        single_point = len(equity_series) <= 1
        marker = "o" if single_point else None

        plt.figure(figsize=(10, 6))
        plt.plot(hpr.index, hpr.values, color="black", label="HPR", marker=marker)
        plt.title("Holding Period Return (Paper Trading)")
        plt.xlabel("Elapsed Trading Seconds")
        plt.ylabel("HPR")
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(result_dir, "hpr.svg"), dpi=300)
        plt.close()

        plt.figure(figsize=(10, 6))
        plt.plot(drawdown.index, drawdown.values, color="black", label="Drawdown", marker=marker)
        plt.title("Drawdown (Paper Trading)")
        plt.xlabel("Elapsed Trading Seconds")
        plt.ylabel("Drawdown")
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(result_dir, "drawdown.svg"), dpi=300)
        plt.close()

        plt.figure(figsize=(10, 6))
        plt.plot(
            inventory_series.index,
            inventory_series.values,
            color="black",
            label="Inventory",
            marker=marker,
        )
        plt.title("Inventory Over Time (Paper Trading)")
        plt.xlabel("Elapsed Trading Seconds")
        plt.ylabel("Contracts")
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(result_dir, "inventory.svg"), dpi=300)
        plt.close()

        plt.figure(figsize=(10, 6))
        plt.plot(price_series.index, price_series.values, color="black", label="Price", marker=marker)
        plt.title("Price Over Time (Paper Trading)")
        plt.xlabel("Elapsed Trading Seconds")
        plt.ylabel("Price")
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(result_dir, "price.svg"), dpi=300)
        plt.close()

    async def _eod_liquidation_loop(self) -> None:
        """Hard-stop risk control at morning and afternoon session ends (GMT+7)."""
        morning_liquidation_start = dt_time(hour=11, minute=29, second=30)
        morning_liquidation_end = dt_time(hour=11, minute=30, second=0)
        afternoon_liquidation_start = dt_time(hour=14, minute=29, second=30)
        afternoon_liquidation_end = dt_time(hour=14, minute=30, second=0)
        vn_tz = ZoneInfo("Asia/Ho_Chi_Minh")

        while True:
            await asyncio.sleep(1)

            if self.trading_halted:
                break

            now_local = datetime.now(vn_tz).time()
            if morning_liquidation_start <= now_local < morning_liquidation_end:
                print("\n" + "#" * 90)
                print("CRITICAL MORNING SESSION LIQUIDATION TRIGGERED (11:29:30-11:30:00 GMT+7)")
                print("Force-halting trading, canceling all resting orders, flattening inventory now.")
                print("#" * 90 + "\n")

                self.trading_halted = True
                self._cancel_all_resting_orders()
                self._flatten_inventory_market(reason="morning_liquidation")
                break

            if now_local >= morning_liquidation_end and now_local < afternoon_liquidation_start:
                print("\n" + "#" * 90)
                print("MORNING SESSION HARD STOP: trading paused after 11:30:00 GMT+7")
                print("Halting trading and stopping all loops.")
                print("#" * 90 + "\n")
                self.trading_halted = True
                self._cancel_all_resting_orders()
                self._flatten_inventory_market(reason="morning_liquidation")
                break

            if afternoon_liquidation_start <= now_local < afternoon_liquidation_end:
                print("\n" + "#" * 90)
                print("CRITICAL EOD LIQUIDATION TRIGGERED (14:29:30-14:30:00 GMT+7)")
                print("Force-halting trading, canceling all resting orders, flattening inventory now.")
                print("#" * 90 + "\n")

                self.trading_halted = True
                self._cancel_all_resting_orders()
                self._flatten_inventory_market(reason="eod_liquidation")
                break

            if now_local >= afternoon_liquidation_end:
                print("\n" + "#" * 90)
                print("EOD HARD STOP: trading session closed (>= 14:30:00 GMT+7)")
                print("Halting trading and stopping all loops.")
                print("#" * 90 + "\n")
                self.trading_halted = True
                self._cancel_all_resting_orders()
                self._flatten_inventory_market(reason="eod_liquidation")
                break

    async def _analytics_loop(self) -> None:
        """Persist analytics snapshots during live trading, independent of reconciliation."""
        while not self.trading_halted:
            await asyncio.sleep(self.analytics_refresh_seconds)

            if self.trading_halted:
                break

            if self.last_price is None:
                continue

            self._record_analytics_snapshot()
            self._pending_snapshots_since_chart += 1
            if self._pending_snapshots_since_chart >= self.analytics_chart_save_every:
                self._save_analytics_charts()
                self._pending_snapshots_since_chart = 0

    def _load_analytics_history(self) -> None:
        if not os.path.exists(self.analytics_history_path):
            return

        try:
            history = pd.read_csv(self.analytics_history_path)
        except Exception as exc:
            print(f"Failed to load analytics history: {exc}")
            return

        required_cols = {"timestamp", "equity", "inventory", "price", "elapsed_seconds"}
        if not required_cols.issubset(set(history.columns)):
            print("Analytics history missing required columns. Starting fresh in-memory.")
            return

        try:
            history = history.dropna(subset=["timestamp"]).copy()
            history["timestamp"] = pd.to_datetime(history["timestamp"], errors="coerce")
            history = history.dropna(subset=["timestamp"]).copy()
        except Exception:
            return

        self.analytics_timestamps = history["timestamp"].tolist()
        self.analytics_equity = history["equity"].astype(float).tolist()
        self.analytics_inventory = history["inventory"].astype(int).tolist()
        self.analytics_price = history["price"].astype(float).tolist()
        self.analytics_elapsed_seconds = history["elapsed_seconds"].astype(float).tolist()
        if "rsi" in history.columns:
            rsi_series = pd.to_numeric(history["rsi"], errors="coerce")
            self.analytics_rsi = [float(value) if pd.notna(value) else None for value in rsi_series]
        else:
            self.analytics_rsi = [None] * len(self.analytics_timestamps)

        if "adx" in history.columns:
            adx_series = pd.to_numeric(history["adx"], errors="coerce")
            self.analytics_adx = [float(value) if pd.notna(value) else None for value in adx_series]
        else:
            self.analytics_adx = [None] * len(self.analytics_timestamps)
        self._analytics_last_wall_ts = None

    def _append_analytics_snapshot(
        self,
        ts: datetime,
        elapsed_seconds: float,
        rsi: Optional[float],
        adx: Optional[float],
    ) -> None:
        # One-time schema migration for older files that don't have rsi/adx columns yet.
        if os.path.exists(self.analytics_history_path):
            try:
                existing_header = pd.read_csv(self.analytics_history_path, nrows=0)
                has_rsi = "rsi" in existing_header.columns
                has_adx = "adx" in existing_header.columns
                if not (has_rsi and has_adx):
                    existing_history = pd.read_csv(self.analytics_history_path)
                    if "rsi" not in existing_history.columns:
                        existing_history["rsi"] = pd.NA
                    if "adx" not in existing_history.columns:
                        existing_history["adx"] = pd.NA
                    existing_history.to_csv(self.analytics_history_path, index=False)
            except Exception as exc:
                print(f"Failed to migrate analytics history schema: {exc}")

        record = pd.DataFrame(
            [
                {
                    "timestamp": ts.isoformat(),
                    "equity": float(self.current_equity),
                    "inventory": int(self.inventory),
                    "price": float(self.last_price) if self.last_price is not None else None,
                    "elapsed_seconds": float(elapsed_seconds),
                    "rsi": float(rsi) if rsi is not None else None,
                    "adx": float(adx) if adx is not None else None,
                }
            ]
        )

        os.makedirs(os.path.dirname(self.analytics_history_path), exist_ok=True)
        write_header = not os.path.exists(self.analytics_history_path)
        try:
            record.to_csv(
                self.analytics_history_path,
                mode="a",
                header=write_header,
                index=False,
            )
        except Exception as exc:
            print(f"Failed to append analytics history: {exc}")

    async def print_dashboard_loop(self) -> None:
        """Print live status block every 10 seconds until trading is halted."""
        while not self.trading_halted:
            await asyncio.sleep(10)

            market_price = (
                f"{self.last_price:,.2f}" if self.last_price is not None else "N/A"
            )

            if self.inventory > 0:
                inventory_str = f"Long {self.inventory}"
            elif self.inventory < 0:
                inventory_str = f"Short {abs(self.inventory)}"
            else:
                inventory_str = "Flat 0"

            pnl = self.current_equity - self.initial_capital
            rsi_val = self.latest_indicators.get("rsi")
            adx_val = self.latest_indicators.get("adx")
            spread_val = self.latest_indicators.get("dynamic_spread")

            rsi_str = f"{rsi_val:.2f}" if rsi_val is not None else "N/A"
            adx_str = f"{adx_val:.2f}" if adx_val is not None else "N/A"
            spread_str = f"{spread_val:.4f}" if spread_val is not None else "N/A"

            dashboard_str = "\n".join(
                [
                    "=" * 52,
                    "LiveTradingEngine Dashboard",
                    "=" * 52,
                    f"Market Price:         {market_price}",
                    f"Inventory:            {inventory_str}",
                    f"Equity:               {self.current_equity:,.0f} VND",
                    f"PnL:                  {pnl:,.0f} VND",
                    f"Total Trades:         {self.total_trades_executed}",
                    f"State Heals:          {self.state_heal_count}",
                    f"RSI(30):              {rsi_str}",
                    f"ADX(30):              {adx_str}",
                    f"Dynamic Spread Width: {spread_str}",
                    "=" * 52,
                ]
            )
            self._write_log(dashboard_str, is_dashboard=True)

    def _extract_latest_price(self, quote_snapshot: Any) -> Optional[float]:
        if quote_snapshot is None:
            return None

        if isinstance(quote_snapshot, dict):
            price = quote_snapshot.get("latest_matched_price")
            if price is None and isinstance(quote_snapshot.get("data"), dict):
                price = quote_snapshot["data"].get("latest_matched_price")
            return float(price) if price is not None else None

        if hasattr(quote_snapshot, "latest_matched_price"):
            price = getattr(quote_snapshot, "latest_matched_price")
            return float(price) if price is not None else None

        payload = self._to_dict(quote_snapshot)
        price = payload.get("latest_matched_price")
        return float(price) if price is not None else None

    def _calculate_indicators(self) -> Optional[Dict[str, float]]:
        if len(self.prices) < self.min_points_for_signals:
            return None

        close = pd.Series(self.prices, dtype="float64")
        if close.empty:
            return None

        delta = close.diff()

        gain = delta.clip(lower=0.0)
        loss = -delta.clip(upper=0.0)
        avg_gain = gain.rolling(window=30, min_periods=30).mean()
        avg_loss = loss.rolling(window=30, min_periods=30).mean()
        rs = avg_gain / avg_loss.replace(0.0, pd.NA)
        rsi = 100 - (100 / (1 + rs))

        high = close
        low = close
        prev_close = close.shift(1)

        tr_components = pd.concat(
            [
                (high - low).abs(),
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        )
        true_range = tr_components.max(axis=1)
        atr = true_range.rolling(window=30, min_periods=30).mean()

        up_move = high.diff()
        down_move = -low.diff()

        plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
        minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

        atr_smoothed = true_range.rolling(window=30, min_periods=30).sum()
        plus_di = 100 * (plus_dm.rolling(window=30, min_periods=30).sum() / atr_smoothed)
        minus_di = 100 * (minus_dm.rolling(window=30, min_periods=30).sum() / atr_smoothed)

        dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, pd.NA)) * 100
        adx = dx.rolling(window=30, min_periods=30).mean()

        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        trend_signal = 1 if ema12.iloc[-1] >= ema26.iloc[-1] else -1

        latest_rsi = rsi.iloc[-1]
        latest_atr = atr.iloc[-1]
        latest_adx = adx.iloc[-1]

        if pd.isna(latest_rsi) or pd.isna(latest_atr) or pd.isna(latest_adx):
            return None

        return {
            "rsi": float(latest_rsi),
            "atr": float(latest_atr),
            "adx": float(latest_adx),
            "trend_signal": trend_signal,
        }

    @staticmethod
    def _fmt_optional_float(value: Optional[float], digits: int = 2) -> str:
        if value is None:
            return "N/A"
        return f"{value:.{digits}f}"

    def _write_log(self, message: str, is_dashboard: bool = False) -> None:
        _ = is_dashboard
        ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        formatted_msg = f"[{ts}] {message}"
        print(formatted_msg)

    def _log_exec_limit_event(
        self,
        action: str,
        side: str,
        target_price: float,
        market_price: Optional[float],
    ) -> None:
        self._write_log(
            f"[EXEC] Action: {action} {side} Limit | "
            f"Target Px: {target_price:.2f} | "
            f"Mkt Px: {self._fmt_optional_float(market_price, 2)} | "
            f"Inv: {self.inventory} | "
            f"RSI: {self._fmt_optional_float(self.latest_indicators.get('rsi'), 2)} | "
            f"ADX: {self._fmt_optional_float(self.latest_indicators.get('adx'), 2)} | "
            f"Spread: {self._fmt_optional_float(self.latest_indicators.get('dynamic_spread'), 4)}"
        )

    def _log_flush_event(self, reason: str, side: str, qty: int) -> None:
        self._write_log(
            f"[FLUSH] Market Order Triggered | Reason: {reason} | "
            f"Side: {side} | Qty: {qty} | "
            f"Inv: {self.inventory} | "
            f"Mkt Px: {self._fmt_optional_float(self.last_price, 2)} | "
            f"Entry Px: {self._fmt_optional_float(self.avg_entry_price, 2)}"
        )

    def _replace_order_if_needed(self, side: str, target_price: Optional[float]) -> None:
        if self.trading_halted:
            return

        now = time.time()
        existing = self.active_orders.get(side)

        if now < self._cancel_retry_block_until.get(side, 0.0):
            return

        if (
            existing is not None
            and self.requote_min_interval_seconds > 0
            and now - self._last_requote_ts.get(side, 0.0) < self.requote_min_interval_seconds
        ):
            return

        # 1. PULL QUOTES (If target_price is None, we just want to cancel)
        if target_price is None:
            if existing is not None:
                self._cancel_order(existing.cl_ord_id)
                self._cancel_retry_block_until[side] = now + 2.0
            return

        # 2. INVENTORY CAPS
        if side == "BUY" and self.inventory >= self.max_inventory:
            return
        if side == "SELL" and self.inventory <= -self.max_inventory:
            return

        # 3. REVERSAL-FLUSH GUARD
        # If the engine is currently in the middle of a reversal flatten (because the OTHER side
        # fired a market order and is waiting for a fill), DO NOT place new limit orders on this side.
        with self._inflight_lock:
            is_flattening = any(
                str(order.get("tag", "")) == "reversal_flatten"
                for order in self.inflight_market_orders.values()
            )
        if is_flattening:
            if existing is not None:
                self._cancel_order(existing.cl_ord_id)
                self._cancel_retry_block_until[side] = now + 2.0
            return
        # -----------------------------

        normalized_price = self._normalize_price(target_price)

        # 4. PRICE ADJUSTMENT (Protect queue priority from micro-requoting)
        if existing is not None:
            order_age_seconds = max(0.0, now - float(getattr(existing, "timestamp", 0.0)))
            requote_threshold = self.tick_size * 2
            if order_age_seconds > 10:
                requote_threshold = self.tick_size / 2

            if abs(existing.price - normalized_price) < requote_threshold:
                return

        if (
            existing is not None
            and self.order_modify_cooldown_seconds > 0
            and now - getattr(existing, "timestamp", 0.0) < self.order_modify_cooldown_seconds
        ):
            return

        # 4. RACE CONDITION FIX
        # If we have an existing order, cancel and wait for exchange confirmation.
        if existing is not None:
            self._cancel_order(existing.cl_ord_id)
            self._cancel_retry_block_until[side] = now + 2.0
            return

        # 5. SAFE TO PLACE (Only when no active local order is tracked)
        self._log_exec_limit_event(
            action="PLACE",
            side=side,
            target_price=normalized_price,
            market_price=self.last_price,
        )
        cl_ord_id = self._place_limit_order(
            side=side,
            qty=self.order_qty,
            price=normalized_price,
        )

        if cl_ord_id:
            self.active_orders[side] = ActiveOrder(
                cl_ord_id=cl_ord_id,
                side=side,
                price=normalized_price,
                qty=self.order_qty,
                timestamp=now,
            )
            self._last_requote_ts[side] = now

    def _cancel_order(self, cl_ord_id: str) -> bool:
        def _execute_cancel() -> None:
            try:
                self.client.cancel_order(cl_ord_id=cl_ord_id)
            except TypeError:
                self.client.cancel_order(cl_ord_id)
            except Exception as exc:  # pragma: no cover - runtime broker error
                print(f"Cancel order failed internally for {cl_ord_id}: {exc}")

        try:
            threading.Thread(target=_execute_cancel, daemon=True).start()
            return True
        except Exception as exc:
            print(f"Failed to spawn cancel thread for {cl_ord_id}: {exc}")
            return False

    def _place_limit_order(self, side: str, qty: int, price: float) -> str:
        try:
            cl_ord_id = self.client.place_order(
                full_symbol=self.symbol,
                side=side.upper(),
                qty=qty,
                price=price,
                ord_type="LIMIT",
            )
            return str(cl_ord_id) if cl_ord_id is not None else ""
        except Exception as exc:
            print(f"Failed to place limit order: {exc}")
            return ""

    def _place_market_order(self, side: str, qty: int, tag: str = "") -> str:
        if qty <= 0 or self.last_price is None:
            return ""

        # Exchange rejects OrdType=MARKET, so emulate with an aggressive LIMIT.
        aggressiveness = self.tick_size * 10
        aggressive_price = (
            self.last_price + aggressiveness
            if side.upper() == "BUY"
            else self.last_price - aggressiveness
        )
        normalized_price = self._normalize_price(aggressive_price)
        flush_reason = tag or "marketable_flatten"
        self._log_flush_event(reason=flush_reason, side=side.upper(), qty=qty)

        try:
            cl_ord_id = self.client.place_order(
                full_symbol=self.symbol,
                side=side.upper(),
                qty=qty,
                price=normalized_price,
                ord_type="LIMIT",
            )
            if cl_ord_id is not None:
                cl_ord_id_str = str(cl_ord_id)
                with self._inflight_lock:
                    self.inflight_market_orders[cl_ord_id_str] = {
                        "side": side.upper(),
                        "qty": qty,
                        "tag": tag,
                        "timestamp": time.time(),
                        "price": normalized_price,
                        "filled_qty": 0,
                    }
                return cl_ord_id_str
            return ""
        except Exception as exc:
            print(f"Failed to place marketable limit order: {exc}")
            return ""

    def _place_passive_flatten_order(self, side: str, qty: int, tag: str = "") -> str:
        if qty <= 0 or self.last_price is None:
            return ""

        side_upper = side.upper()
        aggressiveness_ticks = self.reversal_flatten_escalation_ticks.get(side_upper, 0)
        passive_price = self.last_price
        if aggressiveness_ticks > 0:
            if side_upper == "BUY":
                passive_price = self.last_price + (self.tick_size * aggressiveness_ticks)
            else:
                passive_price = self.last_price - (self.tick_size * aggressiveness_ticks)
        normalized_price = self._normalize_price(passive_price)

        try:
            cl_ord_id = self.client.place_order(
                full_symbol=self.symbol,
                side=side_upper,
                qty=qty,
                price=normalized_price,
                ord_type="LIMIT",
            )
            if cl_ord_id is not None:
                cl_ord_id_str = str(cl_ord_id)
                with self._inflight_lock:
                    self.inflight_market_orders[cl_ord_id_str] = {
                        "side": side_upper,
                        "qty": qty,
                        "tag": tag,
                        "timestamp": time.time(),
                        "price": normalized_price,
                        "filled_qty": 0,
                        "cancel_requested": False,
                        "escalate_after_cancel": False,
                    }
                return cl_ord_id_str
            return ""
        except Exception as exc:
            print(f"Failed to place passive flatten order: {exc}")
            return ""

    def _manage_reversal_flatten_timeouts(self) -> None:
        now = time.time()
        timed_out_ids: List[str] = []

        with self._inflight_lock:
            for cl_ord_id, order in self.inflight_market_orders.items():
                if str(order.get("tag", "")) != "reversal_flatten":
                    continue
                if bool(order.get("cancel_requested", False)):
                    continue
                placed_at = float(order.get("timestamp", 0.0))
                if placed_at <= 0:
                    continue
                if now - placed_at < self.reversal_flatten_timeout_seconds:
                    continue

                filled_qty = int(order.get("filled_qty", 0))
                original_qty = int(order.get("qty", 0))
                if original_qty > 0 and filled_qty >= original_qty:
                    continue

                timed_out_ids.append(cl_ord_id)

            for cl_ord_id in timed_out_ids:
                order = self.inflight_market_orders.get(cl_ord_id)
                if order is None:
                    continue
                order["cancel_requested"] = True
                order["escalate_after_cancel"] = True

        for cl_ord_id in timed_out_ids:
            print(
                f"Reversal flatten order {cl_ord_id} stale > "
                f"{self.reversal_flatten_timeout_seconds:.1f}s. Canceling for one-tick escalation."
            )
            self._cancel_order(cl_ord_id)

    def _apply_fill(self, side: str, qty: int, price: float) -> None:
        if qty <= 0:
            return

        safe_price = float(price)

        if self.inventory != 0 and self.avg_entry_price is None:
            self.state_heal_count += 1
            print(
                "CRITICAL: Inventory/entry-price invariant broken "
                f"(inventory={self.inventory}, avg_entry_price=None). "
                f"Heal count={self.state_heal_count}. "
                f"Healing state with fill price {safe_price:.2f}."
            )
            self.avg_entry_price = safe_price

        signed_qty = qty if side == "BUY" else -qty

        # Deduct exchange fees immediately upon any fill
        self.realized_pnl -= (qty * self.fee_per_contract)

        if self.inventory == 0:
            self.inventory = signed_qty
            self.avg_entry_price = safe_price
            return

        if self.inventory > 0 and signed_qty > 0:
            self.avg_entry_price = (
                (self.avg_entry_price * self.inventory) + (safe_price * signed_qty)
            ) / (self.inventory + signed_qty)
            self.inventory += signed_qty
            return

        if self.inventory < 0 and signed_qty < 0:
            abs_inventory = abs(self.inventory)
            abs_new = abs(signed_qty)
            self.avg_entry_price = (
                (self.avg_entry_price * abs_inventory) + (safe_price * abs_new)
            ) / (abs_inventory + abs_new)
            self.inventory += signed_qty
            return

        # Closing/reversing position.
        if self.inventory > 0 and signed_qty < 0:
            closing_qty = min(self.inventory, abs(signed_qty))
            self.realized_pnl += (
                (safe_price - self.avg_entry_price)
                * closing_qty
                * self.contract_multiplier
            )
            self.inventory -= closing_qty
            remainder = abs(signed_qty) - closing_qty
            if remainder > 0:
                self.inventory = -remainder
                self.avg_entry_price = safe_price
            elif self.inventory == 0:
                self.avg_entry_price = None
            return

        if self.inventory < 0 and signed_qty > 0:
            closing_qty = min(abs(self.inventory), signed_qty)
            self.realized_pnl += (
                (self.avg_entry_price - safe_price)
                * closing_qty
                * self.contract_multiplier
            )
            self.inventory += closing_qty
            remainder = signed_qty - closing_qty
            if remainder > 0:
                self.inventory = remainder
                self.avg_entry_price = safe_price
            elif self.inventory == 0:
                self.avg_entry_price = None
            return

    def _refresh_equity(self, force_pull: bool) -> None:
        _ = force_pull
        self.current_equity = self._compute_internal_equity()

    def _initialize_startup_equity(self) -> None:
        if self._startup_equity_initialized:
            return

        self._startup_equity_initialized = True
        print("\n" + "-" * 40)
        print("Account Identity and Mapping Check")
        print("-" * 40)
        print(f"FIX SenderCompID (Tag 49): {self.sender_comp_id}")
        print(f"Target Sub-Account:        {self.sub_account}")

        try:
            # Query equity using the same REST endpoint validated in test script.
            sub_scope = (
                self.client.use_sub_account(self.sub_account)
                if hasattr(self.client, "use_sub_account")
                else nullcontext()
            )
            with sub_scope:
                payload = self.client.get_account_balance()

            if isinstance(payload, dict) and payload.get("success") is False:
                raise ValueError(f"SDK internal REST error: {payload.get('error')}")

            print("REST Mapping Check:        SUCCESS")
        except Exception as exc:
            resolved_fix_id = None
            account_client = getattr(self.client, "account_client", None)
            if account_client is not None:
                resolved_fix_id = getattr(account_client, "ID", None)

            if resolved_fix_id:
                print(f"REST fixAccountID:         {resolved_fix_id}")
            print("REST Mapping Check:        FAILED")
            print(f"Error Details:             {exc}")
            print("")
            print("CRITICAL: Backend account mapping appears desynced.")
            print("REST does not recognize this FIX identity + sub-account pair.")
            print("Halting trading to prevent ghost orders and order timeouts.")
            print("-" * 40 + "\n")

            self.current_equity = self.initial_capital
            return

        data = self._to_dict(payload)
        if not data and isinstance(payload, dict):
            data = payload

        equity_value: Optional[float] = None
        for src in (data, data.get("data", {})):
            if not isinstance(src, dict):
                continue
            for key in (
                "totalBalance",
                "equity",
                "current_equity",
                "net_asset_value",
                "total_equity",
            ):
                if key in src and src[key] is not None:
                    try:
                        equity_value = float(src[key])
                        break
                    except (TypeError, ValueError):
                        continue
            if equity_value is not None:
                break

        if equity_value is None:
            print("WARNING: Could not parse equity from REST response.")
            print(
                "Using configured initial capital: "
                f"{self.initial_capital:,.0f} VND"
            )
            self.current_equity = self.initial_capital
            print("-" * 40 + "\n")
            return

        self.initial_capital = equity_value
        self.current_equity = equity_value
        print(f"Startup Equity:            {self.current_equity:,.0f} VND")
        print("-" * 40 + "\n")

    def _initialize_startup_position(self) -> None:
        """Fetch actual open positions from broker to prevent startup desync."""
        print("-" * 40)
        print("Position Synchronization Check")
        print("-" * 40)

        self._log_trade_api_capabilities_once()
        method_name, broker_trade_count = self._probe_broker_trade_count()
        if broker_trade_count is not None:
            print(
                f"Startup Broker Order Count: {broker_trade_count} "
                f"(via {method_name})"
            )
        else:
            print("Startup Broker Order Count: NOT AVAILABLE")

        def _instrument_matches(target_symbol: str, candidate_symbol: Any) -> bool:
            target = str(target_symbol or "").strip().upper()
            candidate = str(candidate_symbol or "").strip().upper()
            if not target or not candidate:
                return False

            # Handle broker prefixes like "HNXDS:VN30F2604".
            target_core = target.split(":")[-1]
            candidate_core = candidate.split(":")[-1]
            return (
                candidate == target
                or candidate_core == target_core
                or candidate_core.endswith(target_core)
                or target_core.endswith(candidate_core)
            )

        try:
            sub_scope = (
                self.client.use_sub_account(self.sub_account)
                if hasattr(self.client, "use_sub_account")
                else nullcontext()
            )
            with sub_scope:
                if hasattr(self.client, "get_portfolio_by_sub"):
                    payload = self.client.get_portfolio_by_sub(self.sub_account)
                elif hasattr(self.client, "get_positions"):
                    payload = self.client.get_positions()
                else:
                    print(
                        "WARNING: SDK missing position endpoints "
                        "(get_portfolio_by_sub/get_positions). Defaulting to Flat 0."
                    )
                    self.inventory = 0
                    self.avg_entry_price = None
                    print("-" * 40 + "\n")
                    return

            if isinstance(payload, dict) and payload.get("success") is False:
                raise ValueError(f"REST error: {payload.get('error')}")

            data = self._to_dict(payload)
            if not data and isinstance(payload, dict):
                data = payload

            positions: Any = []
            if isinstance(data, dict):
                if isinstance(data.get("items"), list):
                    positions = data.get("items", [])
                elif isinstance(data.get("holdings"), list):
                    positions = data.get("holdings", [])
                elif isinstance(data.get("data"), list):
                    positions = data.get("data", [])
                elif isinstance(data.get("data"), dict) and isinstance(data["data"].get("items"), list):
                    positions = data["data"].get("items", [])
                elif isinstance(data, list):
                    positions = data
            elif isinstance(data, list):
                positions = data

            found_position = False
            if isinstance(positions, list):
                for pos in positions:
                    if not isinstance(pos, dict):
                        continue

                    symbol = (
                        pos.get("symbol")
                        or pos.get("full_symbol")
                        or pos.get("instrument")
                        or pos.get("ticker")
                        or ""
                    )
                    if not _instrument_matches(self.symbol, symbol):
                        continue

                    try:
                        volume = int(
                            pos.get("quantity", pos.get("volume", pos.get("qty", 0))) or 0
                        )
                    except (TypeError, ValueError):
                        volume = 0

                    side = str(pos.get("side", "")).upper()
                    if side in {"SELL", "SHORT", "S", "2"}:
                        self.inventory = -abs(volume)
                    elif side in {"BUY", "LONG", "B", "1"}:
                        self.inventory = abs(volume)
                    else:
                        # Portfolio quantity is often signed already.
                        self.inventory = volume

                    avg_px_raw = pos.get(
                        "avgPrice",
                        pos.get("average_price", pos.get("avg_px", pos.get("entry_price", 0.0))),
                    )
                    try:
                        avg_px = float(avg_px_raw or 0.0)
                    except (TypeError, ValueError):
                        avg_px = 0.0

                    self.avg_entry_price = avg_px if avg_px > 0 else None
                    found_position = True

                    position_side = "Short" if self.inventory < 0 else "Long"
                    print("REST Position Sync:      SUCCESS")
                    print(
                        f"Recovered Inventory:     {position_side} {abs(self.inventory)}"
                    )
                    entry_str = (
                        f"{self.avg_entry_price:,.1f}"
                        if self.avg_entry_price is not None
                        else "N/A"
                    )
                    print(f"Recovered Entry Price:   {entry_str}")
                    break

            if not found_position:
                print("REST Position Sync:      SUCCESS (No open positions found)")
                self.inventory = 0
                self.avg_entry_price = None

        except Exception as exc:
            print("REST Position Sync:      FAILED")
            print(f"Error Details:           {exc}")
            print("WARNING: Assuming Flat 0. Live PnL may be desynchronized if positions exist.")
            self.inventory = 0
            self.avg_entry_price = None

        print("-" * 40 + "\n")

    def _log_trade_api_capabilities_once(self) -> None:
        if self._printed_trade_api_capabilities:
            return

        self._printed_trade_api_capabilities = True
        method_names = sorted(
            name
            for name in dir(self.client)
            if any(token in name.lower() for token in ("order", "trade", "execution"))
            and callable(getattr(self.client, name, None))
        )

        if method_names:
            print("SDK Trade/Order Methods: " + ", ".join(method_names))
        else:
            print("SDK Trade/Order Methods: NONE DISCOVERED")

    def _probe_broker_trade_count(self) -> Tuple[str, Optional[int]]:
        preferred_names = [
            "get_executions",
            "get_trades",
            "get_order_history",
            "get_orders",
            "list_executions",
            "list_trades",
            "list_orders",
        ]

        discovered = [
            name
            for name in dir(self.client)
            if any(token in name.lower() for token in ("trade", "execution", "order"))
            and callable(getattr(self.client, name, None))
        ]

        candidate_names: List[str] = []
        for name in preferred_names + sorted(discovered):
            if name not in candidate_names:
                candidate_names.append(name)

        for method_name in candidate_names:
            method = getattr(self.client, method_name, None)
            if method is None or not callable(method):
                continue

            for arg_mode in ("none", "sub_account"):
                try:
                    sub_scope = (
                        self.client.use_sub_account(self.sub_account)
                        if hasattr(self.client, "use_sub_account")
                        else nullcontext()
                    )
                    with sub_scope:
                        payload = method() if arg_mode == "none" else method(self.sub_account)
                except TypeError:
                    continue
                except Exception as exc:
                    print(f"Skipping {method_name}: {exc}")
                    continue

                count = self._extract_trade_count_from_payload(payload)
                if count is not None:
                    return method_name, count

        return "", None

    @staticmethod
    def _extract_trade_count_from_payload(payload: Any) -> Optional[int]:
        if payload is None:
            return None

        if isinstance(payload, (list, tuple, set)):
            return len(payload)

        if not isinstance(payload, dict):
            return None

        if payload.get("success") is False:
            return None

        for key in (
            "trades",
            "executions",
            "orders",
            "items",
            "results",
            "records",
            "data",
        ):
            value = payload.get(key)
            if isinstance(value, list):
                return len(value)
            if isinstance(value, dict):
                for nested_key in ("items", "trades", "executions", "orders", "records"):
                    nested_value = value.get(nested_key)
                    if isinstance(nested_value, list):
                        return len(nested_value)

        return None

    def _pull_equity_from_portfolio(self) -> Optional[float]:
        try:
            sub_scope = (
                self.client.use_sub_account(self.sub_account)
                if hasattr(self.client, "use_sub_account")
                else nullcontext()
            )
            with sub_scope:
                payload = self.client.get_account_balance()
            if isinstance(payload, dict) and payload.get("success") is False:
                return None
        except Exception:
            return None

        data = self._to_dict(payload)
        if not data and isinstance(payload, dict):
            data = payload

        for key in (
            "totalBalance",
            "equity",
            "current_equity",
            "net_asset_value",
            "total_equity",
        ):
            if key in data and data[key] is not None:
                try:
                    return float(data[key])
                except (TypeError, ValueError):
                    continue

        if isinstance(data.get("data"), dict):
            nested = data["data"]
            for key in (
                "totalBalance",
                "equity",
                "current_equity",
                "net_asset_value",
                "total_equity",
            ):
                if key in nested and nested[key] is not None:
                    try:
                        return float(nested[key])
                    except (TypeError, ValueError):
                        continue

        return None

    def _compute_internal_equity(self) -> float:
        unrealized = 0.0
        if self.inventory != 0 and self.avg_entry_price is not None and self.last_price is not None:
            if self.inventory > 0:
                unrealized = (
                    (self.last_price - self.avg_entry_price)
                    * abs(self.inventory)
                    * self.contract_multiplier
                )
            else:
                unrealized = (
                    (self.avg_entry_price - self.last_price)
                    * abs(self.inventory)
                    * self.contract_multiplier
                )

        return self.initial_capital + self.realized_pnl + unrealized

    def _evaluate_kill_switch(self) -> None:
        if self.trading_halted:
            return

        if self.current_equity >= self.kill_switch_equity:
            return

        self._write_log("CRITICAL: Global Kill Switch triggered. Equity below 400,000,000 VND.")

        self.trading_halted = True
        self._cancel_all_resting_orders()
        self._flatten_inventory_market(reason="kill_switch")

    def _evaluate_stop_loss(self) -> None:
        if (
            self.trading_halted
            or self.inventory == 0
            or self.avg_entry_price is None
            or self.last_price is None
        ):
            return

        with self._inflight_lock:
            is_flushing = any(
                str(order.get("tag", "")) in {
                    "reversal_flatten",
                    "stop_loss",
                    "kill_switch",
                    "risk_flatten",
                    "eod_liquidation",
                }
                for order in self.inflight_market_orders.values()
            )
        if is_flushing:
            return

        points_against = 0.0
        if self.inventory > 0:
            points_against = self.avg_entry_price - self.last_price
        elif self.inventory < 0:
            points_against = self.last_price - self.avg_entry_price

        if points_against < self.stop_loss_points:
            return

        stop_loss_msg = "\n".join(
            [
                "!" * 52,
                f"STOP LOSS TRIGGERED: Position down {points_against:.1f} pts",
                f"Flushing {abs(self.inventory)} contracts to prevent tail risk.",
                "!" * 52,
            ]
        )
        self._write_log(stop_loss_msg)

        self._cancel_all_resting_orders()
        side_to_close = "SELL" if self.inventory > 0 else "BUY"
        self._place_market_order(
            side=side_to_close,
            qty=abs(self.inventory),
            tag="stop_loss",
        )

    def _cancel_all_resting_orders(self) -> None:
        now = time.time()
        for side in ("BUY", "SELL"):
            order = self.active_orders.get(side)
            if order is None:
                continue

            # Prevent spamming cancel requests over the network.
            if now < self._cancel_retry_block_until.get(side, 0.0):
                continue

            self._cancel_order(order.cl_ord_id)
            # Block duplicate cancel requests while waiting for exchange ack.
            self._cancel_retry_block_until[side] = now + 2.0

    def _flatten_inventory_market(self, reason: str = "risk_flatten") -> None:
        if self.inventory == 0:
            return

        if self.inventory > 0:
            self._place_market_order(side="SELL", qty=abs(self.inventory), tag=reason)
        else:
            self._place_market_order(side="BUY", qty=abs(self.inventory), tag=reason)

    def _requires_flatten_before_reversal(self, side: str) -> bool:
        if side == "BUY" and self.inventory < 0:
            return True
        if side == "SELL" and self.inventory > 0:
            return True
        return False

    def _has_pending_reversal_flatten(self, side: str) -> bool:
        side = side.upper()
        with self._inflight_lock:
            for order in self.inflight_market_orders.values():
                if str(order.get("tag", "")) == "reversal_flatten" and str(order.get("side", "")) == side:
                    return True
        return False

    def _is_reversal_flatten_retry_blocked(self, side: str) -> Tuple[bool, str]:
        side = side.upper()
        now = time.time()
        state = self.reversal_flatten_state.get(side)
        if state is None:
            return False, ""

        if now < float(state.get("next_retry_ts", 0.0)):
            wait_seconds = int(max(1, state["next_retry_ts"] - now))
            return True, (
                f"Reversal flatten retry blocked for {side}. "
                f"Waiting {wait_seconds}s before next attempt."
            )

        return False, ""

    def _mark_reversal_flatten_failure(self, side: str, reason: str) -> None:
        side = side.upper()
        state = self.reversal_flatten_state.get(side)
        if state is None:
            return

        failures = int(state.get("failures", 0.0)) + 1
        state["failures"] = float(failures)

        if failures >= self.reversal_flatten_max_retries:
            cooldown = self.reversal_flatten_lockout_seconds
            print(
                f"Reversal flatten failed {failures} times for {side}. "
                f"Entering lockout for {cooldown:.0f}s. Last error: {reason}"
            )
        else:
            cooldown = self.reversal_flatten_cooldown_seconds
            print(
                f"Reversal flatten failed for {side}. "
                f"Retry in {cooldown:.0f}s. Error: {reason}"
            )

        state["next_retry_ts"] = time.time() + cooldown

    def _reset_reversal_flatten_state(self, side: str) -> None:
        side = side.upper()
        state = self.reversal_flatten_state.get(side)
        if state is None:
            return
        state["failures"] = 0.0
        state["next_retry_ts"] = 0.0
        self.reversal_flatten_escalation_ticks[side] = 0

    async def _recover_pending_orders_startup(self) -> None:
        if not self.startup_recovery_enabled:
            print("Startup pending-order recovery disabled by environment.")
            return

        recover_method = getattr(self.client, "recover_pending_orders", None)
        if recover_method is None:
            return

        try:
            recovered_payload = recover_method()
            if inspect.isawaitable(recovered_payload):
                recovered_payload = await recovered_payload
        except Exception as exc:
            print(f"Pending-order recovery failed: {exc}")
            return

        recovered_ids = self._extract_recovered_order_ids(recovered_payload)
        if not recovered_ids:
            print("No pending orders recovered from previous session.")
            return

        if self.startup_recovery_max_cancel_orders > 0 and (
            len(recovered_ids) > self.startup_recovery_max_cancel_orders
        ):
            print(
                "Recovered "
                f"{len(recovered_ids)} pending orders; limiting startup cancels to "
                f"{self.startup_recovery_max_cancel_orders}."
            )
            recovered_ids = recovered_ids[: self.startup_recovery_max_cancel_orders]

        print(
            f"Recovered {len(recovered_ids)} pending orders from previous session. "
            "Canceling before live loops start..."
        )
        timed_out_cancels = 0
        for cl_ord_id in recovered_ids:
            canceled = await self._cancel_order_startup_with_timeout(cl_ord_id)
            if not canceled:
                timed_out_cancels += 1

        if timed_out_cancels > 0:
            print(
                f"Startup recovery completed with {timed_out_cancels} cancel timeouts. "
                "Continuing live startup without waiting for remaining cancel acknowledgments."
            )

    async def _cancel_order_startup_with_timeout(self, cl_ord_id: str) -> bool:
        timeout = self.startup_recovery_cancel_timeout_seconds
        if timeout <= 0:
            self._cancel_order(cl_ord_id)
            return True

        try:
            # Run cancel in a worker thread so startup loop can move on when broker ack is slow.
            await asyncio.wait_for(asyncio.to_thread(self._cancel_order, cl_ord_id), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            print(
                f"Startup cancel timeout for {cl_ord_id} after {timeout:.2f}s; skipping."
            )
            return False

    @staticmethod
    def _extract_recovered_order_ids(payload: Any) -> List[str]:
        ids: List[str] = []

        if payload is None:
            return ids

        if isinstance(payload, str):
            return [payload]

        if isinstance(payload, (list, tuple, set)):
            for item in payload:
                if isinstance(item, str):
                    ids.append(item)
                elif isinstance(item, dict):
                    cl_ord_id = item.get("cl_ord_id") or item.get("ClOrdID") or item.get("id")
                    if cl_ord_id:
                        ids.append(str(cl_ord_id))
            return ids

        if isinstance(payload, dict):
            for key in ("cl_ord_id", "ClOrdID", "id"):
                value = payload.get(key)
                if value:
                    ids.append(str(value))

            for key in ("orders", "pending_orders", "items", "data"):
                nested = payload.get(key)
                if isinstance(nested, list):
                    for item in nested:
                        if isinstance(item, str):
                            ids.append(item)
                        elif isinstance(item, dict):
                            cl_ord_id = (
                                item.get("cl_ord_id")
                                or item.get("ClOrdID")
                                or item.get("id")
                            )
                            if cl_ord_id:
                                ids.append(str(cl_ord_id))

        # Keep order while removing duplicates.
        return list(dict.fromkeys(ids))

    async def _run_kafka_consumer(self) -> None:
        """Start Kafka consumer and keep lifecycle managed by the SDK."""
        print(f"Subscribing to market data for {self.symbol}...")
        try:
            await self.market_data_client.subscribe(self.symbol, self.on_quote)
            start_result = self.market_data_client.start()
            if inspect.isawaitable(start_result):
                await start_result
        except Exception as exc:
            print(f"Kafka consumer stopped with error: {exc}")
            self.trading_halted = True
            raise

    @staticmethod
    async def _call_client_method_async(client: Any, method_names: Tuple[str, ...]) -> None:
        for name in method_names:
            method = getattr(client, name, None)
            if method is None:
                continue
            result = method()
            if inspect.isawaitable(result):
                await result
            return

    @staticmethod
    def _normalize_side(side: Any) -> Optional[str]:
        if side is None:
            return None

        raw = str(side).strip().upper()
        if raw in {"BUY", "B", "1"}:
            return "BUY"
        if raw in {"SELL", "S", "2"}:
            return "SELL"
        return None

    def _normalize_price(self, value: float) -> float:
        if self.tick_size <= 0:
            return float(value)
        steps = round(float(value) / self.tick_size)
        return round(steps * self.tick_size, 6)

async def main() -> None:
    engine = LiveTradingEngine()
    await engine.start_async()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Keyboard interrupt received. Shutting down.")
