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
import inspect
import os
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, List, Optional, Tuple

import pandas as pd
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
        self.trading_halted = False

        self.fee_per_contract = 40_000.0  # 0.4 points * 100,000 multiplier

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

        self.prices: Deque[float] = deque(maxlen=100)
        self.min_points_for_signals = 60

        self.active_orders: Dict[str, Optional[ActiveOrder]] = {"BUY": None, "SELL": None}
        # Track market orders because they are not represented in active limit quotes.
        self.inflight_market_orders: Dict[str, Dict[str, Any]] = {}
        self._inflight_lock = threading.RLock()

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

        await self._recover_pending_orders_startup()

        dashboard_task = asyncio.create_task(self.print_dashboard_loop(), name="dashboard-loop")
        kafka_task = asyncio.create_task(self._run_kafka_consumer(), name="kafka-consumer")
        trading_task = asyncio.create_task(self._trading_logic_loop(), name="trading-logic")

        try:
            await asyncio.gather(kafka_task, dashboard_task, trading_task)
        finally:
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
            print(f"Ignored fill for untracked order: {cl_ord_id}")
            return

        fill_qty = int(last_qty)
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
        self.prices.append(latest_price)

    async def _trading_logic_loop(self) -> None:
        """Background task to calculate indicators and place orders."""
        last_processed_price = None

        while not self.trading_halted:
            await asyncio.sleep(0.1)

            if self.last_price is None or self.last_price == last_processed_price:
                continue

            last_processed_price = self.last_price

            self._refresh_equity(force_pull=False)
            self._evaluate_kill_switch()

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
            self.latest_indicators["dynamic_spread"] = indicators["atr"] * self.spread_multiplier

            bid_price, ask_price = self.market_maker.calculate_quotes(
                mid_price=self.last_price,
                current_inventory=self.inventory,
                rsi=indicators["rsi"],
                atr=indicators["atr"],
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

            print("\n" + "=" * 52)
            print("LiveTradingEngine Dashboard")
            print("=" * 52)
            print(f"Market Price:         {market_price}")
            print(f"Inventory:            {inventory_str}")
            print(f"Equity:               {self.current_equity:,.0f} VND")
            print(f"PnL:                  {pnl:,.0f} VND")
            print(f"Total Trades:         {self.total_trades_executed}")
            print(f"State Heals:          {self.state_heal_count}")
            print(f"RSI(14):              {rsi_str}")
            print(f"ADX(14):              {adx_str}")
            print(f"Dynamic Spread Width: {spread_str}")
            print("=" * 52)

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
        avg_gain = gain.rolling(window=14, min_periods=14).mean()
        avg_loss = loss.rolling(window=14, min_periods=14).mean()
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
        atr = true_range.rolling(window=14, min_periods=14).mean()

        up_move = high.diff()
        down_move = -low.diff()

        plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
        minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

        atr_smoothed = true_range.rolling(window=14, min_periods=14).sum()
        plus_di = 100 * (plus_dm.rolling(window=14, min_periods=14).sum() / atr_smoothed)
        minus_di = 100 * (minus_dm.rolling(window=14, min_periods=14).sum() / atr_smoothed)

        dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, pd.NA)) * 100
        adx = dx.rolling(window=14, min_periods=14).mean()

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

        if target_price is None:
            if existing is not None:
                canceled = self._cancel_order(existing.cl_ord_id)
                if canceled:
                    self.active_orders[side] = None
                else:
                    self._cancel_retry_block_until[side] = now + self.cancel_retry_backoff_seconds
            return

        if side == "BUY" and self.inventory >= self.max_inventory:
            return
        if side == "SELL" and self.inventory <= -self.max_inventory:
            return

        if self._requires_flatten_before_reversal(side):
            if self._has_pending_reversal_flatten(side):
                return

            retry_blocked, retry_reason = self._is_reversal_flatten_retry_blocked(side)
            if retry_blocked:
                if retry_reason:
                    print(retry_reason)
                return

            self._cancel_all_resting_orders()

            flatten_qty = abs(self.inventory)
            if flatten_qty > 0:
                current_side = "short" if self.inventory < 0 else "long"
                print(
                    f"Flattening {flatten_qty} {current_side} contracts before opening {side}."
                )
                cl_ord_id = self._place_market_order(
                    side=side,
                    qty=flatten_qty,
                    tag="reversal_flatten",
                )
                if not cl_ord_id:
                    self._mark_reversal_flatten_failure(
                        side,
                        "Market flatten placement failed",
                    )
            # Enforce two-step flip rule: flatten first, open new side only after fills update inventory.
            return

        normalized_price = self._normalize_price(target_price)

        if existing is not None and abs(existing.price - normalized_price) < (self.tick_size / 2):
            return

        if (
            existing is not None
            and self.order_modify_cooldown_seconds > 0
            and now - getattr(existing, "timestamp", 0.0) < self.order_modify_cooldown_seconds
        ):
            return

        if existing is not None:
            canceled = self._cancel_order(existing.cl_ord_id)
            if not canceled:
                self._cancel_retry_block_until[side] = now + self.cancel_retry_backoff_seconds
                return

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
        if qty <= 0:
            return ""

        try:
            cl_ord_id = self.client.place_order(
                full_symbol=self.symbol,
                side=side.upper(),
                qty=qty,
                price=0.0,
                ord_type="MARKET",
            )
            if cl_ord_id is not None:
                cl_ord_id_str = str(cl_ord_id)
                with self._inflight_lock:
                    self.inflight_market_orders[cl_ord_id_str] = {
                        "side": side.upper(),
                        "qty": qty,
                        "tag": tag,
                    }
                return cl_ord_id_str
            return ""
        except Exception as exc:
            print(f"Failed to place market order: {exc}")
            return ""

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
        portfolio_equity = self._pull_equity_from_portfolio()
        if portfolio_equity is None:
            print(
                "Startup portfolio equity unavailable. "
                f"Using configured initial capital: {self.initial_capital:,.0f} VND"
            )
            self.current_equity = self.initial_capital
            return

        self.initial_capital = float(portfolio_equity)
        self.current_equity = float(portfolio_equity)
        print(f"Initialized startup equity from portfolio: {self.current_equity:,.0f} VND")

    def _pull_equity_from_portfolio(self) -> Optional[float]:
        try:
            payload = self.client.get_portfolio_by_sub(sub_account_id=self.sub_account)
        except Exception:
            return None

        data = self._to_dict(payload)
        if not data and isinstance(payload, dict):
            data = payload

        for key in ("current_equity", "equity", "net_asset_value", "total_equity"):
            if key in data and data[key] is not None:
                try:
                    return float(data[key])
                except (TypeError, ValueError):
                    continue

        if isinstance(data.get("data"), dict):
            nested = data["data"]
            for key in ("current_equity", "equity", "net_asset_value", "total_equity"):
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

        print("CRITICAL: Global Kill Switch triggered. Equity below 400,000,000 VND.")

        self.trading_halted = True
        self._cancel_all_resting_orders()
        self._flatten_inventory_market()

    def _cancel_all_resting_orders(self) -> None:
        for side in ("BUY", "SELL"):
            order = self.active_orders.get(side)
            if order is None:
                continue
            self._cancel_order(order.cl_ord_id)
            self.active_orders[side] = None

    def _flatten_inventory_market(self) -> None:
        if self.inventory == 0:
            return

        if self.inventory > 0:
            self._place_market_order(side="SELL", qty=abs(self.inventory))
        else:
            self._place_market_order(side="BUY", qty=abs(self.inventory))

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
