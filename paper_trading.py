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
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, Optional, Tuple

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


class LiveTradingEngine:
    """Live paper trading orchestrator.

    The engine consumes tick updates, computes indicators on a rolling window,
    asks PredictiveRSIMarketMaker for quotes, and manages active FIX orders.
    It also enforces a global kill switch to flatten and halt trading.
    """

    def __init__(self) -> None:
        load_dotenv()

        self.username = self._require_env_any("PAPERBROKER_USERNAME", "PAPER_USERNAME")
        self.password = self._require_env_any("PAPERBROKER_PASSWORD", "PAPER_PASSWORD")
        self.sub_account = self._require_env_any("PAPERBROKER_SUB_ACCOUNT", "PAPER_ACCOUNT_ID_D1")
        self.socket_connect_host = self._require_env_any(
            "PAPERBROKER_SOCKET_CONNECT_HOST", "SOCKET_HOST"
        )
        self.socket_connect_port = int(
            self._require_env_any("PAPERBROKER_SOCKET_CONNECT_PORT", "SOCKET_PORT")
        )
        self.sender_comp_id = self._require_env_any("PAPERBROKER_SENDER_COMP_ID", "SENDER_COMP_ID")

        self.symbol = self._get_env_any("PAPERBROKER_SYMBOL", "TARGET_SYMBOL") or "VN30F1M"
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
        self.latest_indicators: Dict[str, Optional[float]] = {
            "rsi": None,
            "adx": None,
            "atr": None,
            "dynamic_spread": None,
        }

        self.prices: Deque[float] = deque(maxlen=100)
        self.min_points_for_signals = 60

        self.active_orders: Dict[str, ActiveOrder] = {"BUY": None, "SELL": None}

        self.portfolio_refresh_seconds = float(
            os.getenv("PAPERBROKER_PORTFOLIO_REFRESH_SECONDS", "3")
        )
        self._last_portfolio_pull = 0.0

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
        kwargs = {
            "username": self.username,
            "password": self.password,
            "default_sub_account": self.sub_account,
            "socket_connect_host": self.socket_connect_host,
            "socket_connect_port": self.socket_connect_port,
            "sender_comp_id": self.sender_comp_id,
            "rest_base_url": os.getenv("PAPER_REST_BASE_URL", "https://papertrade.algotrade.vn/accounting"),
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
        if kafka_username:
            kwargs["username"] = kafka_username
        if kafka_password:
            kwargs["password"] = kafka_password
        return KafkaMarketDataClient(**kwargs)

    def _register_fix_events(self) -> None:
        self.client.on("fix:logon", self.on_logon)
        self.client.on("fix:logout", self.on_logout)
        self.client.on("fix:order:filled", self.on_order_filled)
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

        dashboard_task = asyncio.create_task(self.print_dashboard_loop(), name="dashboard-loop")
        kafka_task = asyncio.create_task(self._run_kafka_consumer(), name="kafka-consumer")
        trading_task = asyncio.create_task(self._trading_logic_loop(), name="trading-logic")

        try:
            await asyncio.gather(kafka_task, dashboard_task, trading_task)
        finally:
            self.stop()

    def stop(self) -> None:
        print("Stopping LiveTradingEngine...")

        if hasattr(self.market_data_client, "stop"):
            self.market_data_client.stop()
        if hasattr(self.client, "disconnect"):
            self.client.disconnect()
        elif hasattr(self.client, "stop"):
            self.client.stop()

    def on_logon(self, event: Any) -> None:
        _ = event
        print("FIX logon successful.")

    def on_logout(self, event: Any) -> None:
        _ = event
        print("FIX logout received.")

    def on_order_canceled(self, event: Any) -> None:
        payload = self._to_dict(event)
        cl_ord_id = str(payload.get("cl_ord_id") or payload.get("clOrdID") or "")
        side = self._normalize_side(payload.get("side"))

        if side and self.active_orders.get(side) and self.active_orders[side].cl_ord_id == cl_ord_id:
            self.active_orders[side] = None

    def on_order_rejected(self, event: Any) -> None:
        payload = self._to_dict(event)
        side = self._normalize_side(payload.get("side"))
        cl_ord_id = str(payload.get("cl_ord_id") or payload.get("clOrdID") or "")
        reason = payload.get("reason") or payload.get("text") or "unknown"

        print(f"Order rejected: side={side}, cl_ord_id={cl_ord_id}, reason={reason}")

        if side and self.active_orders.get(side) and self.active_orders[side].cl_ord_id == cl_ord_id:
            self.active_orders[side] = None

    def on_order_filled(self, event: Any) -> None:
        payload = self._to_dict(event)

        side = self._normalize_side(payload.get("side"))
        qty = int(payload.get("filled_qty") or payload.get("last_qty") or payload.get("qty") or 0)
        fill_price_raw = payload.get("fill_price") or payload.get("last_px") or payload.get("price")

        if side is None or qty <= 0 or fill_price_raw is None:
            print(f"Ignored malformed fill event: {payload}")
            return

        fill_price = float(fill_price_raw)
        self._apply_fill(side=side, qty=qty, price=fill_price)
        self.total_trades_executed += qty

        cl_ord_id = str(payload.get("cl_ord_id") or payload.get("clOrdID") or "")
        if side and self.active_orders.get(side) and self.active_orders[side].cl_ord_id == cl_ord_id:
            self.active_orders[side] = None

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

        existing = self.active_orders.get(side)

        if target_price is None:
            if existing is not None:
                self._cancel_order(existing.cl_ord_id)
                self.active_orders[side] = None
            return

        if side == "BUY" and self.inventory >= self.max_inventory:
            return
        if side == "SELL" and self.inventory <= -self.max_inventory:
            return

        normalized_price = self._normalize_price(target_price)

        if existing is not None and abs(existing.price - normalized_price) < (self.tick_size / 2):
            return

        if existing is not None:
            self._cancel_order(existing.cl_ord_id)

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
            )

    def _cancel_order(self, cl_ord_id: str) -> None:
        try:
            self.client.cancel_order(cl_ord_id=cl_ord_id)
        except TypeError:
            self.client.cancel_order(cl_ord_id)
        except Exception as exc:  # pragma: no cover - runtime broker error
            print(f"Cancel order failed for {cl_ord_id}: {exc}")

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

    def _place_market_order(self, side: str, qty: int) -> None:
        if qty <= 0:
            return

        try:
            self.client.place_order(
                full_symbol=self.symbol,
                side=side.upper(),
                qty=qty,
                price=0.0,
                ord_type="MARKET",
            )
        except Exception as exc:
            print(f"Failed to place market order: {exc}")

    def _apply_fill(self, side: str, qty: int, price: float) -> None:
        signed_qty = qty if side == "BUY" else -qty
        
        # Deduct exchange fees immediately upon any fill
        self.realized_pnl -= (qty * self.fee_per_contract)

        if self.inventory == 0:
            self.inventory = signed_qty
            self.avg_entry_price = price
            return

        if self.inventory > 0 and signed_qty > 0:
            self.avg_entry_price = (
                (self.avg_entry_price * self.inventory) + (price * signed_qty)
            ) / (self.inventory + signed_qty)
            self.inventory += signed_qty
            return

        if self.inventory < 0 and signed_qty < 0:
            abs_inventory = abs(self.inventory)
            abs_new = abs(signed_qty)
            self.avg_entry_price = (
                (self.avg_entry_price * abs_inventory) + (price * abs_new)
            ) / (abs_inventory + abs_new)
            self.inventory += signed_qty
            return

        # Closing/reversing position.
        if self.inventory > 0 and signed_qty < 0:
            closing_qty = min(self.inventory, abs(signed_qty))
            self.realized_pnl += (price - self.avg_entry_price) * closing_qty * self.contract_multiplier
            self.inventory -= closing_qty
            remainder = abs(signed_qty) - closing_qty
            if remainder > 0:
                self.inventory = -remainder
                self.avg_entry_price = price
            elif self.inventory == 0:
                self.avg_entry_price = None
            return

        if self.inventory < 0 and signed_qty > 0:
            closing_qty = min(abs(self.inventory), signed_qty)
            self.realized_pnl += (self.avg_entry_price - price) * closing_qty * self.contract_multiplier
            self.inventory += closing_qty
            remainder = signed_qty - closing_qty
            if remainder > 0:
                self.inventory = remainder
                self.avg_entry_price = price
            elif self.inventory == 0:
                self.avg_entry_price = None
            return

    def _refresh_equity(self, force_pull: bool) -> None:
        now = time.time()
        should_pull = force_pull or (now - self._last_portfolio_pull >= self.portfolio_refresh_seconds)

        if should_pull:
            portfolio_equity = self._pull_equity_from_portfolio()
            self._last_portfolio_pull = now
            if portfolio_equity is not None:
                self.current_equity = portfolio_equity
                return

        self.current_equity = self._compute_internal_equity()

    def _pull_equity_from_portfolio(self) -> Optional[float]:
        try:
            payload = self.client.get_portfolio_by_sub(sub_account=self.sub_account)
        except TypeError:
            try:
                payload = self.client.get_portfolio_by_sub(self.sub_account)
            except Exception:
                return None
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

    async def _run_kafka_consumer(self) -> None:
            """Start Kafka consumer and subscribe to the instrument."""
            print(f"Subscribing to market data for {self.symbol}...")
            await self.market_data_client.subscribe(self.symbol, self.on_quote)
            await self.market_data_client.start()

            while not self.trading_halted:
                await asyncio.sleep(1)

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
