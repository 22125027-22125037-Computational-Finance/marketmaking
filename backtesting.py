"""
This is main module for strategy backtesting
"""

import numpy as np
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Optional, Tuple
import pandas as pd
import matplotlib.pyplot as plt

from config.config import BACKTESTING_CONFIG, BEST_CONFIG
from metrics.metric import get_returns, Metric
from utils import get_expired_dates, from_cash_to_tradeable_contracts, round_decimal

FEE_PER_CONTRACT = Decimal(BACKTESTING_CONFIG["fee"]) * Decimal('100')


class PredictiveRSIMarketMaker:
    """
    Predictive market making engine with RSI and trend filters.

    Args:
        spread_multiplier (float): multiplier applied to ATR to derive quote spread.
    """

    def __init__(self, spread_multiplier: float):
        if spread_multiplier <= 0:
            raise ValueError("spread_multiplier must be > 0")

        self.spread_multiplier = float(spread_multiplier)

    def calculateQuotes(
        self,
        midPrice: float,
        currentInventory: int,
        rsi: float,
        atr: float,
        trend_signal: int,
        adx: float,
        dynamic_spread: Optional[float] = None,
        max_inventory: int = 5,
    ) -> Tuple[Optional[float], Optional[float]]:
        """
        Calculate bid/ask with predictive RSI + trend + inventory filters.
        """
        spread_value = (
            max(float(dynamic_spread), 0.0)
            if dynamic_spread is not None
            else max(float(atr) * self.spread_multiplier, 0.0)
        )

        if rsi < 30:
            bid_price = float(midPrice) - (spread_value * 0.5)
            ask_price = None
        elif rsi > 70:
            bid_price = None
            ask_price = float(midPrice) + (spread_value * 0.5)
        else:
            bid_price = float(midPrice) - spread_value
            ask_price = float(midPrice) + spread_value

        if currentInventory >= max_inventory:
            bid_price = None
        if currentInventory <= -max_inventory:
            ask_price = None

        # In strong trends, block RSI counter-trend extremes.
        if adx > 25:
            if trend_signal == 1 and rsi > 70:
                ask_price = None
            elif trend_signal == -1 and rsi < 30:
                bid_price = None

        return bid_price, ask_price

    def calculate_quotes(
        self,
        mid_price: float,
        current_inventory: int,
        rsi: float,
        atr: float,
        trend_signal: int,
        adx: float,
        dynamic_spread: Optional[float] = None,
        max_inventory: int = 5,
    ) -> Tuple[Optional[float], Optional[float]]:
        """Snake-case alias for calculateQuotes."""
        return self.calculateQuotes(
            midPrice=mid_price,
            currentInventory=current_inventory,
            rsi=rsi,
            atr=atr,
            dynamic_spread=dynamic_spread,
            trend_signal=trend_signal,
            adx=adx,
            max_inventory=max_inventory,
        )


class Backtesting:
    """
    Backtesting main class
    """

    def __init__(
        self,
        capital: Decimal,
        printable=True,
        market_maker: Optional[PredictiveRSIMarketMaker] = None,
    ):
        """
        Initiate required data

        Args:
            buy_fee (Decimal)
            sell_fee (Decimal)
            from_date_str (str)
            to_date_str (str)
            capital (Decimal)
            path (str, optional). Defaults to "data/is/pe_dps.csv".
            index_path (str, optional). Defaults to "data/is/vnindex.csv".
        """
        self.printable = printable
        self.metric = None

        self.inventory = 0
        self.inventory_price = Decimal('0')

        self.daily_assets: List[Decimal] = [capital]
        self.daily_returns: List[Decimal] = []
        self.tracking_dates = []
        self.daily_inventory = []
        self.monthly_tracking = []

        self.old_timestamp = None
        self.bid_price = None
        self.ask_price = None
        self.ac_loss = Decimal("0.0")
        self.transactions = []
        self.order_logs = []
        self.market_maker = market_maker
        self.session_start_timestamp = None
        self.max_inventory = 5
        self.stop_loss_threshold = Decimal('5.0')
        self.kill_switch_threshold = Decimal('4e8')
        self.trading_halted = False
        self.total_trades = 0

    def move_f1_to_f2(self, f1_price, f2_price):
        """
        TODO: move f1 to f2
        """
        if self.inventory > 0:
            self.ac_loss += (self.inventory_price - f1_price) * 100
            self.inventory_price = f2_price
            self.ac_loss += FEE_PER_CONTRACT * abs(self.inventory)
        elif self.inventory < 0:
            self.ac_loss += (f1_price - self.inventory_price) * 100
            self.inventory_price = f2_price
            self.ac_loss += FEE_PER_CONTRACT * abs(self.inventory)

    def update_pnl(self, close_price: Decimal):
        """
        Daily update pnl

        Args:
            close_price (Decimal)
        """
        cur_asset = self.daily_assets[-1]
        new_asset = None
        if self.inventory == 0:
            new_asset = cur_asset - self.ac_loss
        else:
            sign = 1 if self.inventory > 0 else -1
            pnl = (
                sign * abs(self.inventory) * (close_price - self.inventory_price) * 100
                - self.ac_loss
            )
            new_asset = cur_asset + pnl
            self.inventory_price = close_price

        self.daily_returns.append(new_asset / self.daily_assets[-1] - 1)
        self.daily_assets.append(new_asset)

    def final_exit(self, close_price: Decimal):
        """
        Force-liquidate all remaining inventory at end of day.

        Args:
            close_price (Decimal): End-of-day closing price used for liquidation.
        """
        if self.inventory == 0:
            return

        contracts = abs(self.inventory)
        sign = 1 if self.inventory > 0 else -1
        realized_per_contract = sign * (close_price - self.inventory_price) * Decimal('100')

        # ac_loss tracks costs net of realized trading gains.
        self.ac_loss += FEE_PER_CONTRACT * contracts - realized_per_contract * contracts
        self.total_trades += contracts
        self.inventory = 0
        self.inventory_price = Decimal('0')

    def handle_force_sell(self, price: Decimal):
        """
        Handle force sell

        Args:
            price (Decimal): _description_
        """
        while self.get_maximum_placeable(price) < 0:
            sign = 1 if self.inventory < 0 else -1
            self.inventory += sign
            self.ac_loss += abs(price - self.inventory_price) * 100 + FEE_PER_CONTRACT
            self.total_trades += 1

    def evaluate_stop_loss(self, price: Decimal):
        """
        Force-flush inventory when adverse move exceeds stop-loss threshold.

        Args:
            price (Decimal): Current instrument price.
        """
        if self.inventory > 0 and (self.inventory_price - price) >= self.stop_loss_threshold:
            self.ac_loss += (
                (self.inventory_price - price) * 100 * abs(self.inventory)
                + (FEE_PER_CONTRACT * abs(self.inventory))
            )
            self.total_trades += abs(self.inventory)
            self.inventory = 0
            self.inventory_price = Decimal('0')

        elif self.inventory < 0 and (price - self.inventory_price) >= self.stop_loss_threshold:
            self.ac_loss += (
                (price - self.inventory_price) * 100 * abs(self.inventory)
                + (FEE_PER_CONTRACT * abs(self.inventory))
            )
            self.total_trades += abs(self.inventory)
            self.inventory = 0
            self.inventory_price = Decimal('0')

    def evaluate_kill_switch(self, current_price: Decimal):
        """
        Trigger catastrophic circuit breaker when equity breaches threshold.

        Args:
            current_price (Decimal): Current instrument price for emergency liquidation.
        """
        if self.daily_assets[-1] < self.kill_switch_threshold and not self.trading_halted:
            print(
                "CRITICAL: Kill switch triggered. Equity dropped below 400M VND. Halting all trading."
            )
            self.final_exit(current_price)
            self.trading_halted = True

    def get_maximum_placeable(self, inst_price: Decimal):
        """
        Get maximum placeable

        Args:
            inst_price (Decimal): _description_

        Returns:
            _type_: _description_
        """
        total_placeable = max(
            from_cash_to_tradeable_contracts(
                self.daily_assets[-1] - self.ac_loss, inst_price
            ),
            0,
        )
        return total_placeable - abs(self.inventory)

    def handle_matched_order(self, price):
        """
        Handle matched order

        Args:
            price (_type_): _description_
        """
        matched = 0
        placeable = self.get_maximum_placeable(price)
        if self.bid_price is None and self.ask_price is None:
            return matched

        if (
            self.bid_price is not None
            and self.bid_price > price
            and self.inventory >= 0
            and placeable > 0
        ):
            self.inventory_price = (
                self.inventory_price * abs(self.inventory) + price
            ) / (abs(self.inventory) + 1)
            self.inventory += 1
            matched += 1
            self.total_trades += 1
        elif self.bid_price is not None and self.bid_price > price and self.inventory < 0:
            self.ac_loss += (FEE_PER_CONTRACT - (self.inventory_price - price) * Decimal('100'))
            self.inventory += 1
            if self.inventory == 0:
                self.inventory_price = Decimal('0')
            matched -= 1
            self.total_trades += 1

        if (
            self.ask_price is not None
            and self.ask_price < price
            and self.inventory <= 0
            and placeable > 0
        ):
            self.inventory_price = (
                self.inventory_price * abs(self.inventory) + price
            ) / (abs(self.inventory) + 1)
            self.inventory -= 1
            matched += 1
            self.total_trades += 1
        elif self.ask_price is not None and self.ask_price < price and self.inventory > 0:
            self.ac_loss += (FEE_PER_CONTRACT - (price - self.inventory_price) * Decimal('100'))
            self.inventory -= 1
            if self.inventory == 0:
                self.inventory_price = Decimal('0')
            matched -= 1
            self.total_trades += 1

        return matched

    def update_bid_ask(self, price: Decimal, step, timestamp, rsi, atr, adx, trend_signal=0):
        """
        Placing bid ask formula

        Args:
            price (Decimal)
        """
        matched = self.handle_matched_order(price)

        if self.old_timestamp is None or timestamp > self.old_timestamp + timedelta(
            seconds=int(BACKTESTING_CONFIG["time"])
        ):
            self.old_timestamp = timestamp
            if self.market_maker is not None:
                bid, ask = self.market_maker.calculateQuotes(
                    midPrice=float(price),
                    currentInventory=self.inventory,
                    rsi=float(rsi),
                    atr=float(atr),
                    trend_signal=trend_signal,
                    adx=float(adx),
                    max_inventory=self.max_inventory,
                )
                self.bid_price = (
                    Decimal(str(bid)).quantize(Decimal("0.0"), rounding=ROUND_HALF_UP)
                    if bid is not None
                    else None
                )
                self.ask_price = (
                    Decimal(str(ask)).quantize(Decimal("0.0"), rounding=ROUND_HALF_UP)
                    if ask is not None
                    else None
                )
            else:
                if step is None:
                    raise ValueError("step is required when market_maker is not provided")
                self.bid_price = (
                    price - step * Decimal(max(self.inventory, 0) * 0.02 + 1)
                ).quantize(Decimal("0.0"), rounding=ROUND_HALF_UP)
                self.ask_price = (
                    price - step * Decimal(min(self.inventory, 0) * 0.02 - 1)
                ).quantize(Decimal("0.0"), rounding=ROUND_HALF_UP)
        elif matched != 0:
            if self.market_maker is not None:
                bid, ask = self.market_maker.calculateQuotes(
                    midPrice=float(price),
                    currentInventory=self.inventory,
                    rsi=float(rsi),
                    atr=float(atr),
                    trend_signal=trend_signal,
                    adx=float(adx),
                    max_inventory=self.max_inventory,
                )
                self.bid_price = (
                    Decimal(str(bid)).quantize(Decimal("0.0"), rounding=ROUND_HALF_UP)
                    if bid is not None
                    else None
                )
                self.ask_price = (
                    Decimal(str(ask)).quantize(Decimal("0.0"), rounding=ROUND_HALF_UP)
                    if ask is not None
                    else None
                )
            else:
                if step is None:
                    raise ValueError("step is required when market_maker is not provided")
                self.bid_price = (
                    price - step * Decimal(max(self.inventory, 0) * 0.02 + 1)
                ).quantize(Decimal("0.0"), rounding=ROUND_HALF_UP)
                self.ask_price = (
                    price - step * Decimal(min(self.inventory, 0) * 0.02 - 1)
                ).quantize(Decimal("0.0"), rounding=ROUND_HALF_UP)

    @staticmethod
    def process_data(evaluation=False):
        prefix_path = "data/os/" if evaluation else "data/is/"
        f1_data = pd.read_csv(f"{prefix_path}VN30F1M_data.csv")
        f1_data["datetime"] = pd.to_datetime(
            f1_data["datetime"], format="%Y-%m-%d %H:%M:%S.%f"
        )
        f1_data["date"] = (
            pd.to_datetime(f1_data["date"], format="%Y-%m-%d").copy().dt.date
        )
        rounding_columns = ["close", "price", "best-bid", "best-ask", "spread"]
        for col in rounding_columns:
            f1_data = round_decimal(f1_data, col)

        f2_data = pd.read_csv(f"{prefix_path}VN30F2M_data.csv")
        f2_data = f2_data[["date", "datetime", "tickersymbol", "price", "close"]].copy()
        f2_data["datetime"] = pd.to_datetime(
            f2_data["datetime"], format="%Y-%m-%d %H:%M:%S.%f"
        )
        f2_data["date"] = (
            pd.to_datetime(f2_data["date"], format="%Y-%m-%d").copy().dt.date
        )
        f2_data.rename(
            columns={
                "price": "f2_price",
                "close": "f2_close",
                "tickersymbol": "f2-tickersymbol",
            },
            inplace=True,
        )
        rounding_columns = ["f2_close", "f2_price"]
        for col in rounding_columns:
            f2_data = round_decimal(f2_data, col)

        f1_data = pd.merge(
            f1_data,
            f2_data,
            on=["datetime", "date"],
            how="outer",
            sort=True,
        )
        f1_data = f1_data.ffill()
        f1_data["rolling_sigma"] = f1_data["price"].rolling(window=60).std()
        f1_data["rolling_sigma"] = f1_data["rolling_sigma"].ffill().fillna(15.0)
        f1_data["ema_fast"] = f1_data["price"].ewm(span=12, adjust=False).mean()
        f1_data["ema_slow"] = f1_data["price"].ewm(span=26, adjust=False).mean()

        delta = f1_data["price"].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        f1_data["rsi"] = 100 - (100 / (1 + rs))
        f1_data["rsi"] = f1_data["rsi"].fillna(50)

        high = f1_data["best-ask"].astype(float)
        low = f1_data["best-bid"].astype(float)
        close = f1_data["close"].astype(float)

        up_move = high.diff()
        down_move = low.shift(1) - low

        plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

        tr_components = pd.concat(
            [
                (high - low).abs(),
                (high - close.shift(1)).abs(),
                (low - close.shift(1)).abs(),
            ],
            axis=1,
        )
        true_range = tr_components.max(axis=1)
        atr = true_range.rolling(window=14).mean()
        f1_data["atr"] = atr.ffill().fillna(0.0)

        plus_di = 100 * (pd.Series(plus_dm, index=f1_data.index).rolling(window=14).mean() / atr)
        minus_di = 100 * (pd.Series(minus_dm, index=f1_data.index).rolling(window=14).mean() / atr)
        di_sum = (plus_di + minus_di).replace(0, np.nan)
        dx = ((plus_di - minus_di).abs() / di_sum) * 100
        f1_data["adx"] = dx.rolling(window=14).mean().fillna(0.0)

        return f1_data

    def run(self, data: pd.DataFrame, step: Optional[Decimal] = None):
        """
        Main backtesting function
        """

        trading_dates = data["date"].unique().tolist()

        start_date = data["datetime"].iloc[0]
        end_date = data["datetime"].iloc[-1]
        expiration_dates = get_expired_dates(start_date, end_date)

        cur_index = 0
        moving_to_f2 = False
        for index, row in data.iterrows():
            self.cur_date = row["datetime"]
            self.ticker = row["tickersymbol"]
            rsi_value = row["rsi"]
            adx_value = row["adx"]
            atr_value = row["atr"]
            ema_fast = row["ema_fast"]
            ema_slow = row["ema_slow"]
            if ema_fast < ema_slow - 2.0:
                trend_signal = -1
            elif ema_fast > ema_slow + 2.0:
                trend_signal = 1
            else:
                trend_signal = 0
            if (
                cur_index != len(trading_dates) - 1
                and not expiration_dates.empty()
                and trading_dates[cur_index + 1] >= expiration_dates.queue[0]
            ):
                self.move_f1_to_f2(row["price"], row["f2_price"])
                expiration_dates.get()
                moving_to_f2 = True

            current_price = row["f2_price"] if moving_to_f2 else row["price"]
            self.handle_force_sell(current_price)
            if not self.trading_halted:
                self.evaluate_stop_loss(current_price)
                self.evaluate_kill_switch(current_price)
                if not self.trading_halted:
                    self.update_bid_ask(
                        current_price,
                        step,
                        row["datetime"],
                        rsi_value,
                        atr_value,
                        adx_value,
                        trend_signal,
                    )

            if index == len(data) - 1 or row["date"] != data.iloc[index + 1]["date"]:
                cur_index += 1
                close_price = row["f2_close"] if moving_to_f2 else row["close"]
                self.final_exit(close_price)
                self.update_pnl(close_price)
                if self.printable:
                    print(
                        f"Realized asset {row['date']}: {int(self.daily_assets[-1] * Decimal('1000'))} VND"
                    )
                if moving_to_f2:
                    self.monthly_tracking.append([row["date"], self.daily_assets[-1]])

                moving_to_f2 = False
                self.ac_loss = Decimal("0.0")
                self.bid_price = None
                self.ask_price = None
                self.old_timestamp = None
                self.session_start_timestamp = None

                self.tracking_dates.append(row["date"])
                self.daily_inventory.append(self.inventory)

        self.metric = Metric(self.daily_returns, None)

    def plot_hpr(self, path="result/backtest/hpr.svg"):
        """
        Plot and save NAV chart to path

        Args:
            path (str, optional): _description_. Defaults to "result/backtest/hpr.svg".
        """
        plt.figure(figsize=(10, 6))

        assets = pd.Series(self.daily_assets)
        ac_return = assets.apply(lambda x: x / assets.iloc[0])
        ac_return = [(val - 1) * 100 for val in ac_return.to_numpy()[1:]]
        plt.plot(
            self.tracking_dates,
            ac_return,
            label="Portfolio",
            color='black',
        )

        plt.title('Holding Period Return Over Time')
        plt.xlabel('Time Step')
        plt.ylabel('Holding Period Return (%)')
        plt.grid(True)
        plt.legend()
        plt.savefig(path, dpi=300, bbox_inches='tight')

    def plot_drawdown(self, path="result/backtest/drawdown.svg"):
        """
        Plot and save drawdown chart to path

        Args:
            path (str, optional): _description_. Defaults to "result/backtest/drawdown.svg".
        """
        _, drawdowns = self.metric.maximum_drawdown()

        plt.figure(figsize=(10, 6))
        plt.plot(
            self.tracking_dates,
            drawdowns,
            label="Portfolio",
            color='black',
        )

        plt.title('Draw down Value Over Time')
        plt.xlabel('Time Step')
        plt.ylabel('Percentage')
        plt.grid(True)
        plt.savefig(path, dpi=300, bbox_inches='tight')

    def plot_inventory(self, path="result/backtest/inventory.svg"):
        plt.figure(figsize=(10, 6))
        plt.plot(
            self.tracking_dates,
            self.daily_inventory,
            label="Portfolio",
            color='black',
        )

        plt.title('Inventory Value Over Time')
        plt.xlabel('Time Step')
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(path, dpi=300, bbox_inches='tight')


if __name__ == "__main__":
    spread_multiplier = float(BEST_CONFIG.get("spread_multiplier", BEST_CONFIG.get("spread", 0.2)))
    stop_loss = Decimal(str(BEST_CONFIG.get("stop_loss", "5.0")))
    max_inv = int(BEST_CONFIG.get("max_inv", 5))

    market_maker = PredictiveRSIMarketMaker(spread_multiplier=spread_multiplier)

    bt = Backtesting(
        capital=Decimal("5e5"),
        market_maker=market_maker,
    )
    bt.stop_loss_threshold = stop_loss
    bt.max_inventory = max_inv

    data = bt.process_data()
    bt.run(data)

    print(
        f"Sharpe ratio: {bt.metric.sharpe_ratio(risk_free_return=Decimal('0.00023')) * Decimal(np.sqrt(250))}"
    )
    print(
        f"Sortino ratio: {bt.metric.sortino_ratio(risk_free_return=Decimal('0.00023')) * Decimal(np.sqrt(250))}"
    )
    mdd, _ = bt.metric.maximum_drawdown()
    print(f"Maximum drawdown: {mdd}")

    monthly_df = pd.DataFrame(bt.monthly_tracking, columns=["date", "asset"])
    returns = get_returns(monthly_df)

    print(f"HPR {bt.metric.hpr()}")
    print(f"Monthly return {returns['monthly_return']}")
    print(f"Annual return {returns['annual_return']}")
    print(f"Total trades: {bt.total_trades}")

    bt.plot_hpr()
    bt.plot_drawdown()
    bt.plot_inventory()
