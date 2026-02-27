"""
This is main module for strategy backtesting

Strategy: Market-Making on VN30F1 (Trading_Hypothesis.md)
- Range-bound: PriceRange (last 20 ticks high-low) < 0.2% of FairPrice.
- Quote: Limit Buy at FairPrice - 0.5, Limit Sell at FairPrice + 0.5 (2 contracts per side).
- Cancel quotes when PriceRange >= 0.2%.
- Stop-loss: close entire inventory if adverse move >= 2 points per contract.
- Time exit: no overnight; close by ATC at end of day.
"""

import numpy as np
from collections import deque
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import List
import pandas as pd
import matplotlib.pyplot as plt

from config.config import BACKTESTING_CONFIG
from metrics.metric import get_returns, Metric
from utils import get_expired_dates, from_cash_to_tradeable_contracts, round_decimal

FEE_PER_CONTRACT = Decimal(BACKTESTING_CONFIG["fee"]) * Decimal('100')

# Market-making parameters (Trading_Hypothesis.md)
LOOKBACK_TICKS = 20
RANGE_PCT = Decimal("0.002")   # 0.2% — range-bound if PriceRange < this × FairPrice
SPREAD_HALF = Decimal("0.5")   # Quote at FairPrice ± 0.5 points
CONTRACTS_PER_SIDE = 2
STOP_LOSS_POINTS = Decimal("2")


class Backtesting:
    """
    Backtesting main class
    """

    def __init__(self, capital: Decimal, printable=True):
        self.printable = printable
        self.metric = None

        self.inventory = 0
        self.inventory_price = Decimal('0')

        self.daily_assets: List[Decimal] = [capital]
        self.daily_returns: List[Decimal] = []
        self.tracking_dates = []
        self.daily_inventory = []
        self.monthly_tracking = []

        # FIX: Added missing attributes
        self.trade_results = [] # <--- This fixes your AttributeError
        self.cur_date = None
        self.ticker = None
        
        self.old_timestamp = None
        self.bid_price = None
        self.ask_price = None
        self.ac_loss = Decimal("0.0")
        self.transactions = []
        self.order_logs = []

        self.price_window: deque = deque(maxlen=LOOKBACK_TICKS)
        self.daily_realized_pnl = Decimal("0.0")

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
        Daily update pnl (includes intraday realized PnL e.g. stop-loss).
        """
        cur_asset = self.daily_assets[-1]
        if self.inventory == 0:
            new_asset = cur_asset + self.daily_realized_pnl - self.ac_loss
        else:
            sign = 1 if self.inventory > 0 else -1
            pnl = (
                sign * abs(self.inventory) * (close_price - self.inventory_price) * 100
                - self.ac_loss
            )
            new_asset = cur_asset + self.daily_realized_pnl + pnl
            self.inventory_price = close_price

        self.daily_returns.append(new_asset / self.daily_assets[-1] - 1)
        self.daily_assets.append(new_asset)

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

    def get_maximum_placeable(self, inst_price: Decimal):
        total_placeable = max(
            from_cash_to_tradeable_contracts(
                self.daily_assets[-1] - self.ac_loss, inst_price
            ),
            0,
        )
        return total_placeable - abs(self.inventory)

    def handle_matched_order(self, price: Decimal):
        matched = 0
        order_size = 2 # Trade 02 contracts per side 
        placeable = self.get_maximum_placeable(price)

        if self.bid_price is None or self.ask_price is None:
            return matched

        # Khớp lệnh MUA
        if self.bid_price >= price and self.inventory >= 0 and placeable >= order_size:
            self.inventory_price = (self.inventory_price * abs(self.inventory) + price * order_size) / (abs(self.inventory) + order_size)
            self.inventory += order_size
            matched += order_size
        elif self.bid_price >= price and self.inventory < 0:
            # Mua để đóng vị thế Bán (Cover Short)
            trade_pnl = self.inventory_price - price
            self.trade_results.append(1 if trade_pnl > 0 else 0)
            self.ac_loss += (FEE_PER_CONTRACT * order_size - trade_pnl * Decimal('100') * order_size)
            self.inventory += order_size
            matched -= order_size

        # Khớp lệnh BÁN
        if self.ask_price <= price and self.inventory <= 0 and placeable >= order_size:
            self.inventory_price = (self.inventory_price * abs(self.inventory) + price * order_size) / (abs(self.inventory) + order_size)
            self.inventory -= order_size
            matched += order_size
        elif self.ask_price <= price and self.inventory > 0:
            # Bán để đóng vị thế Mua (Close Long)
            trade_pnl = price - self.inventory_price
            self.trade_results.append(1 if trade_pnl > 0 else 0)
            self.ac_loss += (FEE_PER_CONTRACT * order_size - trade_pnl * Decimal('100') * order_size)
            self.inventory -= order_size
            matched -= order_size

        return matched

    def update_bid_ask(self, price: Decimal, _step, _timestamp):
        """
        Market-making quotes: FairPrice = SMA(last 20 ticks), PriceRange = high-low.
        If PriceRange < 0.2% × FairPrice → quote Limit Buy at FairPrice - 0.5,
        Limit Sell at FairPrice + 0.5. Else cancel quotes.
        """
        if len(self.price_window) >= LOOKBACK_TICKS:
            prices = list(self.price_window)
            fair_price = sum(prices) / len(prices)
            price_range = max(prices) - min(prices)
            threshold = fair_price * RANGE_PCT
            if price_range < threshold:
                self.bid_price = (Decimal(str(fair_price)) - SPREAD_HALF).quantize(
                    Decimal("0.1"), rounding=ROUND_HALF_UP
                )
                self.ask_price = (Decimal(str(fair_price)) + SPREAD_HALF).quantize(
                    Decimal("0.1"), rounding=ROUND_HALF_UP
                )
            else:
                self.bid_price = None
                self.ask_price = None
        else:
            self.bid_price = None
            self.ask_price = None

        self.handle_matched_order(price)

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
        return f1_data

    def run(self, data: pd.DataFrame, step: Decimal):
        """
        Main backtesting function — market-making on each tick with EOD flatten and stop-loss.
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
            price = row["f2_price"] if moving_to_f2 else row["price"]
            close_price = row["f2_close"] if moving_to_f2 else row["close"]

            if (
                cur_index != len(trading_dates) - 1
                and not expiration_dates.empty()
                and trading_dates[cur_index + 1] >= expiration_dates.queue[0]
            ):
                self.move_f1_to_f2(row["price"], row["f2_price"])
                expiration_dates.get()
                moving_to_f2 = True

            self.handle_force_sell(price)

            # Stop-loss: adverse move >= 2 points → close entire inventory at market
            if self.inventory > 0 and price <= self.inventory_price - STOP_LOSS_POINTS:
                pnl = (price - self.inventory_price) * self.inventory * Decimal("100")
                self.daily_realized_pnl += pnl - abs(self.inventory) * FEE_PER_CONTRACT
                self.inventory = 0
            elif self.inventory < 0 and price >= self.inventory_price + STOP_LOSS_POINTS:
                pnl = (self.inventory_price - price) * abs(self.inventory) * Decimal("100")
                self.daily_realized_pnl += pnl - abs(self.inventory) * FEE_PER_CONTRACT
                self.inventory = 0

            last_tick_of_day = index == len(data) - 1 or row["date"] != data.iloc[index + 1]["date"]

            if last_tick_of_day:
                # Time-based exit: no overnight; close by ATC at end of day
                if self.inventory != 0:
                    sign = 1 if self.inventory > 0 else -1
                    self.daily_realized_pnl += (
                        sign * abs(self.inventory) * (close_price - self.inventory_price) * Decimal("100")
                        - abs(self.inventory) * FEE_PER_CONTRACT
                    )
                    self.inventory = 0
                cur_index += 1
                self.update_pnl(close_price)
                if self.printable:
                    print(
                        f"Realized asset {row['date']}: {int(self.daily_assets[-1] * Decimal('1000'))} VND"
                    )
                if moving_to_f2:
                    self.monthly_tracking.append([row["date"], self.daily_assets[-1]])
                moving_to_f2 = False
                self.ac_loss = Decimal("0.0")
                self.daily_realized_pnl = Decimal("0.0")
                self.bid_price = None
                self.ask_price = None
                self.old_timestamp = None
                self.tracking_dates.append(row["date"])
                self.daily_inventory.append(self.inventory)
            else:
                self.update_bid_ask(price, step, row["datetime"])
                self.price_window.append(price)

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
    bt = Backtesting(
        capital=Decimal("5e5"),
    )

    data = bt.process_data()
    bt.run(data, Decimal("1.8"))

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

    bt.plot_hpr()
    bt.plot_drawdown()
    bt.plot_inventory()
