"""
This is main module for strategy backtesting
"""

import numpy as np
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import List
import pandas as pd
import matplotlib.pyplot as plt
from collections import deque

from config.config import BACKTESTING_CONFIG
from metrics.metric import get_returns, Metric
from utils import get_expired_dates, from_cash_to_tradeable_contracts, round_decimal

FEE_PER_CONTRACT = Decimal(BACKTESTING_CONFIG["fee"]) * Decimal('100')


class Backtesting:
    """
    Backtesting main class
    """

    def __init__(
        self,
        capital: Decimal,
        printable=True,
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

        # THÊM MỚI: Cửa sổ lưu trữ 20 ticks gần nhất
        self.tick_window = deque(maxlen=20)

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
        if self.bid_price is None or self.ask_price is None:
            return matched

        if self.bid_price >= price and self.inventory >= 0 and placeable > 0:
            self.inventory_price = (
                self.inventory_price * abs(self.inventory) + price
            ) / (abs(self.inventory) + 1)
            self.inventory += 1
            matched += 1
        elif self.bid_price >= price and self.inventory < 0:
            self.ac_loss += (FEE_PER_CONTRACT - (self.inventory_price - price) * Decimal('100'))
            self.inventory += 1
            matched -= 1

        if self.ask_price <= price and self.inventory <= 0 and placeable > 0:
            self.inventory_price = (
                self.inventory_price * abs(self.inventory) + price
            ) / (abs(self.inventory) + 1)
            self.inventory -= 1
            matched += 1
        elif self.ask_price <= price and self.inventory > 0:
            self.ac_loss += (FEE_PER_CONTRACT - (price - self.inventory_price) * Decimal('100'))
            self.inventory -= 1
            matched -= 1

        return matched

    def update_bid_ask(self, current_price: Decimal, reference_price: Decimal, step, timestamp):
        """
        Placing bid ask formula based on SMA 20 reference price
        """
        # Khớp lệnh dựa trên giá thị trường hiện tại
        matched = self.handle_matched_order(current_price)

        if self.old_timestamp is None or timestamp > self.old_timestamp + timedelta(
            seconds=int(BACKTESTING_CONFIG["time"])
        ):
            self.old_timestamp = timestamp
            # Đặt lệnh mới dựa trên giá trị tham chiếu (SMA 20)
            self.bid_price = (
                reference_price - step * Decimal(max(self.inventory, 0) * 0.02 + 1)
            ).quantize(Decimal("0.0"), rounding=ROUND_HALF_UP)
            self.ask_price = (
                reference_price - step * Decimal(min(self.inventory, 0) * 0.02 - 1)
            ).quantize(Decimal("0.0"), rounding=ROUND_HALF_UP)
        elif matched != 0:
            # Cập nhật lại lệnh nếu vừa khớp lệnh xong
            self.bid_price = (
                reference_price - step * Decimal(max(self.inventory, 0) * 0.02 + 1)
            ).quantize(Decimal("0.0"), rounding=ROUND_HALF_UP)
            self.ask_price = (
                reference_price - step * Decimal(min(self.inventory, 0) * 0.02 - 1)
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
        return f1_data

    def run(self, data: pd.DataFrame, step: Decimal):
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
            if (
                cur_index != len(trading_dates) - 1
                and not expiration_dates.empty()
                and trading_dates[cur_index + 1] >= expiration_dates.queue[0]
            ):
                self.move_f1_to_f2(row["price"], row["f2_price"])
                expiration_dates.get()
                moving_to_f2 = True

            # Lấy giá trị của tick hiện tại
            current_tick_price = row["f2_price"] if moving_to_f2 else row["price"]
            
            # THÊM MỚI: Thêm giá vào window để tính SMA 20
            self.tick_window.append(current_tick_price)
            
            # Tính SMA. Nếu chưa đủ 20 ticks thì lấy trung bình của số lượng hiện có
            sma_20 = sum(self.tick_window) / Decimal(len(self.tick_window))

            self.handle_force_sell(current_tick_price)
            
            # THÊM MỚI: Truyền cả giá hiện tại và SMA 20 vào hàm
            self.update_bid_ask(
                current_price=current_tick_price, 
                reference_price=sma_20, 
                step=step, 
                timestamp=row["datetime"]
            )

            # ... [Phần code bên dưới của hàm run giữ nguyên] ...

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
