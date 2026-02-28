"""
This is main module for strategy backtesting
"""
import os
import numpy as np
from datetime import timedelta, time
from decimal import Decimal, ROUND_HALF_UP
from typing import List
import pandas as pd
import matplotlib.pyplot as plt
from collections import deque

# Giả định các module này đã có sẵn trong môi trường của bạn
from config.config import BACKTESTING_CONFIG
from metrics.metric import get_returns, Metric
from utils import get_expired_dates, from_cash_to_tradeable_contracts, round_decimal

FEE_PER_CONTRACT = Decimal(BACKTESTING_CONFIG["fee"]) * Decimal('100')

class Backtesting:
    """
    Backtesting main class optimized for Market Making
    """

    def __init__(
        self,
        capital: Decimal,
        printable=True,
    ):
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

        self.tick_window = deque(maxlen=20)
        self.trade_results = []

        # --- CÁC THAM SỐ ĐÃ TỐI ƯU ---
        self.sideways_pct = Decimal("0.0015")   # Siết chặt hơn để chỉ trade khi thị trường đi ngang
        self.quote_offset = Decimal("0.2")      # Đặt lệnh cách giá hiện tại 0.2 điểm để tối ưu khớp
        self.entry_band = Decimal("0.8")         # Chỉ vào lệnh khi giá lệch SMA ít nhất 0.8 điểm
        self.cooldown_ticks = 20                 # Chờ 20 ticks sau mỗi lần chốt lệnh để ổn định
        
        # CHẾ ĐỘ PASSIVE: Tắt force_entry để làm Market Maker thực thụ (đặt lệnh chờ)
        self.force_entry_on_signal = False 

        self.signal_count = 0
        self.entry_count = 0
        self.exit_count = 0

        # Edge tối thiểu phải cover được phí round-trip và có buffer lợi nhuận
        self.min_edge_after_fee = (FEE_PER_CONTRACT * Decimal("2")) / Decimal("100") + Decimal("0.5")

        self.tick_count = 0
        self.last_flat_tick = -999999

        self.start_trade_time = time(9, 20)      # Bắt đầu muộn hơn 5p để tránh nhiễu đầu giờ
        self.end_trade_time = time(14, 20)        # Kết thúc sớm hơn để tránh biến động cuối giờ

        self.max_contracts = 2
        self.order_size = 1

        # Nới Stop-loss và tăng Take-profit để bù đắp chi phí giao dịch
        self.stop_loss_points = Decimal("-2.0")  
        self.take_profit_points = Decimal("2.5")

    def move_f1_to_f2(self, f1_price, f2_price):
        if self.inventory > 0:
            self.ac_loss += (self.inventory_price - f1_price) * 100
            self.inventory_price = f2_price
            self.ac_loss += FEE_PER_CONTRACT * abs(self.inventory)
        elif self.inventory < 0:
            self.ac_loss += (f1_price - self.inventory_price) * 100
            self.inventory_price = f2_price
            self.ac_loss += FEE_PER_CONTRACT * abs(self.inventory)

    def update_pnl(self, close_price: Decimal):
        cur_asset = self.daily_assets[-1]
        new_asset = None
        if self.inventory == 0:
            new_asset = cur_asset - self.ac_loss
        else:
            sign = 1 if self.inventory > 0 else -1
            pnl = (
                sign * abs(self.inventory) *
                (close_price - self.inventory_price) * 100
                - self.ac_loss
            )
            new_asset = cur_asset + pnl
            self.inventory_price = close_price

        self.daily_returns.append(new_asset / self.daily_assets[-1] - 1)
        self.daily_assets.append(new_asset)

    def close_all_positions(self, price: Decimal, reason=""):
        if self.inventory != 0:
            sign = 1 if self.inventory > 0 else -1
            trade_pnl = sign * (price - self.inventory_price)
            self.trade_results.append(1 if trade_pnl > Decimal('0') else 0)

            qty = Decimal(str(abs(self.inventory)))

            if self.inventory > 0:
                self.ac_loss += (self.inventory_price - price) * Decimal('100') * qty
            else:
                self.ac_loss += (price - self.inventory_price) * Decimal('100') * qty

            self.ac_loss += FEE_PER_CONTRACT * qty
            self.inventory = 0
            self.inventory_price = Decimal('0')
            self.last_flat_tick = self.tick_count
            self.exit_count += 1

    def handle_force_sell(self, price: Decimal):
        while self.get_maximum_placeable(price) < 0:
            sign = 1 if self.inventory < 0 else -1
            self.inventory += sign
            self.ac_loss += abs(price - self.inventory_price) * 100 + FEE_PER_CONTRACT

    def get_maximum_placeable(self, inst_price: Decimal):
        MAX_CONTRACTS = self.max_contracts
        cash_placeable = max(
            from_cash_to_tradeable_contracts(
                self.daily_assets[-1] - self.ac_loss, inst_price
            ),
            0,
        )
        allowed_by_risk = MAX_CONTRACTS - abs(self.inventory)
        return min(cash_placeable, allowed_by_risk)

    def handle_matched_order(self, price: Decimal):
        matched = 0
        order_size = self.order_size
        placeable = self.get_maximum_placeable(price)

        if self.bid_price is None and self.ask_price is None:
            return matched

        if self.bid_price is not None and price <= self.bid_price:
            fill_price = self.bid_price
            if self.inventory < 0:
                trade_pnl = self.inventory_price - fill_price
                self.trade_results.append(1 if trade_pnl > 0 else 0)
                self.ac_loss += (FEE_PER_CONTRACT * order_size -
                                 trade_pnl * Decimal("100") * order_size)
                self.inventory += order_size
                matched -= order_size
            elif self.inventory == 0 and placeable >= order_size:
                self.inventory_price = fill_price
                self.inventory += order_size
                self.ac_loss += FEE_PER_CONTRACT * order_size
                matched += order_size
            self.bid_price = None # Lệnh đã khớp, xóa lệnh chờ

        elif self.ask_price is not None and price >= self.ask_price:
            fill_price = self.ask_price
            if self.inventory > 0:
                trade_pnl = fill_price - self.inventory_price
                self.trade_results.append(1 if trade_pnl > 0 else 0)
                self.ac_loss += (FEE_PER_CONTRACT * order_size -
                                 trade_pnl * Decimal("100") * order_size)
                self.inventory -= order_size
                matched -= order_size
            elif self.inventory == 0 and placeable >= order_size:
                self.inventory_price = fill_price
                self.inventory -= order_size
                self.ac_loss += FEE_PER_CONTRACT * order_size
                matched += order_size
            self.ask_price = None # Lệnh đã khớp, xóa lệnh chờ

        return matched

    def update_bid_ask(self, current_price: Decimal, timestamp=None):
        self.handle_matched_order(current_price)

        if self.tick_count - self.last_flat_tick < self.cooldown_ticks:
            self.bid_price = None
            self.ask_price = None
            return

        if self.inventory != 0:
            return

        if len(self.tick_window) == 20:
            fair_price = sum(self.tick_window) / Decimal("20")
            price_range = max(self.tick_window) - min(self.tick_window)

            sideways_threshold = fair_price * self.sideways_pct
            required_edge = max(self.entry_band, self.min_edge_after_fee)
            mispricing = current_price - fair_price

            # Chỉ đặt lệnh khi thị trường đi ngang thực sự
            if price_range <= sideways_threshold:
                placeable = self.get_maximum_placeable(current_price)
                order_size = self.order_size

                # LONG signal
                if mispricing <= -required_edge:
                    self.signal_count += 1
                    if self.force_entry_on_signal and self.inventory == 0 and placeable >= order_size:
                        self.inventory = order_size
                        self.inventory_price = current_price
                        self.ac_loss += FEE_PER_CONTRACT * Decimal(order_size)
                        self.entry_count += 1
                        return
                    # Chế độ Passive: Đặt lệnh chờ Bid
                    self.bid_price = (current_price - self.quote_offset).quantize(Decimal("0.0"))
                    self.ask_price = None

                # SHORT signal
                elif mispricing >= required_edge:
                    self.signal_count += 1
                    if self.force_entry_on_signal and self.inventory == 0 and placeable >= order_size:
                        self.inventory = -order_size
                        self.inventory_price = current_price
                        self.ac_loss += FEE_PER_CONTRACT * Decimal(order_size)
                        self.entry_count += 1
                        return
                    # Chế độ Passive: Đặt lệnh chờ Ask
                    self.ask_price = (current_price + self.quote_offset).quantize(Decimal("0.0"))
                    self.bid_price = None
            else:
                self.bid_price = None
                self.ask_price = None

    @staticmethod
    def process_data(evaluation=False):
        prefix_path = "data/os/" if evaluation else "data/is/"
        f1_data = pd.read_csv(f"{prefix_path}VN30F1M_data.csv")
        f1_data["datetime"] = pd.to_datetime(f1_data["datetime"], format="%Y-%m-%d %H:%M:%S.%f")
        f1_data["date"] = pd.to_datetime(f1_data["date"], format="%Y-%m-%d").dt.date
        rounding_columns = ["close", "price", "best-bid", "best-ask", "spread"]
        for col in rounding_columns:
            f1_data = round_decimal(f1_data, col)

        f2_data = pd.read_csv(f"{prefix_path}VN30F2M_data.csv")
        f2_data = f2_data[["date", "datetime", "tickersymbol", "price", "close"]].copy()
        f2_data["datetime"] = pd.to_datetime(f2_data["datetime"], format="%Y-%m-%d %H:%M:%S.%f")
        f2_data["date"] = pd.to_datetime(f2_data["date"], format="%Y-%m-%d").dt.date
        f2_data.rename(columns={"price": "f2_price", "close": "f2_close", "tickersymbol": "f2-tickersymbol"}, inplace=True)
        rounding_columns = ["f2_close", "f2_price"]
        for col in rounding_columns:
            f2_data = round_decimal(f2_data, col)

        f1_data = pd.merge(f1_data, f2_data, on=["datetime", "date"], how="outer", sort=True)
        f1_data = f1_data.ffill()
        return f1_data

    def run(self, data: pd.DataFrame, step: Decimal):
        trading_dates = data["date"].unique().tolist()
        start_date = data["datetime"].iloc[0]
        end_date = data["datetime"].iloc[-1]
        expiration_dates = get_expired_dates(start_date, end_date)

        cur_index = 0
        moving_to_f2 = False
        for index, row in data.iterrows():
            self.tick_count += 1
            self.cur_date = row["datetime"]
            current_time = self.cur_date.time()

            if (cur_index != len(trading_dates) - 1 and not expiration_dates.empty() 
                and trading_dates[cur_index + 1] >= expiration_dates.queue[0]):
                self.move_f1_to_f2(row["price"], row["f2_price"])
                expiration_dates.get()
                moving_to_f2 = True

            current_tick_price = Decimal(str(row["f2_price"] if moving_to_f2 else row["price"]))
            self.tick_window.append(current_tick_price)

            if current_time >= time(14, 30):
                if self.inventory != 0:
                    self.close_all_positions(current_tick_price, reason="ATC 14:30")
                self.bid_price = None
                self.ask_price = None
            else:
                if not (self.start_trade_time <= current_time <= self.end_trade_time):
                    self.bid_price = None
                    self.ask_price = None
                else:
                    if self.inventory != 0:
                        sign = 1 if self.inventory > 0 else -1
                        unrealized_pnl = sign * (current_tick_price - self.inventory_price)

                        if unrealized_pnl <= self.stop_loss_points:
                            self.close_all_positions(current_tick_price, reason="Stop-loss")
                        elif unrealized_pnl >= self.take_profit_points:
                            self.close_all_positions(current_tick_price, reason="Take-profit")

                    self.handle_force_sell(current_tick_price)
                    self.update_bid_ask(current_price=current_tick_price, timestamp=row["datetime"])

            if index == len(data) - 1 or row["date"] != data.iloc[index + 1]["date"]:
                cur_index += 1
                close_price = Decimal(str(row["f2_close"] if moving_to_f2 else row["close"]))
                self.update_pnl(close_price)

                if self.printable:
                    print(f"Realized asset {row['date']}: {int(self.daily_assets[-1] * Decimal('1000'))} VND")

                if moving_to_f2:
                    self.monthly_tracking.append([row["date"], self.daily_assets[-1]])

                moving_to_f2 = False
                self.ac_loss = Decimal("0.0")
                self.bid_price = None
                self.ask_price = None
                self.tick_window.clear()
                self.tracking_dates.append(row["date"])
                self.daily_inventory.append(self.inventory)

        self.metric = Metric(self.daily_returns, None)

    # Các hàm plot giữ nguyên...
    def plot_hpr(self, path="result/backtest/hpr.svg"):
        plt.figure(figsize=(10, 6))
        assets = pd.Series(self.daily_assets)
        ac_return = assets.apply(lambda x: x / assets.iloc[0])
        ac_return = [(val - 1) * 100 for val in ac_return.to_numpy()[1:]]
        plt.plot(self.tracking_dates, ac_return, label="Portfolio", color='black')
        plt.title('Holding Period Return Over Time')
        plt.xlabel('Time Step')
        plt.ylabel('Holding Period Return (%)')
        plt.grid(True)
        plt.legend()
        plt.savefig(path, dpi=300, bbox_inches='tight')

    def plot_drawdown(self, path="result/backtest/drawdown.svg"):
        _, drawdowns = self.metric.maximum_drawdown()
        plt.figure(figsize=(10, 6))
        plt.plot(self.tracking_dates, drawdowns, label="Portfolio", color='black')
        plt.title('Draw down Value Over Time')
        plt.xlabel('Time Step')
        plt.ylabel('Percentage')
        plt.grid(True)
        plt.savefig(path, dpi=300, bbox_inches='tight')

    def plot_inventory(self, path="result/backtest/inventory.svg"):
        plt.figure(figsize=(10, 6))
        plt.plot(self.tracking_dates, self.daily_inventory, label="Portfolio", color='black')
        plt.title('Inventory Value Over Time')
        plt.xlabel('Time Step')
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(path, dpi=300, bbox_inches='tight')

if __name__ == "__main__":
    bt = Backtesting(capital=Decimal("5e5"))
    data = bt.process_data()
    bt.run(data, Decimal("1.8"))

    print(f"Sharpe ratio: {bt.metric.sharpe_ratio(risk_free_return=Decimal('0.00023')) * Decimal(np.sqrt(250))}")
    print(f"Sortino ratio: {bt.metric.sortino_ratio(risk_free_return=Decimal('0.00023')) * Decimal(np.sqrt(250))}")
    mdd, _ = bt.metric.maximum_drawdown()
    print(f"Maximum drawdown: {mdd}")

    monthly_df = pd.DataFrame(bt.monthly_tracking, columns=["date", "asset"])
    if len(monthly_df) > 0:
        returns = get_returns(monthly_df)
        print(f"Monthly return {returns['monthly_return']}")
        print(f"Annual return {returns['annual_return']}")

    print(f"HPR {bt.metric.hpr()}")

    if len(bt.trade_results) > 0:
        win_rate = sum(bt.trade_results) / len(bt.trade_results) * 100
        print(f"Win Rate: {win_rate:.2f}% (Tổng số lệnh chốt: {len(bt.trade_results)})")
    else:
        print("Win Rate: 0% (Không có lệnh nào được khớp)")

    os.makedirs("result/backtest", exist_ok=True)
    bt.plot_hpr()
    bt.plot_drawdown()
    bt.plot_inventory()