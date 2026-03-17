"""
Out-sample evaluation module
"""

from decimal import Decimal
import numpy as np
import pandas as pd
from config.config import BEST_CONFIG
from backtesting import Backtesting, PredictiveRSIMarketMaker
from metrics.metric import get_returns


if __name__ == "__main__":
    spread_multiplier = float(BEST_CONFIG.get("spread_multiplier", BEST_CONFIG.get("spread", 0.2)))
    stop_loss = Decimal(str(BEST_CONFIG.get("stop_loss", "5.0")))
    max_inv = int(BEST_CONFIG.get("max_inv", 5))

    data = Backtesting.process_data(evaluation=True)
    bt = Backtesting(
        capital=Decimal('5e5'),
        market_maker=PredictiveRSIMarketMaker(spread_multiplier=spread_multiplier),
    )
    bt.stop_loss_threshold = stop_loss
    bt.max_inventory = max_inv

    bt.run(data)
    bt.plot_hpr(path="result/optimization/hpr.svg")
    bt.plot_drawdown(path="result/optimization/drawdown.svg")
    bt.plot_inventory(path="result/optimization/inventory.svg")

    monthly_df = pd.DataFrame(bt.monthly_tracking, columns=["date", "asset"])
    returns = get_returns(monthly_df)

    print(f"HPR {bt.metric.hpr()}")
    print(f"Monthly return {returns['monthly_return']}")
    print(f"Annual return {returns['annual_return']}")
    print(
        f"Sharpe ratio: {bt.metric.sharpe_ratio(risk_free_return=Decimal('0.00023')) * Decimal(np.sqrt(250))}"
    )
    print(
        f"Sortino ratio: {bt.metric.sortino_ratio(risk_free_return=Decimal('0.00023')) * Decimal(np.sqrt(250))}"
    )
    mdd, _ = bt.metric.maximum_drawdown()
    print(f"Maximum drawdown: {mdd}")
    print(f"Total trades: {bt.total_trades}")
