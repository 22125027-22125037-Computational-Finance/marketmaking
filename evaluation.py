"""
Out-sample evaluation module
"""

from decimal import Decimal
import numpy as np
import pandas as pd
from config.config import BEST_CONFIG
from backtesting import Backtesting
from metrics.metric import get_returns


if __name__ == "__main__":
    data = Backtesting.process_data(evaluation=True)
    bt = Backtesting(capital=Decimal('5e5'))

    bt.run(data, Decimal(BEST_CONFIG["step"]))
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
