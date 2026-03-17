import itertools
import multiprocessing
import os
import sys
from decimal import Decimal

import numpy as np
import pandas as pd
from joblib import Parallel, delayed

# Add the current working directory to Python's path
sys.path.insert(0, os.path.abspath('.'))
# If the file is inside a subfolder, add the parent directory too
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from backtesting import Backtesting, PredictiveRSIMarketMaker


spread_multipliers = [0.1, 0.2, 0.3, 0.5]
stop_losses = [Decimal('3.0'), Decimal('5.0'), Decimal('7.0'), Decimal('10.0')]
max_inventories = [3, 5, 10]


def run_backtest(spread_multiplier, stop_loss, max_inv, data):
    market_maker = PredictiveRSIMarketMaker(spread_multiplier=spread_multiplier)
    bt = Backtesting(
        capital=Decimal("5e5"),
        printable=False,
        market_maker=market_maker,
    )
    bt.stop_loss_threshold = stop_loss
    bt.max_inventory = max_inv

    bt.run(data)

    final_asset = bt.daily_assets[-1]
    sharpe = bt.metric.sharpe_ratio(
        risk_free_return=Decimal('0.00023')
    ) * Decimal(str(np.sqrt(250)))
    max_drawdown, _ = bt.metric.maximum_drawdown()
    risk_adjusted_score = sharpe * (Decimal('1') - abs(max_drawdown))

    return {
        'spread_multiplier': spread_multiplier,
        'stop_loss': stop_loss,
        'max_inv': max_inv,
        'total_trades': bt.total_trades,
        'final_asset': final_asset,
        'sharpe': sharpe,
        'max_drawdown': max_drawdown,
        'risk_adjusted_score': risk_adjusted_score,
    }


def main() -> None:
    # Load once to avoid repeated CSV I/O during grid search.
    data = Backtesting.process_data(evaluation=False)

    n_cores = multiprocessing.cpu_count()
    param_combinations = list(itertools.product(spread_multipliers, stop_losses, max_inventories))

    results = Parallel(n_jobs=n_cores)(
        delayed(run_backtest)(s, sl, mi, data) for s, sl, mi in param_combinations
    )

    results_df = pd.DataFrame(results)
    ranked_df = results_df.sort_values(by="risk_adjusted_score", ascending=False)
    display_columns = [
        'spread_multiplier',
        'stop_loss',
        'max_inv',
        'total_trades',
        'final_asset',
        'sharpe',
        'max_drawdown',
        'risk_adjusted_score',
    ]

    print("Top 10 parameter combinations by risk-adjusted score:")
    print(ranked_df[display_columns].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
