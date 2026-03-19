import itertools
import multiprocessing
import json
from decimal import Decimal
import numpy as np
import pandas as pd
from joblib import Parallel, delayed

from backtesting import Backtesting, PredictiveRSIMarketMaker

def run_backtest(spread_mult, stop_loss, max_inv, data):
    try:
        market_maker = PredictiveRSIMarketMaker(spread_multiplier=spread_mult)
        bt = Backtesting(
            capital=Decimal("5e5"),
            printable=False,
            market_maker=market_maker,
        )
        bt.stop_loss_threshold = stop_loss
        bt.max_inventory = max_inv
        bt.run(data)
        
        final_asset = bt.daily_assets[-1]
        sharpe = float(bt.metric.sharpe_ratio(risk_free_return=Decimal('0.00023')) * Decimal(str(np.sqrt(250))))
        max_drawdown, _ = bt.metric.maximum_drawdown()
        max_drawdown = float(max_drawdown)
        total_trades = bt.total_trades
        
        # Risk-Adjusted Score Metric
        risk_adjusted_score = sharpe * (1.0 - abs(max_drawdown))
        
        return {
            'spread_mult': spread_mult,
            'stop_loss': stop_loss,
            'max_inv': max_inv,
            'total_trades': total_trades,
            'final_asset': float(final_asset),
            'sharpe': sharpe,
            'max_drawdown': max_drawdown,
            'risk_adjusted_score': risk_adjusted_score
        }
    except Exception as e:
        # If a combination fails, safely skip it
        return {'spread_mult': spread_mult, 'stop_loss': stop_loss, 'max_inv': max_inv, 'total_trades': 0, 'final_asset': 0.0, 'sharpe': -99.0, 'max_drawdown': -1.0, 'risk_adjusted_score': -99.0}

print("\nLoading data...")
data = Backtesting.process_data(evaluation=False)

# Testing multipliers of the ATR instead of fixed points
spread_multipliers = [0.1, 0.2, 0.3, 0.4, 0.5]
stop_losses = [Decimal('3.0'), Decimal('5.0'), Decimal('7.0'), Decimal('10.0')]
max_inventories = [3, 5, 10]

param_combinations = list(itertools.product(spread_multipliers, stop_losses, max_inventories))
n_cores = multiprocessing.cpu_count()

print(f"Testing {len(param_combinations)} combinations across {n_cores} CPU cores...")

results = Parallel(n_jobs=n_cores)(
    delayed(run_backtest)(sm, sl, mi, data) for sm, sl, mi in param_combinations
)

results_df = pd.DataFrame(results)
ranked_df = results_df.sort_values(by="risk_adjusted_score", ascending=False)

print("\n--- Top 10 Parameter Combinations (Dynamic Spread) ---")
print(ranked_df.head(10).to_string(index=False))

# Persist best parameter set so `python backtesting.py` uses the same setup.
best = ranked_df.iloc[0]
best_config = {
    "spread_multiplier": float(best["spread_mult"]),
    "spread": float(best["spread_mult"]),
    "stop_loss": str(best["stop_loss"]),
    "max_inv": int(best["max_inv"]),
}

with open("parameter/optimized_parameter.json", "w", encoding="utf-8") as f:
    json.dump(best_config, f, indent=4)

print("\nSaved best parameters to parameter/optimized_parameter.json:")
print(best_config)