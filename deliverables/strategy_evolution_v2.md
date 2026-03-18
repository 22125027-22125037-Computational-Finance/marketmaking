# Strategy Evolution v2 (Code-Aligned)

## Project Context
This project is a Python backtesting engine for VN30 index futures market making. The implementation has evolved from a theory-driven Avellaneda-Stoikov (AS) baseline to a production-style predictive quoting system designed for snapshot/minute data, where pure passive microstructure assumptions are weak.

Current implementation reference: `backtesting.py`.

## 1. Phase 1: AS Foundation (Historical Baseline)
The initial architecture implemented the classic AS inventory-risk framework:

- Reservation price:
  \[
  r(s,t) = s - q\gamma\sigma^2(T-t)
  \]
- Optimal spread:
  \[
  \delta = \frac{\gamma\sigma^2(T-t)}{2} + \frac{2}{\gamma}\ln\left(1+\frac{\gamma}{\kappa}\right)
  \]

This phase provided a principled inventory-control lens, but direct deployment on snapshot data caused instability.

## 2. Phase 2: Dimensional and Execution Realism Fixes
Backtest collapse prompted two root-cause corrections:

- Time-scale normalization for intraday horizon consistency.
- Volatility scaling to realistic absolute contract-point units.
- Fill realism adjustment (strict crossing instead of touch fills) to reduce adverse selection bias in coarse-granularity data.

These changes stabilized the simulation mechanics but did not fully solve directional-regime losses and fee drag.

## 3. Phase 3: Risk-Circuit Architecture
The strategy added hard risk controls to survive non-mean-reverting conditions.

Implemented controls in `backtesting.py`:

- Inventory hard cap: `max_inventory` enforces one-sided quote shutdown when risk is saturated.
- Stop-loss inventory flush: `evaluate_stop_loss(...)` force-exits full inventory once adverse move exceeds threshold.
- Capital-aware exposure bound: `get_maximum_placeable(...)` + `handle_force_sell(...)` prevents over-allocation.
- End-of-day liquidation: `final_exit(...)` removes overnight inventory carry and realizes PnL daily.
- Contract-roll handling: `move_f1_to_f2(...)` supports F1-to-F2 transition around expiry.

## 4. Phase 4: Predictive Market Making Pivot (Current Model)
The current live model is no longer AS-based in quote generation. It is a predictive, signal-gated market maker implemented by `PredictiveRSIMarketMaker`.

### 4.1 Quote Engine
Primary spread logic:
\[
\text{dynamic\_spread} = \max(\text{ATR} \times \text{spread\_multiplier}, 0)
\]

Signal-conditioned quoting (`calculateQuotes(...)`):

- RSI < 30: bid-only (accumulation bias), ask disabled.
- RSI > 70: ask-only (distribution bias), bid disabled.
- 30 <= RSI <= 70: two-sided symmetric quoting with wider offsets.

### 4.2 Trend Strength Gate
Trend and regime filters reduce counter-trend exposure:

- Trend proxy: EMA(12) vs EMA(26) with a +/-2.0 buffer to classify up/down/neutral regime.
- Strength filter: ADX > 25 activates stricter directional gating.
- In strong trend + RSI extreme, the counter-trend side is disabled.

### 4.3 Backtest Execution Semantics
Order matching uses strict inequalities in `handle_matched_order(...)`:

- Bid fill requires `bid_price > price`.
- Ask fill requires `ask_price < price`.

This better approximates queue priority and limits false-positive fills from touch events.

## 5. Feature Pipeline (Current)
`process_data(...)` computes and feeds:

- `RSI(14)`
- `ATR(14)`
- `ADX(14)`
- `EMA(12)`, `EMA(26)`
- `rolling_sigma(60)`

Note: `rolling_sigma` is currently engineered but not directly consumed in `calculateQuotes(...)`; spread width is driven by ATR.

## 6. Current System Identity
The current system is best characterized as:

**A risk-constrained predictive market maker with volatility-adaptive spread control and directional quote gating.**

It combines:

- Signal asymmetry (RSI)
- Volatility adaptation (ATR x multiplier)
- Regime awareness (EMA crossover + ADX strength)
- Hard risk controls (inventory cap, stop-loss flush, capital constraints, EOD liquidation)

## 7. Why This Design Fits Snapshot Data
Given minute/snapshot inputs (without full L2 order-book dynamics), pure passive AS assumptions are less reliable. The implemented architecture improves robustness by:

- Trading less symmetrically in directional extremes.
- Avoiding aggressive counter-trend inventory accumulation.
- Accounting for realistic fill frictions and fee-sensitive turnover.

## 8. Metrics and Deliverables
`__main__` in `backtesting.py` reports:

- Sharpe ratio
- Sortino ratio
- Maximum drawdown
- HPR
- Monthly return
- Annual return
- Total trades

It also generates plots for HPR, drawdown, and inventory trajectory.

## 9. Interview/Defense One-Liner
I evolved the strategy from a textbook AS inventory model into a production-style predictive market maker tailored to snapshot futures data by introducing volatility-adaptive spreads, directional signal gating, and layered risk circuit breakers that control inventory, tail risk, and execution realism.
