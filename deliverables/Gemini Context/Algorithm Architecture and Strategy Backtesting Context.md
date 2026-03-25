# Algorithm Architecture and Strategy Context

## Executive Summary
This project develops and validates a VN30F1M futures market-making system in the Vietnamese index futures market, beginning with the Avellaneda-Stoikov (2008) inventory-risk paradigm and evolving into a predictive, regime-aware, risk-constrained execution engine tailored to discrete snapshot data.

The central empirical result is the **out-of-sample Sharpe ratio of 3.7739** (`evaluation_log.txt`), achieved alongside controlled tail risk (Maximum Drawdown `-2.93%`) and stable trade throughput (`21,898` trades). The out-of-sample evaluation spans a 16-month period (2024-2025) and generated an absolute profit of `222,189,999 VND` on a base capital of `500,000,000 VND`. This result is not the product of a single theoretical formula; it is the consequence of sequential architectural corrections to simulation fidelity, signal design, and risk control.

The progression can be summarized as:
- **From theory-first symmetry** (continuous-time Avellaneda-Stoikov) 
- **To microstructure-realistic execution** (strict crossing fills, EOD flattening)
- **To predictive asymmetry** (RSI-gated quoting with trend circuit breakers)
- **To adaptive risk geometry** (ATR-scaled spread, stop-loss flush, inventory caps)
- **To robust parameter selection** (parallelized risk-adjusted objective under drawdown penalty)

In other words, the project demonstrates a rigorous transition from elegant stochastic control to empirically robust quantitative software engineering.

## Phase 1 - The Theoretical Foundation
### 1.1 Avellaneda-Stoikov Baseline
The initial foundation follows Avellaneda and Stoikov's market-making framework with inventory penalization under stochastic mid-price dynamics.

Let:
- `s`: mid-price
- `q`: current inventory
- `gamma`: risk-aversion coefficient
- `sigma`: instantaneous volatility
- `T - t`: remaining horizon
- `kappa`: order-arrival sensitivity to quote distance

Core equations:

Reservation price:

\[
r(s,t) = s - q\gamma\sigma^2 (T-t)
\]

Half-spread / spread decomposition (as written in project notes):

\[
\delta = \frac{\gamma\sigma^2(T-t)}{2} + \frac{2}{\gamma}\ln\left(1+\frac{\gamma}{\kappa}\right)
\]

Interpretation:
- The first term increases with volatility and horizon, compensating inventory risk.
- The second term captures adverse execution risk through fill-intensity curvature (`kappa`).
- Inventory sign shifts reservation price away from current mid to induce mean reversion in `q`.

### 1.2 Why the Continuous-Time Symmetric Model Failed in Practice
Despite analytical elegance, the pure AS logic degraded on snapshot-level futures data:

- **Adverse selection under coarse sampling**: in discrete snapshots, fills are disproportionately observed when short-horizon informed flow moves through stale quotes.
- **Symmetric quoting in asymmetric flow**: the strategy simultaneously posted both sides even during directional pressure, effectively fading momentum without a regime filter.
- **Microstructure mismatch**: assumptions of smooth diffusion and continuous quote updates do not hold with event sparsity, queue priority, and batch-like observation.
- **Fee drag amplification**: high turnover under weak edge eroded PnL when spread capture did not compensate for toxic flow.

Before the architecture was repaired, this pure continuous-time deployment experienced a near-total maximum drawdown of approximately `-97%` in early testing, which made clear that a mathematically coherent but microstructure-blind implementation was not operationally survivable.

The empirical failure mode is classic: theoretically neutral inventory control becomes structurally exposed to momentum toxicity in real discrete data.

## Phase 2 - Microstructure Engineering and Simulation Fidelity
The second phase was software-engineering heavy: preserving the quantitative intent while correcting simulation artifacts.

### 2.1 Unit Dimension Normalization (Time and Volatility)
The architecture introduced explicit normalization so control terms live in coherent units:

- **Time normalization**: quote-refresh cadence is discretized through `BACKTESTING_CONFIG["time"]` and enforced in `update_bid_ask(...)`, replacing implicit continuous-time updates with deterministic sampling intervals.
- **Volatility normalization**: raw volatility proxies are mapped to contract-point scale (e.g., rolling sigma, ATR) so quote distances are in executable price units, not abstract variance units.

Conceptually, if `sigma_ann` is annualized volatility and `Delta t` is a snapshot interval (fraction of year), then effective local volatility should scale as:

\[
\sigma_{\Delta t} = \sigma_{ann}\sqrt{\Delta t}
\]

This dimensional correction is critical to avoid pathological spreads and unstable reservation adjustments.

### 2.2 Strict Matching Semantics: No Touch-Fills
A crucial execution realism fix in `handle_matched_order(...)`:
- Bid fills require `bid_price > price`.
- Ask fills require `ask_price < price`.

This intentionally rejects touch-at-quote fills (`=`) and approximates queue-priority friction. Why it matters:
- Touch-fills overstate passive execution probability in snapshots.
- Over-filling at touch creates artificial spread-capture alpha.
- Strict crossing reduces optimistic backtest bias and better reflects adverse queue positioning.

### 2.3 End-of-Day Liquidation to Eliminate Overnight Gap Risk
`final_exit(...)` force-closes inventory each session close, executed at the 14:45 ATC (At-The-Close) session so delta exposure is fully flattened before the market transitions overnight. This design neutralizes overnight jump risk that is not priced by intraday models.

Mechanically:
- For inventory `q`, liquidation realizes `q * (close - inventory_price) * contract_multiplier` net fees.
- Inventory resets to zero before the next session.

Rationale:
- Prevents unmodeled overnight drift/gap contamination.
- Makes daily PnL attribution cleaner and comparable.
- Aligns strategy identity with intraday market-making rather than overnight speculation.

## Phase 3 - Predictive Market Making and Signal Gating
This phase is the decisive pivot: quote generation migrated away from pure AS into a predictive, asymmetric policy implemented in `PredictiveRSIMarketMaker`.

### 3.1 RSI-Driven Asymmetric Quoting
With 14-period RSI computed in `process_data(...)`, quote logic is:
- If `RSI < 30`: bid-only accumulation regime.
- If `RSI > 70`: ask-only distribution regime.
- Else: two-sided quoting around mid.

This converts the strategy from symmetric liquidity provision to **state-conditional liquidity provision**, reducing inventory accumulation against dominant order flow.

### 3.2 Regime Filter as Circuit Breaker
The "freight train" defense combines trend direction and trend strength:

- Direction: EMA crossover with dead-band
  - `trend_signal = +1` if `EMA12 > EMA26 + 2`
  - `trend_signal = -1` if `EMA12 < EMA26 - 2`
  - else neutral
- Strength: `ADX > 25`

When ADX indicates strong trend, counter-trend quoting is disabled:
- In strong uptrend (`trend_signal=+1`) and overbought RSI (`RSI>70`), ask-side fading is blocked.
- In strong downtrend (`trend_signal=-1`) and oversold RSI (`RSI<30`), bid-side fading is blocked.

This acts as a hard regime-aware circuit breaker against repeatedly stepping in front of directional flow.

### 3.3 Feature Pipeline Supporting Predictive Quoting
`process_data(...)` builds:
- `RSI(14)`
- `ATR(14)`
- `ADX(14)`
- `EMA(12), EMA(26)`
- `rolling_sigma(60)`

The current quote engine directly consumes RSI/ATR/ADX/EMA-derived trend signal; `rolling_sigma` remains available as extensible volatility context.

## Phase 4 - Dynamic Risk Management
### 4.1 Volatility-Adjusted Spread (Replacing Static Spread)
Quote distance is volatility-adaptive:

\[
\text{dynamic\_spread}_t = \max(\text{ATR}_{14,t} \times m,\;0)
\]

where `m` is the calibrated spread multiplier.

Price placement:
- Neutral RSI regime: `bid = mid - dynamic_spread`, `ask = mid + dynamic_spread`.
- Extreme RSI regime: one-sided quotes use half-width around mid for controlled skew.

Why superior to static spread:
- Expands in high volatility to preserve adverse-selection buffer.
- Contracts in low volatility to maintain fill probability.
- Maintains approximate signal-to-noise consistency across volatility regimes.

### 4.2 Inventory Hard Limits
`max_inventory` enforces absolute inventory bounds. Once saturation is reached:
- Long cap reached: bid disabled.
- Short cap reached: ask disabled.

This is equivalent to applying a state constraint:

\[
|q_t| \le q_{max}
\]

and actively censoring actions that violate it.

### 4.3 Stop-Loss Flush Mechanism
`evaluate_stop_loss(...)` liquidates full inventory if adverse move exceeds threshold `L` points:

Long inventory flush trigger:
\[
inventory\_price - price_t \ge L
\]

Short inventory flush trigger:
\[
price_t - inventory\_price \ge L
\]

Upon trigger, full position is closed and fee-adjusted loss is realized immediately. In the deployed configuration (`L = 10.0` points), the engine is explicitly designed to aggressively market-order out of the toxic inventory, willingly paying the `0.4`-point exchange fee to preserve core equity and prevent nonlinear loss escalation in one-way markets.

### 4.4 Capital-Aware Execution Bounds
`get_maximum_placeable(...)` maps available capital to max contract count using margin geometry:

\[
N_{max} = \left\lfloor \frac{cash}{price \times multiplier \times margin\_rate} \right\rfloor
\]

with default multiplier `100` and margin rate `0.17` (`utils.py`).

`handle_force_sell(...)` actively reduces inventory if position exceeds feasible margin-constrained capacity.

## Phase 5 - Quantitative Optimization and Out-of-Sample Evaluation
### 5.1 Parallelized Grid Search Architecture
`optimize.py` executes a Cartesian grid over:
- `spread_multiplier in {0.1,0.2,0.3,0.4,0.5}`
- `stop_loss in {3,5,7,10}`
- `max_inventory in {3,5,10}`

Total parameter tuples: `5 x 4 x 3 = 60`.

Evaluation is parallelized using `joblib.Parallel` across `multiprocessing.cpu_count()` cores, materially reducing wall-clock optimization time and enabling broader search under fixed compute budget.

### 5.2 Objective Redesign to Reduce Overfitting
Instead of maximizing Sharpe alone, optimization uses:

\[
\text{risk\_adjusted\_score} = \text{Sharpe} \times (1 - |\text{MDD}|)
\]

This introduces explicit drawdown penalization. Practical effect:
- Two models with similar Sharpe are separated by path risk.
- High-turnover/high-volatility parameter sets with fragile equity curves are demoted.
- Selection aligns with deployability, not just headline return efficiency.

Best configuration persisted in `optimized_parameter.json`:
- `spread_multiplier = 0.5`
- `stop_loss = 10.0`
- `max_inv = 5`

### 5.3 Empirical Results: In-Sample vs Out-of-Sample
From `backtesting_log.txt` (in-sample):
- Sharpe: `3.1265`
- Sortino: `5.2002`
- Maximum Drawdown: `-4.4328%`
- HPR: `38.14%`
- Annual Return: `36.50%`
- Total Trades: `15,340`

From `evaluation_log.txt` (out-of-sample):
- Sharpe: `3.7739`
- Sortino: `6.8091`
- Maximum Drawdown: `-2.9295%`
- HPR: `44.438%`
- Annual Return: `28.3006%`
- Total Trades: `21,898`

Interpretation:
- Out-of-sample risk-adjusted performance remains strong, indicating the architecture generalizes.
- Drawdown compression out-of-sample suggests risk controls are not merely cosmetic.
- Trade count supports statistical relevance of results rather than sparse-luck outcomes.
- The system appears robust to regime transfer from calibration to evaluation period.

## Failure Modes and Engineering Resolutions
Primary problems encountered and fixed:
- **Toxic flow / adverse selection**: mitigated by strict fill semantics, one-sided predictive quoting, and trend-strength gating.
- **Momentum fade blow-ups ("freight train" effect)**: mitigated by EMA crossover plus ADX `> 25` circuit breaker.
- **Overnight gap risk**: neutralized by mandatory EOD liquidation.
- **Volatility-regime instability**: mitigated by ATR-scaled spread rather than fixed offsets.
- **Inventory drift and capital overextension**: constrained via hard inventory caps, stop-loss flush, and margin-aware placeable limits.
- **Objective overfitting risk**: reduced via drawdown-penalized optimization score.

## Conclusion
The final system is best described as a **predictive, volatility-adaptive, risk-constrained market-making architecture** engineered for discrete futures data rather than idealized continuous-time assumptions.

The project demonstrates two competencies valued in quantitative research and quant software engineering interviews:
- Ability to translate stochastic-control theory into production-grade simulation and execution logic.
- Ability to diagnose empirical failure modes and implement mathematically consistent, testable design corrections that improve out-of-sample robustness.

The out-of-sample Sharpe of `3.7739`, combined with low realized drawdown and disciplined risk mechanics, supports the strategy's viability as both an academic contribution and an interview-grade portfolio artifact.
