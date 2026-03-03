# Hybrid Market Making Strategy on VN30F1M

![Static Badge](https://img.shields.io/badge/PLUTUS-Pending-darkblue)
![Static Badge](https://img.shields.io/badge/Course-CS408_APCS-blue)

**Authors:** Group 8 (Dương Trung Hiếu - 22125027 | Dương Ngọc Quang Khiêm - 22125037)

## Abstract
This project presents a Hybrid Market Making Strategy tailored for the VN30 Index Futures (VN30F1M). The primary objective is to capture small, consistent bid-ask spreads while maintaining a strictly neutral inventory position. By utilizing a high-frequency 20-tick Simple Moving Average (SMA) baseline and an adaptive inventory skewing mechanism, the algorithm dynamically filters out toxic flow. Ultimately, the system accepts a trade-off, deliberately sacrificing high absolute yields to prioritize capital survivability and prevent catastrophic margin calls.

## Introduction
Pure market-making models often generate high absolute yields but suffer from severe capital impairment during sudden, strong directional moves—known as "toxic flow." Because they use symmetric quoting, they experience heavy adverse selection, and lagging indicators (like an SMA 80) force the bot to "buy the falling knife" at stale prices. 

To solve this, our adapted strategy acts as a highly disciplined liquidity provider without betting on long-term price direction. By utilizing responsive anchoring and a dynamic "Pressure Release Valve" for inventory management, the algorithm selectively quotes to capture spreads in sideways markets and fades directional spikes. The ultimate goal is to guarantee algorithmic stability and strict risk containment in live trading environments.

## Step 1: Trading Algorithm Hypotheses
Our strategy is built upon three core quantitative hypotheses:
* **Mean Reversion (Exhaustion):** Intraday markets frequently exhibit short-term exhaustion after rapid price spikes, creating an opportunity for reverting fills.
* **Responsive Anchoring:** A high-frequency 20-tick SMA acts as a real-time dynamic baseline, tracking momentum shifts much faster than traditional lagging moving averages.
* **Directional Fading:** Instead of symmetric quoting during volatile spikes, the bot selectively quotes only the reverting side when the price deviates more than 1.0 point from the SMA baseline.

## Step 2 & 3: Data

### Data Collection
* **Target Contract:** VN30 Index Futures (VN30F1M).
* **Format:** Intraday tick data encompassing `close`, `price`, `best-bid`, `best-ask`, and `spread`.
* **Period:** The dataset spans an In-Sample period (2022) and an Out-of-Sample period (2024–2026).

### Data Processing
* **Rollover Management:** The system imports the primary contract (`VN30F1M_data.csv`) alongside the subsequent month's contract (`VN30F2M_data.csv`). It maps prices dynamically to handle expiration rollovers cleanly.
* **Formatting:** Timestamps are converted via `pd.to_datetime`. Tick prices and spreads are rounded, and forward-filling (`ffill()`) is applied to manage missing ticks and maintain data integrity.

## Implementation (How to Run)

### Environment Setup
Ensure you have Python installed along with the required libraries:
```bash
pip install pandas numpy matplotlib

```

### Running the Algorithm

To execute the backtest, run the main module from your terminal:

```bash
python backtesting.py

```

### Algorithm Configurations

The strategy relies on a set of highly optimized parameters located in `backtesting.py`:

* `capital = Decimal("5e5")` (Base starting capital)
* `sideways_pct = Decimal("0.0015")` (Threshold to classify sideways market action)
* `quote_offset = Decimal("0.2")` (Spread capture offset from current price)
* `entry_band = Decimal("0.8")` (Minimum price deviation required to enter a trade)
* `cooldown_ticks = 20` (Tick wait time after closing a position to regain stability)
* `max_contracts = 2` (Hard cap on maximum allowable inventory)
* `stop_loss_points = Decimal("-2.0")` & `take_profit_points = Decimal("2.5")`

## Step 4: In-sample Backtesting

* **Data Segment:** 2022 Trading Year
* **Standard Inputs:** Executed using the default configurations mentioned above with a forced regime filter (09:20 - 14:20).

### Result

The algorithm successfully captured spreads with a well-controlled drawdown profile compared to pure MM strategies.

* **Holding Period Return (HPR):** +7.80%
* **Annual Net Return:** +4.98%
* **Max Drawdown:** -6.57%
* **Sharpe Ratio (Annualized):** 0.248

## Step 5: Optimization

* **Indicator Responsiveness:** We optimized the baseline indicator from a lagging SMA 80 to a `deque(maxlen=20)` (SMA 20) for high-frequency responsiveness.
* **Inventory Skewing Mechanism:** We introduced an internal metric to adjust quotes based on inventory. If holding too many Longs, the bot lowers the Ask price to incentivize a fill, restoring Delta Neutrality.
* **Regime Filtering:** Eliminated catastrophic slippage by physically halting the bot during opening/closing institutional bulk order sessions (trading only mid-session).

## Step 6: Out-of-sample Backtesting

* **Data Segment:** 2024 to Early 2026

### Result

The strategy demonstrated exceptional drawdown protection (-5.20%) during the 2024-2025 period, though it struggled during the highly volatile late-2025/2026 regime due to tight spread conditions and transaction costs overriding edge.

* **Out-of-Sample (2024 - 2025):**
* HPR: +0.29%
* Annual Net Return: +1.59%
* Max Drawdown: -5.20%


* **Out-of-Sample (Late 2025 - Early 2026):**
* HPR: -13.12%
* Annual Net Return: -31.78%
* Max Drawdown: -15.62%



## Conclusion

This project demonstrates the transition from a highly theoretical "Pure" Market Making model to a risk-adjusted, high-frequency execution engine. While a Pure MM approach yields higher absolute returns (+17.10% IS), it carries blowout risk with drawdowns exceeding -20%. By integrating **Responsive Anchoring**, **Inventory Skewing**, and **Strict Regime Constraints**, our Hybrid strategy successfully shifts the probability distribution to prioritize algorithmic survivability and capital protection against toxic market flows.

