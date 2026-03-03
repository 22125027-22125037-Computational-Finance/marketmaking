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

- **Mean Reversion (Exhaustion):** Intraday markets frequently exhibit short-term exhaustion after rapid price spikes, creating an opportunity for reverting fills.
- **Responsive Anchoring:** A high-frequency 20-tick SMA acts as a real-time dynamic baseline, tracking momentum shifts much faster than traditional lagging moving averages.
- **Directional Fading:** Instead of symmetric quoting during volatile spikes, the bot selectively quotes only the reverting side when the price deviates more than 1.0 point from the SMA baseline.

## Step 2 & 3: Data

### Data Collection

- **Target Contract:** VN30 Index Futures (VN30F1M).
- **Format:** Intraday tick data encompassing `close`, `price`, `best-bid`, `best-ask`, and `spread`.
- **Period:** The dataset spans an In-Sample period (2022) and an Out-of-Sample period (2024–2026).

### Data Processing

- **Rollover Management:** The system imports the primary contract (`VN30F1M_data.csv`) alongside the subsequent month's contract (`VN30F2M_data.csv`). It maps prices dynamically to handle expiration rollovers cleanly.
- **Formatting:** Timestamps are converted via `pd.to_datetime`. Tick prices and spreads are rounded, and forward-filling (`ffill()`) is applied to manage missing ticks and maintain data integrity.

## Implementation (How to Run)

### Environment Setup

Ensure you have Python installed along with the required libraries:

```bash
pip install pandas numpy matplotlib
```
