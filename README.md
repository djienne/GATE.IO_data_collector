# Gate.io SPOT Market Data Collector

A Python script for collecting real-time tick-by-tick data from Gate.io's SPOT markets using WebSockets. It captures and saves price tickers, trade executions, and full order book data for specified cryptocurrency pairs.

## Features

- **Real-time Data**: Subscribes to Gate.io's WebSocket API for live data.
- **Multiple Data Types**: Collects best bid/ask prices, individual trades, and full order book snapshots.
- **Organized Storage**: Saves data in per-symbol CSV files.
- **Resilient**: Handles network interruptions with automatic reconnection.
- **No API Keys Needed**: Accesses public market data only.

## Data Collected

- **Price Tickers (`prices_*.csv`)**: Best bid/ask updates.
- **Trade Executions (`trades_*.csv`)**: Real-time trade transactions.
- **Order Book Snapshots (`orderbooks_*.csv`)**: Full market depth snapshots.

## Usage

1.  **Configure**: Edit `gate_data_collector.py` to set `SYMBOLS`, `OUTPUT_DIR`, and `ORDERBOOK_DEPTH`.
2.  **Run**:
    ```bash
    python gate_data_collector.py
    ```
3.  **Stop**: Press `Ctrl+C` to stop and save the data.

## Avellaneda-Stoikov Parameter Calculation

The repository includes a script to calculate optimal market making parameters based on the Avellaneda-Stoikov model.

### `calculate_avellaneda_parameters.py`

This script analyzes the collected market data (prices and trades) to estimate the key parameters for the Avellaneda-Stoikov market making model.

- **Volatility (σ)**: Measures the standard deviation of log-returns.
- **Order Arrival Intensity (A, k)**: Models the rate of order arrivals at different price levels.
- **Risk Aversion (γ)**: Optimizes the trade-off between inventory risk and spread profit via backtesting.

It outputs the calculated parameters, reservation price, and optimal bid/ask quotes.

### `avellaneda_parameters_WOD.json`

A sample output file containing the calculated parameters for the `WOD` ticker. This file includes:
- **Market Data**: Mid-price, volatility (sigma), and intensity parameters (A, k).
- **Optimal Parameters**: The calculated risk aversion parameter (gamma).
- **Calculated Prices**: The reservation price and optimal bid/ask limit orders based on the model.
