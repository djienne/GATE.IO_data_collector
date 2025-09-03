# Gate.io SPOT Market Data Collector

A Python script for collecting real-time tick data from Gate.io's SPOT markets via WebSocket connections. Collects price tickers, trade executions, and full order book data for cryptocurrency pairs.

## Features

- **Real-time data collection** via Gate.io WebSocket API
- **Multiple data types**: Best bid/ask prices, trade executions, and full order book snapshots
- **Configurable order book depth** (default: 20 levels)
- **Per-symbol CSV files** for organized data storage
- **Automatic reconnection** and error handling
- **No API keys required** (public market data only)

## Data Types Collected

### 1. Price Tickers (`prices_*.csv`)
Best bid and ask prices from ticker updates:
- **File format**: `prices_{SYMBOL}.csv`
- **Update frequency**: ~1 second
- **Columns**: `timestamp`, `price`, `size`, `side`, `exchange_timestamp`
- **Example**: Best bid/ask for BTC_USDT

### 2. Trade Executions (`trades_*.csv`)
Individual trade transactions:
- **File format**: `trades_{SYMBOL}.csv`
- **Update frequency**: Real-time (as trades occur)
- **Columns**: `timestamp`, `price`, `size`, `side`, `trade_id`, `exchange_timestamp`
- **Example**: All BTC_USDT trades with buy/sell direction

### 3. Order Book Snapshots (`orderbooks_*.csv`)
Full market depth with configurable levels:
- **File format**: `orderbooks_{SYMBOL}.csv`
- **Update frequency**: 100ms snapshots
- **Columns**: `timestamp`, `sequence`, `exchange_timestamp`, `bid_price_0`, `bid_size_0`, `ask_price_0`, `ask_size_0`, ... (up to configured depth)
- **Example**: 20-level order book for BTC_USDT

## Configuration

Edit the `main()` function to customize:

```python
SYMBOLS = ["BTC_USDT", "ETH_USDT", "SOL_USDT"]  # Trading pairs to collect
OUTPUT_DIR = "gateio_data"                      # Output directory
ORDERBOOK_DEPTH = 20                            # Number of order book levels
```

## Usage

```bash
python gate_data_collector.py
```

Press `Ctrl+C` to stop collection and save final data.

## Output Files

All data is saved to the `gateio_data/` directory:

```
gateio_data/
├── prices_BTC_USDT.csv      # Best bid/ask prices
├── prices_ETH_USDT.csv
├── prices_SOL_USDT.csv
├── trades_BTC_USDT.csv      # Trade executions
├── trades_ETH_USDT.csv
├── trades_SOL_USDT.csv
├── orderbooks_BTC_USDT.csv  # Full order book snapshots
├── orderbooks_ETH_USDT.csv
└── orderbooks_SOL_USDT.csv
```

## Data Format Examples

### Prices File
```csv
timestamp,price,size,side,exchange_timestamp
1756903691.416256,111265.9,0,bid,
1756903691.416256,111266.0,0,ask,
```

### Trades File
```csv
timestamp,price,size,side,trade_id,exchange_timestamp
1756903695.123456,111270.5,0.05,buy,12345678,1756903695000
```

### Order Book File
```csv
timestamp,sequence,exchange_timestamp,bid_price_0,bid_size_0,ask_price_0,ask_size_0,bid_price_1,bid_size_1,ask_price_1,ask_size_1,...
1756903700.123456,26901234567,1756903700000,111265.9,1.5,111266.0,2.1,111265.8,0.8,111266.1,1.2,...
```

## Requirements

- Python 3.7+
- `websockets` library
- `asyncio` (built-in)
- No API keys required

## Installation

```bash
pip install websockets
```

## Technical Details

- Uses Gate.io WebSocket API v4
- Maintains full order book state per symbol
- Handles incremental order book updates
- Automatic buffer flushing every 5 seconds
- Statistics summary every 30 seconds
- Proper bid/ask sorting (bids descending, asks ascending)

## Notes

- **Spot markets only** (not futures/derivatives)
- Data is stored with microsecond precision timestamps
- Order book maintains consistent depth levels
- Network interruptions trigger automatic reconnection
- All data is in CSV format for easy analysis