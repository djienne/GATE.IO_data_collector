#!/usr/bin/env python3
"""
Gate.io SPOT Market Tick Data Collector
Collects time-tagged prices, executed orders, and order book data via websockets
Specifically designed for SPOT markets only (not futures/derivatives)
"""

import asyncio
import json
import time
import websockets
import gzip
from datetime import datetime
from collections import defaultdict, deque
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any
import csv
import os
import threading
from concurrent.futures import ThreadPoolExecutor
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class TickData:
    """Base class for all tick data"""
    timestamp: float
    symbol: str
    exchange_timestamp: Optional[int] = None


@dataclass
class PriceData:
    """Price tick data"""
    timestamp: float
    symbol: str
    price: float
    size: float
    exchange_timestamp: Optional[int] = None
    side: Optional[str] = None  # 'bid' or 'ask' for BBO data


@dataclass
class TradeData:
    """Trade execution data"""
    timestamp: float
    symbol: str
    price: float
    size: float
    side: str  # 'buy' or 'sell'
    exchange_timestamp: Optional[int] = None
    trade_id: Optional[str] = None


@dataclass
class OrderBookLevel:
    """Order book level data"""
    price: float
    size: float


@dataclass
class OrderBookData:
    """Order book snapshot data"""
    timestamp: float
    symbol: str
    bids: List[OrderBookLevel]
    asks: List[OrderBookLevel]
    exchange_timestamp: Optional[int] = None
    sequence: Optional[int] = None


class DataStats:
    """Track data collection statistics"""
    def __init__(self):
        self.start_time = time.time()
        self.counters = defaultdict(int)
        self.last_update = time.time()
        self.recent_data = defaultdict(lambda: deque(maxlen=100))
    
    def update(self, data_type: str, data: Any = None):
        self.counters[data_type] += 1
        self.last_update = time.time()
        if data:
            self.recent_data[data_type].append(data)
    
    def get_summary(self) -> Dict[str, Any]:
        runtime = time.time() - self.start_time
        return {
            'runtime_seconds': runtime,
            'runtime_formatted': f"{runtime//3600:.0f}h {(runtime%3600)//60:.0f}m {runtime%60:.0f}s",
            'counters': dict(self.counters),
            'rates_per_minute': {k: v / (runtime / 60) for k, v in self.counters.items() if runtime > 0},
            'last_update': datetime.fromtimestamp(self.last_update).strftime('%H:%M:%S')
        }


class GateioDataCollector:
    """Main data collector class for Gate.io"""
    
    # Gate.io WebSocket endpoints
    SPOT_WS_URL = "wss://api.gateio.ws/ws/v4/"
    
    def __init__(self, symbols: List[str], output_dir: str = "data", orderbook_depth: int = 20, market: str = "spot"):
        """
        Initialize Gate.io data collector
        
        Args:
            symbols: List of trading pairs (e.g., ["BTC_USDT", "ETH_USDT"])
            output_dir: Directory to save data files
            orderbook_depth: Number of order book levels to capture
            market: Market type ("spot" or "futures")
        """
        self.symbols = symbols
        self.output_dir = output_dir
        self.orderbook_depth = orderbook_depth
        self.market = market
        self.stats = DataStats()
        
        # WebSocket connections
        self.websockets = {}
        
        # Create separate buffers for each symbol
        self.data_buffers = {}
        # Maintain full order book state for each symbol
        self.orderbook_states = {}
        for symbol in symbols:
            self.data_buffers[symbol] = {
                'prices': deque(maxlen=10000),
                'trades': deque(maxlen=10000),
                'orderbooks': deque(maxlen=1000)
            }
            # Initialize order book state
            self.orderbook_states[symbol] = {
                'bids': {},  # price -> size mapping
                'asks': {},  # price -> size mapping
                'last_sequence': None
            }
        
        self.running = False
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        # File paths for each symbol
        self.symbol_files = {}
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize CSV files
        self._init_csv_files()
    
    def _init_csv_files(self):
        """Initialize CSV files for data storage - separate files per symbol"""
        
        for symbol in self.symbols:
            # Clean symbol name for filename (replace slash with underscore)
            clean_symbol = symbol.replace('/', '_')
            
            # Initialize file paths for this symbol
            self.symbol_files[symbol] = {}
            
            # Price data CSV for this symbol
            price_file = os.path.join(self.output_dir, f"prices_{clean_symbol}.csv")
            self.symbol_files[symbol]['prices'] = price_file
            # Only write header if file doesn't exist
            if not os.path.exists(price_file):
                with open(price_file, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(['timestamp', 'price', 'size', 'side', 'exchange_timestamp'])
            
            # Trade data CSV for this symbol
            trade_file = os.path.join(self.output_dir, f"trades_{clean_symbol}.csv")
            self.symbol_files[symbol]['trades'] = trade_file
            # Only write header if file doesn't exist
            if not os.path.exists(trade_file):
                with open(trade_file, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(['timestamp', 'price', 'size', 'side', 'trade_id', 'exchange_timestamp'])
            
            # Order book CSV for this symbol (configurable depth)
            orderbook_file = os.path.join(self.output_dir, f"orderbooks_{clean_symbol}.csv")
            self.symbol_files[symbol]['orderbooks'] = orderbook_file
            # Only write header if file doesn't exist
            if not os.path.exists(orderbook_file):
                with open(orderbook_file, 'w', newline='') as f:
                    writer = csv.writer(f)
                    headers = ['timestamp', 'sequence', 'exchange_timestamp']
                    for i in range(self.orderbook_depth):
                        headers.extend([f'bid_price_{i}', f'bid_size_{i}', f'ask_price_{i}', f'ask_size_{i}'])
                    writer.writerow(headers)
    
    def _write_to_csv(self, filename: str, data: List[Any]):
        """Write data to CSV file"""
        try:
            with open(filename, 'a', newline='') as f:
                writer = csv.writer(f)
                for item in data:
                    if isinstance(item, dict):
                        row = []
                        # Ensure proper order for CSV columns
                        if 'trades' in filename:
                            row = [item.get('timestamp'), item.get('price'), item.get('size'), 
                                   item.get('side'), item.get('trade_id'), item.get('exchange_timestamp')]
                        elif 'prices' in filename:
                            row = [item.get('timestamp'), item.get('price'), item.get('size'), 
                                   item.get('side'), item.get('exchange_timestamp')]
                        elif 'orderbooks' in filename:
                            row = [item.get('timestamp'), item.get('sequence'), item.get('exchange_timestamp')]
                            for i in range(self.orderbook_depth):
                                row.extend([
                                    item.get(f'bid_price_{i}'), item.get(f'bid_size_{i}'),
                                    item.get(f'ask_price_{i}'), item.get(f'ask_size_{i}')
                                ])
                        writer.writerow(row)
                    else:
                        writer.writerow(asdict(item).values())
        except Exception as e:
            logger.error(f"Error writing to CSV {filename}: {e}")
    
    def _handle_ticker_data(self, symbol: str, data: Dict[str, Any]):
        """Handle ticker/BBO data from Gate.io"""
        try:
            timestamp = time.time()
            
            # Gate.io ticker format uses 'highest_bid' and 'lowest_ask'
            if 'highest_bid' in data and data['highest_bid']:
                bid_data = {
                    'timestamp': timestamp,
                    'price': float(data['highest_bid']),
                    'size': 0,  # Size not provided in ticker data
                    'side': 'bid',
                    'exchange_timestamp': data.get('time_ms', data.get('timestamp_ms'))
                }
                self.data_buffers[symbol]['prices'].append(bid_data)
            
            if 'lowest_ask' in data and data['lowest_ask']:
                ask_data = {
                    'timestamp': timestamp,
                    'price': float(data['lowest_ask']),
                    'size': 0,  # Size not provided in ticker data
                    'side': 'ask',
                    'exchange_timestamp': data.get('time_ms', data.get('timestamp_ms'))
                }
                self.data_buffers[symbol]['prices'].append(ask_data)
            
            self.stats.update('ticker_updates')
        except Exception as e:
            logger.error(f"Error handling ticker data: {e}")
            logger.error(f"Ticker data that caused error: {data}")
    
    def _handle_trade_data(self, symbol: str, data: List[Dict[str, Any]]):
        """Handle trade data from Gate.io"""
        try:
            timestamp = time.time()
            
            for trade in data:
                # Gate.io trade format
                trade_data = {
                    'timestamp': timestamp,
                    'price': float(trade['price']),
                    'size': float(trade['amount']),
                    'side': trade['side'],  # 'buy' or 'sell'
                    'trade_id': str(trade.get('id', '')),
                    'exchange_timestamp': trade.get('create_time_ms', trade.get('create_time'))
                }
                self.data_buffers[symbol]['trades'].append(trade_data)
            
            self.stats.update('trades', len(data))
        except Exception as e:
            logger.error(f"Error handling trade data: {e}")
    
    def _handle_orderbook_data(self, symbol: str, data: Dict[str, Any], is_snapshot: bool = False):
        """Handle order book data from Gate.io with proper state management"""
        try:
            timestamp = time.time()
            sequence = data.get('u', data.get('id', data.get('lastUpdateId')))
            
            # Get current order book state for this symbol
            ob_state = self.orderbook_states[symbol]
            
            if is_snapshot:
                # Full snapshot - replace entire order book
                ob_state['bids'].clear()
                ob_state['asks'].clear()
            
            # Update the order book state with new data
            # Gate.io uses 'bids'/'asks' for snapshots and 'b'/'a' for updates
            bid_key = 'bids' if 'bids' in data else 'b'
            ask_key = 'asks' if 'asks' in data else 'a'
            
            if bid_key in data:
                for bid in data[bid_key]:
                    price = float(bid[0])
                    size = float(bid[1])
                    if size == 0 and not is_snapshot:
                        # Size 0 means remove this level (only for updates, not snapshots)
                        ob_state['bids'].pop(price, None)
                    else:
                        # Update or add this level
                        ob_state['bids'][price] = size
            
            if ask_key in data:
                for ask in data[ask_key]:
                    price = float(ask[0])
                    size = float(ask[1])
                    if size == 0 and not is_snapshot:
                        # Size 0 means remove this level (only for updates, not snapshots)
                        ob_state['asks'].pop(price, None)
                    else:
                        # Update or add this level
                        ob_state['asks'][price] = size
            
            # Update sequence
            ob_state['last_sequence'] = sequence
            
            # Only write data if we have sufficient levels (at least 5 bids and asks)
            if len(ob_state['bids']) >= 5 and len(ob_state['asks']) >= 5:
                # Create sorted lists for bids (descending) and asks (ascending)
                # Bids should be highest to lowest price
                sorted_bids = sorted(ob_state['bids'].items(), key=lambda x: x[0], reverse=True)
                # Asks should be lowest to highest price
                sorted_asks = sorted(ob_state['asks'].items(), key=lambda x: x[0])
                
                # Prepare data for CSV (flatten configurable depth levels)
                csv_row = {
                    'timestamp': timestamp,
                    'sequence': sequence,
                    'exchange_timestamp': data.get('t', data.get('time', data.get('timestamp')))
                }
                
                # Fill bid levels (best bids first)
                for i in range(self.orderbook_depth):
                    if i < len(sorted_bids):
                        price, size = sorted_bids[i]
                        csv_row[f'bid_price_{i}'] = price
                        csv_row[f'bid_size_{i}'] = size
                    else:
                        # No more bid levels available
                        csv_row[f'bid_price_{i}'] = None
                        csv_row[f'bid_size_{i}'] = None
                
                # Fill ask levels (best asks first)
                for i in range(self.orderbook_depth):
                    if i < len(sorted_asks):
                        price, size = sorted_asks[i]
                        csv_row[f'ask_price_{i}'] = price
                        csv_row[f'ask_size_{i}'] = size
                    else:
                        # No more ask levels available
                        csv_row[f'ask_price_{i}'] = None
                        csv_row[f'ask_size_{i}'] = None
                
                self.data_buffers[symbol]['orderbooks'].append(csv_row)
                self.stats.update('orderbook_updates')
            # Note: We now consistently get full snapshots from Gate.io, so no need to skip writes
            
        except Exception as e:
            logger.error(f"Error handling order book data: {e}")
            logger.error(f"Order book data that caused error: {data}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
    
    async def _handle_websocket_message(self, websocket, message):
        """Handle incoming WebSocket message"""
        try:
            # Check if message is bytes (compressed)
            if isinstance(message, bytes):
                try:
                    # Try to decompress if it's gzipped
                    message = gzip.decompress(message).decode('utf-8')
                except:
                    # If decompression fails, try to decode as utf-8 directly
                    message = message.decode('utf-8')
            
            # Debug: log raw message for troubleshooting
            logger.debug(f"Raw message: {message}")
            
            data = json.loads(message)
            
            # Gate.io message format
            if 'channel' in data:
                channel = data['channel']
                event = data.get('event')
                result = data.get('result')
                
                # Handle subscription confirmations first
                if event == 'subscribe':
                    if result and result.get('status') == 'success':
                        logger.info(f"Successfully subscribed to {channel}")
                    else:
                        logger.error(f"Failed to subscribe to {channel}: {result}")
                        return
                
                # Handle different channel types for updates
                if event == 'update' or result:
                    if channel.startswith('spot.tickers'):
                        # Ticker/BBO data
                        if result and 'currency_pair' in result:
                            symbol = result['currency_pair']
                            self._handle_ticker_data(symbol, result)
                    
                    elif channel.startswith('spot.trades'):
                        # Trade data
                        if result and 'currency_pair' in result:
                            symbol = result['currency_pair']
                            self._handle_trade_data(symbol, [result])  # Wrap in list for compatibility
                    
                    elif channel.startswith('spot.order_book_update'):
                        # Order book incremental updates
                        if result and 's' in result:
                            symbol = result['s']
                            self._handle_orderbook_data(symbol, result, is_snapshot=False)
                    
                    elif channel.startswith('spot.order_book'):
                        # Order book snapshots
                        if result:
                            # For snapshots, symbol might be in different fields
                            symbol = None
                            if 's' in result:
                                symbol = result['s']
                            elif 'currency_pair' in result:
                                symbol = result['currency_pair']
                            
                            if symbol:
                                # Process snapshot data
                                self._handle_orderbook_data(symbol, result, is_snapshot=True)
            
            # Handle ping/pong
            elif 'pong' in data:
                logger.debug("Received pong")
                    
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error - message: {message[:200]}..., error: {e}")
        except Exception as e:
            logger.error(f"Error handling websocket message - message: {str(message)[:200]}..., error: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
    
    async def _websocket_handler(self, symbol: str):
        """Handle WebSocket connection for a symbol"""
        ws_url = self.SPOT_WS_URL
        
        while self.running:
            try:
                async with websockets.connect(ws_url) as websocket:
                    logger.info(f"Connected to Gate.io WebSocket for {symbol}")
                    
                    # Subscribe to channels for this symbol
                    # Subscribe to ticker (BBO)
                    ticker_sub = {
                        "time": int(time.time()),
                        "channel": f"spot.tickers",
                        "event": "subscribe",
                        "payload": [symbol]
                    }
                    await websocket.send(json.dumps(ticker_sub))
                    
                    # Subscribe to trades
                    trades_sub = {
                        "time": int(time.time()),
                        "channel": f"spot.trades",
                        "event": "subscribe",
                        "payload": [symbol]
                    }
                    await websocket.send(json.dumps(trades_sub))
                    
                    # Subscribe to order book snapshots (full order book)
                    orderbook_snapshot_sub = {
                        "time": int(time.time()),
                        "channel": "spot.order_book",
                        "event": "subscribe",
                        "payload": [symbol, str(self.orderbook_depth), "100ms"]  # symbol, depth, frequency
                    }
                    await websocket.send(json.dumps(orderbook_snapshot_sub))
                    
                    # Handle incoming messages
                    while self.running:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=30)
                            await self._handle_websocket_message(websocket, message)
                        except asyncio.TimeoutError:
                            # Send ping to keep connection alive
                            ping = {"time": int(time.time()), "channel": "spot.ping"}
                            await websocket.send(json.dumps(ping))
                        except websockets.exceptions.ConnectionClosed:
                            logger.warning(f"WebSocket connection closed for {symbol}")
                            break
                            
            except Exception as e:
                logger.error(f"WebSocket error for {symbol}: {e}")
                if self.running:
                    await asyncio.sleep(5)  # Wait before reconnecting
    
    def _flush_buffers(self):
        """Flush data buffers to CSV files - separate files per symbol"""
        try:
            for symbol in self.symbols:
                symbol_buffers = self.data_buffers[symbol]
                symbol_files = self.symbol_files[symbol]
                
                # Flush prices for this symbol
                if symbol_buffers['prices']:
                    prices_to_write = list(symbol_buffers['prices'])
                    symbol_buffers['prices'].clear()
                    self.executor.submit(self._write_to_csv, symbol_files['prices'], prices_to_write)
                
                # Flush trades for this symbol
                if symbol_buffers['trades']:
                    trades_to_write = list(symbol_buffers['trades'])
                    symbol_buffers['trades'].clear()
                    self.executor.submit(self._write_to_csv, symbol_files['trades'], trades_to_write)
                
                # Flush order books for this symbol
                if symbol_buffers['orderbooks']:
                    orderbooks_to_write = list(symbol_buffers['orderbooks'])
                    symbol_buffers['orderbooks'].clear()
                    self.executor.submit(self._write_to_csv, symbol_files['orderbooks'], orderbooks_to_write)
                
        except Exception as e:
            logger.error(f"Error flushing buffers: {e}")
    
    def _print_summary(self):
        """Print data collection summary"""
        summary = self.stats.get_summary()
        print("\n" + "="*60)
        print(f"DATA COLLECTION SUMMARY - {summary['last_update']}")
        print("="*60)
        print(f"Runtime: {summary['runtime_formatted']}")
        print(f"Data collected:")
        for data_type, count in summary['counters'].items():
            rate = summary['rates_per_minute'].get(data_type, 0)
            print(f"  {data_type}: {count:,} ({rate:.1f}/min)")
        
        print(f"\nBuffer sizes by symbol:")
        for symbol in self.symbols:
            symbol_buffers = self.data_buffers[symbol]
            total_buffered = sum(len(buffer) for buffer in symbol_buffers.values())
            print(f"  {symbol}: {total_buffered} ({len(symbol_buffers['prices'])} prices, "
                  f"{len(symbol_buffers['trades'])} trades, {len(symbol_buffers['orderbooks'])} orderbooks)")
        
        print("="*60)
    
    async def _run_collection(self):
        """Run async data collection"""
        # Create tasks for each symbol
        tasks = []
        for symbol in self.symbols:
            task = asyncio.create_task(self._websocket_handler(symbol))
            tasks.append(task)
        
        # Wait for all tasks
        await asyncio.gather(*tasks)
    
    def _periodic_flush(self):
        """Periodically flush buffers to disk"""
        while self.running:
            time.sleep(5)  # Flush every 5 seconds
            self._flush_buffers()
    
    def _periodic_summary(self):
        """Periodically print collection summary"""
        while self.running:
            time.sleep(30)  # Print summary every 30 seconds
            if self.running:
                self._print_summary()
    
    def start_collection(self):
        """Start data collection"""
        print(f"Starting Gate.io data collection for symbols: {self.symbols}")
        print(f"Output directory: {self.output_dir}")
        print(f"Market: {self.market}")
        
        self.running = True
        
        try:
            # Start background tasks
            flush_thread = threading.Thread(target=self._periodic_flush, daemon=True)
            flush_thread.start()
            
            summary_thread = threading.Thread(target=self._periodic_summary, daemon=True)
            summary_thread.start()
            
            print("Data collection started. Press Ctrl+C to stop.")
            
            # Run async event loop
            asyncio.run(self._run_collection())
                
        except KeyboardInterrupt:
            print("\nShutting down...")
            self.stop_collection()
        except Exception as e:
            logger.error(f"Error during data collection: {e}")
            self.stop_collection()
    
    def stop_collection(self):
        """Stop data collection"""
        self.running = False
        
        # Final flush
        self._flush_buffers()
        
        # Wait for executor to finish
        self.executor.shutdown(wait=True)
        
        # Final summary
        self._print_summary()
        print(f"\nData files saved in: {self.output_dir}")


def main():
    """Main function"""
    # Configuration
    # Gate.io uses underscore format for trading pairs (e.g., BTC_USDT)
    SYMBOLS = ["BTC_USDT", "ETH_USDT", "SOL_USDT", "WOD_USDT"]  # Add more symbols as needed
    OUTPUT_DIR = "gateio_data"
    ORDERBOOK_DEPTH = 20  # Number of order book levels to capture (default: 20)
    MARKET = "spot"  # "spot" or "futures"
    
    print("Gate.io Tick Data Collector")
    print("===========================")
    print(f"Order book depth: {ORDERBOOK_DEPTH} levels")
    print(f"Market type: {MARKET}")
    
    # Create collector with configurable order book depth
    collector = GateioDataCollector(SYMBOLS, OUTPUT_DIR, orderbook_depth=ORDERBOOK_DEPTH, market=MARKET)
    
    # Start collection
    collector.start_collection()


if __name__ == "__main__":
    main()
