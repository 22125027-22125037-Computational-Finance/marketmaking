"""
Kafka Market Data Client - Async/await implementation.

Provides two main operations:
1. query() - Get current cached quote snapshot
2. subscribe() - Real-time updates via Kafka consumer

Built on kafka-python-ng with proper async handling.
"""

import asyncio
import json
import logging
import os
import threading
from typing import Optional, Callable, Dict, Set, Any
from datetime import datetime, timezone

try:
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError
except ImportError:
    raise ImportError(
        "kafka-python-ng is required. Install with: pip install 'kafka-python-ng>=2.2.0'"
    )

from .quote import QuoteSnapshot


logger = logging.getLogger(__name__)


class KafkaMarketDataClient:
    """
    Async Kafka client for market data.
    
    Features:
    - subscribe(): Kafka consumer for real-time updates
    - query(): Get cached quote (from last received message)
    - Event-driven callbacks
    - Background consumer thread
    - Resource cleanup
    
    Example:
        import asyncio
        import os
        from dotenv import load_dotenv
        from paperbroker.market_data import KafkaMarketDataClient
        
        load_dotenv()
        
        async def main():
            client = KafkaMarketDataClient(
                bootstrap_servers=os.getenv('MARKET_KAFKA_BOOTSTRAP_SERVERS'),
                username=os.getenv('MARKET_KAFKA_USERNAME'),
                password=os.getenv('MARKET_KAFKA_PASSWORD')
            )
            
            # Define callback
            def on_quote(instrument, quote):
                print(f"{instrument}: {quote.latest_matched_price}")
            
            # Subscribe to instrument
            await client.subscribe('HNXDS:VN30F2511', on_quote)
            
            # Start consuming
            await client.start()
            
            # Keep running
            try:
                while True:
                    await asyncio.sleep(1)
            finally:
                await client.stop()
        
        asyncio.run(main())
    """
    
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        env_id: Optional[str] = None,
        group_id: Optional[str] = None,
        auto_offset_reset: str = "latest",
        merge_updates: bool = True,
    ):
        """
        Initialize Kafka market data client.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers (comma-separated).
                              Falls back to PAPERBROKER_KAFKA_BOOTSTRAP_SERVERS env var.
            username: SASL username. Falls back to PAPERBROKER_KAFKA_USERNAME env var.
            password: SASL password. Falls back to PAPERBROKER_KAFKA_PASSWORD env var.
            env_id: Environment ID for topic prefix (e.g., 'real', 'test').
                   Falls back to PAPERBROKER_ENV_ID env var.
                   Topic format: '{env_id}.{exchange}.{symbol}'
            group_id: Consumer group ID. If None, generates unique group per instance.
            auto_offset_reset: Where to start consuming ('latest' or 'earliest').
            merge_updates: If True, merge updates with previous values (full snapshot).
                          If False, return only changed fields.
        """
        # Load from env if not provided
        self.bootstrap_servers = bootstrap_servers or os.getenv('PAPERBROKER_KAFKA_BOOTSTRAP_SERVERS')
        self.username = username or os.getenv('PAPERBROKER_KAFKA_USERNAME')
        self.password = password or os.getenv('PAPERBROKER_KAFKA_PASSWORD')
        self.env_id = env_id or os.getenv('PAPERBROKER_ENV_ID')
        
        if not self.bootstrap_servers:
            raise ValueError("bootstrap_servers required. Set PAPERBROKER_KAFKA_BOOTSTRAP_SERVERS env var.")
        
        if not self.env_id:
            raise ValueError("env_id required. Set PAPERBROKER_ENV_ID env var (e.g., 'real', 'test').")
        
        self.group_id = group_id or f"paperbroker-{os.getpid()}-{id(self)}"
        self.auto_offset_reset = auto_offset_reset
        self.merge_updates = merge_updates
        
        # Consumer
        self._consumer: Optional[KafkaConsumer] = None
        self._consumer_thread: Optional[threading.Thread] = None
        self._running = False
        self._stop_event = threading.Event()
        
        # Subscription management
        self._subscriptions: Dict[str, Set[Callable]] = {}  # instrument -> callbacks
        self._topics: Set[str] = set()  # subscribed topics
        
        # Cache for merge mode (instrument -> last_quote)
        self._quote_cache: Dict[str, QuoteSnapshot] = {}
        
        # Lock for thread safety
        self._lock = threading.RLock()
        
        logger.info(
            f"Initialized KafkaMarketDataClient: {self.bootstrap_servers}, "
            f"merge_updates={merge_updates}"
        )
    
    def _instrument_to_topic(self, instrument: str) -> str:
        """
        Convert instrument to Kafka topic name.
        
        Topic format: {env_id}.{exchange}.{symbol}
        Example: 'HNXDS:VN30F2602' with env_id='real' -> 'real.HNXDS.VN30F2602'
        """
        # Replace : with .
        base_topic = instrument.replace(':', '.')
        # Add env_id prefix
        topic = f"{self.env_id}.{base_topic}"
        return topic
    
    def _topic_to_instrument(self, topic: str) -> str:
        """
        Convert Kafka topic back to instrument name.
        
        Topic format: {env_id}.{exchange}.{symbol}
        Example: 'real.HNXDS.VN30F2602' -> 'HNXDS:VN30F2602'
        """
        # Topic format: env_id.exchange.symbol
        parts = topic.split('.', 2)  # Split into max 3 parts
        if len(parts) == 3:
            # parts[0] = env_id, parts[1] = exchange, parts[2] = symbol
            return f"{parts[1]}:{parts[2]}"
        elif len(parts) == 2:
            # Fallback: exchange.symbol (no env_id prefix)
            return f"{parts[0]}:{parts[1]}"
        return topic
    
    def _create_consumer(self) -> KafkaConsumer:
        """Create Kafka consumer with SASL authentication."""
        config = {
            'bootstrap_servers': self.bootstrap_servers.split(','),
            'group_id': self.group_id,
            'auto_offset_reset': self.auto_offset_reset,
            'enable_auto_commit': True,
            'value_deserializer': lambda m: json.loads(m.decode('utf-8')) if m else None,
            'key_deserializer': lambda k: k.decode('utf-8') if k else None,
            'request_timeout_ms': 30000,
            'session_timeout_ms': 10000,
        }
        
        # Add SASL authentication if credentials provided
        if self.username and self.password:
            config.update({
                'security_protocol': 'SASL_PLAINTEXT',
                'sasl_mechanism': 'PLAIN',
                'sasl_plain_username': self.username,
                'sasl_plain_password': self.password,
            })
        
        try:
            return KafkaConsumer(**config)
        except KafkaError as e:
            logger.error(f"Failed to create Kafka consumer: {e}")
            raise ConnectionError(
                f"Failed to connect to Kafka at {self.bootstrap_servers}. "
                f"Please check server availability and credentials. Error: {e}"
            )
    
    def _normalize_quote(self, instrument: str, data: Dict[str, Any]) -> QuoteSnapshot:
        """
        Normalize Kafka message to QuoteSnapshot.
        
        Expected format:
        {
            "instrument": "HNXDS:VN100F2602",
            "entries": {
                "latest_matched_price": 1921.7,
                "bid_price_1": 1915.2,
                ...
            }
        }
        """
        entries = data.get('entries', data)  # Support both nested and flat format
        
        quote = QuoteSnapshot(
            instrument=instrument,
            
            # Latest trade
            latest_matched_price=entries.get('latest_matched_price'),
            latest_matched_quantity=entries.get('latest_matched_quantity'),
            
            # Best bid/ask (Level 1)
            bid_price_1=entries.get('bid_price_1'),
            bid_quantity_1=entries.get('bid_quantity_1'),
            ask_price_1=entries.get('ask_price_1'),
            ask_quantity_1=entries.get('ask_quantity_1'),
            
            # Level 2
            bid_price_2=entries.get('bid_price_2'),
            bid_quantity_2=entries.get('bid_quantity_2'),
            ask_price_2=entries.get('ask_price_2'),
            ask_quantity_2=entries.get('ask_quantity_2'),
            
            # Level 3
            bid_price_3=entries.get('bid_price_3'),
            bid_quantity_3=entries.get('bid_quantity_3'),
            ask_price_3=entries.get('ask_price_3'),
            ask_quantity_3=entries.get('ask_quantity_3'),
            
            # Reference prices
            ref_price=entries.get('ref_price'),
            ceiling_price=entries.get('ceiling_price'),
            floor_price=entries.get('floor_price'),
            
            # Session stats
            open_price=entries.get('open_price'),
            highest_price=entries.get('highest_price'),
            lowest_price=entries.get('lowest_price'),
            total_matched_quantity=entries.get('total_matched_quantity'),
            
            # Metadata
            timestamp=datetime.now(timezone.utc).timestamp(),
            datetime_str=datetime.now(timezone.utc).isoformat(),
            raw_data=data,
        )
        
        return quote
    
    def _merge_quotes(self, old: Optional[QuoteSnapshot], new: QuoteSnapshot) -> QuoteSnapshot:
        """Merge new quote with cached values for full snapshot."""
        if old is None:
            return new
        
        # Create merged quote with old values as base
        merged_data = {
            'instrument': new.instrument,
            'latest_matched_price': new.latest_matched_price if new.latest_matched_price is not None else old.latest_matched_price,
            'latest_matched_quantity': new.latest_matched_quantity if new.latest_matched_quantity is not None else old.latest_matched_quantity,
            'bid_price_1': new.bid_price_1 if new.bid_price_1 is not None else old.bid_price_1,
            'bid_quantity_1': new.bid_quantity_1 if new.bid_quantity_1 is not None else old.bid_quantity_1,
            'ask_price_1': new.ask_price_1 if new.ask_price_1 is not None else old.ask_price_1,
            'ask_quantity_1': new.ask_quantity_1 if new.ask_quantity_1 is not None else old.ask_quantity_1,
            'bid_price_2': new.bid_price_2 if new.bid_price_2 is not None else old.bid_price_2,
            'bid_quantity_2': new.bid_quantity_2 if new.bid_quantity_2 is not None else old.bid_quantity_2,
            'ask_price_2': new.ask_price_2 if new.ask_price_2 is not None else old.ask_price_2,
            'ask_quantity_2': new.ask_quantity_2 if new.ask_quantity_2 is not None else old.ask_quantity_2,
            'bid_price_3': new.bid_price_3 if new.bid_price_3 is not None else old.bid_price_3,
            'bid_quantity_3': new.bid_quantity_3 if new.bid_quantity_3 is not None else old.bid_quantity_3,
            'ask_price_3': new.ask_price_3 if new.ask_price_3 is not None else old.ask_price_3,
            'ask_quantity_3': new.ask_quantity_3 if new.ask_quantity_3 is not None else old.ask_quantity_3,
            'ref_price': new.ref_price if new.ref_price is not None else old.ref_price,
            'ceiling_price': new.ceiling_price if new.ceiling_price is not None else old.ceiling_price,
            'floor_price': new.floor_price if new.floor_price is not None else old.floor_price,
            'open_price': new.open_price if new.open_price is not None else old.open_price,
            'highest_price': new.highest_price if new.highest_price is not None else old.highest_price,
            'lowest_price': new.lowest_price if new.lowest_price is not None else old.lowest_price,
            'total_matched_quantity': new.total_matched_quantity if new.total_matched_quantity is not None else old.total_matched_quantity,
            'timestamp': new.timestamp,
            'datetime_str': new.datetime_str,
            'raw_data': new.raw_data,
        }
        
        return QuoteSnapshot(**merged_data)
    
    def _consumer_loop(self):
        """Background consumer thread loop."""
        logger.info("Kafka consumer thread started")
        
        try:
            while not self._stop_event.is_set():
                # Poll for messages with timeout
                messages = self._consumer.poll(timeout_ms=1000)
                
                for topic_partition, records in messages.items():
                    for record in records:
                        try:
                            self._process_message(record)
                        except Exception as e:
                            logger.error(f"Error processing message: {e}", exc_info=True)
                
        except Exception as e:
            logger.error(f"Consumer loop error: {e}", exc_info=True)
        finally:
            logger.info("Kafka consumer thread stopped")
    
    def _process_message(self, record):
        """Process a single Kafka message."""
        topic = record.topic
        value = record.value
        
        if not value:
            return
        
        # Get instrument from message or derive from topic
        instrument = value.get('instrument') or self._topic_to_instrument(topic)
        
        # Normalize to QuoteSnapshot
        quote = self._normalize_quote(instrument, value)
        
        # Merge with cache if enabled
        with self._lock:
            if self.merge_updates:
                old_quote = self._quote_cache.get(instrument)
                quote = self._merge_quotes(old_quote, quote)
            
            # Update cache
            self._quote_cache[instrument] = quote
            
            # Get callbacks for this instrument
            callbacks = list(self._subscriptions.get(instrument, []))
        
        # Call callbacks (outside lock)
        for callback in callbacks:
            try:
                callback(instrument, quote)
            except Exception as e:
                logger.error(f"Callback error for {instrument}: {e}", exc_info=True)
    
    async def subscribe(self, instrument: str, callback: Callable[[str, QuoteSnapshot], None]) -> None:
        """
        Subscribe to real-time updates for an instrument.
        
        Args:
            instrument: Symbol in format "HNXDS:VN30F2511"
            callback: Function called with (instrument, quote) on each update
        
        Example:
            def on_quote(instrument, quote):
                print(f"{instrument}: {quote.latest_matched_price}")
            
            await client.subscribe("HNXDS:VN30F2511", on_quote)
        """
        topic = self._instrument_to_topic(instrument)
        
        with self._lock:
            # Add callback
            if instrument not in self._subscriptions:
                self._subscriptions[instrument] = set()
            self._subscriptions[instrument].add(callback)
            
            # Track topic
            self._topics.add(topic)
        
        logger.info(f"Subscribed to {instrument} (topic: {topic})")
    
    async def unsubscribe(self, instrument: str, callback: Optional[Callable] = None) -> None:
        """
        Unsubscribe from instrument updates.
        
        Args:
            instrument: Symbol to unsubscribe from
            callback: Specific callback to remove. If None, removes all callbacks.
        """
        with self._lock:
            if instrument in self._subscriptions:
                if callback:
                    self._subscriptions[instrument].discard(callback)
                else:
                    self._subscriptions[instrument].clear()
                
                # Remove empty subscription set
                if not self._subscriptions[instrument]:
                    del self._subscriptions[instrument]
                    topic = self._instrument_to_topic(instrument)
                    self._topics.discard(topic)
        
        logger.info(f"Unsubscribed from {instrument}")
    
    def query(self, instrument: str) -> Optional[QuoteSnapshot]:
        """
        Get cached quote snapshot (synchronous).
        
        Returns the last received quote from cache.
        
        Args:
            instrument: Symbol in format "HNXDS:VN30F2511"
        
        Returns:
            QuoteSnapshot if cached, None otherwise
        """
        with self._lock:
            return self._quote_cache.get(instrument)
    
    async def start(self) -> None:
        """
        Start the Kafka consumer.
        
        Must be called after subscribing to instruments.
        """
        if self._running:
            logger.warning("Consumer already running")
            return
        
        with self._lock:
            if not self._topics:
                raise ValueError("No subscriptions. Call subscribe() first.")
            
            topics = list(self._topics)
        
        logger.info(f"Starting Kafka consumer for topics: {topics}")
        
        # Create consumer
        self._consumer = self._create_consumer()
        self._consumer.subscribe(topics)
        
        # Start consumer thread
        self._running = True
        self._stop_event.clear()
        self._consumer_thread = threading.Thread(
            target=self._consumer_loop,
            daemon=True,
            name="kafka-market-data"
        )
        self._consumer_thread.start()
        
        logger.info("Kafka consumer started")
    
    async def stop(self) -> None:
        """Stop the Kafka consumer and clean up."""
        if not self._running:
            return
        
        logger.info("Stopping Kafka consumer...")
        
        self._running = False
        self._stop_event.set()
        
        # Wait for consumer thread
        if self._consumer_thread and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=5)
        
        # Close consumer
        if self._consumer:
            try:
                self._consumer.close()
            except Exception as e:
                logger.error(f"Error closing consumer: {e}")
            self._consumer = None
        
        logger.info("Kafka consumer stopped")
    
    async def close(self) -> None:
        """Alias for stop() for consistency with RedisMarketDataClient."""
        await self.stop()
    
    @property
    def is_running(self) -> bool:
        """Check if consumer is running."""
        return self._running
    
    def __repr__(self) -> str:
        return (
            f"KafkaMarketDataClient("
            f"servers={self.bootstrap_servers}, "
            f"running={self._running}, "
            f"subscriptions={len(self._subscriptions)})"
        )
