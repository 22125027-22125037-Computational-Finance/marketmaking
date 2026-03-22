"""
Redis Market Data Client - Async/await implementation.

Provides two main operations:
1. query() - Get current quote snapshot (direct Redis GET)
2. subscribe() - Real-time updates via Redis pub/sub

Built on redis-py's async support with proper event handling.
"""

import asyncio
import json
import logging
from typing import Optional, Callable, Dict, Set, Any
from datetime import datetime

try:
    from redis.asyncio import Redis
    from redis.asyncio.client import PubSub
except ImportError:
    raise ImportError(
        "redis[asyncio] is required. Install with: pip install 'redis[asyncio]>=5.0.0'"
    )

from .quote import QuoteSnapshot


logger = logging.getLogger(__name__)


class RedisMarketDataClient:
    """
    Async Redis client for market data.
    
    Features:
    - query(): Direct GET for current snapshot
    - subscribe(): Pub/sub for real-time updates
    - Event-driven callbacks
    - Automatic reconnection
    - Resource cleanup
    
    Example:
        import asyncio
        from paperbroker.market_data import RedisMarketDataClient
        
        async def main():
            client = RedisMarketDataClient(
                host='your.redis.host',
                port=6380,
                password='your-password'
            )
            
            # Query mode
            quote = await client.query('HSX:VNM')
            if quote:
                print(f"Price: {quote.latest_matched_price}")
            
            # Subscribe mode
            def on_quote(quote):
                print(f"{quote.instrument}: {quote.latest_matched_price}")
            
            await client.subscribe('HNXDS:VN30F2511', on_quote)
            
            # Keep running
            try:
                while True:
                    await asyncio.sleep(1)
            finally:
                await client.close()
        
        asyncio.run(main())
    """
    
    def __init__(
        self,
        host: str,
        port: int = 6379,
        password: Optional[str] = None,
        db: int = 0,
        decode_responses: bool = True,
        socket_connect_timeout: int = 5,
        socket_keepalive: bool = True,
        health_check_interval: int = 30,
        merge_updates: bool = False,
    ):
        """
        Initialize Redis market data client.
        
        Args:
            host: Redis server hostname/IP
            port: Redis server port (default: 6379)
            password: Redis password (optional)
            db: Redis database number (default: 0)
            decode_responses: Auto-decode responses to strings (default: True)
            socket_connect_timeout: Connection timeout in seconds (default: 5)
            socket_keepalive: Enable TCP keepalive (default: True)
            health_check_interval: Health check interval in seconds (default: 30)
            merge_updates: If True, merge pub/sub updates with previous values.
                          If False (default), return raw updates with None for
                          unchanged fields.
                          - False: Low-level mode (see exact changes)
                          - True: Application mode (always have full snapshot)
        """
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.merge_updates = merge_updates
        
        # Redis clients
        self._redis: Optional[Redis] = None
        self._pubsub: Optional[PubSub] = None
        
        # Connection config
        self._decode_responses = decode_responses
        self._socket_connect_timeout = socket_connect_timeout
        self._socket_keepalive = socket_keepalive
        self._health_check_interval = health_check_interval
        
        # Subscription management
        self._subscriptions: Dict[str, Set[Callable]] = {}
        self._subscription_task: Optional[asyncio.Task] = None
        self._running = False
        
        # Cache for merge mode (instrument -> last_quote)
        self._quote_cache: Dict[str, QuoteSnapshot] = {}
        
        logger.info(
            f"Initialized RedisMarketDataClient: {host}:{port} db={db}, "
            f"merge_updates={merge_updates}"
        )
    
    async def _get_redis(self) -> Redis:
        """Get or create Redis connection."""
        if self._redis is None:
            self._redis = Redis(
                host=self.host,
                port=self.port,
                password=self.password,
                db=self.db,
                decode_responses=self._decode_responses,
                socket_connect_timeout=self._socket_connect_timeout,
                socket_keepalive=self._socket_keepalive,
                health_check_interval=self._health_check_interval,
            )
            # Test connection
            await self._redis.ping()
            logger.info(f"Connected to Redis: {self.host}:{self.port}")
        
        return self._redis
    
    async def query(self, instrument: str) -> Optional[QuoteSnapshot]:
        """
        Query current quote snapshot (direct GET).
        
        This performs a direct Redis GET command to fetch the latest
        quote data for the instrument.
        
        Args:
            instrument: Symbol in format "HSX:VNM" or "HNXDS:VN30F2511"
        
        Returns:
            QuoteSnapshot if data exists, None otherwise
        
        Example:
            quote = await client.query('HSX:VNM')
            if quote:
                print(f"Latest price: {quote.latest_matched_price}")
        """
        try:
            redis = await self._get_redis()
            
            # Redis key is the instrument code
            raw_data = await redis.get(instrument)
            
            if not raw_data:
                logger.debug(f"No data for {instrument}")
                return None
            
            # Parse JSON
            if isinstance(raw_data, bytes):
                raw_data = raw_data.decode('utf-8')
            
            data = json.loads(raw_data)
            
            # Normalize to QuoteSnapshot
            quote = self._normalize_quote(instrument, data)
            
            logger.debug(
                f"Queried {instrument}: "
                f"price={quote.latest_matched_price}"
            )
            
            return quote
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error for {instrument}: {e}")
            return None
        except Exception as e:
            logger.error(f"Query error for {instrument}: {e}")
            return None
    
    async def subscribe(
        self,
        instrument: str,
        callback: Callable[[QuoteSnapshot], None],
    ) -> None:
        """
        Subscribe to real-time quote updates via pub/sub.
        
        The callback will be invoked whenever a new quote is published
        for this instrument.
        
        Args:
            instrument: Symbol to subscribe to
            callback: Function to call with QuoteSnapshot on each update
        
        Example:
            def on_quote(quote):
                print(f"{quote.instrument}: {quote.latest_matched_price}")
            
            await client.subscribe('HNXDS:VN30F2511', on_quote)
        
        Note:
            - Callbacks are executed synchronously in the event loop
            - If callback is async, it will be awaited
            - Multiple callbacks can be registered for same instrument
        """
        if instrument not in self._subscriptions:
            self._subscriptions[instrument] = set()
        
        self._subscriptions[instrument].add(callback)
        
        logger.info(
            f"Registered callback for {instrument}. "
            f"Total callbacks: {len(self._subscriptions[instrument])}"
        )
        
        # Start subscription task if not already running
        if not self._running:
            await self._start_subscription_loop()
    
    async def unsubscribe(
        self,
        instrument: str,
        callback: Optional[Callable[[QuoteSnapshot], None]] = None,
    ) -> None:
        """
        Unsubscribe from quote updates.
        
        Args:
            instrument: Symbol to unsubscribe from
            callback: Specific callback to remove. If None, removes all callbacks
                     for this instrument.
        """
        if instrument not in self._subscriptions:
            return
        
        if callback is None:
            # Remove all callbacks for this instrument
            del self._subscriptions[instrument]
            logger.info(f"Unsubscribed all callbacks from {instrument}")
        else:
            # Remove specific callback
            self._subscriptions[instrument].discard(callback)
            if not self._subscriptions[instrument]:
                del self._subscriptions[instrument]
            logger.info(f"Unsubscribed callback from {instrument}")
        
        # Stop subscription loop if no more subscriptions
        if not self._subscriptions and self._running:
            await self._stop_subscription_loop()
    
    async def _start_subscription_loop(self) -> None:
        """Start the pub/sub subscription loop."""
        if self._running:
            return
        
        self._running = True
        
        # Get Redis connection
        redis = await self._get_redis()
        
        # Create pub/sub instance
        self._pubsub = redis.pubsub()
        
        # Subscribe to all instruments
        channels = list(self._subscriptions.keys())
        if channels:
            await self._pubsub.subscribe(*channels)
            logger.info(f"Subscribed to {len(channels)} channels: {channels}")
        
        # Start background task
        self._subscription_task = asyncio.create_task(
            self._subscription_loop()
        )
        
        logger.info("Subscription loop started")
    
    async def _stop_subscription_loop(self) -> None:
        """Stop the pub/sub subscription loop."""
        if not self._running:
            return
        
        self._running = False
        
        # Cancel task
        if self._subscription_task:
            self._subscription_task.cancel()
            try:
                await self._subscription_task
            except asyncio.CancelledError:
                pass
            self._subscription_task = None
        
        # Close pub/sub
        if self._pubsub:
            await self._pubsub.unsubscribe()
            await self._pubsub.close()
            self._pubsub = None
        
        logger.info("Subscription loop stopped")
    
    async def _subscription_loop(self) -> None:
        """
        Background loop to process pub/sub messages.
        
        This runs continuously while subscriptions are active.
        """
        try:
            logger.info("Pub/sub loop running...")
            
            async for message in self._pubsub.listen():
                if not self._running:
                    break
                
                # Filter out subscription confirmation messages
                if message['type'] != 'message':
                    continue
                
                # Extract channel (instrument) and data
                instrument = message['channel']
                if isinstance(instrument, bytes):
                    instrument = instrument.decode('utf-8')
                
                raw_data = message['data']
                if isinstance(raw_data, bytes):
                    raw_data = raw_data.decode('utf-8')
                
                # Parse and normalize
                try:
                    data = json.loads(raw_data)
                    quote = self._normalize_quote(instrument, data)
                    
                    # Handle merge mode
                    if self.merge_updates:
                        quote = self._merge_with_cache(quote)
                    
                    # Invoke all callbacks for this instrument
                    if instrument in self._subscriptions:
                        for callback in self._subscriptions[instrument]:
                            try:
                                # Check if callback is async
                                if asyncio.iscoroutinefunction(callback):
                                    await callback(instrument, quote)
                                else:
                                    callback(instrument, quote)
                            except Exception as e:
                                logger.error(
                                    f"Callback error for {instrument}: {e}",
                                    exc_info=True
                                )
                
                except json.JSONDecodeError as e:
                    logger.error(
                        f"JSON decode error for {instrument}: {e}"
                    )
                except Exception as e:
                    logger.error(
                        f"Error processing message for {instrument}: {e}",
                        exc_info=True
                    )
        
        except asyncio.CancelledError:
            logger.info("Subscription loop cancelled")
            raise
        except Exception as e:
            logger.error(f"Subscription loop error: {e}", exc_info=True)
            self._running = False
    
    def _normalize_quote(
        self,
        instrument: str,
        data: Dict[str, Any]
    ) -> QuoteSnapshot:
        """
        Normalize raw Redis data to QuoteSnapshot.
        
        Redis data structure has nested objects with 'value' field:
        {
            "latest_matched_price": {"value": 1870, "last_updated": 1763101893, ...},
            "bid_price_1": {"value": 1869.3, ...},
            ...
        }
        
        Args:
            instrument: Symbol code
            data: Raw JSON data from Redis
        
        Returns:
            QuoteSnapshot with normalized data
        """
        # Helper to safely get value from nested object
        def get_value(key: str) -> Optional[float]:
            """Extract 'value' from nested object."""
            obj = data.get(key)
            if obj is None:
                return None
            
            # Handle nested object structure
            if isinstance(obj, dict):
                val = obj.get('value')
                if val is None or val == '':
                    return None
                try:
                    return float(val)
                except (ValueError, TypeError):
                    return None
            
            # Handle direct value (backward compatibility)
            if obj == '':
                return None
            try:
                return float(obj)
            except (ValueError, TypeError):
                return None
        
        # Get timestamp from latest_matched_price object
        timestamp = None
        datetime_str = None
        
        latest_obj = data.get('latest_matched_price')
        if isinstance(latest_obj, dict):
            timestamp = latest_obj.get('last_updated')
            if timestamp:
                dt = datetime.utcfromtimestamp(timestamp)
                datetime_str = dt.isoformat() + 'Z'
        
        # Build QuoteSnapshot
        return QuoteSnapshot(
            instrument=instrument,
            # Latest trade
            latest_matched_price=get_value('latest_matched_price'),
            latest_matched_quantity=get_value('latest_matched_quantity'),
            # Best bid/ask
            bid_price_1=get_value('bid_price_1'),
            bid_quantity_1=get_value('bid_quantity_1'),
            ask_price_1=get_value('ask_price_1'),
            ask_quantity_1=get_value('ask_quantity_1'),
            # Level 2
            bid_price_2=get_value('bid_price_2'),
            bid_quantity_2=get_value('bid_quantity_2'),
            ask_price_2=get_value('ask_price_2'),
            ask_quantity_2=get_value('ask_quantity_2'),
            # Level 3
            bid_price_3=get_value('bid_price_3'),
            bid_quantity_3=get_value('bid_quantity_3'),
            ask_price_3=get_value('ask_price_3'),
            ask_quantity_3=get_value('ask_quantity_3'),
            # Reference prices
            ref_price=get_value('ref_price'),
            ceiling_price=get_value('ceiling_price'),
            floor_price=get_value('floor_price'),
            # Session stats
            open_price=get_value('open_price'),
            highest_price=get_value('highest_price'),
            lowest_price=get_value('lowest_price'),
            total_matched_quantity=get_value('total_matched_quantity'),
            # Metadata
            timestamp=timestamp,
            datetime_str=datetime_str,
            raw_data=data,
        )
    
    def _merge_with_cache(self, quote: QuoteSnapshot) -> QuoteSnapshot:
        """
        Merge quote update with cached values.
        
        In pub/sub mode, Redis only sends changed fields. This method
        fills in None fields with values from the previous quote.
        
        Args:
            quote: New quote with possibly None fields
        
        Returns:
            Merged quote with previous values filled in
        """
        instrument = quote.instrument
        
        # Get cached quote
        cached = self._quote_cache.get(instrument)
        
        if not cached:
            # No cache, this is the first update - store and return
            self._quote_cache[instrument] = quote
            return quote
        
        # Merge: use new value if not None, otherwise use cached
        merged = QuoteSnapshot(
            instrument=instrument,
            # Latest trade
            latest_matched_price=quote.latest_matched_price or cached.latest_matched_price,
            latest_matched_quantity=quote.latest_matched_quantity or cached.latest_matched_quantity,
            # Best bid/ask
            bid_price_1=quote.bid_price_1 or cached.bid_price_1,
            bid_quantity_1=quote.bid_quantity_1 or cached.bid_quantity_1,
            ask_price_1=quote.ask_price_1 or cached.ask_price_1,
            ask_quantity_1=quote.ask_quantity_1 or cached.ask_quantity_1,
            # Level 2
            bid_price_2=quote.bid_price_2 or cached.bid_price_2,
            bid_quantity_2=quote.bid_quantity_2 or cached.bid_quantity_2,
            ask_price_2=quote.ask_price_2 or cached.ask_price_2,
            ask_quantity_2=quote.ask_quantity_2 or cached.ask_quantity_2,
            # Level 3
            bid_price_3=quote.bid_price_3 or cached.bid_price_3,
            bid_quantity_3=quote.bid_quantity_3 or cached.bid_quantity_3,
            ask_price_3=quote.ask_price_3 or cached.ask_price_3,
            ask_quantity_3=quote.ask_quantity_3 or cached.ask_quantity_3,
            # Reference prices (rarely change)
            ref_price=quote.ref_price or cached.ref_price,
            ceiling_price=quote.ceiling_price or cached.ceiling_price,
            floor_price=quote.floor_price or cached.floor_price,
            # Session stats
            open_price=quote.open_price or cached.open_price,
            highest_price=quote.highest_price or cached.highest_price,
            lowest_price=quote.lowest_price or cached.lowest_price,
            total_matched_quantity=quote.total_matched_quantity or cached.total_matched_quantity,
            # Metadata (always use new)
            timestamp=quote.timestamp or cached.timestamp,
            datetime_str=quote.datetime_str or cached.datetime_str,
            raw_data=quote.raw_data,
        )
        
        # Update cache
        self._quote_cache[instrument] = merged
        
        return merged
    
    async def close(self) -> None:
        """
        Close all connections and cleanup resources.
        
        Should be called when done using the client.
        """
        # Stop subscription loop
        await self._stop_subscription_loop()
        
        # Close Redis connection
        if self._redis:
            await self._redis.close()
            self._redis = None
        
        logger.info("RedisMarketDataClient closed")
    
    def __repr__(self) -> str:
        """String representation."""
        return (
            f"RedisMarketDataClient({self.host}:{self.port}, "
            f"subscriptions={len(self._subscriptions)}, "
            f"running={self._running})"
        )
