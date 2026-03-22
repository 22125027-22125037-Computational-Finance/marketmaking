"""
Quote data models - Normalized market data structure.

All quotes use a consistent format regardless of data source.
Prices are in actual values (VND for stocks, points for derivatives).
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import datetime


@dataclass
class QuoteSnapshot:
    """
    Market quote snapshot with essential trading information.
    
    Attributes:
        instrument: Symbol in format "HSX:VNM" or "HNXDS:VN30F2511"
        
        # Latest trade
        latest_matched_price: Last traded price
        latest_matched_quantity: Last traded volume
        
        # Best bid/ask (Level 1)
        bid_price_1: Best bid price
        bid_quantity_1: Best bid volume
        ask_price_1: Best ask price
        ask_quantity_1: Best ask volume
        
        # Level 2 depth
        bid_price_2: Second best bid price
        bid_quantity_2: Second best bid volume
        ask_price_2: Second best ask price
        ask_quantity_2: Second best ask volume
        
        # Level 3 depth
        bid_price_3: Third best bid price
        bid_quantity_3: Third best bid volume
        ask_price_3: Third best ask price
        ask_quantity_3: Third best ask volume
        
        # Reference prices
        ref_price: Reference price (previous close)
        ceiling_price: Upper price limit
        floor_price: Lower price limit
        
        # Session statistics
        open_price: Opening price
        highest_price: Session high
        lowest_price: Session low
        total_matched_quantity: Total traded volume
        
        # Metadata
        timestamp: Unix timestamp (seconds, UTC+0)
        datetime_str: ISO datetime string (UTC+0)
        raw_data: Original raw data from source
    """
    
    instrument: str
    
    # Latest trade
    latest_matched_price: Optional[float] = None
    latest_matched_quantity: Optional[float] = None
    
    # Top of book
    bid_price_1: Optional[float] = None
    bid_quantity_1: Optional[float] = None
    ask_price_1: Optional[float] = None
    ask_quantity_1: Optional[float] = None
    
    # Level 2
    bid_price_2: Optional[float] = None
    bid_quantity_2: Optional[float] = None
    ask_price_2: Optional[float] = None
    ask_quantity_2: Optional[float] = None
    
    # Level 3
    bid_price_3: Optional[float] = None
    bid_quantity_3: Optional[float] = None
    ask_price_3: Optional[float] = None
    ask_quantity_3: Optional[float] = None
    
    # Reference prices
    ref_price: Optional[float] = None
    ceiling_price: Optional[float] = None
    floor_price: Optional[float] = None
    
    # Session stats
    open_price: Optional[float] = None
    highest_price: Optional[float] = None
    lowest_price: Optional[float] = None
    total_matched_quantity: Optional[float] = None
    
    # Metadata
    timestamp: Optional[float] = None
    datetime_str: Optional[str] = None
    raw_data: Optional[Dict[str, Any]] = None
    
    @property
    def spread(self) -> Optional[float]:
        """Calculate bid-ask spread."""
        if self.bid_price_1 is not None and self.ask_price_1 is not None:
            return self.ask_price_1 - self.bid_price_1
        return None
    
    @property
    def mid_price(self) -> Optional[float]:
        """Calculate mid price (bid + ask) / 2."""
        if self.bid_price_1 is not None and self.ask_price_1 is not None:
            return (self.bid_price_1 + self.ask_price_1) / 2
        return None
    
    @property
    def spread_bps(self) -> Optional[float]:
        """Calculate spread in basis points (bps)."""
        spread = self.spread
        mid = self.mid_price
        if spread is not None and mid is not None and mid > 0:
            return (spread / mid) * 10000
        return None
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        price = self.latest_matched_price or self.mid_price or 0
        return (
            f"QuoteSnapshot({self.instrument}, "
            f"price={price:,.2f}, "
            f"bid={self.bid_price_1}, "
            f"ask={self.ask_price_1})"
        )
