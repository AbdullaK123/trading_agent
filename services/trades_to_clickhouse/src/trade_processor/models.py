from pydantic import BaseModel
from datetime import datetime

class Trade(BaseModel):
    product_id: str
    quantity: float
    price: float
    timestamp_ms: int

class Candle(BaseModel):
    time: datetime
    symbol: str
    time_frame: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    num_trades: int

