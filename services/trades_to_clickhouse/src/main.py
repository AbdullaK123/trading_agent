from datetime import timedelta, datetime
from typing import Any, List, Optional, Tuple
from src.config import config
from src.trade_processor.models import Candle, Indicator
from src.trade_processor.processor import TradeProcessor
from quixstreams import Application
from loguru import logger


def init_ohlcv_candle(dict):
    return {
        'open': dict['price'],
        'high': dict['price'],
        'low': dict['price'],
        'close': dict['price'],
        'volume': dict['quantity'],
        'product_id': dict['product_id'],
        'num_trades': 1
    }

def update_ohlcv_candle(candle: dict, trade: dict):
    
    candle['high'] = max(candle['high'], trade['price'])
    candle['low'] = min(candle['low'], trade['price'])
    candle['close'] = trade['price']
    candle['volume'] += trade['quantity']
    candle['product_id'] = trade['product_id']
    candle['num_trades'] += 1

    return candle

def custom_ts_extractor(
    value: Any,
    headers: Optional[List[Tuple[str, bytes]]],
    timestamp: float,
    timestamp_type
) -> int:
    return value["timestamp_ms"]

def run_clickhouse_consumer(
    kafka_broker_address: str,
    kafka_topic: str,
    kafka_consumer_group: str,
    ohlvc_window_seconds: int,
    clickhouse_host: str,
    clickhouse_port: int
):
    
    app = Application(
        broker_address=kafka_broker_address,
        consumer_group=kafka_consumer_group
    )

    # Initialize ClickHouse processor
    processor = TradeProcessor(host=clickhouse_host, port=clickhouse_port)

    topic = app.topic(
        name=kafka_topic,
        value_serializer='json',
        timestamp_extractor=custom_ts_extractor
    )

    sdf = app.dataframe(topic)

    # Apply windowing and reduction
    sdf = (
        sdf.tumbling_window(duration_ms=timedelta(seconds=ohlvc_window_seconds))
        .reduce(
            initializer=init_ohlcv_candle,
            reducer=update_ohlcv_candle
        )
        .final()
    )

    # Process each window result and insert to ClickHouse
    def process_window(window_result: dict):
        """Extract candle data from window result and insert to ClickHouse"""
        try:
            # Window result structure: {'value': {...}, 'start': timestamp_ms, 'end': timestamp_ms}
            candle_data = window_result['value']
            window_start_ms = window_result['start']
            
            # Convert window start timestamp from milliseconds to datetime
            candle_time = datetime.fromtimestamp(window_start_ms / 1000.0)
            
            # Create Candle object
            candle = Candle(
                time=candle_time,
                symbol=candle_data['product_id'],
                time_frame=ohlvc_window_seconds,
                open=candle_data['open'],
                high=candle_data['high'],
                low=candle_data['low'],
                close=candle_data['close'],
                volume=candle_data['volume'],
                num_trades=candle_data.get('num_trades', 1)  # Default to 1 for old data
            )
            
            # Insert to ClickHouse
            _ = processor.insert_candle(candle)
            logger.info(f"Inserted candle: {candle.symbol} @ {candle_time} | OHLCV: {candle.open:.2f}/{candle.high:.2f}/{candle.low:.2f}/{candle.close:.2f}/{candle.volume:.4f} | Trades: {candle.num_trades}")

            # Insert indicators
            atr = processor.calculate_atr(candle_data['product_id'], ohlvc_window_seconds)

            if atr:
                indicator = Indicator(
                    time=candle_time,
                    symbol=candle_data['product_id'],
                    time_frame=ohlvc_window_seconds,
                    atr_14=atr
                )
                _ = processor.insert_indicator(indicator)
                logger.info(f"Inserted indicator: {indicator.symbol} @ {candle_time} | ATR: {atr}")
                
            return candle_data
        except Exception as e:
            logger.error(f"Error processing window: {e}")
            raise

    sdf = sdf.apply(process_window)
    
    app.run()

if __name__ == "__main__":

    run_clickhouse_consumer(
        config.kafka_broker_address,
        config.kafka_topic,
        config.kafka_consumer_group,
        config.ohlcv_window_seconds,
        config.clickhouse_host,
        config.clickhouse_port
    )
    
