import clickhouse_connect
from datetime import datetime
from src.trade_processor.models import Candle


class TradeProcessor:

    def __init__(self, host: str, port: int):
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port
        )
        self._create_database()
        self._create_tables()

    def _create_database(self):
        self.client.command(
            '''
            CREATE DATABASE IF NOT EXISTS crypto_dw;
            '''
        )

    def _create_tables(self):
        self.client.command(
            '''
            CREATE TABLE IF NOT EXISTS crypto_dw.candles (
                time        DateTime64(3, 'UTC'),
                symbol      LowCardinality(String),
                time_frame  UInt16,
                open        Float64,
                high        Float64,
                low         Float64,
                close       Float64,
                volume      Float64,
                num_trades  UInt32
            )
            ENGINE = ReplacingMergeTree
            PARTITION BY toYYYYMM(time)
            ORDER BY (symbol, time_frame, time)
            SETTINGS index_granularity = 8192;
            '''
        )
        self.client.command(
            '''
            CREATE TABLE IF NOT EXISTS crypto_dw.indicators (
                time        DateTime(3, 'UTC'),
                symbol      LowCardinality(String),
                time_frame  UInt16,
                atr_14      Float64
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(time)
            ORDER BY (symbol, time_frame, time)
            SETTINGS index_granularity = 8192;
            '''
        )
        self.client.command(
            '''
            CREATE TABLE IF NOT EXISTS crypto_dw.signals (
                signal_id             String,                    
                time                  DateTime64(3, 'UTC'),
                symbol                LowCardinality(String),
                type                  LowCardinality(String),    
                intended_notional     Float64,                   
                intended_price        Float64,                   
                drop_from_anchor_pct  Float64,                   
                atr_at_signal         Float64,                  
                config_version        LowCardinality(String)    
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(time)
            ORDER BY (symbol, time, signal_id)
            SETTINGS index_granularity = 8192;

            '''
        )
        self.client.command(
            '''
            CREATE TABLE IF NOT EXISTS crypto_dw.executions (
                exec_id     String,                              
                order_id    String,                              
                time        DateTime64(3, 'UTC'),
                symbol      LowCardinality(String),
                side        LowCardinality(String),              
                fill_px     Float64,
                fill_qty    Float64,
                fee_usd     Float64
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(time)
            ORDER BY (symbol, time, order_id, exec_id)
            SETTINGS index_granularity = 8192;
            '''
        )
        self.client.command(
            '''
            CREATE TABLE IF NOT EXISTS crypto_dw.positions_daily (
                date                Date,                        
                btc_qty             Float64,
                avg_cost            Float64,                     
                cash_usd            Float64,
                last_dca_anchor_px  Float64
            )
            ENGINE = ReplacingMergeTree
            PARTITION BY toYYYYMM(date)
            ORDER BY (date)
            SETTINGS index_granularity = 8192;
            '''
        )
    
    def insert_candle(self, candle: Candle):
        result = self.client.insert(
            "crypto_dw.candles",
            [
                [
                    candle.time,
                    candle.symbol,
                    candle.time_frame,
                    candle.open,
                    candle.high,
                    candle.low,
                    candle.close,
                    candle.volume,
                    candle.num_trades
                ]
            ]
        )
        return result.summary
