from typing import Optional
import clickhouse_connect
from datetime import datetime
import math
from src.trade_processor.models import Candle, Indicator


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
                time        DateTime64(3, 'UTC'),
                symbol      LowCardinality(String),
                time_frame  UInt16,
                atr_14      Float64,
                rsi_14      Float64,
                ema_14      Float64,
                sma_14      Float64,
                macd        Float64
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

    def _get_candle_count(
        self,
        symbol: str,
        time_frame: int
    ) -> int:
        candle_count_result = self.client.query(
            f'''
            SELECT
                COUNT(*)
            FROM crypto_dw.candles
            WHERE symbol = '{symbol}'
                AND time_frame = {time_frame}
            '''
        )
        return candle_count_result.first_row[0]
    
    def _has_enough_candles(self, symbol: str, time_frame: int,  periods:int = 14) -> bool:
        return self._get_candle_count(symbol, time_frame) > periods + 1

    def calculate_atr(
        self,
        symbol: str,
        time_frame: int,
        periods: int = 14
    ) -> Optional[float]:
        
        if not self._has_enough_candles(symbol, time_frame, periods):
            return None
        
        atr_result = self.client.query(
            f'''
            WITH tr_data AS (
                SELECT
                    time,
                    high,
                    low,
                    close,
                    lagInFrame(close) OVER( PARTITION BY symbol, time_frame ORDER BY time) AS prev_close
                FROM 
                    crypto_dw.candles
                WHERE
                    symbol = '{symbol}' AND time_frame = {time_frame}
                ORDER BY 
                    time DESC
                LIMIT {periods + 1}
            )
            SELECT
                AVG(GREATEST(
                    high - low,
                    abs(high - prev_close),
                    abs(low - prev_close)
                )) AS atr
            FROM tr_data
            WHERE prev_close IS NOT NULL
            '''
        )
        atr = atr_result.first_row
        return atr[0] if atr and atr[0] else None
    

    def calculate_rsi(
        self,
        symbol: str,
        time_frame: int,
        periods: int = 14
    ) -> Optional[float]:
        
        if not self._has_enough_candles(symbol, time_frame, periods):
            return None
        
        rsi_result = self.client.query(
            f'''
            WITH rsi_data AS (
                SELECT
                    time,
                    close,
                    lagInFrame(close) OVER (PARTITION BY symbol, time_frame ORDER BY time) AS prev_close
                FROM 
                    crypto_dw.candles
                WHERE
                    symbol = '{symbol}' AND time_frame = {time_frame}
                ORDER BY 
                    time DESC
                LIMIT {periods + 1}
            ),
            changes AS (
                SELECT
                    close - prev_close AS change
                FROM 
                    rsi_data
                WHERE 
                    prev_close IS NOT NULL 
            ),
            gains_and_losses AS (
                SELECT
                    AVG(IF(change > 0, change, 0)) AS avg_gain,
                    AVG(IF(change < 0, -change, 0)) AS avg_loss
                FROM 
                    changes
            )
            SELECT
                IF(
                    avg_loss = 0,
                    100,  
                    100 - (100 / (1 + (avg_gain / avg_loss)))
                ) AS rsi
            FROM
                gains_and_losses
            '''
        )
        rsi = rsi_result.first_row
        return rsi[0] if rsi and rsi[0] is not None else None

    def calculate_sma(
        self,
        symbol: str,
        time_frame: int,
        periods: int = 14
    ) -> Optional[float]:
        
        if periods < 1 or not self._has_enough_candles(symbol, time_frame, periods - 1):
            return None
        
        sma_result = self.client.query(
            f'''
            WITH raw_data AS (
                SELECT
                    time,
                    close
                FROM
                    crypto_dw.candles
                WHERE
                    symbol='{symbol}' AND time_frame={time_frame}
                ORDER BY
                    time DESC
                LIMIT {periods}
            )
            SELECT 
                AVG(close)
            FROM 
                raw_data
            '''
        )
        sma = sma_result.first_row
        return sma[0] if sma and sma[0] else None


    def calculate_ema(
        self,
        symbol: str,
        time_frame: int,
        periods: int = 14
    ) -> Optional[float]:
        
        if periods < 1 or not self._has_enough_candles(symbol, time_frame, periods - 1):
            return None
        
        alpha = 2 / (periods + 1)
        half_life = math.log(0.5) / math.log(1 - alpha)

        ema_result = self.client.query(
            f'''
            WITH raw_data AS (
                SELECT
                    time,
                    close
                FROM
                    crypto_dw.candles
                WHERE
                    symbol='{symbol}' AND time_frame={time_frame}
                ORDER BY
                    time DESC
                LIMIT {periods}
            ), 
            sorted AS (
                SELECT *
                FROM raw_data
                ORDER BY time ASC
            ),
            data_with_t AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (ORDER BY time ASC) AS t
                FROM
                    sorted
            )
            SELECT
                exponentialMovingAverage({half_life})(close, t) OVER (ORDER BY t ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ema
            FROM
                data_with_t
            ORDER BY
                t DESC
            LIMIT 1
            '''
        )
        ema = ema_result.first_row
        return ema[0] if ema and ema[0] else None

    def calculate_macd(
        self,
        symbol: str,
        time_frame: int,
        fast: int = 12,
        slow: int = 26
    ) -> Optional[float]:

        if slow < 1 or not self._has_enough_candles(symbol, time_frame, slow - 1):
            return None

        fast_alpha = 2 / (fast + 1)
        slow_alpha = 2 / (slow + 1)

        fast_half_life = math.log(0.5) / math.log(1 - fast_alpha)
        slow_half_life = math.log(0.5) / math.log(1 - slow_alpha)

        macd_result = self.client.query(
            f'''
            WITH raw_data AS (
                SELECT
                    time,
                    close
                FROM
                    crypto_dw.candles
                WHERE
                    symbol='{symbol}' AND time_frame={time_frame}
                ORDER BY
                    time DESC
                LIMIT {slow}
            ), 
            sorted AS (
                SELECT *
                FROM raw_data
                ORDER BY time ASC
            ),
            data_with_t AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (ORDER BY time ASC) AS t
                FROM
                    sorted
            ),
            slow_and_fast AS (
                SELECT
                    exponentialMovingAverage({slow_half_life})(close, t) OVER (ORDER BY t ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS slow_ema,
                    exponentialMovingAverage({fast_half_life})(close, t) OVER (ORDER BY t ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fast_ema
                FROM
                    data_with_t
            )
            SELECT
                fast_ema - slow_ema
            FROM
                slow_and_fast
            '''
        )

        macd = macd_result.first_row

        return macd[0] if macd and macd[0] else None


    
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
    
    def insert_indicator(self, indicator: Indicator):
        result = self.client.insert(
            "crypto_dw.indicators",
            [
                [
                    indicator.time,
                    indicator.symbol,
                    indicator.time_frame,
                    indicator.atr_14,
                    indicator.rsi_14,
                    indicator.ema_14,
                    indicator.sma_14,
                    indicator.macd
                ]
            ]
        )
        return result.summary
