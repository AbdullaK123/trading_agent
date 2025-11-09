from typing import List
from websocket import create_connection
from loguru import logger
import json
from datetime import datetime, timezone
from src.data_source.trade import Trade


class KrakenWebSocketClient:

    URL = 'wss://ws.kraken.com/v2'

    def __init__(self, product_ids: List[str]):
        self.product_ids = product_ids
        self._ws = create_connection(self.URL)
        logger.debug("Connection established")
        self._subscribe(product_ids)

    def _subscribe(self, product_ids: List[str]):
        logger.info(f"Subscribing to trades for {product_ids}")
        msg = {
            'method': 'subscribe',
            'params': {
                'channel': 'trade',
                'symbol': product_ids,
                'snapshot': False
            }
        }
        self._ws.send(json.dumps(msg))
        logger.info("Subscription worked!")
        for product_id in product_ids:
            _ = self._ws.recv()
            _ = self._ws.recv()

    def get_trades(self) -> List[Trade]:
        message = self._ws.recv()

        if 'heartbeat' in message:
            logger.debug("Heartbeat received")
            return []
        
        message = json.loads(message)

        trades = []

        for trade in message['data']:
            trades.append(
                Trade(
                    product_id=trade['symbol'],
                    price=trade['price'],
                    quantity=trade['qty'],
                    timestamp_ms=self.to_ms(trade['timestamp'])
                )
            )

        return trades

    def is_done(self) -> bool:
        return False

    @staticmethod
    def to_ms(timestamp: str) -> int:
        timestamp = datetime.fromisoformat(timestamp[:-1]).replace(tzinfo=timezone.utc)
        return int(timestamp.timestamp()*1000)