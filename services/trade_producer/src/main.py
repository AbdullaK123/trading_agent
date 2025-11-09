from typing import List
from loguru import logger
from quixstreams import Application
from quixstreams.models import TopicConfig
from src.data_source.trade import Trade
from src.data_source.kraken_websocket_client import KrakenWebSocketClient

def produce_trades(
    kafka_broker_address: str,
    kafka_topic: str,
    trade_data_source: KrakenWebSocketClient,
    num_partitions: int
):
    
    # init the quixstreams app with the broker address
    app = Application(broker_address=kafka_broker_address)

    # define a topic with json serialization
    topic = app.topic(
        name=kafka_topic,
        value_serializer='json',
        config=TopicConfig(
            num_partitions=num_partitions,
            replication_factor=1
        )
    )

    # create a produce instance and within it (while not done) loop through the incoming trades serialize them as a topic, then produce them
    with app.get_producer() as producer:

        while not trade_data_source.is_done():

            trades: List[Trade] = trade_data_source.get_trades()

            for trade in trades:

                message = topic.serialize(
                    key=trade.product_id.replace("/", "-"),
                    value=trade.model_dump()
                )

                producer.produce(
                    topic=topic.name,
                    value=message.value,
                    key=message.key
                )

                logger.debug(f"Pushed trade to Kafka: {trade}")

if __name__ == "__main__":
    
    from src.config import config

    kraken_api = KrakenWebSocketClient(product_ids=config.product_ids)

    produce_trades(
        config.kafka_broker_address,
        config.kafka_topic,
        kraken_api,
        len(config.product_ids)
    )