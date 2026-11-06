import json
import logging
from datetime import datetime

from confluent_kafka import Consumer, KafkaError
from pydantic import ValidationError

from app.config import settings
from app.contracts import PdfRequest
from app.database import SessionLocal
from app.service import process_message

log = logging.getLogger(__name__)


def run():
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([settings.kafka_input_topic])
    log.info("Worker started, listening to Kafka.")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Kafka error: %s", msg.error())
                continue

            raw = msg.value()
            try:
                data = json.loads(raw.decode("utf-8"))
                request = PdfRequest(**data)
            except (json.JSONDecodeError, ValidationError) as e:
                log.error("Invalid message -> skip: %s", e)
                consumer.commit(message=msg)
                continue
            kafka_time = datetime.now()
            log.info(
                f"TIMING: [{request.request_id}] Получено из Kafka в {kafka_time.strftime('%H:%M:%S.%f')}"
            )
            db = SessionLocal()
            try:
                process_message(db, request)
            finally:
                db.close()
                consumer.commit(message=msg)
    finally:
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    run()
