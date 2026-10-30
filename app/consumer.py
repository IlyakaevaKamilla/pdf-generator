import json
import logging
import time
from datetime import datetime

import httpx
from confluent_kafka import Consumer, KafkaError
from pydantic import ValidationError

from app.config import settings
from app.contracts import PdfRequest
from app.renderer import render_tour_pdf
from app.storage import storage

log = logging.getLogger(__name__)

logging.getLogger("weasyprint").setLevel(logging.WARNING)
logging.getLogger("fontTools").setLevel(logging.WARNING)
logging.getLogger("fontTools.subset").setLevel(logging.WARNING)

http_client = httpx.Client(base_url="http://localhost:8000", timeout=5.0)


def send_webhook(
        request_id: str, status: str, url: str | None = None, reason: str | None = None
):
    try:
        payload = {"request_id": request_id, "status": status, "url": url, "reason": reason}
        http_client.post("/api/pdf/internal/pdf-ready", json=payload)
    except Exception as e:
        log.error("Webhook failed: %s", e)


def run():
    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": settings.kafka_group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
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
            log.info("TIMING. %s got from Kafka at %s", request.request_id, kafka_time.strftime("%H:%M:%S.%f"))
            key = f"{request.request_id}.pdf"
            if storage.exists(key):
                log.info("PDF %s already exists, skip", request.request_id)
                send_webhook(
                    request_id=request.request_id,
                    status="ready",
                    url=f"{settings.s3_endpoint_url}/{settings.s3_bucket}/{key}"
                )
                consumer.commit(message=msg)
                continue
            render_start = time.time()
            try:
                pdf_bytes = render_tour_pdf(request.payload)
                render_time = time.time() - render_start
                save_start = time.time()
                storage.save(key, pdf_bytes)
                save_time = time.time() - save_start
                log.info(
                    "TIMING. %s render %s, save %s",
                    request.request_id, render_time, save_time
                )
                url = f"{settings.pdf_gateway}/api/pdf/download/{request.request_id}"
                log.info("PDF ready: %s", request.request_id)
                send_webhook(request_id=request.request_id, status="ready", url=url)
                consumer.commit(message=msg)
            except Exception as e:
                log.exception("Render failed: %s", e)
                send_webhook(request.request_id, status="failed", reason=str(e))
                consumer.commit(message=msg)
    finally:
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
