import logging
import time
from datetime import datetime

import httpx
from sqlalchemy.orm import Session

from app.contracts import PdfRequest
from app.renderer import render_tour_pdf
from app.crud import save_pdf_history

log = logging.getLogger(__name__)

logging.getLogger("weasyprint").setLevel(logging.WARNING)
logging.getLogger("fontTools").setLevel(logging.WARNING)
logging.getLogger("fontTools.subset").setLevel(logging.WARNING)

http_client = httpx.Client(base_url="http://localhost:8000", timeout=5.0)


def send_webhook(
        request_id: str, status: str, reason: str | None = None
):
    try:
        payload = {"request_id": request_id, "status": status, "reason": reason}
        http_client.post("/api/pdf/internal/pdf-ready", json=payload)
    except Exception as e:
        log.error("Webhook failed: %s", e)


def process_message(db: Session, request: PdfRequest):
    try:
        render_start = time.time()
        pdf_bytes = render_tour_pdf(request.payload)
        log.info(f"TIMING: [{request.request_id}] Рендер занял {time.time() - render_start:.3f} сек")
        save_pdf_history(db, request, pdf_bytes)
        log.info(f"TIMING: [{request.request_id}] Сохранено в БД")
        log.info("PDF ready: %s", request.request_id)
        send_webhook(request_id=request.request_id, status="ready")
        log.info(f"TIMING: [{request.request_id}] Вебхук отправлен")
    except Exception as e:
        log.exception("Render failed: %s", e)
        send_webhook(request.request_id, status="failed", reason=str(e))
