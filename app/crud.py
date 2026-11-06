from sqlalchemy.orm import Session
from datetime import datetime
from app.models import UserPDFHistory
from app.contracts import PdfRequest


def save_pdf_history(db: Session, request: PdfRequest, pdf_bytes: bytes) -> None:
    """Сохраняет сгенерированный PDF и метаданные в историю."""
    payload = request.payload
    record = UserPDFHistory(
        user_id=request.user_id,
        request_id=request.request_id,
        from_place=payload.route.from_place,
        to_place=payload.route.to_place,
        date_from=payload.dates.date_from,
        date_to=payload.dates.date_to,
        activity=", ".join(payload.activity),
        file_data=pdf_bytes,
        created_at=datetime.now()
    )
    db.add(record)
    db.commit()
