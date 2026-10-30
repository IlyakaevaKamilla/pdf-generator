from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML

from app.contracts import TourPdfPayload

TEMPLATES_DIR = Path(__file__).parent / "templates"

env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))


def render_tour_pdf(payload: TourPdfPayload) -> bytes:
    """Берёт payload, подставляет в HTML-шаблон, рендерит в PDF-байты."""
    template = env.get_template("tour_template.html")
    html = template.render(payload=payload.model_dump())
    return HTML(string=html).write_pdf()
