# pdf-generator (worker)

**Роль**: Тяжелый воркер, рендерит PDF и уведомляет шлюз. Без HTTP-ручек, работает в фоне.

## Поток работы
1. Читает сообщения из Kafka (топик `pdf-generation-requests`).
2. Рендерит PDF (Jinja2 + WeasyPrint).
3. Сохраняет байты в таблицу `user_pdf_history` (вместе с user_id, городами, датами и активностью)..
5. Отправляет вебхук (POST) на `http://<pdf-service>/api/pdf/internal/pdf-ready` с телом `{"request_id": "...", "status": "ready"}.`
6. Если рендер упал — отправляет тот же вебхук, но с `{"status": "failed", "reason": "..."}`.
7. В любом случае коммитит оффсет в Kafka
