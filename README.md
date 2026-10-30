# pdf-generator (worker)

**Роль**: Тяжелый воркер, рендерит PDF и уведомляет шлюз. Без HTTP-ручек, работает в фоне.

## Поток работы
1. Читает сообщения из Kafka (топик `pdf-generation-requests`).
2. Проверяет идемпотентность (если файл уже есть в S3 — пропускает).
3. Рендерит PDF (Jinja2 + WeasyPrint).
4. Сохраняет PDF в S3 (Silo).
5. Формирует ссылку вида `http://<pdf-service>/api/pdf/download/{request_id}.`
6. Отправляет вебхук (POST) на `http://<pdf-service>/api/pdf/internal/pdf-ready` с телом `{"request_id": "...", "status": "ready", "url": "..."}.`
7. Если рендер упал — отправляет тот же вебхук, но с `{"status": "failed", "reason": "..."}`.
8. В любом случае коммитит оффсет в Kafka (чтобы не зацикливаться на битых сообщениях).
