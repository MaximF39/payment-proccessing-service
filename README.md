# Асинхронный сервис процессинга платежей

Микросервис принимает платежи по HTTP, гарантированно публикует событие `payments.new` через **outbox**, обрабатывает его одним consumer через эмуляцию платёжного шлюза и уведомляет клиента webhook'ом.

## Стек

- FastAPI + Pydantic v2
- SQLAlchemy 2.0 (async) + PostgreSQL + Alembic
- RabbitMQ + FastStream
- Docker Compose

## Архитектура

1. `POST /api/v1/payments` в одной транзакции создаёт запись в `payments` и событие в `outbox`.
2. Фоновый publisher API читает неопубликованные события (`SELECT ... FOR UPDATE SKIP LOCKED`) и публикует их в обменник `payments` с routing key `payments.new`.
3. Consumer читает очередь `payments.new`, эмулирует шлюз (2–5 сек, 90% успех / 10% ошибка), обновляет статус и шлёт webhook.
4. Ошибки обработки: до 3 попыток с экспоненциальной задержкой (1s, 2s), затем сообщение уходит в DLQ `payments.new.dlq`.
5. Ошибки webhook: 3 попытки с экспоненциальной задержкой; если не удалось — ошибка обработки сообщения (retry / DLQ). Повторная обработка идемпотентна: шлюз не вызывается повторно, если статус уже не `pending`, webhook не шлётся повторно, если `webhook_sent_at` уже заполнен.

## Запуск

```bash
docker compose up --build
```

Сервисы:

| Сервис | Адрес |
| --- | --- |
| API | http://localhost:8000 |
| Swagger | http://localhost:8000/docs |
| RabbitMQ UI | http://localhost:15672 (guest/guest) |
| PostgreSQL | localhost:5432 (payments/payments) |

API-ключ по умолчанию: `dev-api-key` (заголовок `X-API-Key`). Переопределяется через `API_KEY`.

## Примеры

Создание платежа:

```bash
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-api-key" \
  -H "Idempotency-Key: order-123" \
  -d "{
    \"amount\": \"199.90\",
    \"currency\": \"RUB\",
    \"description\": \"Оплата заказа 123\",
    \"metadata\": {\"order_id\": \"123\"},
    \"webhook_url\": \"https://webhook.site/your-id\"
  }"
```

Ответ `202 Accepted`:

```json
{
  "payment_id": "c0a8012e-7b1a-4c3d-9f0a-1b2c3d4e5f60",
  "status": "pending",
  "created_at": "2026-10-06T09:00:00Z"
}
```

Получение платежа:

```bash
curl -s http://localhost:8000/api/v1/payments/<payment_id> \
  -H "X-API-Key: dev-api-key"
```

Повтор с тем же `Idempotency-Key` и тем же телом вернёт тот же платёж. Другое тело с тем же ключом — `409 Conflict`. Без `X-API-Key` — `401`.

## Очереди RabbitMQ

| Имя | Назначение |
| --- | --- |
| `payments` (exchange) | Публикация новых платежей |
| `payments.new` | Основная очередь consumer |
| `payments.retry.1` / `payments.retry.2` | Отложенный retry (TTL 1s / 2s) |
| `payments.dlx` + `payments.new.dlq` | Dead Letter Queue после 3 неуспешных попыток |

## Таблицы

- `payments` — платёж, уникальный `idempotency_key`
- `outbox` — события к публикации; `published_at IS NULL` означает «ещё не отправлено в брокер»

## Локальная разработка без Docker (сервисы)

Поднимите PostgreSQL и RabbitMQ, затем:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
# поправьте DATABASE_URL и RABBITMQ_URL на localhost
alembic upgrade head
uvicorn app.main:app --reload
faststream run app.consumer:app
```
