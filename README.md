# Payment Service

Микросервис асинхронной обработки платежей: принимает платёж по HTTP, обрабатывает
его в фоне через эмулятор платёжного шлюза и сообщает результат на webhook клиента.

Стек: FastAPI, Pydantic v2, SQLAlchemy 2 (async), PostgreSQL, RabbitMQ (FastStream),
Alembic, Docker Compose.

## Запуск

```bash
docker compose up --build
```

Поднимутся четыре контейнера:

| сервис   | что делает                                                        | порт         |
|----------|-------------------------------------------------------------------|--------------|
| postgres | база                                                              | 5432         |
| rabbitmq | брокер, web-интерфейс http://localhost:15672 (guest / guest)      | 5672, 15672  |
| api      | HTTP API + outbox relay, при старте применяет миграции            | 8000         |
| consumer | обработчик очереди `payments.new`                                 | —            |

Swagger: http://localhost:8000/docs

API-ключ по умолчанию `dev-secret-key`, переопределяется через `API_KEY`:

```bash
API_KEY=my-key docker compose up --build
```

## Примеры

Создать платёж:

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: dev-secret-key" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Order #42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://webhook.site/<your-id>"
  }'
```

```
HTTP/1.1 202 Accepted

{"payment_id":"6c1f...","status":"pending","created_at":"2026-10-12T15:13:10.123456Z"}
```

Повторный запрос с тем же `Idempotency-Key` и тем же телом вернёт тот же платёж,
новый не создастся. Тот же ключ с другим телом — `409 Conflict`.

Получить платёж:

```bash
curl http://localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: dev-secret-key"
```

```json
{
  "id": "6c1f...",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Order #42",
  "metadata": {"order_id": 42},
  "status": "succeeded",
  "idempotency_key": "order-42",
  "webhook_url": "https://webhook.site/<your-id>",
  "created_at": "2026-10-12T15:13:10.123456Z",
  "processed_at": "2026-10-12T15:13:13.512301Z",
  "notified_at": "2026-10-12T15:13:13.640114Z"
}
```

Через 2–5 секунд после создания на `webhook_url` придёт POST:

```json
{
  "payment_id": "6c1f...",
  "status": "succeeded",
  "amount": "1500.00",
  "currency": "RUB",
  "processed_at": "2026-10-12T15:13:13.512301Z"
}
```

Посмотреть, как сообщение уходит в DLQ, можно с недоступным webhook:

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: dev-secret-key" \
  -H "Idempotency-Key: dlq-demo" \
  -H "Content-Type: application/json" \
  -d '{"amount": "10.00", "currency": "USD", "description": "DLQ demo", "webhook_url": "http://localhost:9/"}'

docker compose logs -f consumer
```

В логах будут три попытки (вторая через 2 с, третья ещё через 4 с), после чего
сообщение окажется в очереди `payments.new.dlq`.

## Как устроено

```
POST /payments ──▶ api ──(одна транзакция)──▶ payments + outbox
                                                   │
                         outbox relay (фоновая задача в api)
                                                   ▼
                    exchange payments ──payments.new──▶ queue payments.new ──▶ consumer
                                                                ▲                 │
                                  payments.new.retry.N (TTL) ───┘◀── ошибка ──────┤
                                                                                  │
                    exchange payments.dlx ──▶ payments.new.dlq ◀── 3-я ошибка ────┘
```

**Outbox.** Платёж и событие `payment.created` пишутся в одной транзакции, в брокер
ничего не публикуется из обработчика запроса. Relay раз в секунду забирает
неотправленные записи (`SELECT ... FOR UPDATE SKIP LOCKED`, поэтому несколько реплик
api не мешают друг другу), публикует их с publisher confirms и проставляет
`published_at`. Если брокер недоступен, запись остаётся в таблице и уйдёт позже.

**Идемпотентность.** На `idempotency_key` стоит unique-индекс, вместе с платежом
хранится sha256 тела запроса. Параллельные запросы с одним ключом упираются в
unique, проигравший перечитывает уже созданный платёж.

**Consumer.** Один подписчик делает всё:

1. берёт платёж с `SELECT ... FOR UPDATE`;
2. если статус `pending` — эмулирует шлюз (2–5 с, 90% `succeeded`, 10% `failed`)
   и сохраняет результат;
3. если webhook ещё не доставлен (`notified_at` пустой) — отправляет его.

Шаги идемпотентны: при повторной доставке того же сообщения шлюз второй раз не
вызывается, а уже доставленный webhook не отправляется заново.

**Retry и DLQ.** Если обработка упала (чаще всего — webhook не ответил 2xx), сообщение
публикуется в `payments.new.retry.N` с заголовком `x-attempt`. У каждой retry-очереди
свой TTL (`RETRY_BASE_DELAY * 2^(N-1)`: 2 с, 4 с), по истечении сообщение через
dead-letter возвращается в `payments.new`. Ошибка на третьей попытке приводит к reject,
и сообщение через `payments.dlx` попадает в `payments.new.dlq`.

Отдельная очередь на каждую задержку нужна потому, что RabbitMQ снимает просроченные
сообщения только с головы очереди: при разных TTL в одной очереди короткие задержки
ждали бы длинные.

Отказ шлюза (`failed`) — это нормальный результат платежа, а не ошибка: клиент
получает webhook со статусом `failed`, ретраев нет.

### Гарантии

Доставка везде at-least-once:

- relay может отправить событие повторно, если упадёт между публикацией и commit;
- webhook может прийти повторно, если клиент ответил 2xx, а consumer упал до записи
  `notified_at`.

Получателю webhook стоит дедуплицировать по `payment_id` + `status`.

## Конфигурация

| переменная              | по умолчанию | описание                                            |
|-------------------------|--------------|-----------------------------------------------------|
| `DATABASE_URL`          | —            | `postgresql+asyncpg://...`                          |
| `RABBITMQ_URL`          | —            | `amqp://...`                                        |
| `API_KEY`               | —            | ключ для заголовка `X-API-Key`                      |
| `OUTBOX_POLL_INTERVAL`  | `1.0`        | пауза relay, когда outbox пуст, с                   |
| `OUTBOX_BATCH_SIZE`     | `100`        | сколько записей relay берёт за раз                  |
| `MAX_DELIVERY_ATTEMPTS` | `3`          | попыток обработки до DLQ                            |
| `RETRY_BASE_DELAY`      | `2.0`        | задержка перед первым повтором, с                   |
| `WEBHOOK_TIMEOUT`       | `5.0`        | таймаут запроса на webhook, с                       |
| `GATEWAY_MIN_DELAY`     | `2.0`        | эмулятор шлюза: минимальная задержка, с             |
| `GATEWAY_MAX_DELAY`     | `5.0`        | эмулятор шлюза: максимальная задержка, с            |
| `GATEWAY_SUCCESS_RATE`  | `0.9`        | эмулятор шлюза: доля успешных платежей              |

TTL retry-очередей задаётся при их объявлении. Если поменять `RETRY_BASE_DELAY` или
`MAX_DELIVERY_ATTEMPTS` на уже работающем брокере, RabbitMQ откажет в повторном
объявлении с другими аргументами — старые `payments.new.retry.*` нужно удалить.

## Разработка

Нужны Python 3.12 и [uv](https://docs.astral.sh/uv/).

```bash
uv sync
cp .env.example .env
docker compose up -d postgres rabbitmq
uv run alembic upgrade head
uv run uvicorn app.main:app --reload
uv run faststream run app.worker.main:create_app --factory
```

Тесты используют базу `payments_test` (создаётся при первом старте контейнера postgres).
Шлюз, webhook и брокер в тестах подменены фейками.

```bash
docker compose up -d postgres
uv run pytest
```

Другую базу можно указать через `TEST_DATABASE_URL`.

## Структура

```
app/
  api/            HTTP: роуты, проверка API-ключа
  services/       создание платежа, идемпотентность
  messaging/      топология RabbitMQ, подключение к брокеру
  outbox/         relay из таблицы outbox в брокер
  worker/         consumer: шлюз, webhook, обработка, retry
  models.py       таблицы payments и outbox
  schemas.py      Pydantic-схемы
migrations/       Alembic
tests/
```
