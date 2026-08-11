# Muninn — directory, signaling и chunk metadata service

Muninn — централизованный сервис координации Huginn-пиров. Он:

- регистрирует WebRTC endpoints с TTL;
- ищет endpoints по ID и пользовательскому ключу `login:signature_key`;
- ранжирует storage-пиры по quality score и `peer_flag`;
- пересылает WebRTC offer/answer по WebSocket либо временно хранит сигналы;
- хранит подписанные метаданные зашифрованных чанков;
- проверяет отчёты, подтверждения доставки и read receipts.

Muninn не принимает открытый текст сообщений и байты файлов. Payload хранится у
Huginn-пиров и передаётся по P2P WebRTC DataChannel.

## Запуск

```bash
go run .
```

По умолчанию сервер слушает `:8080` и использует SQLite-файл `muninn.db`.

Проверка:

```bash
curl http://localhost:8080/health
```

Ответ:

```json
{"status":"ok"}
```

## Конфигурация

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `MUNINN_ADDR` | `:8080` | Адрес HTTP/WebSocket-сервера |
| `MUNINN_PURGE_INTERVAL` | `1h` | Период удаления истёкших chunk metadata |
| `MUNINN_STORE_DRIVER` | `sqlite` | `sqlite`, `postgres` или `internal` для memory store |
| `MUNINN_STORE_DSN` | зависит от driver | SQLite DSN либо PostgreSQL connection string |

DSN по умолчанию:

- SQLite: `file:muninn.db?cache=shared&_journal_mode=WAL`;
- PostgreSQL: `postgres://localhost:5432/muninn?sslmode=disable`.

`internal` предназначен для тестов и разработки: данные исчезают после
остановки процесса.

## Docker Compose

```bash
docker compose up --build
```

Текущий compose-файл запускает Muninn, PostgreSQL и coturn. PostgreSQL DSN и
TURN credentials в production необходимо заменить; значения из репозитория
предназначены только для локального окружения.

## HTTP API

Актуальный перечень маршрутов определяется в `internal/api/server.go`.

### Directory

| Метод | Путь | Назначение |
|---|---|---|
| `GET` | `/health` | Liveness probe |
| `POST` | `/api/v1/peers` | Создать либо обновить endpoint |
| `PUT` | `/api/v1/peers` | Обновить `last_seen` существующего endpoint по user key |
| `GET` | `/api/v1/peers` | Список активных non-fake endpoints |
| `GET` | `/api/v1/peers/{id}` | Найти endpoint по ID |
| `GET` | `/api/v1/keys/{login}?signature=...` | Найти все endpoints пользовательского ключа |
| `DELETE` | `/api/v1/peers/{id}` | Удалить endpoint |
| `POST` | `/api/v1/peers/{id}/heartbeat` | Обновить `last_seen` и optional TTL |
| `GET` | `/api/v1/peers/best?n=10` | Лучшие обычные storage-пиры |
| `GET` | `/api/v1/peers/best/thick?n=10` | Лучшие `thick`/`very_thick` пиры |

### Chunk metadata и качество

| Метод | Путь | Назначение |
|---|---|---|
| `PUT` | `/api/v1/files/{file_id}/chunks/{index}` | Зарегистрировать один chunk record |
| `POST` | `/api/v1/files/{file_id}/chunks` | Batch-регистрация chunk records |
| `GET` | `/api/v1/files/{file_id}/chunks` | Все holders и индексы файла/сообщения |
| `GET` | `/api/v1/recipient/chunks?recipient_id=...&date_from=...` | Records получателя, обновлённые после Unix timestamp |
| `POST` | `/api/v1/peers/{id}/chunk-reports` | Отчёт storage-пира о чанке от source endpoint |
| `POST` | `/api/v1/chunks/confirm` | Подтвердить один полученный чанк |
| `POST` | `/api/v1/chunks/confirm-batch` | Batch-подтверждение |
| `POST` | `/api/v1/chunks/read` | Отметить records сообщения прочитанными |

### Signaling

| Метод | Путь | Назначение |
|---|---|---|
| WebSocket | `/api/v1/ws?peer_id={id}` | Основной push-signaling |
| `POST` | `/api/v1/peers/{id}/signals` | Положить offer/answer в fallback queue |
| `GET` | `/api/v1/peers/{id}/signals` | Забрать и удалить fallback queue |

## Регистрация endpoint

```bash
curl -X POST http://localhost:8080/api/v1/peers \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "device-alice-1",
    "login": "alice",
    "encryption_key": "base64-x25519-public-key",
    "signature_key": "base64-ed25519-public-key",
    "ttl_seconds": 120,
    "peer_flag": "thin"
  }'
```

Поля запроса:

| Поле | Обязательно | Назначение |
|---|---|---|
| `id` | да | Уникальный endpoint ID |
| `login` | нет на уровне handler | Отображаемый login |
| `encryption_key` | для обмена сообщениями | X25519 public key в Base64 |
| `signature_key` | для подписанных операций | Ed25519 public key в Base64 |
| `ttl_seconds` | нет | TTL; при `<=0` используется 300 секунд |
| `peer_flag` | нет | `thin`, `thick` или `very_thick` |
| `fake` | нет | Исключить виртуальную запись из обычного списка |

При повторной регистрации существующего ID ключи не заменяются пустыми
значениями. Пользовательский ключ вычисляется как `login:signature_key` и может
соответствовать нескольким endpoint после relogin.

### Refresh

```bash
curl -X PUT http://localhost:8080/api/v1/peers \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "device-alice-1",
    "login": "alice",
    "signature_key": "base64-ed25519-public-key"
  }'
```

### Heartbeat

```bash
curl -X POST http://localhost:8080/api/v1/peers/device-alice-1/heartbeat \
  -H 'Content-Type: application/json' \
  -d '{"ttl_seconds":120}'
```

Если `ttl_seconds` положителен, он заменяет сохранённый TTL. Иначе обновляется
только `last_seen`.

## WebSocket signaling

Клиент подключается по:

```text
ws://localhost:8080/api/v1/ws?peer_id=device-alice-1
```

Request:

```json
{
  "id": "request-uuid",
  "method": "connect_to_peer",
  "params": {
    "target_id": "device-bob-1",
    "offer": "serialized SDP offer"
  }
}
```

Поддерживаемые методы:

- `connect_to_peer {target_id, offer}`;
- `signal_relay {target_id, from, type, data}`.

Target получает notification:

```json
{
  "method": "incoming_signal",
  "params": {
    "from": "device-alice-1",
    "type": "offer",
    "data": "serialized SDP"
  }
}
```

Если target WebSocket не подключён, Muninn сохраняет сигнал в Store. Он будет
выдан при следующем WebSocket connect либо через HTTP `GET .../signals`.

## Регистрация chunk metadata

Muninn хранит metadata, но не поле `data` с содержимым чанка.

Batch-запрос:

```bash
curl -X POST http://localhost:8080/api/v1/files/message-uuid/chunks \
  -H 'Content-Type: application/json' \
  -d '{
    "chunks": [
      {
        "chunk_index": 0,
        "sender_id": "alice:base64-sign-public",
        "recipient_id": "bob:base64-sign-public",
        "hash": "0123456789abcdef",
        "signature": "base64-ed25519-signature",
        "peer_id": "storage-device-1",
        "persist": false,
        "ttl": 604800
      }
    ]
  }'
```

`sender_id` и `recipient_id` здесь являются user keys, а `peer_id` — endpoint,
который физически хранит данные. `ttl` передаётся для каждого batch entry; при
отсутствии используется 86400 секунд.

## Подписанные payload

Перед проверкой hash переводится в нижний регистр и очищается от пробелов по
краям.

Манифест отправителя:

```text
muninn/expected/v1
{file_id}
{chunk_index}
{hash}
```

Отчёт storage-пира; `source_peer_id` — endpoint, качество которого меняется:

```text
muninn/reported/v1
{file_id}
{chunk_index}
{hash}
{source_peer_id}
```

Подтверждение получателя:

```text
muninn/confirmed/v1
{file_id}
{chunk_index}
{hash}
```

Прочтение сообщения:

```text
muninn/read/v1
{file_id}
```

Подписи — Ed25519 в Base64. Неверная подпись возвращает HTTP 401. Валидный
совпавший hash увеличивает quality score на 1, несовпавший уменьшает на 1.

## Ранжирование storage-пиров

Новый пир начинает с `quality_score=1000`. Effective score зависит от флага:

| `peer_flag` | Effective score |
|---|---|
| `thin` или пустой | `score` |
| `thick` | `score²` с сохранением знака |
| `very_thick` | `score³` |

`/peers/best` возвращает активные non-fake записи по убыванию effective score.
`/peers/best/thick` ограничивает результат storage-классами.

## Хранилища

Обе постоянные реализации запускают встроенные миграции:

- SQLite через `modernc.org/sqlite`;
- PostgreSQL через `pgx`.

Memory store потокобезопасен и реализует тот же интерфейс, но не сохраняет
данные между запусками. Chunk records с `persist=false` удаляются после TTL;
очистка запускается с периодом `MUNINN_PURGE_INTERVAL`.

## Структура проекта

```text
main.go                         конфигурация, store и graceful shutdown
internal/api/server.go          единственный источник списка маршрутов
internal/handler/phonebook.go   HTTP handlers
internal/model/                 API request/response structures
internal/sign/                  canonical Ed25519 payloads
internal/store/                 memory, SQLite и PostgreSQL implementations
internal/store/migrations/      embedded migrations для обоих DB drivers
internal/webrtc/handler.go      signaling WebSocket handler
docker-compose.yml              Muninn + PostgreSQL + coturn
```

## Проверка

```bash
GOCACHE=/tmp/huginmunin-muninn-go-cache go test ./...
```

После изменения API нужно синхронно обновить модели и client methods в
`huginn_messenger/src/huginn-messenger/internal/muninn` и запустить тесты в обоих
Go-репозиториях.
