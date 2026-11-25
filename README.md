# Muninn — directory, signaling и chunk metadata service

Muninn — сервис координации Huginn-пиров с опциональной mesh-федерацией. Он:

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
| `MUNINN_MESH_CONFIG` | выключено | Путь к отдельному JSON-конфигу mesh |
| `MUNINN_PURGE_INTERVAL` | `1h` | Период удаления истёкших chunk metadata |
| `MUNINN_STORE_DRIVER` | `sqlite` | `sqlite`, `postgres` или `internal` для memory store |
| `MUNINN_STORE_DSN` | зависит от driver | SQLite DSN либо PostgreSQL connection string |

DSN по умолчанию:

- SQLite: `file:muninn.db?cache=shared&_journal_mode=WAL`;
- PostgreSQL: `postgres://localhost:5432/muninn?sslmode=disable`.

`internal` предназначен для тестов и разработки: данные исчезают после
остановки процесса.

## Mesh: общение между серверами

Каждый Huginn подключается к своему Muninn. Серверы объединяют поиск по ID и
`login:signature_key`, список storage-пиров, WebRTC offer/answer, метаданные
офлайн-сообщений и подтверждения. Текст и файлы по-прежнему передаются между
Huginn-пирами через E2E/WebRTC; общая база данных не требуется.

### Включение

Скопируйте `mesh.example.json` в `mesh.json` и замените адреса:

```json
{
  "enabled": true,
  "public_url": "https://muninn-a.example.org",
  "mode": "open",
  "seeds": ["https://muninn-b.example.org"],
  "state_file": "mesh-state.json",
  "allow_private_networks": false,
  "sync_interval_seconds": 30,
  "request_timeout_seconds": 3,
  "transit_timeout_seconds": 8,
  "max_relay_hops": 3,
  "max_nodes": 64
}
```

```bash
MUNINN_MESH_CONFIG=./mesh.json go run .
```

На сервере B укажите его собственный `public_url`. Достаточно указать seed на
одной стороне: handshake знакомит серверы в обе стороны, затем они обмениваются
списками соседей. `seeds` может быть пустым, если адреса приходят от клиентов.
Без `MUNINN_MESH_CONFIG` или при `enabled: false` работает обычный одиночный Muninn.

`public_url` — HTTP(S) origin без пути, query и credentials, доступный хотя бы
соседним Muninn. Серверы обмениваются адресами и полными маршрутами. Если A не
может подключиться к C напрямую, запрос проходит по известному маршруту A–B–C;
ответ возвращается по той же цепочке. Работают также несколько промежуточных
серверов. При восстановлении прямой связи очередной discovery возвращает ей
приоритет. Reverse proxy должен пропускать `POST /api/v1/mesh/hello`,
`POST /api/v1/mesh/rpc`, `POST /api/v1/mesh/relay` и заголовки `X-Muninn-*`.
Для транзита все серверы выбранного маршрута должны поддерживать relay;
с прежними версиями сохраняется только прямое взаимодействие.

### Транзит и кэш ID

Транзит включён автоматически и охватывает discovery, directory, WebSocket/HTTP
сигналинг, chunk metadata, отчёты и подтверждения. Байты файлов и сообщений
по-прежнему передаются Huginn-пирами по WebRTC с существующим TURN fallback.

- Сначала проверяется прямая связь, затем известные маршруты от коротких к
  длинным. Каждый промежуточный сервер пересылает запрос только следующему
  участнику указанного маршрута, прошедшему его собственный прямой handshake.
  Произвольные URL и HTTP paths не проксируются.
- `max_relay_hops` ограничивает число промежуточных серверов (по умолчанию 3,
  допустимо 1–8). Повторение сервера в маршруте запрещено. Маршруты, полученные
  от соседей, имеют срок действия и обновляются при discovery.
- `request_timeout_seconds` ограничивает прямую пробу и ранние попытки;
  `transit_timeout_seconds` задаёт общий бюджет сетевой операции (по умолчанию
  8 секунд, не больше 8 и не меньше `request_timeout_seconds`). Часть времени
  резервируется для обходного маршрута даже при зависшем прямом соединении.
- Один запрос имеет один случайный 128-битный `id` при всех попытках и
  переходах. **Каждый Muninn**, включая промежуточные и конечный, хранит
  обработанные ID **60 секунд с завершения обработки**. В течение TTL он
  возвращает сохранённый результат без повторной пересылки или выполнения.
  Повтор с тем же ID и изменённым содержимым отклоняется.
- Параллельные дубликаты по одному маршруту ждут первого результата. Если
  разные незавершённые маршруты пересекаются на промежуточном узле, повтор
  получает `in_progress`, чтобы не создавать взаимного ожидания. Конечный
  сервер объединяет параллельные дубликаты независимо от маршрута.
- Кэш хранится в памяти и ограничен 4096 ID и 32 MiB ответов на инстанс.
  При нехватке места для ответа ID сохраняется до конца TTL, а повтор получает
  `processed` без повторной обработки. Если заполнен лимит ID, новый запрос
  получает `busy`; действующие ID не вытесняются. Ошибки обработки также
  кэшируются. После истечения минуты или перезапуска дедупликация этого ID
  больше не гарантируется; клиентский повтор создаёт новый транзитный ID.

`state_file` сохраняет проверенные адреса атомарно; относительный путь считается
от каталога конфига. Каталог должен существовать и быть доступен для записи.
После перезапуска адреса проходят повторную проверку. Секрет в state-файл не
записывается. Без `state_file` найденные адреса живут до перезапуска; seeds
загружаются каждый раз. Для удаления выведенного из эксплуатации узла удалите
его из seeds/state-файлов участников и перезапустите их, исключив повторное
обнаружение через оставшихся соседей.

### Открытый и закрытый режимы

- `mode: "open"`: любой совместимый Muninn может присоединиться через seed или
  пользовательскую подсказку. Данные удалённого directory поступают от других
  операторов; криптографическая идентичность пользователя остаётся его ключом.
- `mode: "closed"`: задайте одинаковый случайный `shared_secret` длиной хотя бы
  32 байта на всех серверах. Пример — `mesh.closed.example.json`; новый секрет
  можно получить командой `openssl rand -hex 32`. Запросы и ответы проверяются
  HMAC-SHA256; nonce и время ограничивают повтор запросов. Часы серверов должны
  быть синхронизированы (допуск ±1 минута). Открытые узлы и узлы с другим секретом
  не присоединяются. Секрет не отправляется кандидатам и не нужен клиентам.

Закрытый режим ограничивает **членство серверов**; публичный клиентский HTTP API
сохраняет прежние правила доступа. Для публичного размещения используйте HTTPS.
Конфиг с секретом храните с ограниченными правами и вне git.

### Адреса от пользователей

Регистрация endpoint принимает необязательное поле `muninn_servers` (до 16
адресов):

```json
{
  "id": "alice-device",
  "login": "alice",
  "signature_key": "base64-ed25519-public-key",
  "encryption_key": "base64-x25519-public-key",
  "muninn_servers": ["https://muninn-b.example.org"]
}
```

Регистрация завершается без ожидания чужого сервера. Подсказки проходят
асинхронный handshake; недопустимые адреса игнорируются. До успешной проверки
узел не участвует в запросах. В Go-клиенте доступны `muninn_servers` в
`config.conf`/сохранённом конфиге, `MUNINN_SERVERS` и `--muninn-servers` (адреса
через запятую). При смене основного Muninn клиент сохраняет старый адрес как
подсказку для нового сервера. Обновлённый клиент выбирает один активный сервер
по задержке успешных `/health`-проверок и направляет все рабочие HTTP/WS-запросы
к нему; альтернативы получают только проверки доступности. Старые клиенты
совместимы с mesh через seeds.

### Поведение при сбоях и ограничения

- Remote directory не копируется в локальную БД: heartbeat и TTL обслуживаются
  сервером регистрации. ID endpoint должны быть уникальны в сети (клиенты
  используют UUID). При смене выбранного Muninn клиент закрывает старый WebSocket
  и регистрирует тот же endpoint на новом сервере; старая запись живёт до TTL.
  Сигналинг предпочитает сервер с активным WebSocket, а при отсутствии сокетов —
  наиболее свежую запись. Подключение WebSocket и HTTP polling собирают ожидающие
  сигналы из mesh, включая прежний сервер. Коллизии ID разных пользователей по-прежнему
  не поддерживаются.
- Внутренний RPC исполняется только на выбранном конечном сервере и читает
  его локальные записи. Промежуточные серверы пересылают envelope по заданному
  маршруту. Сигнал получает один найденный владелец, который доставляет его
  по WebSocket или сохраняет в очередь.
- Запросы ограничены таймаутом, числом параллельных соединений, размером 4 MiB и
  `max_nodes` (по умолчанию 64, максимум 256). Discovery повторяется каждые
  `sync_interval_seconds`, с задержкой повторов для недоступных узлов.
- Поиск и опрос получателя используют доступные ответы. В mesh-режиме
  `date_from` намеренно не отсекает старые записи: возвращается снимок живых
  метаданных, чтобы новый/восстановленный сервер и расхождение часов не приводили
  к потере сообщений. Клиент устраняет повторы. Это увеличивает объём опроса;
  текущая реализация рассчитана на небольшие сети, без пагинации снимков.
- Рассылка подтверждений возвращает 503 при
  недоступности участника; вызывающий клиент может повторить запрос. Успешное
  повторное подтверждение не увеличивает quality score повторно. Оценки качества
  меняются только там, где есть локальная запись соответствующего пира.
- Метаданные не реплицируются на каждый Muninn. Для получения офлайн-сообщения
  должны быть доступны Muninn с его metadata и хотя бы один Huginn с байтами
  чанка. Повторная попытка после восстановления узла не теряет старые metadata.
- По умолчанию запрещены private/loopback адреса, в том числе после DNS resolve;
  redirect и HTTP proxy из окружения не используются. Для LAN/Docker/локальной
  разработки явно задайте `allow_private_networks: true`. Link-local, включая
  cloud metadata endpoints, запрещены в обоих случаях.

Для локальной проверки используйте разные `MUNINN_ADDR`, `public_url`, БД и
state-файлы, например `:8080` / `http://127.0.0.1:8080` и `:8081` /
`http://127.0.0.1:8081`, с `allow_private_networks: true`. Для Docker есть overlay:

```bash
# В mesh.json укажите state_file: /var/lib/muninn-mesh/nodes.json
docker compose -f docker-compose.yml -f docker-compose.mesh.yml up --build
```

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
| `muninn_servers` | нет | До 16 адресов Muninn для mesh discovery |

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
internal/mesh/                  discovery, HMAC transport и федерация Store
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

Для проверки SQL store с PostgreSQL задайте `MUNINN_TEST_POSTGRES_DSN`, указывающий
на тестовую базу. Тест создаёт отдельную схему и удаляет её после завершения;
пользователю базы нужны права на создание схем. В CI PostgreSQL 18 запускается
автоматически. Без этой переменной PostgreSQL-проверка пропускается.

После изменения API нужно синхронно обновить модели и client methods в
`../huginn-messenger/internal/muninn` и
`../huginn-messenger-flutter/src/huginn-messenger/internal/muninn` и запустить тесты в обоих
Go-репозиториях.
