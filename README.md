# Muninn — телефонная книга для P2P

Централизованный **directory-сервер**: узлы регистрируют свои адреса, другие узлы запрашивают контакты по `id`. Записи истекают по TTL, если узел не шлёт heartbeat.

## Запуск

```bash
go run ./cmd/server
```

Переменные окружения:

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `MUNINN_ADDR` | `:8080` | Адрес HTTP-сервера |
| `MUNINN_PURGE_INTERVAL` | `30s` | Интервал очистки просроченных записей |

## API

| Метод | Путь | Описание |
|-------|------|----------|
| `GET` | `/health` | Проверка живости |
| `POST` | `/api/v1/peers` | Регистрация / обновление узла |
| `GET` | `/api/v1/peers` | Список активных узлов |
| `GET` | `/api/v1/peers/{id}` | Поиск узла по id |
| `GET` | `/api/v1/peers/by-username/{username}` | Поиск узла по username |
| `GET` | `/api/v1/peers/best?n=10` | N лучших пиров по quality_score |
| `DELETE` | `/api/v1/peers/{id}` | Удаление записи |
| `POST` | `/api/v1/peers/{id}/heartbeat` | Продление TTL |
| `PUT` | `/api/v1/files/{file_id}/chunks/{index}` | Эталонный хэш чанка (манифест) |
| `POST` | `/api/v1/peers/{id}/chunk-reports` | Отчёт о полученном чанке и начисление качества |

### Регистрация узла

```bash
curl -s -X POST http://localhost:8080/api/v1/peers \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "node-alice",
    "username": "alice",
    "addresses": ["192.168.1.10:9000", "10.0.0.5:9000"],
    "public_key": "legacy-optional",
    "encryption_key": "base64-ed25519-or-x25519-pub",
    "signature_key": "base64-ed25519-pub-32b",
    "ttl_seconds": 300,
    "metadata": {"version": "1.0"}
  }'
```

### Поиск узла

```bash
curl -s http://localhost:8080/api/v1/peers/node-alice
```

### Ключи узла

| Поле | Назначение |
|------|------------|
| `encryption_key` | Публичный ключ шифрования (Base64), для P2P-сессий |
| `signature_key` | Публичный ключ Ed25519 (Base64, 32 байта) для проверки подписей |

При повторной регистрации пустые `encryption_key` / `signature_key` не затирают уже сохранённые значения.

### Качество узлов (проверка чанков и подписей)

1. Отправитель регистрирует эталонный хэш с подписью (ключ `signature_key` в телефонной книге).
2. Получатель отправляет отчёт с подписью своим ключом.
3. Сервер проверяет подписи, затем сравнивает хэши: **+1** / **−1** к `quality_score`.

Формат сообщений для подписи (Ed25519):

```
muninn/expected/v1
{file_id}
{chunk_index}
{hash_normalized}

muninn/reported/v1
{file_id}
{chunk_index}
{hash_normalized}
{source_peer_id}
```

`hash_normalized` — нижний регистр, без пробелов по краям.

```bash
# Эталон (sender_id — узел, подписавший манифест)
curl -s -X PUT http://localhost:8080/api/v1/files/my-file/chunks/0 \
  -H 'Content-Type: application/json' \
  -d '{
    "sender_id": "file-owner",
    "hash": "a1b2c3d4e5f6",
    "signature": "base64-ed25519-sig"
  }'

# Отчёт получателя (reporter_id — кто получил чанк)
curl -s -X POST http://localhost:8080/api/v1/peers/seeder-1/chunk-reports \
  -H 'Content-Type: application/json' \
  -d '{
    "reporter_id": "downloader-1",
    "file_id": "my-file",
    "chunk_index": 0,
    "hash": "a1b2c3d4e5f6",
    "signature": "base64-ed25519-sig"
  }'
```

Ответ: `valid`, `expected_hash`, `reported_hash`, `delta`, обновлённый `peer`. Неверная подпись → HTTP 401.

### Heartbeat

```bash
curl -s -X POST http://localhost:8080/api/v1/peers/node-alice/heartbeat \
  -H 'Content-Type: application/json' \
  -d '{"ttl_seconds": 600}'
```

## Структура проекта

```
cmd/server/          — точка входа
internal/api/        — HTTP-сервер и маршруты
internal/handler/    — обработчики REST
internal/model/      — модели данных
internal/store/      — хранилище (in-memory)
internal/sign/       — Ed25519, канонические payload для подписей
```

## Тесты

```bash
go test ./...
```
