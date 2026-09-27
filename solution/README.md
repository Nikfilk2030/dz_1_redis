# Решение на Go

## Файлы

| Файл | Что там |
| --- | --- |
| `docker-compose.yml` | Redis: мастер и две реплики; три Sentinel; API; worker |
| `redis/redis.conf` | RDB, AOF, память, защита от записи без реплик |
| `redis/sentinel.conf`, `redis/start.sh` | Настройки Sentinel и запуск узлов Redis |
| `app/cmd/api/main.go` | HTTP API, операции с Redis, счётчик входов на Lua |
| `app/internal/redisconn/redisconn.go` | Подключение через Sentinel |
| `app/cmd/worker/main.go` | Чтение и подтверждение уведомлений из Stream |
| `app/Dockerfile`, `app/go.mod`, `app/go.sum` | Сборка Go и зависимости |

## Архитектура

`api` и `worker` находят текущий мастер через три Sentinel (`mymaster`, quorum 2). У Redis один мастер и две реплики. API пишет в мастер; рейтинг и достижения читает с реплики, при ошибке или задержке повторяет запрос к мастеру. `worker` читает Stream `notifications` и подтверждает сообщения через `XACK`.

## Запуск

Из каталога `solution`:

```bash
# На macOS с Colima:
colima start

docker compose up --build -d
docker compose ps
curl -sS http://localhost:8000/health
```

API: `http://localhost:8000`.

Проверка сборки без Docker: `(cd app && go build ./...)`.

## API

| Метод | Адрес | Тело |
| --- | --- | --- |
| `POST` | `/api/players/{id}` | `name`, `level`, `region`; `created_at` необязателен |
| `GET` | `/api/players/{id}` | — |
| `PATCH` | `/api/players/{id}/level` | `delta` |
| `POST` | `/api/players/{id}/login` | — |
| `POST` | `/api/leaderboard/score` | `player_id`, `score` |
| `GET` | `/api/leaderboard/top?limit=10` | — |
| `GET` | `/api/leaderboard/rank/{id}` | — |
| `POST` | `/api/players/{id}/achievements` | `achievement_name` |
| `GET` | `/api/players/{id}/achievements/{name}` | — |
| `GET` | `/api/players/{id1}/achievements/common/{id2}` | — |
| `POST` | `/api/players/batch` | `{"players":[{"id":1001,"name":"Ada","level":1,"region":"EU"}]}` |

`GET /api/players/{id}`: кеш виден в заголовке `X-Cache` (`MISS`/`HIT`). Место в таблице считается от большего счёта, начиная с 1.

| Ключ Redis | Тип |
| --- | --- |
| `player:{id}` | Hash |
| `logins:{id}` | String, TTL 24 часа |
| `tournament:main` | Sorted Set |
| `achievements:{id}` | Set |
| `cache:player:{id}` | JSON String, TTL 60 секунд |
| `notifications` | Stream, записи за последние 7 дней |

## Проверка

Пример данных:

```bash
curl -sS -X POST localhost:8000/api/players/1001 -H 'Content-Type: application/json' -d '{"name":"Ada","level":5,"region":"EU"}'
curl -sS -X POST localhost:8000/api/players/1001/login
curl -sS -X POST localhost:8000/api/leaderboard/score -H 'Content-Type: application/json' -d '{"player_id":1001,"score":120}'
curl -sS 'localhost:8000/api/leaderboard/top?limit=10'
curl -sS localhost:8000/api/leaderboard/rank/1001
curl -sS -X POST localhost:8000/api/players/1001/achievements -H 'Content-Type: application/json' -d '{"achievement_name":"first_win"}'
curl -sS localhost:8000/api/players/1001/achievements/first_win
curl -sS -X PATCH localhost:8000/api/players/1001/level -H 'Content-Type: application/json' -d '{"delta":2}'
curl -i localhost:8000/api/players/1001
curl -i localhost:8000/api/players/1001
```

Пакетная загрузка 19 игроков:

```bash
python3 - <<'PY' | curl -sS -X POST localhost:8000/api/players/batch -H 'Content-Type: application/json' --data-binary @-
import json
print(json.dumps({"players": [{"id": i, "name": f"Player {i}", "level": 1, "region": "EU"} for i in range(1002, 1021)]}))
PY
```

```bash
curl -sS -X POST localhost:8000/api/players/1002/achievements -H 'Content-Type: application/json' -d '{"achievement_name":"first_win"}'
curl -sS localhost:8000/api/players/1001/achievements/common/1002
```

Проверочный скрипт и команды Redis:

```bash
python3 ../dz1_check.py
docker compose exec -T redis-master redis-cli INFO replication
docker compose exec -T sentinel-1 redis-cli -p 26379 SENTINEL get-master-addr-by-name mymaster
docker compose exec -T redis-master redis-cli TTL cache:player:1001
docker compose exec -T redis-master redis-cli XINFO GROUPS notifications
docker compose exec -T redis-master redis-cli XPENDING notifications notifications-group
docker compose logs --tail=20 worker
```

`dz1_check.py` запускать до failover: он обращается к `redis-master` по имени контейнера.
Если failover уже был: `docker compose down -v && docker compose up --build -d`, затем заново выполнить команды создания данных выше.

Запрет записи без реплик (ответ `NOREPLICAS`):

```bash
docker pause redis-replica-1 redis-replica-2
sleep 12
docker compose exec -T redis-master redis-cli SET splitbrain:test 1
docker unpause redis-replica-1 redis-replica-2
```

Перед failover дождаться `connected_slaves:2` в `INFO replication`:

```bash
docker stop redis-master
sleep 20
docker compose exec -T sentinel-1 redis-cli -p 26379 SENTINEL get-master-addr-by-name mymaster
curl -sS -X POST localhost:8000/api/players/1101 -H 'Content-Type: application/json' -d '{"name":"Failover Test","level":1,"region":"EU"}'
docker start redis-master
```

Остановка: `docker compose down`. Удаление данных и сброс ролей: `docker compose down -v`.
