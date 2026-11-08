# Messaging — stack verification

Goal: prove, step by step, that the broker is up, the **topology is declared
by the app**, a **202 SENT** is **buffered** (not consumed), a **consumer**
drains it to `DELIVERED`, a **poison** lands in the **DLQ**, and a **broker
outage** is a clean **503**. Everything here is **copy-pasteable**.

> **Prereq:** the TP1 app (your `springboot` repo) is up at
> `http://localhost:8080`, **TP1 is green**, and you have **two** TP1
> accounts (e.g. `buyer@test` and `seller@test`) plus **one more**
> (e.g. `carol@test`) for the non-participant cases. Create them with
> TP1's `POST /auth/register`.
>
> **Paths:** the commands below use two variables (set them once in the
> terminal, see the "Paths" box below).

## 1) Prereqs and ports

| Host port | Service | Notes |
|---|---|---|
| `8080` | `listings-api` | TP1 (unchanged) |
| `5435` | `listings-postgres` | TP1 (unchanged) |
| `5673` | `rabbitmq` (AMQP) | internal `5672` |
| `15673` | `rabbitmq` (management) | internal `15672` — the UI + the `curl` target |

Broker login (both the UI and the API): **`listings`** / **`listings`**.

## 2) Start

```bash
# 1) RabbitMQ first (the app connects to it on boot — broker must be up first)
cd "$MSG_DIR" && docker compose up -d
docker compose ps                              # rabbitmq healthy

# 2) then the TP1 app (rebuild to pick up the starter + edits from README §0)
cd "$TP1_DIR" && docker compose up -d --build
curl -fsS http://localhost:8080/actuator/health
```

## 3) Set the two repo paths (once per terminal)

The commands below target your **two repos** by the `TP1_DIR` and `MSG_DIR`
variables (your two repo folders can be **named** anything — the commands do **not** assume a relative path):

```bash
export TP1_DIR="/path/to/your-tps1-repo"     # the Spring Boot app repo
export MSG_DIR="/path/to/messaging-repo"     # this messaging repo
```

## 4) The login credentials (used in every curl below)

```bash
BUYER='buyer@test:secret123'
SELLER='seller@test:secret123'
OTHER='carol@test:secret123'
MQ='listings:listings'     # the management API
```

> Replace the emails/passwords with **your** TP1 accounts. The management
> API uses `listings:listings` (the `RABBITMQ_DEFAULT_*` in the compose).

## 5) Step 1 — broker health

```bash
# 200 with Basic auth, 401 without
curl -fsS -u $MQ http://localhost:15673/api/overview | python3 -m json.tool | head -8
curl -s  -o /dev/null -w "no-auth -> HTTP %{http_code}\n" http://localhost:15673/api/overview
```

Expected: first `curl` returns a JSON object (`rabbitmq_version`, …); second →
**HTTP 401**.

```bash
curl -fsS http://localhost:8080/actuator/health | python3 -m json.tool
```

## 6) Step 2 — the app-declared topology

```bash
# the direct exchange, type=direct
curl -fsS -u $MQ "http://localhost:15673/api/exchanges?name=listings.direct" | python3 -m json.tool

# the two durable queues + their dead-letter arguments
curl -fsS -u $MQ "http://localhost:15673/api/queues?name=listings.messages" | python3 -m json.tool
curl -fsS -u $MQ "http://localhost:15673/api/queues?name=listings.messages.dlq" | python3 -m json.tool

# the binding exchange -> main (routing key = listings.messages)
# bindings of the `listings.direct` exchange (vhost = `/`, the default)
curl -fsS -u $MQ "http://localhost:15673/api/exchanges/listings.direct/bindings/queue" \
  | python3 -c 'import sys,json; [print(b["veto"],b["routing_key"],b["destination"]) for b in json.load(sys.stdin)]'
```

Expected: exchange `listings.direct` (`type: direct`); queue `listings.messages`
with `durable: true` **and** an `arguments` object containing
`x-dead-letter-exchange`, `x-dead-letter-routing-key`,
`x-dead-letter-exchange-type`; queue `listings.messages.dlq` (`durable: true`,
`consumers: 0`).

> **Durable check:** `docker compose -f "$MSG_DIR/docker-compose.yml" restart
> rabbitmq && sleep 6` → re-run the two `GET /api/queues` — **both still exist**
> (the volume persisted them).

## 7) Step 3 — publish (202 SENT, buffered, NOT consumed)

```bash
# the seller must own listing 1 (create one as SELLER first if not)
curl -fsS -u $SELLER -X POST http://localhost:8080/listings \
  -H 'Content-Type: application/json' \
  -d '{"title":"Bike","description":"road bike","price":350,"category":"SPORTS"}'

# BUYER posts a question -> 202
resp=$(curl -fsS -u $BUYER -X POST http://localhost:8080/listings/1/messages \
  -H 'Content-Type: application/json' -d '{"body":"Is this still available?"}')
echo "$resp"
MID=$(echo "$resp" | python3 -c 'import sys,json; print(json.load(sys.stdin)["messageId"])')
echo "messageId=$MID"
```

Expected: `202`, body `{"messageId":"<uuid>","status":"SENT"}`.

```bash
# still sitting in the broker (not consumed)
curl -fsS -u $MQ "http://localhost:15673/api/queues?name=listings.messages" \
  | python3 -c 'import sys,json; q=json.load(sys.stdin)[0]; print("ready =",q["messages_ready"],"consumers =",q["consumers"])'

# the DB row is SENT (the 202 happened before any consume)
cd "$TP1_DIR" && docker compose exec listings-postgres psql -U listings -d listings \
  -c "SELECT message_id, from_user_id, to_user_id, status FROM message WHERE message_id='$MID'"
```

Expected: `ready = 1`, `consumers = 0` (Step 3 alone); DB row `status = SENT`.

### 6a) Step 3 — the error contracts

```bash
# 422 self-message (buyer posting on a listing the BUYER owns)
# 422 bad inReplyTo, 422 empty body, 404 unknown listing, 401 anonymous:
curl -s -o - -w "\n-> HTTP %{http_code}\n" -u $BUYER -X POST http://localhost:8080/listings/99999/messages -H 'Content-Type: application/json' -d '{"body":"hi"}'
curl -s -o - -w "\n-> HTTP %{http_code}\n" -u $BUYER -X POST http://localhost:8080/listings/1/messages -H 'Content-Type: application/json' -d '{"body":"hi","inReplyTo":"00000000-0000-0000-0000-000000000000"}'
curl -s -o - -w "\n-> HTTP %{http_code}\n" -u $BUYER -X POST http://localhost:8080/listings/1/messages -H 'Content-Type: application/json' -d '{"body":""}'
curl -s -o - -w "\n-> HTTP %{http_code}\n" -X POST http://localhost:8080/listings/1/messages -H 'Content-Type: application/json' -d '{"body":"hi"}'
```

## 8) Step 4 — consume + reply (the thread)

```bash
# the SELLER now reads the thread -> should see the BUYER's SENT message
curl -fsS -u $SELLER http://localhost:8080/listings/1/messages | python3 -m json.tool

# SELLER replies (inReplyTo = the buyer's messageId)
curl -fsS -u $SELLER -X POST http://localhost:8080/listings/1/messages \
  -H 'Content-Type: application/json' -d "{\"body\":\"Yes, still available!\",\"inReplyTo\":\"$MID\"}"

# sleep for the @RabbitListener to drain, then the BUYER reads the full thread
sleep 3
curl -fsS -u $BUYER http://localhost:8080/listings/1/messages | python3 -m json.tool

# DB: every row is DELIVERED now
cd "$TP1_DIR" && docker compose exec listings-postgres psql -U listings -d listings \
  -c "SELECT to_char(sent_at at time zone 'UTC' as 'HH24:MI:SS'), from_user_id, to_user_id, status FROM message WHERE listing_id=1 ORDER BY sent_at"

# a NON-participant sees an EMPTY thread (not 403)
curl -s -o - -w "\n-> HTTP %{http_code}\n" -u $OTHER http://localhost:8080/listings/1/messages
```

## 9) Step 5 — poison → DLQ

Publish a message that **does not match** the envelope (the consumer cannot
parse it → rejects with `requeue=false` → broker reroutes to the DLQ):

```bash
# publish a "bogus" body to the main exchange with the main routing key
curl -fsS -u $MQ -X POST \
  "http://localhost:15673/api/exchanges/listings.direct/%2F/publish?routing_key=listings.messages&exchange=listings.direct" \
  -H 'content-type: application/json' \
  -d '{"bogus":true,"noMessageId":"x"}' -o /dev/null -w "publish poison -> HTTP %{http_code}\n"

sleep 3
# main queue: ready did NOT grow (the poison is not a legitimate business row)
curl -fsS -u $MQ "http://localhost:15673/api/queues?name=listings.messages" \
  | python3 -c 'import sys,json; q=json.load(sys.stdin)[0]; print("main ready =",q["messages_ready"])'
# DLQ: the poison landed here
curl -fsS -u $MQ "http://localhost:15673/api/queues?name=listings.messages.dlq" \
  | python3 -c 'import sys,json; q=json.load(sys.stdin)[0]; print("dlq ready =",q["messages_ready"])'
# peek the dead message body (read-only)
curl -fsS -u $MQ "http://localhost:15673/api/queues/%2F/listings.messages.dlq/get?count=1" | python3 -m json.tool

# a legit message AFTER the poison still processes (queue not blocked)
curl -fsS -u $BUYER -X POST http://localhost:8080/listings/1/messages \
  -H 'Content-Type: application/json' -d '{"body":"after the poison, still here?"}' >/dev/null
sleep 2
docker compose -f "$TP1_DIR/docker-compose.yml" exec listings-postgres psql -U listings -d listings \
  -c "SELECT count(*) AS delivered FROM message WHERE status='DELIVERED'"
```

> **Clean the DLQ** (optional, after you have read it): in the UI, or
> `curl -fsS -u $MQ -X DELETE "http://localhost:15673/api/queues/%2F/listings.messages.dlq"`.

## 10) Step 6 — broker down → 503 (API survives)

```bash
# stop ONLY the broker
cd "$MSG_DIR" && docker compose stop rabbitmq

# publish -> 503 (clean, not 500, not a hang)
curl -s -o - -w "\n-> HTTP %{http_code}\n" -u $BUYER -X POST \
  http://localhost:8080/listings/1/messages -H 'Content-Type: application/json' -d '{"body":"still there?"}'

# the API is ALIVE and the other TP1 paths are fine
curl -s -o /dev/null -w "actuator health -> HTTP %{http_code}\n" http://localhost:8080/actuator/health
curl -s -o /dev/null -w "GET /listings     -> HTTP %{http_code}\n" -u $BUYER http://localhost:8080/listings

# recover
docker compose start rabbitmq && sleep 6
curl -s -o - -w "\n-> HTTP %{http_code}\n" -u $BUYER -X POST \
  http://localhost:8080/listings/1/messages -H 'Content-Type: application/json' -d '{"body":"back?"}'
```

Expected: `503` (with a JSON `message` mentioning the broker) while stopped;
`200` health + `200` GET while stopped; `202` with a **new** `messageId` after
start.

## 11) Step 7 (bonus) — at-least-once / no duplication

Crash the **consumer** (the API) in the middle of a delivery, then restart —
the message must be **redelivered** but **not duplicated**.

```bash
# one in-flight message, then kill the app before ack
curl -fsS -u $BUYER -X POST http://localhost:8080/listings/1/messages \
  -H 'Content-Type: application/json' -d '{"body":"in flight?"}' >/dev/null
cd "$TP1_DIR" && docker compose stop listings-api     # broker KEEPS it (durable)

# the queue still holds it (ready=1), 0 delivered
curl -fsS -u $MQ "http://localhost:15673/api/queues?name=listings.messages" \
  | python3 -c 'import sys,json;q=json.load(sys.stdin)[0];print("ready =",q["messages_ready"],"consumers =",q["consumers"])'

# restart the consumer -> it drains, deliveries counter shows >= 2 (at-least-once)
cd "$TP1_DIR" && docker compose up -d listings-api && sleep 6
curl -fsS -u $MQ "http://localhost:15673/api/queues?name=listings.messages" \
  | python3 -c 'import sys,json;q=json.load(sys.stdin)[0];print("ready =",q["messages_ready"],"deliveries =",q["deliveries_total"])'

# NO duplication: exactly the expected number of rows (not double)
cd "$TP1_DIR" && docker compose exec listings-postgres psql -U listings -d listings \
  -c "SELECT status, count(*) FROM message WHERE listing_id=1 GROUP BY status"
```

## 12) Step 8 (bonus) — transactional outbox (202 even broker down)

```bash
cd "$MSG_DIR" && docker compose stop rabbitmq
# with the outbox enabled (Steps 8), the publish is 202 PENDING, not 503
curl -s -o - -w "\n-> HTTP %{http_code}\n" -u $BUYER -X POST \
  http://localhost:8080/listings/1/messages -H 'Content-Type: application/json' -d '{"body":"queued while down?"}'

# the row is PENDING in the DB (not in the broker)
cd "$TP1_DIR" && docker compose exec listings-postgres psql -U listings -d listings \
  -c "SELECT message_id, status FROM message ORDER BY sent_at DESC LIMIT 1"

# start the broker -> the poller claims + publishes -> SENT -> DELIVERED
cd "$MSG_DIR" && docker compose start rabbitmq && sleep 10
cd "$TP1_DIR" && docker compose exec listings-postgres psql -U listings -d listings \
  -c "SELECT message_id, status FROM message ORDER BY sent_at DESC LIMIT 1"
```

## 13) Stop

```bash
cd "$MSG_DIR" && docker compose down          # keep the broker volume
cd "$TP1_DIR" && docker compose down          # keep the Postgres
# wipe everything: add -v to either `down`
```

## 14) Common issues

| Symptom | Cause | Fix |
|---|---|---|
| app logs `AmqpConnectException` on boot | broker not up **yet** | start the **broker first**, then rebuild the app (README step 2) |
| `500` instead of `503` when broker down | the `catch (AmqpConnectException)` is missing | add the **Step 6** wrap + `@ExceptionHandler` (see `exercise.md` Step 6) |
| message **loops** (ready count climbs) | a consumer is **rejecting with `requeue=true`** | always `requeue=false` (Step 5) |
| DLQ stays **empty** after a poison | queue **does not have** the `x-dead-letter-*` args | re-check `RabbitConfig` (Step 2) — args are on the **main queue** |
| `guest`/`ACCESS_REFUSED` on connect | you used the stock `guest` account | use `listings`/`listings` (the `RABBITMQ_DEFAULT_*`) |
| `401` on the management API | missing `-u listings:listings` | the mgmt API is **not public**; pass the broker creds |
| second `spring:` key in `application.yml` | pasted the §0 block as a new top-level key | put `rabbitmq:` **under the existing** `spring:` (README §0.2) |
