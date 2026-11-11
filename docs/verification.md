# Messaging — stack verification

Copy-pasteable commands for every `Verify` section of
[`../exercise.md`](../exercise.md): the broker is up, the **topology is
declared by the app**, a **202 SENT** is **buffered**, a **consumer** drains
it to `DELIVERED`, a **poison** lands in the **DLQ**, and a **broker outage**
is a clean **503**.

> **Prerequisite:** TP1 is green and up on `http://localhost:8080`, with the
> TP1 accounts `alice@test.org` (owner of listing `1`), `bob@test.org`
> (buyer), and `carol@test.org` (non-participant), password `secret123`
> (TP1 `docs/verification.md` §4 creates alice and bob; register carol the
> same way).

## 1) Ports

| Host port | Service | Notes |
|---|---|---|
| `8080` | `listings-api` | TP1 (unchanged) |
| `5435` | `listings-postgres` | TP1 (unchanged) |
| `5673` | `rabbitmq` (AMQP) | internal `5672` |
| `15673` | `rabbitmq` (management) | internal `15672` — the UI and the `curl` target |

## 2) Start

```bash
cd "$MSG_DIR" && docker compose up -d          # 1) the broker first
docker compose ps                               # rabbitmq healthy
cd "$TP1_DIR" && docker compose up -d --build   # 2) then the app (README §0 edits applied)
curl -fsS http://localhost:8080/actuator/health
```

## 3) Variables and helpers (once per terminal)

```bash
export TP1_DIR="/path/to/your-tp1-repo"        # the Spring Boot app repo
export MSG_DIR="/path/to/messaging-repo"       # this repo
API=http://localhost:8080
MGMT=http://localhost:15673/api
MQ='listings:listings'                         # management API credentials
SELLER='alice@test.org:secret123'              # owner of listing 1
BUYER='bob@test.org:secret123'
OTHER='carol@test.org:secret123'
JSON='Content-Type: application/json'

# one queue (NB: /api/queues?name=… does NOT filter: always use the path form).
# The management counters (ready, unacked) are refreshed every ~5 s: wait ~6 s
# after an action before reading them.
queue() { curl -fsS -u $MQ "$MGMT/queues/%2F/$1" | python3 -c '
import sys, json; q = json.load(sys.stdin)
print(q["name"], "| type", q["type"], "| ready", q["messages_ready"],
      "| unacked", q["messages_unacknowledged"], "| consumers", q["consumers"])'; }
# publish a raw payload to listings.direct with the main routing key
publish() { curl -fsS -u $MQ -X POST "$MGMT/exchanges/%2F/listings.direct/publish" -H "$JSON" \
  -d "$(python3 -c 'import json,sys; print(json.dumps({"properties":{"delivery_mode":2},"routing_key":"listings.messages","payload":sys.argv[1],"payload_encoding":"string"}))' "$1")"; echo; }
sql() { (cd "$TP1_DIR" && docker compose exec -T listings-postgres psql -U listings -d listings -c "$1"); }
```

## 4) Listing 1 belongs to alice

```bash
curl -fsS -u $SELLER "$API/listings/1" | python3 -m json.tool
# if missing, create it as alice (TP1 Step 2):
# curl -fsS -u $SELLER -X POST "$API/listings" -H "$JSON" -d '{"title":"Bike","price":350,"category":"SPORTS"}'
```

## 5) Step 1 — broker health

```bash
curl -fsS -u $MQ "$MGMT/overview" | python3 -c 'import sys,json; print("rabbitmq", json.load(sys.stdin)["rabbitmq_version"])'
curl -s -o /dev/null -w "no-auth -> HTTP %{http_code}\n" "$MGMT/overview"      # 401
curl -fsS "$API/actuator/health"                                                # {"status":"UP"...}
curl -fsS -u $MQ "$MGMT/queues" | python3 -c 'import sys,json; print([q["name"] for q in json.load(sys.stdin)])'   # [] before Step 2
```

## 6) Step 2 — the app-declared topology

```bash
for x in listings.direct listings.messages.dlx; do
  curl -fsS -u $MQ "$MGMT/exchanges/%2F/$x" | python3 -c 'import sys,json; e=json.load(sys.stdin); print(e["name"], e["type"], "durable", e["durable"])'
done
queue listings.messages            # type quorum
queue listings.messages.dlq        # type classic, consumers 0
curl -fsS -u $MQ "$MGMT/queues/%2F/listings.messages" | python3 -c 'import sys,json; print(json.load(sys.stdin)["arguments"])'
# expected: x-queue-type quorum, x-delivery-limit 3, x-dead-letter-exchange, x-dead-letter-routing-key

# bindings (routing key -> queue)
for x in listings.direct listings.messages.dlx; do
  curl -fsS -u $MQ "$MGMT/exchanges/%2F/$x/bindings/source" \
    | python3 -c 'import sys,json; [print(b["source"], "--", b["routing_key"], "->", b["destination"]) for b in json.load(sys.stdin)]'
done

# durability: restart the broker, the queues are still there
(cd "$MSG_DIR" && docker compose restart rabbitmq) && sleep 15
queue listings.messages; queue listings.messages.dlq
```

## 7) Step 3 — publish (202 SENT)

Before Step 4 exists (no consumer yet):

```bash
resp=$(curl -fsS -u $BUYER -X POST "$API/listings/1/messages" -H "$JSON" -d '{"body":"Is this still available?"}')
echo "$resp"                                                 # {"messageId":"<uuid>","status":"SENT"}
MID=$(echo "$resp" | python3 -c 'import sys,json; print(json.load(sys.stdin)["messageId"])')
queue listings.messages                                      # ready >= 1, consumers 0
sql "SELECT message_id, from_user_id, to_user_id, status FROM message WHERE message_id='$MID'"
```

Error contracts:

```bash
code() { curl -s -o /dev/null -w "%{http_code}\n" "$@"; }
code -X POST "$API/listings/1/messages" -H "$JSON" -d '{"body":"hi"}'                                  # 401
code -u $BUYER -X POST "$API/listings/99999/messages" -H "$JSON" -d '{"body":"hi"}'                    # 404
code -u $BUYER -X POST "$API/listings/1/messages" -H "$JSON" -d '{"body":""}'                          # 422
code -u $BUYER -X POST "$API/listings/1/messages" -H "$JSON" \
  -d '{"body":"hi","inReplyTo":"00000000-0000-0000-0000-000000000000"}'                                # 422
code -u $SELLER -X POST "$API/listings/1/messages" -H "$JSON" -d '{"body":"to myself"}'               # 422
code -u $OTHER "$API/listings/1/messages"                                                              # 200 (empty list)
curl -fsS -u $SELLER "$API/listings/1/messages" | python3 -m json.tool                                 # full thread
```

## 8) Step 4 — consume + reply

```bash
sleep 6; queue listings.messages                             # consumers 1, ready back to 0
sql "SELECT status, delivered_at FROM message WHERE message_id='$MID'"   # DELIVERED

# the seller replies to the buyer
curl -fsS -u $SELLER -X POST "$API/listings/1/messages" -H "$JSON" \
  -d "{\"body\":\"Yes, still available\",\"inReplyTo\":\"$MID\"}"; echo
sleep 2
curl -fsS -u $BUYER "$API/listings/1/messages" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(len(d), "turns:", [m.get("body") for m in d])'   # 2 turns

# duplicate delivery: re-publish the exact envelope of $MID -> still one row
ENV=$(sql "SELECT json_build_object('messageId',message_id,'listingId',listing_id,'fromUserId',from_user_id,'toUserId',to_user_id,'inReplyToMessageId',in_reply_to,'body',body,'sentAt',sent_at) FROM message WHERE message_id='$MID'" | sed -n 3p)
publish "$ENV"; sleep 2
sql "SELECT count(*) FROM message WHERE message_id='$MID'"                # 1
(cd "$TP1_DIR" && docker compose logs --since 2m listings-api | grep -iE "warn|exception" | tail -5)
```

Adapt the column names in the `json_build_object` to your changelog if they
differ (`in_reply_to` / `in_reply_to_message_id`).

## 9) Step 5 — poison → DLQ

```bash
queue listings.messages.dlq                                  # note the ready count
publish '{"oops":true}'                                      # {"routed":true}
sleep 6                                                      # counters lag ~5 s
queue listings.messages                                      # ready 0 (not stuck)
queue listings.messages.dlq                                  # ready +1

# peek the dead message (stays in the DLQ)
curl -fsS -u $MQ -X POST "$MGMT/queues/%2F/listings.messages.dlq/get" -H "$JSON" \
  -d '{"count":5,"ackmode":"ack_requeue_true","encoding":"auto"}' \
  | python3 -c 'import sys,json; [print(m["payload"], "| x-death:", [(d["queue"], d["reason"]) for d in m["properties"].get("headers",{}).get("x-death",[])]) for m in json.load(sys.stdin)]'
# expected: {"oops":true} | x-death: [('listings.messages', 'rejected')]

# a legitimate message afterwards is still processed
curl -fsS -u $BUYER -X POST "$API/listings/1/messages" -H "$JSON" -d '{"body":"after the poison"}'; echo
sleep 2
sql "SELECT status, count(*) FROM message GROUP BY status"   # no SENT left
(cd "$TP1_DIR" && docker compose logs --since 2m listings-api | grep -c "unknown delivery tag")   # 0

# optional: empty the DLQ after reading it
# curl -fsS -u $MQ -X DELETE "$MGMT/queues/%2F/listings.messages.dlq/contents"
```

## 10) Step 6 — broker down → 503

```bash
(cd "$MSG_DIR" && docker compose stop rabbitmq)
BEFORE=$(sql "SELECT count(*) FROM message" | sed -n 3p)

curl -s -w "\n-> HTTP %{http_code} in %{time_total}s\n" -u $BUYER -X POST "$API/listings/1/messages" \
  -H "$JSON" -d '{"body":"still there?"}'                    # 503, TP1 JSON shape, a few seconds
AFTER=$(sql "SELECT count(*) FROM message" | sed -n 3p)
echo "rows before=$BEFORE after=$AFTER (expect equal)"

for e in health/liveness health; do curl -s -o /dev/null -w "/actuator/$e -> %{http_code}\n" "$API/actuator/$e"; done
# expected: liveness 200 ; health 503 (rabbit DOWN)
curl -s -o /dev/null -w "GET /listings -> %{http_code}\n" "$API/listings"   # 200

(cd "$MSG_DIR" && docker compose start rabbitmq)
until (cd "$MSG_DIR" && docker compose ps rabbitmq | grep -q healthy); do sleep 3; done; sleep 10
curl -s -w "\n-> HTTP %{http_code}\n" -u $BUYER -X POST "$API/listings/1/messages" -H "$JSON" -d '{"body":"back?"}'   # 202
queue listings.messages                                      # consumers 1 again
sql "SELECT status, count(*) FROM message GROUP BY status"   # all DELIVERED
```

## 11) Step 7 (bonus) — duplicates and redelivery

Duplicate drill (deterministic): §8 above, last block, publishing the same
envelope **3 times**:

```bash
for i in 1 2 3; do publish "$ENV"; done; sleep 2
sql "SELECT count(*), max(status) FROM message WHERE message_id='$MID'"   # 1 | DELIVERED
```

Crash drill: a burst of valid envelopes, the API killed in the middle.

```bash
OWNER=$(sql "SELECT to_user_id FROM message WHERE message_id='$MID'" | sed -n 3p)
FROM=$(sql "SELECT from_user_id FROM message WHERE message_id='$MID'" | sed -n 3p)
( for i in $(seq 1 300); do
    publish "{\"messageId\":\"$(python3 -c 'import uuid;print(uuid.uuid4())')\",\"listingId\":1,\"fromUserId\":$FROM,\"toUserId\":$OWNER,\"inReplyToMessageId\":null,\"body\":\"burst $i\",\"sentAt\":\"$(date -u +%FT%TZ)\"}" >/dev/null
  done ) &
sleep 3; (cd "$TP1_DIR" && docker compose kill listings-api); wait
queue listings.messages                                      # ready > 0: the rest of the burst waits
(cd "$TP1_DIR" && docker compose start listings-api); sleep 20

sql "SELECT message_id FROM message GROUP BY message_id HAVING count(*) > 1"   # 0 rows
(cd "$TP1_DIR" && docker compose logs listings-api | grep -c "redelivered=true")   # >= 1 (else retry with a larger burst)
(cd "$TP1_DIR" && docker compose logs listings-api | grep -ciE "duplicate key")    # 0
```

## 12) Step 8 (bonus) — transactional outbox

```bash
(cd "$MSG_DIR" && docker compose stop rabbitmq)
curl -s -w "\n-> HTTP %{http_code}\n" -u $BUYER -X POST "$API/listings/1/messages" -H "$JSON" -d '{"body":"queued while down?"}'
# 202 with "status":"PENDING"
sql "SELECT message_id, status FROM message ORDER BY sent_at DESC LIMIT 1"    # PENDING

(cd "$MSG_DIR" && docker compose start rabbitmq)
until (cd "$MSG_DIR" && docker compose ps rabbitmq | grep -q healthy); do sleep 3; done; sleep 15
sql "SELECT message_id, status FROM message ORDER BY sent_at DESC LIMIT 1"    # SENT, then DELIVERED
sql "SELECT count(*) FROM message WHERE status='PENDING'"                     # 0
```

## 13) Stop

```bash
cd "$MSG_DIR" && docker compose down          # keep the broker volume
cd "$TP1_DIR" && docker compose down          # keep the Postgres data
# wipe: add -v
```

## 14) Common issues

| Symptom | Cause | Fix |
|---|---|---|
| app loops on `PRECONDITION_FAILED - invalid arg 'x-delivery-limit'` and does not start | main queue declared **classic** | declare it **quorum** (Step 2); delete the old queue in the UI first |
| `PRECONDITION_FAILED - inequivalent arg …` at startup | the queue already exists with other arguments | delete it in the UI (or `docker compose down -v` on the broker), restart the app |
| the same warn line thousands of times, `unacked` stuck at 1 | a plain exception is **requeued** forever (classic queue, no delivery limit) | throw `AmqpRejectAndDontRequeueException` for poison (Step 5); quorum queue + `x-delivery-limit` |
| `PRECONDITION_FAILED - unknown delivery tag` | manual `basicNack`/`basicAck` in `AUTO` acknowledge mode | remove the manual call (Step 5) |
| `500` with `AmqpIOException` when the broker is down | the `catch` names `AmqpConnectException` only | catch `AmqpException` (Step 6) |
| the first request after the broker stops hangs ~60 s | default connection timeout | `spring.rabbitmq.connection-timeout: 2s` (README §0) |
| `/actuator/health` is 503 while the broker is down | `rabbit` health indicator | expected; `/actuator/health/liveness` is the "app alive" probe |
| a `SENT` row that never became `DELIVERED` | the row was kept after a failed publish | delete it on `AmqpException` (Step 6) |
| `GET /api/queues?name=…` shows the wrong queue | that query does not filter | use `/api/queues/%2F/<name>` |
| `ACCESS_REFUSED` on connect | the stock `guest` account | use `listings`/`listings` |
| `401` on the management API | missing `-u listings:listings` | pass the broker credentials |
| second `spring:` key in `application.yml` | §0 block pasted as a new top-level key | put `rabbitmq:` under the existing `spring:` |
