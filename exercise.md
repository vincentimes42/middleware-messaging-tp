# TP Messaging — asynchronous seller↔buyer messaging (RabbitMQ) (8h)

## Overview

This is a **follow-up** to the Spring Boot TP (TP1, the **Spring Boot repo** you have). We reuse
the **TP1 listings API** — unchanged domain, unchanged port (8080), unchanged
**HTTP Basic** auth — and add a **messaging layer**: an asynchronous
**buyer → seller** conversation, backed by an **external AMQP broker**
(RabbitMQ) that you do **not** implement. Two small skills:

1. **Your API becomes a producer.** `POST /listings/{id}/messages` publishes a
   message to a broker queue and returns **202** (`SENT`) *before anyone
   consumes it* — that is the point of a queue.
2. **Your API also becomes a consumer.** A `@RabbitListener` delivers the
   message, **persists it** (a `message` table), and lets the **seller read it**
   (a new `GET /listings/{id}/messages` thread view). Several buyers can queue
   up before the seller is even online — that **buffer is the middleware**.

There is **one new service**: the **RabbitMQ** broker (management UI on port
**15673**, AMQP on port **5673**). The API stays on **8080**, unchanged.

**Validation method (the contract):** you prove each step with **`curl`** (the
REST endpoint and the RabbitMQ **management API**), a read of the **Postgres**
`message` table, the **management UI** (queue `ready` count, exchanges,
bindings, the DLQ), and a **grep of your own Java** — **not** with a Java test
suite (writing tests is optional and not part of the deliverable). The per-step
`Verify` (Step 9: `Final checks`) **is** the "definition of done". Every command you need
is copy-pasteable in [`docs/verification.md`](docs/verification.md). The
**code** lives in **your Spring Boot (TP1) repo** (a new
`com.app.listings.messaging` package); **this repo** (`messaging/`) only owns
the statement, the broker compose, and the verification commands.

> **Prerequisite: TP1 is done and green.** The TP1 API passes its core
> checklists (register, CRUD, ownership 401/403/404, `?version=V1|V2`) and runs
> on port 8080. Finish TP1 first — this TP only adds a messaging layer on top.

---

## What is a message queue? (one minute)

**A message queue is a broker that sits between two parties that do not have
to be online at the same time.** Your API talks to *RabbitMQ*, not to the
seller's "inbox". This changes the failure model: a request that would be a
**500 / timeout** in a synchronous REST call becomes an **accepted, queued
message** (202) that is delivered later.

| Concept | What it is here |
|---|---|
| **Producer** | your `listings-api` — `POST /listings/{id}/messages` **publishes** a message |
| **Consumer** | the same `listings-api` — a `@RabbitListener` **consumes** and persists it |
| **Exchange** | a **named router**; you pick its **type** (this TP uses **direct**) and a **routing key** |
| **Binding** | the rule "exchange X, routing key Y → queue Z" that your producer must match |
| **Queue** | `listings.messages` — where messages **wait** until consumed |
| **Dead-letter** | a **message the broker cannot deliver** (poison, TTL, max size) is rerouted to the DLQ `listings.messages.dlq` |

> **Key idea for the day:** your `POST` is **decoupled** from delivery. The
> broker **holds** the message (and it is observable in the management UI), and
> your consumer **pulls** it. If the consumer is down, the message does **not**
> disappear — it **waits**. That "waits + can be inspected" is the entire
> reason to use a broker instead of a local `List<Message>`.

---

## Before you start — RabbitMQ & Spring AMQP

New to AMQP? This ~3-minute model makes the steps below click.

### The stack, in one picture

```
   buyer  (any TP1 account)          seller (the listing owner)
        |   POST /listings/{id}/messages   |   GET /listings/{id}/messages
        v                                  ^
   +-----------------------------------------------------------+
   |                 listings-api (TP1, 8080)                  |
   |   producer: publishes to queue      |   consumer: @RabbitListener
   |   com.app.listings.messaging.*      |   persists the message,
   +---------------------------^---------+     flips status SENT -> DELIVERED
                                | AMQP (network name `rabbitmq`:5672)
                                v
   +-----------------------------------------------------------+
   |                    RabbitMQ 4.x (5673 / 15673)            |
   |   exchange `listings.direct` -> binding -> queue ...      |
   |   queue listings.messages   (durable)                     |
   |   DLX  listings.messages.dlx -> DLQ listings.messages.dlq |
   +-----------------------------------------------------------+
```

> **Why one library and one broker:** **Spring AMQP** is the client side of
> your API (the `RabbitTemplate` producer + the `@RabbitListener` consumer).
> **RabbitMQ** is the server that stores and routes the messages. Keep the two
> separate in your head: a buyer `POST`s to your API; your API speaks **AMQP**
> to the broker; the broker routes to the queue; your API reads it back.

---

## Domain & data model (this TP)

The **business domain does not change** from TP1 (actors, `User`, `Listing`,
ownership, `active`, V1/V2 visibility, HTTP Basic). What **changes** is only the
messaging layer:

| Concern | TP1 | This TP |
|---|---|---|
| How a buyer asks the seller a question | (none) | `POST /listings/{id}/messages` → **202**, delivered **asynchronously** via the broker |
| How the seller reads questions | (none) | `GET /listings/{id}/messages` → the **conversation thread** (seller sees every buyer; each buyer sees only their own side) |
| Who is the "buyer" | — | **any** TP1 account (no new `BUYER` role; there is **no** change to TP1's `Role`, still `SELLER`). *Buying = writing to a listing you do not own.* |
| What persists the conversation | — | a **`message` table** (Liquibase **additive** `v1.1-messages.yaml`), plus the broker's own queue state |
| Who owns delivery | the HTTP call | **the broker** (queue) **and** your consumer (`@RabbitListener`) |

> **Key idea for the day:** adding messaging does **not** remove TP1's rules.
> The buyer is still **authenticated** (Basic), a **non-existent** listing is
> still a **404**, and a **self-message** (to yourself) is a **422**. The new
> rule is only one: delivery is **asynchronous** and **observable** in the
> broker.

### The `message` entity (the only new business data)

A **`message`** is one turn of the conversation. One turn links **both** sides:
the **buyer** who wrote it (`from_user_id`) and the **seller** it is for
(`to_user_id`). The **`listing_id`** is the listing the question is about.
`in_reply_to` points at the previous `message_id` in the same thread
(`null` for the opening message). `status` is the *delivery* state of **this**
row: `SENT` (published, broker holds it) → `DELIVERED` (the consumer persisted
it, the seller can read it).


---

## Scope

| Track | Steps | Target (2nd-session binôme) |
|---|---|---|
| **Core** (required — must fit the 8h) | 1 · 2 · 3 · 4 · 5 · 6 | ~5h45 |
| **Bonus** (start only when the core is green) | 7 (at-least-once / redelivery) · 8 (transactional outbox) | ~1h45 |
| **Delivery** | 9 (final integration) | ~0h15 |

Each core step's `Verify` section is your "definition of done".
Bonus steps have their own checklists; doing them is optional but counts for
the grade if you deliver them.

---

## Conventions

Fixed for the whole TP — the **exercise**, the **compose**, and the **hints**
use the **same** names, so a grep of your code must match:

- **AMQP ports (host):** `5673` (AMQP) · `15673` (management). **Network
  name** for the app: `rabbitmq:5672` (its *internal* AMQP port), never
  `localhost`.
- **Broker credentials:** user **`listings`** / pass **`listings`** (see the
  `docker-compose.yml`). The stock `guest` account is **forbidden** — it is
  loopback-restricted and unreachable from the API container.
- **Exchange:** `listings.direct`, type **direct**.
- **Routing keys:** `listings.messages` (main queue) · `listings.messages.dlq`
  (dead-letter queue).
- **Queues (all durable):** main `listings.messages` · DLQ
  `listings.messages.dlq`. **DLX:** `listings.messages.dlx`.
- **Queue args on the main queue:** `x-dead-letter-exchange: listings.messages.dlx`
  + `x-dead-letter-routing-key: listings.messages.dlq`
  + `x-dead-letter-exchange-type: direct` + `x-delivery-limit: 3`
  + `x-expires: 0` (durable) + `single-active-consumer: false`.
- **Message body (the wire format):** a **single JSON document**
  ({messageId (uuid), listingId (long), fromUserId (long), toUserId
  (long), inReplyToMessageId (uuid, nullable), body (string), sentAt (ISO-8601)})
  — the **routing** is decided by the **producer** (a constant routing key
  `listings.messages`), NOT by the message content.
- **DB table (Liquibase additive):** `message` (see
  `v1.1-messages.yaml`). `message_id` is a **unique** PK / idempotency key —
  a duplicated `messageId` is a **NO-OP** on the consumer side (the `@RabbitListener`
  upserts by `messageId`, so `at-least-once` redelivery does **not** create
  a second row). `status` ∈ `PENDING | SENT | DELIVERED`.
- **REST paths:** `POST /listings/{id}/messages` · `GET /listings/{id}/messages`.
- **Auth:** HTTP Basic (TP1, unchanged).
- **Language of the message body:** a **plain string** (no JSON schema beyond
  the envelope) — the TP1 "JSON + XML content negotiation" (Step 4 bonus of
  TP1) is **not** part of this TP.
- **Error JSON shape:** identical to TP1 (`timestamp`, `status`, `error`,
  `message`, optional `fields`).

---

## Error handling & status codes (cross-cutting)

Every step below relies on this table. The new row compared to TP1 is **503**
(broker unavailable). Everything else is **invariant** from TP1.

| Status | Trigger | Suggested mechanism |
|---|---|---|
| **401** | Unauthenticated `POST /listings/{id}/messages` | Spring Security (TP1 — no change) |
| **403** | (TP1 invariant) Non-owner writes are out of scope for this TP (no PUT on a listing you do not own) | Spring Security (TP1) |
| **404** | `POST/GET /listings/{unknown-id}/*messages` (no such listing) | `ResourceNotFoundException` (TP1) |
| **422** | **Self-message** (`from == to`); empty `body`; `inReplyTo` is not a known/visible message id on this listing; `inReplyTo` from a different thread | `MethodArgumentNotValidException` → TP1 `GlobalExceptionHandler` (with `fields` map) |
| **503** | **Broker unavailable** at publish time (or consumer cannot connect at boot — the latter is the *startup* case of Step 6) | custom `BrokerUnavailableException` (see Step 6) → **one** `@ExceptionHandler` returning the TP1 JSON shape **with status 503** and a useful message (example: `"rabbitmq not reachable — check docker compose / RABBITMQ_HOST / RABBITMQ_PORT"`) |
| **500** | (invariant) Unexpected error | TP1 `GlobalExceptionHandler` generic handler |

> **What you do NOT do in production code**
> - You do **not** catch `AmqpConnectException` (or any AMQP exception) in the
>   **listener** and swallow it **silently** — the point of the DLX/DLQ is to
>   *observe* the failure, not to hide it. Log it (TP1 `log.warn`) and **NACK**
>   so the broker reroutes it.
> - You do **not** store the `body` of a buyer message in a **log line** the
>   way you would log a request (privacy — the TP1 rule applies too).


---

## Testing strategy (only if you write tests — optional)

Writing Java tests is **not** a deliverable (see the Overview). If you do, the
levels that fit this TP are:

- **Unit** — `MessageService` mocked with Mockito: `RabbitTemplate` **captured**
  (`ArgumentCaptor`), `MessageRepository.findById(messageId)`, `ListingService`
  lookup, `InReplyToResolver` lookup.
- **Slice** — `@WebMvcTest(MessageController)` with a mocked `MessageService`
  to pin the `202` (success) / `404` (unknown listing) / `422` (self / bad
  inReplyTo) / `503` (broker down → your `BrokerUnavailableException`) contracts.
- **Integration** — `@SpringBootTest` against the running broker **and** the
  running Postgres (both up, same as in `mvn clean verify`). The **smoke test**
  you ship (the `ListingsSmokeTest` you will add) is the *lowest* bar: the
  context boots, the `RabbitAdmin` bean is present, the `@RabbitListener` is
  registered. That is it.

The **Verify** section of each step is the contract. It is verified
manually with `curl`, `psql`, and the management UI — not by a test.


---



## Step 1 — Boot the stack and confirm the connection   CORE

**Outcome** — The broker is **healthy**, the TP1 app is **up and connected**,
`mvn clean verify` stays **green**, and **no queue exists yet** (declared in
Step 2). This is your **bootstrap**: the 5 checks below must pass **before**
you write any AMQP code.

### Build it

1. **Apply** the TP1-repo edits (if not already done) — exact code in
   [`README.md` §0](README.md). Without these, the next steps cannot pass.
2. **Start the broker first**, then the app, in that order
   (README §1 → §2).
3. **Verify** — the Proof below is your checklist.

### Expected behavior
- `rabbitmq` is **healthy** in `docker compose ps` (`rabbitmq-diagnostics ping` passes).
- `GET http://localhost:15673/api/overview` → **200** with Basic `listings:listings`; → **401** without.
- `GET http://localhost:8080/actuator/health` → **200**.
- `mvn clean verify` in the TP1 repo → **BUILD SUCCESS** (the TP1 smoke test does **not** touch the broker — Postgres only).
- `GET /api/queues` with credentials → **200, empty list** (queues arrive in Step 2).

### Verify

- [ ] `cd "$MSG_DIR" && docker compose ps` → `rabbitmq` **healthy**
- [ ] `curl -fsS -u listings:listings http://localhost:15673/api/overview` → **200**; same request **without** `-u` → **401**
- [ ] `curl -fsS http://localhost:8080/actuator/health` → **200**
- [ ] `cd "$TP1_DIR" && mvn clean verify` → **BUILD SUCCESS**
- [ ] `curl -s -u listings:listings http://localhost:15673/api/queues` → **200**, empty (queues declared in Step 2)

### If you are stuck

- If `rabbitmq` is **restarting / unhealthy** → run `docker compose -f <compose> logs rabbitmq` (the broker persists to the named volume `rabbit-data`; `docker compose down -v` **wipes** it).
- If the app **fails to start** because it cannot find the broker → check that `RABBITMQ_HOST=rabbitmq` and `RABBITMQ_PORT=5672` are set in the **TP1 repo's** `docker-compose.yml` (README §0, edit c). From inside the container, the broker is `rabbitmq:5672` (the **internal** port), **not** `localhost:5673` (the **host** port).
- Boot order: **broker first** (README §1 → §2). If the app boots first, it will retry — it will still find the broker, but the clean sequence is RabbitMQ → app.
- Exact commands: [`docs/verification.md` §5](docs/verification.md) (broker health + app actuator).

---

## Step 2 - Declare the AMQP topology   CORE

**Outcome** — After the app boots, the broker contains **two durable queues**
(`listings.messages`, `listings.messages.dlq`), **two `direct` exchanges**
(`listings.direct`, `listings.messages.dlx`), and **two bindings** — all
**declared by your code**, **idempotent** across restarts, **no consumer
attached yet** (that is Step 4).

### Build it

1. **Create** the file
   `com.app.listings.messaging.config.RabbitConfig` (`@Configuration`)
   declaring **six** beans:
   - a `DirectExchange` named **`listings.direct`**
   - a durable `Queue` named **`listings.messages`** with the **exact**
     `x-dead-letter-*` + `x-delivery-limit: 3` arguments from `Conventions`
   - a durable `Queue` named **`listings.messages.dlq`** (plain — no
     dead-letter args of its own, **it is** the dead-letter sink)
   - a `DirectExchange` named **`listings.messages.dlx`** — **a separate
     exchange** (not the same as `listings.direct`)
   - a `Binding` from `listings.direct` → `listings.messages` with routing
     key **`listings.messages`**
   - a `Binding` from `listings.messages.dlx` → `listings.messages.dlq` with
     routing key **`listings.messages.dlq`**

2. **Configure** a `RabbitTemplate` bean with
   `setDefaultExchange("listings.direct")`. This is what lets the Step 3
   producer call `convertAndSend("listings.messages", msg)` with **only**
   the routing key — the exchange is the **named** one, not the
   anonymous `$default`.

3. **Restart** the app (`docker compose restart listings-api`) and
   **verify** the two queues, two exchanges, and two bindings in the
   management UI (or via `curl`, §6).

> **Why declare in code (not in the UI):** the management UI is a **view**,
> not a **source of truth**. A fresh `docker compose down -v` would wipe
> anything hand-drawn in the UI. Declared in `RabbitConfig`, the topology
> re-emerges on every boot — *the topology is code*.

### Expected behavior
- `GET /api/exchanges?name=listings.direct` → exchange **present**, `type=direct`.
- `GET /api/queues?name=listings.messages` → `durable=true`, `arguments` contains the **three** `x-dead-letter-*` keys.
- `GET /api/queues?name=listings.messages.dlq` → `durable=true`, `consumers=0`.
- Binding `listings.direct → listings.messages` (routing key `listings.messages`) — **present** in `GET /api/exchanges/listings.direct/bindings/queue`.
- `docker compose restart rabbitmq` then re-check: the **same** two queues **re-emerge** (durable + persisted in the volume).
- **No consumer attached** yet (`consumers = 0` on the main queue) — that is Step 4.

### Verify

- [ ] The six `curl`s in [`docs/verification.md` §6](docs/verification.md) all return the expected state
- [ ] `grep -n "RabbitAdmin\|@Bean.*Queue\|@Bean.*Exchange\|@Bean.*Binding" src/main/java/com/app/listings/messaging/config/RabbitConfig.java` — the file exists and contains **all** six bean definitions
- [ ] `docker compose restart rabbitmq && sleep 5` then the same two queues re-emerge (persistence)

### If you are stuck

- A `direct` exchange in Spring AMQP is **`org.springframework.amqp.core.DirectExchange`** (or `new Exchange("listings.direct", BuiltinExchangeType.DIRECT, true, false, null)`). The name **must** be literal `listings.direct` (stable, greppable).
- For the main queue args, use `QueueBuilder.durable("listings.messages").withArgument("x-dead-letter-exchange","listings.messages.dlx").withArgument("x-dead-letter-routing-key","listings.messages.dlq").withArgument("x-dead-letter-exchange-type","direct").withArgument("x-delivery-limit",3)` — see `Conventions` for the full set.
- The **DLQ** is a **plain** durable queue — no dead-letter args of its own (it **is** the sink).
- The **DLX** is its **own** direct exchange named `listings.messages.dlx` — **not** the same object as `listings.direct`.


## Step 3 — Publish: buyer asks, seller not yet online   CORE

**Outcome** — A `POST /listings/{id}/messages` from a buyer returns **202
`SENT`** immediately (the broker **holds** the message), **no consumer is
required yet** (Step 4 optional), a `message` row shows up in Postgres, and
the seller (the **listing owner**) can read it via
`GET /listings/{id}/messages`.

### Build it

1. **Create** the Liquibase change set
   `src/main/resources/db/changelog/v1.1-messages.yaml` — **one** additive
   `createTable` for `message` with a **unique** `message_id` (UUID) column,
   plus the include line in `db.changelog-master.yaml`. Do **not** touch
   `v1.0-init.yaml` (TP1 rule).

2. **Create** `com.app.listings.messaging.MessageEntity` — a JPA `@Entity
   @Table(name="message")` matching the `Conventions` (columns
   `messageId`, `listingId`, `fromUserId`, `toUserId`,
   `inReplyToMessageId`, `body`, `status` with enum
   `MessageStatus { PENDING, SENT, DELIVERED }`, `sentAt`).

3. **Create** `com.app.listings.messaging.MessageRepository` —
   `JpaRepository<MessageEntity, Long>` with one query:
   `Optional<MessageEntity> findByMessageId(String messageId)`.

4. **Create** `com.app.listings.messaging.MessageService` (interface +
   impl) with one method `send(...)` that:
   - takes `(listingId, callerId, inReplyToOrNull, body)`
   - **resolves** `toUserId`: if `inReplyTo` is set, take the **`from_user_id`**
     of that message; otherwise take the **listing's owner**
   - **validates**: `fromUserId != toUserId` (else 422), `body` non-blank
     (else 422), `inReplyTo` exists and belongs to the same listing (else 422),
     listing exists (else TP1 `ResourceNotFoundException`)
   - **generates** a `messageId` via `UUID.randomUUID().toString()`
   - **persists** the row (`status=SENT`, `sentAt=now`)
   - **publishes** with `rabbitTemplate.convertAndSend(RK, jsonBody)` — the
     routing key is the **literal** string `listings.messages` (do **not**
     pass the exchange, `setDefaultExchange` is already configured in Step 2)

5. **Create** `com.app.listings.messaging.MessageController` — a `@PostMapping`
   at `/listings/{id}/messages`, a `@GetMapping` at `/listings/{id}/messages`.
   The POST uses TP1's Basic auth, calls `MessageService.send(...)`. The GET
   reads via the repository and filters to the caller's turns (see *Contract*).

6. **Restart** the app and **verify** with the `curl`s below.

### Expected behavior

**The one rule** — `fromUserId` is **always the caller** (the HTTP-Basic
identity, **never** the body). `toUserId` is **always explicit**: the
`from_user_id` of the `inReplyTo` message when **replying**, the **listing's
owner** when **opening**. A self-message (`fromUserId == toUserId`) is a
**422** — the **only** case where an *opening* message (no `inReplyTo`) is
rejected. A non-participant GET returns **`200 []`**, **not 403** — the thread
is not *leaked*; a stranger simply has no view into it.

**Expected behavior matrix** — the full table, all cases:

| Caller (Basic) | **POST — open** (no `inReplyTo`) | **POST — reply** (`inReplyTo` = a real msg on this listing) | **POST — `inReplyTo`** unknown / foreign | **POST — empty `body`** | **GET /listings/{id}/messages** |
|---|---|---|---|---|---|
| **Anonymous** | **401** | **401** | **401** | **401** | **401** |
| **BUYER** (non-owner of this listing) | **202 SENT** (to = the owner) | **202 SENT** (to = the person you reply to) | **422 `inReplyTo`** | **422 `body`** | **200** — only the turns they took part in |
| **SELLER** (owner of this listing) | **422** (an opening message to yourself → `from == to`) | **202 SENT** (replying to a buyer → to = that buyer) | **422 `inReplyTo`** | **422 `body`** | **200** — the whole thread |
| **SELLER** (owner of a *different* listing = a buyer here) | **202 SENT** (to = this listing's owner) | **202 SENT** | **422 `inReplyTo`** | **422 `body`** | **200** — **0 items** (not their listing) |

**The observable side-effects** (for each `202`):
- A **new row** in Postgres `message` **immediately** (`status=SENT`, `sentAt=now`, `fromUserId`/`toUserId` correct).
- **`ready` +1** on `listings.messages` in the management UI — the broker is **holding** it (no consumer in Step 3).
- The **`messageId`** in the `202` body **is** the `message_id` of the new row
  (a **UUID** generated by the **producer**, **not** by the broker).
- **`ready` is cumulative** — a 2nd buyer `POST` on the same listing does **not** replace the 1st; `ready = 2`, two rows in Postgres.
- **`GET` is idempotent** — the same call twice returns the same set of turns
  (the read does **not** change `status` or `ready`).

> **Note on the labels "buyer" / "seller"** — they are shortcuts. The
> **actual** check is `caller == listing.owner`: any TP1 account may **open**
> a question on a listing, and any account may **reply** to a message in its
> own thread. The only structural constraint is `from != to`. (In production,
> `toUserId` could be a **claim** — an `X-Actor` header, a `sub` claim, a
> Basic email — rather than derived. This TP keeps the derivation because it
> is the simplest contract that still fits in one row per turn.)

### Verify

The full matrix (§7) includes all the 401 / 202 / 422 / 404 / GET cases with
copy-paste `curl`s. The 5 items that must **always** pass on their own:

- [ ] `curl -i -u buyer@test:secret123 -X POST http://localhost:8080/listings/1/messages -H 'Content-Type: application/json' -d '{"body":"Is this still available?"}'` → **202**, `"messageId":"<UUID>","status":"SENT"`
- [ ] `docker compose exec listings-postgres psql -U listings -d listings -c "SELECT message_id, from_user_id, to_user_id, in_reply_to, status FROM message WHERE message_id='<?>'"` → **1 row**, `status=SENT`
- [ ] `GET /api/queues?name=listings.messages` → **`ready ≥ 1`**, **`consumers = 0`**
- [ ] `curl -i -u carol@test:secret123 http://localhost:8080/listings/1/messages` → **200 `[]`** (non-participant)
- [ ] `curl -i -u seller@test:secret123 http://localhost:8080/listings/1/messages` → **200**, full thread, ascending `sentAt`

### If you are stuck

- The **`messageId`** is **client-generated** — `UUID.randomUUID().toString()` in the producer, before `convertAndSend`. The **broker** does **not** assign it (that would break **idempotency** in Step 4).
- **The 202 happens BEFORE the consumer exists.** The broker holds it. This is a network write to RabbitMQ — there is **no** synchronous call to any other Java code.
- For the **wire body**, use a Jackson `ObjectMapper` to serialize a small DTO (the envelope in `Conventions`: `messageId`, `listingId`, `fromUserId`, `toUserId`, `inReplyToMessageId`, `body`, `sentAt`). The **`toUserId`** is written into the body so the Step 4 consumer does **not** need to re-lookup the listing.
- For the **thread read**, the simplest is a **single** JPQL query with a `WHERE (from_user_id = ?1) OR (to_user_id = ?1)` filter, sorted by `sent_at ASC`. Two repository calls + a union also work.
- Full `curl`s: [`docs/verification.md` §7](docs/verification.md).

---

## Step 4 — Consume, persist, and reply   CORE

**Outcome** — The app **registers a `@RabbitListener`** on `listings.messages`;
when the broker delivers a published message, the row in `message` is
**upserted** by `message_id` from `SENT` to `DELIVERED` (with
`delivered_at=now`); the seller's `GET` now shows **both sides** of the
thread. A **redelivered** message (duplicated `messageId`) is a **no-op**.

### Build it

1. **Create** a small **DTO** you name, e.g.
   `com.app.listings.messaging.mq.MqMessage` — fields matching the wire
   envelope in `Conventions` (`messageId`, `listingId`, `fromUserId`,
   `toUserId`, `inReplyToMessageId`, `body`, `sentAt`). This is the **parse
   target** for the listener; it is **not** the JPA `MessageEntity`.

2. **Add** the field **`deliveredAt`** (type `Instant`) to
   `com.app.listings.messaging.MessageEntity` and to the Liquibase
   `v1.1-messages.yaml` change set (`timestamp WITH TIME ZONE`, **nullable**).
   The Step 3 upsert sets it, the Step 4 upsert **reads** it.

3. **Create** `com.app.listings.messaging.MessageListener` — a `@Component`
   with **one** method:
   ```java
   @RabbitListener(queues = "listings.messages")
   public void onMessage(String body) { ... }
   ```
   The method does **3 things, in this order**:
   1. **Parse** the raw JSON `String` into `MqMessage` (with a Jackson
      `ObjectMapper`). **A parse error is a *poison* message** (Step 5) —
      do **not** `throw`; let it bubble (Spring AMQP will NACK it).
   2. **Upsert** the row: `findByMessageId(mmq.getMessageId())` → if present,
      set `status=DELIVERED`, `deliveredAt=now`, then `save`; if absent,
      build a **new** `MessageEntity` from the wire fields and `save`.
   3. **Catch** **only** known **business** errors (missing buyer, orphan
      `inReplyTo`, etc.) with `log.warn("skipping messageId={} reason={}",
      mmq.getMessageId(), ex.getMessage())` and **return** — the **ack is
      automatic**, so the message is **removed** from the queue.
   4. **Do NOT catch** **unexpected** errors (parse, NPE, SQL, …) — let them
      bubble; Spring AMQP's default handler `NACK`s them with `requeue=false`
      → they go to the DLQ (Step 5).

4. **Restart** the app and verify the listener is attached
   (`GET /api/queues?name=listings.messages` → `consumers = 1`).

### Expected behavior

- **`consumers = 1`** on `listings.messages` (with the app up).
- **After a buyer `POST` (Step 3)**, within ~2 s, `psql SELECT status FROM
  message WHERE message_id='<that-uuid>'` → `DELIVERED` (the consumer **did**
  process it).
- **The seller replies** to the buyer's message (`POST` with `inReplyTo =
  <buyer's messageId>`): the **buyer's** `GET` now returns **2 rows** (their
  question + the seller's reply), ascending `sentAt`. Each row's `status`
  reflects its own delivery state.
- **`messageId` duplication** (the same wire message delivered **twice**):
  `psql SELECT count(*) FROM message WHERE message_id='<dup>'` → **1**, **not**
  2. The **unique** `message_id` + **upsert** logic is doing its job.
- **Business cases** (a buyer that no longer exists, an orphan `inReplyTo`) —
  `docker compose logs listings-api` shows **`log.warn`**, **no stack
  trace** (the listener chose to skip and the ack is safe).
- **Poison messages** (a JSON body that does **not** parse as `MqMessage`)
  are **rejected** with `requeue=false` → the broker **reroutes** them to
  `listings.messages.dlq` (Step 5 proves this).

### Verify

- [ ] `curl -s -u listings:listings http://localhost:15673/api/queues?name=listings.messages` → `consumers = 1`
- [ ] Right after a buyer `POST /listings/1/messages`: `docker compose exec listings-postgres psql -U listings -d listings -c "SELECT status FROM message WHERE message_id='<that-uuid>'"` → **`DELIVERED`**
- [ ] **Seller replies** with `inReplyTo = <buyer's messageId>` → the **buyer's** `GET /listings/1/messages` → **200, 2 rows** (their question + the seller's reply)
- [ ] **Duplicate delivery**: from the **management UI**, **re-publish** the **exact same** JSON body to routing key `listings.messages` → `psql SELECT count(*) FROM message WHERE message_id='<that-uuid>'` → **1**
- [ ] `docker compose logs listings-api | tail -20` → no stack trace; for a "business" case, a `log.warn` line is present

### If you are stuck

- The **`@RabbitListener`** import is `org.springframework.amqp.rabbit.annotation.RabbitListener`. The **queue name must be the literal string** `listings.messages` (**not** a constant import) — keeping it greppable against `Conventions`.
- The **method parameter** is either a **raw `String`** (the JSON body directly) or Spring AMQP's **`org.springframework.messaging.Message`** (with `getBody()` returning a `String`). Both work. If you choose `String`, you need `com.fasterxml.jackson.databind.ObjectMapper` (already on the classpath via `spring-boot-starter-web`).
- **Idempotency** — upsert by `messageId` **from the wire body**, **not** by the Postgres-generated `id`. `findByMessageId` is **Step 3's** repository method; the pattern is **"SELECT + INSERT-or-UPDATE"** in the listener (Spring Data JPA `save()` + the prior `find`).
- The **`deliveredAt`** column must be **nullable** — the row exists in `SENT` state (without a timestamp) before the consumer touches it.
- **Do NOT `throw`** for **known** business cases (the ack would fire and the message would be deleted — that is the point of the warn-and-skip rule). **DO let unexpected errors bubble** — that is what Step 5's DLQ depends on.
- The reply round-trip walk: [`docs/verification.md` §8](docs/verification.md).


## Step 5 — Poison messages go to the dead-letter queue   CORE

**Outcome** — A single **poison** message (a wire JSON the consumer **cannot parse**)
**does not block** legitimate messages on the main queue, and ends up in the
**`listings.messages.dlq`** where the operator can **inspect it** in the
management UI. The **producer** is **not touched** — this is a **consumer-side**
contract.

### Build it

1. **Extend** the `onMessage(...)` signature in
   `com.app.listings.messaging.MessageListener` to take a
   `com.rabbitmq.client.Channel`:
   ```java
   @RabbitListener(queues = "listings.messages")
   public void onMessage(Message mqMessage, Channel channel) throws IOException {
       long tag = mqMessage.getMessageProperties().getDeliveryTag();
       try {
           // ... existing parse + upsert + business-skip logic from Step 4 ...
       } catch (Exception ex) {
           log.warn("nacking messageId={} reason={}", /* tag or parsed id */, ex.getMessage());
           channel.basicNack(tag, /* multiple = */ false, /* requeue = */ false);
           return; // do NOT re-throw — the NACK is the intent
       }
   }
   ```
   The **last** boolean is `requeue` (**false**) — that is what triggers the
   Step 2 `x-dead-letter-*` arguments. (`basicReject(tag, false)` is the same
   two-arg form.)

2. **Do NOT** `basicNack` (or `basicReject`) for **known** business cases —
   those stay "warn-and-ack" per Step 4. The poison rule is: **unexpected**
   or **unparseable** → `basicNack(..., false, false)`.

### Expected behavior

- **Before** the drill: `GET /api/queues?name=listings.messages.dlq` → `ready
  = 0`.
- **After** the drill (poison published, consumer ran): `listings.messages`
  `ready` is **decreased by 1** (the message **left** the main queue), and
  `listings.messages.dlq` `ready` is **+1** (the message **landed** in the
  DLQ).
- **After** the poison, a **new legitimate** `POST /listings/{id}/messages`
  still results in a `202`, a `DELIVERED` row in Postgres, and a `ready-1`
  decrement on the main queue — **one** bad message did **not** block the
  queue (because `basicNack(requeue=false)` prevents the infinite-loop).
- The app **does not crash** on a poison: `docker compose logs listings-api`
  shows the **expected** `log.warn` (or a swallowed exception trace), **not**
  a `500 stack trace`, **not** a `Restarting`.
- In the management UI, the DLQ message is **visible** — body, routing key
  `listings.messages.dlq`, and `expiration = 0` (no TTL).
- The **producer** (Step 3) is **unchanged**. The DLQ is **purely** on the
  consumer side.

### Verify

Walk it through in 3 commands (from [`docs/verification.md` §9](docs/verification.md)):

1. **Publish** a **poison** wire message via the management API:
   `POST /api/exchanges/listings.direct/publish` with `routing_key=
   listings.messages` and a body **that is not** a `MqMessage` (e.g.
   `{"oops": true}`).

2. **Read** the main queue — `GET /api/queues?name=listings.messages` →
   check `ready` (it should be **decreased** by 1, since the consumer should
   have nack'd).

3. **Read** the DLQ — `GET /api/queues?name=listings.messages.dlq` →
   `ready ≥ 1`, and **peek** the body with
   `GET /api/queues/listings.messages.dlq/get?count=5&requeue=true` — you
   should see the **poison** `{"oops":true}` in the body.

Plus:

- [ ] A **new legitimate** `POST /listings/1/messages` (Step 3 endpoint) still `202`s and `DELIVERED`s within 2 s (the queue is **not stuck**)
- [ ] `docker compose logs listings-api | tail -30` → the poison is **visible** in a log line (warn or exception), **not** a container restart
- [ ] `psql SELECT count(*) FROM message` — the **row count has not grown** for that `messageId` (the consumer **did not** insert a `DELIVERED` row for the poison)

### If you are stuck

- **The class & the method are the same** as Step 4 (`MessageListener.onMessage`). The **only** change is the **signature** (add a `Channel` parameter) and the **`catch` block** (NACK with `requeue=false`).
- The **delivery tag** is on `mqMessage.getMessageProperties().getDeliveryTag()`.
- **The `false` on the last boolean** of `basicNack` is what triggers the **Step 2** `x-dead-letter-*` arguments. If you set it `true`, the broker requeues the message **forever** (poison-pill anti-pattern).
- **Auto-backstop** (Step 2): even without the `basicNack`, `x-delivery-limit: 3` will **reroute** any message that has been **requeued** 3×. The consumer-side `basicNack` is your **explicit** "move to DLQ now" signal.
- **Do not** call **`basicAck`** after **`basicNack`** — the NACK is the final state for that delivery tag.

---

## Step 6 — Broker down: the API survives and returns 503   CORE

**Outcome** — When the **broker is down**, a `POST /listings/{id}/messages`
**fails fast** with a **clean 503** (TP1 JSON shape, no stack trace), the **REST
read** endpoints **keep working** (TP1's listing CRUD is **decoupled** from the
broker), and the app **does not restart**. When the broker **comes back**, the
`202` works again and **backlogged** messages in the **durable** queue
**drain** and **flip to `DELIVERED`**.

> **Superseded by Step 8 (bonus)** — Step 8 replaces the **503** with a
> **`202 PENDING`** + a **poller**. The `@ExceptionHandler` is **kept** as a
> **safety net** for stray publishes outside `MessageService`. Do not assume
> the 503 remains the contract after Step 8.

### Build it

1. **Add** a runtime exception
   `com.app.listings.messaging.exception.BrokerUnavailableException`
   (extends `RuntimeException`, **not** `AmqpException`). A useful message
   like `"rabbitmq not reachable — check docker compose / RABBITMQ_HOST /
   RABBITMQ_PORT"`.

2. **In `MessageService.send(...)`**, **wrap** the
   `rabbitTemplate.convertAndSend(...)` call in either of:
   ```java
   try {
       rabbitTemplate.convertAndSend("listings.messages", json);
   } catch (AmqpConnectException | AmqpTimeoutException ex) {
       throw new BrokerUnavailableException(EXCEPTION_MSG, ex);
   }
   ```
   Or, if you prefer, **catch any** `AmqpException` and re-throw as
   `BrokerUnavailableException`. The **controller** should **never** see an
   `AmqpException`; it should see **either** a `202` **or** a
   `BrokerUnavailableException`.

3. **In TP1's existing `GlobalExceptionHandler`**, **add** one handler:
   ```java
   @ExceptionHandler(BrokerUnavailableException.class)
   public ResponseEntity<ProblemDetail> handleBrokerDown(BrokerUnavailableException ex) {
       return ResponseEntity.status(503)
           .body(ProblemDetail.forStatusAndDetail(503, ex.getMessage()));
   }
   ```
   (or the matching TP1 idiom — **4 keys**: `timestamp`, `status`, `error`,
   `message`; **no** `fields` needed).

4. **Run the drill** (the 6 Proof checks below) to prove it.

### Expected behavior

The 6.1 → 6.7 sequence is the **state machine** of the step. **Every** row
must hold:

| # | Scenario | Expected |
|---|---|---|
| 6.1 | With the broker **up**, `POST /listings/{id}/messages` | **202** with `status=SENT` (the Step 3 contract is **unchanged** in the happy path) |
| 6.2 | `docker compose stop rabbitmq` (broker container **stopped**; **API stays up**), then `POST /listings/{id}/messages` | **`503`**, body in the **TP1 JSON shape** (`timestamp`, `status`, `error`, `message`), the `message` field **mentions** the broker (e.g. `"rabbitmq not reachable — check docker compose / RABBITMQ_HOST / RABBITMQ_PORT"`); **not** 500, **not** a stack trace |
| 6.3 | **Any other** TP1 endpoint during the outage (e.g. `GET /listings`, `GET /listings/{id}`, the TP1 register/CRUD) | **200** — the **HTTP read** path is **not** affected by the broker outage (TP1's business endpoints are **decoupled** from the broker) |
| 6.4 | `GET /actuator/health` during the outage | **200** (the app is **alive**; it is the **broker** that is down, not the app — the **`rabbit`** actuator indicator if you configure one) |
| 6.5 | `docker compose start rabbitmq` → wait for `rabbitmq-diagnostics ping` | `POST /listings/{id}/messages` → **202** again (the producer **reuses its connection pool**; no code change is needed) |
| 6.6 | **Backlog check** — messages in `listings.messages` that were **READY before the stop** **are still there** after the start (the **durable** queue + Mnesia in `rabbit-data` is doing its job). Once the consumer reconnects it **drains** them → the rows flip from `SENT` → `DELIVERED` in Postgres | the **count of `DELIVERED`** rows equals the **count of published** messages from before the stop (plus any new ones) |
| 6.7 | **Speed** of the 503 response | **≤ ~2 s**. An `AmqpConnectException` propagating out of the controller as a **`500`** is a bug — it **must** be caught in `MessageService` and re-thrown as `BrokerUnavailableException` → mapped to **503** by the `@ExceptionHandler` (that is the very point of this step) |

**Why these 7 rows are the contract (not just a checklist):**

- 6.2 and 6.7 are the **failure contract** — together they say "the 503 is
  **fast** and **clean** (TP1 shape, no leak of the AMQP exception)".
- 6.3 and 6.4 are the **decoupling contract** — the broker is a **side effect**
  on the **write** path, **not** a dependency on the **read** path.
- 6.5 and 6.6 are the **recovery contract** — the broker being **down** does
  **not** mean messages are **lost**, and does **not** mean the **API** must be
  recycled.
- The combination is the **entire** value of this step: **degraded, not dead**.

### Verify

- [ ] `docker compose stop rabbitmq` → `docker compose ps` shows `rabbitmq` **exited / Stopped** (not unhealthy / restarting)
- [ ] `curl -i -u buyer@test:secret123 -X POST http://localhost:8080/listings/1/messages -d '{"body":"still there?"}'` → **503**, body has `"status":503` and a `message` mentioning `rabbitmq` / `broker` / `unavailable`; **not** `500`, **no** stack trace
- [ ] `curl -fsS http://localhost:8080/actuator/health` → **200** (the API is **alive**)
- [ ] `curl -fsS http://localhost:8080/listings` → **200** (TP1's read path is **not** affected)
- [ ] `docker compose start rabbitmq && sleep 5` → `docker compose ps` shows `rabbitmq` **running** (healthy)
- [ ] `curl -i -u buyer@test:secret123 -X POST http://localhost:8080/listings/1/messages -d '{"body":"back?"}'` → **202** with a **fresh** `"messageId":"<UUID>"`
- [ ] **Drain check**: the rows that were `SENT` (before the stop) are now `DELIVERED`; `docker compose logs listings-api | tail -10` shows a `log.info` from the listener; `GET /api/queues?name=listings.messages` → `ready` dropped to **0**

### If you are stuck

- The **exception to throw** is the **new** `BrokerUnavailableException`, **runtime** (not `AmqpException`). The `@ExceptionHandler` returns **503**, **not** 500 — and the **JSON shape** is **exactly** TP1's existing handler shape.
- The 503 is a **real** contract, not a defensive one. In production, "broker down" is a **degradable** failure (queue and retry). This step is where your API **proves** it knows the **difference** from "the app crashed".
- **`messageId` is generated on the producer side** (Step 3). During the outage, **nothing** is published, so there is **no** `messageId` to replay. Each new `202` (after restart) uses a **fresh** UUID.
- The **drain step** relies on Step 2's **durable** queue + Step 4's **`@RabbitListener` reconnect** (Spring AMQP does this **for** you via a `CachingConnectionFactory`).
- Full stop→503→start→202 walk: [`docs/verification.md` §10](docs/verification.md).

---


---

## Step 7 — At-least-once delivery and idempotency   BONUS

**Outcome** — Prove the **at-least-once** guarantee **in the database**: a
**redelivered** message (the broker crashed between `deliver` and `ack`, or
the consumer crashed **before** ack'ing) does **not** create a **second** row
in `message`. The **unique index + Step 4's upsert** is what makes this work.

### Pre-req

- Steps 1–5 are **green** (the consumer is attached, the DLQ works, the
  producer is alive).
- The **broker is up** in this drill — only the **API** is going down.

### Build it

1. **Pre-condition**: `curl -s -u listings:listings http://localhost:15673/api/queues?name=listings.messages` → `consumers ≥ 1` and `ready ≥ 1`. There is at least one **in-flight** message.

2. **Crash drill** (do this **3 times** with **different** `messageId`s):
   - **Pick** the most recent buyer's `message_id` — call it `M`.
   - **Stop the API immediately** (within ~1 s of the `ready` going up; you
     do **not** need to catch it mid-processing — the durable queue **holds**
     it either way):
     `docker compose stop listings-api`
   - **Observe**: `docker compose exec rabbitmq rabbitmqctl list_queues name
     messages_ready messages_unacknowledged` → the **ready** for
     `listings.messages` is **≥ 1** (the message **did not get lost**).
   - **Restart** the API: `docker compose up -d listings-api`
   - **Within ~5 s** (Spring AMQP reconnects and the listener re-delivers the
     message): the **row for `M`** in Postgres is now `status = DELIVERED`,
     `deliveries (in the UI) ≥ 2`.
   - **Verify no duplication**: `docker compose exec listings-postgres psql -U
     listings -d listings -c "SELECT count(*) FROM message WHERE message_id =
     '<M>'"` → **1**, **not** 2.

3. **Repeat** steps 2's 4 sub-actions **2 more times** with new buyer `POST`s,
   so that 3 different `messageId`s are each **delivered ≥ 2 times** and the
   `message` table has **exactly 3 rows** for them.

### Expected behavior

- For each of the **3 crashed** `messageId`s:
  - **Postgres count = 1** (no duplication), **`status = DELIVERED`**.
  - **`deliveries` in the management UI ≥ 2** (the at-least-once proof).
- **The producer (Step 3) still works during the API outage.** A buyer `POST`
  during the down → the broker (which is **up**) queues it; on restart, the
  consumer picks it up (so you do **not** get a 503 — only the consumer is
  down, not the broker).
- **TP1 endpoints that do not touch the broker** (e.g. `POST /auth/register`)
  **also** work during the API outage (well, they **don't**, because the
  container is **down** — the point is the **`502 / refused`** from the
  container is **not** the **503** from a broker failure. See the note below.)

> **Note on "producer side during the crash"** — in Step 7, the **API is
> stopped**, not the broker. So `POST /listings/{id}/messages` during the
> outage fails at the **load-balancer / Docker network** layer (502 /
> connection refused) **before** it reaches your 503 handler. That is a
> *different* failure mode from Step 6 (broker down, API up). If you want to
> also prove "the producer survives the consumer down", keep the API up and
> stop the **broker** — **that** is Step 6's drill, not Step 7's.

### Verify

- [ ] For **3** distinct in-flight `messageId`s (`M₁, M₂, M₃`), after the 3 crash-recoveries: `SELECT count(*) FROM message WHERE message_id IN ('M₁','M₂','M₃')` → **3**, **not** 6 (no duplication)
- [ ] For the **same 3**, `GET /api/queues?name=listings.messages` in the management UI shows **`deliveries ≥ 2`** for each (the "at-least-once" evidence)
- [ ] `docker compose exec listings-postgres psql -U listings -d listings -c "SELECT status FROM message WHERE message_id IN ('M₁','M₂','M₃')"` → all **3** rows are **`DELIVERED`**
- [ ] `docker compose logs listings-api | grep -i "delivered\|duplicate\|skipping" | wc -l` → ≥ 3 lines (the successful upserts / the no-op re-upsers)
- [ ] **No** `DuplicateKeyException` anywhere in the logs (the **unique index** + **upsert** did its job)

### If you are stuck

- **`deliveries` ≥ 2** is the **at-least-once** proof. A value of exactly **1** means the consumer ack'd on the **first** delivery — that is "at-least-once" but **not** a **visible** redelivery.
- **`count = 1, deliveries ≥ 2`** is the **whole point** of this step — it is the **combination** of Step 2's **`UNIQUE(message_id)`** index and Step 4's **upsert** that makes "redelivery without duplication" possible.
- **`rabbitmqctl list_queues`** is your **CLI** view of the management UI — `docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged` — gives you the same data without a browser.
- **`docker compose stop`** (not `down`) keeps the **named volume** `rabbit-data`, so the **durable queue** and its contents **survive**. **Do NOT** use `docker compose down -v` mid-drill — that would **wipe** the queue and the evidence.

---

## Step 8 — Transactional outbox: 202 even when the broker is down   BONUS

**Outcome** — **The buyer always gets 202**, **regardless of the broker state**:
`SENT` if the broker is up (the **fast path**, the outbox is **invisible**),
`PENDING` if the broker is down (the **slow path**, a **`@Scheduled` poller**
re-publishes from the DB within ~5 s). The **message is never lost** — it
lives in your **own** Postgres `message` table **until** it lands in the
broker.

> **Overrides Step 6 on the publish path** — the **503** contract is
> **replaced** by **`202 PENDING`** (a **stronger** contract, from the
> buyer's POV: "your message is queued" beats "broker unavailable"). The **Step 5**
> (DLQ) contract is **not** overridden. The **Step 6**
> `@ExceptionHandler(BrokerUnavailableException)` is **kept** as a **safety
> net** for any stray `convertAndSend` **outside** `MessageService`.

### Build it

1. **Verify** `v1.1-messages.yaml` declared `status` as `VARCHAR(16)` (**not**
   a PG `enum`). If it is a PG enum, **change it** and **re-run** Liquibase
   (drop + re-create) — you cannot add `PENDING` to a PG enum without a
   separate migration (out of scope for this TP).

2. **Create** `com.app.listings.messaging.MessageOutbox` — a `@Component`
   with a single **scheduled** method:
   ```java
   @Component
   public class MessageOutbox {
       @Scheduled(fixedDelayString = "${listings.messaging.outbox.interval-ms:5000}")
       public void drain() {
           // 1. select PENDING rows, oldest first
           List<MessageEntity> rows =
               repo.findByStatusOrderBySentAtAsc(MessageStatus.PENDING);
           // 2. for each row: CLAIM first, then publish
           for (MessageEntity row : rows) {
               int affected = repo.claim(row.getMessageId());  // atomic UPDATE
               if (affected == 0) {
                   continue;  // another instance won the race — skip
               }
               try {
                   rabbitTemplate.convertAndSend("listings.messages", /* wire JSON */ );
                   // (the row is already SENT; the poller loop will log or not)
               } catch (AmqpException ex) {
                   repo.revert(row.getMessageId());  // back to PENDING
                   log.warn("outbox publish failed for messageId={}", row.getMessageId(), ex);
               }
           }
       }
   }
   ```

3. **Add** the atomic claim / revert **queries** to
   `com.app.listings.messaging.MessageRepository`:
   ```java
   @Repository
   public interface MessageRepository extends JpaRepository<MessageEntity, Long> {
       Optional<MessageEntity> findByMessageId(String messageId);

       List<MessageEntity> findByStatusOrderBySentAtAsc(MessageStatus status);

       @Modifying
       @Query("""
           UPDATE MessageEntity m
              SET m.status = MessageStatus.SENT
            WHERE m.messageId = :id
              AND m.status = MessageStatus.PENDING
           """)
       int claim(@Param("id") String messageId);

       @Modifying
       @Query("""
           UPDATE MessageEntity m
              SET m.status = MessageStatus.PENDING
            WHERE m.messageId = :id
           """)
       int revert(@Param("id") String messageId);
   }
   ```
   The **`WHERE … status = PENDING`** is what makes `claim()` **atomic**:
   under **two instances** racing on the **same** row, **only one** gets
   `affected = 1` (the rest get **0** and must **not** publish). If you
   write it as **`WHERE messageId = ?`**, both instances update (and both
   publish) — **double-publish race** — which is **the exact failure** 8.6
   guards against.

4. **In `MessageService.send(...)`**, **replace** the Step 6 `try / catch`
   with a **`try / catch that returns a PENDING result`** instead of
   rethrowing. Concretely: **`insert the row as PENDING`, `attempt
   convertAndSend`, `on success update to SENT`, `on BrokerUnavailableException
   leave at PENDING`** — and **return** a DTO with **`status=PENDING`** for the
   broker-down case, **`status=SENT`** for the broker-up case.
   ```
   row = insert(status=PENDING)
   try {
       convertAndSend(RK, row)
       update row → SENT
       return 202 with status=SENT
   } catch (BrokerUnavailableException) {
       // the row is ALREADY PENDING — do not re-throw
       return 202 with status=PENDING
   }
   ```

5. **Enable scheduling** — in your **main** `@SpringBootApplication` (or in a
   separate `@Configuration`), put **`@EnableScheduling`** on it. Without this
   annotation, `@Scheduled` is **silently ignored**.

6. **Add the property** `listings.messaging.outbox.interval-ms` to
   `application.yml` (with an env var, e.g.
   `${LISTINGS_MESSAGING_OUTBOX_INTERVAL_MS:5000}`) so the **polling
   interval** is tunable at deploy time.

### Expected behavior

The **8.1 → 8.6** sequence is the **state machine** of the step. **Every**
row must hold:

| # | Scenario | Expected |
|---|---|---|
| 8.1 | **Broker down** → `POST /listings/{id}/messages` | **`202`** with `{ messageId, "status":"PENDING" }` (a **fresh UUID**, the row is **in the DB**, **not** in the broker); the **shape is the same** as 8.2 — the **`status`** field is the **only** difference |
| 8.2 | **Broker up** → same `POST` | **`202`** with `{ messageId, "status":"SENT" }` (row inserted **and** published in the **same** call); the **outbox is invisible** in the happy path — **that is the whole point** (the 202 looks the same, only the `status` differs) |
| 8.3 | **The poller**: a `@Scheduled` method fires **every `interval-ms`** (default **5000** ms); for **each** `PENDING` row, **claims** it atomically (`UPDATE … WHERE status = 'PENDING'` — **before** the `convertAndSend`), **then** publishes to `listings.messages` | the **order is claim → publish**, not publish → claim. If you publish first, **two instances** on the **same** `PENDING` row both publish (the **race** is lost in the **air**, not in the DB) |
| 8.4 | **Recovery**: broker down → **3** calls → **3** `PENDING` rows. `docker compose start rabbitmq` → within ~5 s, **3** rows become **`SENT`** (the broker's `ready` is **+3**), then **`DELIVERED`** (the Step 4 consumer drains them — **unchanged**); **no re-publish** of an already-SENT row (the `WHERE status = 'PENDING'` is the **guard**) | the **count of `PENDING` rows** is exactly the **number of failures** (the **poller never claims SENT**) |
| 8.5 | **Idempotency** (Step 4 still applies): a redelivered row (the broker dropped the ack — classic) — the **consumer** (Step 4) **upserts** by `message_id` → **no duplicate row** | **`SELECT count(*) FROM message WHERE message_id = '<id>'` = 1**, regardless of how many times it was redelivered |
| 8.6 | **Multi-instance** (run **two** `listings-api` instances simultaneously, both with the **same** DB → both will have their **own** **pollers** on the **same** `PENDING` rows): exactly **one** of them publishes per row | **`count(*) = 1`** per `message_id` (no double-publish). The **`UPDATE … WHERE status = 'PENDING'`** is the **atomic claim** — the **loser** gets **0 rows affected** → skips the publish. This is **the entire reason** the **UPDATE is BEFORE the `convertAndSend`** |

> **Note on 8.1 vs Step 6** — under **Step 6** the broker-down contract is
> "return **503**" (a **failure signal** to the caller). Under **Step 8** it
> is "return **`202 PENDING`**" (a **success signal**, with a **queue** state
> the buyer can poll). Both are **legit** contracts — Step 8 is **stronger**
> because the **buyer cannot distinguish** a broker outage from a slow
> consumer (the **UX** is identical).

### Verify

The full 4-phase drill for **§12** — **broker down → 202 PENDING → broker up
→ poller → SENT → DELIVERED** (with copy-paste `psql` snapshots at each
phase). The **5 rows** that must **always** pass on their own:

- [ ] **Broker down**: `curl -i -u buyer@test:secret123 -X POST http://localhost:8080/listings/1/messages -d '{"body":"still there?"}'` → **202** with `"status":"PENDING"` (a **fresh UUID**)
- [ ] **Same state**: `docker compose exec listings-postgres psql -U listings -d listings -c "SELECT message_id, status FROM message WHERE message_id='<that-uuid>'"` → **1 row**, `status=PENDING` — **not** in the broker (verify in the **UI**: `GET /api/queues?name=listings.messages` has **not** moved)
- [ ] **`docker compose start rabbitmq`**: `sleep 5` (the poller's `fixedDelay`) then `psql SELECT status FROM message WHERE status='PENDING'` → **`[]`** (the poller has **claimed and published** all of them)
- [ ] **`GET /api/queues?name=listings.messages`** → `ready` **≥ N** for N = the number of PENDING rows, then drops to **0** as the **Step 4 consumer** (unchanged) drains them
- [ ] **`psql SELECT message_id, status, sent_at, delivered_at FROM message ORDER BY sent_at DESC LIMIT 5`** → the rows from the **PENDING phase** are now `DELIVERED` (the **full chain PENDING → SENT → DELIVERED** is **visible** in the **same** table)
- [ ] **No 503** anywhere in `docker compose logs listings-api | grep -i 503` **during** the drill (the Step 6 `@ExceptionHandler` is **kept** — but the **`MessageService`** catches internally and **returns PENDING** instead of rethrowing)

### If you are stuck

- The **`@Scheduled`** import is **`org.springframework.scheduling.annotation.Scheduled`**. The **`@EnableScheduling`** **annotation** is a **`@Configuration`** (put it on the **main** app class, or on a **dedicated** `@Configuration`). Without it, `@Scheduled` is **silently ignored** — your **poller will not run** and you will get **`PENDING` rows that never leave the DB**.
- The **`UPDATE … WHERE status = 'PENDING'`** is an **atomic claim**: **`int affected = repo.claim(id);`** — if **`affected == 0`**, **another instance** won the **race**; **do not** publish. **If `affected == 1`**, do **`convertAndSend`**. The **order matters** (claim **first**, publish **second**) — publish **first** = **double-publish race** (the **entire point** of 8.6).
- The **`WHERE`** clause is **the guard** (the **PK** `message_id` is the **lookup**; the **`status`** is the **gate**). Both are correct, but **the status check is non-redundant** — it is the **only** way to claim **without** a lock.
- If your **`v1.1-messages.yaml`** declared `status` as a **PG enum**, you **cannot** add `PENDING` without a **separate migration** — **change it back to a `VARCHAR(16)`** and **re-run** Liquibase (drop + recreate the table). The **schema choice** was **`VARCHAR`** for exactly this reason.
- The **`@Scheduled`** method runs **on** your app's **main** thread pool — **do not** put a `Thread.sleep(...)` in it; the **`fixedDelay`** (default 5 s) is the **cadence**.

---


## Step 9 — Final Integration   DELIVERY

**Outcome** — The **whole stack** (TP1 base + RabbitMQ + the `com.app.listings
.messaging` package) **boots cleanly from a clean checkout**: both repos
`git clone`'d, the broker `docker compose up`'d, the app built with
`mvn clean package`, the app `docker compose up`'d. The **decision log** is
**committed** in the TP1 README. The **evaluator can** `curl` a `202 SENT`,
`psql` a `DELIVERED` row, and **trust** the output.

### Build it

1. **From a fresh clone**, **`git pull`** (or check out your last commit) of
   **both** repos (the TP1 repo **and** this `messaging/` repo).

2. **Start** the **broker first**, then **build** the app (TP1 repo) with
   `docker compose build listings-api` (or a local `mvn clean package` if you
   prefer), **then** `docker compose up -d` in the TP1 repo (the **same**
   compose file that has the Step 1 `RABBITMQ_HOST` + `RABBITMQ_PORT` edits).

3. **Run the smoke checks** — the **Proof** checklist below — and **capture**
   the outputs in your **decision log** (so the evaluator can re-run them).

4. **Add a decision log** to the **TP1 repo's `README.md`** (5–6 short
   paragraphs, one per **major** decision: **why** the topology is in code
   (Step 2), **why** `messageId` is client-generated (Steps 3/4/7–8), **why**
   the DLQ is a **separate** exchange (Step 5), **why** the **503 contract**
   (Step 6) was **chosen** (or **replaced** by the outbox — Step 8), **why**
   the **`UPDATE … WHERE status=PENDING`** is **before** the `convertAndSend`
   (Step 8), **why** the **`message`** table has its **own** `delivered_at`
   (Step 4) instead of re-using `listing.updated_at` (TP1). The **decision
   log is the oral-prep** — it **is** the evaluator's "why" answer bank.

5. **Push** both repos to their remotes (the TP1 repo and this `messaging/`
   repo). **Do NOT** commit: broker credentials (beyond the **demo**
   `listings`), a `.env` file, a **raw log line** with a buyer's message body,
   **or** a hard-coded **real** password.

### Final checks

- [ ] `cd "$TP1_DIR" && docker compose up -d --build` → `docker compose ps` shows **`listings-api`** + **`listings-postgres`** both healthy
- [ ] `cd "$MSG_DIR" && docker compose up -d` → `docker compose ps` shows **`rabbitmq`** healthy
- [ ] `curl -fsS http://localhost:8080/actuator/health` → **200** (the **app** is up and connected to **both** Postgres and RabbitMQ)
- [ ] **One buyer `POST`** → **202** with `"status":"SENT"`
- [ ] **One consumer drain** → `psql SELECT status FROM message WHERE message_id='<that-uuid>'` → **`DELIVERED`**; the **seller's** `GET /listings/{id}/messages` returns the **2 turns** (their own opening + the seller's reply)
- [ ] **`mvn clean verify`** in the TP1 repo → **BUILD SUCCESS** (the TP1 smoke test + **your** messaging smoke test **both pass**)
- [ ] **The decision log is in the TP1 repo's `README.md`** (5–6 short paragraphs of *why*)
- [ ] **No secrets** in either repo: the broker credential is **in** the `messaging/docker-compose.yml` **and** the TP1's `application.yml` reads `RABBITMQ_PASSWORD: ${RABBITMQ_PASSWORD:listings}` (**demo** value `listings`, **overridable** by env var); **no** real password in either **compose** or **application.yml**
- [ ] **`git status`** in **both** repos → **clean** (no uncommitted changes, no tracked secret files)
- [ ] **`git log --oneline -10`** in **both** repos → the commits are **intentional** (message is **specific**, e.g. "add messaging module: producer + consumer + DLX/DLQ" — not "stuff")

### If you are stuck

- **The team README** is the **one place** where the **binôme** writes
  **what** they decided and **why** — it is **not** a **copy** of the
  **exercise statement** (that is in **this** file). It is **5–6** short
  paragraphs, one per **major** decision.
- **The smoke test to add to the TP1 repo** should be the **lowest** bar:
  the **Spring context boots**, the **`RabbitAdmin`** bean is **present**,
  the **`@RabbitListener`** on `listings.messages` is **registered**. That
  is **it**. The test **must not** require a **live broker** (it is a
  **boot test**) — use `@SpringBootTest` with a mock `RabbitTemplate` or,
  more simply, an **assert** that the listener bean is in the application
  context.
- **The broker** (from the TP1 repo's perspective) is just **another
  container** on the shared `middleware` network — the **only** thing you
  change in the TP1 compose is the **env vars** (`RABBITMQ_HOST` +
  `RABBITMQ_PORT`), **never** the **network** or the **image**.
- The **decision log** is the **oral** — the 10 min the evaluator asks
  "**why**?" is covered by **your** 5–6 paragraphs, **not** by `exercise.md`.
  If you can **say the same thing** as your README out loud, you are
  **prepared**. If you find **yourself** writing **"I don't know why we
  did it that way"** — **write the answer in the README**. **Then** re-try.

---

## Deliverables

| Item | Detail |
|---|---|
| **The TP1 repo** (the Spring Boot app) | `+1 dep` (`spring-boot-starter-amqp`, **no** version — Appendix A); `+` the `spring.rabbitmq` block in `application.yml`; `+` the **base compose** `RABBITMQ_HOST` + `RABBITMQ_PORT` env; `+` the `com.app.listings.messaging` package (`RabbitConfig`, `MessageEntity`, `MessageRepository`, `MessageService`, `MessageController`, `MessageListener`, **Step 6**: `BrokerUnavailableException` + a **`@ExceptionHandler`** in TP1's `GlobalExceptionHandler`, **Step 7–8** optional: `MessageOutbox` + the `@EnableScheduling` config + `listings.messaging.outbox.interval-ms` key); Liquibase `v1.1-messages.yaml` + include in the changelog master; `README.md` updated with the **decision log** |
| **This repo** (`messaging/`) | `exercise.md` (this file), `README.md` (operational), `docker-compose.yml` (the RabbitMQ broker), `docs/verification.md` (the copy-paste `curl`s + `psql`). **No Java in this repo** — the **TP1 repo** is the only place code is committed (single owner, one repo per artifact) |
| **`mvn clean verify`** in the TP1 repo | **Green** |
| **Oral** | 10 min + 5 min Q&A |

> **Do not commit** into the pair's TP1 repo (or this one):
> the **broker credential** (if you change it from `listings` — the
> *default* `listings` is fine to **commit** because it is the **demo**
> value); any **`.env`** file; any **raw log line** with a buyer's message
> body; any **`docker-compose`** override that **hard-codes** a **real**
> password (use `RABBITMQ_PASSWORD=${RABBITMQ_PASSWORD:-listings}` at most
> for demo purposes).

---

## Appendix A — the one dependency + the three config keys

The **code to paste is not in this file** — by design, it lives in the
**operational** [`README.md` → "0) Required base edit"](README.md), exactly where the other TPs put their base edits. This appendix is
the **reference** for *what* the additions are and *why*.

| Addition | Coordinates / key | Why |
|---|---|---|
| Spring AMQP starter | `org.springframework.boot:spring-boot-starter-amqp` — **no version** (Boot 4.1.1 BOM pulls spring-amqp **4.1.1** + rabbitmq-client **5.30.0**) | provides **`RabbitTemplate`** (producer), **`@RabbitListener`** (consumer), **`RabbitAdmin`** (auto-declares queues/exchanges/bindings **idempotently**) |
| `application.yml` | `spring.rabbitmq.host` / `port` / `username` / `password` — **all** env-overridable (`${RABBITMQ_HOST:localhost}`, etc.) | the **host** default is for `mvn spring-boot:run`; the compose edit (next row) **overrides** `port` (5672) and **`host`** (the **network name** `rabbitmq`) at runtime |
| the TP1 repo's `docker-compose.yml` | `listings-api.environment`: `RABBITMQ_HOST: rabbitmq` + `RABBITMQ_PORT: "5672"` | from **inside** the API container, `localhost` is the **container itself**; the app must reach the broker via its **network name** on the **internal** port |

> **One owner per fact:** the exact `pom.xml` / `application.yml` /
> `docker-compose.yml` to paste lives in **`README.md`**; this appendix
> explains **which** artifact, **which** key, and **why**. Unlike the **IA**
> TP (no new container), this TP **adds one** (RabbitMQ) — the base compose
> **must** change.

---

## Appendix B — RabbitMQ management API endpoints (used in `docs/verification.md`)

All with **Basic auth** `listings:listings` (the **`guest`** account is **not
available** — see the compose file).

| Endpoint | Purpose |
|---|---|
| `GET /api/overview` | broker version / uptime — the "is the broker up?" health endpoint (Step 1) |
| `GET /api/connections` | one row per **app** that has connected (Step 1) |
| `GET /api/queues` | all queues, with `messages_ready`, `consumers`, `arguments` (Steps 2, 3, 5, 7) |
| `GET /api/exchanges` | all exchanges, with `type` (direct / fanout / topic) (Steps 2, 5) |
| `GET /api/exchanges/{name}/bindings/queue` | the **bindings** from an exchange to queues, with the routing key (Step 2) |
| `GET /api/queues/{name}/get` | **peek** up to N messages from a queue (**read-only**, does **not** consume) — used to read the **DLQ's** body (Step 5) |
| `POST /api/exchanges/{name}/publish` | **publish** a message to an **exchange** with a **routing key** — the **poison-drill** tool (Step 5) |
| `DELETE /api/queues/{name}` | **delete** a queue (**not** **purge**) — used **only** to clean the DLQ **after** you have read it |

> **`GET /api/queues/{name}/get`** accepts a **query** `count=5` to return up
> to **5** messages **without** consuming them (the **`requeue=true`** option
> is the **default** and is the **only** one you should use here).

---

## Appendix C — what is **NOT** committed

- **The broker credential**, if you **change** it from the demo `listings`
  (it **lives** in `docker-compose.yml` via `RABBITMQ_DEFAULT_*` — that is the
  right place, and it is a **demo** value; **not** in `application.yml`).
- **Buyer / seller `message.body`** — even in a **log line** — the **TP1
  privacy** rule applies.
- **A second `application.yml`** in the `messaging/` repo — the **app** reads
  its configuration from the **TP1 repo** (one owner per fact, §6.9 editorial).
- **No Java in this repo** — `messaging/` ships the **statement**, the
  **broker compose**, and the **verification `curl`s**; the **java** lives
  in the **TP1 repo** (the Spring Boot app, the other repo).

---

## Appendix D — Useful links (versions pinned)

- **Spring AMQP** (for Boot 4.1 / spring-amqp 4.1.1):
  <https://docs.spring.io/spring-amqp/reference/html/>
  (sections *"Message Driven Annotation Method"* and *"RabbitMQ support"*).
- **RabbitMQ 4.x** (the broker + the management plugin):
  <https://www.rabbitmq.com/docs/getstarted> ·
  <https://www.rabbitmq.com/management.html> ·
  <https://www.rabbitmq.com/dlx.html> (the **dead-letter** topic — the
  **direct** rationale for Step 2's `x-dead-letter-*`).
- **`rabbitmqctl`** CLI (used in Step 7): <https://www.rabbitmq.com/ctl.html>

> **TP rule (final repeat):** no `TODO`, no stub, no secret in code, no
> **silent** swallow of a message (the **DLQ** is the **right** place), and
> the **broker is a runtime, not a source of truth** — the **topology** is
> code in `com.app.listings.messaging.RabbitConfig`, the **messages** are
> **data**.

