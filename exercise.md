# TP Messaging — asynchronous seller↔buyer messaging (RabbitMQ) (8h)

## Overview

This is a **follow-up** to the Spring Boot TP (TP1, `../springboot/`). We reuse
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

There **is** a new Docker service and two extra host ports this time (unlike the
IA TP): the **RabbitMQ** broker — AMQP on host **5673**, management
UI/API on host **15673**. The API stays on **8080**, unchanged.

**Validation method (the contract):** you prove each step with **`curl`** (the
REST endpoint and the RabbitMQ **management API**), a read of the **Postgres**
`message` table, the **management UI** (queue `ready` count, exchanges,
bindings, the DLQ), and a **grep of your own Java** — **not** with a Java test
suite (writing tests is optional and not part of the deliverable). The per-step
`Acceptance checklist` **is** the "definition of done". Every command you need
is copy-pasteable in [`docs/verification.md`](docs/verification.md). The
**code** lives in **your copy of `../springboot`** (a new
`com.app.listings.messaging` package); **this repo** (`messaging/`) only owns
the statement, the broker compose, and the verification commands.

> **Prerequisite: TP1 is done and green.** The TP1 API passes its core
> checklists (register, CRUD, ownership 401/403/404, `?version=V1|V2`) and runs
> on port 8080. Finish TP1 first — this TP only adds a messaging layer on top.
>
> This is an **alternative** to [TP Keycloak](../keycloak),
> [TP Monitoring](../monitoring) and [TP IA](../IA) for the second session.
> All start the same base `listings-api`, so run **one** at a time.

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

### The stack (versions pinned — do not guess)

| Piece | Coordinates / tag | Used by |
|---|---|---|
| Spring AMQP starter | `org.springframework.boot:spring-boot-starter-amqp` (**no version** — managed by the Boot 4.1.1 BOM: spring-amqp **4.1.1**, rabbitmq-client **5.30.0**) | Steps 1-8 |
| RabbitMQ image | `rabbitmq:4.2-management` (management plugin bundled) | the whole TP |
| Management API/UI | `http://localhost:15673` (`/api/overview`, `/api/queues`, `/api/exchanges`) | the **observable surface** of every step |

> **Why one library and one broker:** Spring AMQP is the **client** (the
> `RabbitTemplate` producer + the `@RabbitListener` consumer). **RabbitMQ** is
> the **server** that stores and routes the messages. Keep the two separate in
> your head: you `POST` an HTTP request to your API; your API speaks **AMQP** to
> the broker; the broker routes to the queue; your API reads it back.

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

Each core step's `Acceptance checklist` is your "definition of done".
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

The **Acceptance checklist** of each step is the contract. It is verified
manually with `curl`, `psql`, and the management UI — not by a test.


---



## Step 1 - Bring up the stack (RabbitMQ + the app talking to it) `CORE`

> **User story** — *As a **developer**, I want the **RabbitMQ broker up** and my
> TP1 app **connected** to it, so that the following steps can publish,
> buffer, and deliver messages on a broker I can **inspect**.*

### Behavior

| # | Requirement |
|---|---|
| 1.1 | After `docker compose up -d` of the **base** and of **this** project, `rabbitmq` reports **healthy** (`rabbitmq-diagnostics ping` passes). There is **no second service** here — RabbitMQ persists to a named volume (`rabbit-data`), not a database. |
| 1.2 | The **management API** answers: `GET http://localhost:15673/api/overview` returns **200** with the broker's `rabbitmq_version` — **with** Basic auth `listings:listings`, **401** without. |
| 1.3 | The app boots with the **`spring-boot-starter-amqp`** on the classpath and **connects** to `rabbitmq:5672` (over the network name, not `localhost`). A connection appears in the management UI / `GET /api/connections` once the app has sent its first publish (Step 3). |
| 1.4 | `mvn clean verify` stays **green** (the TP1 smoke test still boots — it only needs Postgres, so it does **not** require RabbitMQ to be up). |

### Changes to make in TP1 (`../springboot`)

| What | From (TP1) | To (this step) |
|---|---|---|
| `pom.xml` | (no AMQP) | **add** `org.springframework.boot:spring-boot-starter-amqp` — **no version** (Boot 4.1.1 BOM: spring-amqp 4.1.1, rabbitmq-client 5.30.0) |
| `application.yml` | (none) | **add** the `spring.rabbitmq` block — `host: ${RABBITMQ_HOST:localhost}`, `port: ${RABBITMQ_PORT:5673}`, `username: ${RABBITMQ_USERNAME:listings}`, `password: ${RABBITMQ_PASSWORD:listings}` (Appendix A) |
| `../springboot/docker-compose.yml` | (no RABBITMQ_*) | **add** to `listings-api.environment`: `RABBITMQ_HOST: "rabbitmq"` and `RABBITMQ_PORT: "5672"` (the **network name** + **internal** port — see the REQUIRED EDIT at the bottom of `../messaging/docker-compose.yml`) |

### Acceptance checklist

- [ ] `docker compose ps` (in **both** folders) — `listings-api` **healthy**, `rabbitmq` **healthy**
- [ ] `curl -fsS -u listings:listings http://localhost:15673/api/overview` → **200** (and the same call **without** `-u` → **401**)
- [ ] `curl -fsS http://localhost:8080/actuator/health` → **200** (the app boots **with** the AMQP starter on the classpath)
- [ ] `mvn clean verify` in `../springboot` is **green**
- [ ] **No queue exists yet** — `curl -s -u listings:listings http://localhost:15673/api/queues` → **200** with an **empty** list (the queues are declared by the app in Step 2)

### Hints

- The full paste-ready config (pom + yml + the base-compose edit) is in
  **`README.md` → "0) Required base edit"** (the *what* and *why* is in
  **Appendix A**).
- **Network name, not localhost:** from inside the `listings-api` container,
  `localhost` is the container **itself**, so the AMQP target **must** be the
  network name `rabbitmq` on its **internal** port `5672` (host port `5673` is
  only for *your* curl). This is the **same split** the Keycloak TP uses for
  issuer-URI vs JWKS-URI.
- The broker **persists** to a named volume (`rabbit-data`), so `docker
  compose down` (without `-v`) keeps the queue state.
- Verify: [`docs/verification.md` §4](docs/verification.md).

---

## Step 2 - Declare the AMQP topology `CORE`

> **User story** — *As a **platform developer**, I want the **exchange, binding,
> and queues** to be **declared by my app** (not hand-drawn in the UI), so that
> the topology is **code**, reproducible, and **idempotent** across restarts.*

### Behavior

| # | Requirement |
|---|---|
| 2.1 | After the app boots, `GET /api/exchanges?name=listings.direct` → **200** with the **`direct`-type** exchange `listings.direct`. |
| 2.2 | `GET /api/queues` → **200** with **two** durable queues: `listings.messages` and `listings.messages.dlq`. **Both** survive a `docker compose restart rabbitmq` (durable). |
| 2.3 | `GET /api/exchanges/.../listings.direct/bindings/queue` → the exchange is **bound** to `listings.messages` with routing key **`listings.messages`**, and to `listings.messages.dlq` with routing key **`listings.messages.dlq`** (via the DLX `listings.messages.dlx`, which the main queue's `x-dead-letter-exchange` points to). |
| 2.4 | `GET /api/queues/.../listings.messages` → the `arguments` block contains **`x-dead-letter-exchange`**, **`x-dead-letter-routing-key`**, and **`x-dead-letter-exchange-type: direct`** (so a rejected/redelivered message is **rerouted**, not lost). |
| 2.5 | Restarting the app **does not duplicate** any exchange, queue, or binding (declarations are **idempotent** — re-start = same topology). |
| 2.6 | **No consumer is attached yet** — `consumers == 0` on `listings.messages` (that is Step 4). |

### Changes to make in TP1 (`../springboot`)

| What | From (TP1) | To (this step) |
|---|---|---|
| (new) `com.app.listings.messaging.config.RabbitConfig` | — | a `@Configuration` that declares **beans**: a `DirectExchange` **named `listings.direct`**; a durable `Queue` `listings.messages` **with the DLX/DLR + delivery-limit arguments**; a durable `Queue` `listings.messages.dlq`; a `DirectExchange` **named `listings.messages.dlx`** (the DLX); the two **`Binding`s** (exchange `listings.direct`→main on routing key `listings.messages`, DLX `listings.messages.dlx`→DLQ on routing key `listings.messages.dlq`); **and** a **`RabbitTemplate`** bean **pre-configured to target `listings.direct`** (via `setDefaultExchange("listings.direct")`), so the Step 3 producer can call `rabbitTemplate.convertAndSend("listings.messages", message)` with **only** the routing key — the exchange is the **named** one, **not** the anonymous `$default` |

> **Why declare in code (not in the UI):** the management UI is a **view**, not
> a **source of truth**. If the topology lived in the UI, a fresh
> `docker compose down -v` would wipe it and your app would start against an
> empty broker. Declaring it in `RabbitConfig` makes the broker a **plain
> runtime** and your app the **owner** of its own wiring — that is what
> "the topology is code" means.

### Acceptance checklist

- [ ] `GET /api/exchanges?name=listings.direct` → the exchange has `type=direct`
- [ ] `GET /api/queues?name=listings.messages` → `durable=true`, `arguments` contains the **three** `x-dead-letter-*` keys
- [ ] `GET /api/queues?name=listings.messages.dlq` → `durable=true`, `consumers=0`
- [ ] `GET /api/exchanges/listings.direct/bindings/queue` → the **binding** of `listings.direct` → `listings.messages` is present, with routing key `listings.messages`
- [ ] `docker compose restart rabbitmq && sleep 5` — the **same** two queues exist again (persistence + durable)
- [ ] `grep -rn "RabbitAdmin\|@Bean.*Queue\|@Bean.*Exchange\|@Bean.*Binding" ../springboot/src/main/java/com/app/listings/messaging/config` — the `RabbitConfig` class contains **all** of them

### Hints

- A **direct** exchange in Spring AMQP is **`org.springframework.amqp.core.DirectExchange`** (or the equivalent **`new Exchange("listings.direct", BuiltinExchangeType.DIRECT, true, false, null)`**). The name **must** be `listings.direct` (stable, greppable).
- The **main queue** arguments are the **exact** set in `Conventions` above — `QueueBuilder.durable("listings.messages").withArgument("x-dead-letter-exchange", "listings.messages.dlx").withArgument(...)`, etc.
- The **DLQ** is a **plain** durable queue (no dead-letter arguments of its own — it **is** the dead-letter sink).
- The **DLX** is a **separate** `direct` exchange named `listings.messages.dlx` (not the same as `listings.direct`).
- Two `@Bean Binding`s: exchange→main (routing key `listings.messages`), and DLX→DLQ (routing key `listings.messages.dlq`).
- Verify: [`docs/verification.md` §5](docs/verification.md) — six copy-paste `curl`s.


## Step 3 - Publish: buyer asks, seller not yet online `CORE`

> **User story** — *As a **buyer**, I want to **ask the seller a question**
> about a listing and **get a 202 back immediately**, so that I **do not wait
> for the seller to be online** — the broker holds my question until they read it.*

### Behavior

| # | Requirement |
|---|---|
| 3.1 | **`POST /listings/{id}/messages`** with body `{ "body": "<text>" }` (or `{ "body": "<text>", "inReplyTo": "<messageId>" }`) → **202 Accepted** with a body `{ "messageId": "<uuid>", "status": "SENT" }`. **No consumer is attached yet** (still true if Step 4 is not done — this step works alone). `messageId` is a **UUID generated by the producer**, not by the broker. |
| 3.2 | The row appears in Postgres **immediately** (after the 202): `SELECT * FROM message WHERE message_id = '<uuid>'` → `status = 'SENT'`. |
| 3.3 | In the management UI, `listings.messages` shows **`ready` = +1** (or more, if you publish several) — the message is **sitting in the broker, not consumed**. |
| 3.4 | A **second buyer** posting a different question to the **same listing** → the 2nd message is also accepted; `ready` = **2**; two rows in Postgres (one `SENT` per row). |
| 3.5 | **`GET /listings/{id}/messages`** as the **buyer** (their **own** question) → **200**, **1** item (their own message, `status` = `SENT`), `from_user_id` and `to_user_id` correct. |
| 3.6 | **`GET /listings/{id}/messages`** as the **seller** (the **owner** of the listing) → **200**, **2** items (both buyers' messages, `sentAt` ascending, `status` = `SENT` — not yet `DELIVERED`). The **seller sees every buyer** they have. |
| 3.7 | **`GET /listings/{id}/messages`** as a **third, unrelated** TP1 account (neither buyer nor seller) → **200**, **0 items** (empty list — **not** 403; the thread is **not leaked** to non-participants). |
| 3.8 | `fromUserId == toUserId` (posting to your own listing) → **422** with `message` and `fields: { fromUserId: "cannot message yourself" }` — the broker is **not** involved (the check happens **before** the publish). |
| 3.9 | `inReplyTo` references a `messageId` that **does not exist** or belongs to **another listing** → **422** with `fields: { inReplyTo: "unknown or not part of this listing" }` — again, no broker interaction. |
| 3.10 | Missing / empty `body` → **422** with `fields: { body: "must not be blank" }`. |
| 3.11 | Unknown listing id → **404** (TP1 `ResourceNotFoundException` — same as `GET /listings/99999`). |

### Expected behavior matrix (the TP's one full matrix — here, not per step)

The single rule (see the note below): `fromUserId` is **the caller** (the
Basic-auth identity); `toUserId` is the **recipient** — the **`from_user_id`
of the message you reply to** when you send an `inReplyTo`, and the **listing
owner** when you open a new thread. A message where `fromUserId == toUserId`
is rejected (`422`).

| Caller (Basic cred) | POST **open** (no `inReplyTo`) | **reply** (`inReplyTo` = a real msg on this listing) | `inReplyTo` **unknown**/bad | empty `body` | `GET /listings/{id}/messages` |
|---|---|---|---|---|---|
| **Anonymous** | **401** | **401** | **401** | **401** | **401** |
| **BUYER** (non-owner of this listing) | **202 SENT** (to = the owner) | **202 SENT** (to = the person you reply to) | **422 `inReplyTo`** | **422 `body`** | **200** — only the turns they took part in |
| **SELLER** (owner of this listing) | **422** (an opening message to yourself → `from == to`) | **202 SENT** (replying to a buyer → to = that buyer) | **422 `inReplyTo`** | **422 `body`** | **200** — the whole thread |
| **SELLER** (owner of a *different* listing = a buyer here) | **202 SENT** (to = this listing's owner) | **202 SENT** | **422 `inReplyTo`** | **422 `body`** | **200** — **0 items** (not their listing) |

> **The one rule, in full** —
>
> - `fromUserId` is **always the caller** (the HTTP-Basic identity). It is
>   **never** taken from the request body.
> - `toUserId` is **always explicit**: it is the `from_user_id` of the message
>   referenced by `inReplyTo` (a **reply**), or the **listing's owner** when
>   `inReplyTo` is absent (an **opening** question). The controller computes it
>   **before** the publish, so the broker never sees an empty recipient.
> - A **self-message** (`fromUserId == toUserId`) is a **422** — you cannot
>   "ask" yourself, and you cannot reply to your own message. This is the **only**
>   case where an *opening* message (no `inReplyTo`) is rejected.
> - An **unknown listing** is a **404** (TP1's `ResourceNotFoundException` —
>   same as `GET /listings/99999`), independent of the caller.
> - `GET /listings/{id}/messages` returns **200 with only the turns the caller
>   took part in** (they are `from` or `to`). A **non-participant** gets
>   **`200 []`** (not 403) — the thread is not *leaked*; a stranger simply has
>   no view into it.
>
> > **Note:** the labels "buyer" / "seller" are shortcuts. The **actual check**
> > is `caller == listing.owner`: **any** authenticated account may **open a
> > question** on a listing, and any account may **reply** to a message in its
> > own thread. The one structural constraint is `from != to`. (In production
> > `toUserId` could instead be a **claim** — an `X-Actor` header, a `userId`
> > in a Keycloak JWT's `sub`, or the IA TP's Basic email — rather than
> > derived; this TP keeps the derivation because it is the simplest contract
> > that still fits in one row per turn.)

### Changes to make in TP1 (`../springboot`)

| What | From (TP1) | To (this step) |
|---|---|---|
| (new) `com.app.listings.messaging.MessageEntity` | — | a JPA `@Entity @Table(name="message")` with columns matching the **Conventions** (including **unique** `messageId`, `fromUserId`, `toUserId`, `inReplyToMessageId`, `body`, `status` (an **enum** `MessageStatus { PENDING, SENT, DELIVERED }`), `sentAt`) |
| (new) `com.app.listings.messaging.MessageRepository` | — | a Spring Data JPA `JpaRepository` with **`Optional<MessageEntity> findByMessageId(String messageId)`** |
| (new) `com.app.listings.messaging.MessageService` (+impl) | — | a **producer** service: `SENT` = insert a row (status `SENT`, `sentAt=now`, `fromUserId=caller`, `toUserId`=the **recipient** — the `from_user_id` of the `inReplyTo` message when replying, or the listing owner when opening) and `publish` with **`RabbitTemplate`** using the routing key `listings.messages` |
| (new) `com.app.listings.messaging.MessageController` | — | `POST /listings/{id}/messages` (auth: Basic — TP1) that resolves the listing via TP1's `ListingService`, resolves the `inReplyTo` (if any) via `MessageRepository`, does **not** bypass TP1's **404** for unknown id, calls `MessageService.send(...)` |
| `src/main/resources/db/changelog/v1.1-messages.yaml` | (TP1: `v1.0-init.yaml` + `v1.0.1-identity.yaml`; **no** v1.1 yet) | **add** a **single additive** `createTable` changeSet `v1.1-messages` creating the `message` table with a **unique** `message_id` column (UUID), and **add** the include line to `db.changelog-master.yaml` — **do not** modify `v1.0-init.yaml` (TP1 rule) |

### Acceptance checklist

- [ ] `curl -i -u buyer@test:secret123 -X POST http://localhost:8080/listings/1/messages -H 'Content-Type: application/json' -d '{"body":"Is this still available?"}'` → **202** with `"messageId"` (a **UUID** — not a Postgres-generated number) and `"status":"SENT"`
- [ ] `docker compose exec listings-postgres psql -U listings -d listings -c "SELECT message_id, from_user_id, to_user_id, in_reply_to, status FROM message WHERE message_id = '<uuid>'"` → **1 row**, `status=SENT`
- [ ] In the management UI (`http://localhost:15673/queues/...listings.messages`) or `GET /api/queues?name=listings.messages` → **`ready` ≥ 1**, **`consumers` = 0**
- [ ] `curl -i -u carol@test:secret123 http://localhost:8080/listings/1/messages` → **200**, **`[]`** (not in thread) — **not** 403
- [ ] `curl -i -u seller@test:secret123 http://localhost:8080/listings/1/messages` → **200**, a **JSON array** of all buyers' messages, ascending `sentAt`
- [ ] `curl -i -u buyer@test:secret123 -X POST .../messages -d '{"body":"hi","inReplyTo":"<non-existent-uuid>"}'` → **422** with `fields.inReplyTo`
- [ ] `curl -i -u alice@test:secret123 -X POST .../messages -d '{"body":"hi"}'` **on a listing owned by alice** → **422** (self-message)
- [ ] `grep -n "RabbitTemplate" ../springboot/src/main/java/com/app/listings/messaging/` — at least **one** reference (the producer)

### Hints

- **Generate the `messageId`** with `UUID.randomUUID().toString()` in the **producer** (before `RabbitTemplate.convertAndSend`). Do **not** let the broker assign it — the broker is not going to be the source of truth for **idempotency** (Step 4).
- **The 202 happens BEFORE the consumer sees the message.** The broker **holds** it. This is **not** a synchronous call to any other Java code — it is a **network write to RabbitMQ**.
- The **`body`** of the wire message is a **JSON string** (Jackson `ObjectMapper`), with the envelope in `Conventions`. The **`toUserId`** is resolved from the **listing's owner** at publish time and written **in the body**, so the consumer does not need to re-lookup the listing.
- For the **thread read**, a simple `findByListingIdAndFromUserIdOrToUserId(...)` is sufficient — or **two** repository calls and a union.
- `docs/verification.md` §6 walks all the 202/404/422/401 cases with copy-paste `curl`s.

---

## Step 4 - Consume, persist, and reply `CORE`

> **User story** — *As a **seller**, I want to **read a question** and **reply
> through the same endpoint** (with the `inReplyTo` set), so that the whole
> **conversation** is **asynchronous**, **buffered**, and **readable** on both
> sides.*

### Behavior

| # | Requirement |
|---|---|
| 4.1 | A **`@RabbitListener`** method on `listings.messages` is registered (visible in the management UI: **`consumers` = 1** on the queue once the app is up). |
| 4.2 | On delivery, the listener **upserts** the row by `message_id` (an **already-published** row — status `SENT` — is **updated** to `DELIVERED`, with `delivered_at = now`; a **redelivered** message with an **unknown** `message_id` is **inserted**). The **wire body's `messageId`** is the **source of truth** for the upsert — the DB `@GeneratedValue` **is not** the idempotency key (that is why `message_id` is **unique** and **client-generated**). |
| 4.3 | **After** the seller replies (a `POST` with `inReplyTo`), the **buyer**'s `GET /listings/{id}/messages` shows **3** rows: their own question (`SENT`/or `DELIVERED`), the seller's reply (`DELIVERED`), and (if the seller replied to **their own** reply, a 3rd turn) — ascending `sentAt`. |
| 4.4 | **`messageId` duplication** (the same wire message is delivered **twice** — e.g. you restart the app **without** `mvn clean` and replay the queue) → **exactly one row** in `message` for that `message_id` (the second delivery is a **no-op** upsert). This is what the **unique** index + `INSERT … ON CONFLICT (message_id) DO NOTHING` pattern gives you. |
| 4.5 | **Acknowledge** is **automatic** (Spring AMQP default) — no manual `channel.basicAck(...)` is required, but the listener **must not** throw an **uncaught** exception for the "business" cases (a missing buyer, an orphan `inReplyTo`, etc.) — those are **logged and swallowed**, not thrown. |
| 4.6 | **Poison** (a message the listener deliberately **rejects** with `basicRequeue=false`) → the broker **reroutes** it to `listings.messages.dlq` (Step 5 proves this). |

### Changes to make in TP1 (`../springboot`)

| What | From (TP1) | To (this step) |
|---|---|---|
| (new) `com.app.listings.messaging.MessageListener` | — | a `@Component` with a single method annotated `@RabbitListener(queues = "listings.messages")` that: **1.** deserializes the wire JSON (to a small **DTO** you name, e.g. `MqMessage`), **2.** **upserts** the row by `messageId` (status → `DELIVERED`, `deliveredAt=now()`), **3.** **catches** and **logs** any per-message business error with `log.warn("skipping message id={} reason={}", id, ex.getMessage())` and **does not** propagate the exception (the default ack is therefore **safe**) |
| `MessageRepository` (Step 3) | `findByMessageId` | **+** a query/JPQL **upsert** (or an `@Modifying @Query` with `INSERT ... ON CONFLICT (message_id) DO UPDATE SET ...` via JPA `save()` + prior `findByMessageId` check — the latter is **simpler** and sufficient for this TP) |

### Acceptance checklist

- [ ] `GET /api/queues?name=listings.messages` (with credentials) → `consumers` = **1** (the listener is attached)
- [ ] Right after a buyer `POST /listings/1/messages` (Step 3), within **2 seconds**, `psql SELECT status FROM message WHERE listing_id=<id> ORDER BY sent_at DESC LIMIT 1` → `DELIVERED` (the consumer has **already** processed it)
- [ ] **Seller replies** with `inReplyTo = <the buyer's messageId>` → the **buyer's** `GET` now shows **their own + the seller's reply**, both `DELIVERED` (or `SENT` for the latest turn)
- [ ] **Duplicate delivery drill**: open a **second** terminal and **re-publish** the **exact** same JSON to the same routing key via the **management UI** → only **one** row in `message` for that `message_id` (the second is a no-op)
- [ ] `docker compose logs -f listings-api` → **no** stack-trace on the "business" reject cases (a buyer that no longer exists); a `log.warn` is **expected** (the listener is alive and **choosing** to skip)
- [ ] `grep -n "@RabbitListener" ../springboot/src/main/java/com/app/listings/messaging/MessageListener.java` → the annotation is on the queue **name** (the **literal** string `listings.messages`)

### Hints

- **`@RabbitListener`** is in `org.springframework.amqp.rabbit.annotation`; the
  **queue name must match Conventions** (`listings.messages` — the **string**,
  not a constant import — to keep it greppable).
- The **method parameter** is either a **raw `String`** (the JSON body
  directly) or a Spring AMQP **`org.springframework.messaging.Message`**
  (with `getBody()`). Both are **fine**. If you pick `String`, you need a
  `com.fasterxml.jackson.databind.ObjectMapper` (already on the classpath) to
  parse it.
- **Idempotency (row 4.4)** — upsert **by `messageId`** (the **wire** field),
  **not** by the Postgres-generated `@GeneratedValue`. The **repository**
  already has `findByMessageId` (from Step 3); the pattern is **"SELECT +
  INSERT-or-UPDATE"** in the service. Spring Data JPA `save()` + the prior
  `find` is sufficient for this TP.
- **`MessageEntity`** (from Step 3) needs a **`setDeliveredAt(Instant)`**
  setter in addition to `status` and `messageId` (the **fluent** pattern the
  TP1 skeleton already uses — e.g. `User.withPasswordHash(...)`, `Listing.withTitle(...)`).
- **Do not** `throw` inside the listener for the **known** business cases
  (a buyer that no longer exists, an orphan `inReplyTo`) — those are **logged
  and swallowed**, so the **ack** is safe. **Do** let **unexpected** errors
  bubble — that is the contract Step 5's proof (DLQ) depends on.
- `docs/verification.md` §7 walks the full **reply round-trip** — buyer POST,
  seller POST-inReplyTo, both `GET`s.


## Step 5 - Poison messages go to the dead-letter queue `CORE`

> **User story** — *As a **platform developer**, I want a message my consumer
> **cannot process** (an **unparseable** body, a **business exception** I
> decide is fatal) to be **relocated to a DLQ I can inspect** instead of
> **looping forever** on the main queue — so that **one bad message cannot
> block the others**.*

### Behavior

| # | Requirement |
|---|---|
| 5.1 | The **main queue** (`listings.messages`) was declared (Step 2) with `x-dead-letter-exchange: listings.messages.dlx`, `x-dead-letter-routing-key: listings.messages.dlq`, `x-dead-letter-exchange-type: direct`, and `x-delivery-limit: 3`. |
| 5.2 | The **consumer** is **aware** of the DLQ: when it encounters a **deliberate poison** (a message whose JSON body does **not** match the **envelope** — e.g. `{ "bogusField": true }`), it **rejects** the message with `basicAck=false, basicRequeue=false` (i.e. `channel.basicNack(..., false)`), which causes the broker to **reroute** it to `listings.messages.dlq`. |
| 5.3 | After the poison, `listings.messages` `ready` count is **decreased by 1** (the message **left** the main queue), and `listings.messages.dlq` `ready` count is **increased by 1** (the message **landed** in the DLQ). |
| 5.4 | **Subsequent** legitimate messages sent **after** the poison **still** arrive and **process** on the main queue — the poison did **not** block the queue (because `basicRequeue=false` prevents the "infinite requeue loop"). |
| 5.5 | In the management UI, the DLQ message is **visible** (its body, routing key, redelivery count, and `expiration` = `0` = no TTL). |
| 5.6 | The **producer** is **not** changed in this step — the poison is a **wire-level** issue (a message the **broker already has** that the consumer **cannot consume**), not a 202/404 from the **producer** side. |
| 5.7 | The app **does not crash** on a poison — `docker compose logs listings-api` shows the **expected** `log.warn` (or the **exception** bubbling up and the Spring AMQP default error handler sending it to the DLQ), not a **500 stack trace** or a **container restart**. |

### Acceptance checklist

- [ ] **Poison drill** (from [`docs/verification.md` §8](docs/verification.md)):
  - `POST http://localhost:15673/api/exchanges/listings.direct/%2F/publish?routing_key=listings.messages&exchange=listings.direct` with a **body** whose JSON **does not** match the `MqMessage` envelope (e.g. `{"oops":true}`) and **routing key** `listings.messages` → **200** (the broker accepts it — it is routed to the **main** queue, where the **consumer** then rejects it and the broker moves it to the **DLQ**)
  - A **few seconds later**, `GET /api/queues?name=listings.messages` → `ready` is **+0 or -1** (the message **did not stay** in the main queue indefinitely, or it is **still 1** if the consumer has not yet rejected it — either is **correct** depending on timing)
  - `GET /api/queues?name=listings.messages.dlq` → `ready` = **+1** (or more) — the poison **landed** in the DLQ
  - **`psql SELECT count(*) FROM message`** — the row count is **unchanged** or **has not grown** for that `messageId` (the consumer **did not** insert a `DELIVERED` row for a message it could not parse)
- [ ] **After the poison**, a **new legitimate** `POST /listings/{id}/messages` (through the REST endpoint) still results in a `DELIVERED` row and `ready` decrement on the main queue — the queue is **not stuck**
- [ ] `docker compose logs listings-api | tail -30` — the poison is **visible** in a **log line** (either a warn or an exception trace), **without** a `Restarting` from the container being up

### Hints

- The **class** and **method** are the **same** as in Step 4:
  `com.app.listings.messaging.MessageListener` → `onMessage(...)`. Extend the
  signature to **`Message mqMessage, Channel channel`** (the `Channel` is
  `com.rabbitmq.client.Channel`, **already** brought in by the
  `spring-boot-starter-amqp` — **no new dependency**).
- The **rejection call** you need is **`channel.basicNack(…)`** with
  **`requeue = false`** (the **3rd** boolean arg, **not** the **2nd**, which
  is `multiple`). The delivery tag is on
  `mqMessage.getMessageProperties().getDeliveryTag()`. The **`false`** on the
  last arg triggers the **`x-dead-letter-*`** arguments declared by Step 2. If
  you use **`basicReject`** instead (single call, no `multiple`), the same
  **last-boolean** rule applies (`false`).
- **Do not** `requeue=true` for any message — that is the **poison-pill**
  anti-pattern: the broker redelivers the **same bad** message to the **same
  broken consumer**, in a loop.
- **Auto-backstop** (Step 2): even if you do **not** call `basicNack`, the
  `x-delivery-limit: 3` in Step 2 will **reroute** any message that has been
  requeued **3 times** to the DLQ. The **consumer-side** `basicNack` is for
  **deciding on your own** (e.g. an **unparseable** body — the **broker**
  cannot see that — to say "move it now").
- `docs/verification.md` §8 gives the **exact** 3 `curl`s (poison publish,
  then the two `GET /api/queues`) and the **expected** output.

---

## Step 6 - Broker down: the API survives and returns 503 `CORE`

> **User story** — *As the **buyer**, I want my `POST .../messages` to **fail fast** with a **503** (not a hang, not a **silent drop**) when the **broker is down**, so that the **caller** (the UI, or a service) **knows the message was not accepted** and can retry — **without** the rest of the TP1 API being collateral damage.*
>
> **Superseded by Step 8 (bonus, opt-in):** Step 8 upgrades this **failing**
> publish contract from a **503** to a **202 `PENDING`** (transactional outbox).
> If you implement Step 8, the **publish path** of Step 6 **no longer returns
> 503** (it returns **`202 PENDING`**); the **`@ExceptionHandler` for
> `BrokerUnavailableException`** is **kept** as a **safety net** for any direct
> publish **outside** the `MessageService`. Do **not** implement Step 8 expecting
> the Step 6 503 line to still be true — it is **intentionally replaced**.

### Behavior

| # | Requirement |
|---|---|
| 6.1 | With the broker **up**, `POST /listings/{id}/messages` → **202** (Step 3). |
| 6.2 | `docker compose stop rabbitmq` (the **broker** container is **stopped** but the base API **stays up**) → `POST /listings/{id}/messages` → **503** with the **TP1** JSON error shape (`timestamp`, `status:503`, `error`, `message` that **mentions** the broker is unavailable, e.g. `"rabbitmq not reachable — check docker compose / RABBITMQ_HOST / RABBITMQ_PORT"`). |
| 6.3 | **No other endpoint is affected**: `GET /listings` and `GET /listings/{id}` still return **200** during the broker outage (TP1's business endpoints are **decoupled** from the broker). |
| 6.4 | `curl -fsS http://localhost:8080/actuator/health` → **200** (the app is **alive**; it is the **broker** that is down, not the app). |
| 6.5 | `docker compose start rabbitmq` → wait for `rabbitmq-diagnostics ping` → the **producer** can publish again → `POST .../messages` → **202**. |
| 6.6 | **The consumer survives the outage**: messages that were **already in the queue** (ready count > 0 before the stop) are **still there** after the start (the **durable** queue + the **broker's Mnesia table persistence** is doing its job). Once the consumer reconnects (or after `start`), they are **drained** and **persisted** to `message` (the `SENT` rows become `DELIVERED`). |
| 6.7 | **The failure must be fast** (≤ ~2 seconds) — a `connect` timeout of `10s` or `30s` is **too long** for the lab; the **starter** default is fine, but the **503 path** must be a **clean** one (not a **500** from a `AmqpConnectException` being **thrown** uncaught out of the controller). |

### Changes to make in TP1 (`../springboot`)

| What | From (TP1) | To (this step) |
|---|---|---|
| (new) `com.app.listings.messaging.exception.BrokerUnavailableException` | — | a **runtime** exception (extends **`RuntimeException`**, not `AmqpException`, to keep it in the **domain** package `com.app.listings.domain.exception` **or** `com.app.listings.messaging.exception` — your choice, but **document it** in the `Deliverables` table) |
| `MessageService` (Step 3) | the `RabbitTemplate.convertAndSend(...)` call **throws** an **AMQP** exception (e.g. `AmqpConnectException`) when the broker is down — which, **unhandled**, surfaces as a **500** (TP1's generic handler) | **wrap** the call in a `try / catch (AmqpConnectException \\| AmqpTimeoutException ex) { throw new BrokerUnavailableException("rabbitmq not reachable — …", ex); }` — so the **controller** never sees an AMQP exception directly, and the **`GlobalExceptionHandler`** has a **specific** exception to map to **503** |
| (new) `@ExceptionHandler(BrokerUnavailableException.class)` in the **TP1** `GlobalExceptionHandler` | (TP1 has only 422, 409, and 500) | **add** one **method** that returns **503** with the **TP1 JSON shape** — same **shape** as TP1's `handleValidation` / `handleEmailTaken` (i.e. the 4 keys `timestamp`, `status`, `error`, `message`: a **5th** optional `fields` if you want, but **not required** here) |

### Acceptance checklist

- [ ] `docker compose stop rabbitmq` — `ps` shows `rabbitmq` **exited** or **Stopped**
- [ ] `curl -i -u buyer@test:secret123 -X POST http://localhost:8080/listings/1/messages -d '{"body":"still there?"}'` → **503**, body has `"status":503` and a **`message`** mentioning `rabbitmq` (or `broker`, or `unavailable`); **not** `500`, **not** a **stack trace**
- [ ] `curl -fsS http://localhost:8080/actuator/health` → **200** (the API is **alive**)
- [ ] `curl -fsS http://localhost:8080/listings` → **200** (TP1's **read** path is **not** affected by the broker outage)
- [ ] `docker compose start rabbitmq && sleep 5` — `ps` shows `rabbitmq` **up** again
- [ ] `curl -i -u buyer@test:secret123 -X POST http://localhost:8080/listings/1/messages -d '{"body":"back?"}'` → **202** with **`"messageId"`** (a **new** UUID, **not** the one from before the stop)
- [ ] **Drain check** (the consumer is **alive** on the other side): `docker compose logs listings-api | tail -10` shows a **listener activity** (e.g. a `log.info("delivered messageId=…")`) once the broker is back and a `GET /api/queues` shows the **ready** count dropping to **0** for `listings.messages`

### Hints

- The **exception to throw** is the **new** one you define in the
  `Changes` table (`BrokerUnavailableException`). It must be **runtime**
  (extends `RuntimeException`), because **checked** exceptions would force a
  `throws` clause on the producer interface and propagate to the **controller**
  (which you do **not** want — the controller stays **thin**).
- The **`@ExceptionHandler`** you add in TP1's `GlobalExceptionHandler`
  returns **503** (not **500**); the **JSON shape** is **the same** as TP1's
  existing handlers (the **4** keys `timestamp`, `status`, `error`, `message`
  — no `fields` here, **that's fine**).
- **The 503 is a *production* contract, not an *edge* case**: in a **real**
  deployment, "broker down" is a **degradable** failure (read the queue later,
  retry), not a **crash**. This step is where the **API** proves it knows the
  **difference**.
- **`messageId` is generated on the producer side** (Step 3) — even during the
  outage, the **202 path** will always use a **fresh** UUID, **not** a **retry**
  of a previous one. Do **not** try to **replay** a previous `messageId` across
  a stop/start cycle.
- `docs/verification.md` §9 is the full **stop → 503 → start → 202** sequence,
  with the **expected** response bodies.

---


---

## Step 7 - At-least-once delivery and idempotency `BONUS`

> **User story** — *As a **platform developer**, I want to prove that a
> **redelivered** message (e.g. the broker **crashes between `deliver` and
> `ack`**, or the **consumer** crashes **after** delivery) does **not** create
> **two** rows in `message` — so that the **at-least-once** guarantee is
> **visible** in the database, not just in theory.*

### Behavior

| # | Requirement |
|---|---|
| 7.1 | **Pre-condition**: a message with `message_id = M` is **currently** in `listings.messages` with `consumers ≥ 1` and `ready ≥ 1`. |
| 7.2 | **Crash drill**: `docker compose stop listings-api` **immediately after** a buyer `POST` (while the consumer is **in the middle** of processing, or **before** it has ack'd). The **durable** queue still **holds** the message (the `ready` count in the UI **does not go to 0**, or stays at **1**). |
| 7.3 | **Recovery**: `docker compose start listings-api` → the consumer **reconnects**, the **same** `message_id = M` is **delivered** (or **redelivered**) exactly **once** more, and the row in `message` has **status = `DELIVERED`**, and `deliveries` (the column in the management UI) is **`2`** or more (the **first** delivery was consumed **before** the crash — the **second** is the **redelivery**). |
| 7.4 | **No duplication**: `SELECT count(*) FROM message WHERE message_id = 'M'` → **`1`**, **not** **`2`** — the **unique index** + the **listener's** SELECT-then-INSERT-or-UPDATE (Step 4) is doing its job. |
| 7.5 | **Repeat** (optional): run the **same** crash-drill **2 more times** (with different buyers' `message_id`s, each **`M₁`**, **`M₂`**, **`M₃`**). **Each** of the **3** rows is **exactly one** in `message` (`count(*) = 1` per `message_id`), **even though** each was **delivered 2+ times** (the **`deliveries`** counter in the UI will be **≥ 2**). |
| 7.6 | **Producer-side** (the **202** path): during the **crash**, a **second** `POST` from a **different** buyer (a **new** `message_id`) → the **503** path is **not** triggered (the **broker** is **still up** in this drill — only the **consumer** is down). The **new** message is **queued** and **drained** once the consumer returns. |

### Acceptance checklist

- [ ] **Crash-drill #1**: `docker compose stop listings-api` (the **API** container is **down**, **rabbitmq** is **up**); `curl -i -u buyer@test:secret123 -X POST http://localhost:8080/listings/1/messages -d '{"body":"in flight?"}'` returns **`502`** (or **`connection refused`**, because the **API container itself** is down — this is **different** from the Step 6 scenario, where the broker is down and the API is up)
- [ ] `psql SELECT count(*) FROM message WHERE status='SENT' OR status='DELIVERED'` — the **count** is **as expected** (one row for each published message)
- [ ] **`docker compose up -d listings-api`** (restart the container) → the consumer **reconnects**; **within 5 seconds** the **message** is **delivered** and the row is **`DELIVERED`**
- [ ] **`GET /api/queues?name=listings.messages`** — the **`deliveries`** counter is **≥ 2** for the **in-flight** message (it was **delivered** once, **not ack'd**, **redelivered** once — **at least once**)
- [ ] **`SELECT count(*) FROM message WHERE message_id IN (<the 3 in-flight ids>)`** → **`3`**, **not** `6` (no duplication)
- [ ] **`docker compose logs listings-api | grep -i "delivered\|duplicate\|skipping"`** → the **3** deliveries are **logged** (the `log.warn` from Step 4, **or** the `log.info` from a **successful** upsert), **without** any `DuplicateKeyException`
- [ ] **`rabbitmqctl list_queues name messages_ready messages_unacknowledged`** (from the **rabbitmq** container: `docker compose exec rabbitmq rabbitmqctl list_queues ...`) → **`messages_ready = 0`** (the queue is **drained**)
- [ ] **Producer side during the crash**: `POST /auth/register` (TP1 — **does not** touch the broker) → **201** (TP1's **register** is **decoupled** from the messaging layer — it does **not** **503** when the consumer is down)

### Hints

- The **`deliveries`** counter in the management UI is the **raw count** of
  times the broker **sent** the message (including the first one). A **value
  of ≥ 2** on a **delivered** message is the **at-least-once** proof.
- The **`count = 1, deliveries ≥ 2`** is the **whole point** of **Step 4's**
  upsert + **Step 2's** unique index. **Without** the **unique** index, the
  upsert would **insert a 2nd row** and the **`message`** table would have
  `count = 2, deliveries = 2`.
- **`rabbitmqctl`** is **available inside** the `rabbitmq` container
  (`docker compose exec rabbitmq rabbitmqctl list_queues name
  messages_ready messages_unacknowledged`). It is the **CLI** for the same
  data the **management UI** shows (so you **can** verify without the
  browser).
- **`docker compose stop listings-api`** **does not** drop the **volume**
  (`rabbit-data` is **named**, so it **survives** the stop). The **durable**
  queue is **persisted** in that volume. **`down -v`** **would** wipe it
  (do **not** run that mid-drill, or you lose the evidence).
- `docs/verification.md` §10 (if you do this step) is the **exact** sequence,
  with the **pre/post** `psql` snapshots.

---

## Step 8 - Transactional outbox: 202 even when the broker is down `BONUS`

> **User story** — *As the **buyer**, I want `POST /listings/{id}/messages`
> to return **202** even when the **broker is down** (Step 6's **503** is the
> **simpler** contract; this is the **stronger** one), so that the **call
> is not lost** — a **poller** re-publishes the message from the **database**
> to the broker **as soon as it is back**.*

> **Note on scope**: this step **overrides** the Step 6 contract (503) on the
> **publish path** — a **`PENDING`** row is **inserted** instead of a **503**
> being **returned**, and a **`@Scheduled`** poller (or an explicit
> **`POST /outbox/drain`** endpoint) re-publishes the `PENDING` rows when the
> broker is reachable again. The **Step 5** (DLQ) contract is **not**
> overridden — the **DLQ** is still the **dead-letter** path for **poison**
> messages.

### Behavior

| # | Requirement |
|---|---|
| 8.1 | **With the broker down**: `POST /listings/{id}/messages` (the **Step 3** endpoint **reused**, but the **service** changes) → **202** with `{ "messageId": "<uuid>", "status": "PENDING" }` (the **row** is **inserted** in `message` with `status = PENDING`, **not** published to the broker). The response **shape** is the **same** as Step 3's 202 (the **status** field is the **only** difference: `PENDING` vs `SENT`). |
| 8.2 | **With the broker up**: `POST /listings/{id}/messages` → **202** with `{ "messageId": "<uuid>", "status": "SENT" }` (the **row** is **inserted** and the **message is published** **in the same transaction**). **This is the "fast path"** — the **outbox** pattern is **invisible** in the normal case. |
| 8.3 | **The poller**: a **`@Scheduled(fixedDelay = 5000)`** (or `fixedRate`) **method** on the **`MessageOutbox`** bean scans `SELECT message_id FROM message WHERE status = 'PENDING' ORDER BY sent_at` and, for **each** row, **publishes** to the broker (routing key `listings.messages`) and **updates** the row to `SENT` (atomically — **`UPDATE … WHERE message_id = ? AND status = 'PENDING'`** — so a **second** poller on a **second** instance **cannot** also publish the **same** row). |
| 8.4 | **Recovery**: with the broker down, **3** calls → **3** rows `PENDING`. `docker compose start rabbitmq` → **within `~5s`** (the poller's **`fixedDelay`**), the **3** rows become **`SENT`** (the broker's **`ready` count** is **`3`**), then **`DELIVERED`** (the **consumer** drains, as in Step 4). **No** re-publish of an **already-SENT** row (the **`WHERE status = 'PENDING'`** guard does that). |
| 8.5 | **Idempotency** (Step 4 **still** applies): a **redelivered** `PENDING → SENT → DELIVERED` row is **upserted** by the **consumer** (Step 4's `message_id`-unique pattern) so **no** duplication. |
| 8.6 | **Multi-instance safety**: with **2** instances of `listings-api` running, the **poller** does **not** double-publish (the **`UPDATE … WHERE status = 'PENDING'`** is an **atomic** claim; the **loser** gets **0** rows updated and does **not** publish). This is the **whole reason** the **`UPDATE`** is **before** the `RabbitTemplate.convertAndSend`, not after. |

### Changes to make in TP1 (`../springboot`)

| What | From (TP1) | To (this step) |
|---|---|---|
| `MessageEntity` (Step 3) | `status` ∈ `SENT \| DELIVERED` (and the **enum** has those **two**) | **add** `PENDING` to the **`MessageStatus`** enum (the **`v1.1-messages.yaml`** schema **already** allows it if you declared the `status` as a **`VARCHAR(16)`** — **not** a PG `enum` — **which is the choice** for this reason; if you **did** declare a PG `enum`, you **cannot** add a value without a migration, so **re-decide**) |
| `MessageService` (Step 3, 6) | the **publish path** is either: **`SENT`** (202) or **`503`** (Step 6) — **two** paths | **one path**: **`PENDING`** (202) **when the broker is down**, **`SENT`** (202) **when it is up**. **The 503 path is removed** (the contract is **stronger**, and the **buyer** is **not** told "the broker is down" — they are **told** "the message is queued, it will be delivered"). **The Step 6 `@ExceptionHandler(BrokerUnavailableException)` can be removed** or **kept** (the **latter** is **better** — if someone **does** try a **direct** `convertAndSend` **outside** the **service**, the **503** still **fires**) |
| (new) `com.app.listings.messaging.MessageOutbox` | — | a **`@Component`** with a **`@Scheduled(fixedDelayString = "${listings.messaging.outbox.interval-ms:5000}")`** **method** that: **1.** selects `PENDING` rows (oldest first), **2.** for **each** row, **atomically** claims it (`UPDATE message SET status='SENT' WHERE message_id=? AND status='PENDING'`), **3.** **publishes** the claimed row via `RabbitTemplate`, **4.** on **publish failure**, **reverts** the row to `PENDING` (and **logs** — the **poller** will retry on the next cycle) |
| (new) **`application.yml`** | (none) | **add** the key `listings.messaging.outbox.interval-ms: ${LISTINGS_MESSAGING_OUTBOX_INTERVAL_MS:5000}` (and the **equivalent** env var in `../springboot/docker-compose.yml` **if** you want to tune it) |

### Acceptance checklist

- [ ] **With the broker down**: `POST /listings/1/messages` → **202** `{messageId, "status":"PENDING"}` (a **new** UUID, **not** `SENT`)
- [ ] `psql SELECT message_id, status FROM message WHERE message_id = '<id>'` → **`PENDING`** (the row is **in the DB**, **not** in the broker)
- [ ] **`docker compose start rabbitmq`** → **within 5–10 seconds**, `psql SELECT status FROM message WHERE status='PENDING'` → **0 rows** (the **poller** has claimed and published **all** of them)
- [ ] `GET /api/queues?name=listings.messages` → `ready` = **+N** (the **N** messages just published), then → **`0`** as the consumer drains (Step 4's `@RabbitListener` is **unchanged** and **still** picks them up)
- [ ] **`psql SELECT message_id, status FROM message ORDER BY sent_at DESC LIMIT 5`** → the **`3`** PENDING rows are now **`DELIVERED`** (the **full** chain **PENDING → SENT → DELIVERED** is **visible** in the same table)
- [ ] **No 503** anywhere in the **`docker compose logs listings-api`** **during** this drill (the **Step 6 handler** is **not triggered** because the **`MessageService`** **catches** the `BrokerUnavailableException` **internally** and **returns** a **`PENDING`** result **instead of rethrowing** it)
- [ ] **`grep -n "@Scheduled" ../springboot/src/main/java/com/app/listings/messaging/MessageOutbox.java`** → the **annotation** is **present**, with the **`fixedDelayString`** (or `fixedRate`) pointing at the **`application.yml`** key
- [ ] **`grep -n "UPDATE.*status='PENDING'\|UPDATE.*WHERE.*status" ../springboot/src/main/java/com/app/listings/messaging/`** — the **claim** is **before** the **`convertAndSend`** (atomic)
- [ ] **`listings.messaging.outbox.interval-ms`** is **env-overridable** (the `application.yml` reads from `${…}`), **not** hard-coded

### Hints

- The **`@Scheduled`** import is **`org.springframework.scheduling.annotation.Scheduled`**. The **`@EnableScheduling`** is a **`@Configuration`** (one per **app**, **not** one per **bean**).
- The **`UPDATE`-before-`convertAndSend`** is the **atomic claim** — **do not** `convertAndSend` first and then `UPDATE` (the **2nd** instance would **also** see `PENDING` and **also** publish → **doubling**).
- The **`WHERE status = 'PENDING'`** is the **guard** — **not** `WHERE message_id = ?`. Both are **correct**; the **`message_id`** is the **PK** and the **`status`** is the **guard** (the **PK** check would be **redundant** if the `status` check is there).
- The **`UPDATE`** is **`int affected = repo.updateClaim(id);`** — if **`affected == 0`**, **another** instance **won** the **race**; **do not** publish. **If** `affected == 1`, **`convertAndSend`**.
- `docs/verification.md` §11 (if you do this step) is the **full** sequence: **stop → 202 PENDING → start → poller → SENT → DELIVERED**.

---


## Step 9 - Final Integration

> **User story** — *As the **binôme**, I want the **whole stack** (TP1 base +
> RabbitMQ + the messaging module) to **boot cleanly together**, with the
> decision log in the **team README** and a **green** `mvn clean verify`, so
> that the TP is **done** in the sense that the evaluator can **run it** and
> **trust the output**.*

### Acceptance checklist

- [ ] `cd ../springboot && docker compose up -d --build` → **healthy**
- [ ] `cd ../messaging && docker compose up -d` → **healthy**
- [ ] `curl -fsS http://localhost:8080/actuator/health` → **200**
- [ ] **One** buyer `POST` → **202** with `SENT`, the message is in **`listings.messages`** (management UI `ready = 1`)
- [ ] **One** consumer drain → the row flips to **`DELIVERED`** in `psql`, and the **seller's** `GET` returns the message
- [ ] `mvn clean verify` in `../springboot` → **green** (the TP1 **smoke test** + your **messaging** smoke test **both** boot)
- [ ] **The decision log is in the team README** of `../springboot` (see *Deliverables* below, item 2) — the **5–6** short paragraphs of *why* the choices were made
- [ ] **No** secrets in the repo: `RABBITMQ_DEFAULT_PASS: listings` is in the **compose** (the **broker** credential, **not** the API secret) and is **not** in `application.yml` (the `application.yml` reads **`${RABBITMQ_PASSWORD:listings}`**, where `listings` is the **default** for **local** dev — and **any** other password is **env-injected** in compose, not committed)
- [ ] **The `messaging/` repo is committed** (this folder): `exercise.md` (this file), `docker-compose.yml`, `README.md`, `docs/verification.md`
- [ ] **The `springboot/` repo is committed** (the TP1 repo): the **messaging module** code, the **2** edits (`pom.xml` + `application.yml`), the **base compose** edits (`RABBITMQ_HOST` + `RABBITMQ_PORT`), and the **team README** (with the **decision log**)

### Hints

- The **team README** is the **one place** where the **binôme** writes
  *what* they decided and *why* — it is **not** a **copy** of the **exercise
  statement** (that is in `messaging/exercise.md`). It is **5–6** short
  paragraphs, one per **major** decision.
- The **smoke test** to add in `../springboot` is the **lowest** bar: **the
  context boots**. If it needs the **broker** up, it **uses** the **base
  compose's** `rabbitmq` (via `TESTCONTAINERS_RABBITMQ_HOST` or the **same**
  network name). If it needs Postgres, it **already** does (TP1). The **smoke
  test runs against the **same** environment as the student's **dev** machine
  — so **either** (a) it is **fast** (no real network calls) and the broker /
  Postgres are **not required** to be up, **or** (b) it is **slow** and the
  evaluator must **start** the two containers first — **pick (a)** for the lab.

---

## Deliverables

| Item | Detail |
|---|---|
| **Pair's TP1 repo** (`../springboot/`) | `+1 dep` (`spring-boot-starter-amqp`, **no** version — Appendix A); `+` the `spring.rabbitmq` block in `application.yml`; `+` the **base compose** `RABBITMQ_HOST` + `RABBITMQ_PORT` env; `+` the `com.app.listings.messaging` package (`RabbitConfig`, `MessageEntity`, `MessageRepository`, `MessageService`, `MessageController`, `MessageListener`, **Step 6**: `BrokerUnavailableException` + a **`@ExceptionHandler`** in TP1's `GlobalExceptionHandler`, **Step 7–8** optional: `MessageOutbox` + the `@EnableScheduling` config + `listings.messaging.outbox.interval-ms` key); Liquibase `v1.1-messages.yaml` + include in the changelog master; `README.md` updated with the **decision log** |
| **This repo** (`messaging/`) | `exercise.md` (this file), `README.md` (operational), `docker-compose.yml` (the RabbitMQ broker), `docs/verification.md` (the copy-paste `curl`s + `psql`). **No Java in this repo** — the pair's `../springboot` is the only place code is committed (single owner, one repo per artifact) |
| **`mvn clean verify`** in `../springboot` | **Green** |
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
**operational** [`README.md` → "0) Required base edit"](README.md), exactly
where the Keycloak and Monitoring TPs put their base edits. This appendix is
the **reference** for *what* the additions are and *why*.

| Addition | Coordinates / key | Why |
|---|---|---|
| Spring AMQP starter | `org.springframework.boot:spring-boot-starter-amqp` — **no version** (Boot 4.1.1 BOM pulls spring-amqp **4.1.1** + rabbitmq-client **5.30.0**) | provides **`RabbitTemplate`** (producer), **`@RabbitListener`** (consumer), **`RabbitAdmin`** (auto-declares queues/exchanges/bindings **idempotently**) |
| `application.yml` | `spring.rabbitmq.host` / `port` / `username` / `password` — **all** env-overridable (`${RABBITMQ_HOST:localhost}`, etc.) | the **host** default is for `mvn spring-boot:run`; the compose edit (next row) **overrides** `port` (5672) and **`host`** (the **network name** `rabbitmq`) at runtime |
| `../springboot/docker-compose.yml` | `listings-api.environment`: `RABBITMQ_HOST: rabbitmq` + `RABBITMQ_PORT: "5672"` | from **inside** the API container, `localhost` is the **container itself**; the app must reach the broker via its **network name** on the **internal** port |

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
  its configuration from `../springboot` (one owner per fact, §6.9 editorial).
- **No Java in this repo** — `messaging/` ships the **statement**, the
  **broker compose**, and the **verification `curl`s**; the **java** lives in
  `../springboot` (like the **Keycloak** `realm/` ships the **realm JSON**).

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

