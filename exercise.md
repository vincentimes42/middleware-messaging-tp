# TP Messaging — asynchronous seller↔buyer messaging (RabbitMQ)

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

**Validation method (the contract):** each step's **`Expected behavior`** is
the contract. Its **`Verify`** section (Step 9: **`Final checks`**) is the
definition of done: one section of [`docs/verification.md`](docs/verification.md)
to run (`curl` on the REST endpoint and the RabbitMQ **management API**, `psql`
on the `message` table, `grep` of your Java), plus the few checks the contract
cannot show. Java tests are optional and not part of the deliverable. The
**code** lives in **your Spring Boot (TP1) repo** (a new
`com.app.listings.messaging` package); **this repo** (`messaging/`) only owns
the statement, the broker compose, and the verification commands.

New to message queues or AMQP? Read [`docs/primer.md`](docs/primer.md) first
(5 minutes).

> **Prerequisite: TP1 is done and green.** The TP1 API passes its core
> checklists (register, CRUD, ownership 401/403/404, `?version=V1|V2`) and runs
> on port 8080. Finish TP1 first — this TP only adds a messaging layer on top.

### How to read a step

| Section | What it gives you |
|---|---|
| **Outcome** | the observable result |
| **Why this step** | the concept taught and the real problem it solves |
| **How it fits** | where the message goes (API, broker, queue, consumer, database) and which part you change |
| **Build it** | the changes, grouped when the step is long, in order; the `→` line under each item says what breaks without it |
| **Checkpoint** | a quick call to confirm a group works before starting the next one |
| **Expected behavior** | the contract: statuses, queue counters, rows, rules |
| **Verify** | the definition of done: the [`docs/verification.md`](docs/verification.md) section to run, plus the checks the contract cannot show |
| **If you are stuck** | folded: open it only when blocked; pointers to the class, annotation, or documentation |

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
| What persists the conversation | — | a **`message` table** (Liquibase **additive** `v1.2-messages.yaml`), plus the broker's own queue state |
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

### The pieces of this TP, in one picture

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

## Scope

| Track | Steps | Target (2nd-session binôme) |
|---|---|---|
| **Core** (required — must fit the 8h) | 1 · 2 · 3a · 3b · 4 · 5 · 6 | ~5h45 |
| **Bonus** (start only when the core is green) | 7 (at-least-once / redelivery) · 8a-8b (transactional outbox) | ~1h45 |
| **Delivery** | 9 (final integration) | ~0h15 |

Bonus steps are optional but count for the grade if you deliver them.

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
- **Queues (all durable):** main `listings.messages` (**quorum** queue) · DLQ
  `listings.messages.dlq` (classic). **DLX:** `listings.messages.dlx` (direct).
- **Queue args on the main queue:** `x-queue-type: quorum`
  + `x-delivery-limit: 3`
  + `x-dead-letter-exchange: listings.messages.dlx`
  + `x-dead-letter-routing-key: listings.messages.dlq`.
  A **quorum** queue is required: RabbitMQ 4 rejects `x-delivery-limit` on a
  classic queue (`PRECONDITION_FAILED`), and the app then fails to start.
- **Message body (the wire format):** a **single JSON document**
  ({messageId (uuid), listingId (long), fromUserId (long), toUserId
  (long), inReplyToMessageId (uuid, nullable), body (string), sentAt (ISO-8601)})
  — the **routing** is decided by the **producer** (a constant routing key
  `listings.messages`), NOT by the message content.
- **DB table (Liquibase additive):** `message` (see
  `v1.2-messages.yaml`). `message_id` is a **unique** PK / idempotency key —
  a duplicated `messageId` is a **NO-OP** on the consumer side (the `@RabbitListener`
  upserts by `messageId`, so `at-least-once` redelivery does **not** create
  a second row). `status` ∈ `PENDING | SENT | DELIVERED`.
- **REST paths:** `POST /listings/{id}/messages` · `GET /listings/{id}/messages`.
- **Auth:** HTTP Basic (TP1, unchanged).
- **Test accounts** (TP1 accounts, password `secret123`): `alice@test.org`
  owns listing `1` (the **seller**), `bob@test.org` is the **buyer**,
  `carol@test.org` is a non-participant. Create them with TP1's
  `POST /auth/register`, and listing `1` as alice.
- **Language of the message body:** a **plain string** (no JSON schema beyond
  the envelope) — the TP1 "JSON + XML content negotiation" (Step 4 bonus of
  TP1) is **not** part of this TP.
- **Error JSON shape:** identical to TP1 (`timestamp`, `status`, `error`,
  `message`, optional `fields`). Every status and its trigger:
  [Appendix E](#appendix-e--status-codes).
- **Rebuild after every Java change:** in the TP1 repo,
  `docker compose up -d --build listings-api` (the container runs the image
  built earlier).

---

## Step 1 — Boot the stack and confirm the connection   CORE

**Outcome** — The broker is **healthy**, the TP1 app is **up and connected**,
`mvn clean verify` stays **green**, and **no queue exists yet** (declared in
Step 2). This is your **bootstrap**: the 5 checks below must pass **before**
you write any AMQP code.

**Why this step** — The broker is a second piece of infrastructure the API
now depends on. Checking that both run and that the API can reach it, before
any AMQP code, means a later failure points at your code, not at the setup.

### How it fits

```
../springboot/docker-compose.yml           messaging/docker-compose.yml (this repo)
  listings-api      :8080                    rabbitmq  :5673 (AMQP)  :15673 (management UI + API)
  listings-postgres :5435                    user listings / listings
     └── RABBITMQ_HOST=rabbitmq, RABBITMQ_PORT=5672 ──► rabbitmq:5672 (network `middleware`)
```

### Build it

1. **Apply** the TP1-repo edits (if not already done) — exact code in
   [`README.md` §0](README.md). Without these, the next steps cannot pass.
   → Without the AMQP starter the app has no broker client at all; without
   `RABBITMQ_HOST=rabbitmq` it looks for the broker on `localhost`, which
   inside the API container is the API itself.
2. **Start the broker first**, then the app, in that order
   (README §1 → §2).
   → The app opens its broker connection at startup; with the broker first,
   the boot is clean instead of a series of retries.

### Expected behavior
- `rabbitmq` is **healthy** in `docker compose ps` (`rabbitmq-diagnostics ping` passes).
- `GET http://localhost:15673/api/overview` → **200** with Basic `listings:listings`; → **401** without.
- `GET http://localhost:8080/actuator/health` → **200**.
- `mvn clean verify` in the TP1 repo → **BUILD SUCCESS** (the TP1 smoke test does **not** touch the broker — Postgres only).
- `GET /api/queues` with credentials → **200, empty list** (queues arrive in Step 2).

### Verify

- [ ] [`docs/verification.md` §5](docs/verification.md): every result matches *Expected behavior*
- [ ] `cd "$TP1_DIR" && mvn clean verify` → **BUILD SUCCESS**

<details>
<summary><strong>If you are stuck</strong></summary>

- If `rabbitmq` is **restarting / unhealthy** → run `docker compose -f <compose> logs rabbitmq` (the broker persists to the named volume `rabbit-data`; `docker compose down -v` **wipes** it).
- If the app **fails to start** because it cannot find the broker → check that `RABBITMQ_HOST=rabbitmq` and `RABBITMQ_PORT=5672` are set in the **TP1 repo's** `docker-compose.yml` (README §0, edit c). From inside the container, the broker is `rabbitmq:5672` (the **internal** port), **not** `localhost:5673` (the **host** port).
- Boot order: **broker first** (README §1 → §2). If the app boots first, it will retry — it will still find the broker, but the clean sequence is RabbitMQ → app.
- Exact commands: [`docs/verification.md` §5](docs/verification.md) (broker health + app actuator).

</details>

---

## Step 2 - Declare the AMQP topology   CORE

**Outcome** — After the app boots, the broker contains **two durable queues**
(`listings.messages`, `listings.messages.dlq`), **two `direct` exchanges**
(`listings.direct`, `listings.messages.dlx`), and **two bindings** — all
**declared by your code**, **idempotent** across restarts, **no consumer
attached yet** (that is Step 4).

**Why this step** — In AMQP the producer never writes to a queue: it sends to
an **exchange**, and **bindings** decide which queue receives the message.
Declaring this routing in code makes it reproducible, and the dead-letter
wiring prepared here is what Step 5 relies on.

### How it fits

```
producer ──► exchange listings.direct ── binding (rk listings.messages) ──► queue listings.messages
                                                                              │ args: x-dead-letter-exchange,
                                                                              │       x-dead-letter-routing-key, x-delivery-limit
                                                                              ▼ (rejected / too many deliveries)
             exchange listings.messages.dlx ── binding (rk listings.messages.dlq) ──► queue listings.messages.dlq
```

### Build it

1. **Create** the file
   `com.app.listings.messaging.config.RabbitConfig` (`@Configuration`)
   declaring **six** beans:
   - a `DirectExchange` named **`listings.direct`**
   - a durable **quorum** `Queue` named **`listings.messages`** with the
     **exact** arguments from `Conventions` (`x-delivery-limit: 3` and the two
     `x-dead-letter-*`)
   - a durable `Queue` named **`listings.messages.dlq`** (plain — no
     dead-letter args of its own, **it is** the dead-letter sink)
   - a `DirectExchange` named **`listings.messages.dlx`** — **a separate
     exchange** (not the same as `listings.direct`)
   - a `Binding` from `listings.direct` → `listings.messages` with routing
     key **`listings.messages`**
   - a `Binding` from `listings.messages.dlx` → `listings.messages.dlq` with
     routing key **`listings.messages.dlq`**

   → Spring's `RabbitAdmin` declares every `Queue`, `Exchange` and `Binding`
   bean on the broker when the app connects. A message sent to an exchange
   with no matching binding is **silently dropped**; without the
   `x-dead-letter-*` arguments, rejected messages are dropped too. A wrong
   argument (e.g. `x-delivery-limit` on a classic queue) is refused by the
   broker and the app does **not** start.

2. **Configure** a `RabbitTemplate` bean with
   `setExchange("listings.direct")` (the exchange used when a send names no
   exchange; Boot's equivalent property is `spring.rabbitmq.template.exchange`).
   This is what lets the Step 3a
   producer call `convertAndSend("listings.messages", msg)` with **only**
   the routing key — the exchange is the **named** one, not the
   anonymous `$default`.
   → Without it, `convertAndSend(routingKey, msg)` uses the default
   exchange, which routes by queue name and bypasses `listings.direct` and
   its binding.

3. **Restart** the app (`docker compose restart listings-api`), **open a
   first broker connection** (`curl -s localhost:8080/actuator/health`: its
   `rabbit` indicator connects), then **verify** the two queues, two
   exchanges, and two bindings in the management UI (or via `curl`, §6).
   → The declarations run when the app opens its first broker connection,
   and Spring AMQP connects lazily: until Step 4 adds a listener (which
   connects at startup), nothing connects at boot, and the topology is not
   there yet.

> **Why declare in code (not in the UI):** the management UI is a **view**,
> not a **source of truth**. A fresh `docker compose down -v` would wipe
> anything hand-drawn in the UI. Declared in `RabbitConfig`, the topology
> re-emerges on every boot — *the topology is code*.

### Expected behavior
- `GET /api/exchanges?name=listings.direct` → exchange **present**, `type=direct`.
- `GET /api/queues/%2F/listings.messages` → `type=quorum`, `durable=true`, `arguments` contains `x-delivery-limit` and the **two** `x-dead-letter-*` keys.
- `GET /api/queues/%2F/listings.messages.dlq` → `durable=true`, `consumers=0`.
- Binding `listings.direct → listings.messages` (routing key `listings.messages`) — **present** in `GET /api/exchanges/%2F/listings.direct/bindings/source`.
- `docker compose restart rabbitmq` then re-check: the **same** two queues **re-emerge** (durable + persisted in the volume).
- **No consumer attached** yet (`consumers = 0` on the main queue) — that is Step 4.

### Verify

- [ ] [`docs/verification.md` §6](docs/verification.md): every result matches *Expected behavior*
- [ ] `grep -n "@Bean" src/main/java/com/app/listings/messaging/config/RabbitConfig.java` — the six topology beans (plus the `RabbitTemplate` of item 2) are **declared in code**, none drawn in the UI

<details>
<summary><strong>If you are stuck</strong></summary>

- A `direct` exchange in Spring AMQP is **`org.springframework.amqp.core.DirectExchange`** (or `new Exchange("listings.direct", BuiltinExchangeType.DIRECT, true, false, null)`). The name **must** be literal `listings.direct` (stable, greppable).
- `QueueBuilder` has a method for each argument: `durable(…)`, `quorum()`, `deliveryLimit(…)`, `deadLetterExchange(…)`, `deadLetterRoutingKey(…)`. Reference: <https://docs.spring.io/spring-amqp/reference/4.1/index.html>
- App stuck in a loop of `PRECONDITION_FAILED - invalid arg …` at startup: the broker refuses an argument. A queue that already exists with **other** arguments is refused too (`inequivalent arg`): delete it in the management UI, then restart the app.
- The **DLQ** is a **plain** durable queue — no dead-letter args of its own (it **is** the sink).
- The **DLX** is its **own** direct exchange named `listings.messages.dlx` — **not** the same object as `listings.direct`.

</details>

## Step 3a — Publish: buyer asks, seller not yet online   CORE

**Outcome** — A `POST /listings/{id}/messages` from a buyer returns **202
`SENT`** immediately (the broker **holds** the message), **no consumer is
required yet**, and a `message` row shows up in Postgres.

**Why this step** — The API becomes a **producer**: it accepts the request,
stores it, and hands it to the broker without waiting for anyone to process
it. That is why the answer is **202 Accepted** (taken in charge), not
**201 Created** (done).

### How it fits

```
POST /listings/{id}/messages (Basic)
   │
   ▼
MessageController (group C) ──► MessageService.send (group B)
                                   ├─ validate (404, 422), resolve toUserId, new messageId (UUID)
                                   ├─ INSERT message row, status SENT ──► Postgres `message` (group A)
                                   └─ convertAndSend("listings.messages") ──► listings.direct ──► queue
                                                                               ready +1, consumers 0
◄── 202 { messageId, status: SENT }
```

### Build it

**A. Storage — where a message lives**

1. **Create** the Liquibase change set
   `src/main/resources/db/changelog/v1.2-messages.yaml` — **one** additive
   `createTable` for `message` with a **unique** `message_id` (UUID) column,
   plus the include line in `db.changelog-master.yaml`. Do **not** touch
   `v1.0-init.yaml` (TP1 rule).
   → This table is the conversation's record, independent of the broker.
   The **unique** `message_id` is what makes the Step 4 consumer idempotent.

2. **Create** `com.app.listings.messaging.MessageEntity` — a JPA `@Entity
   @Table(name="message")` matching the `Conventions` (columns
   `messageId`, `listingId`, `fromUserId`, `toUserId`,
   `inReplyToMessageId`, `body`, `status` with enum
   `MessageStatus { PENDING, SENT, DELIVERED }`, `sentAt`).
   → The Java view of the table. Store `status` as text: a value added
   later (`PENDING`, Step 8a) then needs no migration.

3. **Create** `com.app.listings.messaging.MessageRepository` —
   `JpaRepository<MessageEntity, Long>` with one query:
   `Optional<MessageEntity> findByMessageId(String messageId)`.
   → Looking a row up by the producer's id (not the database id) is what the
   consumer needs to update the right row.

> **Checkpoint** — rebuild; the app starts and
> `docker compose exec listings-postgres psql -U listings -d listings -c '\d message'`
> shows the table with its unique `message_id`.

**B. Producer — the rules and the publish**

4. **Create** `com.app.listings.messaging.MessageService` (interface +
   impl) with one method `send(...)` that:
   - takes `(listingId, callerId, inReplyToOrNull, body)`
   - **resolves** `toUserId`: if `inReplyTo` is set, take the **`from_user_id`**
     of that message; otherwise take the **listing's owner**
   - **validates**: `fromUserId != toUserId` (else 422), `body` non-blank
     (else 422), `inReplyTo` exists and belongs to the same listing (else 422),
     listing exists (else TP1 `ResourceNotFoundException`). The blank body
     is a DTO constraint (`@NotBlank`, TP1's 422); the other two rules need
     the database: throw your own exception and map it to a 422 with the
     same JSON shape (`fields`) in `GlobalExceptionHandler`
   - **generates** a `messageId` via `UUID.randomUUID().toString()`
   - **persists** the row (`status=SENT`, `sentAt=now`), **committed**
     before the publish (do not put `send` in one transaction)
   - **publishes** with `rabbitTemplate.convertAndSend(RK, jsonBody)`, the
     JSON of a small envelope DTO (e.g. `messaging.mq.MqMessage`, the
     `Conventions` fields; Step 4 parses the same DTO) — the
     routing key is the **literal** string `listings.messages` (do **not**
     pass the exchange, `setExchange` is already configured in Step 2)

   → All the conversation rules live in one place. The id is created
   **before** publishing, by the producer: the broker gives none, and the
   same id travels in the message, the row, and the 202 answer. A row
   committed only after the publish (one transaction) races with the Step 4
   consumer: it can receive the message, find no row, insert one, and the
   producer's commit then fails on the unique `message_id`.

**C. HTTP — the endpoint**

5. **Create** `com.app.listings.messaging.MessageController` — a `@PostMapping`
   at `/listings/{id}/messages` that uses TP1's Basic auth and calls
   `MessageService.send(...)`.
   → The caller is always the authenticated user, never a field of the body:
   otherwise anyone could write in someone else's name.

### Expected behavior

**The one rule** — `fromUserId` is **always the caller** (the HTTP-Basic
identity, **never** the body). `toUserId` is **always explicit**: the
`from_user_id` of the `inReplyTo` message when **replying**, the **listing's
owner** when **opening**. A self-message (`fromUserId == toUserId`) is a
**422** — the **only** case where an *opening* message (no `inReplyTo`) is
rejected.

| Caller (Basic) | **POST — open** (no `inReplyTo`) | **POST — reply** (`inReplyTo` = a real msg on this listing) | **POST — `inReplyTo`** unknown / foreign | **POST — empty `body`** |
|---|---|---|---|---|
| **Anonymous** | **401** | **401** | **401** | **401** |
| **BUYER** (non-owner of this listing) | **202 SENT** (to = the owner) | **202 SENT** (to = the person you reply to) | **422 `inReplyTo`** | **422 `body`** |
| **SELLER** (owner of this listing) | **422** (an opening message to yourself → `from == to`) | **202 SENT** (replying to a buyer → to = that buyer) | **422 `inReplyTo`** | **422 `body`** |
| **SELLER** (owner of a *different* listing = a buyer here) | **202 SENT** (to = this listing's owner) | **202 SENT** | **422 `inReplyTo`** | **422 `body`** |

**The observable side-effects** (for each `202`):
- A **new row** in Postgres `message` **immediately** (`status=SENT`, `sentAt=now`, `fromUserId`/`toUserId` correct).
- **`ready` +1** on `listings.messages` in the management UI — the broker is **holding** it (no consumer in Step 3a).
- The **`messageId`** in the `202` body **is** the `message_id` of the new row
  (a **UUID** generated by the **producer**, **not** by the broker).
- **`ready` is cumulative** — a 2nd buyer `POST` on the same listing does **not** replace the 1st; `ready = 2`, two rows in Postgres.

> **Note on the labels "buyer" / "seller"** — they are shortcuts. The
> **actual** check is `caller == listing.owner`: any TP1 account may **open**
> a question on a listing, and any account may **reply** to a message in its
> own thread. The only structural constraint is `from != to`. (In production,
> `toUserId` could be a **claim** — an `X-Actor` header, a `sub` claim, a
> Basic email — rather than derived. This TP keeps the derivation because it
> is the simplest contract that still fits in one row per turn.)

### Verify

- [ ] [`docs/verification.md` §7a](docs/verification.md): every result matches *Expected behavior*

<details>
<summary><strong>If you are stuck</strong></summary>

- The **`messageId`** is **client-generated** — `UUID.randomUUID().toString()` in the producer, before `convertAndSend`. The **broker** does **not** assign it (that would break **idempotency** in Step 4).
- **The 202 happens BEFORE the consumer exists.** The broker holds it. This is a network write to RabbitMQ — there is **no** synchronous call to any other Java code.
- For the **wire body**, use Spring Boot's Jackson 3 mapper (inject `tools.jackson.databind.json.JsonMapper`) to serialize a small DTO (the envelope in `Conventions`: `messageId`, `listingId`, `fromUserId`, `toUserId`, `inReplyToMessageId`, `body`, `sentAt`). The **`toUserId`** is written into the body so the Step 4 consumer does **not** need to re-lookup the listing.

</details>

---

## Step 3b — Read the thread   CORE

**Outcome** — `GET /listings/{id}/messages` returns the conversation: the
listing owner sees the whole thread, a participant sees only the turns they
took part in, a stranger gets an empty list.

**Why this step** — A thread is private to its participants. The read path
filters by caller, and a stranger gets an empty list rather than a 403, so
the API does not even reveal that a conversation exists.

### Build it

1. **Add** to `MessageController` a `@GetMapping` at `/listings/{id}/messages`
   that reads via the repository and filters to the caller's turns.
   → Without the filter, any authenticated account reads every buyer's
   questions.
2. **Protect** the path in `SecurityConfig`: a rule
   `/listings/*/messages` → `authenticated()`, **before** TP1's public
   `GET /listings/**` (first match wins).
   → TP1 makes every `GET` under `/listings` public: without the rule, an
   anonymous `GET` reaches your controller with no user, instead of a 401.

### Expected behavior

A non-participant GET returns **`200 []`**, **not 403** — the thread is not
*leaked*; a stranger simply has no view into it.

| Caller (Basic) | **GET /listings/{id}/messages** |
|---|---|
| **Anonymous** | **401** |
| **BUYER** (non-owner of this listing) | **200** — only the turns they took part in |
| **SELLER** (owner of this listing) | **200** — the whole thread |
| **SELLER** (owner of a *different* listing = a buyer here) | **200** — **0 items** (not their listing) |

- Turns come in ascending `sentAt`.
- **`GET` is idempotent** — the same call twice returns the same set of turns
  (the read does **not** change `status` or `ready`).

### Verify

- [ ] [`docs/verification.md` §7b](docs/verification.md): every result matches *Expected behavior*

<details>
<summary><strong>If you are stuck</strong></summary>

- For the **thread read**, the simplest is a **single** JPQL query with a `WHERE (from_user_id = ?1) OR (to_user_id = ?1)` filter, sorted by `sent_at ASC`. Two repository calls + a union also work.

</details>

---

## Step 4 — Consume, persist, and reply   CORE

**Outcome** — The app **registers a `@RabbitListener`** on `listings.messages`;
when the broker delivers a published message, the row in `message` is
**upserted** by `message_id` from `SENT` to `DELIVERED` (with
`delivered_at=now`); the seller's `GET` now shows **both sides** of the
thread. A **redelivered** message (duplicated `messageId`) is a **no-op**.

**Why this step** — The API becomes its own **consumer**. A broker
guarantees *at-least-once* delivery: the same message may arrive twice. The
consumer must therefore be **idempotent** — processing a message a second
time changes nothing.

### How it fits

```
queue listings.messages ──► @RabbitListener MessageListener.onMessage (item 3)
                               ├─ parse JSON ──► MqMessage (item 1)
                               ├─ upsert by message_id: SENT ──► DELIVERED, delivered_at (item 2)
                               ├─ known business problem ──► log.warn, return (message acked, removed)
                               └─ unexpected error ──► exception ──► rejected (Step 5: DLQ)
```

### Build it

1. **Reuse** (or create) the small envelope **DTO** of Step 3a, e.g.
   `com.app.listings.messaging.mq.MqMessage` — fields matching the wire
   envelope in `Conventions` (`messageId`, `listingId`, `fromUserId`,
   `toUserId`, `inReplyToMessageId`, `body`, `sentAt`). This is the **parse
   target** for the listener; it is **not** the JPA `MessageEntity`.
   → The wire format and the database table evolve separately; mixing them
   ties a broker message to a JPA mapping.

2. **Add** the field **`deliveredAt`** (type `Instant`) to
   `com.app.listings.messaging.MessageEntity`, and the column with a **new**
   change set in `v1.2-messages.yaml` (`addColumn`, `timestamp WITH TIME
   ZONE`, **nullable**). The Step 3a row creation leaves it `null`, the
   Step 4 upsert **sets** it.
   → It records *when* the consumer processed the message, which is how you
   measure the delay a queue introduces. Nullable, because the row exists
   (SENT) before the consumer sees it. A new change set, not an edit of the
   Step 3a one: that one is already applied, and Liquibase refuses to start
   when an applied change set changed (checksum) — migrations stay additive
   (TP1 rule).

3. **Create** `com.app.listings.messaging.MessageListener` — a `@Component`
   with **one** method:
   ```java
   @RabbitListener(queues = "listings.messages")
   public void onMessage(String body) { ... }
   ```
   The method does **3 things, in this order**:
   1. **Parse** the raw JSON `String` into `MqMessage` (with Spring Boot's
      Jackson `JsonMapper`). **A parse error is a *poison* message**: it is
      handled in Step 5.
   2. **Upsert** the row: `findByMessageId(mmq.getMessageId())` → if present,
      set `status=DELIVERED`, `deliveredAt=now`, then `save`; if absent,
      build a **new** `MessageEntity` from the wire fields and `save`.
   3. **Catch** **only** known **business** errors (missing buyer, orphan
      `inReplyTo`, etc.) with `log.warn("skipping messageId={} reason={}",
      mmq.getMessageId(), ex.getMessage())` and **return** — the **ack is
      automatic**, so the message is **removed** from the queue.
   4. **Do NOT catch** **unexpected** errors (NPE, SQL, …) — let them
      bubble. Spring AMQP then **requeues** the message (its default); the
      quorum queue counts the attempts and, past `x-delivery-limit: 3`,
      dead-letters it to the DLQ.

   → Without a listener the queue only grows. The upsert by `messageId`
   makes a second delivery harmless; the warn-and-return rule removes
   messages the code knows it must skip, while unexpected errors are retried
   a few times and then parked in the DLQ instead of looping forever.

4. **Restart** the app and verify the listener is attached
   (`GET /api/queues/%2F/listings.messages` → `consumers = 1`).
   → `consumers = 1` is the broker's own proof that your listener is
   registered; messages queued in Step 3a are delivered right away.

### Expected behavior

- **`consumers = 1`** on `listings.messages` (with the app up).
- **After a buyer `POST` (Step 3a)**, within ~2 s, `psql SELECT status FROM
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
- **Poison messages** (a body that cannot be read as an `MqMessage`) end up
  in `listings.messages.dlq`: in this step after the first delivery and 3
  redeliveries (`x-death` reason `delivery_limit`), immediately once Step 5 is
  done (`rejected`).

### Verify

- [ ] [`docs/verification.md` §8](docs/verification.md): every result matches *Expected behavior*
- [ ] **Duplicate delivery**: from the **management UI**, **re-publish** the **exact same** JSON body to routing key `listings.messages` → `count(*)` for that `message_id` stays **1**

<details>
<summary><strong>If you are stuck</strong></summary>

- The **`@RabbitListener`** import is `org.springframework.amqp.rabbit.annotation.RabbitListener`. The **queue name must be the literal string** `listings.messages` (**not** a constant import) — keeping it greppable against `Conventions`.
- The **method parameter** is either a **raw `String`** (the JSON body directly) or Spring AMQP's **`org.springframework.amqp.core.Message`** (`getBody()` returns the bytes). With `String`, parse it with the **Jackson 3** `JsonMapper` bean (`tools.jackson.databind.json.JsonMapper`): Spring Boot 4 configures Jackson 3; a `com.fasterxml.jackson…ObjectMapper` is only on the classpath by accident (through springdoc) and is not configured by Boot.
- **Idempotency** — upsert by `messageId` **from the wire body**, **not** by the Postgres-generated `id`. `findByMessageId` is **Step 3a's** repository method; the pattern is **"SELECT + INSERT-or-UPDATE"** in the listener (Spring Data JPA `save()` + the prior `find`).
- The **`deliveredAt`** column must be **nullable** — the row exists in `SENT` state (without a timestamp) before the consumer touches it.
- **Do NOT `throw`** for **known** business cases: return normally, the message is acknowledged and removed (that is the warn-and-skip rule). **DO let unexpected errors bubble**: requeue + `x-delivery-limit` send them to the DLQ after 3 attempts.
- Reference: <https://docs.spring.io/spring-amqp/reference/4.1/amqp/receiving-messages/async-annotation-driven.html>
- The reply round-trip walk: [`docs/verification.md` §8](docs/verification.md).

</details>

## Step 5 — Poison messages go to the dead-letter queue   CORE

**Outcome** — A single **poison** message (a wire JSON the consumer **cannot parse**)
**does not block** legitimate messages on the main queue, and ends up in the
**`listings.messages.dlq`** where the operator can **inspect it** in the
management UI. The **producer** is **not touched** — this is a **consumer-side**
contract.

**Why this step** — A message the consumer can never process (a *poison*
message) is redelivered again and again, and blocks the queue for everyone.
Dead-lettering moves it aside, where an operator can inspect it, so the
other messages keep flowing.

### How it fits

```
poison {"oops":true} ──► listings.messages ──► onMessage: parse fails ──► AmqpRejectAndDontRequeueException
                                                                              │
     queue arguments x-dead-letter-exchange / -routing-key (Step 2) ◄─────────┘
                                                                              ▼
                                   listings.messages.dlx ──► listings.messages.dlq (ready +1, inspectable)
```

### Build it

1. **Modify** `MessageListener.onMessage` — when the body cannot be read as
   an `MqMessage` (not JSON, **or** JSON without the envelope's required
   fields: Jackson ignores unknown properties, so `{"oops":true}` parses into
   an envelope full of `null`), log a `warn` (the delivery, never the body)
   and throw `org.springframework.amqp.AmqpRejectAndDontRequeueException`
   (without the parser exception as cause: its message quotes the body, and
   Spring's error handler logs the whole chain).
   → This exception tells Spring AMQP to **reject without requeue**: the
   broker applies the queue's `x-dead-letter-*` arguments at once. Any other
   exception is **requeued** (Spring AMQP's default): the poison would come
   back 3 times before `x-delivery-limit` parks it.
2. **Keep** the listener's signature and the default acknowledge mode: no
   `Channel` parameter, no manual `basicAck` / `basicNack`.
   → In the default mode (`AUTO`) Spring acknowledges after your method
   returns. Calling `basicNack` yourself acknowledges the same delivery twice:
   the broker closes the channel with `PRECONDITION_FAILED - unknown delivery
   tag`.
3. **Do NOT** reject for **known** business cases — those stay
   "warn-and-return" per Step 4.
   → The DLQ is for what the code cannot handle. Sending known cases there
   would fill it with messages nobody needs to look at.

### Expected behavior

- **Before** the drill: `GET /api/queues/%2F/listings.messages.dlq` → `ready
  = 0`.
- **After** the drill (poison published, consumer ran): `listings.messages`
  `ready` is **decreased by 1** (the message **left** the main queue), and
  `listings.messages.dlq` `ready` is **+1** (the message **landed** in the
  DLQ).
- **After** the poison, a **new legitimate** `POST /listings/{id}/messages`
  still results in a `202`, a `DELIVERED` row in Postgres, and a `ready-1`
  decrement on the main queue — **one** bad message did **not** block the
  queue (the reject without requeue prevents the infinite loop).
- The app **does not crash** on a poison: `docker compose logs listings-api`
  shows the **expected** `log.warn` (or a swallowed exception trace), **not**
  a `500 stack trace`, **not** a `Restarting`.
- In the management UI (or `POST /api/queues/%2F/listings.messages.dlq/get`),
  the DLQ message is **visible** — its body, and an `x-death` header with
  `reason: rejected` and the original queue `listings.messages`.
- The **producer** (Step 3a) is **unchanged**. The DLQ is **purely** on the
  consumer side.

### Verify

- [ ] [`docs/verification.md` §9](docs/verification.md) (publish the poison, read both queues, peek the DLQ): every result matches *Expected behavior*
- [ ] A **new legitimate** `POST /listings/1/messages` still `202`s and `DELIVERED`s within 2 s (the queue is **not stuck**)

<details>
<summary><strong>If you are stuck</strong></summary>

- `AmqpRejectAndDontRequeueException` is in `org.springframework.amqp`. Spring AMQP's error handling, including which exceptions are requeued: <https://docs.spring.io/spring-amqp/reference/4.1/amqp/exception-handling.html>
- A poison that loops (`deliveries` climbing in the UI, the same warn line thousands of times) means the exception thrown is requeued: check it is `AmqpRejectAndDontRequeueException`, and that the main queue is a quorum queue with `x-delivery-limit`.
- `PRECONDITION_FAILED - unknown delivery tag` in the logs: a manual `basicAck`/`basicNack` in `AUTO` mode. Remove it.
- **Do not** set `requeue` behaviour globally to hide the problem: the explicit exception documents the decision in the code.
- `{"oops":true}` dead-lettered with `x-death` reason `delivery_limit` (not `rejected`): it parsed (unknown properties are ignored) and failed later. Check the required fields of the envelope right after the parse.

</details>

---

## Step 6 — Broker down: the API survives and returns 503   CORE

**Outcome** — When the **broker is down**, a `POST /listings/{id}/messages`
**fails fast** with a **clean 503** (TP1 JSON shape, no stack trace), the **REST
read** endpoints **keep working** (TP1's listing CRUD is **decoupled** from the
broker), and the app **does not restart**. When the broker **comes back**, the
`202` works again and **backlogged** messages in the **durable** queue
**drain** and **flip to `DELIVERED`**.

**Why this step** — Adding a broker adds a way to fail. The API must
**degrade**, not die: the write that needs the broker answers a clean,
fast 503 ("retry later"), and everything that does not need it keeps working.

### How it fits

```
broker stopped
POST /messages ──► MessageService.send ──► convertAndSend ──✗ AmqpConnectException
                        catch ──► BrokerUnavailableException (item 1-2)
                                       ──► GlobalExceptionHandler ──► 503, TP1 JSON shape (item 3)
GET /listings, /actuator/health ──► no broker on this path ──► 200
broker restarted ──► connection re-established ──► 202 again; the durable queue still holds older messages
```

> **Superseded by Step 8a (bonus)** — Step 8a replaces the **503** with a
> **`202 PENDING`** + a **poller**. The `@ExceptionHandler` is **kept** as a
> **safety net** for stray publishes outside `MessageService`. Do not assume
> the 503 remains the contract after Step 8a.

### Build it

1. **Add** a runtime exception
   `com.app.listings.messaging.exception.BrokerUnavailableException`
   (extends `RuntimeException`, **not** `AmqpException`). A useful message
   like `"rabbitmq not reachable — check docker compose / RABBITMQ_HOST /
   RABBITMQ_PORT"`.
   → A business-level signal: the controller and the error handler do not
   need to know AMQP's exception types.

2. **In `MessageService.send(...)`**, **wrap** the
   `rabbitTemplate.convertAndSend(...)` call: on any
   `org.springframework.amqp.AmqpException`, **delete the row** saved just
   before, then throw `BrokerUnavailableException`. The **controller** should
   **never** see an `AmqpException`; it should see **either** a `202` **or** a
   `BrokerUnavailableException`.
   → Uncaught, the AMQP exception reaches TP1's catch-all handler and the
   client gets a 500: "the server is broken" instead of "try again later".
   Catch the parent `AmqpException`: a stopped broker raises
   `AmqpIOException`, not only `AmqpConnectException`. Without the delete, a
   `SENT` row stays in the database for a message that was never published
   (the *dual write* problem that Steps 8a-8b solve properly).
3. **Set** `spring.rabbitmq.connection-timeout: 2s` (README §0).
   → The default connection timeout is **60 s**: the first request after the
   broker stops would hang for a minute before failing.

4. **In TP1's existing `GlobalExceptionHandler`**, **add** one handler for
   `BrokerUnavailableException` that answers **503** with TP1's JSON shape
   (`timestamp`, `status`, `error`, `message`; **no** `fields`), the content
   type set to JSON like the others.
   → 503 tells the client the failure is temporary and on the server side;
   keeping TP1's JSON shape keeps one error format for the whole API.

5. **Run the drill** (the Proof checks below) to prove it.
   → A failure path is only proven by causing the failure: stop the broker
   for real.

### Expected behavior

The 6.1 → 6.7 sequence is the **state machine** of the step. **Every** row
must hold:

| # | Scenario | Expected |
|---|---|---|
| 6.1 | With the broker **up**, `POST /listings/{id}/messages` | **202** with `status=SENT` (the Step 3a contract is **unchanged** in the happy path) |
| 6.2 | `docker compose stop rabbitmq` (broker container **stopped**; **API stays up**), then `POST /listings/{id}/messages` | **`503`**, body in the **TP1 JSON shape** (`timestamp`, `status`, `error`, `message`), the `message` field **mentions** the broker (e.g. `"rabbitmq not reachable — check docker compose / RABBITMQ_HOST / RABBITMQ_PORT"`); **not** 500, **not** a stack trace |
| 6.3 | **Any other** TP1 endpoint during the outage (e.g. `GET /listings`, `GET /listings/{id}`, the TP1 register/CRUD) | **200** — the **HTTP read** path is **not** affected by the broker outage (TP1's business endpoints are **decoupled** from the broker) |
| 6.4 | `GET /actuator/health/liveness` during the outage | **200**: the app is **alive**. The aggregate `GET /actuator/health` is **503** (`DOWN`): Spring Boot includes a `rabbit` health indicator, and a dependency is down. Both answers are correct — they answer different questions |
| 6.5 | `docker compose start rabbitmq` → wait for `healthy` | `POST /listings/{id}/messages` → **202** again (the connection factory reconnects; no code change, no app restart) |
| 6.6 | **Backlog check** — messages in `listings.messages` that were **ready before the stop** **are still there** after the start (durable queue, data in the `rabbit-data` volume). Within ~10 s the consumer reconnects and **drains** them → the rows flip from `SENT` → `DELIVERED` in Postgres | the **count of `DELIVERED`** rows equals the **count of published** messages (plus any new ones); **no** row left `SENT` by a failed 503 call |
| 6.7 | **Speed** of the 503 response | **a few seconds** (a small multiple of the 2 s connection timeout; never the 60 s default). An `AmqpException` propagating out of the controller as a **`500`** is a bug — it **must** be caught in `MessageService` and re-thrown as `BrokerUnavailableException` → mapped to **503** by the `@ExceptionHandler` |

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

- [ ] [`docs/verification.md` §10](docs/verification.md) (stop → 503 → start → 202 → drain): rows 6.1 to 6.7 all hold
- [ ] The 503 `POST` answers in a few seconds (`curl -w "%{time_total}"` well under 60 s), and leaves **no** new row in `message`

<details>
<summary><strong>If you are stuck</strong></summary>

- The **exception to throw** is the **new** `BrokerUnavailableException`, **runtime** (not `AmqpException`). The `@ExceptionHandler` returns **503**, **not** 500 — and the **JSON shape** is **exactly** TP1's existing handler shape.
- A `500` with `AmqpIOException` in the message: your `catch` names `AmqpConnectException` only. Catch `AmqpException`.
- `/actuator/health` at 503 during the drill is expected (the `rabbit` indicator); `/actuator/health/liveness` is the "is the app alive" probe. Reference: <https://docs.spring.io/spring-boot/4.1/reference/actuator/endpoints.html>
- The 503 is a **real** contract, not a defensive one. In production, "broker down" is a **degradable** failure (queue and retry). This step is where your API **proves** it knows the **difference** from "the app crashed".
- **`messageId` is generated on the producer side** (Step 3a). During the outage, **nothing** is published, so there is **no** `messageId` to replay. Each new `202` (after restart) uses a **fresh** UUID.
- The **drain step** relies on Step 2's **durable** queue + Step 4's **`@RabbitListener` reconnect** (Spring AMQP does this **for** you via a `CachingConnectionFactory`).
- Full stop→503→start→202 walk: [`docs/verification.md` §10](docs/verification.md).

</details>

---

## Step 7 — At-least-once delivery and idempotency   BONUS

**Outcome** — Prove the **at-least-once** guarantee **in the database**: a
**redelivered** message (the broker crashed between `deliver` and `ack`, or
the consumer crashed **before** ack'ing) does **not** create a **second** row
in `message`. The **unique index + Step 4's upsert** is what makes this work.

**Why this step** — "At-least-once" is a promise *and* a warning: no
message is lost, but some arrive twice. This step makes the duplicate
happen on purpose and checks that the database still holds one row.

### How it fits

```
message delivered to the consumer ──✗ consumer stops before the ack
   └─ broker keeps it (unacked ──► ready again) ──► consumer restarts ──► delivered again (redelivered = true)
                                                                       └─ upsert by message_id ──► still 1 row
```

### Pre-req

- Steps 1–5 are **green** (the consumer is attached, the DLQ works, the
  producer is alive).
- The **broker is up** in this drill — only the **API** goes down.

### Build it

1. **Log the redelivery flag** — add a `@Header(AmqpHeaders.REDELIVERED)
   boolean redelivered` parameter to `MessageListener.onMessage` and write it
   in the listener's `info` line, with the `messageId` (never the body).
   → The broker sets this flag on a message it delivers a second time: it is
   the only direct evidence that a redelivery happened.
2. **Duplicate drill** (deterministic) — take the wire JSON of one delivered
   message (its envelope, rebuilt from the row) and **publish it 3 times**
   to `listings.direct` / `listings.messages` with the management API.
   → The broker cannot tell a duplicate from a new message; only your
   upsert by `messageId` keeps the table at one row.
3. **Crash drill** — publish a **burst** of envelopes (a loop over the
   management API, [`docs/verification.md` §11](docs/verification.md)) and
   **kill** the API during the burst
   (`docker compose kill listings-api`), then start it again.
   → Messages handed to the consumer but not yet acknowledged go back to the
   queue and are delivered again after the restart. `kill` (not `stop`)
   gives the app no time to finish them. If no `redelivered=true` line
   appears, the burst was drained before the kill: run it again with a larger
   burst.

### Expected behavior

- **Duplicate drill**: `SELECT count(*) FROM message WHERE message_id = '<id>'`
  → **1**, `status = DELIVERED`, however many times the envelope is published.
- **Crash drill**: every envelope of the burst has **exactly one** row; at
  least one `redelivered=true` line in the logs, for a `messageId` that still
  has one row.
- No `DuplicateKeyException` / unique-constraint error in the logs: the
  **unique** `message_id` never fires because the upsert looks the row up
  first.

> **Note on "producer side during the crash"** — in Step 7, the **API is
> stopped**, not the broker. So `POST /listings/{id}/messages` during the
> outage fails with "connection refused" **before** it reaches your 503
> handler. That is a *different* failure mode from Step 6 (broker down, API
> up).

### Verify

- [ ] [`docs/verification.md` §11](docs/verification.md) (duplicate drill, then crash drill): every result matches *Expected behavior*
- [ ] Crash drill: `docker compose logs listings-api | grep -c "redelivered=true"` → **≥ 1** (else retry with a larger burst)

<details>
<summary><strong>If you are stuck</strong></summary>

- `AmqpHeaders.REDELIVERED` is in `org.springframework.amqp.support`; `@Header` is `org.springframework.messaging.handler.annotation.Header`.
- **`count = 1` with `redelivered=true` in the logs** is the **whole point** of this step: the unique `message_id` (Step 3a) **and** the upsert (Step 4) together make "redelivery without duplication" possible.
- `docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged` shows the same counters as the UI from the CLI; `messages_unacknowledged > 0` during the burst means messages are in flight.
- **`docker compose kill`/`stop`** (not `down -v`) keep the broker's named volume: the durable queue and its contents **survive**.

</details>

---

## Step 8a — Outbox, producer side: 202 PENDING when the broker is down   BONUS

**Outcome** — **The buyer always gets 202**, **regardless of the broker
state**: `SENT` if the broker is up (the **fast path**, the outbox is
**invisible**), `PENDING` if the broker is down. The message is saved in your
**own** Postgres `message` table either way.

**Why this step** — Writing a row *and* publishing a message are two
systems that cannot share one transaction (the "dual write" problem). The
**outbox** pattern writes only to the database in the request, and a
background job moves rows to the broker, so a broker outage delays messages
instead of losing them.

### How it fits

```
POST /messages ──► MessageService.send: INSERT row PENDING ──► try publish ──┬─ ok ──► UPDATE SENT ──► 202 SENT
                                                                             └─ broker down ──► 202 PENDING
@Scheduled MessageOutbox.drain (every 5 s)
   └─ for each PENDING row: claim (UPDATE … WHERE status = PENDING) ──► publish ──► Step 4 consumer ──► DELIVERED
                                       └─ 0 rows affected: another instance has it ──► skip
```

> **Overrides Step 6 on the publish path** — the **503** contract is
> **replaced** by **`202 PENDING`** (a **stronger** contract, from the
> buyer's POV: "your message is queued" beats "broker unavailable"). The **Step 5**
> (DLQ) contract is **not** overridden. The **Step 6**
> `@ExceptionHandler(BrokerUnavailableException)` is **kept** as a **safety
> net** for any stray `convertAndSend` **outside** `MessageService`.

### Build it

**A. Data — a status that can say PENDING**

1. **Verify** `v1.2-messages.yaml` declared `status` as `VARCHAR(16)` (**not**
   a PG `enum`). If it is a PG enum, **change it** and **re-run** Liquibase
   (drop + re-create) — you cannot add `PENDING` to a PG enum without a
   separate migration (out of scope for this TP).
   → `PENDING` is a new value: with a text column it is just a new string.

**B. Producer — never fail the buyer because of the broker**

2. **In `MessageService.send(...)`**, **replace** the Step 6 `try / catch`
   so that the broker failure no longer reaches the caller: insert the row as
   `PENDING`, attempt the publish, set the row to `SENT` on success **only if
   it is still `PENDING`** (the conditional `UPDATE` of Step 8b), and leave
   it `PENDING` when the publish fails. The consumer may process the message
   before your update: an unconditional `SET status = SENT` would turn a
   `DELIVERED` row back to `SENT` for good. Return the same 202 body in both
   cases, with `status` = `SENT` or `PENDING`.
   → The buyer's message is saved in your database in every case; the broker
   state only changes *when* it is published.

### Expected behavior

| # | Scenario | Expected |
|---|---|---|
| 8.1 | **Broker down** → `POST /listings/{id}/messages` | **`202`** with `{ messageId, "status":"PENDING" }` (a **fresh UUID**, the row is **in the DB**, **not** in the broker); the **shape is the same** as 8.2 — the **`status`** field is the **only** difference |
| 8.2 | **Broker up** → same `POST` | **`202`** with `{ messageId, "status":"SENT" }` (row inserted **and** published in the **same** call); the **outbox is invisible** in the happy path — **that is the whole point** (the 202 looks the same, only the `status` differs) |

> **Note on 8.1 vs Step 6** — under **Step 6** the broker-down contract is
> "return **503**" (a **failure signal** to the caller). Under **Step 8a** it
> is "return **`202 PENDING`**" (a **success signal**, with a **queue** state
> the buyer can poll). Both are **legit** contracts — Step 8a is **stronger**
> because the **buyer cannot distinguish** a broker outage from a slow
> consumer (the **UX** is identical).

### Verify

- [ ] [`docs/verification.md` §12a](docs/verification.md): broker down → **202** with `"status":"PENDING"` (a **fresh UUID**), and the row is `PENDING` in Postgres, **not** in the broker
- [ ] **No 503** in `docker compose logs listings-api | grep -i 503` during the drill (`MessageService` returns PENDING instead of rethrowing)

<details>
<summary><strong>If you are stuck</strong></summary>

- If your **`v1.2-messages.yaml`** declared `status` as a **PG enum**, you **cannot** add `PENDING` without a **separate migration** — **change it back to a `VARCHAR(16)`** and **re-run** Liquibase (drop + recreate the table). The **schema choice** was **`VARCHAR`** for exactly this reason.

</details>

---

## Step 8b — Outbox, poller side: PENDING rows reach the broker   BONUS

**Outcome** — A **`@Scheduled` poller** re-publishes the `PENDING` rows within
~5 s of the broker coming back; each row is published **once**, even with two
API instances. A **broker outage** never loses a message.

**Why this step** — The outbox is only half done while rows wait in the
database. The poller moves them to the broker, and an atomic claim makes sure
two instances never publish the same row.

### Build it

**A. Poller — move PENDING rows to the broker**

1. **Create** `com.app.listings.messaging.MessageOutbox` — a `@Component`
   with one `@Scheduled` method, its delay read from
   `listings.messaging.outbox.interval-ms` (default 5000). On each run it
   loads the `PENDING` rows, oldest first, and for each row: **claims** it
   (item 2), skips it when the claim affected no row, publishes the wire
   JSON to `listings.messages`, and **reverts** the row to `PENDING` with a
   `warn` (the `messageId`, never the body) when the publish fails.
   → Without the poller, a `PENDING` row stays in the database forever.

2. **Add** to `com.app.listings.messaging.MessageRepository`: a derived query
   for the `PENDING` rows ordered by `sentAt`, and two modifying JPQL updates
   that return the number of affected rows: `claim` (`PENDING` → `SENT` for
   one `messageId`, **only if** it is still `PENDING`) and `revert` (back to
   `PENDING`).
   The **`WHERE … status = PENDING`** is what makes `claim()` **atomic**:
   under **two instances** racing on the **same** row, **only one** gets
   `affected = 1` (the rest get **0** and must **not** publish). If you
   write it as **`WHERE messageId = ?`**, both instances update (and both
   publish) — **double-publish race** — which is **the exact failure** 8.6
   guards against.
   → The claim is a database-level lock without a lock: only one poller can
   move a row out of `PENDING`.

**B. Scheduling — make the poller run**

3. **Enable scheduling** — in your **main** `@SpringBootApplication` (or in a
   separate `@Configuration`), put **`@EnableScheduling`** on it. Without this
   annotation, `@Scheduled` is **silently ignored**.
   → No error, no log: `PENDING` rows simply never leave the database.

4. **Add the property** `listings.messaging.outbox.interval-ms` to
   `application.yml` (with an env var, e.g.
   `${LISTINGS_MESSAGING_OUTBOX_INTERVAL_MS:5000}`) so the **polling
   interval** is tunable at deploy time.
   → The interval trades delay against database load; it is a deployment
   decision, not a code constant.

### Expected behavior

| # | Scenario | Expected |
|---|---|---|
| 8.3 | **The poller**: a `@Scheduled` method fires **every `interval-ms`** (default **5000** ms); for **each** `PENDING` row, **claims** it atomically (`UPDATE … WHERE status = 'PENDING'` — **before** the `convertAndSend`), **then** publishes to `listings.messages` | the **order is claim → publish**, not publish → claim. If you publish first, **two instances** on the **same** `PENDING` row both publish (the **race** is lost in the **air**, not in the DB) |
| 8.4 | **Recovery**: broker down → **3** calls → **3** `PENDING` rows. `docker compose start rabbitmq` → within ~5 s, **3** rows become **`SENT`** (the broker's `ready` is **+3**), then **`DELIVERED`** (the Step 4 consumer drains them — **unchanged**); **no re-publish** of an already-SENT row (the `WHERE status = 'PENDING'` is the **guard**) | the **count of `PENDING` rows** is exactly the **number of failures** (the **poller never claims SENT**) |
| 8.5 | **Idempotency** (Step 4 still applies): a redelivered row (the broker dropped the ack — classic) — the **consumer** (Step 4) **upserts** by `message_id` → **no duplicate row** | **`SELECT count(*) FROM message WHERE message_id = '<id>'` = 1**, regardless of how many times it was redelivered |
| 8.6 | **Multi-instance** (run **two** `listings-api` instances simultaneously — [`docs/verification.md` §12b](docs/verification.md), optional block — both with the **same** DB → both will have their **own** **pollers** on the **same** `PENDING` rows): exactly **one** of them publishes per row | **`count(*) = 1`** per `message_id` (no double-publish). The **`UPDATE … WHERE status = 'PENDING'`** is the **atomic claim** — the **loser** gets **0 rows affected** → skips the publish. This is **the entire reason** the **UPDATE is BEFORE the `convertAndSend`** |

### Verify

- [ ] [`docs/verification.md` §12b](docs/verification.md) (broker up → poller → SENT → DELIVERED): every result matches *Expected behavior*
- [ ] `psql SELECT message_id, status, sent_at, delivered_at FROM message ORDER BY sent_at DESC LIMIT 5` → the rows from the PENDING phase are now `DELIVERED` (the **full chain PENDING → SENT → DELIVERED** in the same table)

<details>
<summary><strong>If you are stuck</strong></summary>

- The **`@Scheduled`** import is **`org.springframework.scheduling.annotation.Scheduled`**. The **`@EnableScheduling`** **annotation** is a **`@Configuration`** (put it on the **main** app class, or on a **dedicated** `@Configuration`). Without it, `@Scheduled` is **silently ignored** — your **poller will not run** and you will get **`PENDING` rows that never leave the DB**.
- The **`UPDATE … WHERE status = 'PENDING'`** is an **atomic claim**: **`int affected = repo.claim(id);`** — if **`affected == 0`**, **another instance** won the **race**; **do not** publish. **If `affected == 1`**, do **`convertAndSend`**. The **order matters** (claim **first**, publish **second**) — publish **first** = **double-publish race** (the **entire point** of 8.6).
- The price of claim → publish: an instance that dies between the claim and the `convertAndSend` leaves a `SENT` row that was never published. The other order (publish, then mark) would publish twice instead, which the idempotent consumer absorbs. Name the trade-off in your decision log.
- The **`WHERE`** clause is **the guard** (the **PK** `message_id` is the **lookup**; the **`status`** is the **gate**). Both are correct, but **the status check is non-redundant** — it is the **only** way to claim **without** a lock.
- `@Scheduled(fixedDelayString = "${…:5000}")` reads the delay from a property, with a default.
- A JPQL `UPDATE` in a repository needs `@Modifying` + `@Query` (`org.springframework.data.jpa.repository`), returns an `int` (rows affected), and runs inside a transaction: put `@Transactional` on the method or on its caller, otherwise Spring Data raises `TransactionRequiredException`. Reference: <https://docs.spring.io/spring-data/jpa/reference/4.1/jpa/modifying-queries.html>
- The **`@Scheduled`** method runs **on** your app's **main** thread pool — **do not** put a `Thread.sleep(...)` in it; the **`fixedDelay`** (default 5 s) is the **cadence**.

</details>

---

## Step 9 — Final Integration   DELIVERY

**Outcome** — The **whole stack** (TP1 base + RabbitMQ + the `com.app.listings
.messaging` package) **boots cleanly from a clean checkout**: both repos
`git clone`'d, the broker `docker compose up`'d, the app built with
`mvn clean package`, the app `docker compose up`'d. The **decision log** is
**committed** in the TP1 README. The **evaluator can** `curl` a `202 SENT`,
`psql` a `DELIVERED` row, and **trust** the output.

**Why this step** — The evaluator rebuilds your work from the repositories,
not from your laptop. This step proves it starts from a clean checkout and
that you can explain each decision.

### Build it

1. **From a fresh clone**, **`git pull`** (or check out your last commit) of
   **both** repos (the TP1 repo **and** this `messaging/` repo).
   → It proves nothing depends on a local, uncommitted file.

2. **Start** the **broker first**, then **build** the app (TP1 repo) with
   `docker compose build listings-api` (or a local `mvn clean package` if you
   prefer), **then** `docker compose up -d` in the TP1 repo (the **same**
   compose file that has the Step 1 `RABBITMQ_HOST` + `RABBITMQ_PORT` edits).
   → Same order as Step 1: the app expects the broker at startup.

3. **Run the smoke checks** — the **Proof** checklist below — and **capture**
   the outputs in your **decision log** (so the evaluator can re-run them).
   → The evaluator replays the same commands and compares.

4. **Add a decision log** to the **TP1 repo's `README.md`** (5–6 short
   paragraphs, one per **major** decision: **why** the topology is in code
   (Step 2), **why** `messageId` is client-generated (Steps 3a/4/7–8b), **why**
   the DLQ is a **separate** exchange (Step 5), **why** the **503 contract**
   (Step 6) was **chosen** (or **replaced** by the outbox — Steps 8a-8b), **why**
   the **`UPDATE … WHERE status=PENDING`** is **before** the `convertAndSend`
   (Step 8b), **why** the **`message`** table has its **own** `delivered_at`
   (Step 4) instead of re-using `listing.updated_at` (TP1). The **decision
   log is the oral-prep** — it **is** the evaluator's "why" answer bank.
   → The code shows *what* you did; only the log shows *why*.

5. **Push** both repos to their remotes (the TP1 repo and this `messaging/`
   repo). **Do NOT** commit: broker credentials (beyond the **demo**
   `listings`), a `.env` file, a **raw log line** with a buyer's message body,
   **or** a hard-coded **real** password.
   → A pushed secret stays in the git history even after a later commit
   removes it.

### Final checks

- [ ] `cd "$TP1_DIR" && docker compose up -d --build` → `docker compose ps` shows **`listings-api`** + **`listings-postgres`** both healthy
- [ ] `cd "$MSG_DIR" && docker compose up -d` → `docker compose ps` shows **`rabbitmq`** healthy
- [ ] `curl -fsS http://localhost:8080/actuator/health` → **200** (the **app** is up and connected to **both** Postgres and RabbitMQ)
- [ ] **One buyer `POST`** → **202** with `"status":"SENT"`
- [ ] **One consumer drain** → `psql SELECT status FROM message WHERE message_id='<that-uuid>'` → **`DELIVERED`**; the **buyer's** and the **seller's** `GET /listings/{id}/messages` return the **2 turns** (the buyer's opening + the seller's reply)
- [ ] **`mvn clean verify`** in the TP1 repo → **BUILD SUCCESS** (the TP1 smoke test + **your** messaging smoke test **both pass**)
- [ ] **The decision log is in the TP1 repo's `README.md`** (5–6 short paragraphs of *why*)
- [ ] **No secrets** in either repo: the broker credential is **in** the `messaging/docker-compose.yml` **and** the TP1's `application.yml` reads `RABBITMQ_PASSWORD: ${RABBITMQ_PASSWORD:listings}` (**demo** value `listings`, **overridable** by env var); **no** real password in either **compose** or **application.yml**
- [ ] **`git status`** in **both** repos → **clean** (no uncommitted changes, no tracked secret files)
- [ ] **`git log --oneline -10`** in **both** repos → the commits are **intentional** (message is **specific**, e.g. "add messaging module: producer + consumer + DLX/DLQ" — not "stuff")

<details>
<summary><strong>If you are stuck</strong></summary>

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

</details>

---

## Deliverables

| Item | Detail |
|---|---|
| **The TP1 repo** (the Spring Boot app) | `+1 dep` (`spring-boot-starter-amqp`, **no** version — Appendix A); `+` the `spring.rabbitmq` block in `application.yml`; `+` the **base compose** `RABBITMQ_HOST` + `RABBITMQ_PORT` env; `+` the `com.app.listings.messaging` package (`RabbitConfig`, `MessageEntity`, `MessageRepository`, `MessageService`, `MessageController`, `MessageListener`, **Step 6**: `BrokerUnavailableException` + a **`@ExceptionHandler`** in TP1's `GlobalExceptionHandler`, **Step 7–8** optional: `MessageOutbox` + the `@EnableScheduling` config + `listings.messaging.outbox.interval-ms` key); Liquibase `v1.2-messages.yaml` + include in the changelog master; `README.md` updated with the **decision log** |
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
| `application.yml` | `spring.rabbitmq.host` / `port` / `username` / `password` — **all** env-overridable (`${RABBITMQ_HOST:localhost}`, etc.) — and `connection-timeout: 2s` | the **host** default is for `mvn spring-boot:run`; the compose edit (next row) **overrides** `port` (5672) and **`host`** (the **network name** `rabbitmq`) at runtime. The 2 s timeout replaces the 60 s default, so a stopped broker gives a fast 503 (Step 6) |
| the TP1 repo's `docker-compose.yml` | `listings-api.environment`: `RABBITMQ_HOST: rabbitmq` + `RABBITMQ_PORT: "5672"` | from **inside** the API container, `localhost` is the **container itself**; the app must reach the broker via its **network name** on the **internal** port |

> **One owner per fact:** the exact `pom.xml` / `application.yml` /
> `docker-compose.yml` to paste lives in **`README.md`**; this appendix
> explains **which** artifact, **which** key, and **why**. Unlike the **IA**
> TP (no new container), this TP **adds one** (RabbitMQ) — the base compose
> **must** change.

---

## Appendix B — RabbitMQ management API endpoints (used in `docs/verification.md`)

All with **Basic auth** `listings:listings` (the **`guest`** account is **not
available** — see the compose file). The default vhost `/` is written `%2F`
in the paths.

| Endpoint | Purpose |
|---|---|
| `GET /api/overview` | broker version / uptime — the "is the broker up?" health endpoint (Step 1) |
| `GET /api/connections` | one row per **app** that has connected (Step 1) |
| `GET /api/queues/%2F/{name}` | **one** queue: `type`, `messages_ready`, `messages_unacknowledged`, `consumers`, `arguments` (Steps 2, 3, 5, 7) |
| `GET /api/exchanges/%2F/{name}` | **one** exchange, with `type` (Steps 2, 5) |
| `GET /api/exchanges/%2F/{name}/bindings/source` | the **bindings** from an exchange to queues, with the routing key (Step 2) |
| `POST /api/queues/%2F/{name}/get` | **peek** up to N messages; body `{"count":5,"ackmode":"ack_requeue_true","encoding":"auto"}` keeps them in the queue — used to read the **DLQ** (Step 5) |
| `POST /api/exchanges/%2F/{name}/publish` | **publish**; body `{"properties":{},"routing_key":"…","payload":"…","payload_encoding":"string"}` — the **poison** and **duplicate** drills (Steps 4, 5, 7) |
| `DELETE /api/queues/%2F/{name}/contents` | **purge** a queue (keeps the queue) — to empty the DLQ **after** you have read it |

> `GET /api/queues?name=…` does **not** filter by name: it returns every
> queue. Address one queue by its path, `/api/queues/%2F/{name}`.
>
> The counters (`messages_ready`, `consumers`, …) are refreshed about every
> **5 s**, and the first read after a queue is created can lag more: read
> them **~10 s** after an action, or ask the broker directly
> (`docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready consumers`).

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

- **Spring AMQP 4.1** (the version of Boot 4.1.1): <https://docs.spring.io/spring-amqp/reference/4.1/index.html> —
  *Annotation-driven listener endpoints*: <https://docs.spring.io/spring-amqp/reference/4.1/amqp/receiving-messages/async-annotation-driven.html> —
  *Exception handling* (requeue, `AmqpRejectAndDontRequeueException`): <https://docs.spring.io/spring-amqp/reference/4.1/amqp/exception-handling.html>
- **Spring Boot 4.1 — AMQP** (the `spring.rabbitmq.*` keys): <https://docs.spring.io/spring-boot/4.1/reference/messaging/amqp.html>
- **RabbitMQ 4.x**: quorum queues <https://www.rabbitmq.com/docs/quorum-queues> ·
  dead lettering <https://www.rabbitmq.com/docs/dlx> ·
  management API <https://www.rabbitmq.com/docs/management> ·
  `rabbitmqctl` <https://www.rabbitmq.com/docs/rabbitmqctl>

> **TP rule (final repeat):** no `TODO`, no stub, no secret in code, no
> **silent** swallow of a message (the **DLQ** is the **right** place), and
> the **broker is a runtime, not a source of truth** — the **topology** is
> code in `com.app.listings.messaging.config.RabbitConfig`, the **messages** are
> **data**.

---

## Appendix E — Status codes

The new row compared to TP1 is **503**
(broker unavailable). Everything else is **invariant** from TP1.

| Status | Trigger | Suggested mechanism |
|---|---|---|
| **401** | Unauthenticated `POST /listings/{id}/messages` | Spring Security (TP1 — no change) |
| **403** | (TP1 invariant) Non-owner writes are out of scope for this TP (no PUT on a listing you do not own) | Spring Security (TP1) |
| **404** | `POST/GET /listings/{unknown-id}/*messages` (no such listing) | `ResourceNotFoundException` (TP1) |
| **422** | **Self-message** (`from == to`); empty `body`; `inReplyTo` is not a known/visible message id on this listing; `inReplyTo` from a different thread | blank `body`: `@NotBlank` → `MethodArgumentNotValidException` → TP1 `GlobalExceptionHandler`; self-message and `inReplyTo`: your own exception from `MessageService`, mapped to a 422 with the same shape (`fields` map) |
| **503** | **Broker unavailable** at publish time (Step 6) | any `AmqpException` from `RabbitTemplate` (in practice `AmqpIOException` / `AmqpConnectException`) → custom `BrokerUnavailableException` → **one** `@ExceptionHandler` returning the TP1 JSON shape **with status 503** and a useful message (example: `"rabbitmq not reachable — check docker compose / RABBITMQ_HOST / RABBITMQ_PORT"`) |
| **500** | (invariant) Unexpected error | TP1 `GlobalExceptionHandler` generic handler |
