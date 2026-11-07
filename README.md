# TP Messaging — asynchronous seller↔buyer messaging (RabbitMQ)

Follow-up to the Spring Boot TP (TP1, `../springboot/`). We add an
**asynchronous messaging layer** on top of the TP1 listings API: buyers post
questions, the **RabbitMQ broker** buffers them, and the seller reads them —
**without being online**.

The student modifies **the TP1 repo** (a new `com.app.listings.messaging`
package) and the base compose (to point the API at the broker):

- `docker-compose.yml` — RabbitMQ + management, on the shared `middleware`
  network
- [`exercise.md`](exercise.md) — the full statement (8h, core 1-6 + bonus 7-8)
- [`docs/verification.md`](docs/verification.md) — copy-paste `curl`s

> **One owner per fact**: the *rules* live in `exercise.md`, the *commands*
> live here. See [`exercise.md`](exercise.md) for the pedagogical statement.

## Prerequisites

- **TP1 is done and green** (register, CRUD, ownership, `?version=V1|V2`).
- `docker compose version` (the base `../springboot` must run).
- JDK 27 + Maven 3.9+ (already installed for TP1).
- **Ports free on the host:** `8080` (API, unchanged) · `5435` (TP1 Postgres) ·
  **`5673`** (AMQP) · **`15673`** (management UI)

> **Tip:** Keycloak, Monitoring, IA, and Messaging are **alternatives** — all
> start the same base `listings-api`, so run **one** at a time.

## How the pieces fit (one shared network)

This is **two Docker Compose projects on ONE shared network** (`middleware`,
created by the base project in `../springboot/docker-compose.yml`):

| Project | Folder | Services | Host ports |
|---|---|---|---|
| base | `../springboot` | `listings-api`, `listings-postgres` | API `8080`, pg `5435` |
| this | `messaging` | `rabbitmq` | AMQP `5673`, management `15673` |

Services find each other by **container name** over the shared network, never
by `localhost`. So the API reaches the broker as `rabbitmq:5672` (its
*internal* AMQP port), **not** `localhost:5673`.

> **Why the host ports are 5673 / 15673 (not 5672 / 15672)?** Because
> RabbitMQ's *default* host ports are 5672 (AMQP) / 15672 (management), and a
> student may already have a local RabbitMQ. The same "avoid the collision"
> rule as `listings-postgres` (5435) and `keycloak` (8081): the host port is
> offset by +1, the *internal* container port stays the standard one. From
> inside the API container, the broker is always `rabbitmq:5672` (internal).

```
   buyer  (any TP1 account)              seller (the listing owner)
        |  POST /listings/{id}/messages   |  GET /listings/{id}/messages
        v                                 ^
   +----------------------------------------------------------+
   |              listings-api (TP1, port 8080)               |
   |   producer: RabbitTemplate  |  consumer: @RabbitListener |
   +-----------------------------^----------------------------+
                                  | AMQP  rabbitmq:5672
                                  v
   +----------------------------------------------------------+
   |                  RabbitMQ 4.x (5673 / 15673)             |
   |   exchange listings.direct -> binding -> queue           |
   |   queue listings.messages (durable)                      |
   |   DLX listings.messages.dlx -> DLQ listings.messages.dlq |
   +----------------------------------------------------------+
```

## Run

### 0) Required base edit — three additive edits

The TP's Java lives in your copy of `../springboot` (this repo ships **no
Java**; `docker-compose.yml` and `docs/` here are the broker + verification,
the way `../keycloak/` ships `realm/` and `../monitoring/` ships the config
YAMLs).

Three **additive** edits are required — `pom.xml`, `application.yml`, and the
base compose — all in `../springboot` (see [`exercise.md` Appendix A](exercise.md)
for the *what* and *why*):

**1. `../springboot/pom.xml` — add the Spring AMQP starter** (no version, it
is managed by the Boot 4.1.1 BOM):

```xml
<!-- Spring AMQP: RabbitTemplate (producer) + @RabbitListener (consumer) + RabbitAdmin -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-amqp</artifactId>
</dependency>
```

**2. `../springboot/src/main/resources/application.yml` — add the
`rabbitmq` sub-block** under the **existing** `spring:` key (the file already
has `spring.datasource` and `spring.jpa` — **do not** start a second `spring:`):

```yaml
spring:
  datasource: { /* existing */ }
  jpa:        { /* existing */ }
  rabbitmq:                        # <- add this block (indented under `spring:`)
    host:     ${RABBITMQ_HOST:localhost}
    port:     ${RABBITMQ_PORT:5673}
    username: ${RABBITMQ_USERNAME:listings}
    password: ${RABBITMQ_PASSWORD:listings}
```

Every value is **env-overridable**; the defaults target a local
`mvn spring-boot:run`, and the compose edit below **overrides** `host`
(network name) and `port` (internal 5672) inside Docker.

**3. `../springboot/docker-compose.yml` — point the API at the broker by
network name** (in `listings-api` → `environment`). From **inside** the API
container `localhost` is the container itself, so it **must** use the
network name `rabbitmq` on its *internal* port `5672` (not the host `5673`):

```yaml
  listings-api:
    environment:
      # the broker, by network name + INTERNAL port (same split as Keycloak's JWKS-URI)
      RABBITMQ_HOST: "rabbitmq"
      RABBITMQ_PORT: "5672"
```

That is the **entire** required change to `../springboot`. This TP **adds one
container** (unlike the IA TP), so there **is** a base-compose edit here, and
there is a new `docker-compose.yml` in this folder.

### 1) Start the TP1 base (unchanged)

```bash
cd ../springboot
docker compose up -d
curl -fsS http://localhost:8080/actuator/health      # API on 8080
```

### 2) Start the broker (this folder)

```bash
cd ../messaging
docker compose up -d
docker compose ps          # rabbitmq healthy
curl -fsS -u listings:listings http://localhost:15673/api/overview
```

Then rebuild the API **once** so it picks up the new starter + the compose edit:

```bash
cd ../springboot
docker compose up -d --build listings-api && docker compose ps
```

> **Order matters:** the broker must be **up** before the app's
> `@RabbitListener` connects. If you start the app first it **retries**
> (Spring AMQP's default reconnection) — but the cleanest lab sequence is
> **base → broker → app rebuild**.

### 3) Endpoints on the host

| URL | Purpose |
|---|---|
| `GET http://localhost:8080/listings` | TP1 read path (unchanged) |
| `POST http://localhost:8080/listings/{id}/messages` | the **new** publish endpoint (Step 3) |
| `GET http://localhost:8080/listings/{id}/messages` | the **new** thread read (Step 4) |
| <http://localhost:15673/> | **RabbitMQ management UI** (`listings` / `listings`) |
| `http://localhost:15673/api/overview` | broker health (the "is it up?" call) |

A full, copy-paste walkthrough per step is in
[`docs/verification.md`](docs/verification.md).

### 4) Stop

```bash
cd ../messaging    && docker compose down          # stop the broker (keep its volume)
cd ../springboot   && docker compose down          # stop the base
# (add -v in either folder to WIPE the named volume / Postgres data)
```

## Build & test

```bash
# make sure the base (Postgres) AND the broker are reachable, then:
cd ../springboot && mvn clean verify
```

The TP1 `ListingsSmokeTest` still needs a Postgres reachable on
`localhost:5435`. A **messaging smoke test** you add (Step 9) needs the broker
too — the simplest is to assert the `RabbitAdmin` bean is present and the
`@RabbitListener` is registered (context-level — no real publish).

## Tree

```
messaging/
├── README.md                 # this file
├── exercise.md               # the statement (8h, core 1-6 + bonus 7-8 + delivery 9)
├── docker-compose.yml        # RabbitMQ 4.2-management + host 5673/15673
└── docs/
    └── verification.md       # copy-paste curls per step (API + management + psql)

../springboot/  (the code you extend — NOT in this repo)
├── pom.xml                   # +1 dep (spring-boot-starter-amqp)   [Steps 1-8]
├── src/main/resources/application.yml   # + spring.rabbitmq block  [Step 1]
├── docker-compose.yml        # + RABBITMQ_HOST / RABBITMQ_PORT    [Step 1]
├── src/main/java/com/app/listings/messaging/
│   ├── RabbitConfig.java             # exchanges, queues, bindings [Step 2]
│   ├── MessageEntity.java            # the `message` JPA entity    [Step 3]
│   ├── MessageRepository.java        # Spring Data JPA + idempotent [Step 3, 4]
│   ├── MessageService.java           # producer + 503 path          [Step 3, 6]
│   ├── MessageController.java        # the two REST endpoints       [Step 3]
│   ├── MessageListener.java          # @RabbitListener + poison     [Step 4, 5]
│   └── exception/BrokerUnavailableException.java   [Step 6]
│   (optional bonus)
│   ├── MessageOutbox.java            # the @Scheduled poller        [Step 8]
│   └── config/SchedulingConfig.java  # @EnableScheduling            [Step 8]
└── src/main/resources/db/changelog/
    ├── v1.1-messages.yaml                    # the additive createTable
    └── db.changelog-master.yaml              # + the include line
```

> **Solutions** (supervisor only, NOT shipped): none in this repo. Like the
> other TPs, the Java lives in your copy of `../springboot`, the statement +
> provided config live here.

## To implement (reminder — see exercise.md)

**Core** (must fit the ~6h of the second session):

1. **Broker up** + the app **connected** (Step 1)
2. **Topology in code** — exchange, bindings, queues **declared by the app**
   (Step 2)
3. **Publish** — buyer asks, the **broker holds it** (`202 SENT`) (Step 3)
4. **Consume + conversation** — seller reads, replies, the thread is
   **readable** (Step 4) — contains the **full behavior matrix**
5. **Poison → DLQ** — a bad message **lands in `listings.messages.dlq`**,
   the queue is **not** blocked (Step 5)
6. **Broker down → 503** — the API **survives**, the endpoint is **honest**
   (Step 6)

**Bonus** (start only when the core is green):

7. **At-least-once** — a redelivered message is **not duplicated** (Step 7)
8. **Transactional outbox** — `202 PENDING` even when the broker is down
   (Step 8)

## Technical notes

- The **broker is a runtime, not a source of truth.** The topology
  (exchange, queue, bindings, DLX/DLQ) is **declared in Java** in
  `com.app.listings.messaging.RabbitConfig` (Step 2). The **management UI**
  is a **view**, not the source — a `docker compose down -v` wipes it, the
  Java re-creates it on the next boot.
- **`message_id`** is **client-generated** (the producer does
  `UUID.randomUUID()` in Step 3), and it is **unique** on the table (Liquibase
  `v1.1-messages.yaml`). This is the **idempotency key**: Step 4's
  **SELECT-then-INSERT-or-UPDATE** upsert (not an `INSERT IGNORE`) is **why**
  a redelivered message does **not** duplicate the row (Step 7's proof).
- **`x-dead-letter-*`** arguments are **queue arguments** (not exchange
  arguments). RabbitMQ applies them **when the consumer NACKs** a message with
  `requeue=false` **or** when the `x-delivery-limit` (3) is reached — either
  way the exchange is **`listings.messages.dlx`** and the target queue is
  **`listings.messages.dlq`** (Step 2's `RabbitConfig`).
- The **broker credential** (`listings` / `listings`) is in
  `docker-compose.yml` via `RABBITMQ_DEFAULT_USER` / `RABBITMQ_DEFAULT_PASS`
  — the **right** place (it is the **broker's** setting, not the API's). The
  API reads it via `${RABBITMQ_USERNAME:listings}` (the **default** is for
  `mvn spring-boot:run`; the compose environment **overrides** it if you
  change the broker credential to something else).
- The **`503`** (Step 6) is **not** the **IA** TP's 503 (which is "the AI
  key is not set"). The **meaning** differs: here it is "the broker is
  unavailable"; there it is "the third-party LLM is unavailable". Same HTTP
  code, different trigger. **Do not mix the two** in a single sentence — each
  step has its own.

## Useful links (project version)

- Spring AMQP — reference: <https://docs.spring.io/spring-amqp/reference/html/>
- RabbitMQ — getting started + management:
  <https://www.rabbitmq.com/docs/getstarted> ·
  <https://www.rabbitmq.com/management.html>
- RabbitMQ — dead-letter topic: <https://www.rabbitmq.com/dlx.html>
- `rabbitmqctl` — <https://www.rabbitmq.com/ctl.html>

> **TP rule:** no `TODO`, no stub, no secret in production code.
> The broker's `listings`/`listings` is a **demo** credential committed in
> `docker-compose.yml` — not a **real** secret, and not **in Java**.
> The full "why" of each choice is in
> [`exercise.md`](exercise.md) (Appendix A).
