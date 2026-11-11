# TP Messaging — asynchronous seller↔buyer messaging (RabbitMQ)

Follow-up to the Spring Boot TP (TP1). We add an **asynchronous messaging
layer** on top of the TP1 listings API: buyers post questions, the
**RabbitMQ broker** buffers them, and the seller reads them — without being
online.

Two repos are involved:

| Repo | Role |
|---|---|
| **TP1 repo** (the Spring Boot app you have) | the code you extend with a `messaging` package |
| **Messaging repo** (this one) | the broker compose + the statement + the verification |

## Prerequisites

- **TP1 is done and green** (register, CRUD, ownership, `?version=V1|V2`).
- Docker + Docker Compose (`docker compose version`).
- JDK 27 + Maven 3.9+ (already installed for TP1).
- **Ports free on the host:** `8080` (API, unchanged) · `5435` (Postgres) ·
  `5673` (AMQP) · `15673` (management UI)

## Run

### 0) Required edits in the TP1 repo

Three additive edits are required. The *what* and *why* is in
[`exercise.md`, Appendix A](exercise.md).

**a) `pom.xml`** — add the Spring AMQP starter (no version — managed by the
Boot 4.1.1 BOM):

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-amqp</artifactId>
</dependency>
```

**b) `application.yml`** — add `rabbitmq:` under the **existing** `spring:`
key (do **not** start a second `spring:`):

```yaml
spring:
  rabbitmq:
    host:     ${RABBITMQ_HOST:localhost}
    port:     ${RABBITMQ_PORT:5673}
    username: ${RABBITMQ_USERNAME:listings}
    password: ${RABBITMQ_PASSWORD:listings}
    connection-timeout: 2s     # default 60 s: a stopped broker would hang requests (Step 6)
```

**c) `docker-compose.yml`** (the TP1 one) — in `listings-api` →
`environment`, add:

```yaml
      RABBITMQ_HOST: "rabbitmq"
      RABBITMQ_PORT: "5672"
```

> **Why these values?** From inside the `listings-api` container, `localhost`
> *is the container itself*. The broker is reachable by its **container
> name** `rabbitmq` on its **internal** port **5672**. The host port `5673`
> is only for *your* `curl` (management). Use `localhost:5673` when you test
> with `curl` from the host.

> **Two repos, two folders.** You cloned the TP1 repo (the Spring Boot app) and
> this messaging repo **side by side** (any folder names). Set these once per
> terminal so the commands below do not need hard-coded paths:
>
> ```bash
> export TP1_DIR="/path/to/your/tps-1-repo"     # the Spring Boot app repo
> export MSG_DIR="/path/to/messaging-repo"      # this messaging repo
> ```

### 1) Start RabbitMQ first

```bash
cd "$MSG_DIR"
docker compose up -d
docker compose ps          # rabbitmq healthy
```

> **Why RabbitMQ first?** The app's `@RabbitListener` connects on **boot**.
> Starting the broker first means the app connects cleanly on its very first
> boot instead of failing and retrying.

### 2) Start the TP1 app

```bash
cd "$TP1_DIR"
docker compose up -d --build
curl -fsS http://localhost:8080/actuator/health
```

### 3) Useful URLs

| URL | Purpose |
|---|---|
| `http://localhost:8080/actuator/health` | the app (TP1, unchanged) |
| `POST http://localhost:8080/listings/{id}/messages` | **new** — buyer posts a question |
| `GET http://localhost:8080/listings/{id}/messages` | **new** — read the thread |
| `http://localhost:15673` | **RabbitMQ management UI** (login: `listings` / `listings`) |

A full per-step walkthrough (`curl` commands) is in
[`docs/verification.md`](docs/verification.md).

### 4) Stop

```bash
cd "$MSG_DIR" && docker compose down      # stop the broker (keep its data)
cd "$TP1_DIR" && docker compose down      # stop the app + its Postgres
# add -v to either to WIPE the data
```

## Build

```bash
cd "$TP1_DIR" && mvn clean verify
```

## What you implement

**Core (Steps 1-6, ~5h45):**

| Step | What you do | How you prove it |
|---|---|---|
| 1 | RabbitMQ up + the app connects | `docker compose ps` healthy, `/api/overview` 200 |
| 2 | Declare the topology in Java (`RabbitConfig`) | management UI shows the exchanges, the **quorum** main queue, the DLQ, and the bindings |
| 3 | `POST /listings/{id}/messages` → 202 SENT | the message is in the broker (`ready ≥ 1`, `consumers = 0`) |
| 4 | Consume + reply — the full conversation | both sides see the thread in `GET /listings/{id}/messages` |
| 5 | Poison message → DLQ | the message **lands in** `listings.messages.dlq`, visible in the UI |
| 6 | Broker down → 503 | `docker compose stop rabbitmq` → `POST …/messages` returns **503** in a few seconds, `/actuator/health/liveness` stays 200 |

**Bonus (Steps 7-8, ~1h45):**

| Step | What you do |
|---|---|
| 7 | A redelivered or duplicated message is **not stored twice** |
| 8 | `PENDING → SENT → DELIVERED` (transactional outbox) |

The full pedagogical statement is in [`exercise.md`](exercise.md).

## Tree

```
messaging/
├── README.md
├── exercise.md
├── docker-compose.yml
└── docs/
    └── verification.md

springboot/  (your TP1 repo — you add the messaging package here)
├── pom.xml                              # + spring-boot-starter-amqp
├── src/main/resources/application.yml   # + spring.rabbitmq block
├── src/main/resources/db/changelog/v1.2-messages.yaml   # [Step 3] + include in the master
├── docker-compose.yml                   # + RABBITMQ_HOST / PORT
└── src/main/java/com/app/listings/messaging/
    ├── RabbitConfig.java                # [Step 2]
    ├── MessageEntity.java               # [Step 3]
    ├── MessageRepository.java           # [Step 3-4]
    ├── MessageService.java              # [Step 3, 6]
    ├── MessageController.java           # [Step 3]
    ├── MessageListener.java             # [Step 4-5]
    └── BrokerUnavailableException.java  # [Step 6]
```

## Useful links

- [Spring AMQP 4.1 reference](https://docs.spring.io/spring-amqp/reference/4.1/index.html)
- [Spring Boot 4.1 — AMQP](https://docs.spring.io/spring-boot/4.1/reference/messaging/amqp.html)
- [RabbitMQ quorum queues](https://www.rabbitmq.com/docs/quorum-queues)
- [RabbitMQ dead lettering](https://www.rabbitmq.com/docs/dlx)
