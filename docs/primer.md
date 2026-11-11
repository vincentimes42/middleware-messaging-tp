# TP Messaging primer — message queues, RabbitMQ, Spring AMQP

Background for [`../exercise.md`](../exercise.md): what a message queue is,
how Spring AMQP and RabbitMQ split the work, what not to do, and how to test
if you choose to. Read it once before Step 1.

## What is a message queue?

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

## Spring AMQP and RabbitMQ

> **Why one library and one broker:** **Spring AMQP** is the client side of
> your API (the `RabbitTemplate` producer + the `@RabbitListener` consumer).
> **RabbitMQ** is the server that stores and routes the messages. Keep the two
> separate in your head: a buyer `POST`s to your API; your API speaks **AMQP**
> to the broker; the broker routes to the queue; your API reads it back.

## What NOT to do in production code

- You do **not** catch an unexpected exception in the **listener** and
  swallow it **silently** — the point of the DLX/DLQ is to *observe* the
  failure, not to hide it. Log it (`log.warn`) and **reject** it so the
  broker reroutes it (Step 5).
- You do **not** store the `body` of a buyer message in a **log line** the
  way you would log a request (privacy — the TP1 rule applies too).

## Testing strategy (optional)

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
