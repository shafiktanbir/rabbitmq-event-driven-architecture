# Mission 3 — Fan-out (pub/sub)

**Curiosity hook:** You don’t want to hard-code “notify analytics, logging, and email.” You publish once; whoever is bound gets it.

## What you’ll use

- **Exchange** — Receives messages from producers and routes them to queues. So far we used the default exchange (routing by queue name). Now we use a named exchange.
- **Fan-out** — Sends every message to every queue bound to the exchange (no routing key used).
- **Binding** — Links a queue to an exchange.

## Task

- **Producer:** Publishes to a fan-out exchange (e.g. `logs`). No queue in the producer — only “publish to exchange.”
- **Consumers:** Two different “apps”: e.g. one that prints to console, one that “saves to file” (just log “saved to file: …”). Each has its own queue bound to the same exchange. Both get every message.

## How to run

1. Start both consumers (in any order):

   ```bash
   node levels/03-fanout/consumer-console.js
   node levels/03-fanout/consumer-file.js
   ```

2. Run the producer:

   ```bash
   node levels/03-fanout/producer.js
   ```

3. Both consumers should receive the same log line. Add more consumers with their own queues — they all get a copy.

## What to try next

- Bind a third queue to the same exchange and see it get the same messages.
- **Next:** Mission 4 — direct exchange so only subscribers that care about a routing key (e.g. `error` vs `info`) get the message.
