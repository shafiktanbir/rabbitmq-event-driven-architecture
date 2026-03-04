# Mission 4 — Routing (direct)

**Curiosity hook:** Only the “billing” service cares about `payment.completed`; others don’t. Routing keys do that.

## What you’ll use

- **Direct exchange** — Routes messages to queues whose binding key **exactly** matches the message’s routing key.
- **Routing key** — String you set when publishing (e.g. `error`, `info`, `warning`). Each consumer binds its queue with the key(s) it cares about.

## Task

- **Producer:** Sends log-level messages to a direct exchange with routing keys `error`, `info`, and `warning`.
- **Consumers:** One consumer bound to `error` only (e.g. “alert”); one bound to `info` only (e.g. “general log”). Each receives only the messages that match its binding.

## How to run

1. Start both consumers:

   ```bash
   node levels/04-direct/consumer-error.js
   node levels/04-direct/consumer-info.js
   ```

2. Run the producer:

   ```bash
   node levels/04-direct/producer.js
   ```

3. Error consumer gets only `error` messages; info consumer gets only `info` (and you can add `warning` to one of them to see selective routing).

## What to try next

- Add a consumer that binds to both `error` and `warning`.
- **Next:** Mission 5 — topic exchange so you can subscribe to patterns like “all backend errors” or “orders from EU.”
