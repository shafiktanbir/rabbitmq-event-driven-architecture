# Mission 5 — Topics

**Curiosity hook:** Subscribe to “all errors” or “orders from EU” with one binding pattern.

## What you’ll use

- **Topic exchange** — Routing key is a dot-separated list of words (e.g. `user.signup`, `order.created.eu`). Binding key can use wildcards: `*` (one word), `#` (zero or more words).
- **Patterns** — e.g. `user.#` matches `user.signup`, `user.profile.updated`; `order.*.eu` matches `order.created.eu`, `order.shipped.eu`.

## Task

- **Producer:** Publishes to a topic exchange with keys like `user.signup`, `order.created.eu`, `order.created.us`, `backend.error`.
- **Consumers:** One bound to `user.#` (all user events); one bound to `order.*.eu` (EU orders only). Each receives only matching messages.

## How to run

1. Start both consumers:

   ```bash
   node levels/05-topics/consumer-user.js
   node levels/05-topics/consumer-order-eu.js
   ```

2. Run the producer:

   ```bash
   node levels/05-topics/producer.js
   ```

3. User consumer gets all `user.*`; order-EU consumer gets only `order.*.eu`.

## What to try next

- Add a consumer for `*.error` or `backend.#`.
- **Next:** Mission 6 — RPC: when the worker must send a response back to the caller.
