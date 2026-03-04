# Mission 8 — Dead letters

**Curiosity hook:** After N failures, stop retrying and send to a “graveyard” for humans to fix.

## What you’ll use

- **Dead-letter exchange (DLX)** — When a message is nack’d (or TTL expires, or queue length exceeded, depending on config), it can be routed to another exchange: the DLX. A queue bound to the DLX is the “dead-letter queue” (DLQ).
- **TTL / retry** — Optionally combine with per-message TTL or a retry counter in headers so after N redeliveries you nack without requeue and the message goes to the DLQ.

## Task

- **Setup:** Main queue declared with `deadLetterExchange` pointing to a DLX; DLQ bound to the DLX.
- **Producer:** Sends a few messages to the main queue.
- **Consumer:** Processes messages; for one specific message (e.g. payload `"fail"`), nack without requeue (or simulate 3 retries via a header, then nack). That message goes to the DLQ. Other messages are ack’d.
- **DLQ consumer:** Separate script that reads from the DLQ and logs “Dead letter: …” so you can inspect failed messages.

## How to run

1. Start the main consumer:

   ```bash
   node levels/08-dead-letters/consumer.js
   ```

2. Run the producer:

   ```bash
   node levels/08-dead-letters/producer.js
   ```

3. Main consumer will nack the `"fail"` message (no requeue) — it goes to the DLQ. Start the DLQ consumer to see it:

   ```bash
   node levels/08-dead-letters/consumer-dlq.js
   ```

   (If the message was already dead-lettered, run producer again and have the main consumer running so it nacks again; or run consumer-dlq first, then producer and main consumer.)

## What to try next

- Add a retry counter in message headers and only dead-letter after 3 nacks.
- **Next:** Mission 9 (Boss) — mini project combining patterns (e.g. event-driven scoreboard or order flow).
