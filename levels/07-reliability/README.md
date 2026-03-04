# Mission 7 — Reliability

**Curiosity hook:** If the worker crashes, the message must not vanish. Acks and confirms are the contract.

## What you’ll use

- **Manual ack** — Consumer explicitly acks (or nacks) each message. Until then, if the consumer dies, the message is re-queued (or dropped if you nack without requeue).
- **Persistent messages** — `deliveryMode: 2` and a **durable** queue so messages survive broker restarts (when supported by the broker).
- **Publish confirms** — Producer puts the channel in confirm mode; for each publish it gets an ack (or nack) from the broker so it knows the message was accepted.

## Task

- **Producer:** Uses a durable queue, persistent messages, and confirm mode. Sends a few messages and waits for confirms before exiting.
- **Consumer:** Uses manual ack. Simulate a crash: on a specific message (e.g. the 2nd), don’t ack and kill the process (or exit). Restart the consumer — the unacked message is delivered again (at-least-once).

## How to run

1. Start the consumer:

   ```bash
   node levels/07-reliability/consumer.js
   ```

2. Run the producer:

   ```bash
   node levels/07-reliability/producer.js
   ```

3. Consumer acks all. Then change behavior: on the 2nd message, comment out the ack and exit (or use a flag). Run producer again, then consumer — see the 2nd message re-delivered after restart.

## What to try next

- Kill the consumer mid-task (before ack) and restart to see re-queue.
- **Next:** Mission 8 — dead-letter queue: after N failures, stop retrying and send to a “graveyard” for humans.
