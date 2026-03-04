# Mission 2 — Work queue

**Curiosity hook:** One slow “send email” shouldn’t block 100 “update profile” tasks. See how the broker shares work.

## What you’ll use

- **Fair dispatch** — RabbitMQ sends a message to the next consumer that’s free, not round-robin. A busy consumer gets fewer messages.
- **Prefetch** — `prefetch(1)` means “don’t give me another message until I ack the current one.” Keeps work balanced.

## Task

- **Producer:** Sends several tasks (e.g. strings like `"task 1"`, `"task 2"`, …), with a few “slow” tasks (e.g. dots or a delay in the message so the worker can simulate work).
- **Worker:** Consumes one message at a time, “processes” it (e.g. log and sleep 1s for messages containing `"slow"`, instant for others), then acks.

Run one producer and two (or more) workers. See that slow tasks don’t block fast ones — the other worker takes the next message.

## How to run

1. Start two workers in separate terminals:

   ```bash
   node levels/02-work-queue/worker.js
   ```

2. Run the producer:

   ```bash
   node levels/02-work-queue/producer.js
   ```

3. Watch how tasks are distributed. Workers with prefetch(1) get one message at a time; when one is busy with a “slow” task, the other gets the next messages.

## What to try next

- Add a third worker and see the distribution.
- **Next:** Mission 3 — fan-out exchange so you publish once and multiple different consumers (e.g. logger, email) all get the same event.
