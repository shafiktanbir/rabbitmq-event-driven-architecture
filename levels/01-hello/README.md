# Mission 1 — Hello message

**Curiosity hook:** What if the service that handles orders is down? With a queue, the order sits there until someone is ready.

## What you’ll use

- **Producer** — Sends messages to a queue
- **Consumer** — Receives messages from a queue
- **Queue as buffer** — Messages survive until a consumer takes them (and until you add persistence in later levels)

## Task

- **Producer:** Sends a few messages to a queue and exits.
- **Consumer:** Listens on the same queue, logs each message, and keeps running.

Run producer and consumer in separate terminals. Try: start consumer first, then producer. Then try: start producer first, then consumer — the messages wait in the queue.

## How to run

1. Terminal 1 (consumer — leave running):

   ```bash
   node levels/01-hello/consumer.js
   ```

2. Terminal 2 (producer):

   ```bash
   node levels/01-hello/producer.js
   ```

3. You should see the consumer print each message. Stop the consumer (Ctrl+C), run the producer again, then start the consumer — messages that were sent while the consumer was down are still delivered.

## What to try next

- Send more messages from the producer and watch the consumer.
- **Next:** Mission 2 — multiple workers sharing the same queue so one slow job doesn’t block the rest.
