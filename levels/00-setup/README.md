# Mission 0 — Setup

**Curiosity hook:** Servers don’t only call each other with HTTP — they push events into a broker. Here you’ll open the pipe.

## What you’ll use

- **Connection** — TCP link to the RabbitMQ server
- **Channel** — Lightweight “session” on top of the connection (you do work on channels)
- **Queue** — A named mailbox. Declaring it creates it if it doesn’t exist.

## Task

Run a script that connects to the broker, creates a channel, declares a queue, sends one message, and exits.

## How to run

1. Ensure RabbitMQ is running: `npm run rabbit` (or your Docker command).
2. From repo root:

   ```bash
   node levels/00-setup/send.js
   ```

3. You should see a log like `Sent: Hello RabitPlay!` and the script exits. The message is now in the queue (you can confirm in the [management UI](http://localhost:15672) under Queues).

## What to try next

- Change the queue name and run again — you’ll see a new queue in the UI.
- **Next:** Mission 1 — add a consumer that receives the message and see what happens when the receiver is offline.
