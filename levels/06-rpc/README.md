# Mission 6 — RPC

**Curiosity hook:** Sometimes you need an answer (e.g. “get user by id”). That’s RPC over a queue.

## What you’ll use

- **Reply queue** — Client creates a private queue for responses. Sends its queue name in the request (via `replyTo`).
- **Correlation id** — Client sends a unique id with the request; server copies it onto the response so the client can match “this response is for that request.”

## Task

- **Client:** Sends a number `n`, waits for the response (e.g. `fib(n)` or `n * n`), prints it, exits.
- **Server (worker):** Consumes from the RPC queue, computes the result (e.g. square or simple fib), publishes to the client’s `replyTo` queue with the same `correlationId`.

## How to run

1. Start the RPC server (leave running):

   ```bash
   node levels/06-rpc/server.js
   ```

2. Run the client:

   ```bash
   node levels/06-rpc/client.js
   ```

3. You should see the client print something like `fib(10) = 55` (or square result). Try different numbers.

## What to try next

- Send multiple requests from the client and ensure responses match (correlation id).
- **Next:** Mission 7 — reliability: acks, nacks, persistent messages, and publish confirms so messages don’t vanish when the broker or worker dies.
