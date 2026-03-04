# Mission 9 — Boss: Mini project

**Curiosity hook:** Can I build something that feels real? Combine patterns and choose: topic vs direct, one queue vs several.

## What you’ll use

- **Topic exchange** — Order lifecycle: `order.created`, `order.reserved`, `order.shipped`.
- **Multiple consumers** — Reserve worker (created → reserved), ship worker (reserved → shipped), optional tracker (all `order.#`).
- **Small HTTP API** — POST an order to trigger the flow; optionally poll or use a simple “status” that reflects the last event (in memory).

## Task

- **API:** One endpoint `POST /orders` with body `{ "item": "Widget" }`. Publishes `order.created` to a topic exchange with payload `{ orderId, item }`. Returns `{ orderId }`.
- **Reserve worker:** Consumes `order.created`, “reserves” (log + delay), publishes `order.reserved` with `{ orderId }`.
- **Ship worker:** Consumes `order.reserved`, “ships” (log + delay), publishes `order.shipped` with `{ orderId }`.
- **Tracker (optional):** Consumes `order.#` and logs every event so you see the full flow in one place.

Run API + both workers (+ optional tracker). POST an order, watch logs across terminals. You’ve built a tiny event-driven order pipeline.

## How to run

1. Start workers and tracker (each in its own terminal):

   ```bash
   node levels/09-mini-project/worker-reserve.js
   node levels/09-mini-project/worker-ship.js
   node levels/09-mini-project/tracker.js
   ```

2. Start the API:

   ```bash
   node levels/09-mini-project/api.js
   ```

3. Create an order:

   ```bash
   curl -X POST http://localhost:3000/orders -H "Content-Type: application/json" -d '{"item":"Cool Widget"}'
   ```

4. Watch reserve → ship → tracker logs. Mission complete.

## What to try next

- Add a second queue for “notifications” (e.g. fan-out from `order.shipped`) to simulate “send email when shipped.”
- Decide: would you use direct or topic for “payment.completed”? Why?
