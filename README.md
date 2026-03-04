# RabitPlay — Learn RabbitMQ by Doing

A gamified, level-by-level RabbitMQ curriculum for the Node.js ecosystem. Each **mission** introduces one concept, answers "why would I care?", and gives you runnable code so you build muscle memory and curiosity.

## Prerequisites

- **Node.js 18+**
- **Docker** (for RabbitMQ)

## Quick start

1. **Start RabbitMQ**

   ```bash
   npm run rabbit
   ```

   Or manually: `docker run -d -p 5672:5672 -p 15672:15672 rabbitmq:3-management`

2. **Management UI** (optional): open [http://localhost:15672](http://localhost:15672) — login `guest` / `guest`. Watch queues and messages in real time.

3. **Pick a mission** under `levels/` and follow its README.

## Missions (levels)

| Level | Mission        | Concept                    |
|-------|----------------|----------------------------|
| 0     | Setup          | Connection, channel, queue |
| 1     | Hello message  | Producer, consumer, buffer |
| 2     | Work queue     | Fair dispatch, prefetch    |
| 3     | Fan-out        | Exchange, pub/sub          |
| 4     | Direct         | Routing keys               |
| 5     | Topics         | Topic patterns             |
| 6     | RPC            | Request/reply              |
| 7     | Reliability    | Acks, confirms             |
| 8     | Dead letters   | DLX, retries               |
| 9     | **Boss**       | Mini project — combine all |

Run scripts from the repo root, e.g.:

```bash
node levels/00-setup/send.js
node levels/01-hello/producer.js
node levels/01-hello/consumer.js
```

## Structure

```
rabitplay/
├── package.json
├── docker-compose.yml
├── levels/           # One folder per mission
└── shared/           # Reusable connection helper
```

Have fun. One mission at a time.
