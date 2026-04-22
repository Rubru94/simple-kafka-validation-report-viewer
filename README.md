# validator-reports

Small TypeScript + Express app that reads Kafka messages from a configured topic, starting at a user-provided offset, and builds a simple validation report (`valid === true` vs `valid === false`).

## Requirements

- Node.js 18+
- Access to a Kafka cluster/topic with JSON messages

## Setup

1. Install dependencies:

   ```bash
   npm install
   ```

2. Create your local env file:

   ```bash
   cp .env.example .env
   ```

3. Update `.env` values:

   - `KAFKA_BROKERS`: comma-separated brokers (e.g. `localhost:9092`)
   - `KAFKA_CLIENT_ID`: client id used by KafkaJS
   - `KAFKA_GROUP_ID`: base group id (the app appends a unique suffix per request)
   - `TOPIC_NAME`: topic to read
   - `PORT`: web server port

## Run

Development mode:

```bash
npm run dev
```

Build:

```bash
npm run build
```

Run built app:

```bash
npm start
```

Then open:

`http://localhost:3000` (or your configured `PORT`)

## How it works

- UI form accepts a starting offset (non-negative integer).
- Backend reads topic end offsets per partition.
- Consumer seeks to the provided start offset and processes messages up to each partition's latest offset at query time.
- Each message is parsed as JSON:
  - if `valid === true`, increments the `true` counter
  - if `valid === false`, increments the `false` counter
  - otherwise ignored
- Page renders:
  - numeric totals in a header/summary section
  - a bar chart (Chart.js CDN) for true vs false

## Notes / assumptions

- Only messages in the range `[startOffset, latestOffsetExclusive)` are considered for each partition.
- `latestOffsetExclusive` is captured at request start, so newly arriving messages during processing are not included.
- Invalid JSON or messages without a boolean `valid` field are ignored.
