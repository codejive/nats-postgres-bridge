# NATS to PostgreSQL Bridge

Consume JSON objects from an existing NATS JetStream durable pull consumer and store their leaf values in PostgreSQL. Each message becomes triples of the original NATS server receipt timestamp, slash-separated JSON path, and text value. Empty objects and arrays are dropped.

NATS connection configuration and lifecycle handling follow [ewelink-nats-bridge](https://github.com/codejive/ewelink-nats-bridge); Node tests and Docker workflows follow the same project, adapted from [ewelink-mqtt-bridge](https://github.com/codejive/ewelink-mqtt-bridge).

## Setup

1. Provision PostgreSQL using [schema.sql](schema.sql), or create an equivalent table with your configured names. The column types must be `timestamptz`, `text`, `text`; the value column must allow SQL NULL. Grant the bridge account SELECT and INSERT access. The bridge validates columns but does not create or alter tables.
2. Enable JetStream on NATS and provision a stream capturing JSON object subjects and a durable **pull** consumer with **explicit** acknowledgements. Choose delivery policy and retention to match your needs. Set its acknowledgement timeout comfortably above your normal transaction time. Configure `max_ack_pending` and retry/delivery limits for your workload.
3. Copy `.env.example` to `.env`, and configure service URLs, stream and consumer names. NATS and PostgreSQL must be reachable from the bridge; container localhost refers to that container.
4. Run `npm ci`, then `node --env-file=.env bridge.js`, or run `docker compose up -d --build`. `npm start` reads the process environment; it does not load `.env` automatically.

For the eWeLink NATS producer, capture raw JSON subjects such as `ewelink.*.state.raw`, and optionally set `PUBLISH_RAW_STATE=only`. Raw payloads contain the full action, including `deviceid` and `params`; individual-key publications are not JSON objects. The source subject is not added to stored keys. Use an appropriate static prefix if you need a namespace.

## Data mapping

With `TOPIC_PREFIX=house`, `{"sensor":{"temperature":22.5,"online":true,"missing":null},"samples":[1,2],"empty":{}}` produces:

| topic | value |
| --- | --- |
| house/sensor/temperature | `22.5` |
| house/sensor/online | `true` |
| house/sensor/missing | SQL NULL |
| house/samples/0 | `1` |
| house/samples/1 | `2` |

All rows share the original JetStream message timestamp, obtained from `timestampNanos`, including replays and redeliveries. Nanoseconds are rounded to PostgreSQL microseconds without converting the fractional timestamp through JavaScript Date. See the [NATS message API](https://nats-io.github.io/nats.js/jetstream/types/JsMsg.html).

Nested objects are traversed recursively, arrays use zero-based index segments, strings are stored unchanged, and numbers and booleans use JavaScript string conversion. Numbers follow JSON.parse/JavaScript numeric precision. Empty containers produce no rows; an entirely empty message is acknowledged without a database write. Literal `~` and `/` in object keys are escaped to `~0` and `~1`. Dots remain literal. Empty property names are preserved as empty path segments. Leading/trailing slashes on the static prefix are removed.

## Environment variables

All configuration comes from environment variables. Empty variables use their fallback/default. Boolean values accept `true/false`, `1/0`, `yes/no`, and `on/off`.

| Variable | Default | Description |
| --- | --- | --- |
| NATS_SERVERS | nats://127.0.0.1:4222 | Comma-separated servers; `NATS_URL` fallback alias. |
| NATS_USER / NATS_PASS | empty | User/password authentication. |
| NATS_TOKEN | empty | Token authentication. |
| NATS_CREDS | empty | Credentials file **contents**, not a path. |
| NATS_NAME | nats-postgres-bridge | Client name. |
| NATS_TLS | false | Require TLS. |
| NATS_TLS_CA / NATS_TLS_CERT / NATS_TLS_KEY | empty | Certificate file paths; certificate and key must be paired. |
| NATS_CONNECT_TIMEOUT | 10000 | Initial connection timeout in milliseconds. |
| NATS_RECONNECT_TIME_WAIT | 3000 | Reconnect delay in milliseconds. |
| NATS_MAX_RECONNECT_ATTEMPTS | -1 | Per-server retry limit; -1 unlimited, 0 disables retries. |
| NATS_STREAM / NATS_CONSUMER | required | Existing stream and durable pull consumer. |
| NATS_BATCH_SIZE | 100 | Consumer buffer size; processing is sequential. |
| TOPIC_PREFIX | empty | Static path prefix. |
| POSTGRES_URL | required | PostgreSQL connection URL; `DATABASE_URL` fallback alias. URL supports PostgreSQL TLS options. |
| POSTGRES_SCHEMA | public | Schema name. |
| POSTGRES_TABLE | messages | Table name, separate from schema. |
| POSTGRES_TIME_COLUMN | time | Timestamp column. |
| POSTGRES_TOPIC_COLUMN | topic | Path column. |
| POSTGRES_VALUE_COLUMN | value | Nullable text column. |
| POSTGRES_POOL_MAX | 5 | Maximum pooled connections. |
| POSTGRES_CONNECT_TIMEOUT | 10000 | Database connection timeout in milliseconds. |
| POSTGRES_QUERY_TIMEOUT | 30000 | Client query and server statement timeout in milliseconds. |
| RETRY_DELAY | 3000 | Delayed negative acknowledgement and consumption backoff in milliseconds. |
| SHUTDOWN_TIMEOUT | 5000 | Maximum graceful shutdown duration in milliseconds. |
| VERBOSE | false | Log successful message sequence and row count. |

Choose only one NATS authentication method. URLs and message values are not logged. Schema, table, and column names are safely quoted; values are parameterized.

## Delivery and operation

All rows from a message are inserted in one transaction, in chunks of at most 1000 rows. The bridge acknowledges after commit, rolls back on failure, and negatively acknowledges for retry. It sends progress acknowledgements during processing. A crash after commit but before acknowledgement may insert duplicates on redelivery: delivery is at least once, without database deduplication. Buffered messages may also be redelivered if their acknowledgement timeout expires before processing begins; reduce batch size or increase the consumer acknowledgement timeout when necessary.

Malformed JSON and non-object roots are logged by subject and sequence and terminally acknowledged to avoid endless retries. Archive input in your stream if these messages need later inspection. Permanent database errors remain retryable until fixed or the consumer's configured delivery limit is reached. Monitor server advisories when using a finite delivery limit.

NATS reconnects automatically. Loss of the connection after retries are exhausted exits with failure. Missing consumer heartbeats restart consumption. SIGINT/SIGTERM stops consumption, waits for the active write, drains NATS, and closes PostgreSQL, with a bounded shutdown deadline. Container restart policy handles process failures.

## Development and releases

`npm test` runs the Node built-in test suite. Optional service integration tests run when `NATS_TEST_URL` and `POSTGRES_TEST_URL` are both set; use a dedicated test database and NATS server. They provision and remove isolated test resources.

Build Check runs on main pushes, pull requests, and manual dispatch: Node 24, npm ci, syntax checks, tests, and Docker builds for linux/amd64 and linux/arm64. Docker Release publishes `v*` tags using `DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN`, and `DOCKERHUB_NAMESPACE`, matching the sibling workflows. Configure the namespace before publishing.

Licensed under Apache-2.0; see [LICENSE](LICENSE).
