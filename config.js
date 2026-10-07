'use strict';

function loadConfig(env = process.env) {
  const get = (name, fallback) => env[name] === undefined || env[name] === '' ? fallback : env[name];
  const bool = (name, fallback) => {
    const value = String(get(name, fallback)).toLowerCase();
    if (!['true', 'false', '1', '0', 'yes', 'no', 'on', 'off'].includes(value)) throw new Error(`${name} must be a boolean.`);
    return ['true', '1', 'yes', 'on'].includes(value);
  };
  const integer = (name, fallback, minimum) => {
    const value = Number(get(name, fallback));
    if (!Number.isSafeInteger(value) || value < minimum) throw new Error(`${name} must be an integer >= ${minimum}.`);
    return value;
  };
  const config = {
    prefix: get('TOPIC_PREFIX', ''),
    verbose: bool('VERBOSE', false),
    stream: get('NATS_STREAM'),
    consumer: get('NATS_CONSUMER'),
    batchSize: integer('NATS_BATCH_SIZE', 100, 1),
    retryDelay: integer('RETRY_DELAY', 3000, 1),
    shutdownTimeout: integer('SHUTDOWN_TIMEOUT', 5000, 1),
    postgres: {
      connectionString: get('POSTGRES_URL', get('DATABASE_URL')),
      max: integer('POSTGRES_POOL_MAX', 5, 1),
      connectionTimeoutMillis: integer('POSTGRES_CONNECT_TIMEOUT', 10000, 1),
      query_timeout: integer('POSTGRES_QUERY_TIMEOUT', 30000, 1),
      statement_timeout: integer('POSTGRES_QUERY_TIMEOUT', 30000, 1)
    },
    schema: get('POSTGRES_SCHEMA', 'public'),
    table: get('POSTGRES_TABLE', 'messages'),
    timeColumn: get('POSTGRES_TIME_COLUMN', 'time'),
    topicColumn: get('POSTGRES_TOPIC_COLUMN', 'topic'),
    valueColumn: get('POSTGRES_VALUE_COLUMN', 'value'),
    natsOptions: {
      servers: get('NATS_SERVERS', get('NATS_URL', 'nats://127.0.0.1:4222')).split(',').map(value => value.trim()),
      name: get('NATS_NAME', 'nats-postgres-bridge'),
      reconnect: true,
      reconnectTimeWait: integer('NATS_RECONNECT_TIME_WAIT', 3000, 0),
      maxReconnectAttempts: integer('NATS_MAX_RECONNECT_ATTEMPTS', -1, -1),
      timeout: integer('NATS_CONNECT_TIMEOUT', 10000, 1)
    }
  };
  for (const [name, value] of Object.entries({NATS_STREAM: config.stream, NATS_CONSUMER: config.consumer, POSTGRES_URL: config.postgres.connectionString})) {
    if (!value) throw new Error(name + ' is required.');
  }
  const url = new URL(config.postgres.connectionString);
  if (!['postgres:', 'postgresql:'].includes(url.protocol)) throw new Error('POSTGRES_URL must be a PostgreSQL URL.');
  if (config.natsOptions.servers.some(server => !server)) throw new Error('NATS_SERVERS must contain nonempty server addresses.');
  for (const name of ['schema', 'table', 'timeColumn', 'topicColumn', 'valueColumn']) {
    if (config[name].includes('\0') || Buffer.byteLength(config[name]) > 63) throw new Error(name + ' is not a valid PostgreSQL identifier.');
  }
  if (new Set([config.timeColumn, config.topicColumn, config.valueColumn]).size !== 3) throw new Error('Column names must be distinct.');
  for (const value of [config.retryDelay, config.shutdownTimeout, config.postgres.connectionTimeoutMillis, config.postgres.query_timeout, config.natsOptions.timeout, config.natsOptions.reconnectTimeWait]) {
    if (value > 2147483647) throw new Error('Timing values must be <= 2147483647 milliseconds.');
  }
  const user = get('NATS_USER');
  const pass = get('NATS_PASS');
  const token = get('NATS_TOKEN');
  const creds = get('NATS_CREDS');
  if ([Boolean(user || pass), Boolean(token), Boolean(creds)].filter(Boolean).length > 1) throw new Error('Choose one NATS authentication method: user/pass, token, or credentials.');
  if (pass && !user) throw new Error('NATS_PASS requires NATS_USER.');
  Object.assign(config.natsOptions, {user, pass, token});
  config.natsCreds = creds;
  const ca = get('NATS_TLS_CA');
  const cert = get('NATS_TLS_CERT');
  const key = get('NATS_TLS_KEY');
  if (Boolean(cert) !== Boolean(key)) throw new Error('NATS_TLS_CERT and NATS_TLS_KEY must be set together.');
  if (bool('NATS_TLS', false) || ca || cert || key) config.natsOptions.tls = {ca, cert, key};
  return config;
}

module.exports = {loadConfig};
