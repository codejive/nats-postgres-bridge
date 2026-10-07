'use strict';
const {connect, credsAuthenticator} = require('@nats-io/transport-node');
const {jetstream} = require('@nats-io/jetstream');
const {Pool} = require('pg');
const {setTimeout: delay} = require('node:timers/promises');
const {loadConfig} = require('./config');
const {createWriter} = require('./database');
const {processMessage} = require('./processor');

async function run(config, dependencies = {}) {
  const pool = config.outputDryRun ? undefined : new (dependencies.Pool || Pool)(config.postgres);
  let nc;
  let messages;
  let stopping = false;
  let shutdownTimer;
  const abort = new AbortController();
  const stop = (code = 0) => {
    if (stopping) return;
    stopping = true;
    process.exitCode = code;
    abort.abort();
    messages?.stop();
    shutdownTimer = setTimeout(() => process.exit(process.exitCode || 0), config.shutdownTimeout);
    shutdownTimer.unref();
  };
  const onSignal = () => stop();
  process.on('SIGINT', onSignal);
  process.on('SIGTERM', onSignal);
  pool?.on('error', () => { console.error('PostgreSQL pool connection failed.'); stop(1); });
  try {
    const writer = pool && createWriter(pool, config);
    if (writer) await writer.validate();
    if (config.outputDryRun) console.log('Output dry run enabled: database access and message acknowledgements are disabled.');
    if (stopping) return;
    const options = {...config.natsOptions};
    if (config.natsCreds) options.authenticator = credsAuthenticator(Buffer.from(config.natsCreds));
    nc = await (dependencies.connect || connect)(options);
    if (stopping) return;
    nc.closed().then(() => { if (!stopping) { console.error('NATS connection closed.'); stop(1); } });
    const monitoring = (async () => {
      for await (const status of nc.status()) console.log(`NATS ${status.type}`);
    })();
    monitoring.catch(() => stop(1));
    const consumer = await (dependencies.jetstream || jetstream)(nc).consumers.get(config.stream, config.consumer);
    const info = await consumer.info();
    if (info.config.ack_policy !== 'explicit' || info.config.deliver_subject || !info.config.durable_name) {
      throw new Error('An existing durable pull consumer with explicit acknowledgements is required.');
    }
    const ackWaitMillis = Math.floor((info.config.ack_wait || 30000000000) / 1000000);
    if (ackWaitMillis < 300) throw new Error('Consumer ack_wait must be at least 300 milliseconds.');
    while (!stopping) {
      messages = await consumer.consume({max_messages: config.batchSize});
      const activeMessages = messages;
      if (stopping) { messages.stop(); break; }
      const statusTask = (async () => {
        for await (const status of activeMessages.status()) {
          if (status.type === 'heartbeats_missed' && status.count >= 2) activeMessages.stop();
        }
      })();
      statusTask.catch(() => activeMessages.stop());
      console.log('Bridge running. Waiting for JSON messages.');
      for await (const message of messages) {
        if (stopping) break;
        const progress = config.outputDryRun ? undefined : setInterval(() => message.working(), Math.max(100, Math.floor(ackWaitMillis / 3)));
        try {
          await processMessage(message, writer, config);
        } catch {
          console.error(`Message write failed: sequence=${message.seq}; will retry.`);
          if (!config.outputDryRun) message.nak(config.retryDelay);
          await delay(config.retryDelay, undefined, {signal: abort.signal}).catch(() => {});
        } finally {
          clearInterval(progress);
        }
      }
      if (!stopping) await delay(config.retryDelay, undefined, {signal: abort.signal}).catch(() => {});
    }
  } catch (error) {
    stop(1);
    throw error;
  } finally {
    stopping = true;
    messages?.stop();
    try { if (nc && !nc.isClosed()) await nc.drain(); }
    finally {
      await pool?.end();
      clearTimeout(shutdownTimer);
      process.off('SIGINT', onSignal);
      process.off('SIGTERM', onSignal);
    }
  }
}

if (require.main === module) {
  let config;
  try { config = loadConfig(); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
  if (config) run(config).catch(() => {
    console.error('Bridge failed; check service connectivity, table types and consumer configuration.');
    process.exitCode = 1;
  });
}
module.exports = {run};
