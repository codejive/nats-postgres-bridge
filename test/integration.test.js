'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {randomUUID} = require('node:crypto');
const {connect} = require('@nats-io/transport-node');
const {jetstream, jetstreamManager} = require('@nats-io/jetstream');
const {Pool} = require('pg');
const {loadConfig} = require('../config');
const {createWriter} = require('../database');
const {processMessage} = require('../processor');

test('real JetStream replay writes original timestamps and nullable text values', {
  skip: !process.env.NATS_TEST_URL || !process.env.POSTGRES_TEST_URL,
  timeout: 30000
}, async () => {
  const id = randomUUID().replaceAll('-', '');
  const stream = 'TEST_' + id;
  const table = 'test_' + id;
  const subject = 'test.' + id;
  const pool = new Pool({connectionString: process.env.POSTGRES_TEST_URL});
  let nc;
  let manager;
  let createdStream = false;
  let createdTable = false;
  try {
    nc = await connect({servers:process.env.NATS_TEST_URL});
    manager = await jetstreamManager(nc);
    await manager.streams.add({name:stream, subjects:[subject]});
    createdStream = true;
    await manager.consumers.add(stream, {durable_name:'postgres', ack_policy:'explicit', ack_wait:1000000000});
    await pool.query(`CREATE TABLE "${table}" (time timestamptz NOT NULL, topic text NOT NULL, value text)`);
    createdTable = true;
    const config = loadConfig({NATS_STREAM:stream, NATS_CONSUMER:'postgres', POSTGRES_URL:process.env.POSTGRES_TEST_URL, POSTGRES_TABLE:table, TOPIC_PREFIX:'home'});
    const writer = createWriter(pool, config);
    await writer.validate();
    const js = jetstream(nc);
    await js.publish(subject, Buffer.from('{"nested":{"a":12,"b":null},"empty":[]}'));
    const consumer = await js.consumers.get(stream, 'postgres');
    const message = await consumer.next({expires:3000});
    assert.ok(message);
    const original = message.timestampNanos;
    // Force redelivery before writing, verifying the metadata survives replay.
    message.nak(1);
    const replay = await consumer.next({expires:3000});
    assert.ok(replay);
    assert.equal(replay.timestampNanos, original);
    await processMessage(replay, writer, config);
    const result = await pool.query(`SELECT to_char(time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS time, topic, value FROM "${table}" ORDER BY topic`);
    const {receiptTimestamp} = require('../flatten');
    assert.deepEqual(result.rows, [
      {time:receiptTimestamp(original), topic:'home/nested/a', value:'12'},
      {time:receiptTimestamp(original), topic:'home/nested/b', value:null}
    ]);
  } finally {
    try { if (createdStream) await manager.streams.delete(stream); }
    finally {
      try { if (nc) await nc.close(); if (createdTable) await pool.query(`DROP TABLE "${table}"`); }
      finally { await pool.end(); }
    }
  }
});
