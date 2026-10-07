'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {flatten, receiptTimestamp} = require('../flatten');
const {loadConfig} = require('../config');
const {createWriter} = require('../database');
const {processMessage} = require('../processor');
const env = {NATS_STREAM: 'events', NATS_CONSUMER: 'postgres', POSTGRES_URL: 'postgres://localhost/db'};

test('flatten nested objects, arrays, escaped keys, empty containers and typed leaves', () => {
  assert.deepEqual(flatten({sensor: {temperature: 22.5, online: true, missing: null}, a: [false, {}, []], 'x/y~z': '', 'a.b': 1}, '/house/'), [
    ['house/sensor/temperature', '22.5'], ['house/sensor/online', 'true'], ['house/sensor/missing', null],
    ['house/a/0', 'false'], ['house/x~1y~0z', ''], ['house/a.b', '1']
  ]);
  assert.deepEqual(flatten({a: {}, b: []}), []);
  for (const value of [null, [], true, 'x']) assert.throws(() => flatten(value));
});
test('timestamp retains microseconds and rounds nanoseconds across second boundaries', () => {
  assert.equal(receiptTimestamp(1700000000123456789n), '2023-11-14T22:13:20.123457Z');
  assert.equal(receiptTimestamp(1700000000999999999n), '2023-11-14T22:13:21.000000Z');
  assert.throws(() => receiptTimestamp(undefined));
});
test('configuration uses compatible aliases and rejects conflicting auth and invalid identifiers', () => {
  const config = loadConfig({...env, NATS_URL: 'nats://localhost:4222', TOPIC_PREFIX: 'home'});
  assert.equal(config.prefix, 'home');
  assert.deepEqual(config.natsOptions.servers, ['nats://localhost:4222']);
  assert.equal(config.valueColumn, 'value');
  assert.equal(loadConfig(env).prefix, '');
  for (const overrides of [{NATS_TOKEN:'t', NATS_USER:'u'}, {POSTGRES_TABLE:'a\0b'}, {POSTGRES_TIME_COLUMN:'value'}, {VERBOSE:'maybe'}, {NATS_BATCH_SIZE:'0'}, {POSTGRES_URL:'https://host'}]) {
    assert.throws(() => loadConfig({...env, ...overrides}));
  }
  assert.throws(() => loadConfig({}));
});
test('database chunks inserts within one transaction and quotes custom identifiers', async () => {
  const calls = [];
  let released = 0;
  const client = {query: async (...args) => calls.push(args), release: () => released++};
  const writer = createWriter({connect: async () => client}, {...loadConfig(env), table:'odd"table'});
  await writer.write('2023-11-14T22:13:20.123457Z', Array.from({length:1001}, (_, index) => ['key/'+index, 'v']));
  assert.equal(calls[0][0], 'BEGIN');
  assert.match(calls[1][0], /"public"\."odd""table"/);
  assert.equal(calls[1][1].length, 3000);
  assert.equal(calls[2][1].length, 3);
  assert.equal(calls[3][0], 'COMMIT');
  assert.equal(released, 1);
});
test('failed insert rolls back and releases the connection', async () => {
  const calls = [];
  let released;
  const client = {query: async sql => {calls.push(sql); if (sql.startsWith('INSERT')) throw Error('failed');}, release: broken => {released = broken;}};
  await assert.rejects(createWriter({connect: async () => client}, loadConfig(env)).write('time', [['key','v']]));
  assert.equal(calls.at(-1), 'ROLLBACK');
  assert.equal(released, false);
});
test('commit precedes acknowledgement and redelivery retains the original timestamp', async () => {
  const calls = [];
  const message = {json: () => ({a:1}), timestampNanos:1700000000123456789n, ack: () => calls.push('ack')};
  const writer = {write: async time => calls.push(time)};
  await processMessage(message, writer, {});
  await processMessage(message, writer, {});
  assert.deepEqual(calls, ['2023-11-14T22:13:20.123457Z', 'ack', '2023-11-14T22:13:20.123457Z', 'ack']);
  calls.length = 0;
  await assert.rejects(processMessage(message, {write: async () => {throw Error('DB unavailable');}}, {}));
  assert.deepEqual(calls, []);
});
test('invalid roots are terminated, empty messages are acknowledged without a database connection', async () => {
  let terminated = false;
  await processMessage({json:()=>[], term:()=>{terminated=true;}}, {}, {}, {error:()=>{}});
  assert.equal(terminated,true);
  let acknowledged = false;
  const writer = createWriter({connect:()=>{throw Error('should not connect');}}, loadConfig(env));
  await processMessage({json:()=>({a:[]}),timestampNanos:0n,ack:()=>{acknowledged=true;}}, writer, {});
  assert.equal(acknowledged,true);
});

test('SIGTERM waits for active commit before draining NATS and closing the pool', async () => {
  const {EventEmitter} = require('node:events');
  const {run} = require('../bridge');
  const events = [];
  let finishCommit;
  let reachedCommit;
  const commitReached = new Promise(resolve => {reachedCommit = resolve;});
  const commitAllowed = new Promise(resolve => {finishCommit = resolve;});
  class FakePool extends EventEmitter {
    async query() { return {fields:[{dataTypeID:1184},{dataTypeID:25},{dataTypeID:25}]}; }
    async connect() {
      return {query:async sql => {
        if (sql === 'COMMIT') {reachedCommit(); await commitAllowed; events.push('commit');}
      }, release:()=>{}};
    }
    async end() {events.push('pool closed');}
  }
  const message = {json:()=>({a:1}), timestampNanos:0n, ack:()=>events.push('ack'), working:()=>{}};
  const messages = {
    stop:()=>events.push('stop consumption'),
    status:async function* () {},
    [Symbol.asyncIterator]:async function* () {yield message;}
  };
  const nc = {closed:()=>new Promise(()=>{}), status:async function* () {}, isClosed:()=>false, drain:async()=>events.push('drain')};
  const consumer = {info:async()=>({config:{ack_policy:'explicit',durable_name:'postgres'}}),consume:async()=>messages};
  const running = run(loadConfig(env), {Pool:FakePool, connect:async()=>nc, jetstream:()=>({consumers:{get:async()=>consumer}})});
  await commitReached;
  process.emit('SIGTERM');
  assert.deepEqual(events,['stop consumption']);
  finishCommit();
  await running;
  assert.ok(events.indexOf('commit') < events.indexOf('ack'));
  assert.ok(events.indexOf('ack') < events.indexOf('drain'));
  assert.ok(events.indexOf('drain') < events.indexOf('pool closed'));
});

test('schema validation rejects incompatible value column types', async () => {
  const writer = createWriter({query:async()=>({fields:[{dataTypeID:1184},{dataTypeID:25},{dataTypeID:3802}]})}, loadConfig(env));
  await assert.rejects(writer.validate(), /timestamptz, text, text/);
});
