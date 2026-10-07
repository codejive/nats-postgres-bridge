'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {EventEmitter} = require('node:events');
const {formatError} = require('../errors');
const {loadConfig} = require('../config');
const {run} = require('../bridge');

test('error diagnostics retain nested connection codes and redact credentials', () => {
  const config = loadConfig({NATS_STREAM: 'events', NATS_CONSUMER: 'postgres',
    POSTGRES_URL: 'postgres://admin:secret%21@localhost/db', NATS_TOKEN: 'private-token'});
  const error = new Error('Cannot connect postgres://admin:secret%21@localhost/db private-token', {
    cause: new AggregateError([Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:5432'),
      {code: 'ECONNREFUSED'}), new Error('password secret! rejected')], 'All addresses failed')
  });
  const text = formatError(error, config);
  assert.match(text, /All addresses failed.*connect ECONNREFUSED 127.0.0.1:5432 \[ECONNREFUSED\]/);
  assert.doesNotMatch(text, /admin|secret|private-token/);
  assert.match(formatError(new Error('nats://someone:password@host:4222')), /nats:\/\/\[REDACTED\]@host/);
});

test('startup failures identify the failing operation and retain the original cause', async () => {
  const config = loadConfig({NATS_STREAM: 'events', NATS_CONSUMER: 'postgres', POSTGRES_URL: 'postgres://localhost/db'});
  const failure = Object.assign(new Error('permission denied for table messages'), {code: '42501'});
  class Pool extends EventEmitter {
    async query() { throw failure; }
    async end() {}
  }
  const previousExitCode = process.exitCode;
  try {
    await assert.rejects(run(config, {Pool}), error => {
      assert.equal(error.cause, failure);
      assert.match(formatError(error, config), /validating PostgreSQL target columns: permission denied for table messages \[42501\]/);
      return true;
    });
    await assert.rejects(run({...config, outputDryRun: true}, {connect: async () => {throw failure;}}),
      /connecting to NATS/);
  } finally {
    process.exitCode = previousExitCode;
  }
});
