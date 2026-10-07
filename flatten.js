'use strict';

function flatten(object, prefix = '') {
  if (!object || typeof object !== 'object' || Array.isArray(object)) throw new Error('Message root must be a JSON object.');
  const rows = [];
  const escape = key => key.replace(/~/g, '~0').replace(/\//g, '~1');
  const visit = (value, path) => {
    if (value !== null && typeof value === 'object') {
      for (const [key, child] of Object.entries(value)) visit(child, [...path, escape(key)]);
    } else {
      rows.push([path.join('/'), value === null ? null : String(value)]);
    }
  };
  const base = prefix.replace(/\{([^{}]+)\}/g, (_, path) => {
    let value = object;
    for (const segment of path.split('/')) {
      const key = segment.replace(/~1/g, '/').replace(/~0/g, '~');
      if (value === null || typeof value !== 'object' || !Object.hasOwn(value, key)) {
        throw new Error('Topic prefix placeholder does not resolve to a message value.');
      }
      value = value[key];
    }
    if (value === null || typeof value === 'object') {
      throw new Error('Topic prefix placeholder must reference a string, number or boolean.');
    }
    return escape(String(value));
  }).replace(/^\/+|\/+$/g, '');
  for (const [key, value] of Object.entries(object)) visit(value, [...(base ? [base] : []), escape(key)]);
  return rows;
}

function receiptTimestamp(nanos) {
  if (typeof nanos !== 'bigint' || nanos < 0n) throw new Error('Missing or invalid NATS timestamp.');
  const micros = (nanos + 500n) / 1000n;
  const seconds = micros / 1000000n;
  return new Date(Number(seconds * 1000n)).toISOString().replace(/\.\d{3}Z$/, `.${String(micros % 1000000n).padStart(6, '0')}Z`);
}

module.exports = {flatten, receiptTimestamp};
