'use strict';

function formatError(error, config = {}) {
  const secrets = [config.postgres?.connectionString, config.natsCreds,
    config.natsOptions?.user, config.natsOptions?.pass, config.natsOptions?.token];
  for (const address of [config.postgres?.connectionString, ...(config.natsOptions?.servers || [])]) {
    if (!address) continue;
    try {
      const url = new URL(address);
      for (const value of [url.username, url.password]) {
        if (value) secrets.push(value, decodeURIComponent(value));
      }
    } catch {}
  }
  const seen = new Set();
  const describe = value => {
    if (value == null) return 'Unknown error';
    if (seen.has(value)) return '[circular error]';
    seen.add(value);
    const message = typeof value === 'object' ? value.message || value.name || 'Unknown error' : String(value);
    const code = value.code ? ` [${value.code}]` : '';
    const children = [...(value.cause ? [value.cause] : []), ...(Array.isArray(value.errors) ? value.errors : [])];
    return `${message}${code}${children.length ? ': ' + children.map(describe).join('; ') : ''}`;
  };
  let text = describe(error);
  for (const secret of secrets.filter(Boolean).sort((a, b) => b.length - a.length)) {
    text = text.split(secret).join('[REDACTED]');
  }
  return text.replace(/([a-z][a-z0-9+.-]*:\/\/)[^\s/@]+@/gi, '$1[REDACTED]@').replace(/[\r\n]+/g, ' ');
}

module.exports = {formatError};
