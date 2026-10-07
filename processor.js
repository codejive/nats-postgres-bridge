'use strict';
const {flatten, receiptTimestamp} = require('./flatten');
async function processMessage(message, writer, config, logger = console) {
  let rows;
  try {
    rows = flatten(message.json(), config.prefix);
  } catch {
    logger.error(`Invalid JSON object: subject=${message.subject} sequence=${message.seq}; terminating delivery.`);
    message.term();
    return;
  }
  const timestamp = receiptTimestamp(message.timestampNanos);
  await writer.write(timestamp, rows);
  message.ack();
  if (config.verbose) logger.log(`Stored sequence=${message.seq} rows=${rows.length}`);
}
module.exports = {processMessage};
