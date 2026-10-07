'use strict';
const {flatten, receiptTimestamp} = require('./flatten');
async function processMessage(message, writer, config, logger = console) {
  let msg;
  try {
    msg = message.json();
  } catch {
    logger.error(`Invalid JSON: subject=${message.subject} sequence=${message.seq}; ${config.outputDryRun ? 'dry run skipped delivery' : 'terminating delivery'}.`);
    if (!config.outputDryRun) message.term();
    return;
  }
  if (config.inputFilter) {
    let accepted;
    try {
      accepted = config.inputFilter(msg);
    } catch {
      logger.error(`INPUT_FILTER evaluation failed: subject=${message.subject} sequence=${message.seq}; ${config.outputDryRun ? 'dry run skipped delivery' : 'terminating delivery'}.`);
      if (!config.outputDryRun) message.term();
      return;
    }
    if (!accepted) {
      if (!config.outputDryRun) message.ack();
      if (config.verbose) logger.log(`Filtered sequence=${message.seq}`);
      return;
    }
  }
  let rows;
  try {
    rows = flatten(msg, config.prefix, config.outputKeys);
  } catch {
    logger.error(`Invalid JSON object or topic prefix placeholder: subject=${message.subject} sequence=${message.seq}; ${config.outputDryRun ? 'dry run skipped delivery' : 'terminating delivery'}.`);
    if (!config.outputDryRun) message.term();
    return;
  }
  const timestamp = receiptTimestamp(message.timestampNanos);
  if (!config.outputDryRun) {
    await writer.write(timestamp, rows);
    message.ack();
  }
  if (config.verbose) logger.log(`${config.outputDryRun ? 'Dry run' : 'Stored'} sequence=${message.seq} rows=${rows.length}`);
}
module.exports = {processMessage};
