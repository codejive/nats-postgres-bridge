'use strict';

const quote = name => '"' + name.replace(/"/g, '""') + '"';
function createWriter(pool, config) {
  const table = `${quote(config.schema)}.${quote(config.table)}`;
  const columns = [config.timeColumn, config.topicColumn, config.valueColumn].map(quote).join(', ');
  return {
    async validate() {
      const result = await pool.query(`SELECT ${columns} FROM ${table} LIMIT 0`);
      if (result.fields.map(field => field.dataTypeID).join(',') !== '1184,25,25') {
        throw new Error('Target columns must be timestamptz, text, text.');
      }
    },
    async write(timestamp, rows) {
      if (!rows.length) return;
      const client = await pool.connect();
      let broken = false;
      try {
        await client.query('BEGIN');
        for (let offset = 0; offset < rows.length; offset += 1000) {
          const values = [];
          const tuples = rows.slice(offset, offset + 1000).map(([key, value], index) => {
            values.push(timestamp, key, value);
            return `($${index * 3 + 1}, $${index * 3 + 2}, $${index * 3 + 3})`;
          });
          await client.query(`INSERT INTO ${table} (${columns}) VALUES ${tuples.join(', ')}`, values);
        }
        await client.query('COMMIT');
      } catch (error) {
        try { await client.query('ROLLBACK'); } catch { broken = true; }
        throw error;
      } finally {
        client.release(broken);
      }
    }
  };
}
module.exports = {createWriter};
