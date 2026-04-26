/**
 * Batch insert helper — handles chunking, conflict resolution
 *
 * Replaces per-record INSERT loops with chunked batch inserts.
 * Default chunk size 100 rows.
 */
async function batchInsert(db, table, rows, options = {}) {
  if (!rows || rows.length === 0) return { inserted: 0 };
  const chunkSize = options.chunkSize || 100;
  const onConflict = options.onConflict || 'ignore';
  let inserted = 0;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    try {
      let q = db(table).insert(chunk);
      if (onConflict === 'ignore' && options.conflictColumn) {
        q = q.onConflict(options.conflictColumn).ignore();
      } else if (onConflict === 'merge' && options.conflictColumn) {
        q = q.onConflict(options.conflictColumn).merge();
      }
      await q;
      inserted += chunk.length;
    } catch (err) {
      if (options.retryIndividual !== false) {
        for (const row of chunk) {
          try { await db(table).insert(row); inserted++; }
          catch (rowErr) { if (options.onError) options.onError(row, rowErr); }
        }
      } else { throw err; }
    }
  }
  return { inserted };
}
async function batchUpsert(db, table, rows, conflictColumn, mergeColumns) {
  if (!rows || rows.length === 0) return { upserted: 0 };
  const chunkSize = 100;
  let upserted = 0;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    let q = db(table).insert(chunk).onConflict(conflictColumn);
    q = mergeColumns ? q.merge(mergeColumns) : q.merge();
    await q;
    upserted += chunk.length;
  }
  return { upserted };
}
module.exports = { batchInsert, batchUpsert };
