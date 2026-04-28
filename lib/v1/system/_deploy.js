/**
 * Deploy log helper. Every shipped engine should log via deployLog() per RULES.md.
 * Adds to system_changelog with kind='deploy'.
 */
const { getDb } = require('../../_db');

async function deployLog({ name, version, summary, ref, files }) {
  const db = getDb();
  try {
    await db('system_changelog').insert({
      kind: 'deploy',
      title: `Deploy: ${name}@${version || 'main'}`,
      summary: summary || '',
      ref: ref || null,
      meta: JSON.stringify({ name, version, files: files || [] }),
      author: 'system',
      created_at: new Date()
    });
    return { ok: true };
  } catch (e) { return { ok: false, error: e.message }; }
}

module.exports = { deployLog };
