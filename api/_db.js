/**
 * Shared Neon PostgreSQL connection for serverless functions
 */
const knex = require('knex');

let db;

function getDb() {
  if (!db) {
    db = knex({
      client: 'pg',
      connection: {
        connectionString: process.env.DATABASE_URL,
        ssl: { rejectUnauthorized: false }
      },
      pool: { min: 0, max: 5 },
      searchPath: ['public']
    });
  }
  return db;
}

module.exports = { getDb };
