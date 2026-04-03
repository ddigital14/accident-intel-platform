const knex = require('knex');
require('dotenv').config();

const db = knex({
  client: 'pg',
  connection: process.env.DATABASE_URL || {
    host: process.env.DB_HOST || 'localhost',
    port: process.env.DB_PORT || 5432,
    database: process.env.DB_NAME || 'accident_intel',
    user: process.env.DB_USER || 'aip_user',
    password: process.env.DB_PASSWORD,
    ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : false
  },
  pool: {
    min: 2,
    max: 20,
    acquireTimeoutMillis: 30000
  },
  searchPath: ['public']
});

module.exports = db;
