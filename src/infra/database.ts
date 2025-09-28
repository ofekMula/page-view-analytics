import { Pool } from 'pg';
import dotenv from 'dotenv';
dotenv.config();

import { logger } from "../shared/utils/logger";

export const pool = new Pool({
  user: process.env.POSTGRES_USER || 'postgres',
  password: process.env.POSTGRES_PASSWORD,
  host: process.env.POSTGRES_HOST || 'localhost',
  port: parseInt(process.env.POSTGRES_PORT || '5432'),
  database: process.env.POSTGRES_DB || 'analytics'
});

async function createTables() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS page_views (
        page VARCHAR(255) NOT NULL,
        view_hour TIMESTAMP NOT NULL,
        views INTEGER NOT NULL DEFAULT 0,
        partition INTEGER NOT NULL,
        shard_key SMALLINT NOT NULL DEFAULT 0,
        PRIMARY KEY (page, view_hour, shard_key)
      );
    `);

    logger.info({ table: 'page_views' }, 'database tables ensured');
  } catch (error) {
    logger.error(
      { error: error instanceof Error ? error.message : String(error) },
      'error creating database tables'
    );
    throw error;
  }
}

export const connectDB = async () => {
  try {
    await pool.connect();
    logger.info(
      { host: process.env.POSTGRES_HOST, db: process.env.POSTGRES_DB },
      'postgresql connected'
    );

    await createTables();
  } catch (error) {
    logger.error(
      { error: error instanceof Error ? error.message : String(error) },
      'postgresql connection error'
    );
    process.exit(1);
  }
};
