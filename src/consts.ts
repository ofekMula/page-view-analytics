export const NUM_PARTITIONS = parseInt(process.env.NUM_PARTITIONS || '2');
export const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '100');
export const FLUSH_INTERVAL_MS = parseInt(process.env.FLUSH_INTERVAL_MS || '5000');
