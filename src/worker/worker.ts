import { connectDB } from '../infra/database';
import { RabbitMQClient } from '../infra/rabbitmq';
import { BATCH_SIZE, FLUSH_INTERVAL_MS, NUM_PARTITIONS } from '../shared/consts';
import { AggregatorService } from '../services/aggregatorService';
import { logger } from '../shared/utils/logger';
import { connectWithRetry } from '../shared/utils/retry'


async function startWorker() {
  logger.info({ partitions: NUM_PARTITIONS }, 'Starting workers');

  try {
    await connectWithRetry(() => connectDB(), 'PostgreSQL');

    const rabbitmqClient = new RabbitMQClient();
    await connectWithRetry(() => rabbitmqClient.connect(), 'RabbitMQ');

    const aggregators: AggregatorService[] = [];
    for (let i = 0; i < NUM_PARTITIONS; i++) {
      aggregators.push(new AggregatorService(rabbitmqClient, i, BATCH_SIZE, FLUSH_INTERVAL_MS));
    }

    await Promise.all(aggregators.map(a => a.start()));

    logger.info({ partitions: NUM_PARTITIONS }, 'All aggregator workers started successfully');

  } catch (err) {
    logger.error(
      { error: err instanceof Error ? err.message : String(err) },
      'failed to start workers'
    ); process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
  logger.warn('shutting down workers');
  process.exit(0);
});

startWorker();
