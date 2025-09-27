import { connectDB } from './config/database';
import { RabbitMQClient } from './config/rabbitmq';
import { BATCH_SIZE, FLUSH_INTERVAL_MS, NUM_PARTITIONS } from './consts';
import { AggregatorService } from './services/aggregatorService';


const MAX_RETRIES = 5;
const RETRY_DELAY = 5000;

async function connectWithRetry(fn: () => Promise<void>, service: string): Promise<void> {
  for (let i = 0; i < MAX_RETRIES; i++) {
    try {
      await fn();
      console.log(`Successfully connected to ${service}`);
      return;
    } catch (error) {
      console.error(`Failed to connect to ${service}, attempt ${i + 1}/${MAX_RETRIES}`);
      if (i === MAX_RETRIES - 1) throw error;
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
    }
  }
}

async function startWorker() {
  try {
    // Connect to services with retry
    await connectWithRetry(() => connectDB(), 'PostgreSQL');

    const rabbitmqClient = new RabbitMQClient();
    await connectWithRetry(() => rabbitmqClient.connect(), 'RabbitMQ');

    const aggregators: AggregatorService[] = [];
    for (let i = 0; i < NUM_PARTITIONS; i++) {
      aggregators.push(new AggregatorService(rabbitmqClient, i,BATCH_SIZE, FLUSH_INTERVAL_MS));
    }

    // Start aggregators based on partitions number
    await Promise.all(aggregators.map(a => a.start()));

    console.log(`Aggregator workers for ${NUM_PARTITIONS} partitions started successfully`);
  } catch (err) {
    console.error('Error starting workers:', err);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down workers...');
  process.exit(0);
});

startWorker();
