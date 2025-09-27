import fastify from 'fastify';
import { TypeBoxTypeProvider } from '@fastify/type-provider-typebox';
import { connectDB } from './config/database';
import { RabbitMQClient } from './config/rabbitmq';
import { PageViewService } from './services/pageViewService';
import { SinglePageViewSchema, MultiPageViewSchema, ReportResponseSchema } from './types';
import { NUM_PARTITIONS } from './consts';

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

const server = fastify({
  logger: true
}).withTypeProvider<TypeBoxTypeProvider>();

const rabbitmqClient = new RabbitMQClient();

// Health check route
server.get('/health', async () => {
  return { status: 'ok' };
});

const start = async () => {
  try {
    // Connect to services with retry
    await connectWithRetry(() => connectDB(), 'PostgreSQL');
    await connectWithRetry(() => rabbitmqClient.connect(), 'RabbitMQ');

    const pageViewService = new PageViewService(rabbitmqClient, NUM_PARTITIONS);

    // Register routes after successful connection
    server.post('/page-views/single', {
      schema: {
        body: SinglePageViewSchema
      }
    }, async (request) => {
      await pageViewService.incrementSingleView(request.body.page, request.body.timestamp);
      return { success: true };
    });

    server.post('/page-views/multi', {
      schema: {
        body: MultiPageViewSchema
      }
    }, async (request) => {
      await pageViewService.incrementMultipleViews(request.body);
      return { success: true };
    });

    server.get<{
      Querystring: { page: string, now?: string, order?: 'asc' | 'desc', take?: number }
    }>('/report', {
      schema: {
        response: {
          200: ReportResponseSchema
        }
      }
    }, async (request) => {
      const { page, now, order = 'asc', take } = request.query;

      const data = await pageViewService.getReport(page, now, order, take);
      return { data };
    });

    // Start the server
    await server.listen({ port: 3000, host: '0.0.0.0' });
    console.log('Server is running on port 3000');
  } catch (err) {
    server.log.error(err);
    process.exit(1);
  }
};

// Handle graceful shutdown
process.on('SIGINT', async () => {
  await rabbitmqClient.close();
  process.exit(0);
});

start();
