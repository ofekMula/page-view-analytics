import fastify from 'fastify';
import { TypeBoxTypeProvider } from '@fastify/type-provider-typebox';

import { connectDB } from '../infra/database';
import { RabbitMQClient } from '../infra/rabbitmq';
import { connectWithRetry } from '../shared/utils/retry';

import { PageViewService } from '../services/pageViewService';
import { NUM_PARTITIONS } from '../shared/consts';
import { registerPageViewRoutes } from './routes/pageViews.routes';
import { logger } from '../shared/utils/logger';

const PORT = Number(process.env.PORT ?? 3000);
const rabbitmqClient = new RabbitMQClient();

async function start() {
  logger.info({ port: PORT, partitions: NUM_PARTITIONS }, 'starting service');

  try {
    await connectWithRetry(() => connectDB(), 'PostgreSQL');
    await connectWithRetry(() => rabbitmqClient.connect(), 'RabbitMQ');

    const app = fastify({
      logger: false
    }).withTypeProvider<TypeBoxTypeProvider>();

    app.get('/health', async () => ({ status: 'ok' }));

    const pageViewService = new PageViewService(rabbitmqClient, NUM_PARTITIONS);
    registerPageViewRoutes(app, { pageViewService });

    const shutdown = async () => {
      try { await rabbitmqClient.close(); } catch { }
      try { await app.close(); } catch { }
      process.exit(0);
    };

    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);

    await app.listen({ port: PORT, host: '0.0.0.0' });

  } catch (err) {
    // eslint-disable-next-line no-console
    logger.error({ error: err instanceof Error ? err.message : String(err) }, 'startup failed');
    process.exit(1);
  }
}
start();
