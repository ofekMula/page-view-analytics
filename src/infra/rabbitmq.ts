import * as amqp from 'amqplib/callback_api';
import { logger } from '../shared/utils/logger';

export class RabbitMQClient {
  private connection: amqp.Connection | null = null;
  private channel: amqp.Channel | null = null;

  async connect(): Promise<void> {
    try {
      const url = process.env.RABBITMQ_URL || 'amqp://localhost:5672';

      this.connection = await new Promise<amqp.Connection>((resolve, reject) => {
        amqp.connect(url, (err, conn) => {
          if (err) reject(err);
          else resolve(conn);
        });
      });

      this.channel = await new Promise<amqp.Channel>((resolve, reject) => {
        this.connection!.createChannel((err, channel) => {
          if (err) reject(err);
          else resolve(channel);
        });
      });

      logger.info({ url }, 'rabbitmq connected');
    } catch (error) {
      logger.error(
        { error: error instanceof Error ? error.message : String(error) },
        'rabbitmq connection error'
      );
      process.exit(1);
    }
  }

  getChannel(): amqp.Channel {
    if (!this.channel) {
      throw new Error('RabbitMQ channel not initialized');
    }
    return this.channel;
  }

  async close(): Promise<void> {
    try {
      if (this.channel) {
        await new Promise<void>((resolve, reject) => {
          this.channel!.close((err) => {
            if (err) reject(err);
            else resolve();
          });
        });
      }
      if (this.connection) {
        await new Promise<void>((resolve, reject) => {
          this.connection!.close((err) => {
            if (err) reject(err);
            else resolve();
          });
        });
      }
      logger.info('rabbitmq connection closed');
    } catch (error) {
      logger.error(
        { error: error instanceof Error ? error.message : String(error) },
        'error closing rabbitmq connection'
      );
    }
  }
}
