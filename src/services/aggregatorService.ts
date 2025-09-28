import { Channel, Message } from "amqplib/callback_api";
import { pool } from "../infra/database";
import { RabbitMQClient } from "../infra/rabbitmq";
import { logger } from '../shared/utils/logger';
import { Aggregation } from "../types";

interface PageViewMessage {
  page: string;
  timestamp: string;
  views: number;
  partition: number;
  shard_key: number;
}

type BufferedMessage = PageViewMessage & { raw: Message };

export class AggregatorService {
  private channel: Channel;
  private partition: number;
  private flushIntervalMs = 5000; // flush every 5s
  private batchSize = 100;
  private messageBuffer: BufferedMessage[] = [];
  private flushTimer: NodeJS.Timeout | null = null;

  constructor(
    rabbitmqClient: RabbitMQClient,
    partition: number,
    batchSize: number,
    flushIntervalMs: number,
  ) {
    this.channel = rabbitmqClient.getChannel();
    this.partition = partition;
    this.batchSize = batchSize;
    this.flushIntervalMs = flushIntervalMs;
  }

  async start(): Promise<void> {
    const queueName = `pageviews.p${this.partition}`;
    await new Promise<void>((resolve, reject) => {
      this.channel.assertQueue(queueName, { durable: true }, (err) => {
        if (err) reject(err);
        else resolve();
      });
    });

    logger.info(
      { partition: this.partition, queue: queueName },
      'aggregator started listening'
    );


    this.channel.consume(queueName, (msg) => {
      if (msg) {
        try {
          const message: PageViewMessage = JSON.parse(msg.content.toString());
          this.messageBuffer.push({ ...message, raw: msg });

          if (this.messageBuffer.length >= this.batchSize) {
            this.flushBuffer();
          } else if (!this.flushTimer) {
            this.flushTimer = setTimeout(
              () => this.flushBuffer(),
              this.flushIntervalMs,
            );
          }
        } catch (error) {
          logger.error({ err: error, msg: msg.content.toString() }, 'error parsing message');

          this.channel.nack(msg, false, true);
        }
      }
    });
  }

  private validateTimestamp(timestamp: string): Date | null {
    const normalizedTimestamp = timestamp.replace("_", "T");
    const date = new Date(normalizedTimestamp);
    return isNaN(date.getTime()) ? null : date;
  }

  private async flushBuffer(): Promise<void> {
    if (this.messageBuffer.length === 0) return;

    const batch = this.messageBuffer.splice(0, this.messageBuffer.length); // clear buffer
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }

    try {
      const rows = await this.processBatch(batch);
      await this.storeAggregatedPageViews(rows);

      batch.forEach((msg) => this.channel.ack(msg.raw));
    } catch (error) {
      logger.error({ err: error }, 'batch processing failed');
      batch.forEach((msg) => this.channel.nack(msg.raw, false, true));
    }
  }

  private async processBatch(batch: BufferedMessage[]): Promise<Aggregation[]> {
    logger.info({ batchSize: batch.length }, 'started processing new batch');
    // Internal sharding: aggregate by (page, viewHour, partition, shard_key)

    const aggregates = new Map<
      string,
      Aggregation
    >();

    for (const msg of batch) {
      const validatedDate = this.validateTimestamp(msg.timestamp);
      if (!validatedDate)
        throw new Error(
          `Invalid timestamp format in message: ${msg.timestamp}`,
        );

      const roundedHour = new Date(validatedDate);
      roundedHour.setMinutes(0, 0, 0);

      const key = `${msg.page}|${roundedHour.toISOString()}|${msg.partition}`;
      const existing = aggregates.get(key);
      if (existing) {
        existing.views += msg.views;
      } else {
        aggregates.set(key, {
          page: msg.page,
          viewHour: roundedHour,
          views: msg.views,
          partition: msg.partition,
          shard_key: msg.shard_key,
        });
      }
    }

    return Array.from(aggregates.values());
  }


  private async storeAggregatedPageViews(rows: Aggregation[]): Promise<void> {
    logger.info({ batchSize: rows.length }, 'storing aggregated page views');

    if (rows.length === 0) {
      logger.warn({ partition: this.partition }, 'no valid rows to insert');
      return;
    }

    const values = rows.flatMap(r => [r.page, r.viewHour, r.views, r.partition, r.shard_key]);
    const placeholders = rows.map((_, i) => {
      const b = i * 5;
      return `($${b + 1}, $${b + 2}, $${b + 3}, $${b + 4}, $${b + 5})`;
    });

    const query = `
      INSERT INTO page_views (page, view_hour, views, partition, shard_key)
      VALUES ${placeholders.join(', ')}
      ON CONFLICT (page, view_hour, shard_key)
      DO UPDATE SET views = page_views.views + EXCLUDED.views
    `;

    try {
      await pool.query(query, values);
      logger.info({ inserted: rows.length, partition: this.partition }, 'completed storing page views');
    } catch (err) {
      logger.error(
        { err, partition: this.partition },
        'failed to store aggregated page views'
      );
      throw err;
    }
  }
}
