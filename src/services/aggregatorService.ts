import { Channel, Message } from "amqplib/callback_api";
import { pool } from "../config/database";
import { RabbitMQClient } from "../config/rabbitmq";
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

    console.log(
      `Aggregator P${this.partition} started listening on queue: ${queueName}`,
    );

    this.channel.consume(queueName, (msg) => {
      if (msg) {
        try {
          const message: PageViewMessage = JSON.parse(msg.content.toString());
          this.messageBuffer.push({ ...message, raw: msg }); // store the original msg for ack/nack

          if (this.messageBuffer.length >= this.batchSize) {
            this.flushBuffer();
          } else if (!this.flushTimer) {
            this.flushTimer = setTimeout(
              () => this.flushBuffer(),
              this.flushIntervalMs,
            );
          }
        } catch (error) {
          console.error(`Error parsing message:`, error);
          this.channel.nack(msg, false, true);
        }
      }
    });
  }

  private validateTimestamp(timestamp: string): Date | null {
    // Handle underscore format (convert to ISO-compatible format)
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
      await this.processBatch(batch);
      batch.forEach((msg) => this.channel.ack(msg.raw)); // ack after successful processing
    } catch (error) {
      console.error("Batch processing failed:", error);
      batch.forEach((msg) => this.channel.nack(msg.raw, false, true)); // requeue
    }
  }

  private async processBatch(batch: BufferedMessage[]): Promise<void> {
    console.log("Started Processing new batch");

    // Internal sharding: aggregate by (page, viewHour, partition, shard_key)
    const aggregates = new Map<
      string,
      {
        page: string;
        viewHour: Date;
        views: number;
        partition: number;
        shard_key: number;
      }
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

    // Prepare bulk values
    const aggregations = Array.from(aggregates.values());

    if (aggregations.length === 0) {
      console.log("No valid rows to insert.");
      return;
    }

    const values = aggregations.flatMap((agg) => [
      agg.page,
      agg.viewHour,
      agg.views,
      agg.partition,
      agg.shard_key,
    ]);

    const placeholders = aggregations.map((_, i) => {
      const base = i * 5;
      return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5})`;
    });

    const query = `
      INSERT INTO page_views (page, view_hour, views, partition, shard_key)
      VALUES ${placeholders.join(", ")}
      ON CONFLICT (page, view_hour, shard_key)
      DO UPDATE SET views = page_views.views + EXCLUDED.views
    `;

    await pool.query(query, values);
    console.log("Completed Processing new batch");
  }
}
