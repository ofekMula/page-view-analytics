import { Channel } from "amqplib/callback_api";
import { pool } from "../infra/database";
import { RabbitMQClient } from "../infra/rabbitmq";
import type { MultiPageView, ReportResponse, SinglePageView } from "../types";
import crypto from "crypto";
import { logger } from "../shared/utils/logger";

export class PageViewService {
  private channel: Channel;
  private partitionsNum: number;

  constructor(
    private rabbitmqClient: RabbitMQClient,
    partitionsNum: number,
  ) {
    this.channel = rabbitmqClient.getChannel();
    this.partitionsNum = partitionsNum;
  }

  private validateTimestamp(timestamp: string): Date | null {
    const date = new Date(timestamp);
    return isNaN(date.getTime()) ? null : date;
  }

  private getShardKey(): number {
    const NUM_SHARDS = process.env.NUM_SHARDS ? parseInt(process.env.NUM_SHARDS) : 10;
    return Math.floor(Math.random() * NUM_SHARDS);
  }

  private async publishToQueue(
    page: string,
    timestamp: string,
    views: number,
  ): Promise<void> {
    const validatedDate = this.validateTimestamp(timestamp);
    if (!validatedDate) {
      throw new Error(`Invalid timestamp format: ${timestamp}`);
    }

    const partition = this.getPartition(page);
    const shard_key = this.getShardKey();
    const queueName = `pageviews.p${partition}`;

    await new Promise<void>((resolve, reject) => {
      this.channel.assertQueue(queueName, { durable: true }, (err) => {
        if (err) reject(err);
        else resolve();
      });
    });

    const message = {
      page,
      timestamp: validatedDate.toISOString(),
      views,
      partition,
      shard_key,
    };

    logger.debug(
      { queue: queueName, page, views, partition, shardKey: shard_key },
      "published page view message"
    );
    this.channel.sendToQueue(queueName, Buffer.from(JSON.stringify(message)), {
      persistent: true,
    });
  }

  private getPartition(page: string): number {
    const hash = crypto.createHash("md5").update(page).digest("hex");

    const hashInt = parseInt(hash.slice(0, 8), 16);
    return hashInt % this.partitionsNum;
  }

  async incrementSingleView(page: string, timestamp: string): Promise<void> {
    const validatedDate = this.validateTimestamp(timestamp);
    if (!validatedDate) {
      throw new Error(`Invalid timestamp format: ${timestamp}`);
    }

    await this.publishToQueue(page, timestamp, 1);
  }

  async incrementMultipleViews(data: MultiPageView): Promise<void> {
    const promises: Promise<void>[] = [];

    for (const [page, hourViews] of Object.entries(data)) {
      for (const [timestamp, views] of Object.entries(hourViews)) {
        const validatedDate = this.validateTimestamp(
          timestamp.replace("_", "T"),
        );
        if (!validatedDate) {
          throw new Error(`Invalid timestamp format: ${timestamp}`);
        }
        const isoTimestamp = validatedDate.toISOString();
        promises.push(this.publishToQueue(page, isoTimestamp, views));
      }
    }


    await Promise.all(promises);
  }

  async getReport(
    page: string,
    now?: string,
    order: "asc" | "desc" = "asc",
    take?: number,
  ): Promise<ReportResponse> {
    const currentTime = now ? new Date(now) : new Date();
    const endTime = new Date(currentTime);
    endTime.setUTCMinutes(0, 0, 0);

    const startTime = new Date(endTime);
    startTime.setUTCHours(endTime.getUTCHours() - 24);
    endTime.setUTCHours(endTime.getUTCHours() - 1);

    // Aggregate across all shard_keys for the same page and hour
    const result = await pool.query(
      `WITH RECURSIVE hours AS (
        SELECT generate_series(
          DATE_TRUNC('hour', $1::timestamp),
          DATE_TRUNC('hour', $2::timestamp),
          '1 hour'
        ) AS view_hour
      )
      SELECT
        EXTRACT(HOUR FROM hours.view_hour AT TIME ZONE 'UTC') as hour,
        COALESCE(SUM(page_views.views), 0) as views
      FROM hours
      LEFT JOIN page_views ON
        page_views.view_hour = hours.view_hour AND
        page_views.page = $3
      GROUP BY hours.view_hour
      ORDER BY hours.view_hour ${order === "asc" ? "ASC" : "DESC"}
      ${take ? "LIMIT $4" : ""}`,
      take
        ? [
          startTime.toISOString(),
          endTime.toISOString(),
          page,
          take,
        ]
        : [startTime.toISOString(), endTime.toISOString(), page],
    );

    logger.info({ page, rows: result.rows.length }, "fetched page view report");


    return {
      page,
      start: startTime.toISOString(),
      end: endTime.toISOString(),
      data: result.rows.map((row) => ({
        hour: Number(row.hour),
        views: Number(row.views),
      })),
    };
  }
}
