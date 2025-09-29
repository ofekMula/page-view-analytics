import { AggregatorService } from "../src/services/aggregatorService";
import { RabbitMQClient } from "../src/infra/rabbitmq";
import { Aggregation } from "../src/types";

describe("AggregatorService - core logic", () => {
    let service: AggregatorService;

    beforeEach(() => {
        const mockClient = { getChannel: jest.fn() } as any as RabbitMQClient;
        service = new AggregatorService(mockClient, 0, 100, 5000);
    });

    describe("validateTimestamp", () => {
        it("returns Date for valid timestamp with underscore", () => {
            const date = service["validateTimestamp"]("2025-01-01_12:00:00Z");
            expect(date).toBeInstanceOf(Date);
        });

        it("returns null for invalid timestamp", () => {
            const date = service["validateTimestamp"]("bad");
            expect(date).toBeNull();
        });
    });

    describe("processBatch", () => {
        it("aggregates multiple messages for same page/hour/partition", async () => {
            const batch: any = [
                {
                    page: "home",
                    timestamp: "2025-01-01T12:15:00Z",
                    views: 5,
                    partition: 0,
                    shard_key: 1,
                },
                {
                    page: "home",
                    timestamp: "2025-01-01T12:45:00Z", // same hour
                    views: 7,
                    partition: 0,
                    shard_key: 1,
                },
            ];

            const result: Aggregation[] = await service["processBatch"](batch);

            expect(result).toHaveLength(1);
            expect(result[0].page).toBe("home");
            expect(result[0].views).toBe(12);
            expect(result[0].partition).toBe(0);
            expect(result[0].shard_key).toBe(1);
        });

        it("creates separate rows for different partitions/shards", async () => {
            const batch: any = [
                {
                    page: "altman.html",
                    timestamp: "2025-01-01T12:00:00Z",
                    views: 3,
                    partition: 0,
                    shard_key: 1,
                },
                {
                    page: "home.html",
                    timestamp: "2025-01-01T12:10:00Z",
                    views: 4,
                    partition: 1,
                    shard_key: 1,
                },
                {
                    page: "altman.html",
                    timestamp: "2025-01-01T12:20:00Z",
                    views: 6,
                    partition: 0,
                    shard_key: 2,
                },
            ];


            const result: Aggregation[] = await service["processBatch"](batch);

            expect(result).toHaveLength(2);
            expect(result.map(r => r.views).sort()).toEqual([4, 9]);
        });
    });
});
