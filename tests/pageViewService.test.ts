import { PageViewService } from "../src/services/pageViewService";
import { RabbitMQClient } from "../src/infra/rabbitmq";

describe("PageViewService - core logic", () => {
    let service: PageViewService;
    let mockClient: jest.Mocked<RabbitMQClient>;

    beforeEach(() => {
        mockClient = { getChannel: jest.fn() } as any;
        service = new PageViewService(mockClient, 4);
    });

    describe("validateTimestamp", () => {
        it("returns Date for valid timestamp", () => {
            const date = service["validateTimestamp"]("2025-01-01T12:00:00Z");
            expect(date).toBeInstanceOf(Date);
        });

        it("returns null for invalid timestamp", () => {
            const date = service["validateTimestamp"]("not-a-date");
            expect(date).toBeNull();
        });
    });

    describe("getPartition", () => {
        it("is consistent for same page", () => {
            const p1 = service["getPartition"]("home");
            const p2 = service["getPartition"]("home");
            expect(p1).toBe(p2);
        });

        it("stays within partitionsNum", () => {
            const p = service["getPartition"]("home");
            expect(p).toBeGreaterThanOrEqual(0);
            expect(p).toBeLessThan(4);
        });
    });

    describe("incrementMultipleViews", () => {
        it("calls publishToQueue for each entry", async () => {
            const spy = jest
                .spyOn(service as any, "publishToQueue")
                .mockResolvedValue(undefined);

            const data = {
                "/home": {
                    "2025-01-01T00:00:00Z": 5,
                    "2025-01-01T01:00:00Z": 10,
                },
            };

            await service.incrementMultipleViews(data as any);
            expect(spy).toHaveBeenCalledTimes(2);
        });
    });
});
