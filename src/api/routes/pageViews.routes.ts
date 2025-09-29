import type { FastifyInstance } from 'fastify';
import { SinglePageViewSchema, MultiPageViewSchema, ReportResponseSchema } from '../../types';
import type { PageViewService } from '../../services/pageViewService';

type Deps = { pageViewService: PageViewService };

export function registerPageViewRoutes(app: FastifyInstance, { pageViewService }: Deps) {
    app.post('/page-views/single', {
        schema: { body: SinglePageViewSchema }
    }, async (request) => {
        // @ts-ignore
        await pageViewService.incrementSingleView(request.body.page, request.body.timestamp);
        return { success: true };
    });

    app.post('/page-views/multi', {
        schema: { body: MultiPageViewSchema }
    }, async (request) => {
        // @ts-ignore
        await pageViewService.incrementMultipleViews(request.body);
        return { success: true };
    });

    app.get<{
        Querystring: { page: string; now?: string; order?: 'asc' | 'desc'; take?: number }
    }>('/report', {
        schema: { response: { 200: ReportResponseSchema } }
    }, async (request) => {
        const { page, now, order = 'asc', take } = request.query;
        return await pageViewService.getReport(page, now, order, take);
    });
}
