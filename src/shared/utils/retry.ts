import { logger } from '../../shared/utils/logger';
const MAX_RETRIES = 5;
const RETRY_DELAY = 5000;

export async function connectWithRetry(fn: () => Promise<void>, service: string): Promise<void> {
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
            await fn();
            return;
        } catch (e) {
            const errMsg = e instanceof Error ? e.message : String(e);
            const isLast = attempt === MAX_RETRIES;

            const fields = { event: 'connect_fail', service, attempt, max: MAX_RETRIES, err: errMsg };
            if (isLast) logger.error(fields, 'connect failed');
            else logger.warn(fields, 'retrying');

            if (isLast) throw e;
            await new Promise(r => setTimeout(r, RETRY_DELAY));
        }
    }
}
