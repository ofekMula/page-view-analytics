import logzioLogger from 'logzio-nodejs';

export const logger = logzioLogger.createLogger({
  token: process.env.LOGZIO_TOKEN! || "mFJpupXOLGfnACdjyGZlomwDdpxVfUFI",
  protocol: 'https',
  host: 'listener-eu.logz.io',
  port: '8071',
});
