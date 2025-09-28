import pino from 'pino';

export const logger = pino({
  level: process.env.LOG_LEVEL || 'debug',
  base: { service: 'page-view-analytics' },
  transport: {
    target: 'pino-pretty',
    options: {
      singleLine: true,
      colorize: true,
      translateTime: 'SYS:standard',
    },
  },
});
