import pino from 'pino';

// Regra: NUNCA logar payloads financeiros completos. Logamos só ids e status.
export const logger = pino({ level: process.env.LOG_LEVEL ?? 'info' });