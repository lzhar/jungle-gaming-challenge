// src/main.ts
import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { config } from './config';
import { AppExceptionFilter } from './application/exception.filter';
import { logger } from './infrastructure/logger';

const app = await NestFactory.create(AppModule, { logger: false });
app.useGlobalFilters(new AppExceptionFilter());
app.enableShutdownHooks(); // SIGTERM: o Nest chama onModuleDestroy e drena o consumidor
await app.listen(config.port);
logger.info({ port: config.port }, 'api listening');