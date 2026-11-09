import { Controller, Get, Inject, Res } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/postgresql';
import type { Response } from 'express';
import { Db } from '../infrastructure/db';
import { registry } from '../infrastructure/metrics';
import { Queues } from '../application/sqs';

@Controller()
export class OpsController {
  constructor(
    @Inject(MikroORM) private readonly orm: MikroORM,
    @Inject(Queues) private readonly queues: Queues,
  ) {}

  @Get('health/live')
  live() {
    return { status: 'ok' };
  }

  @Get('health/ready')
  async ready(@Res({ passthrough: true }) res: Response) {
    try {
      await new Db(this.orm.em.fork()).q('SELECT 1');
      await this.queues.ping();
      return { status: 'ready' };
    } catch {
      res.status(503);
      return { status: 'unavailable' };
    }
  }

  @Get('metrics')
  async metrics(@Res() res: Response) {
    res.type(registry.contentType).send(await registry.metrics());
  }
}