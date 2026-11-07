import { Inject, Injectable, type OnApplicationShutdown } from '@nestjs/common';
import { MikroORM, type EntityManager } from '@mikro-orm/postgresql';
import { Migrator } from '@mikro-orm/migrations';
import { config } from '../config';
import { Migration20260101000000 } from './migrations/Migration20260101000000';

export const createOrm = () =>
  MikroORM.init({
    clientUrl: config.databaseUrl,
    entities: [],
    discovery: { warnWhenNoEntities: false },
    extensions: [Migrator],
    migrations: {
      migrationsList: [{ name: 'Migration20260101000000', class: Migration20260101000000 }],
      transactional: true,
      snapshot: false,
    },
    pool: { min: 2, max: 20 },
  });

/** Executa SQL dentro da transação corrente do EntityManager. */
export class Db {
  constructor(private readonly em: EntityManager) {}
  q<T = any>(sql: string, params: unknown[] = []): Promise<T[]> {
    return this.em.getConnection().execute(sql, params, 'all', this.em.getTransactionContext()) as Promise<T[]>;
  }
}

@Injectable()
export class OrmShutdown implements OnApplicationShutdown {
  constructor(@Inject(MikroORM) private readonly orm: MikroORM) {}
  async onApplicationShutdown() {
    await this.orm.close();
  }
}