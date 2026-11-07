import { createOrm } from './infrastructure/db';

const orm = await createOrm();
const direction = process.argv[2];
if (direction === 'down') await orm.migrator.down();

else await orm.migrator.up();
await orm.close();
console.log('migration ' + direction + ' done');