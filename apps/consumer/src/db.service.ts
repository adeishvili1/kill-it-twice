import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { Pool, types } from 'pg';
import { config } from './config';
types.setTypeParser(20, (v) => Number(v));

@Injectable()
export class DbService implements OnModuleDestroy {
  readonly pool = new Pool({ connectionString: config.databaseUrl, max: 5 });
  async query<T = any>(text: string, values: unknown[] = []): Promise<T[]> { return (await this.pool.query(text, values)).rows as T[]; }
  async ping() { try { await this.pool.query('select 1'); return true; } catch { return false; } }
  onModuleDestroy() { return this.pool.end(); }
}
