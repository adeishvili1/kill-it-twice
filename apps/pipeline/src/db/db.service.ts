import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { Pool, PoolClient, QueryResultRow, types } from 'pg';

// bigint (int8) and numeric come back as strings by default; ids and counts fit in a JS number.
types.setTypeParser(20, (v) => Number(v));
types.setTypeParser(1700, (v) => Number(v));
import { config } from '../config';

@Injectable()
export class DbService implements OnModuleDestroy {
  readonly pool = new Pool({ connectionString: config.databaseUrl, max: 10 });

  async query<T extends QueryResultRow = any>(text: string, params: any[] = []): Promise<T[]> {
    const res = await this.pool.query<T>(text, params);
    return res.rows;
  }
  async one<T extends QueryResultRow = any>(text: string, params: any[] = []): Promise<T> {
    return (await this.query<T>(text, params))[0];
  }
  async withClient<T>(fn: (c: PoolClient) => Promise<T>): Promise<T> {
    const c = await this.pool.connect();
    try { return await fn(c); } finally { c.release(); }
  }
  async ping(): Promise<boolean> {
    try { await this.pool.query('select 1'); return true; } catch { return false; }
  }
  onModuleDestroy() { return this.pool.end(); }
}
