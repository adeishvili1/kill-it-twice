import { Injectable, OnModuleInit } from '@nestjs/common';
import { DbService } from '../db/db.service';
import { MetricsService } from '../metrics/metrics.service';
import { config } from '../config';

export type BackfillStatus = 'idle' | 'running' | 'paused' | 'done';
export type IncrementalStatus = 'running' | 'paused';
export interface BackfillState { status: BackfillStatus; lastId: number; targetMaxId: number; updatedAt: string }
export interface IncrementalState { status: IncrementalStatus; cursorSeq: number; updatedAt: string }
export interface Params { batchSize: number; incrementalIntervalMs: number; safetyWindowMs: number }

const STATUS_CODE: Record<BackfillStatus, number> = { idle: 0, running: 1, paused: 2, done: 3 };

/**
 * The only durable progress marker. Written AFTER both sinks acknowledged a batch (SPEC §5).
 * In-memory copies are kept so loops do not re-read state every iteration; every write goes
 * to Postgres first and the memory copy is updated only when the write succeeded.
 */
@Injectable()
export class CheckpointService implements OnModuleInit {
  backfill: BackfillState = { status: 'idle', lastId: 0, targetMaxId: 0, updatedAt: '' };
  incremental: IncrementalState = { status: 'running', cursorSeq: 0, updatedAt: '' };
  params: Params = { batchSize: config.batchSize, incrementalIntervalMs: config.incrementalIntervalMs, safetyWindowMs: config.safetyWindowMs };

  constructor(private readonly db: DbService, private readonly metrics: MetricsService) {}

  async onModuleInit() { await this.reload(); }

  async reload() {
    const rows = await this.db.query('select key, status, last_id, target_max_id, cursor_seq, updated_at from pipeline_state');
    for (const r of rows) {
      if (r.key === 'backfill') this.backfill = { status: r.status, lastId: Number(r.last_id), targetMaxId: Number(r.target_max_id), updatedAt: r.updated_at };
      if (r.key === 'incremental') this.incremental = { status: r.status, cursorSeq: Number(r.cursor_seq), updatedAt: r.updated_at };
    }
    const params = await this.db.query('select key, value from pipeline_params');
    for (const p of params) {
      if (p.key === 'batch_size') this.params.batchSize = Number(p.value);
      if (p.key === 'incremental_interval_ms') this.params.incrementalIntervalMs = Number(p.value);
      if (p.key === 'safety_window_ms') this.params.safetyWindowMs = Number(p.value);
    }
    this.publish();
  }

  publish() {
    this.metrics.backfillPosition.set(this.backfill.lastId);
    this.metrics.backfillTarget.set(this.backfill.targetMaxId);
    this.metrics.backfillStatus.set(STATUS_CODE[this.backfill.status]);
    this.metrics.incrementalCursor.set(this.incremental.cursorSeq);
    this.metrics.incrementalStatus.set(this.incremental.status === 'running' ? 1 : 2);
  }

  async setBackfill(patch: Partial<BackfillState>) {
    const next = { ...this.backfill, ...patch };
    await this.db.query(
      `update pipeline_state set status=$1, last_id=$2, target_max_id=$3, updated_at=clock_timestamp() where key='backfill'`,
      [next.status, next.lastId, next.targetMaxId],
    );
    this.backfill = next;
    this.publish();
  }

  /** Commit backfill progress. Only called after both sinks acknowledged the batch. */
  async commitBackfill(lastId: number) {
    await this.db.query(`update pipeline_state set last_id=$1, updated_at=clock_timestamp() where key='backfill'`, [lastId]);
    this.backfill.lastId = lastId;
    this.metrics.backfillPosition.set(lastId);
    this.metrics.batchesCommitted.inc({ mode: 'backfill' });
  }

  async setIncremental(patch: Partial<IncrementalState>) {
    const next = { ...this.incremental, ...patch };
    await this.db.query(`update pipeline_state set status=$1, cursor_seq=$2, updated_at=clock_timestamp() where key='incremental'`, [next.status, next.cursorSeq]);
    this.incremental = next;
    this.publish();
  }

  /** Commit incremental progress. Only called after both sinks acknowledged the batch. */
  async commitIncremental(cursorSeq: number) {
    await this.db.query(`update pipeline_state set cursor_seq=$1, updated_at=clock_timestamp() where key='incremental'`, [cursorSeq]);
    this.incremental.cursorSeq = cursorSeq;
    this.metrics.incrementalCursor.set(cursorSeq);
    this.metrics.batchesCommitted.inc({ mode: 'incremental' });
  }

  async setParams(patch: Partial<Params>) {
    const next = { ...this.params, ...patch };
    if (next.batchSize < 1 || next.batchSize > 10000) throw new Error('batch_size must be 1..10000');
    if (next.incrementalIntervalMs < 100) throw new Error('incremental_interval_ms must be >= 100');
    if (next.safetyWindowMs < 0) throw new Error('safety_window_ms must be >= 0');
    const rows: Array<[string, number]> = [['batch_size', next.batchSize], ['incremental_interval_ms', next.incrementalIntervalMs], ['safety_window_ms', next.safetyWindowMs]];
    for (const [k, v] of rows) {
      await this.db.query(`insert into pipeline_params(key, value) values ($1,$2) on conflict (key) do update set value=excluded.value`, [k, String(v)]);
    }
    this.params = next;
  }
}
