import { Component, OnDestroy, OnInit, inject, signal } from '@angular/core';
import { Api, fmt } from '../api';

/** UI screen 1 — the visual side of G5: where is the backfill, throughput, lag, DLQ, health. */
@Component({
  selector: 'page-dashboard',
  template: `
    <h2>Pipeline state <span class="small">refreshed every second · {{ s()?.now }}</span></h2>
    @if (s(); as st) {
      <div class="tiles" style="margin-bottom:14px">
        <div class="tile" [class.ok]="st.health.status==='ok'" [class.bad]="st.health.status!=='ok'"><div class="k">pipeline</div><div class="v">{{ st.health.status }}</div></div>
        <div class="tile" [class.ok]="st.health.db" [class.bad]="!st.health.db"><div class="k">postgres</div><div class="v">{{ st.health.db ? 'up' : 'down' }}</div></div>
        <div class="tile" [class.ok]="st.sinks.es.up" [class.bad]="!st.sinks.es.up"><div class="k">elasticsearch</div><div class="v">{{ st.sinks.es.up ? 'up' : 'waiting' }}</div></div>
        <div class="tile" [class.ok]="st.sinks.rabbitmq.up && st.sinks.rabbitmq.connected" [class.bad]="!(st.sinks.rabbitmq.up && st.sinks.rabbitmq.connected)"><div class="k">rabbitmq</div><div class="v">{{ st.sinks.rabbitmq.connected ? 'up' : 'down' }}</div></div>
        <div class="tile" [class.ok]="dlqTotal(st)===0" [class.warn]="dlqTotal(st)>0"><div class="k">DLQ</div><div class="v">{{ f(dlqTotal(st)) }}</div></div>
        <div class="tile" [class.ok]="st.incremental.lagSeq===0" [class.warn]="st.incremental.lagSeq>0"><div class="k">incremental lag</div><div class="v">{{ f(st.incremental.lagSeq) }} <span class="small">rows · {{ st.incremental.lagSeconds }}s</span></div></div>
      </div>
      <div class="grid">
        <div class="card">
          <h3>Backfill <span class="pill" [class.ok]="st.backfill.status==='done'" [class.warn]="st.backfill.status==='running'">{{ st.backfill.status }}</span></h3>
          <div class="bar"><div [style.width.%]="st.backfill.pct"></div></div>
          <div class="kv">
            <span>position</span><span>{{ f(st.backfill.lastId) }} / {{ f(st.backfill.targetMaxId) }} ({{ st.backfill.pct }}%)</span>
            <span>throughput</span><span>{{ f(st.backfill.rate) }} rows/s</span>
            <span>resumed from</span><span>{{ st.backfill.resumed_from ? f(st.backfill.resumed_from) + ' (after a restart)' : '– (fresh start)' }}</span>
            <span>last batch</span><span>{{ st.backfill.last_batch_at || '–' }}</span>
          </div>
        </div>
        <div class="card">
          <h3>Incremental sync <span class="pill" [class.ok]="st.incremental.status==='running'" [class.warn]="st.incremental.status!=='running'">{{ st.incremental.status }}</span></h3>
          <div class="kv">
            <span>cursor</span><span>{{ f(st.incremental.cursorSeq) }} / {{ f(st.incremental.maxSeq) }} (change_log.seq)</span>
            <span>lag</span><span>{{ f(st.incremental.lagSeq) }} changes · {{ st.incremental.lagSeconds }} s</span>
            <span>throughput</span><span>{{ f(st.incremental.rate) }} rows/s</span>
            <span>interval / batch</span><span>{{ st.params.incrementalIntervalMs }} ms / {{ st.params.batchSize }} · safety window {{ st.params.safetyWindowMs }} ms</span>
            <span>last batch</span><span>{{ st.incremental.last_batch_at || '–' }}</span>
          </div>
        </div>
        <div class="card">
          <h3>Counts</h3>
          <div class="kv">
            <span>source rows</span><span>{{ f(st.counts.source) }}</span>
            <span>indexed (ES)</span><span>{{ f(st.counts.es) }} <span class="small">{{ diff(st.counts.source, st.counts.es) }}</span></span>
            <span>consumer events</span><span>{{ f(st.counts.consumer.events) }} <span class="small">({{ f(st.counts.consumer.products) }} distinct products)</span></span>
            <span>queue depth</span><span>{{ f(st.counts.queue_depth) }}</span>
            <span>DLQ open</span><span>es {{ f(st.counts.dlq.es) }} · rabbitmq {{ f(st.counts.dlq.rabbitmq) }}</span>
          </div>
        </div>
        <div class="card">
          <h3>Since process start</h3>
          <div class="kv">
            <span>written to ES</span><span>{{ f(st.totals.written.es) }}</span>
            <span>published to RabbitMQ</span><span>{{ f(st.totals.written.rabbitmq) }}</span>
            <span>replays absorbed (ES 409)</span><span>{{ f(st.totals.duplicates_absorbed.es) }}</span>
            <span>sink retries</span><span>es {{ f(st.totals.retries.es) }} · rabbitmq {{ f(st.totals.retries.rabbitmq) }}</span>
            <span>uptime</span><span>{{ st.health.uptime_s }} s</span>
          </div>
        </div>
      </div>
    } @else {
      <div class="card">Pipeline API unreachable{{ err() ? ': ' + err() : '' }} — is <code>docker compose up</code> running?</div>
    }
  `,
})
export class DashboardPage implements OnInit, OnDestroy {
  private api = inject(Api);
  s = signal<any>(null); err = signal('');
  private timer: any;
  f = fmt;
  dlqTotal(st: any) { return (st.counts?.dlq?.es ?? 0) + (st.counts?.dlq?.rabbitmq ?? 0); }
  diff(a: number, b: number | null) { if (b == null) return '(es unavailable)'; const d = a - b; return d === 0 ? '(in sync)' : d > 0 ? `(${fmt(d)} behind)` : `(${fmt(-d)} ahead)`; }
  ngOnInit() { this.tick(); this.timer = setInterval(() => this.tick(), 1000); }
  ngOnDestroy() { clearInterval(this.timer); }
  private async tick() { try { this.s.set(await this.api.status()); this.err.set(''); } catch (e: any) { this.s.set(null); this.err.set(e?.message ?? ''); } }
}
