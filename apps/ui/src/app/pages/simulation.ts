import { Component, OnDestroy, OnInit, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Api, fmt, pretty } from '../api';

/** UI screen 4 — cause failures on purpose: stop sinks, corrupt rows, generate load, crash the pipeline. */
@Component({
  selector: 'page-simulation',
  imports: [FormsModule],
  template: `
    <h2>Simulation</h2>
    <div class="grid">
      <div class="card">
        <h3>Sink outage (real container stop via the chaos service)</h3>
        @for (s of services(); track s.service) {
          <div class="row">
            <span style="width:130px">{{ s.service }}</span>
            <span class="pill" [class.ok]="s.state==='running'" [class.bad]="s.state!=='running'">{{ s.state }}</span>
            <button class="danger" (click)="chaos('stop', s.service)" [disabled]="s.state!=='running'">Stop</button>
            <button class="primary" (click)="chaos('start', s.service)" [disabled]="s.state==='running'">Start</button>
          </div>
        } @empty { <div class="small">chaos service unreachable{{ chaosErr() ? ' — ' + chaosErr() : '' }}</div> }
        <p class="small">Watch the dashboard: the sink tile turns red, pipeline becomes <b>degraded</b>, lag grows, nothing is lost; after Start it recovers on its own.</p>
      </div>
      <div class="card">
        <h3>Source activity</h3>
        <div class="row">
          <input type="number" [(ngModel)]="genCount" style="width:100px" />
          <label><input type="checkbox" [(ngModel)]="ops.update" /> update</label>
          <label><input type="checkbox" [(ngModel)]="ops.insert" /> insert</label>
          <label><input type="checkbox" [(ngModel)]="ops.delete" /> soft delete</label>
          <button class="primary" (click)="generate()">Generate changes</button>
        </div>
        <div class="row">
          <input type="number" [(ngModel)]="corruptCount" style="width:100px" />
          <button class="danger" (click)="corrupt()">Corrupt rows</button>
          <span class="small">sets attributes.weight_kg to a string → the index rejects them per item → DLQ</span>
        </div>
        @if (lastCorrupted().length) { <div class="row"><span class="small">corrupted ids: {{ lastCorrupted().join(', ') }}</span><button (click)="fix()">Fix them at the source</button></div> }
      </div>
      <div class="card">
        <h3>Process crash</h3>
        <div class="row"><button class="danger" (click)="crash()">Crash after next batch</button> <span class="pill" [class.warn]="armed()">{{ armed() ? 'armed' : 'not armed' }}</span></div>
        <p class="small">The pipeline exits with code 1 right after both sinks acknowledged the next batch and <b>before</b> the checkpoint is written — the exact window a crash must survive. The container has no restart policy (so the killed state can be inspected): restart it with <code>docker compose start pipeline</code>; it resumes from the checkpoint and the replayed batch is absorbed as duplicates.</p>
        <h3>Reseed</h3>
        <div class="row"><input type="number" [(ngModel)]="seedRows" style="width:140px" /><button class="danger" (click)="seed()">Reset world &amp; seed</button></div>
        <p class="small">Truncates source, change log, DLQ and consumer store; recreates the index; purges the queue.</p>
      </div>
      <div class="card">
        <h3>Consumer</h3>
        @if (consumer(); as c) {
          <div class="kv">
            <span>connected</span><span>{{ c.connected }}</span>
            <span>events stored (rows)</span><span>{{ f(c.events) }}</span>
            <span>distinct products</span><span>{{ f(c.distinct_products) }}</span>
            <span>received since start</span><span>{{ f(c.received) }}</span>
            <span>duplicates absorbed</span><span>{{ f(c.duplicates_absorbed) }}</span>
            <span>consumer DLQ</span><span>{{ f(c.dlq) }}</span>
          </div>
        } @else { <div class="small">consumer unreachable</div> }
      </div>
    </div>
    @if (out()) { <div class="card"><h3>Last response</h3><div class="out">{{ out() }}</div></div> }
  `,
})
export class SimulationPage implements OnInit, OnDestroy {
  private api = inject(Api);
  services = signal<any[]>([]); chaosErr = signal(''); consumer = signal<any>(null); out = signal(''); armed = signal(false); lastCorrupted = signal<number[]>([]);
  genCount = 1000; corruptCount = 3; seedRows = 1000000; ops = { update: true, insert: false, delete: false };
  f = fmt;
  private timer: any;
  ngOnInit() { this.tick(); this.timer = setInterval(() => this.tick(), 2000); }
  ngOnDestroy() { clearInterval(this.timer); }
  private async tick() {
    try { this.services.set(await this.api.get('/chaos/services')); this.chaosErr.set(''); } catch (e: any) { this.services.set([]); this.chaosErr.set(e.message); }
    try { this.consumer.set(await this.api.consumerStats()); } catch { this.consumer.set(null); }
    try { this.armed.set((await this.api.get('/api/sim')).crash_after_next_batch); } catch { /* pipeline may be down */ }
  }
  private async run(p: Promise<any>) { try { const r = await p; this.out.set(pretty(r)); return r; } catch (e: any) { this.out.set('error: ' + (e.error?.error ?? e.error?.message ?? e.message)); return null; } finally { this.tick(); } }
  chaos(action: string, service: string) { return this.run(this.api.post(`/chaos/${action}`, { service })); }
  generate() { const ops = Object.entries(this.ops).filter(([, v]) => v).map(([k]) => k); return this.run(this.api.post('/api/sim/generate-changes', { count: this.genCount, ops })); }
  async corrupt() { const r = await this.run(this.api.post('/api/sim/corrupt', { count: this.corruptCount })); if (r?.corrupted) this.lastCorrupted.set(r.corrupted); }
  async fix() { await this.run(this.api.post('/api/sim/fix', { ids: this.lastCorrupted() })); this.lastCorrupted.set([]); }
  crash() { return this.run(this.api.post('/api/sim/crash-after-next-batch')); }
  seed() { if (confirm(`Reset everything and seed ${this.seedRows} rows?`)) return this.run(this.api.post('/api/admin/seed', { rows: this.seedRows })); return null; }
}
