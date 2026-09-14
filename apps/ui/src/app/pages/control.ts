import { Component, OnDestroy, OnInit, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { SlicePipe } from '@angular/common';
import { Api, fmt, pretty } from '../api';

/** UI screen 3 — run the pipeline: backfill/incremental controls, parameters, DLQ replay. */
@Component({
  selector: 'page-control',
  imports: [FormsModule, SlicePipe],
  template: `
    <h2>Control</h2>
    <div class="grid">
      <div class="card">
        <h3>Backfill <span class="pill">{{ st()?.backfill?.status }}</span> <span class="small">{{ f(st()?.backfill?.lastId) }} / {{ f(st()?.backfill?.targetMaxId) }}</span></h3>
        <div class="row">
          <button class="primary" (click)="act('/api/control/backfill/start')" [disabled]="st()?.backfill?.status==='running'">Start</button>
          <button (click)="act('/api/control/backfill/pause')" [disabled]="st()?.backfill?.status!=='running'">Pause</button>
          <button (click)="act('/api/control/backfill/resume')" [disabled]="st()?.backfill?.status!=='paused'">Resume</button>
          <button class="danger" (click)="act('/api/control/backfill/reset')">Reset checkpoint</button>
        </div>
        <p class="small">Start snapshots max(id) and walks the table in keyset pages; a restart resumes from the last committed checkpoint. Reset sets the checkpoint back to 0 (the sinks are idempotent, so re-running is harmless).</p>
        <h3>Incremental sync <span class="pill">{{ st()?.incremental?.status }}</span> <span class="small">lag {{ f(st()?.incremental?.lagSeq) }}</span></h3>
        <div class="row">
          <button (click)="act('/api/control/incremental/pause')" [disabled]="st()?.incremental?.status==='paused'">Pause</button>
          <button class="primary" (click)="act('/api/control/incremental/resume')" [disabled]="st()?.incremental?.status==='running'">Resume</button>
        </div>
      </div>
      <div class="card">
        <h3>Parameters (runtime, persisted)</h3>
        <div class="row"><label style="width:180px">batch size</label><input type="number" [(ngModel)]="params.batchSize" style="width:120px" /></div>
        <div class="row"><label style="width:180px">incremental interval (ms)</label><input type="number" [(ngModel)]="params.incrementalIntervalMs" style="width:120px" /></div>
        <div class="row"><label style="width:180px">safety window (ms)</label><input type="number" [(ngModel)]="params.safetyWindowMs" style="width:120px" /></div>
        <button class="primary" (click)="saveParams()">Save</button>
      </div>
    </div>
    <div class="card">
      <h3>Dead-letter queue
        <button (click)="tab='open'; loadDlq()" [class.primary]="tab==='open'">open ({{ f(st()?.counts?.dlq?.es) }})</button>
        <button (click)="tab='resolved'; loadDlq()" [class.primary]="tab==='resolved'">resolved</button>
        <button class="danger" (click)="replayAll()" [disabled]="tab!=='open' || !dlq().length">Replay all open</button>
      </h3>
      <table>
        <thead><tr><th>id</th><th>sink</th><th>record</th><th>v</th><th>mode</th><th>attempts</th><th>error</th><th>batch</th><th>updated</th><th></th></tr></thead>
        <tbody>
          @for (d of dlq(); track d.id) {
            <tr class="click" (click)="expanded = expanded === d.id ? 0 : d.id">
              <td>{{ d.id }}</td><td>{{ d.sink }}</td><td>{{ d.record_id }}</td><td>{{ d.version }}</td><td>{{ d.mode }}</td><td>{{ d.attempts }}</td><td class="small">{{ d.error | slice:0:140 }}</td><td class="small">{{ d.batch_id | slice:0:8 }}</td><td class="small">{{ d.updated_at | slice:0:19 }}</td>
              <td>@if (d.status==='open') {<button (click)="replayOne(d.id); $event.stopPropagation()">Replay</button>}</td>
            </tr>
            @if (expanded === d.id) { <tr><td colspan="10"><pre>{{ p(d.payload) }}</pre><div class="small">{{ d.error }}</div></td></tr> }
          } @empty { <tr><td colspan="10" class="small">empty</td></tr> }
        </tbody>
      </table>
    </div>
    @if (out()) { <div class="card"><h3>Last response</h3><div class="out">{{ out() }}</div></div> }
  `,
})
export class ControlPage implements OnInit, OnDestroy {
  private api = inject(Api);
  st = signal<any>(null); dlq = signal<any[]>([]); out = signal('');
  params: any = { batchSize: 500, incrementalIntervalMs: 1000, safetyWindowMs: 2000 };
  tab: 'open' | 'resolved' = 'open'; expanded = 0;
  f = fmt; p = pretty;
  private timer: any;
  async ngOnInit() { this.params = await this.api.get('/api/control/params').catch(() => this.params); this.tick(); this.loadDlq(); this.timer = setInterval(() => { this.tick(); this.loadDlq(); }, 2000); }
  ngOnDestroy() { clearInterval(this.timer); }
  private async tick() { try { this.st.set(await this.api.status()); } catch { this.st.set(null); } }
  async loadDlq() { try { this.dlq.set(await this.api.get('/api/dlq', { status: this.tab, limit: 100 })); } catch { /* ignore */ } }
  async act(url: string) { try { this.out.set(pretty(await this.api.post(url))); } catch (e: any) { this.out.set('error: ' + (e.error?.message ?? e.message)); } await this.tick(); }
  async saveParams() { try { this.out.set(pretty(await this.api.put('/api/control/params', this.params))); } catch (e: any) { this.out.set('error: ' + (e.error?.message ?? e.message)); } }
  async replayOne(id: number) { await this.act(`/api/dlq/${id}/replay`); this.loadDlq(); }
  async replayAll() { try { this.out.set(pretty(await this.api.post('/api/dlq/replay', { all: true }))); } catch (e: any) { this.out.set('error: ' + e.message); } this.loadDlq(); }
}
