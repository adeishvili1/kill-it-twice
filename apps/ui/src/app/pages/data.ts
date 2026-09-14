import { Component, OnDestroy, OnInit, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { SlicePipe } from '@angular/common';
import { Api, fmt, pretty } from '../api';

/** UI screen 2 — browse replicated records, compare source / index / stream, watch changes arrive. */
@Component({
  selector: 'page-data',
  imports: [FormsModule, SlicePipe],
  template: `
    <h2>Replicated data</h2>
    <div class="grid" style="grid-template-columns: 2fr 1fr">
      <div class="card">
        <div class="row">
          <input [(ngModel)]="q" placeholder="search name / sku / brand / description, or an id" (keyup.enter)="search()" style="flex:1" />
          <select [(ngModel)]="category" (change)="search()"><option value="">all categories</option>@for (c of cats(); track c.key) {<option [value]="c.key">{{ c.key }} ({{ f(c.count) }})</option>}</select>
          <button class="primary" (click)="search()">Search</button>
          <span class="small">{{ f(total()) }} hits · page {{ page }}</span>
          <button (click)="page = page - 1; search()" [disabled]="page <= 1">‹</button><button (click)="page = page + 1; search()" [disabled]="page * 25 >= total()">›</button>
        </div>
        <table>
          <thead><tr><th>id</th><th>sku</th><th>name</th><th>category</th><th>price</th><th>stock</th><th>v</th><th>updated</th></tr></thead>
          <tbody>
            @for (h of hits(); track h.id) {
              <tr class="click" [class.sel]="sel()?.source?.id === h.id" (click)="open(h.id)">
                <td>{{ h.id }}</td><td>{{ h.sku }}</td><td>{{ h.name }} @if (h.deleted_at) {<span class="pill bad">deleted</span>}</td><td>{{ h.category }}</td><td>{{ h.price }}</td><td>{{ h.stock }}</td><td>{{ h.version }}</td><td class="small">{{ h.updated_at }}</td>
              </tr>
            } @empty { <tr><td colspan="8" class="small">no hits{{ error() ? ' — ' + error() : '' }}</td></tr> }
          </tbody>
        </table>
      </div>
      <div class="card">
        <h3>Recent events (consumer) <span class="small">live</span></h3>
        <table>
          <thead><tr><th>event</th><th>op</th><th>name</th><th>consumed</th></tr></thead>
          <tbody>@for (e of events(); track e.event_id) {<tr class="click" (click)="open(e.product_id)"><td>{{ e.event_id }}</td><td>{{ e.op }}</td><td>{{ e.name }}</td><td class="small">{{ e.consumed_at | slice:11:19 }}</td></tr>}</tbody>
        </table>
      </div>
    </div>
    @if (sel(); as d) {
      <div class="card">
        <h3>Record {{ d.source?.id ?? '–' }} across the three stores
          @if (d.source && d.es) { <span class="pill" [class.ok]="d.source.version === d.es.version" [class.warn]="d.source.version !== d.es.version">source v{{ d.source.version }} · index v{{ d.es.version }}</span> }
          @if (d.dlq?.length) { <span class="pill bad">in DLQ</span> }
        </h3>
        <div class="grid" style="grid-template-columns: 1fr 1fr 1fr">
          <div><h3>Source (Postgres)</h3><pre>{{ p(d.source) }}</pre></div>
          <div><h3>Search index (Elasticsearch)</h3><pre>{{ p(d.es) }}</pre></div>
          <div><h3>Stream (consumer events + DLQ)</h3><pre>{{ p({ events: d.events, dlq: d.dlq }) }}</pre></div>
        </div>
      </div>
    }
  `,
})
export class DataPage implements OnInit, OnDestroy {
  private api = inject(Api);
  q = ''; category = ''; page = 1;
  hits = signal<any[]>([]); total = signal(0); cats = signal<any[]>([]); events = signal<any[]>([]); sel = signal<any>(null); error = signal('');
  f = fmt; p = pretty;
  private timer: any;
  async ngOnInit() { this.search(); this.cats.set(await this.api.get('/api/data/categories').catch(() => [])); this.tickEvents(); this.timer = setInterval(() => this.tickEvents(), 2000); }
  ngOnDestroy() { clearInterval(this.timer); }
  async search() { try { const r = await this.api.get('/api/data/search', { q: this.q, category: this.category, page: this.page, size: 25 }); this.hits.set(r.hits); this.total.set(r.total); this.error.set(r.error ?? ''); } catch (e: any) { this.error.set(e.message); } }
  async open(id: number) { this.sel.set(await this.api.get(`/api/data/${id}`)); }
  private async tickEvents() { try { this.events.set(await this.api.get('/api/data/events', { limit: 30 })); } catch { /* keep last */ } }
}
