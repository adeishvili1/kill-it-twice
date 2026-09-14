import { Component, OnDestroy, OnInit, inject, signal } from '@angular/core';
import { RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';
import { Api } from './api';

@Component({
  selector: 'app-root',
  imports: [RouterOutlet, RouterLink, RouterLinkActive],
  template: `
    <header class="top">
      <div class="brand">Kill It Twice <span class="muted">· replication pipeline</span></div>
      <nav>
        <a routerLink="/dashboard" routerLinkActive="active">Dashboard</a>
        <a routerLink="/data" routerLinkActive="active">Data</a>
        <a routerLink="/control" routerLinkActive="active">Control</a>
        <a routerLink="/simulation" routerLinkActive="active">Simulation</a>
        <a href="/metrics" target="_blank">/metrics</a>
      </nav>
      <div class="badge" [class.ok]="health() === 'ok'" [class.bad]="health() === 'degraded'" [class.unknown]="health() === 'unreachable'">
        {{ health() }}
      </div>
    </header>
    <main><router-outlet /></main>
  `,
})
export class App implements OnInit, OnDestroy {
  private api = inject(Api);
  health = signal<'ok' | 'degraded' | 'unreachable'>('unreachable');
  private timer: any;
  ngOnInit() { this.tick(); this.timer = setInterval(() => this.tick(), 2000); }
  ngOnDestroy() { clearInterval(this.timer); }
  private async tick() {
    try { const h = await this.api.get('/health'); this.health.set(h.status === 'ok' ? 'ok' : 'degraded'); }
    catch { this.health.set('unreachable'); }
  }
}
