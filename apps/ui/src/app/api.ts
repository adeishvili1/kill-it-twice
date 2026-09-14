import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { firstValueFrom } from 'rxjs';

/** Thin wrapper over the pipeline / consumer / chaos HTTP APIs. All URLs are relative: nginx proxies them. */
@Injectable({ providedIn: 'root' })
export class Api {
  private http = inject(HttpClient);
  get<T = any>(url: string, params: Record<string, string | number> = {}) { return firstValueFrom(this.http.get<T>(url, { params: params as any })); }
  post<T = any>(url: string, body: unknown = {}) { return firstValueFrom(this.http.post<T>(url, body)); }
  put<T = any>(url: string, body: unknown = {}) { return firstValueFrom(this.http.put<T>(url, body)); }

  status() { return this.get('/api/status'); }
  consumerStats() { return this.get('/consumer/api/stats'); }
}

export const fmt = (n: unknown) => (typeof n === 'number' ? n.toLocaleString('en-US') : n == null ? '–' : String(n));
export const pretty = (v: unknown) => JSON.stringify(v, null, 2);
