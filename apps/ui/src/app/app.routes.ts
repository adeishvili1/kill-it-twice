import { Routes } from '@angular/router';
import { DashboardPage } from './pages/dashboard';
import { DataPage } from './pages/data';
import { ControlPage } from './pages/control';
import { SimulationPage } from './pages/simulation';

export const routes: Routes = [
  { path: '', pathMatch: 'full', redirectTo: 'dashboard' },
  { path: 'dashboard', component: DashboardPage },
  { path: 'data', component: DataPage },
  { path: 'control', component: ControlPage },
  { path: 'simulation', component: SimulationPage },
  { path: '**', redirectTo: 'dashboard' },
];
