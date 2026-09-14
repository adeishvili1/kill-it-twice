import { Controller, Get } from '@nestjs/common';
import { StatusService } from './status.service';

@Controller()
export class StatusController {
  constructor(private readonly status: StatusService) {}
  @Get('health') health() { return this.status.health(); }
  @Get('api/status') status_() { return this.status.status(); }
}
