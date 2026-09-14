import { Global, Module } from '@nestjs/common';
import { SourceService } from './source.service';
@Global()
@Module({ providers: [SourceService], exports: [SourceService] })
export class SourceModule {}
