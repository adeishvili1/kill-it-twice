import { Global, Module } from '@nestjs/common';
import { CheckpointService } from './checkpoint.service';
@Global()
@Module({ providers: [CheckpointService], exports: [CheckpointService] })
export class CheckpointModule {}
