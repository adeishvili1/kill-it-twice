import { Global, Module } from '@nestjs/common';
import { EsSink } from './es/es.sink';
import { RabbitSink } from './rabbit/rabbit.sink';
import { SinkGate } from './sink-gate';
@Global()
@Module({ providers: [EsSink, RabbitSink, SinkGate], exports: [EsSink, RabbitSink, SinkGate] })
export class SinksModule {}
