/// <reference types="amqplib" />
/// <reference types="node" />

import { Options, Connection, Channel, credentials } from "amqplib";
import { EventEmitter } from "events";
import { Broker } from 'smqp';

export interface FakeAmqplibChannel extends Channel {
  _channelName: string;
  _version: number;
  _broker: Broker;
  new(broker: Broker, connection: FakeAmqplibConnection): FakeAmqplibChannel;
  get _closed(): boolean;
  get _emitter(): EventEmitter;
}

export interface FakeAmqplibConnection extends Connection {
  _channels: FakeAmqplibChannel[];
  _url: URL;
  _id: string;
  _broker: Broker;
  _version: number;
  new(broker: Broker, version: number, amqpUrl: string, options?: any): FakeAmqplibConnection;
  get _closed(): boolean;
  get _emitter(): EventEmitter;
}

interface SocketOptions {
  host?: string;
  keepAlive?: boolean;
  keepAliveDelay?: number;
  noDelay?: boolean;
  port?: number;
  serverName?: string;
  timeout?: number;
  [x: string]: any;
}

type connectCallback = (
  err: Error,
  connection: FakeAmqplibConnection
) => void;

export class FakeAmqplib {
  connections: FakeAmqplibConnection[];
  constructor(version?: number)
  connect(url: string | Options.Connect, socketOptions?: SocketOptions): Promise<FakeAmqplibConnection>;
  connect(url: string | Options.Connect, socketOptions: SocketOptions, callback: connectCallback): void;
  connect(url: string | Options.Connect, callback: (err: Error, connection: FakeAmqplibConnection) => void): void;
  connectSync(url: string | Options.Connect, socketOptions?: SocketOptions): FakeAmqplibConnection;
  resetMock(): void;
  setVersion(minorVersion: number): void;
}

export function connect(url: string | Options.Connect, socketOptions?: SocketOptions): Promise<FakeAmqplibConnection>;
export function connect(url: string | Options.Connect, socketOptions: SocketOptions, callback: connectCallback): void;
export function connect(url: string | Options.Connect, callback: connectCallback): void;
export function connectSync(url: string | Options.Connect, socketOptions?: SocketOptions): FakeAmqplibConnection;
export function resetMock(): void;
export function setVersion(minorVersion: number): void;
