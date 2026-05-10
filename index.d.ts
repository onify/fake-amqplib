/// <reference types="node" />

declare module '@onify/fake-amqplib' {
  import { Options, Channel, ChannelModel, Replies, RecoveryOptions, SocketOptions } from 'amqplib';
  import { Broker } from 'smqp';

  export { RecoveryOptions, SocketOptions };

  export interface FakeAmqplibChannel extends Channel {
    /** Channel name and identifier, for faking purposes */
    _channelName: string;
    _broker: Broker;
    _version: number;
    readonly _closed: boolean;
  }

  export const FakeAmqplibChannel: {
    new (broker: Broker, connection: FakeAmqplibConnection): FakeAmqplibChannel;
  };

  export interface FakeAmqplibConfirmChannel extends FakeAmqplibChannel {
    publish(
      exchange: string,
      routingKey: string,
      content: Buffer,
      options?: Options.Publish,
      callback?: (err: Error | null, ok: Replies.Empty) => void
    ): boolean;
    sendToQueue(
      queue: string,
      content: Buffer,
      options?: Options.Publish,
      callback?: (err: Error | null, ok: Replies.Empty) => void
    ): boolean;
    waitForConfirms(callback?: (err: Error | null) => void): Promise<void>;
  }

  export const FakeAmqplibConfirmChannel: {
    new (broker: Broker, connection: FakeAmqplibConnection): FakeAmqplibConfirmChannel;
  };

  export interface FakeAmqplibConnection extends ChannelModel {
    _channels: FakeAmqplibChannel[];
    _url: URL;
    /** Connection identifier, for faking purposes */
    _id: string;
    _broker: Broker;
    _version: number;
    readonly _closed: boolean;
  }

  export const FakeAmqplibConnection: {
    new (broker: Broker, version: number, amqpUrl: string, options?: SocketOptions): FakeAmqplibConnection;
  };

  export type ConnectCallback = (err: Error | null, connection: FakeAmqplibConnection) => void;

  export interface FakeAmqplib {
    version: number;
    connections: FakeAmqplibConnection[];
    connect(url: string | Options.Connect, socketOptions?: SocketOptions): Promise<FakeAmqplibConnection>;
    connect(url: string | Options.Connect, socketOptions: SocketOptions, callback: ConnectCallback): void;
    connect(url: string | Options.Connect, callback: ConnectCallback): void;
    connectSync(url: string | Options.Connect, socketOptions?: SocketOptions): FakeAmqplibConnection;
    connectWithRecoveryPromise(
      url: string | Options.Connect,
      recoveryOptions?: RecoveryOptions,
      socketOptions?: SocketOptions
    ): Promise<FakeAmqplibConnection>;
    connectWithRecoveryCallback(
      url: string | Options.Connect,
      recoveryOptions: RecoveryOptions,
      socketOptions: SocketOptions,
      callback: ConnectCallback
    ): void;
    resetMock(): void;
    setVersion(minorVersion: number | string): void;
  }

  export const FakeAmqplib: {
    new (minorVersion?: number | string): FakeAmqplib;
    (minorVersion?: number | string): FakeAmqplib;
  };

  export const connections: FakeAmqplibConnection[];

  export function connect(url: string | Options.Connect, socketOptions?: SocketOptions): Promise<FakeAmqplibConnection>;
  export function connect(url: string | Options.Connect, socketOptions: SocketOptions, callback: ConnectCallback): void;
  export function connect(url: string | Options.Connect, callback: ConnectCallback): void;
  export function connectSync(url: string | Options.Connect, socketOptions?: SocketOptions): FakeAmqplibConnection;
  export function connectWithRecoveryPromise(
    url: string | Options.Connect,
    recoveryOptions?: RecoveryOptions,
    socketOptions?: SocketOptions
  ): Promise<FakeAmqplibConnection>;
  export function connectWithRecoveryCallback(
    url: string | Options.Connect,
    recoveryOptions: RecoveryOptions,
    socketOptions: SocketOptions,
    callback: ConnectCallback
  ): void;
  export function resetMock(): void;
  export function setVersion(minorVersion: number | string): void;
}
