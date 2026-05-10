# Changelog

## unreleased

## v3.6.0 - 2026-05-10

- support [amqplib@1.1](https://github.com/amqp-node/amqplib/blob/main/CHANGELOG.md): `connectWithRecoveryPromise` / `connectWithRecoveryCallback` (no-op recovery — the fake never disconnects)
- bump amqplib devDep to `^1.2.0`; drop `@types/amqplib` (amqplib now ships its own types)
- reshape `index.d.ts` as `declare module '@onify/fake-amqplib'`; re-export `RecoveryOptions` and `SocketOptions` from amqplib
- `FakeAmqplibConnection` no longer extends `Connection` — `serverProperties` lives on `connection.serverProperties`, matching amqplib
- fix `recover()` infinite loop with active consumers: snapshot channel queue before draining

## v3.5.0 - 2026-05-02

- support [amqplib@1](https://github.com/amqp-node/amqplib/blob/main/CHANGELOG.md); bump engines to node `>=18`
- bump [smqp@12](https://github.com/paed01/smqp/blob/default/CHANGELOG.md)
- channel `recover()`
- confirm channel `waitForConfirms()`
- connection `updateSecret()` (no-op) and `'update-secret-ok'` event

## v3.4.0 - 2025-11-27

- bump [smqp@11](https://github.com/paed01/smqp/blob/default/CHANGELOG.md)

## v3.3.0 - 2025-11-15

- bump [smqp@10](https://github.com/paed01/smqp/blob/default/CHANGELOG.md)
- adjust some typings

## 3.2.0

- bump [smqp@9](https://github.com/paed01/smqp/blob/default/CHANGELOG.md)
- patch some dev dependencies

## 3.1.0

- use prettier for formatting rules since they are deprecated in eslint

## 3.0.0

- acking/nacking/rejecting an already acked message kills the channel with an unknown delivery tag error (406)
- support per channel prefetch, kills connection if prefetch is called with global argument before version 3.3
- nackAll requeue argument is true by default, as per documentation
- fake connection and channel inherits from EventEmitter

## 2.0.0

- drop node 12 support
- convert to esm with exports for node
- bump `smqp@8`
- lint some

## 1.0.0

- smqp@6.1
- lint some

## 0.9.1

- Always return queue name when asserting queue

## 0.9.0

- support connecting with urlish object
- smqp@6

## 0.8.5

- ack/nack all only cares about messages consumed by channel, previously everything was gone

## 0.8.4

- ack/nack all fix

## 0.8.3

- Call confirm channel callback when the message is queued, not when it is consumed!
- implement publish with empty string special case
- hide some internal props from message

## 0.8.2

- share behind the scenes broker if connection hosts and vhost are the same
- add new `connectSync` helper method to be able to get a connection synchronously to facilitate testing

## 0.8.1

- be a better mimic of amqplib, some stuff didn't work at all prior to this version

## Additions

- Handle different behaviours between RabbitMQ versions

## 0.8.0

- bump `smqp@5`
- stop building for node 10 (mocha's fault)

## 0.7.0

- bump `smqp@4`

## 0.6.0

- bump `smqp@3.2`

## 0.5.0

- support exclusive queue and its behaviour
- emit return on channel if mandatory message was not routed

## 0.4.0

- apparently connection is killed as well when trying to consume exclusive consumed queue
- try to mimic real behaviour and throw some errors with code

## 0.3.0

- kill channel if trying to consume exclusive consumed queue
