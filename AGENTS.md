# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`@onify/fake-amqplib` is a drop-in fake of [`amqplib`](https://www.npmjs.com/package/amqplib) backed by [`smqp`](https://www.npmjs.com/package/smqp), used to test RabbitMQ-consuming code without a real broker. It is published as a dual-format package (ESM `index.js` + CJS `main.cjs` rolled up via Rollup) and ships TypeScript types in `index.d.ts`.

## Common commands

```bash
npm test                     # mocha (recursive, loads chai/register-expect.js)
npm test -- --grep "<name>"  # run a single test or describe block by name
npm run lint                 # eslint (cached) + prettier --check (cached)
npm run dist                 # rollup -c → main.cjs
npm run cov:html             # c8 HTML coverage report into ./coverage
```

`posttest` runs `lint` and `dist`, so `npm test` does the full pre-publish gate. Node 18 (`.nvmrc`).

**Coverage is a USP.** Aim for 100% statements / branches / functions / lines on every change. Run `npx c8 mocha` after touching `index.js`; if a line you introduced isn't covered, add a test or delete the line. The bar is enforced by hand and reviewed per-PR — do not add `.c8rc.json` thresholds or CI gates.

## Architecture

`index.js` is the single source file. Everything else (`main.cjs`, `index.d.ts`) is generated or hand-maintained alongside it.

Key types and how they fit together:

- **`FakeAmqplib`** (the factory) holds a list of `connections` and a current `version`. The module exports a default singleton instance plus bound `connect` / `connectSync` / `resetMock` / `setVersion` functions, **and** the class itself for callers that want isolated instances. Tests sometimes `new FakeAmqplib('2.2')` to pin a version without touching the singleton — keep both surfaces working.
- **`AmqplibBroker extends Broker` (smqp)** — one broker is shared across all connections that point at the same `host + vhost` (see `compareConnectionString`). This is intentional: it lets a publisher and a consumer in the same test see each other when they `connect()` independently. It also means `resetMock()` must reset every broker, which it does by iterating connections.
- **`FakeAmqplibConnection`** wraps a broker and a parsed URL, tracks open `_channels`, and emits `close`. The connection's `_version` is captured at connect time, so `setVersion()` only affects _future_ connections.
- **`FakeAmqplibChannel` / `FakeAmqplibConfirmChannel`** translate the amqplib API onto smqp. The confirm-channel variant differs only in `publish` / `sendToQueue`, which inject a confirm callback via `addConfirmCallback` (subscribes to `message.*` on the broker to detect nack/undelivered before resolving the user callback).

### Delivery-tag and channel-queue model

Each channel maintains its own internal smqp queue named `#channel-<id>` (`_channelQueue`). Every consumed/get'd message is wrapped in a `Message` with a broker-wide monotonically increasing `deliveryTag` and queued on the channel queue. `ack` / `nack` / `reject` look the message up by delivery tag; if it's missing or already settled, they throw `FakeAmqpUnknownDeliveryTag` (406) which the channel's `_callBroker` catches and (because `_killChannel` is set) tears down the channel and re-emits as an `error`. This is what the v3.0 changelog entry "acking an already-acked message kills the channel" refers to — preserve this behavior.

`ackAll` / `nackAll` drain the channel queue and ack/reject the underlying smqp messages in order. `allUpToDeliveryTag` implements the `allUpTo: true` variants by spawning a temporary smqp consumer with infinite prefetch.

**Snapshot before draining `_channelQueue`.** Anything that drains the channel queue and triggers requeues to the source queue (`recover()`, and similar paths) must collect entries into a snapshot array first — never use a live `while ((msg = channelQ.get())) { ... }` loop. Each `reject(true)` on the underlying smqp message synchronously calls `_consumeNext()`, which dispatches to any active consumer, which pushes a new entry into `_channelQueue` mid-drain. A live loop will never terminate (Node OOM). The faithful-to-RabbitMQ behavior is that consumers stay subscribed and receive redelivered messages with `fields.redelivered === true`; consumer code that ignores `redelivered` and re-nacks will still loop forever, but that mirrors real RabbitMQ.

### Prefetch

Two values per channel: consumer prefetch (`kPrefetch`, default 10000) and channel-wide prefetch (`kChannelPrefetch`, default Infinity). `_calculateChannelCapacity` clamps a consumer's reported `capacity` by remaining channel-queue room. **Calling `prefetch(val, true)` (the global/channel-wide form) on a connection with `_version < 3.3` closes the connection** — that is RabbitMQ-faithful behavior, not a bug.

### RabbitMQ version gating

Several methods branch on `this.connection._version` or `this.owner.version` to mimic version-specific RabbitMQ quirks:

- `< 2.3`: `nack()` throws "not implemented".
- `< 3.2`: `deleteExchange` / `deleteQueue` / `purgeQueue` / `unbindExchange` / `unbindQueue` throw `404 NOT-FOUND` for missing targets; later versions return falsy/idempotent.
- `< 3.3`: channel-wide prefetch unsupported (closes connection, see above).

When adding new behaviour, check `test/rabbitmq-version-test.js` first — it documents the expected divergences.

### Error model

`FakeAmqpError` carries `code`, `_killChannel`, `_killConnection`, and optionally `_emit`. `_callBroker` is the single chokepoint that interprets these flags: kill the connection, kill the channel, and/or emit an `error` event. New error paths should funnel through this so the kill semantics stay consistent.

**When to throw `FakeAmqpError` vs a plain `Error`.** Use `FakeAmqpError` when there is a corresponding AMQP class.method code (e.g. 404 NOT-FOUND, 406 PRECONDITION-FAILED, 504 CHANNEL-ERROR, 403 ACCESS-REFUSED, 312 NO-ROUTE) — that code drives `_killChannel`/`_killConnection` semantics and is what consumers branch on. When there is no AMQP code, throw a plain `new Error(...)`:

- For publish-confirm `basic.nack`, mirror amqplib's source verbatim: `new Error('message nacked')`. Consumer code that does `if (err.message === 'message nacked')` keeps working against both the real lib and this fake.
- For close-related errors (closed-channel ops, drain on close), use the shared `CHANNEL_CLOSED_ERROR` constant. amqplib uses `'channel closed'` here, but the fake unifies all close paths on one message so the closed-check in `_callBroker`, `publish`/`sendToQueue`, and the confirm-drain all match.

## Testing notes

- Mocha + chai's `expect` (registered globally via `chai/register-expect.js` in `.mocharc.json`).
- Tests import from `@onify/fake-amqplib` (the package name), which resolves to `index.js` via `package.json` `exports`. The default singleton leaks state across files, so most suites call `resetMock()` in `after`/`afterEach` and reset version with `setVersion('3.5')`.
- `connectSync` exists specifically so tests can grab a connection without awaiting — use it when writing setup that doesn't need to be async.
- `test/channel-prefetch-test.js` and `test/rabbitmq-version-test.js` are the canonical references for the two trickiest behaviours (prefetch math, version gating). Mirror their patterns when adding cases.
