# Onify fake-amqplib

[![Built latest](https://github.com/onify/fake-amqplib/actions/workflows/build-latest.yaml/badge.svg)](https://github.com/onify/fake-amqplib/actions/workflows/build-latest.yaml)[![Coverage Status](https://coveralls.io/repos/github/onify/fake-amqplib/badge.svg?branch=default)](https://coveralls.io/github/onify/fake-amqplib?branch=default)

Mocked version of https://www.npmjs.com/package/amqplib.

<!-- toc -->

- [Fake api](#fake-api)
- [RabbitMQ versions](#rabbitmq-versions)
- [Mocking amqplib](#mocking-amqplib)
  - [ESM](#esm)
    - [Node 20+ — `node:test` `mock.module` (recommended)](#node-20-nodetest-mockmodule-recommended)
    - [Alternative — Quibble](#alternative-quibble)
  - [CommonJS](#commonjs)

<!-- /toc -->

## Fake api

- `async connect(amqpurl[, ...otherOptions, callback])`: wait for a fake connection or expect one in the callback
- `connectSync(amqpurl[, ...otherOptions])`: utility method to create a connection without waiting for promise to resolve - synchronous
- `resetMock()`: reset all connections and brokers
- `setVersion(minor)`: next connection will be to a amqp of a specific version
- `connections`: list of faked connections

## RabbitMQ versions

RabbitMQ behaviour differs between versions. To specify your version of RabbitMQ you can call `setVersion(minorVersionFloatOrString)`. Default version is 3.5.

Example:

```js
var fakeAmqp = require('@onify/fake-amqplib');

// prepare your connections
(async () => {
  fakeAmqp.setVersion('2.2');
  const conn2 = await fakeAmqp.connect('amqp://rabbit2-2');

  fakeAmqp.setVersion('3.2');
  const conn3 = await fakeAmqp.connect('amqp://rabbit3-2');

  fakeAmqp.setVersion('3.7');
  const conn37 = await fakeAmqp.connect('amqp://rabbit3-7');
})();
```

## Mocking amqplib

You might want to override `amqplib` with `@onify/fake-amqplib` in tests. This can be done in a number of ways.

### ESM

Example on how to mock the `amqplib` import when working with modules.

#### Node 20+ — `node:test` `mock.module` (recommended)

Node's built-in test runner ships an experimental module-mocking API that needs no extra dependency. Set it up at the top of the test file (or in a setup file), then dynamically import `amqplib`.

_.mocharc.json_

```json
{
  "recursive": true,
  "require": ["chai/register-expect.js"],
  "node-option": ["experimental-test-module-mocks", "no-warnings"]
}
```

The `experimental-test-module-mocks` flag enables `mock.module`; `no-warnings` silences the "experimental feature" notice. Drop it if you'd rather see the warning.

_test/amqplib-connection-test.js_

```javascript
import { mock } from 'node:test';
import { connect as fakeConnect, resetMock } from '@onify/fake-amqplib';

describe('connection', () => {
  let connect;
  let ctx;

  before(async () => {
    ctx = mock.module('amqplib', { namedExports: { connect: fakeConnect } });
    ({ connect } = await import('amqplib'));
  });

  after(() => {
    ctx.restore();
    resetMock();
  });

  it('connects to the fake', async () => {
    const connection = await connect('amqp://host');
    expect(connection.connection.serverProperties).to.have.property('product', 'RabbitMQ');
  });
});
```

If you also use `mocha --parallel` or run tests via `node --test`, the same setup works — `mock.module` is process-global, so register it before the first dynamic import in each test file.

#### Alternative — Quibble

[Quibble](https://www.npmjs.com/package/quibble) is useful if you're on a Node version older than 20, or prefer not to rely on an experimental flag.

_test/setup.js_

```js
import * as fakeAmqpLib from '@onify/fake-amqplib';
import { connect as fakeConnect } from '@onify/fake-amqplib';
import quibble from 'quibble';

(async () => {
  await quibble.esm('amqplib', { connect: fakeConnect });
  await quibble.esm('@onify/fake-amqplib', { ...fakeAmqpLib });
})();
```

_.mocharc.json_ (the `loader=quibble` option is only needed on Node < 20)

```json
{
  "recursive": true,
  "require": ["test/setup.js"],
  "node-option": ["experimental-specifier-resolution=node", "no-warnings", "loader=quibble"]
}
```

Then import `amqplib` normally in your tests; quibble rewires the import.

### CommonJS

Example on how to mock amqplib when working with commonjs.

```js
const amqplib = require('amqplib');
const fakeAmqp = require('@onify/fake-amqplib');

amqplib.connect = fakeAmqp.connect;
```

or:

```js
const mock = require('mock-require');
const fakeAmqp = require('@onify/fake-amqplib');

mock('amqplib/callback_api', fakeAmqp);
```

or just mock the entire amqplib with:

```js
const mock = require('mock-require');
const fakeAmqp = require('@onify/fake-amqplib');

mock('amqplib', fakeAmqp);
```
