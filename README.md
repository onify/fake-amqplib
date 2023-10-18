# Onify fake-amqplib

[![Built latest](https://github.com/onify/fake-amqplib/actions/workflows/build-latest.yaml/badge.svg)](https://github.com/onify/fake-amqplib/actions/workflows/build-latest.yaml)[![Coverage Status](https://coveralls.io/repos/github/onify/fake-amqplib/badge.svg?branch=default)](https://coveralls.io/github/onify/fake-amqplib?branch=default)

Mocked version of https://www.npmjs.com/package/amqplib.

## Fake api

- `async connect(amqpurl[, ...otherOptions, callback])`: wait for a fake connection or expect one in the callback
- `connectSync(amqpurl[, ...otherOptions])`: utility method to create a connection without waiting for promise to resolve - synchronous
- `resetMock()`: reset all connections and brokers
- `setVersion(minor)`: next connection will be to a amqp of a specific version
- `connections`: list of faked connections

## RabbitMQ versions

Some behaviour differs between versions. To specify your version of RabbitMQ you can call `setVersion(minorVersionFloatOrString)`. Default version is 3.5.

Example:
```js
var fakeAmqp = require('@onify/fake-amqplib');

// prepare your connections
(async () => {
  fakeAmqp.setVersion('2.2');
  const conn2 = await fakeAmqp.connect('amqp://rabbit2-2');

  fakeAmqp.setVersion('3.2');
  const conn3 = await fakeAmqp.connect('amqp://rabbit3-1');

  fakeAmqp.setVersion('3.7');
  const conn37 = await fakeAmqp.connect('amqp://rabbit3-7');
})()
```

## Mocking amqplib

You might want to override `amqplib` with `@onify/fake-amqplib` in tests. This can be done this way:

### CommonJS

Example on how to mock amqplib when working with commonjs.

```javascript
const amqplib = require('amqplib');
const fakeAmqp = require('@onify/fake-amqplib');

amqplib.connect = fakeAmqp.connect;
```

or:

```javascript
const mock = require('mock-require');
const fakeAmqp = require('@onify/fake-amqplib');

mock('amqplib/callback_api', fakeAmqp);
```

or just mock the entire amqplib with:

```javascript
const mock = require('mock-require');
const fakeAmqp = require('@onify/fake-amqplib');

mock('amqplib', fakeAmqp);
```

### ESM

Example on how to mock amqplib import when working with modules.

**[Quibble](https://www.npmjs.com/package/quibble) mocha example**

Both amqplib and fake-amqplib have to be mocked if reset mock is used during testing.

_test/setup.js_
```js
import * as fakeAmqpLib from '@onify/fake-amqplib';
import { connect as fakeConnect } from '@onify/fake-amqplib';
import quibble from 'quibble';

(async () => {
  await quibble.esm('amqplib', { connect: fakeConnect });
  await quibble.esm('amqplib/callback_api', { connect: fakeConnect });
  await quibble.esm('@onify/fake-amqplib', { ...fakeAmqpLib });
})();
```

_.mocharc.json_ (true for node version < 20)
```json
{
  "recursive": true,
  "require": ["test/setup.js"],
  "node-option": [
    "experimental-specifier-resolution=node",
    "no-warnings",
    "loader=quibble"
  ]
}
```

_test/amqplib-connection-test.js_
```js
import assert from 'node:assert';
import { connect } from 'amqplib';
import { connect as connectCb } from 'amqplib/callback_api';

import { resetMock } from '@onify/fake-amqplib';

describe('connection', () => {
  afterEach(resetMock);

  it('connect promise', async () => {
    const connection = await connect('amqp://host');
    assert.equal(connection.connection.serverProperties.version, '3.5.0');
  });

  it('connect callback', (done) => {
    connectCb('amqp://host', (err, connection) => {
      if (err) return done(err);
      assert.equal(connection.connection.serverProperties.version, '3.5.0');
      done();
    });
  });
});
```
