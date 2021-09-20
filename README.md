Onify fake-amqplib
==================

[![Built latest](https://github.com/onify/fake-amqplib/actions/workflows/build-latest.yaml/badge.svg)](https://github.com/onify/fake-amqplib/actions/workflows/build-latest.yaml)

Mocked version of https://www.npmjs.com/package/amqplib.

### Overriding amqplib

You might want to override `amqplib` with `@onify/fake-amqplib` in tests. This can be done this way:

```javascript
const amqplib = require('amqplib');
const fakeAmqp = require('@onify/fake-amqplib');

amqplib.connect = fakeAmqp.connect;
```

If you are using version 2 or higher of [exp-amqp-connection](https://www.npmjs.com/package/exp-amqp-connection)
you can use [mock-require](https://www.npmjs.com/package/mock-require) to replace `amqplib` with `@onify/fake-amqplib` in your tests like this:

```javascript
const mock = require('mock-require');
const fakeAmqp = require('@onify/fake-amqplib');

mock('amqplib/callback_api', fakeAmqp);
```

or just mock the entire amqplib with

```javascript
const mock = require('mock-require');
const fakeAmqp = require('@onify/fake-amqplib');

mock('amqplib', fakeAmqp);
```

### RabbitMQ versions

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

# Api
- `resetMock()`: reset all connections and brokers
