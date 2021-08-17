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
