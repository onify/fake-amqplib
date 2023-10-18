import { Broker } from 'smqp';
import { EventEmitter } from 'events';
import { format as urlFormat } from 'url';

const kSmqp = Symbol.for('smqp');
const kClosed = Symbol.for('closed');
const kEmitter = Symbol.for('event emitter');
const kPrefetch = Symbol.for('prefetch');

class FakeAmqpError extends Error {
  constructor(message, code, killChannel, killConnection) {
    super(message);
    this.code = code;
    this._killChannel = killChannel;
    this._killConnection = killConnection;
  }
}

class FakeAmqpNotFoundError extends FakeAmqpError {
  constructor(type, name, vhost, killConnection = false) {
    super(`Channel closed by server: 404 (NOT-FOUND) with message "NOT_FOUND - no ${type} '${name}' in vhost '${vhost || '/'}'`, 404, true, killConnection);
  }
}

export class FakeAmqplibChannel {
  constructor(broker, connection) {
    this.connection = connection;

    this[kPrefetch] = 10000;
    this[kClosed] = false;
    this[kEmitter] = new EventEmitter();
    this._channelName = `channel-${generateId()}`;
    this._version = connection._version;
    this._broker = broker;

    this._emitReturn = this._emitReturn.bind(this);

    broker.on('return', this._emitReturn);
  }
  get _emitter() {
    return this[kEmitter];
  }
  get _closed() {
    return this[kClosed];
  }
  assertExchange(...args) {
    return this._callBroker(assertExchange, ...args);

    function assertExchange(exchange, ...assertArgs) {
      this.assertExchange(exchange, ...assertArgs);
      return { exchange };
    }
  }
  assertQueue(...args) {
    const connection = this.connection;
    return this._callBroker(assertQueue, ...args);

    function assertQueue(queueName, ...assertArgs) {
      const name = queueName ? queueName : `amqp.gen-${generateId()}`;
      const options = typeof assertArgs[0] === 'object' ? assertArgs.shift() : {};
      const queue = this.assertQueue(name, { ...options, _connectionId: connection._id }, ...assertArgs);
      return {
        queue: name,
        messageCount: queue.messageCount,
        consumerCount: queue.consumerCount,
      };
    }
  }
  bindExchange(destination, source, ...args) {
    const broker = this._broker;
    return Promise.all([ this.checkExchange(source), this.checkExchange(destination) ]).then(() => {
      return this._callBroker(broker.bindExchange, source, destination, ...args);
    });
  }
  bindQueue(queue, source, ...args) {
    return Promise.all([ this.checkQueue(queue), this.checkExchange(source) ]).then(() => {
      return this._callBroker(this._broker.bindQueue, queue, source, ...args);
    });
  }
  checkExchange(name, ...args) {
    const connPath = this.connection._url.pathname;
    return this._callBroker(check, ...args);

    function check() {
      if (!this.getExchange(name)) throw new FakeAmqpNotFoundError('exchange', name, connPath);
      return true;
    }
  }
  checkQueue(name, ...args) {
    const connPath = this.connection._url.pathname;
    return this._callBroker(check, ...args);

    function check() {
      let queue;
      if (!(queue = this.getQueue(name))) {
        throw new FakeAmqpNotFoundError('queue', name, connPath);
      }

      return {
        messageCount: queue.messageCount,
        consumerCount: queue.consumerCount,
      };
    }
  }
  get(queue, ...args) {
    const connPath = this.connection._url.pathname;
    return this._callBroker(getMessage, ...args);

    function getMessage(...getargs) {
      const q = this.getQueue(queue);
      if (!q) throw new FakeAmqpNotFoundError('queue', queue, connPath._url.pathname);
      const msg = q.get(...getargs) || false;
      if (!msg) return msg;
      return new Message(msg);
    }
  }
  deleteExchange(exchange, ...args) {
    const connPath = this.connection._url.pathname;
    return this._callBroker(check, ...args);

    function check() {
      const result = this.deleteExchange(exchange, ...args);
      if (!result && this.owner.version < 3.2) throw new FakeAmqpNotFoundError('exchange', exchange, connPath);
      return result;
    }
  }
  deleteQueue(queue, ...args) {
    const connPath = this.connection._url.pathname;
    return this._callBroker(check, ...args);

    function check() {
      const result = this.deleteQueue(queue, ...args);
      if (!result && this.owner.version < 3.2) throw new FakeAmqpNotFoundError('queue', queue, connPath);
      return result;
    }
  }
  publish(exchange, routingKey, content, options, callback) {
    if (!Buffer.isBuffer(content)) throw new TypeError('content is not a buffer');
    if (exchange === '') return this.sendToQueue(routingKey, content, options, callback);

    const args = [ this._broker.publish, exchange, routingKey, content ];

    args.push(options, callback);

    this.checkExchange(exchange).then(() => {
      return this._callBroker(...args);
    }).catch((err) => {
      this[kEmitter].emit('error', err);
    });

    return true;
  }
  purgeQueue(queue, ...args) {
    const connPath = this.connection._url.pathname;
    return this._callBroker(check, ...args);

    function check() {
      const result = this.purgeQueue(queue);
      if (!result && this.owner.version < 3.2) throw new FakeAmqpNotFoundError('queue', queue, connPath);
      return result === undefined ? undefined : { messageCount: result };
    }
  }
  sendToQueue(queue, content, options, callback) {
    if (!Buffer.isBuffer(content)) throw new TypeError('content is not a buffer');

    const args = [ this._broker.sendToQueue, queue, content ];

    args.push(options, callback);

    this.checkQueue(queue).then(() => {
      return this._callBroker(...args);
    }).catch((err) => {
      this[kEmitter].emit('error', err);
    });

    return true;
  }
  unbindExchange(destination, source, pattern, ...args) {
    const connPath = this.connection._url.pathname;
    return this._callBroker(check, ...args);

    function check() {
      const q = this.getExchange(destination);
      if (!q) throw new FakeAmqpNotFoundError('exchange', destination);

      const exchange = this.getExchange(source);
      if (!exchange) throw new FakeAmqpNotFoundError('exchange', source);

      const result = this.unbindExchange(source, destination, pattern);
      if (!result && this.owner.version <= 3.2) throw new FakeAmqpNotFoundError('binding', pattern, connPath);

      return true;
    }
  }
  unbindQueue(queue, source, pattern, ...args) {
    const connPath = this.connection._url.pathname;
    return this._callBroker(check, ...args);

    function check() {
      const q = this.getQueue(queue);
      if (!q) throw new FakeAmqpNotFoundError('queue', queue);

      const exchange = this.getExchange(source);
      if (!exchange) throw new FakeAmqpNotFoundError('exchange', source);

      const binding = exchange.getBinding(queue, pattern);
      if (!binding && this.owner.version <= 3.2) {
        throw new FakeAmqpNotFoundError('binding', pattern, connPath, this.owner.version < 3.2);
      }

      this.unbindQueue(queue, source, pattern);
      return true;
    }
  }
  consume(queue, onMessage, options = {}, callback) {
    const { _id: connId, _url: connUrl } = this.connection;
    const channelName = this._channelName;
    const prefetch = this[kPrefetch];

    return this._callBroker(check, callback);

    function check() {
      const q = queue && this.getQueue(queue);
      if (!q) {
        throw new FakeAmqpNotFoundError('queue', queue, connUrl.pathname);
      }

      if (q.exclusive || (q.options.exclusive && q.options._connectionId !== connId)) {
        throw new FakeAmqpError(`Channel closed by server: 403 (ACCESS-REFUSED) with message "ACCESS_REFUSED - queue '${queue}' in vhost '${connUrl.pathname}' in exclusive use"`, 403, true, true);
      }

      const { consumerTag } = this.consume(queue, onMessage && handler, {
        ...options,
        channelName,
        prefetch,
      });
      return { consumerTag };
    }

    function handler(_, msg) {
      onMessage(new Message(msg));
    }
  }
  cancel(consumerTag, ...args) {
    return this._callBroker(this._broker.cancel, consumerTag, ...args);
  }
  close(callback) {
    if (this[kClosed]) return;
    const channelName = this._channelName;
    const broker = this._broker;

    broker.off('return', this._emitReturn);
    const channelConsumers = broker.getConsumers().filter((f) => f.options.channelName === channelName);
    channelConsumers.forEach((c) => broker.cancel(c.consumerTag));
    this[kClosed] = true;
    this[kEmitter].emit('close');
    return resolveOrCallback(callback);
  }
  ack(message, ...args) {
    this._broker.ack(message[kSmqp], ...args);
  }
  ackAll() {
    const broker = this._broker;
    const channelName = this._channelName;

    const consumers = broker.getConsumers().filter(({ options }) => options.channelName === channelName);
    consumers.forEach((c) => broker.getConsumer(c.consumerTag).ackAll());
  }
  nack(message, ...args) {
    if (this.connection._version >= 2.3) throw new Error(`Nack is not implemented in versions before 2.3 (${this.connection._version})`);
    return this._broker.nack(message[kSmqp], ...args);
  }
  reject(message, ...args) {
    this._broker.reject(message[kSmqp], ...args);
  }
  nackAll(requeue = false) {
    const broker = this._broker;
    const channelName = this._channelName;

    const consumers = broker.getConsumers().filter(({ options }) => options.channelName === channelName);
    consumers.forEach((c) => broker.getConsumer(c.consumerTag).nackAll(requeue));
  }
  prefetch(val) {
    this[kPrefetch] = val;
  }
  on(...args) {
    return this[kEmitter].on(...args);
  }
  once(...args) {
    return this[kEmitter].once(...args);
  }
  _callBroker(fn, ...args) {
    let [ poppedCb ] = args.slice(-1);
    if (typeof poppedCb === 'function') args.splice(-1);
    else poppedCb = null;

    if (this.connection._closed) throw new FakeAmqpError('Connection is closed', 504);
    if (this[kClosed]) throw new Error('Channel is closed');

    return new Promise((resolve, reject) => {
      try {
        const result = fn.call(this._broker, ...args);
        if (poppedCb) poppedCb(null, result);
        return resolve(result);
      } catch (err) {
        if (err._killConnection) this.connection.close();
        else if (err._killChannel) this[kClosed] = true;
        if (!poppedCb) return reject(err);
        poppedCb(err);
        return resolve();
      }
    });
  }
  _emitReturn({ fields, content, properties }) {
    process.nextTick(() => {
      this[kEmitter].emit('return', { fields, content, properties });
    });
  }
}

export class FakeAmqplibConfirmChannel extends FakeAmqplibChannel {
  publish(exchange, routingKey, content, options, callback) {
    if (!Buffer.isBuffer(content)) throw new TypeError('content is not a buffer');
    if (exchange === '') return this.sendToQueue(routingKey, content, options, callback);

    const args = [ this._broker.publish, exchange, routingKey, content ];

    args.push(...addConfirmCallback(this._broker, options, callback));

    this.checkExchange(exchange).then(() => {
      return this._callBroker(...args);
    }).catch((err) => {
      this[kEmitter].emit('error', err);
    });

    return true;
  }
  sendToQueue(queue, content, options, callback) {
    if (!Buffer.isBuffer(content)) throw new TypeError('content is not a buffer');

    const args = [ this._broker.sendToQueue, queue, content ];

    args.push(...addConfirmCallback(this._broker, options, callback));

    this.checkQueue(queue).then(() => {
      return this._callBroker(...args);
    }).catch((err) => {
      this[kEmitter].emit('error', err);
    });

    return true;
  }
}

export class FakeAmqplibConnection {
  constructor(broker, version, amqpUrl) {
    this[kEmitter] = new EventEmitter();
    this[kClosed] = false;
    this._channels = [];
    this._url = normalizeAmqpUrl(amqpUrl);
    this._id = generateId();
    this._broker = broker;
    this._version = version;
  }
  get _closed() {
    return this[kClosed];
  }
  get _emitter() {
    return this[kEmitter];
  }
  get connection() {
    return {
      serverProperties: {
        host: this._url.host,
        product: 'RabbitMQ',
        version: `${this._version.toString()}.0`,
        platform: 'OS',
        copyright: 'MIT',
        information: 'fake',
      },
    };
  }
  createChannel(...args) {
    const callback = args.slice(-1)[0];
    if (this[kClosed]) return resolveOrCallback(callback, new FakeAmqpError('Connection closed: 504', 504));

    const channel = new FakeAmqplibChannel(this._broker, this);
    this._channels.push(channel);
    return resolveOrCallback(callback, null, channel);
  }
  createConfirmChannel(...args) {
    if (this[kClosed]) return resolveOrCallback(args.slice(-1)[0], new FakeAmqpError('Connection closed: 504', 504));

    const channel = new FakeAmqplibConfirmChannel(this._broker, this);
    this._channels.push(channel);
    return resolveOrCallback(args.slice(-1)[0], null, channel);
  }
  close(...args) {
    if (this[kClosed]) return resolveOrCallback(args.slice(-1)[0]);
    this[kClosed] = true;

    this._channels.splice(0).forEach((channel) => channel.close());

    this[kEmitter].emit('close');

    return resolveOrCallback(args.slice(-1)[0]);
  }
  on(...args) {
    return this[kEmitter].on(...args);
  }
  once(...args) {
    return this[kEmitter].once(...args);
  }
}

export function FakeAmqplib(minorVersion = '3.5') {
  if (!(this instanceof FakeAmqplib)) {
    return new FakeAmqplib(minorVersion);
  }

  this.version = Number(minorVersion);
  this.connections = [];

  this.connect = this.connect.bind(this);
  this.connectSync = this.connectSync.bind(this);
  this.resetMock = this.resetMock.bind(this);
  this.setVersion = this.setVersion.bind(this);
}

FakeAmqplib.prototype.connect = function fakeConnect(amqpUrl, ...args) {
  const connection = this.connectSync(amqpUrl, ...args);
  return resolveOrCallback(args.slice(-1)[0], null, connection);
};

FakeAmqplib.prototype.connectSync = function fakeConnectSync(amqpUrl, ...args) {
  const { _broker } = this.connections.find((conn) => compareConnectionString(conn._url, amqpUrl)) || {};
  const broker = _broker || new Broker(this);
  const connection = new FakeAmqplibConnection(broker, this.version, amqpUrl, ...args);

  const connections = this.connections;

  connections.push(connection);

  connection._emitter.once('close', () => {
    const idx = connections.indexOf(connection);
    if (idx > -1) connections.splice(idx, 1);
  });

  return connection;
};

FakeAmqplib.prototype.resetMock = function fakeResetMock() {
  for (const connection of this.connections.splice(0)) {
    connection._broker.reset();
  }
};

FakeAmqplib.prototype.setVersion = function fakeSetVersion(minorVersion) {
  const n = Number(minorVersion);
  if (!isNaN(n)) this.version = n;
};

function resolveOrCallback(optionalCb, err, ...args) {
  if (typeof optionalCb === 'function') optionalCb(err, ...args);
  if (err) return Promise.reject(err);
  return Promise.resolve(...args);
}

function generateId() {
  return Math.random().toString(16).substring(2, 12);
}

function compareConnectionString(url1, url2) {
  const parsedUrl1 = normalizeAmqpUrl(url1);
  const parsedUrl2 = normalizeAmqpUrl(url2);

  return parsedUrl1.host === parsedUrl2.host && parsedUrl1.pathname === parsedUrl2.pathname;
}

function Message(smqpMessage) {
  this[kSmqp] = smqpMessage;
  this.content = smqpMessage.content;
  this.fields = smqpMessage.fields;
  this.properties = smqpMessage.properties;
}

function normalizeAmqpUrl(url) {
  if (!url) return new URL('amqp://localhost:5672/');
  if (typeof url === 'string') url = new URL(url);

  if (!(url instanceof URL)) {
    const {
      protocol = 'amqp',
      hostname = 'localhost',
      port = 5672,
      vhost = '/',
      username,
      password,
      ...rest
    } = url;
    let auth = username;
    if (auth && password) {
      auth += `:${password}`;
    }
    url = new URL(urlFormat({
      protocol,
      hostname,
      port,
      pathname: vhost,
      slashes: true,
      auth,
    }));

    for (const k in rest) {
      switch (k) {
        case 'locale':
        case 'frameMax':
        case 'heartbeat':
          url.searchParams.set(k, rest[k]);
          break;
      }
    }
  }

  if (!url.port) url.port = 5672;
  if (!url.pathname) url.pathname = '/';
  return url;
}

function addConfirmCallback(broker, options, callback) {
  const confirm = `msg.${generateId()}`;
  const consumerTag = `ct-${confirm}`;
  options = { ...options, confirm };

  broker.on('message.*', onConsumeMessage, { consumerTag });

  let undelivered;
  function onConsumeMessage(event) {
    switch (event.name) {
      case 'message.nack':
      case 'message.undelivered':
        undelivered = event.name;
        break;
    }
  }

  function confirmCallback() {
    broker.off('message.*', consumerTag);
    switch (undelivered) {
      case 'message.nack':
        return callback(new Error('message nacked'));
      case 'message.undelivered':
        throw callback(new Error('message undelivered'));
      default:
        return callback(null, true);
    }
  }

  return [ options, confirmCallback ];
}

const defaultFake = new FakeAmqplib('3.5');
export const connections = defaultFake.connections;

export function connect(amqpUrl, ...args) {
  return defaultFake.connect(amqpUrl, ...args);
}

export function connectSync(amqpUrl, ...args) {
  return defaultFake.connectSync(amqpUrl, ...args);
}

export function resetMock() {
  return defaultFake.resetMock();
}

export function setVersion(minorVersion) {
  return defaultFake.setVersion(minorVersion);
}
