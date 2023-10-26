import { Broker } from 'smqp';
import { EventEmitter } from 'events';
import { format as urlFormat } from 'url';

const kSmqp = Symbol.for('smqp');
const kClosed = Symbol.for('closed');
const kDeliveryTag = Symbol.for('channel delivery tag');
const kPrefetch = Symbol.for('prefetch');
const kChannelPrefetch = Symbol.for('channel prefetch');

class AmqplibBroker extends Broker {
  constructor(...args) {
    super(...args);
    this[kDeliveryTag] = 0;
  }
  _getNextDeliveryTag() {
    return ++this[kDeliveryTag];
  }
  _getMessageByDeliveryTag(queue, deliveryTag) {
    const q = this.getQueue(queue);
    return q.messages.find((m) => m.fields.deliveryTag === deliveryTag);
  }
  _getChannelConsumers(channelName) {
    return this.getConsumers().filter((f) => f.options.channelName === channelName);
  }
}

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

class FakeAmqpUnknownDeliveryTag extends FakeAmqpError {
  constructor(deliveryTag) {
    super(`Channel closed by server: 406 (PRECONDITION-FAILED) with message "PRECONDITION_FAILED - unknown delivery tag ${deliveryTag}`, 406, true, false);
  }
  get _emit() {
    return true;
  }
}

function Message(smqpMessage, deliveryTag) {
  this[kSmqp] = smqpMessage;
  this.fields = { ...smqpMessage.fields, deliveryTag };
  this.content = Buffer.from(smqpMessage.content);
  this.properties = { ...smqpMessage.properties };
}

export class FakeAmqplibChannel extends EventEmitter {
  constructor(broker, connection) {
    super();
    this.connection = connection;

    this[kPrefetch] = 10000;
    this[kChannelPrefetch] = Infinity;
    this[kClosed] = false;
    const channelName = this._channelName = `channel-${generateId()}`;
    this._version = connection._version;
    this._broker = broker;

    this._channelQueue = broker.assertQueue(`#${channelName}`);
    this._emitReturn = this._emitReturn.bind(this);

    broker.on('return', this._emitReturn);

    this._createChannelMessage = this._createChannelMessage.bind(this);
    this._calculateChannelCapacity = this._calculateChannelCapacity.bind(this);
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
    const createMessage = this._createChannelMessage;
    return this._callBroker(getMessage, ...args);

    function getMessage(...getargs) {
      const q = this.getQueue(queue);
      if (!q) throw new FakeAmqpNotFoundError('queue', queue, connPath);
      const msg = q.get(...getargs) || false;
      if (!msg) return msg;

      return createMessage(msg, args[0]?.noAck);
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
      this.emit('error', err);
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
      this.emit('error', err);
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
    const createMessage = this._createChannelMessage;
    const calculateCapacity = this._calculateChannelCapacity;
    const channelName = this._channelName;
    const prefetch = this[kPrefetch];

    return this._callBroker(consume, callback);

    function consume() {
      const q = queue && this.getQueue(queue);
      if (!q) {
        throw new FakeAmqpNotFoundError('queue', queue, connUrl.pathname);
      }

      if (q.exclusive || (q.options.exclusive && q.options._connectionId !== connId)) {
        throw new FakeAmqpError(`Channel closed by server: 403 (ACCESS-REFUSED) with message "ACCESS_REFUSED - queue '${queue}' in vhost '${connUrl.pathname}' in exclusive use"`, 403, true, true);
      }

      const consumer = this.consume(queue, onMessage && handler, {
        ...options,
        channelName,
        prefetch: calculateCapacity(prefetch),
        _consumerPrefetch: prefetch,
      });

      const capacityProp = Object.getOwnPropertyDescriptor(Object.getPrototypeOf(consumer), 'capacity');
      Object.defineProperty(consumer, 'capacity', {
        get() {
          return calculateCapacity(capacityProp.get.call(this));
        },
      });

      return { consumerTag: consumer.consumerTag };
    }

    function handler(_, msg) {
      onMessage(createMessage(msg, options.noAck));
    }
  }
  cancel(consumerTag, ...args) {
    return this._callBroker(this._broker.cancel, consumerTag, ...args);
  }
  close(callback) {
    if (this[kClosed]) return;
    this._teardown();
    this.emit('close');
    return resolveOrCallback(callback);
  }
  ack(message, allUpTo) {
    const deliveryTag = message.fields.deliveryTag;
    const channelMessage = this._broker._getMessageByDeliveryTag(this._channelQueue.name, deliveryTag);
    const channelQ = this._channelQueue;

    if (!allUpTo) this._callBroker(ackMessage);
    else this._callBroker(ackAllUpToMessage);

    function ackMessage() {
      const msg = message[kSmqp];
      if (!channelMessage || !msg.pending) {
        throw new FakeAmqpUnknownDeliveryTag(message.fields.deliveryTag);
      }

      channelQ.ack(channelMessage, false);
      this.ack(msg, false);
    }

    function ackAllUpToMessage() {
      const msg = message[kSmqp];
      if (!channelMessage || !msg.pending) {
        throw new FakeAmqpUnknownDeliveryTag(message.fields.deliveryTag);
      }

      const brokerMessages = allUpToDeliveryTag(channelQ, deliveryTag, 'ack', false);
      for (const brokerMessage of brokerMessages) {
        brokerMessage.ack(false);
      }

      channelQ.ack(channelMessage, false);
      this.ack(msg, false);
    }
  }
  ackAll() {
    const channelQ = this._channelQueue;
    let msg;
    const brokerMessages = [];
    while ((msg = channelQ.get())) {
      brokerMessages.push(msg.content[kSmqp]);
      msg.ack();
    }

    for (const brokerMessage of brokerMessages) {
      brokerMessage.ack();
    }
  }
  reject(message, requeue = false) {
    const deliveryTag = message.fields.deliveryTag;
    const channelMessage = this._broker._getMessageByDeliveryTag(this._channelQueue.name, deliveryTag);
    const channelQ = this._channelQueue;

    this._callBroker(rejectMessage);

    function rejectMessage() {
      const msg = message[kSmqp];
      if (!channelMessage || !msg.pending) {
        throw new FakeAmqpUnknownDeliveryTag(deliveryTag);
      }

      channelQ.reject(channelMessage, false);
      this.reject(msg, requeue);
    }
  }
  nack(message, allUpTo = false, requeue = false) {
    if (this.connection._version < 2.3) throw new Error(`Nack is not implemented in versions before 2.3 (${this.connection._version})`);

    const deliveryTag = message.fields.deliveryTag;
    const channelMessage = this._broker._getMessageByDeliveryTag(this._channelQueue.name, deliveryTag);
    const channelQ = this._channelQueue;

    if (!allUpTo) this._callBroker(nackMessage);
    else this._callBroker(nackAllUpToMessage);

    function nackMessage() {
      const msg = message[kSmqp];
      if (!channelMessage || !msg.pending) {
        throw new FakeAmqpUnknownDeliveryTag(deliveryTag);
      }

      channelQ.nack(channelMessage, false, false);
      this.nack(msg, false, requeue);
    }

    function nackAllUpToMessage() {
      const msg = message[kSmqp];
      if (!channelMessage || !msg.pending) {
        throw new FakeAmqpUnknownDeliveryTag(deliveryTag);
      }

      const brokerMessages = allUpToDeliveryTag(channelQ, deliveryTag, 'nack', false, false);
      for (const brokerMessage of brokerMessages) {
        brokerMessage.nack(false, requeue);
      }

      channelMessage.nack(false, false);
      this.nack(msg, false, requeue);
    }
  }
  nackAll(requeue = true) {
    const channelQ = this._channelQueue;
    let msg;
    const brokerMessages = [];
    while ((msg = channelQ.get())) {
      brokerMessages.push(msg.content[kSmqp]);
      msg.reject(false);
    }

    for (const brokerMessage of brokerMessages) {
      brokerMessage.reject(requeue);
    }
  }
  prefetch(val, isChannelPrefetch) {
    if (this.connection._version < 3.3) {
      if (isChannelPrefetch !== undefined) {
        return this.connection.close();
      }
      this[kChannelPrefetch] = val;
    } else {
      if (isChannelPrefetch) {
        this[kChannelPrefetch] = val;
      } else {
        this[kPrefetch] = val;
      }
    }
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
        else if (err._killChannel) this._teardown();
        if (err._emit) this.emit('error', err);
        if (!poppedCb) return reject(err);
        poppedCb(err);
        return resolve();
      }
    });
  }
  _emitReturn({ fields, content, properties }) {
    process.nextTick(() => {
      this.emit('return', { fields, content, properties });
    });
  }
  _createChannelMessage(smqpMessage, noAck) {
    const deliveryTag = this._broker._getNextDeliveryTag();
    const consumeMessage = new Message(smqpMessage, deliveryTag);
    if (!noAck) {
      const channelQ = this._channelQueue;
      channelQ.queueMessage(consumeMessage.fields, consumeMessage);
    }
    return consumeMessage;
  }
  _teardown() {
    this[kClosed] = true;
    const channelName = this._channelName;
    const broker = this._broker;
    const channelConsumers = broker._getChannelConsumers(channelName);
    channelConsumers.forEach((c) => broker.cancel(c.consumerTag));

    let msg;
    while ((msg = this._channelQueue.get())) {
      msg.content[kSmqp].reject(true);
      msg.reject(false);
    }

    broker.off('return', this._emitReturn);
  }
  _calculateChannelCapacity(consumerCapacity) {
    const channelPrefetch = this[kChannelPrefetch];
    if (channelPrefetch === Infinity) return consumerCapacity;

    const channelCapacity = channelPrefetch - this._channelQueue.messageCount;

    let capacity = consumerCapacity;
    if (channelCapacity <= 0) capacity = 0;
    else if (channelCapacity < capacity) capacity = channelCapacity;
    return capacity;
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
      this.emit('error', err);
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
      this.emit('error', err);
    });

    return true;
  }
}

export class FakeAmqplibConnection extends EventEmitter {
  constructor(broker, version, amqpUrl) {
    super();
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

    this.emit('close');

    return resolveOrCallback(args.slice(-1)[0]);
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
  const broker = _broker || new AmqplibBroker(this);
  const connection = new FakeAmqplibConnection(broker, this.version, amqpUrl, ...args);

  const connections = this.connections;

  connections.push(connection);

  connection.once('close', () => {
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

function allUpToDeliveryTag(q, deliveryTag, op, ...args) {
  const brokerMessages = [];

  const consumer = q.consume((_, cmsg) => {
    const msgDeliveryTag = cmsg.fields.deliveryTag;
    if (msgDeliveryTag >= deliveryTag) {
      return q.cancel(cmsg.fields.consumerTag);
    }
    brokerMessages.push(cmsg.content[kSmqp]);
    cmsg[op](...args);
  }, { prefetch: Infinity });

  consumer.cancel();

  return brokerMessages;
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
