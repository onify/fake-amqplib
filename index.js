import { Broker } from 'smqp';
import { EventEmitter } from 'events';
import { format as urlFormat } from 'url';

const kSmqp = Symbol.for('smqp');
const kClosed = Symbol.for('closed');
const kDeliveryTag = Symbol.for('channel delivery tag');
const kPrefetch = Symbol.for('prefetch');
const kChannelPrefetch = Symbol.for('channel prefetch');
const kPendingConfirms = Symbol.for('pending confirms');

const CHANNEL_CLOSED_ERROR = 'Channel is closed';

// AMQP [classId, methodId] of the method that caused a server-side channel close, exposed like amqplib does
const AMQP_METHOD = {
  ExchangeDeclare: [40, 10],
  ExchangeDelete: [40, 20],
  ExchangeUnbind: [40, 40],
  QueueDeclare: [50, 10],
  QueuePurge: [50, 30],
  QueueDelete: [50, 40],
  QueueUnbind: [50, 50],
  BasicConsume: [60, 20],
  BasicGet: [60, 70],
  BasicAck: [60, 80],
  BasicReject: [60, 90],
  BasicNack: [60, 120],
};

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
  constructor(message, code, killChannel, killConnection, method) {
    super(message);
    this.code = code;
    if (method) [this.classId, this.methodId] = method;
    this._killChannel = killChannel;
    this._killConnection = killConnection;
  }
}

class FakeAmqpNotFoundError extends FakeAmqpError {
  constructor(type, name, vhost, method, killConnection = false) {
    super(
      `Channel closed by server: 404 (NOT-FOUND) with message "NOT_FOUND - no ${type} '${name}' in vhost '${vhost}'`,
      404,
      true,
      killConnection,
      method
    );
  }
}

class FakeAmqpUnknownDeliveryTag extends FakeAmqpError {
  constructor(deliveryTag, method) {
    super(
      `Channel closed by server: 406 (PRECONDITION-FAILED) with message "PRECONDITION_FAILED - unknown delivery tag ${deliveryTag}`,
      406,
      true,
      false,
      method
    );
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
    const channelName = (this._channelName = `channel-${generateId()}`);
    this._version = connection._version;
    this._broker = broker;

    this._channelQueue = broker.assertQueue(`#${channelName}`);
    this._emitReturn = this._emitReturn.bind(this);

    broker.on('return', this._emitReturn);

    this._createChannelMessage = this._createChannelMessage.bind(this);
    this._channelCredit = this._channelCredit.bind(this);
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
      const queue = this.assertQueue(name, { autoDelete: false, ...options, _connectionId: connection._id }, ...assertArgs);
      return { queue: name, ...queueStats(queue) };
    }
  }
  bindExchange(destination, source, ...args) {
    const broker = this._broker;
    return Promise.all([this.checkExchange(source), this.checkExchange(destination)]).then(() => {
      return this._callBroker(broker.bindExchange, source, destination, ...args);
    });
  }
  bindQueue(queue, source, ...args) {
    return Promise.all([this.checkQueue(queue), this.checkExchange(source)]).then(() => {
      return this._callBroker(this._broker.bindQueue, queue, source, ...args);
    });
  }
  checkExchange(name, ...args) {
    const connPath = this.connection._url.pathname;
    return this._callBroker(check, ...args);

    function check() {
      if (!this.getExchange(name)) throw new FakeAmqpNotFoundError('exchange', name, connPath, AMQP_METHOD.ExchangeDeclare);
      return true;
    }
  }
  checkQueue(name, ...args) {
    const connPath = this.connection._url.pathname;
    return this._callBroker(check, ...args);

    function check() {
      let queue;
      if (!(queue = this.getQueue(name))) {
        throw new FakeAmqpNotFoundError('queue', name, connPath, AMQP_METHOD.QueueDeclare);
      }

      return queueStats(queue);
    }
  }
  get(queue, ...args) {
    const connPath = this.connection._url.pathname;
    const createMessage = this._createChannelMessage;
    return this._callBroker(getMessage, ...args);

    function getMessage(...getargs) {
      const q = this.getQueue(queue);
      if (!q) throw new FakeAmqpNotFoundError('queue', queue, connPath, AMQP_METHOD.BasicGet);
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
      if (!result && this.owner.version < 3.2) throw new FakeAmqpNotFoundError('exchange', exchange, connPath, AMQP_METHOD.ExchangeDelete);
      return result;
    }
  }
  deleteQueue(queue, ...args) {
    const connPath = this.connection._url.pathname;
    return this._callBroker(check, ...args);

    function check() {
      const result = this.deleteQueue(queue, ...args);
      if (!result && this.owner.version < 3.2) throw new FakeAmqpNotFoundError('queue', queue, connPath, AMQP_METHOD.QueueDelete);
      return result;
    }
  }
  publish(exchange, routingKey, content, options, callback) {
    if (!Buffer.isBuffer(content)) throw new TypeError('content is not a buffer');
    if (exchange === '') return this.sendToQueue(routingKey, content, options, callback);
    this._assertOpen();

    const args = [this._broker.publish, exchange, routingKey, content];

    args.push(options, callback);

    this.checkExchange(exchange)
      .then(() => {
        return this._callBroker(...args);
      })
      .catch((err) => this._emitUnobserved(err));

    return true;
  }
  purgeQueue(queue, ...args) {
    const connPath = this.connection._url.pathname;
    return this._callBroker(check, ...args);

    function check() {
      const result = this.purgeQueue(queue);
      if (!result && this.owner.version < 3.2) throw new FakeAmqpNotFoundError('queue', queue, connPath, AMQP_METHOD.QueuePurge);
      return result === undefined ? undefined : { messageCount: result };
    }
  }
  sendToQueue(queue, content, options, callback) {
    if (!Buffer.isBuffer(content)) throw new TypeError('content is not a buffer');
    this._assertOpen();

    const args = [this._broker.sendToQueue, queue, content];

    args.push(options, callback);

    this.checkQueue(queue)
      .then(() => {
        return this._callBroker(...args);
      })
      .catch((err) => this._emitUnobserved(err));

    return true;
  }
  unbindExchange(destination, source, pattern, ...args) {
    const connPath = this.connection._url.pathname;
    return this._callBroker(check, ...args);

    function check() {
      const q = this.getExchange(destination);
      if (!q) throw new FakeAmqpNotFoundError('exchange', destination, connPath, AMQP_METHOD.ExchangeUnbind);

      const exchange = this.getExchange(source);
      if (!exchange) throw new FakeAmqpNotFoundError('exchange', source, connPath, AMQP_METHOD.ExchangeUnbind);

      const result = this.unbindExchange(source, destination, pattern);
      if (!result && this.owner.version <= 3.2) {
        throw new FakeAmqpNotFoundError('binding', pattern, connPath, AMQP_METHOD.ExchangeUnbind);
      }

      return true;
    }
  }
  unbindQueue(queue, source, pattern, ...args) {
    const connPath = this.connection._url.pathname;
    return this._callBroker(check, ...args);

    function check() {
      const q = this.getQueue(queue);
      if (!q) throw new FakeAmqpNotFoundError('queue', queue, connPath, AMQP_METHOD.QueueUnbind);

      const exchange = this.getExchange(source);
      if (!exchange) throw new FakeAmqpNotFoundError('exchange', source, connPath, AMQP_METHOD.QueueUnbind);

      const binding = exchange.getBinding(queue, pattern);
      if (!binding && this.owner.version <= 3.2) {
        throw new FakeAmqpNotFoundError('binding', pattern, connPath, AMQP_METHOD.QueueUnbind, this.owner.version < 3.2);
      }

      this.unbindQueue(queue, source, pattern);
      return true;
    }
  }
  consume(queue, onMessage, options = {}, callback) {
    const { _id: connId, _url: connUrl } = this.connection;
    const createMessage = this._createChannelMessage;
    const capacity = this._channelCredit;
    const channelName = this._channelName;
    const prefetch = this[kPrefetch];

    return this._callBroker(consume, callback);

    function consume() {
      const q = queue && this.getQueue(queue);
      if (!q) {
        throw new FakeAmqpNotFoundError('queue', queue, connUrl.pathname, AMQP_METHOD.BasicConsume);
      }

      if (q.exclusive || (q.options.exclusive && q.options._connectionId !== connId)) {
        throw new FakeAmqpError(
          `Channel closed by server: 403 (ACCESS-REFUSED) with message "ACCESS_REFUSED - queue '${queue}' in vhost '${connUrl.pathname}' in exclusive use"`,
          403,
          true,
          true,
          AMQP_METHOD.BasicConsume
        );
      }

      const consumer = this.consume(queue, onMessage && handler, { ...options, channelName, prefetch, capacity });
      return { consumerTag: consumer.consumerTag };
    }

    function handler(_, msg) {
      onMessage(createMessage(msg, options.noAck));
    }
  }
  cancel(consumerTag, ...args) {
    return this._callBroker(cancel, ...args);

    function cancel() {
      return this.cancel(consumerTag, { keepPending: true });
    }
  }
  close(callback) {
    if (this[kClosed]) return;
    this._teardown();
    this.emit('close');
    return resolveOrCallback(callback);
  }
  ack(message, allUpTo) {
    this._assertOpen();
    const deliveryTag = message.fields.deliveryTag;
    const channelMessage = this._broker._getMessageByDeliveryTag(this._channelQueue.name, deliveryTag);
    const channelQ = this._channelQueue;

    if (!allUpTo) this._callBroker(ackMessage);
    else this._callBroker(ackAllUpToMessage);
    this._consumeNext();

    function ackMessage() {
      const msg = message[kSmqp];
      if (!channelMessage || !msg.pending) {
        throw new FakeAmqpUnknownDeliveryTag(message.fields.deliveryTag, AMQP_METHOD.BasicAck);
      }

      channelQ.ack(channelMessage, false);
      this.ack(msg, false);
    }

    function ackAllUpToMessage() {
      const msg = message[kSmqp];
      if (!channelMessage || !msg.pending) {
        throw new FakeAmqpUnknownDeliveryTag(message.fields.deliveryTag, AMQP_METHOD.BasicAck);
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
    this._assertOpen();
    const channelQ = this._channelQueue;
    let msg;
    const brokerMessages = [];
    while ((msg = channelQ.get())) {
      brokerMessages.push(msg.content[kSmqp]);
      msg.ack();
    }

    for (const brokerMessage of brokerMessages) {
      brokerMessage.ack(false);
    }
    this._consumeNext();
  }
  reject(message, requeue = false) {
    this._assertOpen();
    const deliveryTag = message.fields.deliveryTag;
    const channelMessage = this._broker._getMessageByDeliveryTag(this._channelQueue.name, deliveryTag);
    const channelQ = this._channelQueue;

    this._callBroker(rejectMessage);
    this._consumeNext();

    function rejectMessage() {
      const msg = message[kSmqp];
      if (!channelMessage || !msg.pending) {
        throw new FakeAmqpUnknownDeliveryTag(deliveryTag, AMQP_METHOD.BasicReject);
      }

      channelQ.reject(channelMessage, false);
      this.reject(msg, requeue);
    }
  }
  nack(message, allUpTo = false, requeue = false) {
    this._assertOpen();
    if (this.connection._version < 2.3) throw new Error(`Nack is not implemented in versions before 2.3 (${this.connection._version})`);

    const deliveryTag = message.fields.deliveryTag;
    const channelMessage = this._broker._getMessageByDeliveryTag(this._channelQueue.name, deliveryTag);
    const channelQ = this._channelQueue;

    if (!allUpTo) this._callBroker(nackMessage);
    else this._callBroker(nackAllUpToMessage);
    this._consumeNext();

    function nackMessage() {
      const msg = message[kSmqp];
      if (!channelMessage || !msg.pending) {
        throw new FakeAmqpUnknownDeliveryTag(deliveryTag, AMQP_METHOD.BasicNack);
      }

      channelQ.nack(channelMessage, false, false);
      this.nack(msg, false, requeue);
    }

    function nackAllUpToMessage() {
      const msg = message[kSmqp];
      if (!channelMessage || !msg.pending) {
        throw new FakeAmqpUnknownDeliveryTag(deliveryTag, AMQP_METHOD.BasicNack);
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
    this._assertOpen();
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
    this._consumeNext();
  }
  recover(...args) {
    const channelQ = this._channelQueue;
    return this._callBroker(recoverChannel, ...args);

    function recoverChannel() {
      const snapshot = [];
      let msg;
      while ((msg = channelQ.get())) snapshot.push(msg);
      for (const m of snapshot) {
        m.content[kSmqp].reject(true);
        m.reject(false);
      }
      return {};
    }
  }
  _consumeNext() {
    if (this[kChannelPrefetch] === Infinity) return;
    const broker = this._broker;
    for (const consumer of broker._getChannelConsumers(this._channelName)) {
      broker.getQueue(consumer.queue).consumeNext();
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
    let [poppedCb] = args.slice(-1);
    if (typeof poppedCb === 'function') args.splice(-1);
    else poppedCb = null;

    // like amqplib, the callback API throws synchronously on a closed channel while the promise API rejects
    if (poppedCb) this._assertOpen();

    return new Promise((resolve, reject) => {
      try {
        this._assertOpen();
        const result = fn.call(this._broker, ...args);
        if (poppedCb) poppedCb(null, result);
        return resolve(result);
      } catch (err) {
        if (err._killConnection) this.connection.close();
        else if (err._killChannel) this._kill(err);
        if (!poppedCb) return reject(err);
        poppedCb(err);
        return resolve();
      }
    });
  }
  _assertOpen() {
    if (this.connection._closed) throw new FakeAmqpError('Connection is closed', 504);
    if (this[kClosed]) throw new Error(CHANNEL_CLOSED_ERROR);
  }
  _kill(err) {
    this._teardown();
    if (err._emit || this.listenerCount('error')) this.emit('error', err);
    this.emit('close');
  }
  _emitUnobserved(err) {
    // _kill() already delivered server-close errors to a listener; without one, re-emit so the failure surfaces loudly
    if (!err._killChannel || !this.listenerCount('error')) this.emit('error', err);
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
  _channelCredit() {
    return this[kChannelPrefetch] - this._channelQueue.messageCount;
  }
}

export class FakeAmqplibConfirmChannel extends FakeAmqplibChannel {
  constructor(broker, connection) {
    super(broker, connection);
    this[kPendingConfirms] = new Set();
  }
  publish(exchange, routingKey, content, options, callback) {
    if (!Buffer.isBuffer(content)) throw new TypeError('content is not a buffer');
    if (exchange === '') return this.sendToQueue(routingKey, content, options, callback);
    this._assertOpen();

    const args = [this._broker.publish, exchange, routingKey, content];

    args.push(...addConfirmCallback(this._broker, options, this._trackConfirm(callback)));

    this.checkExchange(exchange)
      .then(() => {
        return this._callBroker(...args);
      })
      .catch((err) => {
        if (err.message === CHANNEL_CLOSED_ERROR) return;
        this._emitUnobserved(err);
      });

    return true;
  }
  sendToQueue(queue, content, options, callback) {
    if (!Buffer.isBuffer(content)) throw new TypeError('content is not a buffer');
    this._assertOpen();

    const args = [this._broker.sendToQueue, queue, content];

    args.push(...addConfirmCallback(this._broker, options, this._trackConfirm(callback)));

    this.checkQueue(queue)
      .then(() => {
        return this._callBroker(...args);
      })
      .catch((err) => {
        if (err.message === CHANNEL_CLOSED_ERROR) return;
        this._emitUnobserved(err);
      });

    return true;
  }
  _trackConfirm(userCallback) {
    let resolveSettled;
    const settled = new Promise((resolve) => {
      resolveSettled = resolve;
    });
    const entry = { settled, resolveSettled, userCallback };
    this[kPendingConfirms].add(entry);

    return (err, ok) => {
      this[kPendingConfirms].delete(entry);
      resolveSettled(err || null);
      if (typeof userCallback === 'function') userCallback(err, ok);
    };
  }
  waitForConfirms(callback) {
    const snapshot = [...this[kPendingConfirms]].map((entry) => entry.settled);
    const promise = Promise.all(snapshot).then((errs) => {
      const firstErr = errs.find((e) => e !== null);
      if (firstErr) throw firstErr;
    });
    if (typeof callback === 'function') {
      promise.then(
        () => callback(null),
        (err) => callback(err)
      );
    }
    return promise;
  }
  _teardown() {
    super._teardown();
    if (!this[kPendingConfirms] || this[kPendingConfirms].size === 0) return;
    const err = new Error(CHANNEL_CLOSED_ERROR);
    for (const entry of [...this[kPendingConfirms]]) {
      this[kPendingConfirms].delete(entry);
      entry.resolveSettled(err);
      if (typeof entry.userCallback === 'function') entry.userCallback(err);
    }
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
  updateSecret(...args) {
    process.nextTick(() => this.emit('update-secret-ok'));
    return resolveOrCallback(args.slice(-1)[0]);
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
  this.connectWithRecoveryPromise = this.connectWithRecoveryPromise.bind(this);
  this.connectWithRecoveryCallback = this.connectWithRecoveryCallback.bind(this);
  this.resetMock = this.resetMock.bind(this);
  this.setVersion = this.setVersion.bind(this);
}

FakeAmqplib.prototype.connect = function fakeConnect(amqpUrl, ...args) {
  const connection = this.connectSync(amqpUrl, ...args);
  return resolveOrCallback(args.slice(-1)[0], null, connection);
};

FakeAmqplib.prototype.connectWithRecoveryPromise = function fakeConnectWithRecoveryPromise(amqpUrl, recoveryOptions, socketOptions) {
  return this.connect(amqpUrl, socketOptions);
};

FakeAmqplib.prototype.connectWithRecoveryCallback = function fakeConnectWithRecoveryCallback(
  amqpUrl,
  recoveryOptions,
  socketOptions,
  callback
) {
  return this.connect(amqpUrl, socketOptions, callback);
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
    const { protocol = 'amqp', hostname = 'localhost', port = 5672, vhost = '/', username, password, ...rest } = url;
    let auth = username;
    if (auth && password) {
      auth += `:${password}`;
    }
    url = new URL(
      urlFormat({
        protocol,
        hostname,
        port,
        pathname: vhost,
        slashes: true,
        auth,
      })
    );

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
    broker.off('message.*', { consumerTag });
    switch (undelivered) {
      case 'message.nack':
        return callback(new Error('message nacked'));
      case 'message.undelivered':
        throw callback(new Error('message undelivered'));
      default:
        return callback(null, true);
    }
  }

  return [options, confirmCallback];
}

/**
 * Queue counts as reported by queue.declare-ok, expired messages are evicted first
 * @param {import('smqp').Queue} queue smqp queue
 * @returns {{ messageCount: number, consumerCount: number }} message- and consumer count
 */
function queueStats(queue) {
  queue.evictExpired();
  const { messageCount, consumerCount } = queue.getStats();
  return { messageCount, consumerCount };
}

function allUpToDeliveryTag(q, deliveryTag, op, ...args) {
  const brokerMessages = [];

  const consumer = q.consume(
    (_, cmsg) => {
      const msgDeliveryTag = cmsg.fields.deliveryTag;
      if (msgDeliveryTag >= deliveryTag) {
        return q.cancel(cmsg.fields.consumerTag);
      }
      brokerMessages.push(cmsg.content[kSmqp]);
      cmsg[op](...args);
    },
    { prefetch: Infinity }
  );

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

export function connectWithRecoveryPromise(amqpUrl, recoveryOptions, socketOptions) {
  return defaultFake.connectWithRecoveryPromise(amqpUrl, recoveryOptions, socketOptions);
}

export function connectWithRecoveryCallback(amqpUrl, recoveryOptions, socketOptions, callback) {
  return defaultFake.connectWithRecoveryCallback(amqpUrl, recoveryOptions, socketOptions, callback);
}

export function resetMock() {
  return defaultFake.resetMock();
}

export function setVersion(minorVersion) {
  return defaultFake.setVersion(minorVersion);
}
