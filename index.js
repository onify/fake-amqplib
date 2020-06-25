'use strict';

const {Broker} = require('smqp');
const {EventEmitter} = require('events');

const connections = [];

module.exports = Fake();

function Fake() {
  return {
    connections,
    resetMock,
    connect,
  };

  function connect(amqpUrl, ...args) {
    const {_broker} = connections.find((conn) => conn.options[0] === amqpUrl) || {};
    const broker = _broker || Broker();
    const connection = Connection(broker, amqpUrl, ...args);
    connections.push(connection);
    return resolveOrCallback(args.slice(-1)[0], null, connection);
  }

  function resetMock() {
    for (const connection of connections.splice(0)) {
      connection._broker.reset();
    }
  }

  function Connection(broker, ...connArgs) {
    const emitter = new EventEmitter();
    const options = connArgs.filter((a) => typeof a !== 'function');
    const channels = [];

    return {
      _broker: broker,
      get _emitter() {
        return emitter;
      },
      options,
      createChannel(...args) {
        const channel = Channel(broker);
        channels.push(channel);
        return resolveOrCallback(args.slice(-1)[0], null, channel);
      },
      createConfirmChannel(...args) {
        const channel = Channel(broker, true);
        channels.push(channel);
        return resolveOrCallback(args.slice(-1)[0], null, channel);
      },
      async close(...args) {
        const idx = connections.indexOf(this);
        if (idx > -1) connections.splice(idx, 1);
        channels.splice(0).forEach((channel) => channel.close());

        // broker.reset();
        return resolveOrCallback(args.slice(-1)[0]);
      },
      on(...args) {
        return emitter.on(...args);
      },
      once(...args) {
        return emitter.once(...args);
      },
    };
  }

  function Channel(broker, confirm) {
    let closed = false;
    const emitter = new EventEmitter();
    const channelName = 'channel-' + generateId();
    return {
      _broker: broker,
      get _emitter() {
        return emitter;
      },
      assertExchange(...args) {
        return callBroker(broker.assertExchange, ...args);
      },
      assertQueue(...args) {
        return callBroker(broker.assertQueue, ...args);
      },
      bindExchange(...args) {
        return callBroker(broker.bindExchange, ...args);
      },
      bindQueue(...args) {
        return callBroker(broker.bindQueue, ...args);
      },
      checkExchange(name, ...args) {
        return callBroker(check, ...args);

        function check() {
          if (!broker.getExchange(name)) throw new Error(`exchange with name ${name} not found`);
          return true;
        }
      },
      checkQueue(name, ...args) {
        return callBroker(check, ...args);

        function check() {
          if (!broker.getQueue(name)) throw new Error(`queue with name ${name} not found`);
          return true;
        }
      },
      get(queue, ...args) {
        return callBroker(getMessage, ...args);

        function getMessage(...getargs) {
          const q = broker.getQueue(queue);
          if (!q) throw new Error(`queue with name ${queue} not found`);
          return q.get(...getargs) || false;
        }
      },
      deleteExchange(...args) {
        return callBroker(broker.deleteExchange, ...args);
      },
      deleteQueue(...args) {
        return callBroker(broker.deleteQueue, ...args);
      },
      publish(exchange, routingKey, content, ...args) {
        if (!Buffer.isBuffer(content)) throw new TypeError('content is not a buffer');
        return confirm ? callBroker(broker.publish, exchange, routingKey, content, ...args) : broker.publish(exchange, routingKey, content, ...args);
      },
      purgeQueue(...args) {
        return callBroker(broker.purgeQueue, ...args);
      },
      sendToQueue(queue, content, ...args) {
        if (!Buffer.isBuffer(content)) throw new TypeError('content is not a buffer');
        return confirm ? callBroker(broker.sendToQueue, queue, content, ...args) : broker.sendToQueue(queue, content, ...args);
      },
      unbindExchange(...args) {
        return callBroker(broker.unbindExchange, ...args);
      },
      unbindQueue(...args) {
        return callBroker(broker.unbindQueue, ...args);
      },
      consume(queue, onMessage, options = {}, callback) {
        return callBroker(broker.consume, queue, onMessage && handler, {...options, channelName}, callback);
        function handler(_, msg) {
          onMessage(msg);
        }
      },
      cancel(consumerTag, ...args) {
        return callBroker(broker.cancel, consumerTag, ...args);
      },
      close(callback) {
        if (closed) return;
        const channelConsumers = broker.getConsumers().filter((f) => f.options.channelName === channelName);
        channelConsumers.forEach((c) => broker.cancel(c.consumerTag));
        closed = true;
        emitter.emit('close');
        return resolveOrCallback(callback);
      },
      ack: broker.ack,
      ackAll: broker.ackAll,
      nack: broker.nack,
      reject: broker.reject,
      nackAll: broker.nackAll,
      prefetch() {},
      on(...args) {
        return emitter.on(...args);
      },
      once(...args) {
        return emitter.once(...args);
      },
    };

    function callBroker(fn, ...args) {
      let [poppedCb] = args.slice(-1);
      if (typeof poppedCb === 'function') args.splice(-1);
      else poppedCb = null;

      if (closed) throw new Error('Channel is closed');

      return new Promise((resolve, reject) => {
        try {
          const result = fn(...args);
          if (poppedCb) poppedCb(null, result);
          return resolve(result);
        } catch (err) {
          if (!poppedCb) return reject(err);
          poppedCb(err);
          return resolve();
        }
      });
    }
  }
}

function resolveOrCallback(optionalCb, err, ...args) {
  if (typeof optionalCb === 'function') optionalCb(err, ...args);
  return Promise.resolve(...args);
}

function generateId() {
  const min = 110000;
  const max = 9999999;
  const rand = Math.floor(Math.random() * (max - min)) + min;

  return rand.toString(16);
}
