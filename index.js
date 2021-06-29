'use strict';

const {Broker} = require('smqp');
const {EventEmitter} = require('events');

class FakeAmqpError extends Error {
  constructor(message, code, killChannel, killConnection) {
    super(message);
    this.code = code;
    this._killChannel = killChannel;
    this._killConnection  = killConnection;
  }
}

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
    let closed = false;
    const channels = [];

    return {
      _id: generateId(),
      _broker: broker,
      get _closed() {
        return closed;
      },
      get _emitter() {
        return emitter;
      },
      options,
      createChannel(...args) {
        if (closed) return resolveOrCallback(args.slice(-1)[0], unavailable());

        const channel = Channel(broker, this);
        channels.push(channel);
        return resolveOrCallback(args.slice(-1)[0], null, channel);
      },
      createConfirmChannel(...args) {
        if (closed) return resolveOrCallback(args.slice(-1)[0], unavailable());

        const channel = Channel(broker, this, true);
        channels.push(channel);
        return resolveOrCallback(args.slice(-1)[0], null, channel);
      },
      async close(...args) {
        closed = true;

        const idx = connections.indexOf(this);
        if (idx > -1) connections.splice(idx, 1);
        channels.splice(0).forEach((channel) => channel.close());

        return resolveOrCallback(args.slice(-1)[0]);
      },
      on(...args) {
        return emitter.on(...args);
      },
      once(...args) {
        return emitter.once(...args);
      },
    };

    function unavailable() {
      return new FakeAmqpError('Connection closed: 504', 504);
    }
  }

  function Channel(broker, connection, confirmChannel) {
    let closed = false, prefetch = 10000;
    const emitter = new EventEmitter();
    const channelName = 'channel-' + generateId();

    broker.on('return', emitReturn);

    return {
      _broker: broker,
      get _emitter() {
        return emitter;
      },
      assertExchange(...args) {
        return callBroker(assertExchange, ...args);

        function assertExchange(exchange, ...args) {
          broker.assertExchange(exchange, ...args);
          return {
            exchange,
          };
        }
      },
      assertQueue(...args) {
        return callBroker(assertQueue, ...args);

        function assertQueue(queueName, ...args) {
          const name = queueName ? queueName : 'amqp.gen-' + generateId();
          const options = typeof args[0] === 'object' ? args.shift() : {};
          const queue = broker.assertQueue(queueName, {...options, _connectionId: connection._id}, ...args);
          return {
            ...(!queueName ? {queue: name} : undefined),
            messageCount: queue.messageCount,
            consumerCount: queue.consumerCount,
          };
        }
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
          if (!broker.getExchange(name)) throw new FakeAmqpError(`exchange with name ${name} not found`, 404);
          return true;
        }
      },
      checkQueue(name, ...args) {
        return callBroker(check, ...args);

        function check() {
          let queue;
          if (!(queue = broker.getQueue(name))) {
            throw new FakeAmqpError(`Channel closed by server: 404 (NOT-FOUND) with message "NOT_FOUND - no queue '${name}' in vhost '/'`, 404, true);
          }

          return {
            messageCount: queue.messageCount,
            consumerCount: queue.consumerCount,
          };
        }
      },
      get(queue, ...args) {
        return callBroker(getMessage, ...args);

        function getMessage(...getargs) {
          const q = broker.getQueue(queue);
          if (!q) throw new FakeAmqpError(`queue with name ${queue} not found`, 404);
          return q.get(...getargs) || false;
        }
      },
      deleteExchange(...args) {
        return callBroker(broker.deleteExchange, ...args);
      },
      deleteQueue(...args) {
        return callBroker(broker.deleteQueue, ...args);
      },
      publish(exchange, routingKey, content, options, callback) {
        if (!Buffer.isBuffer(content)) throw new TypeError('content is not a buffer');
        return confirmChannel ? publish() : callBroker(broker.publish, exchange, routingKey, content, options);

        function publish() {
          const confirm = makeConfirmCallback(callback);
          broker.publish(exchange, routingKey, content, {...options, confirm});
        }
      },
      purgeQueue(...args) {
        return callBroker(broker.purgeQueue, ...args);
      },
      sendToQueue(queue, content, options, callback) {
        if (!Buffer.isBuffer(content)) throw new TypeError('content is not a buffer');
        return confirmChannel ? sendToQueue() : callBroker(broker.sendToQueue, queue, content, options);

        function sendToQueue() {
          const confirm = makeConfirmCallback(callback);
          broker.sendToQueue(queue, content, {...options, confirm});
        }
      },
      unbindExchange(...args) {
        return callBroker(broker.unbindExchange, ...args);
      },
      unbindQueue(...args) {
        return callBroker(broker.unbindQueue, ...args);
      },
      consume(queue, onMessage, options = {}, callback) {
        return callBroker(check, queue, onMessage, options, callback);

        function check() {
          const q = queue && broker.getQueue(queue);
          if (queue && !q) {
            throw new FakeAmqpError(`Channel closed by server: 404 (NOT-FOUND) with message "NOT_FOUND - no queue '${queue}' in vhost '/'`, 404, true);
          }

          if (q) {
            if (q.exclusive || (q.options.exclusive && q.options._connectionId !== connection._id)) {
              throw new FakeAmqpError(`Channel closed by server: 403 (ACCESS-REFUSED) with message "ACCESS_REFUSED - queue '${queue}' in vhost '/' in exclusive use"`, 403, true, true);
            }
          }

          return broker.consume(queue, onMessage && handler, {...options, channelName, prefetch});
        }

        function handler(_, msg) {
          onMessage(msg);
        }
      },
      cancel(consumerTag, ...args) {
        return callBroker(broker.cancel, consumerTag, ...args);
      },
      close(callback) {
        if (closed) return;
        broker.off('return', emitReturn);
        const channelConsumers = broker.getConsumers().filter((f) => f.options.channelName === channelName);
        channelConsumers.forEach((c) => broker.cancel(c.consumerTag));
        closed = true;
        emitter.emit('close');
        return resolveOrCallback(callback);
      },
      ack: broker.ack,
      ackAll: broker.ackAll,
      nack(message, ...args) {
        return broker.nack(message, ...args);
      },
      reject: broker.reject,
      nackAll: broker.nackAll,
      prefetch(val) {
        prefetch = val;
      },
      on(...args) {
        return emitter.on(...args);
      },
      once(...args) {
        return emitter.once(...args);
      },
    };

    function makeConfirmCallback(callback) {
      const confirm = 'msg.' + generateId();
      const consumerTag = 'ct-' + confirm;
      broker.on('message.*', onConsumeMessage, {consumerTag});

      function onConsumeMessage(event) {
        if (event.properties.confirm !== confirm) return;

        switch(event.name) {
          case 'message.nack':
            return confirmCallback(new Error('message nacked'));
          case 'message.undelivered':
            return confirmCallback(new Error('message undelivered'));
          case 'message.ack':
            return confirmCallback(null, true);
        }

        function confirmCallback(err, ok) {
          broker.off('message.*', {consumerTag});
          callback(err, ok);
        }
      }

      return confirm;
    }

    function callBroker(fn, ...args) {
      let [poppedCb] = args.slice(-1);
      if (typeof poppedCb === 'function') args.splice(-1);
      else poppedCb = null;

      if (connection._closed) throw new FakeAmqpError('Connection is closed', 504);
      if (closed) throw new Error('Channel is closed');

      return new Promise((resolve, reject) => {
        try {
          const result = fn.call(broker, ...args);
          if (poppedCb) poppedCb(null, result);
          return resolve(result);
        } catch (err) {
          if (err._killConnection) connection.close();
          else if (err._killChannel) closed = true;

          if (!poppedCb) return reject(err);
          poppedCb(err);
          return resolve();
        }
      });
    }

    function emitReturn({fields, content, properties}) {
      process.nextTick(() => {
        emitter.emit('return', {fields, content, properties});
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
