'use strict';

const {connect, resetMock, connections} = require('..');
const {expect} = require('chai');
const {EventEmitter} = require('events');

describe('fake amqplib', () => {
  describe('#connect', () => {
    it('exposes the expected api on connection', (done) => {
      connect('amqp://localhost', null, (err, connection) => {
        if (err) return done(err);
        expect(connection).have.property('createChannel').that.is.a('function');
        expect(connection).have.property('createConfirmChannel').that.is.a('function');
        expect(connection).have.property('close').that.is.a('function');
        expect(connection).have.property('on').that.is.a('function');
        expect(connection).have.property('once').that.is.a('function');
        expect(connection).have.property('_emitter').that.is.instanceof(EventEmitter);
        expect(connection).have.property('_closed');
        done();
      });
    });

    it('connection with the same amqpUrl shares broker', async () => {
      const conn1 = await connect('amqp://testrabbit:5672');
      const conn2 = await connect('amqp://testrabbit:5672');

      expect(conn1._broker === conn2._broker).to.be.true;
    });

    it('connection with different amqpUrls has different brokers', async () => {
      const conn1 = await connect('amqp://testrabbit:5672');
      const conn2 = await connect('amqp://testrabbit:15672');

      expect(conn1._broker === conn2._broker).to.be.false;
    });

    it('exposes connection list', async () => {
      const conn1 = await connect('amqp://localhost:5672');
      const conn2 = await connect('amqp://localhost:15672');
      expect(connections).to.have.length.above(2).and.include.members([conn1, conn2]);
    });

    it('connection.close() removes connection from list', async () => {
      const conn = await connect('amqp://testrabbit:5672');
      expect(connections).to.include(conn);

      conn.close();

      expect(connections).to.not.include(conn);
    });

    it('closed connection cannot create channel', async () => {
      const conn = await connect('amqp://testrabbit:5672');
      expect(connections).to.include(conn);

      conn.close();

      try {
        await conn.createChannel();
      } catch (e) {
        var err = e;
      }
      expect(err).to.have.property('code', 504);
    });

    it('closed connection cannot create confirm channel', async () => {
      const conn = await connect('amqp://testrabbit:5672');
      expect(connections).to.include(conn);

      conn.close();

      try {
        await conn.createConfirmChannel();
      } catch (e) {
        var err = e;
      }
      expect(err).to.have.property('code', 504);
    });

    it('connection.close() removes connection from list but keeps other connections to same url', async () => {
      const conn1 = await connect('amqp://testrabbit:5672');
      const conn2 = await connect('amqp://testrabbit:5672');
      await conn1.close();
      expect(connections).to.not.include(conn1);
      expect(connections).to.include(conn2);
    });

    it('closed connection keeps broker for other connection', async () => {
      const conn1 = await connect('amqp://testrabbit:5672');

      const conn2 = await connect('amqp://testrabbit:5672');
      const channel2 = await conn2.createChannel();
      await channel2.assertExchange('event', 'topic');
      await channel2.assertQueue('event-q');
      await channel2.bindQueue('event-q', 'event', '#');
      let msgs = [];
      await channel2.consume('event-q', (msg) => {
        msgs.push(msg);
      }, {noAck: true});

      const channel1 = await conn1.createChannel();
      await channel1.assertQueue('event1-q');
      await channel1.bindQueue('event1-q', 'event', '#');
      await channel1.consume('event1-q', (msg) => {
        msgs.push(msg);
      }, {noAck: true});

      await conn1.close();

      await channel2.publish('event', 'test.event', Buffer.from('test'));
      expect(msgs).to.have.length(1);
    });
  });

  describe('connections', () => {
    it('emits close when closed', (done) => {
      connect('amqp://conn.test').then((connection) => {
        connection.on('close', () => {
          done();
        });

        connection.close();
      });
    });

    it('exposes #once', (done) => {
      connect('amqp://conn.test').then((connection) => {
        connection.once('close', () => {
          done();
        });

        connection.close();
      });
    });
  });

  describe('channels', () => {
    let connection;
    before((done) => {
      connect('amqp://localhost', null, (err, conn) => {
        if (err) return done(err);
        connection = conn;
        done();
      });
    });

    it('#createChannel exposes the expected api', (done) => {
      connection.createChannel((err, channel) => {
        if (err) return done(err);
        expect(channel).to.have.property('ack').that.is.a('function');
        expect(channel).to.have.property('ackAll').that.is.a('function');
        expect(channel).to.have.property('assertExchange').that.is.a('function');
        expect(channel).to.have.property('assertQueue').that.is.a('function');
        expect(channel).to.have.property('bindExchange').that.is.a('function');
        expect(channel).to.have.property('bindQueue').that.is.a('function');
        expect(channel).to.have.property('cancel').that.is.a('function');
        expect(channel).to.have.property('checkExchange').that.is.a('function');
        expect(channel).to.have.property('checkQueue').that.is.a('function');
        expect(channel).to.have.property('consume').that.is.a('function');
        expect(channel).to.have.property('deleteExchange').that.is.a('function');
        expect(channel).to.have.property('deleteQueue').that.is.a('function');
        expect(channel).to.have.property('get').that.is.a('function');
        expect(channel).to.have.property('nack').that.is.a('function');
        expect(channel).to.have.property('nackAll').that.is.a('function');
        expect(channel).to.have.property('prefetch').that.is.a('function');
        expect(channel).to.have.property('publish').that.is.a('function');
        expect(channel).to.have.property('purgeQueue').that.is.a('function');
        expect(channel).to.have.property('reject').that.is.a('function');
        expect(channel).to.have.property('sendToQueue').that.is.a('function');
        expect(channel).to.have.property('unbindExchange').that.is.a('function');
        expect(channel).to.have.property('unbindQueue').that.is.a('function');
        expect(channel).to.have.property('on').that.is.a('function');
        expect(channel).to.have.property('once').that.is.a('function');
        expect(channel).to.have.property('close').that.is.a('function');
        expect(channel).to.have.property('_emitter').that.is.instanceof(EventEmitter);
        done();
      });
    });

    it('#createConfirmChannel exposes the expected api', (done) => {
      connection.createConfirmChannel((err, channel) => {
        if (err) return done(err);
        expect(channel).have.property('ack').that.is.a('function');
        expect(channel).have.property('ackAll').that.is.a('function');
        expect(channel).have.property('assertExchange').that.is.a('function');
        expect(channel).have.property('assertQueue').that.is.a('function');
        expect(channel).have.property('bindExchange').that.is.a('function');
        expect(channel).have.property('bindQueue').that.is.a('function');
        expect(channel).have.property('cancel').that.is.a('function');
        expect(channel).have.property('checkExchange').that.is.a('function');
        expect(channel).have.property('checkQueue').that.is.a('function');
        expect(channel).have.property('consume').that.is.a('function');
        expect(channel).have.property('deleteExchange').that.is.a('function');
        expect(channel).have.property('deleteQueue').that.is.a('function');
        expect(channel).have.property('get').that.is.a('function');
        expect(channel).have.property('nack').that.is.a('function');
        expect(channel).have.property('nackAll').that.is.a('function');
        expect(channel).have.property('prefetch').that.is.a('function');
        expect(channel).have.property('publish').that.is.a('function');
        expect(channel).have.property('purgeQueue').that.is.a('function');
        expect(channel).have.property('reject').that.is.a('function');
        expect(channel).have.property('sendToQueue').that.is.a('function');
        expect(channel).have.property('unbindExchange').that.is.a('function');
        expect(channel).have.property('unbindQueue').that.is.a('function');
        expect(channel).have.property('on').that.is.a('function');
        expect(channel).have.property('once').that.is.a('function');
        expect(channel).have.property('close').that.is.a('function');
        done();
      });
    });

    it('createChannel returns a promise with resolved channel', async () => {
      const channel = await connection.createChannel();
      expect(channel).to.have.property('assertExchange').that.is.a('function');
    });

    it('can assert exchange into existance', (done) => {
      connection.createChannel((err, channel) => {
        if (err) return done(err);
        channel.assertExchange('event', 'topic', () => {
          done();
        });
      });
    });

    it('returns error in callback', (done) => {
      connection.createChannel((channelErr, channel) => {
        if (channelErr) return done(channelErr);
        channel.assertExchange('wrong-type', {}, (err) => {
          expect(err).to.be.ok.and.have.property('message').that.match(/topic or direct/);
          done();
        });
      });
    });

    it('returns promise that can be caught', async () => {
      const channel = await connection.createChannel();
      const err = await channel.assertExchange('event', 'directly').catch((e) => e);
      expect(err).to.be.ok.and.have.property('message');
    });

    it('throws if unsupported function is called', async () => {
      const channel = await connection.createChannel();
      expect(() => {
        channel.subscribeOnce('event');
      }).to.throw(Error, /is not a function/);
    });

    it('throws if consume() is called without message callback', async () => {
      const channel = await connection.createChannel();
      await channel.assertQueue('event-q');
      try {
        await channel.consume('event-q');
      } catch (e) {
        var err = e; // eslint-disable-line
      }
      expect(err).to.be.ok;
      expect(err.message).to.match(/Message callback/i);
    });

    it('consume() returns promise', async () => {
      const channel = await connection.createChannel();
      await channel.assertQueue('event-q');
      const ok = await channel.consume('event-q', onMessage).then((result) => result);

      expect(ok).to.be.ok.and.have.property('consumerTag').that.is.ok;

      function onMessage() {}
    });

    it('consume() returns message with excpected arguments in message callback', async () => {
      const channel = await connection.createChannel();
      await channel.assertExchange('consume');
      await channel.assertQueue('consume-q');
      await channel.bindQueue('consume-q', 'consume', '#');
      await channel.consume('consume-q', onMessage).then((result) => result);

      let onMessageArgs;

      await channel.publish('consume', 'test', Buffer.from(JSON.stringify({data: 1})));

      expect(onMessageArgs, 'message arguments').to.be.ok;
      expect(onMessageArgs.length).to.equal(1);
      const msg = onMessageArgs[0];

      expect(msg).to.have.property('fields').with.property('routingKey', 'test');

      function onMessage(...args) {
        onMessageArgs = args;
      }
    });
  });

  describe('#assertExchange', () => {
    let channel;
    before(async () => {
      resetMock();
      const connection = await connect('amqp://localhost');
      channel = await connection.createChannel();
    });

    it('creates exchange', async () => {
      const ok = await channel.assertExchange('event');
      expect(ok).to.be.ok;
      expect(ok).to.have.property('exchange', 'event');
    });
  });

  describe('#checkExchange', () => {
    let connection;
    before(async () => {
      resetMock();
      connection = await connect('amqp://localhost');
    });

    it('returns ok in callback if exists', (done) => {
      connection.createChannel().then((channel) => {
        channel.assertExchange('eventcb', () => {
          channel.checkExchange('eventcb', (err, ok) => {
            if (err) return done(err);
            expect(ok).to.be.true;
            done();
          });
        });
      });
    });

    it('returns error in callback if not found', (done) => {
      connection.createChannel().then((channel) => {
        channel.checkExchange('notfound', (err, ok) => {
          expect(err).to.be.an('error');
          expect(ok).to.be.undefined;
          done();
        });
      });
    });

    it('promise returns true if exists', async () => {
      const channel = await connection.createChannel();
      await channel.assertExchange('event');

      const ok = await channel.checkExchange('event');
      expect(ok).to.be.true;
    });

    it('closes channel if not found', async () => {
      const channel = await connection.createChannel();
      try {
        await channel.checkExchange('notfound');
      } catch (err) {
        var error = err; // eslint-disable-line no-var
      }

      expect(error).to.be.an('error');
    });
  });

  describe('#assertQueue', () => {
    let channel;
    before(async () => {
      resetMock();
      const connection = await connect('amqp://localhost');
      channel = await connection.createChannel();
    });

    it('creates named queue', async () => {
      const ok = await channel.assertQueue('event-q');
      expect(ok).to.be.ok;
      expect(ok).to.not.have.property('queue');
      expect(ok).to.have.property('consumerCount');
      expect(ok).to.have.property('messageCount');
    });

    it('returns named queue properties in callback', (done) => {
      channel.assertQueue('eventcb-q', (err, ok) => {
        if (err) return done(err);
        expect(ok).to.be.ok;
        expect(ok).to.not.have.property('queue');
        expect(ok).to.have.property('consumerCount');
        expect(ok).to.have.property('messageCount');
        done();
      });
    });

    it('creates server named queue if name is falsy', async () => {
      const ok = await channel.assertQueue('');
      expect(ok).to.be.ok;
      expect(ok).to.have.property('queue').that.is.ok;
      expect(ok).to.have.property('consumerCount');
      expect(ok).to.have.property('messageCount');
    });

    it('creates exclusive queue if exclusive is passed', async () => {
      const ok = await channel.assertQueue('excl-q', {exclusive: true});
      expect(ok).to.be.ok;
      expect(ok).to.not.have.property('queue');
      expect(ok).to.have.property('consumerCount');
      expect(ok).to.have.property('messageCount');
    });
  });

  describe('#checkQueue', () => {
    let channel;
    beforeEach(async () => {
      resetMock();
      const connection = await connect('amqp://localhost');
      channel = await connection.createChannel();
    });

    it('returns message- and consumer count in callback if exists', (done) => {
      channel.assertQueue('eventcb-q', () => {
        channel.checkQueue('eventcb-q', (err, ok) => {
          if (err) return done(err);
          expect(ok).to.be.ok;
          expect(ok).to.have.property('consumerCount');
          expect(ok).to.have.property('messageCount');
          done();
        });
      });
    });

    it('returns error in callback if not found', (done) => {
      channel.checkQueue('notfound', (err, ok) => {
        expect(err).to.be.an('error');
        expect(ok).to.be.undefined;
        done();
      });
    });

    it('promise returns true if exists', async () => {
      await channel.assertQueue('event-q');

      const ok = await channel.checkQueue('event-q');
      expect(ok).to.be.ok;
      expect(ok).to.have.property('consumerCount');
      expect(ok).to.have.property('messageCount');
    });

    it('rejects queue if not found', async () => {
      try {
        await channel.checkQueue('notfound');
      } catch (err) {
        var error = err; // eslint-disable-line no-var
      }

      expect(error).to.be.an('error');
    });
  });

  describe('#deleteQueue', () => {
    let channel;
    beforeEach(async () => {
      resetMock();
      const connection = await connect('amqp://localhost');
      channel = await connection.createChannel();
      await channel.assertQueue('events-q');
      await channel.sendToQueue('events-q', Buffer.from('MSG'));
      await channel.sendToQueue('events-q', Buffer.from('MSG'));
    });

    it('returns message count', async () => {
      const {messageCount} = await channel.deleteQueue('events-q');
      expect(messageCount).to.equal(2);
    });

    it('returns message count in callback', (done) => {
      channel.deleteQueue('events-q', (err, {messageCount}) => {
        if (err) return done(err);
        expect(messageCount).to.equal(2);
        done();
      });
    });

    it('ignored if not found', async () => {
      return channel.deleteQueue('notfound');
    });
  });

  describe('#purgeQueue', () => {
    let channel;
    beforeEach(async () => {
      resetMock();
      const connection = await connect('amqp://localhost');
      channel = await connection.createChannel();
      await channel.assertQueue('events-q');
      await channel.sendToQueue('events-q', Buffer.from('MSG'));
      await channel.sendToQueue('events-q', Buffer.from('MSG'));
    });

    it('returns message count', async () => {
      const {messageCount} = await channel.purgeQueue('events-q');
      expect(messageCount).to.equal(2);
    });

    it('returns message count in callback', (done) => {
      channel.purgeQueue('events-q', (err, {messageCount}) => {
        if (err) return done(err);
        expect(messageCount).to.equal(2);
        done();
      });
    });

    it('ignored if not found', async () => {
      return channel.purgeQueue('notfound');
    });
  });

  describe('#bindQueue', () => {
    beforeEach(resetMock);

    it('separate channels can bind to exchange created by one of them', async () => {
      const connection = await connect('amqp://localhost');

      const channel1 = await connection.createChannel();
      const channel2 = await connection.createChannel();

      await channel1.assertExchange('events');

      await channel2.assertQueue('event-q');
      await channel2.bindQueue('event-q', 'events', '#');
    });

    it('separate connections to same broker can bind to exchange created by one of them', async () => {
      const connection1 = await connect('amqp://localhost');
      const connection2 = await connect('amqp://localhost');

      const channel1 = await connection1.createChannel();
      const channel2 = await connection2.createChannel();

      await channel1.assertExchange('events');

      await channel2.assertQueue('event-q');
      await channel2.bindQueue('event-q', 'events', '#');
    });
  });

  describe('#unbindQueue', () => {
    let connection;
    beforeEach(async () => {
      resetMock();
      connection = await connect('amqp://localhost');
    });

    it('removes queue to exchange binding', async () => {
      const channel = await connection.createChannel();
      await channel.assertExchange('events');
      await channel.assertQueue('events-q');
      await channel.bindQueue('events-q', 'events', '#');
      await channel.bindQueue('events-q', 'events', 'events.#');
      expect(channel._broker.getExchange('events').bindingCount).to.equal(2);

      const result = await channel.unbindQueue('events-q', 'events', '#');
      expect(result).to.be.true;
      expect(channel._broker.getExchange('events').bindingCount).to.equal(1);
      expect(channel._broker.getExchange('events').getBinding('events-q', 'events.#')).to.be.ok;
    });

    it('throws and closes channel is queue doesn\'t exist', async () => {
      const channel = await connection.createChannel();
      await channel.assertExchange('events');

      try {
        await channel.unbindQueue('events-q', 'events', 'event.#');
      } catch (e) {
        var err = e;
      }

      expect(err).to.match(/queue/).and.have.property('code', 404);
      expect(channel._closed, 'closed channel').to.be.true;
      expect(connection._closed, 'closed connection').to.be.false;
    });

    it('throws and closes channel is exchange doesn\'t exist', async () => {
      const channel = await connection.createChannel();
      await channel.assertQueue('events-q');

      try {
        await channel.unbindQueue('events-q', 'events', 'event.#');
      } catch (e) {
        var err = e;
      }

      expect(err).to.match(/exchange/).and.have.property('code', 404);
      expect(channel._closed, 'closed channel').to.be.true;
      expect(connection._closed, 'closed connection').to.be.false;
    });

    it('returns ok binding doesn\'t exist', async () => {
      const connection = await connect('amqp://localhost');
      const channel = await connection.createChannel();
      await channel.assertExchange('events');
      await channel.assertQueue('events-q');

      const result = await channel.unbindQueue('events-q', 'events', 'event.#');
      expect(result).to.be.true;
    });
  });

  describe('#bindExchange', () => {
    beforeEach(resetMock);

    it('bind exchange to exchange', async () => {
      const connection = await connect('amqp://localhost');
      const channel = await connection.createChannel();

      await channel.assertExchange('events');
      await channel.assertExchange('sub-events');
      await channel.assertQueue('sub-events-q');
      await channel.bindQueue('sub-events-q', 'sub-events', '#');

      await channel.bindExchange('sub-events', 'events', 'event.#');

      channel._broker.publish('events', 'test.1', Buffer.from('MSG'));
      channel._broker.publish('events', 'event.1', Buffer.from('MSG'));

      expect(channel._broker.getQueue('sub-events-q')).to.have.property('messageCount', 1);
    });
  });

  describe('#unbindExchange', () => {
    let connection, channel;
    beforeEach(async () => {
      resetMock();
      connection = await connect('amqp://localhost');
      channel = await connection.createChannel();
    });

    it('unbinds bound exchange to exchange', async () => {
      await channel.assertExchange('events');
      await channel.assertExchange('sub-events');
      await channel.assertQueue('sub-events-q');
      await channel.bindQueue('sub-events-q', 'sub-events', '#');

      await channel.bindExchange('sub-events', 'events', 'event.#');

      channel._broker.publish('events', 'test.1', Buffer.from('MSG'));
      channel._broker.publish('events', 'event.1', Buffer.from('MSG'));
      channel._broker.publish('events', 'event.2', Buffer.from('MSG'));

      expect(channel._broker.getQueue('sub-events-q')).to.have.property('messageCount', 2);

      await channel.unbindExchange('sub-events', 'events', 'event.#');

      channel._broker.publish('events', 'event.3', Buffer.from('MSG'));

      expect(channel._broker.getQueue('sub-events-q')).to.have.property('messageCount', 2);
    });

    it('returns ok in callback', async () => {
      await channel.assertExchange('events');
      await channel.assertExchange('sub-events');
      await channel.assertQueue('sub-events-q');
      await channel.bindQueue('sub-events-q', 'sub-events', '#');

      await channel.bindExchange('sub-events', 'events', 'event.#');

      return new Promise((resolve, reject) => {
        channel.unbindExchange('sub-events', 'events', 'event.#', (err, result) => {
          if (err) return reject(err);
          expect(result).to.be.true;
          resolve();
        });
      });
    });

    it('throws and closes channel is destination exchange doesn\'t exist', async () => {
      await channel.assertExchange('events');

      try {
        await channel.unbindExchange('sub-events', 'events', 'event.#');
      } catch (e) {
        var err = e;
      }

      expect(err).to.match(/sub-events/).and.have.property('code', 404);
      expect(channel._closed, 'closed channel').to.be.true;
      expect(connection._closed, 'closed connection').to.be.false;
    });

    it('throws and closes channel is source exchange doesn\'t exist', async () => {
      await channel.assertExchange('sub-events');

      try {
        await channel.unbindExchange('sub-events', 'events', 'event.#');
      } catch (e) {
        var err = e;
      }

      expect(err).to.match(/'events/).and.have.property('code', 404);
      expect(channel._closed, 'closed channel').to.be.true;
      expect(connection._closed, 'closed connection').to.be.false;
    });

    it('returns ok binding doesn\'t exist', async () => {
      await channel.assertExchange('events');
      await channel.assertExchange('sub-events');

      const result = await channel.unbindExchange('sub-events', 'events', 'event.#');
      expect(result).to.be.true;
    });
  });

  describe('#deleteExchange', () => {
    beforeEach(resetMock);

    it('deletes exchange', async () => {
      const connection = await connect('amqp://localhost');

      const channel = await connection.createChannel();
      await channel.assertExchange('events');

      await channel.deleteExchange('events');

      expect(channel._broker.exchangeCount).to.equal(0);
    });
  });

  describe('#publish', () => {
    let connection;
    beforeEach(async () => {
      resetMock();
      connection = await connect('amqp://localhost');
    });

    it('ignores callback and returns true', async () => {
      const channel = await connection.createChannel();
      await channel.assertExchange('consume');
      await channel.assertQueue('consume-q');
      await channel.bindQueue('consume-q', 'consume', '#');

      return new Promise((resolve, reject) => {
        const result = channel.publish('consume', 'test.1', Buffer.from('msg'), {type: 'test'}, {}, () => {
          reject(new Error('Ignore callback'));
        });
        expect(result, 'return value').to.be.true;
        channel.consume('consume-q', resolve);
      });
    });

    it('confirm channel calls callback with error if message was undeliverable', async () => {
      const channel = await connection.createConfirmChannel();
      await channel.assertExchange('consume');

      return new Promise((resolve, reject) => {
        channel.publish('consume', 'test.1', Buffer.from('MSG'), {}, (err, ok) => {
          if (ok) return reject(new Error('undeliverable message was delivered'));
          resolve(err);
        });
      });
    });

    it('confirm channel calls callback with error message was nacked', async () => {
      const channel = await connection.createConfirmChannel();
      await channel.assertExchange('consume');
      await channel.assertQueue('consume-q');
      await channel.bindQueue('consume-q', 'consume', '#');

      channel.consume('consume-q', (message) => {
        channel.nack(message, false, false);
      });

      return new Promise((resolve, reject) => {
        channel.publish('consume', 'test.1', Buffer.from('MSG'), {}, (err, ok) => {
          if (ok) return reject(new Error('is ok'));
          resolve(err);
        });
      });
    });

    it('confirm channel calls callback when message is acked', async () => {
      const channel = await connection.createConfirmChannel();
      await channel.assertExchange('consume');
      await channel.assertQueue('consume-q');
      await channel.bindQueue('consume-q', 'consume', '#');

      channel.consume('consume-q', (message) => {
        channel.ack(message);
      });

      return new Promise((resolve, reject) => {
        const result = channel.publish('consume', 'test.1', Buffer.from('MSG'), {}, (err, ok) => {
          if (err) return reject(err);
          resolve(ok);
        });
        expect(result, 'return value').to.be.true;
      });
    });

    it('emits return on channel if mandatory message was not routed', async () => {
      const channel = await connection.createChannel();
      await channel.assertExchange('consume');

      const onReturn = new Promise((resolve) => {
        channel.on('return', (msg) => {
          resolve(msg);
        });
      });

      channel.publish('consume', 'test.1', Buffer.from('MSG'), {mandatory: true});

      const msg = await onReturn;
      expect(msg).to.be.ok;

      expect(msg).to.have.property('fields').with.property('routingKey', 'test.1');
      expect(msg).to.have.property('content');
      expect(msg.content.toString()).to.equal('MSG');
      expect(msg).to.have.property('properties');
    });

    it('closes channel if exchange doesn\'t exist', (done) => {
      connection.createChannel().then((channel) => {
        channel.on('error', (err) => {
          expect(err.code).to.equal(404);
          done();
        });

        const result = channel.publish('consume', 'test.1', Buffer.from('MSG'));

        expect(result).to.be.true;
      }).catch(done);
    });

    it('throws TypeError if content is not a Buffer', async () => {
      const channel = await connection.createChannel();
      expect(() => {
        channel.publish('events', {});
      }).to.throw(TypeError);
    });
  });

  describe('#sendToQueue', () => {
    let connection;
    beforeEach(async () => {
      resetMock();
      connection = await connect('amqp://localhost');
    });

    it('throws TypeError if content is not a Buffer', async () => {
      const channel = await connection.createChannel();
      expect(() => {
        channel.sendToQueue('events-q', {});
      }).to.throw(TypeError);
    });

    it('ignores callback if not confirm channel', async () => {
      const channel = await connection.createChannel();
      await channel.assertExchange('consume');
      await channel.assertQueue('consume-q');
      await channel.bindQueue('consume-q', 'consume', '#');

      return new Promise((resolve, reject) => {
        const result = channel.sendToQueue('consume-q', Buffer.from('msg'), {}, () => {
          reject(new Error('Ignore callback'));
        });
        expect(result, 'return value').to.be.true;

        channel.consume('consume-q', resolve, {noAck: true});
      });
    });

    it('confirm channel calls callback with error message was nacked', async () => {
      const channel = await connection.createConfirmChannel();
      await channel.assertQueue('consume-q');

      channel.consume('consume-q', (message) => {
        channel.nack(message, false, false);
      });

      return new Promise((resolve, reject) => {
        channel.sendToQueue('consume-q', Buffer.from('MSG'), {}, (err, ok) => {
          if (ok) return reject(new Error('is ok'));
          resolve(err);
        });
      });
    });

    it('confirm channel calls callback with error message was rejected', async () => {
      const channel = await connection.createConfirmChannel();
      await channel.assertQueue('consume-q');

      channel.consume('consume-q', (message) => {
        channel.reject(message, false, false);
      });

      return new Promise((resolve, reject) => {
        channel.sendToQueue('consume-q', Buffer.from('MSG'), {}, (err, ok) => {
          if (ok) return reject(new Error('is ok'));
          resolve(err);
        });
      });
    });

    it('confirm channel calls callback when message was acked', async () => {
      const channel = await connection.createConfirmChannel();
      await channel.assertQueue('consume-q');

      channel.consume('consume-q', (message) => {
        channel.ack(message);
      });

      return new Promise((resolve, reject) => {
        const result = channel.sendToQueue('consume-q', Buffer.from('MSG'), {}, (err, ok) => {
          if (err) return reject(err);
          resolve(ok);
        });
        expect(result, 'return value').to.be.true;
      });
    });

    it('closes channel if queue doesn\'t exist', (done) => {
      connection.createChannel().then((channel) => {
        channel.on('error', (err) => {
          expect(err.code).to.equal(404);
          done();
        });

        const result = channel.sendToQueue('consume-q', Buffer.from('MSG'));
        expect(result).to.be.true;
      }).catch(done);
    });
  });

  describe('#get', () => {
    let channel;
    beforeEach(async () => {
      resetMock();
      const connection = await connect('amqp://amqp.test');
      channel = await connection.createChannel();
      await channel.assertQueue('event-q');
      await channel.sendToQueue('event-q', Buffer.from('MSG'));
    });

    it('returns message in callback', (done) => {
      channel.get('event-q', (err, msg) => {
        if (err) return done(err);
        expect(msg).to.be.ok;
        expect(msg).to.to.have.property('content');
        done();
      });
    });

    it('returns false in callback if no more messages', (done) => {
      channel.get('event-q', (err1) => {
        if (err1) return done(err1);
        channel.get('event-q', (err2, msg) => {
          if (err2) return done(err2);
          expect(msg).to.to.be.false;
          done();
        });
      });
    });

    it('returns error in callback if queue doesn`t exist', (done) => {
      channel.get('notfound-q', (err, msg) => {
        expect(err).to.be.an('error');
        expect(msg).to.be.undefined;
        done();
      });
    });

    it('promise returns message', async () => {
      const msg = await channel.get('event-q');
      expect(msg).to.be.ok;
      expect(msg).to.to.have.property('content');
    });

    it('promise returns false if no more messages', async () => {
      await channel.get('event-q', {noAck: true});
      const msg = await channel.get('event-q');
      expect(msg).to.be.false;
    });

    it('rejects if queue not found', async () => {
      try {
        await channel.get('notfound-q');
      } catch (err) {
        var error = err; // eslint-disable-line no-var
      }

      expect(error).to.be.an('error');
    });
  });

  describe('#consume', () => {
    let connection, channel;
    beforeEach(async () => {
      resetMock();
      connection = await connect('amqp://amqp.test');
      channel = await connection.createChannel();
      await channel.assertExchange('event');
      await channel.assertQueue('event-q');
    });

    it('returns consumerTag in callback', (done) => {
      channel.bindQueue('event-q', 'event', '#').then(() => {
        channel.consume('event-q', () => {}, {}, (err, ok) => {
          if (err) return done(err);
          expect(ok).to.have.property('consumerTag');
          done();
        });
      });
    });

    it('returns published message in message callback', (done) => {
      channel.bindQueue('event-q', 'event', '#').then(() => {
        channel.consume('event-q', (msg) => {
          expect(msg).to.be.ok;
          expect(msg).to.to.have.property('content');
          done();
        });

        channel.publish('event', 'test.message', Buffer.from('MSG'));
      });
    });

    it('with noAck and double bindings to same queue returns published message in callback', async () => {
      await channel.bindQueue('event-q', 'event', 'test.message');
      await channel.bindQueue('event-q', 'event', 'live.#');

      const waitMessages = new Promise((resolve) => {
        const messages = [];
        channel.consume('event-q', (msg) => {
          messages.push(msg);
          if (messages.length === 3) resolve(messages);
        }, {noAck: true});
      });

      channel.publish('event', 'test.prod', Buffer.from('PROD'));
      channel.publish('event', 'test.message', Buffer.from('TEST'));
      channel.publish('event', 'live.message', Buffer.from('LIVE'));
      channel.publish('event', 'live.message.test', Buffer.from('LIVE-TEST'));

      const msgs = await waitMessages;
      expect(msgs[0], 'message #1').to.have.property('fields').with.property('routingKey', 'test.message');
      expect(msgs[1], 'message #2').to.have.property('fields').with.property('routingKey', 'live.message');
      expect(msgs[2], 'message #3').to.have.property('fields').with.property('routingKey', 'live.message.test');
    });

    it('with ack and double bindings to same queue returns published message in callback', async () => {
      await channel.bindQueue('event-q', 'event', 'test.message');
      await channel.bindQueue('event-q', 'event', 'live.#');

      const waitMessages = new Promise((resolve) => {
        const messages = [];
        channel.consume('event-q', (msg) => {
          messages.push(msg);
          channel.ack(msg);
          if (messages.length === 3) resolve(messages);
        });
      });

      channel.publish('event', 'test.prod', Buffer.from('PROD'));
      channel.publish('event', 'test.message', Buffer.from('TEST'));
      channel.publish('event', 'live.message', Buffer.from('LIVE'));
      channel.publish('event', 'live.message.test', Buffer.from('LIVE-TEST'));

      const msgs = await waitMessages;
      expect(msgs[0], 'message #1').to.have.property('fields').with.property('routingKey', 'test.message');
      expect(msgs[1], 'message #2').to.have.property('fields').with.property('routingKey', 'live.message');
      expect(msgs[2], 'message #3').to.have.property('fields').with.property('routingKey', 'live.message.test');
    });

    it('with exclusive returns message in callback', async () => {
      await channel.bindQueue('event-q', 'event', 'live.#');

      const waitMessages = new Promise((resolve) => {
        const messages = [];
        channel.consume('event-q', (msg) => {
          messages.push(msg);

          channel.ack(msg);
          if (messages.length === 2) resolve(messages);
        }, {exclusive: true});
      });

      channel.publish('event', 'live.message', Buffer.from('LIVE'));
      channel.publish('event', 'live.message.test', Buffer.from('LIVE-TEST'));

      const msgs = await waitMessages;
      expect(msgs[0], 'message #1').to.have.property('fields').with.property('routingKey', 'live.message');
      expect(msgs[1], 'message #2').to.have.property('fields').with.property('routingKey', 'live.message.test');
    });

    it('kills channel if trying to consume missing queue', async () => {
      try {
        await channel.consume('non-event-q', (msg) => {
          channel.ack(msg);
        });
      } catch (err) {
        var consumeError = err;
      }

      expect(consumeError).to.be.ok.to.have.property('code', 404);
      expect(consumeError.message).to.equal('Channel closed by server: 404 (NOT-FOUND) with message "NOT_FOUND - no queue \'non-event-q\' in vhost \'/\'');
    });

    it('kills channel and connection if trying to consume exclusive consumed queue', async () => {
      await channel.bindQueue('event-q', 'event', 'live.#');

      const waitMessage = new Promise((resolve) => {
        channel.consume('event-q', (msg) => {
          channel.ack(msg);
          resolve(msg);
        }, {exclusive: true});
      });

      const secondConnection = await connect('amqp://amqp.test');
      const secondChannel = await secondConnection.createChannel();

      try {
        await secondChannel.consume('event-q', (msg) => {
          channel.ack(msg);
        }, {exclusive: true});
      } catch (err) {
        var consumeError = err;
      }

      expect(consumeError).to.be.ok.to.have.property('code', 403);
      expect(consumeError.message).to.equal('Channel closed by server: 403 (ACCESS-REFUSED) with message "ACCESS_REFUSED - queue \'event-q\' in vhost \'/\' in exclusive use"');

      try {
        await secondChannel.publish('event', 'live.dead', Buffer.from('DEAD'));
      } catch (err) {
        var channelError = err;
      }

      expect(channelError).to.be.ok.and.match(/closed/i);
      expect(channelError).to.have.property('code', 504);

      await channel.publish('event', 'live.message', Buffer.from('LIVE'));

      const msg = await waitMessage;
      expect(msg).to.have.property('fields').with.property('routingKey', 'live.message');
    });

    it('kills channel and connection if trying to consume exclusive queue', async () => {
      await channel.assertQueue('exclusive-q', {exclusive: true});
      await channel.bindQueue('exclusive-q', 'event', 'live.#');

      const waitMessage = new Promise((resolve) => {
        channel.consume('exclusive-q', (msg) => {
          channel.ack(msg);
          resolve(msg);
        });
      });

      const secondConnection = await connect('amqp://amqp.test');
      const secondChannel = await secondConnection.createChannel();

      try {
        await secondChannel.consume('exclusive-q', (msg) => {
          channel.ack(msg);
        });
      } catch (err) {
        var consumeError = err;
      }

      expect(consumeError).to.be.ok.to.have.property('code', 403);
      expect(consumeError.message).to.equal('Channel closed by server: 403 (ACCESS-REFUSED) with message "ACCESS_REFUSED - queue \'exclusive-q\' in vhost \'/\' in exclusive use"');

      try {
        await secondChannel.publish('event', 'live.dead', Buffer.from('DEAD'));
      } catch (err) {
        var channelError = err;
      }

      expect(channelError).to.be.ok.and.match(/closed/i);
      expect(channelError).to.have.property('code', 504);

      await channel.publish('event', 'live.message', Buffer.from('LIVE'));

      const msg = await waitMessage;
      expect(msg).to.have.property('fields').with.property('routingKey', 'live.message');
    });
  });

  describe('#cancel', () => {
    let connection, channel, consumerTag;
    beforeEach(async () => {
      resetMock();
      connection = await connect('amqp://amqp.test');
      channel = await connection.createChannel();
      await channel.assertQueue('event-q');
      const result = await channel.consume('event-q', () => {});
      consumerTag = result.consumerTag;
    });

    it('cancels consumer', async () => {
      expect(channel._broker).to.have.property('consumerCount', 1);

      await channel.cancel(consumerTag);

      expect(channel._broker).to.have.property('consumerCount', 0);
    });

    it('invokes callback when consumer is cancelled', (done) => {

      channel.cancel(consumerTag, (err) => {
        if (err) return done(err);
        expect(channel._broker).to.have.property('consumerCount', 0);
        done();
      });
    });
  });

  describe('#prefetch', () => {
    let channel;
    beforeEach(async () => {
      resetMock();
      const connection = await connect('amqp://amqp.test');
      channel = await connection.createChannel();
      await channel.assertExchange('event');
      await channel.assertQueue('event-q');
      await channel.bindQueue('event-q', 'event', '#');
    });

    it('consumes prefetch count messages at a time', async () => {
      channel.prefetch(3);

      await Promise.all(Array(9).fill().map((_, idx) => {
        return channel.publish('event', `test.${idx}`, Buffer.from('' + idx));
      }));

      const messages = [];
      channel.consume('event-q', async (msg) => {
        messages.push(msg);
      });

      expect(messages).to.have.length(3);
      messages.splice(0).forEach((msg) => channel.ack(msg));

      let queueOptions = await channel.checkQueue('event-q');
      expect(queueOptions).to.have.property('messageCount', 6);

      expect(messages).to.have.length(3);
      messages.splice(0).forEach((msg) => channel.ack(msg));

      queueOptions = await channel.checkQueue('event-q');
      expect(queueOptions).to.have.property('messageCount', 3);

      expect(messages).to.have.length(3);
      messages.splice(0).forEach((msg) => channel.ack(msg));
    });

    it('no prefetch consumes "all" messages', async () => {
      await Promise.all(Array(9).fill().map((_, idx) => {
        return channel.publish('event', `test.${idx}`, Buffer.from('' + idx));
      }));

      const messages = [];
      channel.consume('event-q', async (msg) => {
        messages.push(msg);
      });

      expect(messages).to.have.length(9);
    });
  });

  describe('#close', () => {
    let channel;
    beforeEach(async () => {
      resetMock();
      const connection = await connect('amqp://amqp.test');
      channel = await connection.createChannel();
      await channel.assertQueue('event-q');
      await channel.consume('event-q', () => {});
    });

    it('closes all consumers', (done) => {
      expect(channel._broker.consumerCount).to.equal(1);

      channel.close((err) => {
        if (err) return done(err);
        expect(channel._broker.consumerCount).to.equal(0);
        done();
      });
    });

    it('emits close when done', (done) => {
      channel.once('close', () => {
        done();
      });
      channel.close();
    });

    it('promise closes all consumers', async () => {
      expect(channel._broker.consumerCount).to.equal(1);
      await channel.close();
      expect(channel._broker.consumerCount).to.equal(0);
    });

    it('throws if trying to invoke closed channel', async () => {
      await channel.close();
      try {
        await channel.publish('events', 'event.1', Buffer.from('MSG'));
      } catch (e) {
        var err = e;
      }

      expect(err).to.match(/closed/);
    });
  });

  describe('resetMock()', () => {
    it('clears queues, exchanges, and consumers', async () => {
      const connection = await connect('amqp://localhost:15672');
      const channel = await connection.createChannel();
      await channel.assertExchange('event', 'topic', {durable: true, autoDelete: false});
      await channel.assertQueue('event-q');

      await channel.bindQueue('event-q', 'event', '#', {durable: true});

      await channel.assertExchange('temp', 'topic', {durable: false});
      await channel.assertQueue('frifras-q', {durable: false});

      await channel.bindQueue('frifras-q', 'temp', '#');

      await channel.publish('event', 'test', Buffer.from('msg1'));
      await channel.publish('temp', 'test', Buffer.from('msg2'));

      resetMock();

      expect(connection._broker).to.have.property('exchangeCount', 0);
      expect(connection._broker).to.have.property('queueCount', 0);
      expect(connection._broker).to.have.property('consumerCount', 0);
    });

    it('creates new connections after reset', async () => {
      let connection = await connect('amqp://localhost:15672');
      let channel = await connection.createChannel();
      await channel.assertExchange('event', 'topic', {durable: true, autoDelete: false});
      await channel.assertQueue('event-q');

      await channel.bindQueue('event-q', 'event', '#', {durable: true});

      await channel.assertExchange('temp', 'topic', {durable: false});
      await channel.assertQueue('frifras-q', {durable: false});

      await channel.bindQueue('frifras-q', 'temp', '#');

      await channel.publish('event', 'test', Buffer.from('msg1'));
      await channel.publish('temp', 'test', Buffer.from('msg2'));

      resetMock();

      expect(connection._broker).to.have.property('exchangeCount', 0);
      expect(connection._broker).to.have.property('queueCount', 0);
      expect(connection._broker).to.have.property('consumerCount', 0);

      connection = await connect('amqp://localhost:5672');
      channel = await connection.createChannel();

      await channel.assertExchange('event', 'topic', {durable: true, autoDelete: false});
      await channel.assertQueue('event-q');

      expect(connection._broker, 'exchangeCount').to.have.property('exchangeCount', 1);
      expect(connection._broker, 'queueCount').to.have.property('queueCount', 1);
      expect(connection._broker, 'consumerCount').to.have.property('consumerCount', 0);
    });
  });
});
