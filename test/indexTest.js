'use strict';

const {connect, resetMock, connections} = require('..');
const {expect} = require('chai');

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
    let channel;
    before(async () => {
      resetMock();
      const connection = await connect('amqp://localhost');
      channel = await connection.createChannel();
    });

    it('returns ok in callback if exists', (done) => {
      channel.assertExchange('eventcb', () => {
        channel.checkExchange('eventcb', (err, ok) => {
          if (err) return done(err);
          expect(ok).to.be.true;
          done();
        });
      });
    });

    it('returns error in callback if not found', (done) => {
      channel.checkExchange('notfound', (err, ok) => {
        expect(err).to.be.an('error');
        expect(ok).to.be.undefined;
        done();
      });
    });

    it('promise returns true if exists', async () => {
      await channel.assertExchange('event');

      const ok = await channel.checkExchange('event');
      expect(ok).to.be.true;
    });

    it('rejects if not found', async () => {
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

  describe('#publish', () => {
    let connection;
    beforeEach(async () => {
      resetMock();
      connection = await connect('amqp://localhost');
    });

    it('ignores callback', async () => {
      const channel = await connection.createChannel();
      await channel.assertExchange('consume');
      await channel.assertQueue('consume-q');
      await channel.bindQueue('consume-q', 'consume', '#');

      return new Promise((resolve, reject) => {
        channel.publish('consume', 'test.1', Buffer.from('msg'), {type: 'test'}, {}, () => {
          reject(new Error('Ignore callback'));
        });
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

      return new Promise((resolve, reject) => {
        channel.publish('consume', 'test.1', Buffer.from('MSG'), {}, (err, ok) => {
          if (ok) return reject(new Error('is ok'));
          resolve(err);
        });

        channel.get('consume-q', (err, message) => {
          channel.nack(message, false, false);
        });
      });
    });

    it('confirm channel calls callback when message is acked', async () => {
      const channel = await connection.createConfirmChannel();
      await channel.assertExchange('consume');
      await channel.assertQueue('consume-q');
      await channel.bindQueue('consume-q', 'consume', '#');

      return new Promise((resolve, reject) => {
        channel.publish('consume', 'test.1', Buffer.from('MSG'), {}, (err, ok) => {
          if (err) return reject(err);
          resolve(ok);
        });

        channel.get('consume-q', (err, message) => {
          channel.ack(message);
        });
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
  });

  describe('#sendToQueue', () => {
    let connection;
    beforeEach(async () => {
      resetMock();
      connection = await connect('amqp://localhost');
    });

    it('breaks if message is not a buffer', async () => {
      const channel = await connection.createChannel();
      await channel.assertExchange('consume');
      await channel.assertQueue('consume-q');
      await channel.bindQueue('consume-q', 'consume', '#');

      expect(() => channel.sendToQueue('consume-q', {})).to.throw(/not a buffer/i);
    });

    it('ignores callback if not confirm channel', async () => {
      const channel = await connection.createChannel();
      await channel.assertExchange('consume');
      await channel.assertQueue('consume-q');
      await channel.bindQueue('consume-q', 'consume', '#');

      return new Promise((resolve, reject) => {
        channel.sendToQueue('consume-q', Buffer.from('msg'), {}, () => {
          reject(new Error('Ignore callback'));
        });

        channel.consume('consume-q', resolve, {noAck: true});
      });
    });

    it('confirm channel calls callback with error message was nacked', async () => {
      const channel = await connection.createConfirmChannel();
      await channel.assertQueue('consume-q');

      return new Promise((resolve, reject) => {
        channel.sendToQueue('consume-q', Buffer.from('MSG'), {}, (err, ok) => {
          if (ok) return reject(new Error('is ok'));
          resolve(err);
        });

        channel.get('consume-q', (err, message) => {
          if (err) reject(err);
          channel.nack(message, false, false);
        });
      });
    });

    it('confirm channel calls callback with error message was rejected', async () => {
      const channel = await connection.createConfirmChannel();
      await channel.assertQueue('consume-q');

      return new Promise((resolve, reject) => {
        channel.sendToQueue('consume-q', Buffer.from('MSG'), {}, (err, ok) => {
          if (ok) return reject(new Error('is ok'));
          resolve(err);
        });

        channel.get('consume-q', (err, message) => {
          channel.reject(message, false);
        });
      });
    });

    it('confirm channel calls callback when message was acked', async () => {
      const channel = await connection.createConfirmChannel();
      await channel.assertQueue('consume-q');

      return new Promise((resolve, reject) => {
        channel.sendToQueue('consume-q', Buffer.from('MSG'), {}, (err, ok) => {
          if (err) return reject(err);
          resolve(ok);
        });

        channel.get('consume-q', (err, message) => {
          channel.ack(message);
        });
      });
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

    it('returns published message in callback', (done) => {
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
      channel.on('close', () => {
        done();
      });
      channel.close();
    });

    it('promise closes all consumers', async () => {
      expect(channel._broker.consumerCount).to.equal(1);
      await channel.close();
      expect(channel._broker.consumerCount).to.equal(0);
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
