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

  describe('#publish', () => {
    let connection;
    before((done) => {
      connect('amqp://localhost', null, (err, conn) => {
        if (err) return done(err);
        connection = conn;
        done();
      });
    });

    it('ignores callback', async () => {
      const channel = await connection.createChannel();
      await channel.assertExchange('consume');
      await channel.assertQueue('consume-q');
      await channel.bindQueue('consume-q', 'consume', '#');

      return new Promise((resolve, reject) => {
        channel.publish('consume', 'test.1', Buffer.from('msg'), {type: 'test'}, () => {
          reject(new Error('Ignore callback'));
        });
        channel.consume('consume-q', resolve);
      });
    });

    it('confirm channel calls callback when published', async () => {
      const channel = await connection.createConfirmChannel();
      await channel.assertExchange('consume');
      await channel.assertQueue('consume-q');
      await channel.bindQueue('consume-q', 'consume', '#');

      return new Promise((resolve) => {
        channel.publish('consume', 'test.1', Buffer.from(JSON.stringify({})), () => {
          resolve();
        });
      });
    });
  });

  describe('#sendToQueue', () => {
    let connection;
    before((done) => {
      connect('amqp://localhost', null, (err, conn) => {
        if (err) return done(err);
        connection = conn;
        done();
      });
    });

    it('breaks if message is not a buffer', async () => {
      const channel = await connection.createChannel();
      await channel.assertQueue('consume-q');
      await channel.bindQueue('consume-q', 'consume', '#');

      expect(() => channel.sendToQueue('consume-q', {})).to.throw(/not a buffer/i);
    });

    it('ignores callback', async () => {
      const channel = await connection.createChannel();
      await channel.assertQueue('consume-q');
      await channel.bindQueue('consume-q', 'consume', '#');

      return new Promise((resolve, reject) => {
        channel.sendToQueue('consume-q', Buffer.from('msg'), () => {
          reject(new Error('Ignore callback'));
        });
        channel.consume('consume-q', resolve);
      });
    });

    it('confirm channel calls callback when sent', async () => {
      const channel = await connection.createConfirmChannel();
      await channel.assertQueue('consume-q');
      await channel.bindQueue('consume-q', 'consume', '#');

      return new Promise((resolve) => {
        channel.sendToQueue('consume-q', Buffer.from(JSON.stringify({})), () => {
          resolve();
        });
      });
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

  describe('#checkQueue', () => {
    let channel;
    before(async () => {
      resetMock();
      const connection = await connect('amqp://localhost');
      channel = await connection.createChannel();
    });

    it('returns ok in callback if exists', (done) => {
      channel.assertQueue('eventcb-q', () => {
        channel.checkQueue('eventcb-q', (err, ok) => {
          if (err) return done(err);
          expect(ok).to.be.true;
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
      expect(ok).to.be.true;
    });

    it('rejects if not found', async () => {
      try {
        await channel.checkQueue('notfound');
      } catch (err) {
        var error = err; // eslint-disable-line no-var
      }

      expect(error).to.be.an('error');
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
