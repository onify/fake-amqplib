import { connect, connectSync, connections, resetMock, setVersion } from '../index.js';

describe('fake amqplib connections', () => {
  describe('#connect', () => {
    beforeEach(resetMock);

    it('exposes the expected api on connection', (done) => {
      connect('amqp://localhost', null, (err, connection) => {
        if (err) return done(err);
        expect(connection).have.property('createChannel').that.is.a('function');
        expect(connection).have.property('createConfirmChannel').that.is.a('function');
        expect(connection).have.property('close').that.is.a('function');
        expect(connection).have.property('on').that.is.a('function');
        expect(connection).have.property('once').that.is.a('function');
        expect(connection).have.property('_closed');
        done();
      });
    });

    it('no amqpUrl defaults to amqp://localhost:5672/', async () => {
      const conn = await connect();
      expect(conn._url.toString()).to.equal('amqp://localhost:5672/');
    });

    it('username only defaults defaults hostname etc', async () => {
      const conn = await connect({ username: 'guest' });
      expect(conn._url.toString()).to.equal('amqp://guest@localhost:5672/');
    });

    it('tuning parameters only defaults hostname etc', async () => {
      const conn = await connect({ locale: 'sv_SE' });
      expect(conn._url.toString()).to.equal('amqp://localhost:5672/?locale=sv_SE');
    });

    it('connections with the same amqpUrl shares broker', async () => {
      const conn1 = await connect('amqp://testrabbit:5672');
      const conn2 = await connect('amqp://testrabbit:5672');

      expect(conn1._broker === conn2._broker, 'shared broker').to.be.true;
      expect(conn1 === conn2, 'same connection').to.be.false;
    });

    it('connection with the same new URL(amqpUrl) shares broker', async () => {
      const conn1 = await connect(new URL('amqp://testrabbit:5672'));
      const conn2 = await connect(new URL('amqp://testrabbit:5672'));

      expect(conn1._broker === conn2._broker, 'shared broker').to.be.true;
      expect(conn1 === conn2, 'same connection').to.be.false;
    });

    it('connection with the equal amqpUrl object shares broker', async () => {
      const connObj = {
        protocol: 'amqp',
        hostname: 'localhost',
        port: 15672,
        username: 'guest',
        password: 'guest',
        locale: 'en_US',
        frameMax: 0,
        heartbeat: 0,
        vhost: '/myhost',
      };

      const conn1 = await connect({ ...connObj });
      expect(conn1._url.toString()).to.equal('amqp://guest:guest@localhost:15672/myhost?locale=en_US&frameMax=0&heartbeat=0');

      const conn2 = await connect({ ...connObj });

      expect(conn1._broker === conn2._broker, 'shared broker').to.be.true;
      expect(conn1 === conn2, 'same connection').to.be.false;
    });

    it('connection with different amqpUrls has different brokers', async () => {
      const conn1 = await connect('amqp://testrabbit:5672');
      const conn2 = await connect('amqp://testrabbit:15672');

      expect(conn1._broker === conn2._broker).to.be.false;
    });

    it('connection with the same amqpUrl but different querystrings shares broker', async () => {
      const conn1 = await connect('amqp://testrabbit:5672?heartbeat=10');
      const conn2 = await connect('amqp://testrabbit:5672');

      expect(conn1._broker === conn2._broker, 'shared broker').to.be.true;
      expect(conn1 === conn2, 'same connection').to.be.false;
    });

    it('connection with the same amqpUrl but different username password shares broker', async () => {
      const conn1 = await connect('amqp://username@testrabbit:5672');
      const conn2 = await connect('amqp://username:password@testrabbit:5672');

      expect(conn1._broker === conn2._broker, 'shared broker').to.be.true;
      expect(conn1 === conn2, 'same connection').to.be.false;
    });

    it('connection with the same amqpUrl but different vhost shares broker', async () => {
      const conn1 = await connect('amqp://testrabbit:5672/host1');
      const conn2 = await connect('amqp://testrabbit:5672/host2');

      expect(conn1._broker === conn2._broker, 'same broker').to.be.false;
      expect(conn1 === conn2, 'same connection').to.be.false;
    });

    it('connection with the same amqpUrl but different protocols shares broker', async () => {
      const conn1 = await connect('amqp://testrabbit:5672');
      const conn2 = await connect('amqps://testrabbit:5672');

      expect(conn1._broker === conn2._broker, 'same broker').to.be.true;
      expect(conn1 === conn2, 'same connection').to.be.false;
    });

    it('exposes connection list', async () => {
      const conn1 = await connect('amqp://localhost:5672');
      const conn2 = await connect('amqp://localhost:15672');
      await connect('amqp://localhost:15672/vhost');
      expect(connections).to.have.length.above(2).and.include.members([ conn1, conn2 ]);
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
      const msgs = [];
      await channel2.consume('event-q', (msg) => {
        msgs.push(msg);
      }, { noAck: true });

      const channel1 = await conn1.createChannel();
      await channel1.assertQueue('event1-q');
      await channel1.bindQueue('event1-q', 'event', '#');
      await channel1.consume('event1-q', (msg) => {
        msgs.push(msg);
      }, { noAck: true });

      await conn1.close();

      await channel2.publish('event', 'test.event', Buffer.from('test'));
      expect(msgs).to.have.length(1);
    });
  });

  describe('connection properties', () => {
    beforeEach(resetMock);

    it('exposes connection serverProperties', async () => {
      setVersion('3.8');
      const connection = await connect('amqp://testrabbit:5672/host1');
      expect(connection).to.have.property('connection').with.property('serverProperties');
      expect(connection.connection.serverProperties).to.deep.equal({
        host: 'testrabbit:5672',
        product: 'RabbitMQ',
        version: '3.8.0',
        platform: 'OS',
        copyright: 'MIT',
        information: 'fake',
      });
    });
  });

  describe('helper method #connectSync', () => {
    beforeEach(resetMock);

    it('adds connection synchronously', async () => {
      await connect('amqp://testrabbit:5672');
      const connection = connectSync('amqp://testrabbit:15672');

      expect(connection).have.property('createChannel').that.is.a('function');
      expect(connection).have.property('createConfirmChannel').that.is.a('function');

      expect(connections).to.have.length(2);
    });

    it('returns shared broker from existing connection', async () => {
      const conn1 = await connect('amqp://testrabbit:5672');
      const conn2 = connectSync('amqp://testrabbit:5672');

      expect(conn1._broker === conn2._broker, 'shared broker').to.be.true;
      expect(conn1 === conn2, 'same connection').to.be.false;
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

    it('emits close once', (done) => {
      connect('amqp://conn.test').then((connection) => {
        connection.on('close', () => {
          done();
        });

        connection.close();
        connection.close();
      });
    });

    it('calls callback if closed twice', (done) => {
      connect('amqp://conn.test').then((connection) => {
        connection.close();
        connection.close(done);
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
});
