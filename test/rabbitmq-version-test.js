import { connect, resetMock, setVersion, FakeAmqplib } from '../index.js';

describe('different behaviour between RabbitMQ versions', () => {
  after(() => {
    setVersion('3.5');
    resetMock();
  });

  describe('setVersion(minor)', () => {
    it('there can be connections with different versions', async () => {
      setVersion('2.2');
      const conn2 = await connect('amqp://localhost');
      expect(conn2._version).to.equal(2.2);

      setVersion('3.2');
      const conn3 = await connect('amqp://localhost');
      expect(conn3._version).to.equal(3.2);

      const channel2 = await conn2.createChannel();
      expect(channel2.nack).to.be.ok;
      expect(() => channel2.nack()).to.throw(Error);

      expect(channel2._version).to.equal(2.2);

      const channel3 = await conn3.createChannel();
      expect(channel3.nack).to.be.a('function');
      expect(channel3._version).to.equal(3.2);
    });

    it('numbers can be used', async () => {
      setVersion(3.7);
      const conn3 = await connect('amqp://localhost');
      expect(conn3._version).to.equal(3.7);
    });

    it('NaN is ignored', async () => {
      setVersion('3.5');
      setVersion('not-a-number');
      const conn3 = await connect('amqp://localhost');
      expect(conn3._version).to.equal(3.5);
    });
  });

  describe('version 2.3', () => {
    it('before 2.3 there was no nack function, and it throws in this fake one', async () => {
      const fakeAmqplib = new FakeAmqplib('2.2');
      const conn = await fakeAmqplib.connect('amqp://localhost');
      const channel = await conn.createChannel();
      expect(() => channel.nack()).to.throw(Error, /not implemented/d);
    });
  });

  describe('version < 3.2', () => {
    let fakeAmqplib3, conn;
    before(async () => {
      fakeAmqplib3 = new FakeAmqplib('3.1');
      conn = await fakeAmqplib3.connect('amqp://localhost');
    });

    describe('#deleteQueue', () => {
      it('threw if queue didn\'t exist', async () => {
        const channel = await conn.createChannel();

        try {
          await channel.deleteQueue('not-here-q');
        } catch (e) {
          var err = e;
        }

        expect(err).to.be.ok.and.have.property('code', 404);
      });

      it('returned error in callback if queue didn\'t exist', (done) => {
        conn.createChannel((cerr, channel) => {
          if (cerr) return done(cerr);

          channel.deleteQueue('not-here-q', (err) => {
            expect(err).to.be.ok.and.have.property('code', 404);
            done();
          });
        });
      });
    });

    describe('#purgeQueue', () => {
      it('threw if queue didn\'t exist', async () => {
        const channel = await conn.createChannel();

        try {
          await channel.purgeQueue('not-here-q');
        } catch (e) {
          var err = e;
        }

        expect(err).to.be.ok.and.have.property('code', 404);
      });

      it('returned error in callback if queue didn\'t exist', (done) => {
        conn.createChannel((cerr, channel) => {
          if (cerr) return done(cerr);

          channel.purgeQueue('not-here-q', (err) => {
            expect(err).to.be.ok.and.have.property('code', 404);
            done();
          });
        });
      });
    });

    describe('#deleteExchange', () => {
      it('threw if exchange didn\'t exist', async () => {
        const channel = await conn.createChannel();

        try {
          await channel.deleteExchange('not-here');
        } catch (e) {
          var err = e;
        }

        expect(err).to.be.ok.and.have.property('code', 404);
      });

      it('returned error in callback if exchange didn\'t exist', (done) => {
        conn.createChannel((cerr, channel) => {
          if (cerr) return done(cerr);

          channel.deleteExchange('not-here', (err) => {
            expect(err).to.be.ok.and.have.property('code', 404);
            done();
          });
        });
      });
    });

    describe('#unbindQueue', () => {
      let connection, channel;
      beforeEach(async () => {
        fakeAmqplib3.resetMock();
        connection = await fakeAmqplib3.connect('amqp://localhost');
        channel = await connection.createChannel();
        await channel.assertExchange('events');
        await channel.assertQueue('events-q');
      });

      it('threw and closed connection if binding didn\'t exist', async () => {
        try {
          await channel.unbindQueue('events-q', 'events', 'event.#');
        } catch (e) {
          var err = e;
        }

        expect(err).to.match(/binding/).and.have.property('code', 404);
        expect(channel._closed, 'closed channel').to.be.true;
        expect(connection._closed, 'closed connection').to.be.true;
      });

      it('returned error in callback and closed connection if binding didn\'t exist', (done) => {
        channel.unbindQueue('events-q', 'events', 'event.#', (err) => {
          expect(err).to.match(/binding/).and.have.property('code', 404);
          expect(channel._closed, 'closed channel').to.be.true;
          expect(connection._closed, 'closed connection').to.be.true;
          done();
        });
      });
    });

    describe('#unbindExchange', () => {
      let connection, channel;
      beforeEach(async () => {
        fakeAmqplib3.resetMock();
        connection = await fakeAmqplib3.connect('amqp://localhost');
        channel = await connection.createChannel();
        await channel.assertExchange('events');
        await channel.assertExchange('sub-events');
      });

      it('threw and closed channel if binding didn\'t exist', async () => {
        try {
          await channel.unbindExchange('sub-events', 'events', 'event.#');
        } catch (e) {
          var err = e;
        }

        expect(err).to.match(/binding/).and.have.property('code', 404);
        expect(channel._closed, 'closed channel').to.be.true;
        expect(connection._closed, 'closed connection').to.be.false;
      });

      it('returned error in callback if binding didn\'t exist', (done) => {
        channel.unbindExchange('sub-events', 'events', 'event.#', (err) => {
          expect(err).to.match(/binding/).and.have.property('code', 404);
          expect(channel._closed, 'closed channel').to.be.true;
          expect(connection._closed, 'closed connection').to.be.false;
          done();
        });
      });
    });
  });

  describe('version 3.2', () => {
    let fakeAmqplib32;
    before(() => {
      fakeAmqplib32 = new FakeAmqplib('3.2');
    });

    describe('#unbindQueue', () => {
      let connection, channel;
      beforeEach(async () => {
        fakeAmqplib32.resetMock();
        connection = await fakeAmqplib32.connect('amqp://localhost');
        channel = await connection.createChannel();
        await channel.assertExchange('events');
        await channel.assertQueue('events-q');
      });

      it('threw and closed channel if binding didn\'t exist', async () => {
        try {
          await channel.unbindQueue('events-q', 'events', 'event.#');
        } catch (e) {
          var err = e;
        }

        expect(err).to.match(/binding/).and.have.property('code', 404);
        expect(channel._closed, 'closed channel').to.be.true;
        expect(connection._closed, 'closed connection').to.be.false;
      });

      it('returned error in callback and closed channel if binding didn\'t exist', (done) => {
        channel.unbindQueue('events-q', 'events', 'event.#', (err) => {
          expect(err).to.match(/binding/).and.have.property('code', 404);
          expect(channel._closed, 'closed channel').to.be.true;
          expect(connection._closed, 'closed connection').to.be.false;
          done();
        });
      });
    });
  });

  describe('#prefetch', () => {
    let connection, channel;
    let fakeAmqplib32;
    before(() => {
      fakeAmqplib32 = new FakeAmqplib('3.2');
    });

    it('setting prefetch with global flag before version 3.3 kills connection', async () => {
      connection = await fakeAmqplib32.connect('amqp://localhost');
      channel = await connection.createChannel();

      channel.prefetch(4, true);

      expect(channel._closed, 'channel closed').to.be.true;
      expect(connection._closed, 'connection closed').to.be.true;

      try {
        await channel.assertExchange('events', 'topic');
      } catch (err) {
        var error = err;
      }

      expect(error).to.match(/closed/i);
      expect(error.code).to.equal(504);
    });
  });
});
