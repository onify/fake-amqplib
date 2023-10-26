import { FakeAmqplib } from '../index.js';

describe('channel #prefetch', () => {
  describe('version > 3.2', () => {
    let fakeAmqplib35, channel;

    beforeEach(async () => {
      fakeAmqplib35 = new FakeAmqplib('3.5');
      const connection = await fakeAmqplib35.connect('amqp://localhost');
      channel = await connection.createChannel();

      await channel.assertExchange('event');

      await channel.assertQueue('event-q');
      await channel.bindQueue('event-q', 'event', 'event.#');
      await channel.assertQueue('other-q');
      await channel.bindQueue('other-q', 'event', 'other.#');
    });
    afterEach(() => fakeAmqplib35.resetMock());

    it('no prefetch consumes "all" (10000 in this fake one) messages', async () => {
      await Promise.all(Array(9).fill().map((_, idx) => {
        return channel.publish('event', `event.${idx}`, Buffer.from(`${idx}`));
      }));

      const messages = [];
      channel.consume('event-q', (msg) => {
        messages.push(msg);
      });

      expect(messages).to.have.length(9);
    });

    describe('prefetch per consumer', () => {
      it('prefetch limits number of messages sent per consumer', async () => {
        channel.prefetch(3);

        await Promise.all(Array(9).fill().map((_, idx) => {
          return channel.publish('event', `event.${idx}`, Buffer.from(`${idx}`));
        }));

        const messages = [];
        channel.consume('event-q', (msg) => {
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
    });

    describe('global prefetch', () => {
      it('limits number of messages sent to channel', async () => {
        channel.prefetch(10, true);

        const messages = [];
        channel.consume('event-q', (msg) => {
          messages.push(msg);
        }, { consumerTag: 'event-tag' });

        channel.consume('other-q', (msg) => {
          messages.push(msg);
        }, { consumerTag: 'other-tag' });

        await Promise.all(Array(31).fill().map((_, idx) => {
          return channel.publish('event', `event.${idx}`, Buffer.from(`${idx}`));
        }));

        await Promise.all(Array(31).fill().map((_, idx) => {
          return channel.publish('event', `other.${idx}`, Buffer.from(`${idx}`));
        }));

        expect(messages).to.have.length(10);
      });

      it('acked message allows next message to be consumed', async () => {
        channel.prefetch(10, true);

        const messages = [];
        channel.consume('event-q', (msg) => {
          messages.push(msg);
        }, { consumerTag: 'event-tag' });

        channel.consume('other-q', (msg) => {
          messages.push(msg);
        }, { consumerTag: 'other-tag' });

        await Promise.all(Array(31).fill().map((_, idx) => {
          return channel.publish('event', `event.${idx}`, Buffer.from(`${idx}`));
        }));

        expect(messages, 'before ack consumed messages').to.have.length(10);

        channel.ack(messages[0]);

        await new Promise((resolve) => process.nextTick(resolve));

        expect(messages, 'after ack consumed messages').to.have.length(11);
      });

      it('nacked message allows next message to be consumed', async () => {
        channel.prefetch(10, true);

        const messages = [];
        channel.consume('event-q', (msg) => {
          messages.push(msg);
        }, { consumerTag: 'event-tag' });

        await Promise.all(Array(31).fill().map((_, idx) => {
          return channel.publish('event', `event.${idx}`, Buffer.from(`${idx}`));
        }));

        expect(messages, 'before ack consumed messages').to.have.length(10);

        channel.nack(messages[0]);

        await new Promise((resolve) => process.nextTick(resolve));

        expect(messages, 'after ack consumed messages').to.have.length(11);
      });

      it('rejected message with requeue consumes message again', async () => {
        channel.prefetch(10, true);

        const messages = [];
        channel.consume('event-q', (msg) => {
          messages.push(msg);
        }, { consumerTag: 'event-tag' });

        await Promise.all(Array(31).fill().map((_, idx) => {
          return channel.publish('event', `event.${idx}`, Buffer.from(`${idx}`));
        }));

        expect(messages, 'before ack consumed messages').to.have.length(10);

        channel.reject(messages[0], true);

        await new Promise((resolve) => process.nextTick(resolve));

        expect(messages, 'after reject message').to.have.length(11);
      });

      it('ackAll consumes next messages', async () => {
        channel.prefetch(10, true);

        const messages = [];
        channel.consume('event-q', (msg) => {
          messages.push(msg);
        }, { consumerTag: 'event-tag' });

        await Promise.all(Array(31).fill().map((_, idx) => {
          return channel.publish('event', `event.${idx}`, Buffer.from(`${idx}`));
        }));

        expect(messages).to.have.length(10);

        channel.ackAll();

        await new Promise((resolve) => process.nextTick(resolve));

        expect(messages).to.have.length(20);
      });

      it('limits messages on all channel consumers', async () => {
        channel.prefetch(5);
        channel.prefetch(20, true);
        await channel.assertQueue('other-q');

        await Promise.all(Array(31).fill().map((_, idx) => {
          return channel.publish('event', `event.${idx}`, Buffer.from(`${idx}`));
        }));

        await Promise.all(Array(31).fill().map((_, idx) => {
          return channel.publish('event', `other.${idx}`, Buffer.from(`${idx}`));
        }));

        const messages = [];
        channel.consume('event-q', (msg) => {
          messages.push(msg);
        }, { consumerTag: 'event-tag' });

        channel.consume('other-q', (msg) => {
          messages.push(msg);
        }, { consumerTag: 'other-tag' });

        expect(messages).to.have.length(10);

        channel.ack(messages.slice(-1)[0]);

        expect(messages).to.have.length(11);

        channel.ackAll();

        expect(messages).to.have.length(21);
      });
    });
  });

  describe('version <= 3.2', () => {
    let fakeAmqplib32, channel;

    beforeEach(async () => {
      fakeAmqplib32 = new FakeAmqplib('3.2');
      const connection = await fakeAmqplib32.connect('amqp://localhost');
      channel = await connection.createChannel();

      await channel.assertExchange('event');

      await channel.assertQueue('event-q');
      await channel.bindQueue('event-q', 'event', 'event.#');
      await channel.assertQueue('other-q');
      await channel.bindQueue('other-q', 'event', 'other.#');
    });
    afterEach(() => fakeAmqplib32.resetMock());

    describe('global prefetch', () => {
      it('limits number of messages sent to channel', async () => {
        channel.prefetch(10);

        const messages = [];
        channel.consume('event-q', (msg) => {
          messages.push(msg);
        }, { consumerTag: 'event-tag' });

        channel.consume('other-q', (msg) => {
          messages.push(msg);
        }, { consumerTag: 'other-tag' });

        await Promise.all(Array(31).fill().map((_, idx) => {
          return channel.publish('event', `event.${idx}`, Buffer.from(`${idx}`));
        }));

        await Promise.all(Array(31).fill().map((_, idx) => {
          return channel.publish('event', `other.${idx}`, Buffer.from(`${idx}`));
        }));

        expect(messages).to.have.length(10);

        channel.ackAll();

        expect(messages).to.have.length(20);

        channel.nackAll();

        expect(messages).to.have.length(30);
      });
    });
  });
});
