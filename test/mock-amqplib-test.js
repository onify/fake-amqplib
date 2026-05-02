import { mock } from 'node:test';
import quibble from 'quibble';

import { connect as fakeConnect, resetMock } from '@onify/fake-amqplib';

describe('mocking amqplib in user code', () => {
  describe('via node:test mock.module (Node 20+ recommended)', () => {
    let connect, ctx;

    before(async () => {
      ctx = mock.module('amqplib', { namedExports: { connect: fakeConnect } });
      ({ connect } = await import('amqplib'));
    });

    after(() => {
      ctx.restore();
      resetMock();
    });

    it('imported amqplib.connect resolves to a fake connection', async () => {
      const connection = await connect('amqp://localhost');
      expect(connection.connection.serverProperties).to.have.property('product', 'RabbitMQ');
      await connection.close();
    });

    it('a channel publishes and consumes through the mocked import', async () => {
      const connection = await connect('amqp://localhost');
      const channel = await connection.createChannel();
      await channel.assertQueue('mock-node-q');
      channel.sendToQueue('mock-node-q', Buffer.from('hello'));

      const msg = await new Promise((resolve) => {
        channel.consume('mock-node-q', resolve, { noAck: true });
      });
      expect(msg.content.toString()).to.equal('hello');

      await channel.close();
      await connection.close();
    });
  });

  describe('via quibble', () => {
    let connect;

    before(async () => {
      await quibble.esm('amqplib', { connect: fakeConnect });
      ({ connect } = await import('amqplib'));
    });

    after(() => {
      quibble.reset();
      resetMock();
    });

    it('imported amqplib.connect resolves to a fake connection', async () => {
      const connection = await connect('amqp://localhost');
      expect(connection.connection.serverProperties).to.have.property('product', 'RabbitMQ');
      await connection.close();
    });

    it('a channel publishes and consumes through the mocked import', async () => {
      const connection = await connect('amqp://localhost');
      const channel = await connection.createChannel();
      await channel.assertQueue('mock-quibble-q');
      channel.sendToQueue('mock-quibble-q', Buffer.from('world'));

      const msg = await new Promise((resolve) => {
        channel.consume('mock-quibble-q', resolve, { noAck: true });
      });
      expect(msg.content.toString()).to.equal('world');

      await channel.close();
      await connection.close();
    });
  });
});
