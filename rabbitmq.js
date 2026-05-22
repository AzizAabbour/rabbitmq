const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const QUEUE_NAME = process.env.RABBITMQ_QUEUE || 'express_messages';

class RabbitMQService {
  constructor() {
    this.url = RABBITMQ_URL;
    this.queueName = QUEUE_NAME;
    this.connection = null;
    this.publishChannel = null;
    this.consumeChannel = null;
    this.connectPromise = null;
    this.consumeHandler = null;
    this.reconnectTimer = null;
  }

  async connect() {
    if (this.connection) {
      return this.connection;
    }

    if (this.connectPromise) {
      return this.connectPromise;
    }

    this.connectPromise = (async () => {
      const connection = await amqp.connect(this.url);

      connection.on('error', (error) => {
        console.error(`[RabbitMQ] connection error: ${error.message}`);
      });

      connection.on('close', () => {
        this.connection = null;
        this.publishChannel = null;
        this.consumeChannel = null;

        if (this.consumeHandler) {
          this.scheduleReconnect();
        }
      });

      this.connection = connection;
      this.publishChannel = await connection.createChannel();
      await this.publishChannel.assertQueue(this.queueName, { durable: true });

      return connection;
    })();

    try {
      return await this.connectPromise;
    } finally {
      this.connectPromise = null;
    }
  }

  scheduleReconnect() {
    if (this.reconnectTimer) {
      return;
    }

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;

      if (this.consumeHandler) {
        this.startConsumer(this.consumeHandler).catch((error) => {
          console.error(`[RabbitMQ] reconnect failed: ${error.message}`);
        });
      }
    }, 3000);
  }

  async publishMessage(message) {
    await this.connect();

    const body = Buffer.from(JSON.stringify(message));
    const published = this.publishChannel.sendToQueue(this.queueName, body, {
      contentType: 'application/json',
      persistent: true,
    });

    if (!published) {
      throw new Error('Message could not be queued.');
    }
  }

  async startConsumer(onMessage) {
    this.consumeHandler = onMessage;

    try {
      await this.connect();
    } catch (error) {
      this.scheduleReconnect();
      throw error;
    }

    if (this.consumeChannel) {
      return;
    }

    this.consumeChannel = await this.connection.createChannel();
    await this.consumeChannel.assertQueue(this.queueName, { durable: true });

    await this.consumeChannel.consume(
      this.queueName,
      (msg) => {
        if (!msg) {
          return;
        }

        const channel = this.consumeChannel;

        try {
          const payload = JSON.parse(msg.content.toString());
          onMessage(payload);
        } catch (error) {
          console.error(`[RabbitMQ] failed to process message: ${error.message}`);
        } finally {
          if (channel) {
            channel.ack(msg);
          }
        }
      },
      { noAck: false }
    );
  }

  async close() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    this.consumeHandler = null;

    if (this.consumeChannel) {
      await this.consumeChannel.close().catch(() => {});
      this.consumeChannel = null;
    }

    if (this.publishChannel) {
      await this.publishChannel.close().catch(() => {});
      this.publishChannel = null;
    }

    if (this.connection) {
      await this.connection.close().catch(() => {});
      this.connection = null;
    }
  }
}

module.exports = new RabbitMQService();
