import { EventEmitter } from 'events';
import type { ChannelModel, Channel, ConfirmChannel, ConsumeMessage, Options } from 'amqplib';

export class MockChannel extends EventEmitter {
  public prefetchValue = 0;
  public acknowledgedMessages: ConsumeMessage[] = [];
  public nackedMessages: { msg: ConsumeMessage; allUpTo?: boolean; requeue?: boolean }[] = [];
  public publishedMessages: {
    exchange: string;
    routingKey: string;
    content: Buffer;
    options?: Options.Publish;
  }[] = [];
  public queues = new Map<string, { options?: Options.AssertQueue; bindings: Set<string> }>();
  public exchanges = new Map<string, { type: string; options?: Options.AssertExchange }>();
  public consumers = new Map<string, (msg: ConsumeMessage | null) => void>();
  public closed = false;

  async prefetch(count: number): Promise<void> {
    this.prefetchValue = count;
  }

  async assertExchange(exchange: string, type: string, options?: Options.AssertExchange): Promise<any> {
    this.exchanges.set(exchange, { type, options });
    return { exchange };
  }

  async assertQueue(queue: string, options?: Options.AssertQueue): Promise<any> {
    if (!this.queues.has(queue)) {
      this.queues.set(queue, { options, bindings: new Set() });
    }
    return { queue, messageCount: 0, consumerCount: 0 };
  }

  async bindQueue(queue: string, source: string, pattern: string): Promise<any> {
    const q = this.queues.get(queue);
    if (q) {
      q.bindings.add(`${source}:${pattern}`);
    }
    return {};
  }

  publish(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options?: Options.Publish,
    callback?: (err: any, ok: any) => void
  ): boolean {
    this.publishedMessages.push({ exchange, routingKey, content, options });
    if (callback) {
      process.nextTick(() => callback(null, {}));
    }
    return true;
  }

  ack(message: ConsumeMessage, _allUpTo?: boolean): void {
    this.acknowledgedMessages.push(message);
  }

  nack(message: ConsumeMessage, allUpTo?: boolean, requeue?: boolean): void {
    this.nackedMessages.push({ msg: message, allUpTo, requeue });
  }

  async consume(
    queue: string,
    onMessage: (msg: ConsumeMessage | null) => void,
    _options?: Options.Consume
  ): Promise<{ consumerTag: string }> {
    const consumerTag = `consumer-${Math.random().toString(36).substring(7)}`;
    this.consumers.set(consumerTag, onMessage);
    return { consumerTag };
  }

  async cancel(consumerTag: string): Promise<any> {
    this.consumers.delete(consumerTag);
    return { consumerTag };
  }

  async close(): Promise<void> {
    this.closed = true;
    this.emit('close');
  }

  // Test helper: simulate broker sending a message to a consumer
  simulateIncomingMessage(consumerTag: string, msg: ConsumeMessage): void {
    const callback = this.consumers.get(consumerTag);
    if (callback) {
      callback(msg);
    }
  }
}

export class MockConfirmChannel extends MockChannel {
  public willNack = false;
  public willBlockFlow = false;
  public willHang = false;

  override publish(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options?: Options.Publish,
    callback?: (err: any, ok: any) => void
  ): boolean {
    this.publishedMessages.push({ exchange, routingKey, content, options });

    if (callback && !this.willHang) {
      process.nextTick(() => {
        if (this.willNack) {
          callback(new Error('Simulated broker NACK'), null);
        } else {
          callback(null, {});
        }
      });
    }

    if (this.willBlockFlow) {
      return false;
    }

    return true;
  }
}

export class MockConnection extends EventEmitter {
  public channels: MockChannel[] = [];
  public confirmChannels: MockConfirmChannel[] = [];
  public closed = false;

  async createChannel(): Promise<Channel> {
    const channel = new MockChannel();
    this.channels.push(channel);
    return channel as unknown as Channel;
  }

  async createConfirmChannel(): Promise<ConfirmChannel> {
    const confirmChannel = new MockConfirmChannel();
    this.confirmChannels.push(confirmChannel);
    return confirmChannel as unknown as ConfirmChannel;
  }

  async close(): Promise<void> {
    this.closed = true;
    this.emit('close', 'Normal shutdown');
  }

  simulateDisconnect(error?: Error): void {
    if (error) {
      this.emit('error', error);
    }
    this.emit('close', 'Simulated disconnect');
  }
}

export function createMockConsumeMessage(
  payload: any,
  options: {
    messageId?: string;
    correlationId?: string;
    headers?: Record<string, any>;
    exchange?: string;
    routingKey?: string;
  } = {}
): ConsumeMessage {
  const content = Buffer.isBuffer(payload)
    ? payload
    : Buffer.from(typeof payload === 'string' ? payload : JSON.stringify(payload));

  return {
    content,
    fields: {
      deliveryTag: 1,
      redelivered: false,
      exchange: options.exchange || 'orders.exchange',
      routingKey: options.routingKey || 'orders.process',
      consumerTag: 'consumer-tag-1',
    },
    properties: {
      contentType: 'application/json',
      contentEncoding: 'utf-8',
      headers: options.headers || {},
      deliveryMode: 2,
      priority: 0,
      correlationId: options.correlationId,
      replyTo: undefined,
      expiration: undefined,
      messageId: options.messageId || 'msg-123',
      timestamp: Date.now(),
      type: undefined,
      userId: undefined,
      appId: 'test-app',
      clusterId: undefined,
    },
  };
}
