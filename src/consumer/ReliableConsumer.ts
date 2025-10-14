import { EventEmitter } from 'events';
import type { Channel, ConsumeMessage } from 'amqplib';
import type { ConnectionManager } from '../connection/ConnectionManager.js';
import type { MessageHandler, RetryConfig, MessageMetadata } from '../types/index.js';
import { RetryManager, type RetryResult } from '../retry/RetryManager.js';
import { DEFAULT_RETRY_CONFIG } from '../retry/RetryTopology.js';

export interface ReliableConsumerOptions {
  queue?: string;
  prefetch?: number;
  retryConfig?: Partial<RetryConfig>;
  autoAck?: boolean;
}

export class ReliableConsumer extends EventEmitter {
  private connectionManager: ConnectionManager;
  private channel: Channel | null = null;
  private queue: string;
  private prefetch: number;
  private retryManager: RetryManager;
  private consumerTag: string | null = null;
  private isRunning = false;
  private handler: MessageHandler | null = null;

  constructor(
    connectionManager: ConnectionManager,
    options: ReliableConsumerOptions = {}
  ) {
    super();
    this.connectionManager = connectionManager;
    this.queue = options.queue || DEFAULT_RETRY_CONFIG.mainQueue;
    this.prefetch = options.prefetch ?? 10;
    this.retryManager = new RetryManager(options.retryConfig);

    // Auto-resubscribe when connection is restored
    this.connectionManager.on('reconnected', async () => {
      if (this.isRunning && this.handler) {
        this.emit('resubscribing');
        try {
          await this.setupChannelAndConsume();
        } catch (err) {
          this.emit('error', err);
        }
      }
    });
  }

  public isConsuming(): boolean {
    return this.isRunning && this.channel !== null;
  }

  public async consume<T = unknown>(handler: MessageHandler<T>): Promise<string> {
    this.handler = handler as MessageHandler;
    this.isRunning = true;
    return await this.setupChannelAndConsume();
  }

  public async stop(): Promise<void> {
    this.isRunning = false;
    if (this.channel && this.consumerTag) {
      try {
        await this.channel.cancel(this.consumerTag);
      } catch (err) {
        // Channel may already be closed
      }
      this.consumerTag = null;
    }
  }

  private async setupChannelAndConsume(): Promise<string> {
    this.channel = await this.connectionManager.createChannel(this.prefetch);

    this.channel.on('error', (err) => {
      this.emit('channelError', err);
      this.channel = null;
    });

    this.channel.on('close', () => {
      this.channel = null;
    });

    const { consumerTag } = await this.channel.consume(
      this.queue,
      async (msg: ConsumeMessage | null) => {
        if (!msg) {
          return;
        }

        const metadata = this.retryManager.extractMetadata(msg);
        let parsedContent: unknown;
        try {
          const raw = msg.content.toString();
          parsedContent = JSON.parse(raw);
        } catch {
          parsedContent = msg.content;
        }

        try {
          const result = await this.handler!(parsedContent, msg, metadata);
          if (this.channel) {
            this.channel.ack(msg);
          }
          this.emit('processed', { messageId: metadata.messageId, result });
        } catch (error) {
          this.emit('processingFailed', {
            messageId: metadata.messageId,
            error: error as Error,
            metadata,
          });

          if (this.channel) {
            try {
              const retryResult: RetryResult = await this.retryManager.handleFailure(
                this.channel,
                msg,
                error as Error
              );
              this.emit('failureHandled', {
                messageId: metadata.messageId,
                ...retryResult,
              });
            } catch (retryError) {
              this.emit('error', retryError);
              // Fallback: nack without requeue to trigger DLX if broker configured
              if (this.channel) {
                this.channel.nack(msg, false, false);
              }
            }
          }
        }
      },
      { noAck: false }
    );

    this.consumerTag = consumerTag;
    this.emit('subscribed', { queue: this.queue, consumerTag });
    return consumerTag;
  }
}
