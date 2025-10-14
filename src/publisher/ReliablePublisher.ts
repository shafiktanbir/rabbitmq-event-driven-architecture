import { randomUUID } from 'crypto';
import type { ConfirmChannel } from 'amqplib';
import type { ConnectionManager } from '../connection/ConnectionManager.js';
import type { PublishConfirmOptions, PublishResult } from '../types/index.js';

export class ReliablePublisher {
  private connectionManager: ConnectionManager;
  private inFlightPublishes = new Map<string, {
    exchange: string;
    routingKey: string;
    content: Buffer;
    reject: (reason: Error) => void;
  }>();

  private boundOnDisconnect: () => void;

  constructor(connectionManager: ConnectionManager) {
    this.connectionManager = connectionManager;

    this.boundOnDisconnect = () => {
      this.rejectAllInFlight(new Error('Broker disconnected while awaiting publisher confirm'));
    };

    this.connectionManager.on('disconnected', this.boundOnDisconnect);
  }

  public close(): void {
    this.connectionManager.removeListener('disconnected', this.boundOnDisconnect);
    this.rejectAllInFlight(new Error('Publisher closed'));
  }

  public getInFlightCount(): number {
    return this.inFlightPublishes.size;
  }

  public publish<T = unknown>(
    exchange: string,
    routingKey: string,
    payload: T,
    options: PublishConfirmOptions = {}
  ): Promise<PublishResult> {
    const messageId = options.messageId || options.idempotencyKey || randomUUID();
    const timestamp = options.timestamp || Date.now();
    const timeoutMs = options.timeoutMs ?? 10000;

    let contentBuffer: Buffer;
    if (Buffer.isBuffer(payload)) {
      contentBuffer = payload;
    } else if (typeof payload === 'string') {
      contentBuffer = Buffer.from(payload);
    } else {
      contentBuffer = Buffer.from(JSON.stringify(payload));
    }

    const publishOptions: PublishConfirmOptions = {
      persistent: true,
      deliveryMode: 2, // 2 = persistent message (disk storage)
      messageId,
      timestamp,
      contentType: typeof payload === 'object' && !Buffer.isBuffer(payload) ? 'application/json' : 'application/octet-stream',
      ...options,
      headers: {
        ...(options.headers || {}),
        'x-published-at': timestamp,
        'x-idempotency-key': options.idempotencyKey || messageId,
      },
    };

    return new Promise<PublishResult>((resolve, reject) => {
      let timeoutTimer: NodeJS.Timeout | null = null;

      const cleanup = () => {
        if (timeoutTimer) {
          clearTimeout(timeoutTimer);
          timeoutTimer = null;
        }
        this.inFlightPublishes.delete(messageId);
      };

      if (timeoutMs > 0) {
        timeoutTimer = setTimeout(() => {
          cleanup();
          reject(new Error(`Publisher confirm timed out after ${timeoutMs}ms for messageId: ${messageId}`));
        }, timeoutMs);
      }

      this.inFlightPublishes.set(messageId, {
        exchange,
        routingKey,
        content: contentBuffer,
        reject: (err) => {
          cleanup();
          reject(err);
        },
      });

      this.connectionManager.getConfirmChannel()
        .then((channel: ConfirmChannel) => {
          if (!this.inFlightPublishes.has(messageId)) {
            return;
          }

          try {
            const canAcceptMore = channel.publish(
              exchange,
              routingKey,
              contentBuffer,
              publishOptions,
              (err) => {
                cleanup();
                if (err) {
                  return reject(new Error(`Publisher confirm NACKed by broker: ${err.message}`));
                }
                resolve({
                  messageId,
                  confirmed: true,
                  routingKey,
                  exchange,
                  timestamp,
                });
              }
            );

            if (!canAcceptMore) {
              // Flow control backpressure: buffer full, wait for drain event
              channel.once('drain', () => {
                // Channel drained, flow resumed
              });
            }
          } catch (error) {
            cleanup();
            reject(error as Error);
          }
        })
        .catch((err) => {
          cleanup();
          reject(err as Error);
        });
    });
  }

  private rejectAllInFlight(error: Error): void {
    const pending = Array.from(this.inFlightPublishes.values());
    this.inFlightPublishes.clear();
    for (const item of pending) {
      item.reject(error);
    }
  }
}
