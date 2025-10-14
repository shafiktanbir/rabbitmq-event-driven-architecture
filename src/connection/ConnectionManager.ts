import { EventEmitter } from 'events';
import amqplib from 'amqplib';
import type { ChannelModel, Channel, ConfirmChannel } from 'amqplib';
import type { ConnectionManagerConfig } from '../types/index.js';

export class ConnectionManager extends EventEmitter {
  private config: Required<ConnectionManagerConfig>;
  private connection: ChannelModel | null = null;
  private defaultChannel: Channel | null = null;
  private defaultConfirmChannel: ConfirmChannel | null = null;
  private reconnectAttempt = 0;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private isExplicitlyClosed = false;
  private isConnecting = false;

  constructor(config: ConnectionManagerConfig = {}) {
    super();
    this.config = {
      url: config.url || process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672',
      heartbeat: config.heartbeat ?? 60,
      initialReconnectDelayMs: config.initialReconnectDelayMs ?? 1000,
      maxReconnectDelayMs: config.maxReconnectDelayMs ?? 30000,
      backoffFactor: config.backoffFactor ?? 2,
      jitter: config.jitter ?? true,
      maxReconnectAttempts: config.maxReconnectAttempts ?? Infinity,
    };
  }

  public isConnected(): boolean {
    return this.connection !== null && !this.isExplicitlyClosed;
  }

  public getConnection(): ChannelModel | null {
    return this.connection;
  }

  public async connect(): Promise<ChannelModel> {
    if (this.connection) {
      return this.connection;
    }

    if (this.isConnecting) {
      return new Promise<ChannelModel>((resolve, reject) => {
        const onConnected = () => {
          this.removeListener('error', onError);
          resolve(this.connection!);
        };
        const onError = (err: Error) => {
          this.removeListener('connected', onConnected);
          reject(err);
        };
        this.once('connected', onConnected);
        this.once('error', onError);
      });
    }

    this.isConnecting = true;
    this.isExplicitlyClosed = false;

    try {
      this.emit('connecting', { attempt: this.reconnectAttempt });
      const connection = await amqplib.connect(this.config.url, {
        heartbeat: this.config.heartbeat,
      });

      this.connection = connection;
      const wasReconnecting = this.reconnectAttempt > 0;
      this.reconnectAttempt = 0;
      this.isConnecting = false;

      this.setupConnectionHandlers(connection);

      if (wasReconnecting) {
        this.emit('reconnected', { connection });
      }
      this.emit('connected', { connection });

      return connection;
    } catch (error) {
      this.isConnecting = false;
      this.connection = null;
      this.emit('error', error as Error);

      if (!this.isExplicitlyClosed) {
        this.scheduleReconnect();
      }
      throw error;
    }
  }

  public async getChannel(prefetch = 10): Promise<Channel> {
    if (!this.isConnected()) {
      await this.connect();
    }

    if (!this.defaultChannel) {
      this.defaultChannel = await this.connection!.createChannel();
      await this.defaultChannel.prefetch(prefetch);
      this.defaultChannel.on('error', (err) => {
        this.emit('channelError', { type: 'standard', error: err });
        this.defaultChannel = null;
      });
      this.defaultChannel.on('close', () => {
        this.defaultChannel = null;
      });
    }

    return this.defaultChannel;
  }

  public async getConfirmChannel(): Promise<ConfirmChannel> {
    if (!this.isConnected()) {
      await this.connect();
    }

    if (!this.defaultConfirmChannel) {
      this.defaultConfirmChannel = await this.connection!.createConfirmChannel();
      this.defaultConfirmChannel.on('error', (err) => {
        this.emit('channelError', { type: 'confirm', error: err });
        this.defaultConfirmChannel = null;
      });
      this.defaultConfirmChannel.on('close', () => {
        this.defaultConfirmChannel = null;
      });
    }

    return this.defaultConfirmChannel;
  }

  public async createChannel(prefetch = 10): Promise<Channel> {
    if (!this.isConnected()) {
      await this.connect();
    }
    const channel = await this.connection!.createChannel();
    await channel.prefetch(prefetch);
    return channel;
  }

  public async createConfirmChannel(): Promise<ConfirmChannel> {
    if (!this.isConnected()) {
      await this.connect();
    }
    return await this.connection!.createConfirmChannel();
  }

  public calculateBackoffDelay(attempt: number): number {
    const { initialReconnectDelayMs, maxReconnectDelayMs, backoffFactor, jitter } = this.config;
    let delay = Math.min(
      initialReconnectDelayMs * Math.pow(backoffFactor, attempt),
      maxReconnectDelayMs
    );

    if (jitter) {
      // ±20% jitter to prevent thundering herd
      const jitterFactor = 0.8 + Math.random() * 0.4;
      delay = Math.round(delay * jitterFactor);
    }

    return delay;
  }

  public scheduleReconnect(): void {
    if (this.isExplicitlyClosed || this.reconnectTimer) {
      return;
    }

    if (this.reconnectAttempt >= this.config.maxReconnectAttempts) {
      this.emit('reconnectFailed', {
        attempts: this.reconnectAttempt,
        maxAttempts: this.config.maxReconnectAttempts,
      });
      return;
    }

    const delay = this.calculateBackoffDelay(this.reconnectAttempt);
    this.reconnectAttempt++;

    this.emit('reconnecting', {
      attempt: this.reconnectAttempt,
      delayMs: delay,
    });

    this.reconnectTimer = setTimeout(async () => {
      this.reconnectTimer = null;
      try {
        await this.connect();
      } catch (err) {
        // Handled in connect() -> calls scheduleReconnect
      }
    }, delay);
  }

  public async close(): Promise<void> {
    this.isExplicitlyClosed = true;

    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    try {
      if (this.defaultChannel) {
        await this.defaultChannel.close().catch(() => {});
        this.defaultChannel = null;
      }
      if (this.defaultConfirmChannel) {
        await this.defaultConfirmChannel.close().catch(() => {});
        this.defaultConfirmChannel = null;
      }
      if (this.connection) {
        await this.connection.close().catch(() => {});
        this.connection = null;
      }
    } finally {
      this.emit('closed');
    }
  }

  private setupConnectionHandlers(connection: ChannelModel): void {
    connection.on('error', (error) => {
      if (this.isExplicitlyClosed) {
        return;
      }
      this.emit('error', error);
      this.cleanupConnectionState();
      this.scheduleReconnect();
    });

    connection.on('close', (reason) => {
      if (this.isExplicitlyClosed) {
        return;
      }
      this.emit('disconnected', { reason });
      this.cleanupConnectionState();
      this.scheduleReconnect();
    });

    connection.on('blocked', (reason) => {
      this.emit('blocked', { reason });
    });

    connection.on('unblocked', () => {
      this.emit('unblocked');
    });
  }

  private cleanupConnectionState(): void {
    this.connection = null;
    this.defaultChannel = null;
    this.defaultConfirmChannel = null;
  }
}
