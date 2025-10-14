import type { Options, Message, ConsumeMessage, Channel, ConfirmChannel, ChannelModel } from 'amqplib';

export interface ConnectionManagerConfig {
  url?: string;
  heartbeat?: number;
  initialReconnectDelayMs?: number;
  maxReconnectDelayMs?: number;
  backoffFactor?: number;
  jitter?: boolean;
  maxReconnectAttempts?: number;
}

export interface PublishConfirmOptions extends Options.Publish {
  timeoutMs?: number;
  idempotencyKey?: string;
  retryOnUnconfirmed?: boolean;
}

export interface PublishResult {
  messageId: string;
  confirmed: boolean;
  routingKey: string;
  exchange: string;
  timestamp: number;
}

export interface RetryConfig {
  mainExchange: string;
  mainQueue: string;
  mainRoutingKey: string;
  retryExchange: string;
  retry10sQueue: string;
  retry60sQueue: string;
  poisonDlqQueue: string;
  maxRetries: number;
}

export interface MessageMetadata {
  messageId: string;
  correlationId?: string;
  retryCount: number;
  firstFailedAt?: number;
  lastFailedAt?: number;
  errorReason?: string;
  poisoned?: boolean;
}

export type MessageProcessingStatus = 'PENDING' | 'COMPLETED' | 'FAILED';

export interface IdempotencyRecord {
  key: string;
  status: MessageProcessingStatus;
  createdAt: number;
  expiresAt: number;
  result?: unknown;
  error?: string;
}

export interface IdempotencyStore {
  get(key: string): Promise<IdempotencyRecord | null>;
  setIfAbsent(key: string, ttlMs: number): Promise<boolean>;
  markCompleted(key: string, result?: unknown): Promise<void>;
  markFailed(key: string, error?: string): Promise<void>;
  delete(key: string): Promise<void>;
  clear(): Promise<void>;
}

export interface IdempotencyOptions {
  store?: IdempotencyStore;
  ttlMs?: number;
  keyExtractor?: (msg: ConsumeMessage) => string | undefined;
  onDuplicate?: (msg: ConsumeMessage, record: IdempotencyRecord) => Promise<void> | void;
}

export type MessageHandler<T = unknown> = (
  content: T,
  rawMessage: ConsumeMessage,
  metadata: MessageMetadata
) => Promise<unknown> | unknown;
