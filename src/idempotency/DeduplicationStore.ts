import type { IdempotencyRecord, IdempotencyStore, MessageProcessingStatus } from '../types/index.js';

export interface DeduplicationStoreOptions {
  defaultTtlMs?: number;
  maxEntries?: number;
  cleanupIntervalMs?: number;
}

export class InMemoryDeduplicationStore implements IdempotencyStore {
  private records = new Map<string, IdempotencyRecord>();
  private defaultTtlMs: number;
  private maxEntries: number;
  private cleanupTimer: NodeJS.Timeout | null = null;

  constructor(options: DeduplicationStoreOptions = {}) {
    this.defaultTtlMs = options.defaultTtlMs ?? 24 * 60 * 60 * 1000; // 24 hours
    this.maxEntries = options.maxEntries ?? 50000;
    const cleanupInterval = options.cleanupIntervalMs ?? 60000; // 1 minute

    this.cleanupTimer = setInterval(() => {
      this.evictExpired();
    }, cleanupInterval);

    // Unref cleanup timer so it does not block Node.js process exit
    if (this.cleanupTimer.unref) {
      this.cleanupTimer.unref();
    }
  }

  public async get(key: string): Promise<IdempotencyRecord | null> {
    const record = this.records.get(key);
    if (!record) {
      return null;
    }

    if (Date.now() > record.expiresAt) {
      this.records.delete(key);
      return null;
    }

    return record;
  }

  public async setIfAbsent(key: string, ttlMs?: number): Promise<boolean> {
    const now = Date.now();
    const existing = await this.get(key);

    if (existing) {
      return false;
    }

    // Enforce LRU/size limits
    if (this.records.size >= this.maxEntries) {
      this.evictOldest();
    }

    const effectiveTtl = ttlMs ?? this.defaultTtlMs;
    const record: IdempotencyRecord = {
      key,
      status: 'PENDING',
      createdAt: now,
      expiresAt: now + effectiveTtl,
    };

    this.records.set(key, record);
    return true;
  }

  public async markCompleted(key: string, result?: unknown): Promise<void> {
    const record = this.records.get(key);
    if (record) {
      record.status = 'COMPLETED';
      record.result = result;
    }
  }

  public async markFailed(key: string, error?: string): Promise<void> {
    const record = this.records.get(key);
    if (record) {
      record.status = 'FAILED';
      record.error = error;
      // Allow retry by deleting or keeping as FAILED
      this.records.delete(key);
    }
  }

  public async delete(key: string): Promise<void> {
    this.records.delete(key);
  }

  public async clear(): Promise<void> {
    this.records.clear();
  }

  public size(): number {
    return this.records.size;
  }

  public destroy(): void {
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = null;
    }
    this.records.clear();
  }

  private evictExpired(): void {
    const now = Date.now();
    for (const [key, record] of this.records.entries()) {
      if (now > record.expiresAt) {
        this.records.delete(key);
      }
    }
  }

  private evictOldest(): void {
    // Delete first entry in iteration order (oldest insertion)
    const firstKey = this.records.keys().next().value;
    if (firstKey) {
      this.records.delete(firstKey);
    }
  }
}
