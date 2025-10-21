import { jest } from '@jest/globals';
import amqplib from 'amqplib';
import { ConnectionManager } from '../src/connection/ConnectionManager.js';
import { MockConnection, MockChannel, MockConfirmChannel } from './mocks/amqplib.mock.js';

describe('ConnectionManager', () => {
  let mockConnection: MockConnection;

  beforeEach(() => {
    jest.clearAllMocks();
    mockConnection = new MockConnection();
    jest.spyOn(amqplib, 'connect').mockImplementation(async () => {
      return mockConnection as any;
    });
  });

  afterEach(async () => {
    jest.restoreAllMocks();
  });

  it('connects to RabbitMQ with default 60s heartbeat and URL', async () => {
    const manager = new ConnectionManager({
      url: 'amqp://guest:guest@localhost:5672',
      heartbeat: 60,
    });

    const conn = await manager.connect();
    expect(amqplib.connect).toHaveBeenCalledWith('amqp://guest:guest@localhost:5672', {
      heartbeat: 60,
    });
    expect(manager.isConnected()).toBe(true);
    expect(conn).toBe(mockConnection);

    await manager.close();
  });

  it('calculates exponential backoff delay with jitter within expected bounds', () => {
    const manager = new ConnectionManager({
      initialReconnectDelayMs: 1000,
      maxReconnectDelayMs: 10000,
      backoffFactor: 2,
      jitter: true,
    });

    // Attempt 0: base 1000ms -> with jitter [800, 1200]
    const delay0 = manager.calculateBackoffDelay(0);
    expect(delay0).toBeGreaterThanOrEqual(800);
    expect(delay0).toBeLessThanOrEqual(1200);

    // Attempt 1: base 2000ms -> with jitter [1600, 2400]
    const delay1 = manager.calculateBackoffDelay(1);
    expect(delay1).toBeGreaterThanOrEqual(1600);
    expect(delay1).toBeLessThanOrEqual(2400);

    // Attempt 5: base 32000ms capped at 10000ms -> with jitter [8000, 12000]
    const delay5 = manager.calculateBackoffDelay(5);
    expect(delay5).toBeGreaterThanOrEqual(8000);
    expect(delay5).toBeLessThanOrEqual(12000);
  });

  it('automatically reconnects when broker connection drops', async () => {
    jest.useFakeTimers();

    const manager = new ConnectionManager({
      initialReconnectDelayMs: 500,
      jitter: false,
    });

    const disconnectedEvents: any[] = [];
    const reconnectingEvents: any[] = [];
    const reconnectedEvents: any[] = [];

    manager.on('disconnected', (e) => disconnectedEvents.push(e));
    manager.on('reconnecting', (e) => reconnectingEvents.push(e));
    manager.on('reconnected', (e) => reconnectedEvents.push(e));

    await manager.connect();
    expect(manager.isConnected()).toBe(true);

    const secondConnection = new MockConnection();
    (amqplib.connect as any).mockImplementation(async () => secondConnection as any);

    // Simulate broker disconnect
    mockConnection.simulateDisconnect();
    expect(disconnectedEvents.length).toBe(1);
    expect(reconnectingEvents.length).toBe(1);
    expect(manager.isConnected()).toBe(false);

    // Fast-forward timer to trigger reconnect
    await jest.advanceTimersByTimeAsync(600);

    expect(reconnectedEvents.length).toBe(1);
    expect(manager.isConnected()).toBe(true);
    expect(manager.getConnection()).toBe(secondConnection);

    await manager.close();
    jest.useRealTimers();
  });

  it('stops reconnecting and emits reconnectFailed after maxReconnectAttempts', async () => {
    jest.useFakeTimers();

    const manager = new ConnectionManager({
      initialReconnectDelayMs: 100,
      maxReconnectAttempts: 2,
      jitter: false,
    });

    const failedEvents: any[] = [];
    manager.on('reconnectFailed', (e) => failedEvents.push(e));
    manager.on('error', () => {}); // swallow expected connection refused errors

    await manager.connect();

    // Mock subsequent connection failures
    (amqplib.connect as any).mockRejectedValue(new Error('Connection refused'));

    // Trigger disconnect
    mockConnection.simulateDisconnect();

    // First reconnect attempt (100ms)
    await jest.advanceTimersByTimeAsync(150);

    // Second reconnect attempt (200ms)
    await jest.advanceTimersByTimeAsync(250);

    // Flush any pending async tasks/timers
    await jest.runOnlyPendingTimersAsync();

    // Max attempts exceeded
    expect(failedEvents.length).toBe(1);
    expect(failedEvents[0].attempts).toBe(2);

    await manager.close();
    jest.useRealTimers();
  });

  it('creates and pools default channels with flow control prefetch', async () => {
    const manager = new ConnectionManager();
    await manager.connect();

    const channel = await manager.getChannel(15);
    const mockChan = (channel as unknown) as MockChannel;
    expect(mockChan.prefetchValue).toBe(15);

    // Calling again returns the pooled channel
    const sameChannel = await manager.getChannel();
    expect(sameChannel).toBe(channel);

    // Confirm channel creation
    const confirmChannel = await manager.getConfirmChannel();
    expect(confirmChannel).toBeDefined();

    await manager.close();
  });

  it('cleans up resources and does not reconnect on explicit close()', async () => {
    const manager = new ConnectionManager();
    await manager.connect();

    await manager.close();
    expect(manager.isConnected()).toBe(false);

    // Simulate disconnect on closed connection
    mockConnection.simulateDisconnect();
    expect(manager.isConnected()).toBe(false);
  });
});
