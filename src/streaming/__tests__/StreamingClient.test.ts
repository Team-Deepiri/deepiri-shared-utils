import { StreamingClient, StreamTopics } from '../StreamingClient';

const mockXack = jest.fn();
const mockOn = jest.fn();
const mockPing = jest.fn().mockResolvedValue('PONG');
const mockQuit = jest.fn().mockResolvedValue('OK');

jest.mock('ioredis', () =>
  jest.fn().mockImplementation(() => ({
    on: mockOn,
    ping: mockPing,
    quit: mockQuit,
    xack: mockXack,
  }))
);

describe('StreamingClient safeAck', () => {
  let warnSpy: jest.SpyInstance;
  let errorSpy: jest.SpyInstance;

  beforeEach(() => {
    jest.clearAllMocks();
    warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => undefined);
    errorSpy = jest.spyOn(console, 'error').mockImplementation(() => undefined);
  });

  afterEach(() => {
    warnSpy.mockRestore();
    errorSpy.mockRestore();
  });

  it('acknowledges successfully without retries', async () => {
    mockXack.mockResolvedValue(1);
    const client = new StreamingClient('localhost', 6379, '');
    const sleepSpy = jest.spyOn(client as any, 'sleep').mockResolvedValue(undefined);

    await (client as any).safeAck('stream-a', 'group-a', '1-0');

    expect(mockXack).toHaveBeenCalledTimes(1);
    expect(mockXack).toHaveBeenCalledWith('stream-a', 'group-a', '1-0');
    expect(sleepSpy).not.toHaveBeenCalled();
    expect(errorSpy).not.toHaveBeenCalled();
  });

  it('logs a warning when xack returns 0', async () => {
    mockXack.mockResolvedValue(0);
    const client = new StreamingClient('localhost', 6379, '');

    await (client as any).safeAck('stream-a', 'group-a', '1-0');

    expect(mockXack).toHaveBeenCalledTimes(1);
    expect(warnSpy).toHaveBeenCalledWith(
      '[StreamingClient] XACK no-op (not pending/already acknowledged)',
      expect.objectContaining({
        streamName: 'stream-a',
        consumerGroup: 'group-a',
        msgId: '1-0',
        rootCause: 'message-not-pending',
      })
    );
  });

  it('retries transient xack failures and then succeeds', async () => {
    const transientError = Object.assign(new Error('Connection is closed.'), { code: 'ECONNRESET' });
    mockXack.mockRejectedValueOnce(transientError).mockResolvedValueOnce(1);

    const client = new StreamingClient('localhost', 6379, '');
    const sleepSpy = jest.spyOn(client as any, 'sleep').mockResolvedValue(undefined);

    await (client as any).safeAck('stream-a', 'group-a', '1-0');

    expect(mockXack).toHaveBeenCalledTimes(2);
    expect(sleepSpy).toHaveBeenCalledWith(50);
    expect(warnSpy).toHaveBeenCalledWith(
      '[StreamingClient] XACK transient failure, retrying',
      expect.objectContaining({
        streamName: 'stream-a',
        consumerGroup: 'group-a',
        msgId: '1-0',
        attempt: 1,
        delayMs: 50,
        rootCause: 'redis-transport-econnreset',
      })
    );
    expect(errorSpy).not.toHaveBeenCalled();
  });

  it('does not retry non-retryable xack failures', async () => {
    mockXack.mockRejectedValue(new Error("NOGROUP No such key 'stream-a' or consumer group 'group-a'"));
    const client = new StreamingClient('localhost', 6379, '');
    const sleepSpy = jest.spyOn(client as any, 'sleep').mockResolvedValue(undefined);

    await (client as any).safeAck('stream-a', 'group-a', '1-0');

    expect(mockXack).toHaveBeenCalledTimes(1);
    expect(sleepSpy).not.toHaveBeenCalled();
    expect(errorSpy).toHaveBeenCalledWith(
      '[StreamingClient] XACK failed (non-retryable)',
      expect.objectContaining({
        streamName: 'stream-a',
        consumerGroup: 'group-a',
        msgId: '1-0',
        attempt: 1,
        rootCause: 'missing-consumer-group-or-stream',
      })
    );
  });

  it('classifies wrong Redis key type as a setup bug', async () => {
    mockXack.mockRejectedValue(new Error('WRONGTYPE Operation against a key holding the wrong kind of value'));
    const client = new StreamingClient('localhost', 6379, '');
    const sleepSpy = jest.spyOn(client as any, 'sleep').mockResolvedValue(undefined);

    await (client as any).safeAck('stream-a', 'group-a', '1-0');

    expect(mockXack).toHaveBeenCalledTimes(1);
    expect(sleepSpy).not.toHaveBeenCalled();
    expect(errorSpy).toHaveBeenCalledWith(
      '[StreamingClient] XACK failed (non-retryable)',
      expect.objectContaining({
        streamName: 'stream-a',
        consumerGroup: 'group-a',
        msgId: '1-0',
        attempt: 1,
        rootCause: 'stream-key-has-wrong-redis-type',
      })
    );
  });

  it('classifies Redis server state failures as retryable root causes', async () => {
    mockXack.mockRejectedValueOnce(new Error('LOADING Redis is loading the dataset in memory')).mockResolvedValueOnce(1);
    const client = new StreamingClient('localhost', 6379, '');
    jest.spyOn(client as any, 'sleep').mockResolvedValue(undefined);

    await (client as any).safeAck('stream-a', 'group-a', '1-0');

    expect(mockXack).toHaveBeenCalledTimes(2);
    expect(warnSpy).toHaveBeenCalledWith(
      '[StreamingClient] XACK transient failure, retrying',
      expect.objectContaining({
        rootCause: 'redis-loading-dataset',
      })
    );
    expect(errorSpy).not.toHaveBeenCalled();
  });

  it('stops retrying after max attempts', async () => {
    const transientError = Object.assign(new Error('Connection is closed.'), { code: 'ECONNRESET' });
    mockXack.mockRejectedValue(transientError);

    const client = new StreamingClient('localhost', 6379, '');
    const sleepSpy = jest.spyOn(client as any, 'sleep').mockResolvedValue(undefined);

    await (client as any).safeAck('stream-a', 'group-a', '1-0');

    expect(mockXack).toHaveBeenCalledTimes(3);
    expect(sleepSpy).toHaveBeenCalledTimes(2);
    expect(errorSpy).toHaveBeenCalledWith(
      '[StreamingClient] XACK failed (retry-exhausted)',
      expect.objectContaining({
        streamName: 'stream-a',
        consumerGroup: 'group-a',
        msgId: '1-0',
        attempt: 3,
        rootCause: 'redis-transport-econnreset',
      })
    );
  });
});

describe('StreamTopics document routing constants', () => {
  it('defines document.* stream names', () => {
    expect(StreamTopics.DOCUMENT_VECTORIZE).toBe('document.vectorize');
    expect(StreamTopics.DOCUMENT_TRAINING).toBe('document.training');
    expect(StreamTopics.DOCUMENT_STRUCTURED).toBe('document.structured');
    expect(StreamTopics.DOCUMENT_ARTIFACTS).toBe('document.artifacts');
  });
});
