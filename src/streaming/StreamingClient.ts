/**
 * Streaming Client for Platform Services
 * Wraps Redis Streams for event publishing and consumption
 */
import Redis from 'ioredis';

export interface StreamEvent {
  event: string;
  timestamp: string;
  source: string;
  correlation_id?: string;
  [key: string]: any;
}

export class StreamingClient {
  private redis: Redis;
  private connected: boolean = false;
  private static readonly ACK_MAX_ATTEMPTS = 3;
  private static readonly ACK_RETRY_BASE_DELAY_MS = 50;

  constructor(
    redisHost: string = process.env.REDIS_HOST || 'redis',
    redisPort: number = parseInt(process.env.REDIS_PORT || '6379'),
    redisPassword: string = process.env.REDIS_PASSWORD || 'redispassword'
  ) {
    this.redis = new Redis({
      host: redisHost,
      port: redisPort,
      password: redisPassword,
      retryStrategy: (times) => {
        const delay = Math.min(times * 50, 2000);
        return delay;
      },
      maxRetriesPerRequest: 3,
    });

    this.redis.on('connect', () => {
      this.connected = true;
      console.log('[StreamingClient] Connected to Redis');
    });

    this.redis.on('error', (err) => {
      console.error('[StreamingClient] Redis error:', err);
      this.connected = false;
    });
  }

  async connect(): Promise<void> {
    if (this.connected) return;
    
    try {
      await this.redis.ping();
      this.connected = true;
    } catch (error) {
      console.error('[StreamingClient] Failed to connect:', error);
      throw error;
    }
  }

  async disconnect(): Promise<void> {
    await this.redis.quit();
    this.connected = false;
  }

  private async safeAck(streamName: string, consumerGroup: string, msgId: string): Promise<void> {
    for (let attempt = 1; attempt <= StreamingClient.ACK_MAX_ATTEMPTS; attempt += 1) {
      try {
        const acked = await this.redis.xack(streamName, consumerGroup, msgId);

        if (acked === 0) {
          console.warn('[StreamingClient] XACK no-op (not pending/already acknowledged)', {
            streamName,
            consumerGroup,
            msgId,
          });
        }

        return;
      } catch (error) {
        const retryable = this.isRetryableAckError(error);
        const nonRetryable = this.isNonRetryableAckError(error);
        const exhausted = attempt >= StreamingClient.ACK_MAX_ATTEMPTS;

        if (nonRetryable || !retryable || exhausted) {
          const reason = nonRetryable ? 'non-retryable' : exhausted ? 'retry-exhausted' : 'non-retryable';
          console.error(`[StreamingClient] XACK failed (${reason})`, {
            streamName,
            consumerGroup,
            msgId,
            attempt,
            error: this.formatError(error),
          });
          return;
        }

        const delayMs = StreamingClient.ACK_RETRY_BASE_DELAY_MS * attempt;
        console.warn('[StreamingClient] XACK transient failure, retrying', {
          streamName,
          consumerGroup,
          msgId,
          attempt,
          delayMs,
          error: this.formatError(error),
        });
        await this.sleep(delayMs);
      }
    }
  }

  private isRetryableAckError(error: unknown): boolean {
    const code = this.getErrorCode(error);
    const message = this.formatError(error).toUpperCase();

    if (code && ['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE', 'ENOTFOUND', 'NR_CLOSED'].includes(code)) {
      return true;
    }

    return (
      message.includes('CONNECTION IS CLOSED') ||
      message.includes('TRYAGAIN') ||
      message.includes('CLUSTERDOWN') ||
      message.includes('LOADING') ||
      message.includes('TIMEOUT') ||
      message.includes('READONLY')
    );
  }

  private isNonRetryableAckError(error: unknown): boolean {
    const message = this.formatError(error).toUpperCase();
    return message.includes('NOGROUP') || message.includes('WRONGTYPE');
  }

  private formatError(error: unknown): string {
    if (error instanceof Error) {
      return error.message;
    }
    return String(error);
  }

  private getErrorCode(error: unknown): string | null {
    if (
      typeof error === 'object' &&
      error !== null &&
      'code' in error &&
      typeof (error as { code: unknown }).code === 'string'
    ) {
      return ((error as { code: string }).code || '').toUpperCase();
    }
    return null;
  }

  private async sleep(ms: number): Promise<void> {
    await new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Publish event to stream
   */
  async publish(
    streamName: string,
    event: StreamEvent,
    maxLength: number = 10000
  ): Promise<string> {
    if (!this.connected) {
      await this.connect();
    }

    try {
      // Ensure stream exists and set max length
      const messageId = await this.redis.xadd(
        streamName,
        'MAXLEN',
        '~',
        maxLength.toString(),
        '*',
        ...this.flattenEvent(event)
      );

      return messageId as string;
    } catch (error) {
      console.error(`[StreamingClient] Failed to publish to ${streamName}:`, error);
      throw error;
    }
  }

  /**
   * Subscribe to stream with callback
   */
  async subscribe(
    streamName: string,
    callback: (event: StreamEvent) => Promise<void> | void,
    options: {
      consumerGroup?: string;
      consumerName?: string;
      lastId?: string;
      blockMs?: number;
    } = {}
  ): Promise<void> {
    if (!this.connected) {
      await this.connect();
    }

    const {
      consumerGroup,
      consumerName,
      lastId = '0',
      blockMs = 1000,
    } = options;

    // Create consumer group if provided
    if (consumerGroup && consumerName) {
      try {
        await this.redis.xgroup('CREATE', streamName, consumerGroup, '0', 'MKSTREAM');
      } catch (error: any) {
        // Group already exists is fine
        if (!error.message?.includes('BUSYGROUP')) {
          console.warn(`[StreamingClient] Failed to create consumer group:`, error);
        }
      }
    }

    // Start consuming
    while (true) {
      try {
        let messages: any[];

        if (consumerGroup && consumerName) {
          // Read from consumer group
          messages = await this.redis.xreadgroup(
            'GROUP',
            consumerGroup,
            consumerName,
            'COUNT',
            '10',
            'BLOCK',
            blockMs.toString(),
            'STREAMS',
            streamName,
            '>'
          );
        } else {
          // Direct read
          const readResult = await this.redis.xread(
            'COUNT',
            '10',
            'BLOCK',
            blockMs.toString(),
            'STREAMS',
            streamName,
            lastId
          );
          messages = readResult || [];
        }

        if (messages && messages.length > 0) {
          const streamData = messages[0];
          if (streamData && streamData[1]) {
            const streamMessages = streamData[1] as any[];

            for (const [msgId, data] of streamMessages) {
              try {
                const event = this.unflattenEvent(data);
                await callback(event);
              } catch (error) {
                console.error(`[StreamingClient] Callback error:`, error);
              }

              // ACK outside try-catch: a callback error must not skip the ACK
              if (consumerGroup && consumerName) {
                await this.safeAck(streamName, consumerGroup, msgId);
              }
            }
          }
        }
      } catch (error) {
        console.error(`[StreamingClient] Subscription error:`, error);
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }
  }

  /**
   * Flatten event object for Redis
   */
  private flattenEvent(event: StreamEvent): string[] {
    const flat: string[] = [];
    for (const [key, value] of Object.entries(event)) {
      flat.push(key);
      flat.push(typeof value === 'object' ? JSON.stringify(value) : String(value));
    }
    return flat;
  }

  /**
   * Unflatten Redis data to event object
   */
  private unflattenEvent(data: any[]): StreamEvent {
    const event: any = {};
    for (let i = 0; i < data.length; i += 2) {
      const key = data[i];
      let value = data[i + 1];
      
      // Try to parse JSON
      try {
        value = JSON.parse(value);
      } catch {
        // Not JSON, keep as string
      }
      
      event[key] = value;
    }
    return event as StreamEvent;
  }
}

/**
 * Stream topic constants
 */
export const StreamTopics = {
  MODEL_EVENTS: 'model-events',
  INFERENCE_EVENTS: 'inference-events',
  PLATFORM_EVENTS: 'platform-events',
  AGI_DECISIONS: 'agi-decisions',
  TRAINING_EVENTS: 'training-events',
} as const;

