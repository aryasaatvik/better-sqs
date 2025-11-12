/**
 * Better-SQS Type Definitions
 *
 * Generic, type-safe queue abstraction for AWS SQS
 */

import type { SQSEvent } from "aws-lambda";

/**
 * Topic-to-payload mapping interface
 * 
 * This interface can be augmented via module augmentation to provide
 * type-safe topic and payload pairs across your application.
 * 
 * @example Module augmentation
 * ```typescript
 * declare module "better-sqs" {
 *   interface TopicPayloadMap {
 *     "user-notifications": { userId: string; type: string };
 *     "order-processing": { orderId: string; action: string };
 *   }
 * }
 * 
 * // Now send() is fully type-safe:
 * await send("user-notifications", {
 *   userId: "123",
 *   type: "welcome"
 * }); // ✅ Type-safe
 * 
 * await send("user-notifications", {
 *   wrong: "field"
 * }); // ❌ TypeScript error
 * ```
 */
export interface TopicPayloadMap {
  // Default empty - users augment this via module declaration merging
  [topic: string]: unknown;
}

/**
 * Logger interface for optional logging
 * Compatible with most logging libraries (pino, winston, console, etc.)
 */
export interface Logger {
  debug?(message: string, ...args: unknown[]): void;
  info?(message: string, ...args: unknown[]): void;
  warn?(message: string, ...args: unknown[]): void;
  error?(message: string, ...args: unknown[]): void;
}

/**
 * Default logger using console
 * Uses console methods for logging when no custom logger is provided
 */
export const consoleLogger: Logger = {
  debug: console.debug.bind(console),
  info: console.info.bind(console),
  warn: console.warn.bind(console),
  error: console.error.bind(console),
};

/**
 * No-op logger for when logging should be disabled
 * Pass this explicitly to disable logging: createClient({ logger: noopLogger })
 */
export const noopLogger: Logger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
};

/**
 * Message metadata provided to handlers
 * Extracted from SQS record attributes
 */
export interface MessageMetadata {
  /**
   * SQS message ID
   */
  messageId: string;

  /**
   * Number of times this message has been received
   */
  receiveCount: number;

  /**
   * Queue name this message came from
   */
  queueName: string;

  /**
   * Approximate first receive timestamp (Unix timestamp in milliseconds)
   */
  approximateFirstReceiveTimestamp?: number;
}

/**
 * Options for sending messages to SQS
 */
export interface SendOptions {
  /**
   * Idempotency key for duplicate prevention (FIFO queues)
   * If not provided and FIFO queue, a default will be generated
   */
  idempotencyKey?: string;

  /**
   * Delay message delivery by N seconds
   * Max: 900 seconds (15 minutes) for standard queues
   * Max: 900 seconds (15 minutes) for FIFO queues
   */
  delaySeconds?: number;

  /**
   * Message group ID for FIFO queues
   * Messages with the same group ID are processed in order
   * Required for FIFO queues
   */
  groupId?: string;

  /**
   * Message deduplication ID for FIFO queues
   * If not provided, idempotencyKey will be used
   * Required for FIFO queues (unless content-based deduplication is enabled)
   */
  deduplicationId?: string;

  /**
   * Custom message attributes to attach to the message
   */
  messageAttributes?: Record<
    string,
    {
      StringValue?: string;
      BinaryValue?: Uint8Array;
      DataType: "String" | "Number" | "Binary";
    }
  >;
}

/**
 * Handler result that can control retry behavior
 * Similar to other queue libraries' retry patterns
 */
export interface HandlerRetryResult {
  /**
   * Time in seconds before the message becomes visible again
   * Used to extend visibility timeout for custom retry logic
   */
  retryAfterSeconds?: number;
}

/**
 * Message handler function type
 * Handlers can return void or a retry result to control visibility timeout
 */
export type MessageHandler<T = unknown> = (
  payload: T,
  metadata: MessageMetadata,
) => Promise<void | HandlerRetryResult>;

/**
 * Handler configuration object
 * Maps queue topic names to handler functions
 * 
 * If TopicPayloadMap is augmented, this will automatically use those types.
 * Otherwise, you can provide explicit types.
 * 
 * @example With module augmentation
 * ```typescript
 * declare module "better-sqs" {
 *   interface TopicPayloadMap {
 *     "user-notifications": { userId: string; type: string };
 *     "order-processing": { orderId: string; action: string };
 *   }
 * }
 * 
 * // Types are automatically inferred from TopicPayloadMap
 * const handlers: HandlerConfig = {
 *   "user-notifications": async (payload, metadata) => {
 *     // payload is typed as { userId: string; type: string }
 *   },
 *   "order-processing": async (payload, metadata) => {
 *     // payload is typed as { orderId: string; action: string }
 *   }
 * };
 * ```
 * 
 * @example Without module augmentation
 * ```typescript
 * type Topics = "user-notifications" | "order-processing";
 * type Payloads = {
 *   "user-notifications": { userId: string; type: string };
 *   "order-processing": { orderId: string; action: string };
 * };
 * 
 * const handlers: HandlerConfig<Topics, Payloads> = {
 *   "user-notifications": async (payload, metadata) => {
 *     // payload is typed as { userId: string; type: string }
 *   },
 *   "order-processing": async (payload, metadata) => {
 *     // payload is typed as { orderId: string; action: string }
 *   }
 * };
 * ```
 */
export type HandlerConfig<
  TopicName extends keyof TopicPayloadMap = keyof TopicPayloadMap,
  PayloadMap extends { [key: string]: unknown } = TopicPayloadMap,
> = {
  [K in TopicName]: MessageHandler<PayloadMap[K]>;
};

/**
 * SQS batch response for partial failure handling
 * Returned by Lambda handlers to indicate which messages failed
 */
export interface SQSBatchResponse {
  batchItemFailures: Array<{ itemIdentifier: string }>;
}

/**
 * Lambda handler function type
 */
export type SQSHandlerFunction = (
  event: SQSEvent,
) => Promise<SQSBatchResponse>;

/**
 * Queue URL resolver function
 * Used to resolve queue URLs from topic names
 */
export type QueueUrlResolver = (topic: string) => string | Promise<string>;

/**
 * Better-SQS configuration options
 */
export interface BetterSQSConfig {
  /**
   * Optional logger instance
   * If not provided, uses no-op logger
   */
  logger?: Logger;

  /**
   * Optional SQS client instance
   * If not provided, creates a new one with default config
   */
  sqsClient?: import("@aws-sdk/client-sqs").SQSClient;

  /**
   * Queue URL resolver function
   * If not provided, uses environment variable lookup
   */
  queueUrlResolver?: QueueUrlResolver;

  /**
   * AWS region for SQS client
   * Defaults to process.env.AWS_REGION or "us-east-1"
   */
  region?: string;
}

/**
 * Better-SQS client instance
 * Created via createClient() factory function
 */
export interface BetterSQSClient {
  /**
   * Send a message to an SQS queue
   *
   * @param topic - Queue topic name (maps to SQS queue name)
   * @param payload - Message payload (any serializable data)
   * @param options - Send options (idempotency, delay, FIFO settings)
   * @returns Promise that resolves with message ID when message is sent
   */
  send<Topic extends keyof TopicPayloadMap>(
    topic: Topic,
    payload: TopicPayloadMap[Topic],
    options?: SendOptions,
  ): Promise<{ messageId: string }>;
}

