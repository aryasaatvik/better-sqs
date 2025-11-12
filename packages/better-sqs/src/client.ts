/**
 * Better-SQS Client
 *
 * Provides a simple `send()` function that mimics Vercel Queue's API
 * but uses AWS SQS under the hood with full type safety and configurability.
 * Uses a factory pattern for serverless-first design with no global state.
 */

import { createHash, randomUUID } from "node:crypto";
import {
  SendMessageCommand,
  type SendMessageCommandInput,
  SQSClient,
  type SQSClientConfig,
} from "@aws-sdk/client-sqs";
import type {
  BetterSQSClient,
  BetterSQSConfig,
  Logger,
  QueueUrlResolver,
  SendOptions,
  TopicPayloadMap,
} from "./types";
import { consoleLogger } from "./types";

/**
 * Default queue URL resolver using environment variables
 * Looks for SQS_<TOPIC>_URL or SQS_<TOPIC_UPPERCASE>_URL
 */
function envQueueUrlResolver(topic: string): string {
  // Try SQS_<TOPIC>_URL first (e.g., SQS_user-notifications_URL)
  const envKey1 = `SQS_${topic.toUpperCase().replace(/-/g, "_")}_URL`;
  const url1 = process.env[envKey1];
  if (url1) {
    return url1;
  }

  // Try SQS_<TOPIC>_URL with dashes preserved
  const envKey2 = `SQS_${topic}_URL`;
  const url2 = process.env[envKey2];
  if (url2) {
    return url2;
  }

  throw new Error(
    `Queue URL not found for topic "${topic}". ` +
      `Expected environment variable: ${envKey1} or ${envKey2}`,
  );
}

/**
 * Generate a deterministic deduplication ID from payload
 * Uses stable stringification (sorted keys) and SHA-256 hashing
 * for better collision resistance than simple hash functions.
 *
 * SQS FIFO queues require MessageDeduplicationId to be max 128 characters.
 * We use SHA-256 (64 hex chars) which is well within the limit.
 */
function generateDeduplicationId(payload: unknown): string {
  // Stable stringify: deterministic JSON with sorted object keys
  // This ensures logically equivalent payloads produce the same hash
  // (e.g., {a:1,b:2} and {b:2,a:1} produce the same string)
  function stableStringify(value: unknown): string {
    if (value === undefined) {
      return "";
    }
    if (value === null) {
      return "null";
    }
    if (typeof value !== "object") {
      return JSON.stringify(value);
    }

    if (Array.isArray(value)) {
      return `[${value.map((item) => stableStringify(item)).join(",")}]`;
    }

    // Sort object keys for deterministic output
    const keys = Object.keys(value).sort();
    const pairs = keys.map(
      (key) => `${JSON.stringify(key)}:${stableStringify((value as Record<string, unknown>)[key])}`,
    );
    return `{${pairs.join(",")}}`;
  }

  const stableStr = stableStringify(payload);

  // If payload is empty/undefined, use a UUID-based fallback
  if (!stableStr) {
    // Remove dashes to keep it shorter (32 chars)
    return randomUUID().replace(/-/g, "");
  }

  // Use SHA-256 for better collision resistance
  // SHA-256 produces 64 hex characters, well within SQS's 128-char limit
  const hash = createHash("sha256").update(stableStr).digest("hex");
  return hash;
}

/**
 * Core send message logic
 * Extracted for reuse by both client instances and standalone send()
 */
async function sendMessage<Topic extends keyof TopicPayloadMap>(
  topic: Topic,
  payload: TopicPayloadMap[Topic],
  options: SendOptions | undefined,
  sqsClient: SQSClient,
  resolveQueueUrl: QueueUrlResolver,
  logger: Logger,
): Promise<{ messageId: string }> {
  // Resolve queue URL (topic is keyof TopicPayloadMap, convert to string)
  const queueUrl = await Promise.resolve(resolveQueueUrl(String(topic)));

  // Determine if this is a FIFO queue (ends with .fifo)
  const isFIFO = queueUrl.endsWith(".fifo");

  // Build message body
  const messageBody = JSON.stringify(payload);

  // Build SQS message parameters
  const params: SendMessageCommandInput = {
    QueueUrl: queueUrl,
    MessageBody: messageBody,
  };

  // Add delay if specified
  if (options?.delaySeconds !== undefined) {
    if (options.delaySeconds < 0 || options.delaySeconds > 900) {
      throw new Error(
        "delaySeconds must be between 0 and 900 (15 minutes)",
      );
    }
    params.DelaySeconds = options.delaySeconds;
  }

  // FIFO queue requirements
  if (isFIFO) {
    // MessageGroupId is required for FIFO queues
    if (!options?.groupId) {
      throw new Error(
        `MessageGroupId is required for FIFO queues. ` +
          `Provide groupId in options.`,
      );
    }
    params.MessageGroupId = options.groupId;

    // MessageDeduplicationId is required unless content-based deduplication is enabled
    // Use provided deduplicationId, idempotencyKey, or generate from payload
    params.MessageDeduplicationId =
      options.deduplicationId ||
      options.idempotencyKey ||
      generateDeduplicationId(payload);
  }

  // Add message attributes
  const messageAttributes: SendMessageCommandInput["MessageAttributes"] = {
    topic: {
      StringValue: String(topic),
      DataType: "String",
    },
    ...options?.messageAttributes,
  };

  // Add idempotency key as attribute if provided (for non-FIFO queues)
  if (options?.idempotencyKey && !isFIFO) {
    messageAttributes.idempotencyKey = {
      StringValue: options.idempotencyKey,
      DataType: "String",
    };
  }

  params.MessageAttributes = messageAttributes;

  try {
    const command = new SendMessageCommand(params);
    const result = await sqsClient.send(command);

    logger.info?.(
      `Message sent to queue: ${topic} (SQS MessageId: ${result.MessageId})`,
    );

    return { messageId: result.MessageId || "" };
  } catch (error) {
    logger.error?.(
      `Failed to send message to queue: ${topic}`,
      error instanceof Error ? error.message : String(error),
    );
    throw error;
  }
}

/**
 * Create a configured Better-SQS client instance
 *
 * Uses a factory pattern for serverless-first design with no global state.
 * Each client instance is independent and stateless.
 *
 * @param config - Optional configuration (uses defaults/env vars if not provided)
 * @returns Configured client instance with send() method
 *
 * @example
 * ```typescript
 * import { createClient } from "better-sqs";
 * import pino from "pino";
 *
 * // Create client with custom config
 * const queue = createClient({
 *   logger: pino(),
 *   region: "us-west-2",
 *   queueUrlResolver: (topic) => `https://sqs.us-west-2.amazonaws.com/123456789/${topic}`,
 * });
 *
 * await queue.send("my-topic", { data: "example" });
 * ```
 *
 * @example
 * ```typescript
 * // Create client with defaults (uses env vars)
 * const queue = createClient();
 * await queue.send("my-topic", { data: "example" });
 * ```
 */
export function createClient(config?: BetterSQSConfig): BetterSQSClient {
  // Resolve SQS client
  const sqsClient =
    config?.sqsClient ||
    new SQSClient({
      region: config?.region || process.env.AWS_REGION || "us-east-1",
    });

  // Resolve queue URL resolver
  const resolveQueueUrl: QueueUrlResolver = config?.queueUrlResolver || envQueueUrlResolver;

  // Resolve logger
  const logger = config?.logger || consoleLogger;

  // Return client instance
  return {
    send: async <Topic extends keyof TopicPayloadMap>(
      topic: Topic,
      payload: TopicPayloadMap[Topic],
      options?: SendOptions,
    ) => {
      return sendMessage(topic, payload, options, sqsClient, resolveQueueUrl, logger);
    },
  };
}

/**
 * Standalone send function - convenience wrapper around createClient()
 * Uses environment variables and defaults - no configuration needed
 *
 * @param topic - Queue topic name (maps to SQS queue name)
 * @param payload - Message payload (any serializable data)
 * @param options - Send options (idempotency, delay, FIFO settings)
 * @param config - Optional per-call configuration override
 * @returns Promise that resolves with message ID when message is sent
 *
 * @example
 * ```typescript
 * import { send } from "better-sqs";
 *
 * // Simple send - uses env vars automatically
 * await send("user-notifications", {
 *   userId: "123",
 *   type: "welcome"
 * });
 *
 * // With options
 * await send("order-processing", { orderId: "456" }, {
 *   idempotencyKey: "unique-key",
 *   delaySeconds: 60,
 *   groupId: "order-456", // For FIFO queues
 * });
 *
 * // With per-call config override
 * await send("topic", payload, options, {
 *   logger: myLogger,
 *   region: "us-west-2",
 * });
 * ```
 */
export async function send<
  Topic extends keyof TopicPayloadMap = keyof TopicPayloadMap,
>(
  topic: Topic,
  payload: TopicPayloadMap[Topic],
  options?: SendOptions,
  config?: BetterSQSConfig,
): Promise<{ messageId: string }> {
  // Create a temporary client with defaults + optional config
  const client = createClient(config);
  return client.send(topic, payload, options);
}
