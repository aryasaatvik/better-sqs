/**
 * Better-SQS Handler Builder
 *
 * Creates SQS Lambda handler function with routing logic
 * and type-safe message handling.
 */

import type { SQSEvent, SQSRecord } from "aws-lambda";
import type {
  BetterSQSConfig,
  HandlerConfig,
  HandlerRetryResult,
  MessageMetadata,
  SQSBatchResponse,
  SQSHandlerFunction,
  TopicPayloadMap,
} from "./types";
import { consoleLogger } from "./types";

/**
 * Extract queue name from SQS event source ARN
 *
 * ARN format: arn:aws:sqs:region:account-id:queue-name
 * For FIFO queues: arn:aws:sqs:region:account-id:queue-name.fifo
 */
function extractQueueName(eventSourceArn: string): string {
  const parts = eventSourceArn.split(":");
  const queueNameWithFifo = parts[parts.length - 1] || "unknown";
  // Remove .fifo suffix if present for routing (handlers are registered without .fifo)
  return queueNameWithFifo.replace(/\.fifo$/, "");
}

/**
 * Parse SQS record into message payload and metadata
 */
function parseRecord<T = unknown>(record: SQSRecord): {
  payload: T;
  metadata: MessageMetadata;
  topic: string | null;
} {
  let payload: T;
  try {
    payload = JSON.parse(record.body) as T;
  } catch (error) {
    throw new Error(
      `Failed to parse message body as JSON: ${error instanceof Error ? error.message : String(error)}`,
    );
  }

  // Extract topic from message attributes (preferred for routing)
  const topic = record.messageAttributes?.topic?.stringValue || null;

  const metadata: MessageMetadata = {
    messageId: record.messageId,
    receiveCount: Number.parseInt(
      record.attributes.ApproximateReceiveCount || "1",
      10,
    ),
    queueName: extractQueueName(record.eventSourceARN || ""),
    approximateFirstReceiveTimestamp: record.attributes
      .ApproximateFirstReceiveTimestamp
      ? Number.parseInt(record.attributes.ApproximateFirstReceiveTimestamp, 10)
      : undefined,
  };

  return { payload, metadata, topic };
}

/**
 * Create SQS Lambda handler function
 *
 * Provides type-safe message handling with automatic routing
 *
 * @param handlers - Handler configuration mapping topics to functions
 * @param config - Optional configuration (logger, etc.)
 * @returns Lambda handler function
 *
 * @example
 * ```typescript
 * type Topics = "user-notifications" | "order-processing";
 * type Payloads = {
 *   "user-notifications": { userId: string; type: string };
 *   "order-processing": { orderId: string; action: string };
 * };
 *
 * export const handler = createHandler<Topics, Payloads>({
 *   "user-notifications": async (payload, metadata) => {
 *     // payload is typed as { userId: string; type: string }
 *     await processNotification(payload);
 *   },
 *   "order-processing": async (payload, metadata) => {
 *     // payload is typed as { orderId: string; action: string }
 *     await processOrder(payload);
 *   }
 * });
 * ```
 */
export function createHandler<
  TopicName extends keyof TopicPayloadMap = keyof TopicPayloadMap,
  PayloadMap extends { [key: string]: unknown } = TopicPayloadMap,
>(
  handlers: HandlerConfig<TopicName, PayloadMap>,
  config?: BetterSQSConfig,
): SQSHandlerFunction {
  const logger = config?.logger || consoleLogger;

  return async (event: SQSEvent): Promise<SQSBatchResponse> => {
    logger.info?.(
      `Processing SQS batch: ${event.Records.length} record(s)`,
    );

    const failures: string[] = [];

    // Process all records in parallel
    await Promise.all(
      event.Records.map(async (record) => {
        try {
          const { payload, metadata, topic } = parseRecord(record);

          // Determine routing key: prefer topic from message attributes, fall back to queue name
          const routingKey = (topic || metadata.queueName) as TopicName;

          logger.debug?.(
            `Processing record: ${metadata.messageId} from queue: ${metadata.queueName}` +
            (topic ? ` (topic: ${topic})` : '') +
            ` (attempt ${metadata.receiveCount})`,
          );

          // Route to appropriate handler based on topic (or queue name as fallback)
          const handler = handlers[routingKey];

          if (!handler) {
            const availableHandlers = Object.keys(handlers).join(", ");
            logger.warn?.(
              `No handler found for routing key "${routingKey}". ` +
              `Topic: ${topic || 'none'}, Queue: ${metadata.queueName}. ` +
              `Available handlers: ${availableHandlers}`,
            );
            // Mark as success to prevent infinite retries for unknown queues
            return;
          }

          // Execute handler
          const result = await handler(
            payload as PayloadMap[TopicName],
            metadata,
          );

          // Handle retry result (similar to other queue libraries' retry patterns)
          if (result && typeof result === "object" && "retryAfterSeconds" in result) {
            const retryResult = result as HandlerRetryResult;
            if (retryResult.retryAfterSeconds !== undefined) {
              logger.info?.(
                `Handler requested retry after ${retryResult.retryAfterSeconds}s for message ${metadata.messageId}`,
              );
              // Note: In SQS, we can't directly control retry timing from the handler
              // The message will be retried according to the queue's visibility timeout
              // This is informational for logging/monitoring purposes
              // To actually extend visibility timeout, you'd need to use ChangeMessageVisibility API
              // which is outside the scope of this handler abstraction
            }
          }

          logger.info?.(
            `Record processed successfully: ${metadata.messageId}` +
            (topic ? ` (topic: ${topic})` : '') +
            ` from queue: ${metadata.queueName}`,
          );
        } catch (error) {
          logger.error?.(
            `Failed to process record: ${record.messageId} (attempt ${record.attributes.ApproximateReceiveCount})`,
            error instanceof Error ? error.message : String(error),
          );

          // Add to failures for retry
          failures.push(record.messageId);
        }
      }),
    );

    logger.info?.(
      `SQS batch processing complete: ${event.Records.length - failures.length} succeeded, ${failures.length} failed`,
    );

    // Return batch item failures for partial batch failure handling
    return {
      batchItemFailures: failures.map((id) => ({ itemIdentifier: id })),
    };
  };
}

