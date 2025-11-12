/**
 * Better-SQS
 *
 * A TypeScript library that wraps AWS SQS with a Vercel Queue-like API.
 * Provides a simple, type-safe interface for SQS FIFO queues with built-in handler routing.
 *
 * @example Sending messages (standalone)
 * ```typescript
 * import { send } from "better-sqs";
 *
 * // Simple usage - uses env vars automatically
 * await send("user-notifications", {
 *   userId: "123",
 *   type: "welcome"
 * });
 * ```
 *
 * @example Sending messages (with client instance)
 * ```typescript
 * import { createClient } from "better-sqs";
 *
 * const queue = createClient({
 *   region: "us-west-2",
 *   logger: myLogger,
 * });
 *
 * await queue.send("user-notifications", {
 *   userId: "123",
 *   type: "welcome"
 * });
 * ```
 *
 * @example Creating handler
 * ```typescript
 * import { createHandler } from "better-sqs";
 *
 * type Topics = "user-notifications" | "order-processing";
 * type Payloads = {
 *   "user-notifications": { userId: string; type: string };
 *   "order-processing": { orderId: string; action: string };
 * };
 *
 * export const handler = createHandler<Topics, Payloads>({
 *   "user-notifications": async (payload, metadata) => {
 *     // Process notification...
 *   },
 *   "order-processing": async (payload, metadata) => {
 *     // Process order...
 *   }
 * });
 * ```
 */

// Client
export { createClient, send } from "./client";

// Handler
export { createHandler } from "./handler";

// Types
export type {
  BetterSQSClient,
  BetterSQSConfig,
  HandlerConfig,
  HandlerRetryResult,
  Logger,
  MessageHandler,
  MessageMetadata,
  QueueUrlResolver,
  SendOptions,
  SQSBatchResponse,
  SQSHandlerFunction,
  TopicPayloadMap,
} from "./types";

// Logger utilities
export { consoleLogger, noopLogger } from "./types";

