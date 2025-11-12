/**
 * Basic Usage Example
 *
 * Demonstrates simple send and receive patterns with Better-SQS
 * using explicit type definitions.
 *
 * For a more advanced approach using module augmentation (which provides
 * automatic type inference across your application), see module-augmentation.ts
 */

import { send, createClient, createHandler } from "better-sqs";
import type { SQSEvent } from "aws-lambda";

// Define your topics and payload types
type Topics = "user-notifications" | "order-processing";
type Payloads = {
  "user-notifications": { userId: string; type: string };
  "order-processing": { orderId: string; action: string };
};

// Example 1: Standalone send() - Simple usage with env vars
// Use this pattern when you want the simplest API and rely on environment variables
// for configuration (e.g., SQS_USER_NOTIFICATIONS_URL, SQS_ORDER_PROCESSING_URL)
async function sendMessagesStandalone() {
  // Simple send - uses env vars automatically
  await send(
    "user-notifications",
    {
      userId: "123",
      type: "welcome",
    } satisfies Payloads["user-notifications"],
  );

  // With options - types are inferred automatically
  await send(
    "order-processing",
    {
      orderId: "456",
      action: "create",
    } satisfies Payloads["order-processing"],
    {
      idempotencyKey: "order-456-create",
      delaySeconds: 30,
      groupId: "order-456", // Required for FIFO queues
    },
  );

  // With per-call config override
  await send(
    "user-notifications",
    { userId: "789", type: "reminder" } satisfies Payloads["user-notifications"],
    undefined,
    {
      // Override config for this specific call
      region: "us-west-2",
    },
  );
}

// Example 2: createClient() - Explicit client instance
// Use this pattern when you need explicit configuration, want to reuse a client,
// or need multiple clients with different configurations
async function sendMessagesWithClient() {
  // Create a configured client instance
  const queue = createClient({
    // Optional: provide custom SQS client
    // sqsClient: mySQSClient,
    
    // Optional: provide custom logger
    // logger: myLogger,
    
    // Optional: provide custom queue URL resolver
    // queueUrlResolver: (topic) => `https://sqs.us-west-2.amazonaws.com/123456789/${topic}`,
    
    // Optional: specify region (defaults to AWS_REGION env var or "us-east-1")
    region: "us-west-2",
  });

  // Use the client instance
  await queue.send(
    "user-notifications",
    {
      userId: "123",
      type: "welcome",
    } satisfies Payloads["user-notifications"],
  );

  await queue.send(
    "order-processing",
    {
      orderId: "456",
      action: "create",
    } satisfies Payloads["order-processing"],
    {
      idempotencyKey: "order-456-create",
      delaySeconds: 30,
      groupId: "order-456",
    },
  );
}

// Example 3: createClient() with defaults
// Even simpler - uses env vars automatically, but gives you a reusable client
async function sendMessagesWithDefaultClient() {
  const queue = createClient(); // Uses env vars and defaults
  
  await queue.send(
    "user-notifications",
    { userId: "123", type: "welcome" } satisfies Payloads["user-notifications"],
  );
}

// Example: Creating a handler
export const handler = async (event: SQSEvent) => {
  return createHandler<Topics, Payloads>({
    "user-notifications": async (payload, metadata) => {
      console.log("Processing notification:", payload);
      console.log("Message ID:", metadata.messageId);
      console.log("Receive count:", metadata.receiveCount);
      // Process notification...
    },
    "order-processing": async (payload, metadata) => {
      console.log("Processing order:", payload);
      // Process order...
    },
  })(event);
};

