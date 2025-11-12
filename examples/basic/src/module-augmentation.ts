/**
 * Module Augmentation Example
 *
 * Demonstrates how to use module augmentation for type-safe topics and payloads
 * without needing explicit generics or satisfies clauses.
 */

import { send, createClient, createHandler } from "better-sqs";
import type { SQSEvent } from "aws-lambda";

// Augment the TopicPayloadMap interface to define your topic-to-payload mapping
// This provides type safety across your entire application
declare module "better-sqs" {
  interface TopicPayloadMap {
    "user-notifications": { userId: string; type: string };
    "order-processing": { orderId: string; action: string };
    "email-sending": {
      to: string;
      subject: string;
      body: string;
      from?: string;
    };
  }
}

// Example 1: Standalone send() - fully type-safe without explicit generics!
async function sendMessagesStandalone() {
  // TypeScript automatically knows the payload type from TopicPayloadMap
  await send("user-notifications", {
    userId: "123",
    type: "welcome",
    // wrongField: "error" // ❌ TypeScript error - not in the type
  });

  // TypeScript enforces the correct payload type
  await send("order-processing", {
    orderId: "456",
    action: "create",
  });

  // TypeScript knows optional fields too
  await send("email-sending", {
    to: "user@example.com",
    subject: "Welcome!",
    body: "Thanks for joining!",
    // from is optional, so this is valid
  });

  await send("email-sending", {
    to: "user@example.com",
    subject: "Welcome!",
    body: "Thanks for joining!",
    from: "noreply@example.com", // ✅ Optional field
  });

  // TypeScript prevents using unknown topics
  // await send("unknown-topic", { data: "test" }); // ❌ TypeScript error

  // With options - still fully type-safe
  await send(
    "order-processing",
    {
      orderId: "789",
      action: "update",
    },
    {
      idempotencyKey: "order-789-update",
      delaySeconds: 60,
      groupId: "order-789", // Required for FIFO queues
    },
  );
}

// Example 2: createClient() with module augmentation
// The client instance also benefits from module augmentation!
async function sendMessagesWithClient() {
  // Create a client instance - types are still inferred from TopicPayloadMap
  const queue = createClient({
    // Optional: configure logger, region, queueUrlResolver, etc.
    region: "us-west-2",
  });

  // All the same type safety benefits apply
  await queue.send("user-notifications", {
    userId: "123",
    type: "welcome",
    // wrongField: "error" // ❌ TypeScript error
  });

  await queue.send("order-processing", {
    orderId: "456",
    action: "create",
  });

  // With options
  await queue.send(
    "order-processing",
    {
      orderId: "789",
      action: "update",
    },
    {
      idempotencyKey: "order-789-update",
      delaySeconds: 60,
      groupId: "order-789",
    },
  );
}

// Example: Creating a handler - types are automatically inferred
export const handler = async (event: SQSEvent) => {
  // No need to specify types - they're inferred from TopicPayloadMap
  return createHandler({
    "user-notifications": async (payload, metadata) => {
      // payload is automatically typed as { userId: string; type: string }
      console.log("User ID:", payload.userId);
      console.log("Type:", payload.type);
      // console.log(payload.wrongField); // ❌ TypeScript error
    },
    "order-processing": async (payload, metadata) => {
      // payload is automatically typed as { orderId: string; action: string }
      console.log("Order ID:", payload.orderId);
      console.log("Action:", payload.action);
    },
    "email-sending": async (payload, metadata) => {
      // payload is automatically typed with optional from field
      console.log("To:", payload.to);
      console.log("Subject:", payload.subject);
      if (payload.from) {
        console.log("From:", payload.from);
      }
    },
  })(event);
};

