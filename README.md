# Better-SQS

A TypeScript library that wraps AWS SQS with a Vercel Queue-like API. Provides a simple, type-safe interface for SQS FIFO queues with built-in handler routing.

## Features

- **Developer-friendly API**: `send(topic, payload)` instead of raw SQS SDK calls
- **Type-safe**: Full TypeScript support with generics
- **Handler routing**: Automatically routes messages to handlers based on queue name
- **FIFO support**: Built-in message grouping, deduplication, and batch failure handling
- **Serverless-first**: Factory pattern with no global state - perfect for Lambda
- **Vercel Queue compatibility**: Familiar API for teams already using Vercel Queue
- **Framework-agnostic**: Works with any AWS Lambda setup (SST, CDK, Serverless, etc.)
- **Optional logger**: Accepts any logger interface (pino, winston, console, etc.)

## Installation

```bash
npm install better-sqs
# or
bun add better-sqs
# or
pnpm add better-sqs
```

## Quick Start

### 1. Sending Messages

**Option A: Standalone `send()` (simplest)**

```typescript
import { send } from "better-sqs";

// Simple send - uses env vars automatically
await send("user-notifications", {
  userId: "123",
  type: "welcome"
});

// With options
await send("order-processing", { orderId: "456" }, {
  idempotencyKey: "unique-key",
  delaySeconds: 60,
  groupId: "order-456", // Required for FIFO queues
});
```

**Option B: Client Instance (more control)**

```typescript
import { createClient } from "better-sqs";

// Create a configured client
const queue = createClient({
  region: "us-west-2",
  // Optional: logger, sqsClient, queueUrlResolver
});

// Use the client
await queue.send("user-notifications", {
  userId: "123",
  type: "welcome"
});
```

### 2. Creating Handlers

```typescript
import { createHandler } from "better-sqs";
import type { SQSEvent } from "aws-lambda";

// Define your topics and payload types
type Topics = "user-notifications" | "order-processing";
type Payloads = {
  "user-notifications": { userId: string; type: string };
  "order-processing": { orderId: string; action: string };
};

// Create handler with type safety
export const handler = async (event: SQSEvent) => {
  return createHandler<Topics, Payloads>({
    "user-notifications": async (payload, metadata) => {
      // payload is typed as { userId: string; type: string }
      console.log("Processing notification:", payload);
      await sendWelcomeEmail(payload.userId);
    },
    "order-processing": async (payload, metadata) => {
      // payload is typed as { orderId: string; action: string }
      console.log("Processing order:", payload);
      await processOrder(payload.orderId);
    }
  })(event);
};
```

## Configuration

### Environment Variables

Better-SQS automatically looks for queue URLs in environment variables:

```bash
# Format: SQS_<TOPIC>_URL or SQS_<TOPIC_UPPERCASE>_URL
SQS_USER_NOTIFICATIONS_URL=https://sqs.us-east-1.amazonaws.com/123456789/user-notifications.fifo
SQS_ORDER_PROCESSING_URL=https://sqs.us-east-1.amazonaws.com/123456789/order-processing.fifo
```

The standalone `send()` function uses these environment variables automatically - no configuration needed!

### Creating a Client Instance

For more control, create a configured client instance using the factory pattern:

```typescript
import { createClient } from "better-sqs";
import pino from "pino";

// Create a configured client
const queue = createClient({
  logger: pino(),
  region: "us-west-2",
  queueUrlResolver: (topic) => {
    // Custom resolver logic
    return `https://sqs.us-west-2.amazonaws.com/123456789/${topic}.fifo`;
  },
});

// Use the client
await queue.send("user-notifications", { userId: "123", type: "welcome" });
```

### Using Custom SQS Client

```typescript
import { createClient } from "better-sqs";
import { SQSClient } from "@aws-sdk/client-sqs";

const sqsClient = new SQSClient({
  region: "us-west-2",
  // ... custom config
});

const queue = createClient({
  sqsClient,
});

await queue.send("my-topic", { data: "example" });
```

### Client with Defaults

You can also create a client that uses environment variables and defaults:

```typescript
import { createClient } from "better-sqs";

// Uses env vars automatically - no config needed
const queue = createClient();

await queue.send("my-topic", { data: "example" });
```

## FIFO Queues

Better-SQS fully supports SQS FIFO queues with message grouping and deduplication:

```typescript
import { send } from "better-sqs";

// FIFO queue requires groupId
await send("order-processing", { orderId: "123" }, {
  groupId: "order-123", // Messages with same groupId are processed in order
  idempotencyKey: "unique-key", // Prevents duplicates
  delaySeconds: 30,
});
```

### FIFO Queue Requirements

- **groupId**: Required for FIFO queues. Messages with the same groupId are processed in order.
- **deduplicationId**: Required unless content-based deduplication is enabled. If not provided, `idempotencyKey` or a hash of the payload will be used.

## Handler Retry Logic

Handlers can return a retry result to control visibility timeout (similar to Vercel Queue's `timeoutSeconds`):

```typescript
export const handler = createHandler({
  "user-notifications": async (payload, metadata) => {
    if (!isSystemReady()) {
      // Request retry after 5 minutes
      return { retryAfterSeconds: 300 };
    }
    
    await processNotification(payload);
  }
});
```

**Note**: SQS doesn't support direct retry timing control from handlers. The `retryAfterSeconds` is informational for logging/monitoring. Actual retries follow the queue's visibility timeout configuration.

## Error Handling

Better-SQS handles errors gracefully:

- **Failed messages**: Automatically added to `batchItemFailures` for partial batch failure handling
- **Unknown queues**: Logged as warnings and marked as successful to prevent infinite retries
- **Parse errors**: Caught and logged, message added to failures for retry

```typescript
export const handler = createHandler({
  "my-topic": async (payload, metadata) => {
    try {
      await processMessage(payload);
    } catch (error) {
      // Error is automatically caught and message is added to batchItemFailures
      // Message will be retried according to queue configuration
      throw error; // Re-throw if you want to ensure it's logged
    }
  }
});
```

## Type Safety

Better-SQS provides full type safety through TypeScript generics:

```typescript
// Define your topics
type Topics = "user-notifications" | "order-processing" | "email-sending";

// Define payload types for each topic
type Payloads = {
  "user-notifications": { userId: string; type: string };
  "order-processing": { orderId: string; action: string };
  "email-sending": { to: string; subject: string; body: string };
};

// Type-safe send
await send<"user-notifications", Payloads["user-notifications"]>(
  "user-notifications",
  { userId: "123", type: "welcome" } // TypeScript ensures correct shape
);

// Type-safe handler
export const handler = createHandler<Topics, Payloads>({
  "user-notifications": async (payload, metadata) => {
    // payload is typed as { userId: string; type: string }
    console.log(payload.userId); // ✅ Type-safe
    console.log(payload.invalid); // ❌ TypeScript error
  }
});
```

## Advanced Usage

### Custom Message Attributes

```typescript
import { send } from "better-sqs";

await send("my-topic", { data: "example" }, {
  messageAttributes: {
    priority: {
      StringValue: "high",
      DataType: "String",
    },
    timestamp: {
      StringValue: Date.now().toString(),
      DataType: "Number",
    },
  },
});
```

### Per-Call Configuration

The standalone `send()` function supports per-call configuration overrides:

```typescript
import { send } from "better-sqs";

await send(
  "my-topic",
  { data: "example" },
  { delaySeconds: 60 }, // Send options
  { region: "us-west-2" } // Optional per-call config override
);
```

For more control, create a client instance:

```typescript
import { createClient } from "better-sqs";
import { SQSClient } from "@aws-sdk/client-sqs";

const customClient = new SQSClient({ region: "us-west-2" });

const queue = createClient({ sqsClient: customClient });

await queue.send("my-topic", { data: "example" }, { delaySeconds: 60 });
```

## Comparison with Vercel Queue

| Feature | Vercel Queue | Better-SQS |
|---------|-------------|------------|
| API Style | `send(topic, payload)` | `send(topic, payload)` ✅ |
| Type Safety | Full TypeScript | Full TypeScript ✅ |
| Handler Routing | `handleCallback()` | `createHandler()` ✅ |
| FIFO Support | No | Yes ✅ |
| Consumer Groups | Yes | No (SQS limitation) |
| Framework | Vercel-specific | Framework-agnostic ✅ |
| Infrastructure | Vercel-managed | AWS SQS ✅ |

## Limitations

- **Consumer Groups**: SQS doesn't support consumer groups natively. Use separate queues or SQS message filtering if needed.
- **Retry Timing**: SQS retry timing is controlled by queue configuration, not handler return values.
- **Message Size**: SQS has a 256KB message size limit (payload must be serializable to JSON).

## License

MIT
