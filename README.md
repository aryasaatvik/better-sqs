# Better-SQS Monorepo

This monorepo contains the Better-SQS library and example packages.

## Packages

### [`better-sqs`](./packages/better-sqs)

A TypeScript library that wraps AWS SQS with a Vercel Queue-like API. Provides a simple, type-safe interface for SQS FIFO queues with built-in handler routing.

**See the [package README](./packages/better-sqs/README.md) for full documentation.**

## Examples

### [`@better-sqs/example-basic`](./examples/basic)

Basic usage examples demonstrating:
- Standalone `send()` pattern
- `createClient()` factory pattern
- Module augmentation for type safety
- Handler creation

## Development

### Prerequisites

- Node.js >= 18.0.0
- pnpm >= 8.0.0

### Setup

```bash
pnpm install
```

### Build

```bash
pnpm build
```

### Type Check

```bash
pnpm typecheck
```

### Clean

```bash
pnpm clean
```

## License

MIT
