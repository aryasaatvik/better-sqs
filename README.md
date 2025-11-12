# Better-SQS Monorepo

This monorepo contains the Better-SQS library and example packages.

## Packages

### [`better-sqs`](./packages/better-sqs)

Type-safe queues for AWS SQS. A simple, developer-friendly API with full TypeScript support.

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
