# Hypersync Quickstart

Minimal example showing how to use Hypersync to stream blockchain events from Uniswap V3.

## Prerequisites

- Node.js
- pnpm

## How to run

```bash
# Install dependencies
pnpm i

# Run full version with UI
node run.js

# Run minimal version (recommended for beginners)
node run-simple.js

# Run version with interactive terminal dashboard
node run-tui.js
```

The script streams events directly from Ethereum mainnet and displays progress as it scans the blockchain.

> **Note:** To use other networks, change the URL in `client = HypersyncClient.new({url: "http://eth.hypersync.xyz"})`. See [documentation](https://docs.envio.dev/docs/HyperSync/overview) for supported networks.

## Documentation

For more information about Hypersync, visit the [official documentation](https://docs.envio.dev/docs/HyperSync/overview).
