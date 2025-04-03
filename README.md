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

# Run single event decoder example (Uniswap V3 swaps)
node run-decoder.js

# Run multi-event decoder example (ERC20 Transfer and Approval events)
node run-decoder-multi.js
```

The script streams events directly from Ethereum mainnet and displays progress as it scans the blockchain.

> **Note:** To use other networks, change the URL in `client = HypersyncClient.new({url: "http://eth.hypersync.xyz"})`. See [documentation](https://docs.envio.dev/docs/HyperSync/overview) for supported networks.

## Example Files

- `run.js` - Main example with UI for tracking Uniswap V3 events
- `run-simple.js` - Simplified version for beginners
- `run-tui.js` - Example with interactive terminal dashboard
- `run-decoder.js` - Shows how to use Hypersync's decoder to parse Uniswap V3 swap events
- `run-decoder-multi.js` - Demonstrates decoding multiple event types (ERC20 Transfer and Approval)

## Documentation

For more information about Hypersync, visit the [official documentation](https://docs.envio.dev/docs/HyperSync/overview).
