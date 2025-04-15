# Last Tap Game Event Tracker

A beautiful terminal UI application for monitoring real-time events from the Last Tap Game on MegaETH testnet using Hypersync.

## Prerequisites

* [Node.js](https://nodejs.org/)
* [npm](https://www.npmjs.com/) or [pnpm](https://pnpm.io/)

## About

This script streams events directly from the MegaETH testnet and displays them in a beautiful terminal interface, showing real-time game state, event tracking, and statistics.

* Note: To use other networks, change the network in the CONFIG section at the top of the script. Supported networks include Ethereum, Arbitrum, Optimism, and others.

## Features

* Real-time tracking of Tapped and RoundEnded events
* Beautiful terminal UI with live updates
* Current round information display
* Event statistics and distribution visualization
* Progress bar showing blockchain scanning status
* Continuous monitoring with automatic detection of new blocks

## Configuration

You can adjust the following settings in the CONFIG object at the top of the file:

* `contractAddress`: The address of the Last Tap game contract
* `startBlock`: The block number to start scanning from
* `network`: The network to connect to (e.g., "ethereum", "arbitrum", "megaethTestnet")

## How to run

```bash
# Install dependencies
npm install
# or with pnpm
pnpm i

# Run the terminal UI tracker
npm start
# or with pnpm
pnpm start
```

## Controls

Press `q`, `Escape`, or `Ctrl+C` to exit the application

## About Last Tap Game

Last Tap is a competitive blockchain game where players tap to stay in the game. The last player to tap wins the round and takes the prize. Each tap increases in cost, creating exciting gameplay dynamics.
