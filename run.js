// last-tap-tracker-headless.js
import { keccak256, toHex } from "viem";
import {
  HypersyncClient,
  LogField,
  JoinMode,
  Decoder,
} from "@envio-dev/hypersync-client";

// Configuration
const CONFIG = {
  contractAddress: "0x16ED00aC93b37B7481eD3CCfa2a87C342aCB816C",
  startBlock: 3258331,
  network: "megaethTestnet",
  logLevel: "event-only", // 'verbose', 'normal', 'event-only'
};

// Network URL mapping
const NETWORK_URLS = {
  ethereum: "https://eth.hypersync.xyz",
  arbitrum: "https://arbitrum.hypersync.xyz",
  optimism: "https://optimism.hypersync.xyz",
  megaethTestnet: "https://megaeth-testnet.hypersync.xyz",
};

// Initialize client
const client = HypersyncClient.new({
  url: NETWORK_URLS[CONFIG.network],
  timeout: 30000,
});

// Define event signatures and hashes
const TAPPED_TOPIC = keccak256(toHex("Tapped(address,uint256,uint256,uint256)"));
const ROUND_ENDED_TOPIC = keccak256(toHex("RoundEnded(address,uint256,uint256,uint256)"));
const topic0_list = [TAPPED_TOPIC, ROUND_ENDED_TOPIC];

const POLLING_INTERVAL = 200; //ms

// Helper functions
const formatAddress = (address) => {
  if (!address || address.length < 12) return address;
  return `${address.substring(0, 8)}...${address.substring(address.length - 4)}`;
};

const formatEth = (wei) => {
  try {
    const eth = parseFloat(wei) / 1e18;
    return eth.toFixed(4) + " ETH";
  } catch (e) {
    return wei.toString();
  }
};

// Logging function that respects log level
const log = (message, level = "normal") => {
  if (level === "error") {
    console.error(message);
    return;
  }

  if (CONFIG.logLevel === "event-only" && level !== "event" && level !== "startup") {
    return;
  }

  if (CONFIG.logLevel === "normal" || level === "event" || level === "startup") {
    console.log(message);
  }
};

// Main function
async function main() {
  // Track metrics
  let eventCounts = {
    Tapped: 0,
    RoundEnded: 0
  };

  // Track game state
  let currentRound = null;
  let lastTapper = null;
  let tapCost = null;
  let lastWinner = null;
  let lastPrize = null;

  // Start time for tracking
  const startTime = performance.now();

  // Track chain tip status to avoid log spam
  let lastTipReachedTime = 0;
  let chainTipReportInterval = 5 * 60 * 1000; // Report chain tip status every 5 minutes

  try {
    log("Starting Last Tap Game Event Tracker...", "startup");

    // Get chain height
    const height = await client.getHeight();
    log(`Chain height: ${height}`, "startup");

    // Create decoder
    const decoder = Decoder.fromSignatures([
      "Tapped(address indexed tapper, uint256 roundNumber, uint256 tapCostPaid, uint256 timestamp)",
      "RoundEnded(address indexed winner, uint256 prizeAmount, uint256 roundNumber, uint256 timestamp)",
    ]);

    // Set up query
    let query = {
      fromBlock: CONFIG.startBlock,
      logs: [
        {
          address: [CONFIG.contractAddress],
          topics: [topic0_list],
        },
      ],
      fieldSelection: {
        log: [
          LogField.BlockNumber,
          LogField.TransactionHash,
          LogField.Data,
          LogField.Topic0,
          LogField.Topic1,
        ],
      },
      joinMode: JoinMode.JoinNothing,
    };

    log("Starting event stream...", "startup");
    let stream = await client.stream(query, {});

    let currentBlock = CONFIG.startBlock;
    let lastProgressLogBlock = currentBlock;
    let consecutiveChainTips = 0;

    // Main event loop
    while (true) {
      const res = await stream.recv();

      // Check if we've reached chain tip
      if (res === null) {
        const now = Date.now();
        consecutiveChainTips++;

        // Only log chain tip status occasionally to reduce spam
        if (now - lastTipReachedTime > chainTipReportInterval) {
          log(`Waiting for new blocks... (Been at chain tip for ${Math.floor(consecutiveChainTips * 5)} seconds)`, "verbose");
          lastTipReachedTime = now;
        }

        // Wait for new blocks
        await new Promise(resolve => setTimeout(resolve, POLLING_INTERVAL));

        try {
          // Check for new blocks
          const newHeight = await client.getHeight();

          if (newHeight > height) {
            log(`Chain advanced to ${newHeight}`, "verbose");
            consecutiveChainTips = 0;

            // Restart stream
            await stream.close();
            stream = await client.stream(query, {});
          }
        } catch (err) {
          log(`Error checking height: ${err.message}`, "error");
        }

        continue;
      }

      // Reset consecutive chain tips counter
      consecutiveChainTips = 0;

      // Process logs
      if (res.data && res.data.logs && res.data.logs.length > 0) {
        const decodedLogs = await decoder.decodeLogs(res.data.logs);

        for (let i = 0; i < decodedLogs.length; i++) {
          const decodedLog = decodedLogs[i];
          const rawLog = res.data.logs[i];

          if (decodedLog === null) continue;

          // Get event type
          const topic0 = rawLog.topics[0];
          const eventType = topic0 === TAPPED_TOPIC ? "Tapped" : "RoundEnded";

          if (eventType === "Tapped") {
            eventCounts.Tapped++;

            // Extract data
            const tapper = decodedLog.indexed[0]?.val.toString();
            const roundNumber = decodedLog.body[0]?.val.toString();
            const tapCostPaid = decodedLog.body[1]?.val.toString();
            const timestamp = decodedLog.body[2]?.val.toString();
            const date = new Date(Number(timestamp) * 1000).toISOString();

            // Update game state
            currentRound = roundNumber;
            lastTapper = tapper;
            tapCost = tapCostPaid;

            // Log the event
            log(`TAPPED | Round: ${roundNumber} | Tapper: ${formatAddress(tapper)} | Cost: ${formatEth(tapCostPaid)} | ${date}`, "event");
          }
          else if (eventType === "RoundEnded") {
            eventCounts.RoundEnded++;

            // Extract data
            const winner = decodedLog.indexed[0]?.val.toString();
            const prizeAmount = decodedLog.body[0]?.val.toString();
            const roundNumber = decodedLog.body[1]?.val.toString();
            const timestamp = decodedLog.body[2]?.val.toString();
            const date = new Date(Number(timestamp) * 1000).toISOString();

            // Update game state
            lastWinner = winner;
            lastPrize = prizeAmount;
            currentRound = (parseInt(roundNumber) + 1).toString();

            // Log the event
            log(`ROUND ENDED | Round: ${roundNumber} | Winner: ${formatAddress(winner)} | Prize: ${formatEth(prizeAmount)} | ${date}`, "event");
          }
        }
      }

      // Update block position
      if (res.nextBlock) {
        currentBlock = res.nextBlock;
        query.fromBlock = currentBlock;

        // Log progress occasionally (every 10,000 blocks) to avoid log spam
        if (currentBlock - lastProgressLogBlock >= 10000) {
          const seconds = (performance.now() - startTime) / 1000;
          const totalEvents = eventCounts.Tapped + eventCounts.RoundEnded;
          log(`Block ${currentBlock} | ${totalEvents} events (${eventCounts.Tapped} Tapped, ${eventCounts.RoundEnded} RoundEnded) | ${seconds.toFixed(1)}s`, "normal");
          lastProgressLogBlock = currentBlock;
        }
      }
    }
  } catch (error) {
    log(`Fatal error: ${error.message}`, "error");

    // Wait a bit and restart the application
    log("Restarting in 30 seconds...", "normal");
    await new Promise(resolve => setTimeout(resolve, 30000));
    main();
  }
}

// Handle unhandled promise rejections
process.on("unhandledRejection", (error) => {
  log(`Unhandled promise rejection: ${error.message}`, "error");
});

// Start the application
main().catch(error => {
  log(`Startup error: ${error.message}`, "error");
  // Restart after a delay
  setTimeout(() => main(), 30000);
});