import { keccak256, toHex } from "viem";
import {
  HypersyncClient,
  LogField,
  JoinMode,
  Decoder,
} from "@envio-dev/hypersync-client";
import blessed from "blessed";
import contrib from "blessed-contrib";
import chalk from "chalk";
import figlet from "figlet";

//=============================================================================
// CONFIGURATION
//=============================================================================
const CONFIG = {
  contractAddress: "0x20382A8962d3C5e1CD3D117b8ecB2c6A8694E8DA",
  startBlock: 5507082,
  network: "megaethTestnet",
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
  bearerToken: process.env.HYPERSYNC_BEARER_TOKEN,
  timeout: 30000,
});

// Define event signatures and hashes - FIXED to match actual events
const TAPPED_TOPIC = keccak256(toHex("Tapped(uint256,address,uint256,uint256,uint256)"));
const ROUND_ENDED_TOPIC = keccak256(toHex("RoundEnded(uint256,address,uint256,uint256)"));
const topic0_list = [TAPPED_TOPIC, ROUND_ENDED_TOPIC];

// Create mapping from topic0 hash to event name
const TOPIC_TO_NAME = {
  [TAPPED_TOPIC]: "Tapped",
  [ROUND_ENDED_TOPIC]: "RoundEnded",
};

//=============================================================================
// UI HELPER FUNCTIONS
//=============================================================================
// Format numbers with commas
const formatNumber = (num) => {
  if (num === null || num === undefined) return "0";
  return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
};

// Format addresses
const formatAddress = (address) => {
  if (!address || address.length < 12) return address;
  return `${address.substring(0, 8)}...${address.substring(address.length - 4)}`;
};

// Format ETH values
const formatEth = (wei) => {
  try {
    const eth = parseFloat(wei) / 1e18;
    return eth.toFixed(4) + " ETH";
  } catch (e) {
    return wei.toString();
  }
};

//=============================================================================
// UI SETUP
//=============================================================================
// Create screen
const screen = blessed.screen({
  smartCSR: true,
  title: "Last Tap Game Event Tracker",
  dockBorders: true,
});

// Define UI color scheme
const uiColor = "#04b00f"; // Green theme for Last Tap

// Create a grid layout
const grid = new contrib.grid({
  rows: 12,
  cols: 12,
  screen: screen,
});

// Create ASCII logo
const logo = grid.set(0, 0, 3, 12, blessed.box, {
  content: chalk.hex(uiColor)(
    figlet.textSync("LAST TAP", {
      font: "ANSI Shadow",
      horizontalLayout: "full",
    })
  ),
  tags: true,
  align: "center",
  valign: "middle",
  border: {
    type: "line",
    fg: uiColor,
  },
});

// Create subtitle
const subtitle = grid.set(3, 0, 1, 12, blessed.box, {
  content: chalk.yellow(" Last Tap Game Event Tracker - MegaETH Testnet "),
  tags: true,
  align: "center",
  valign: "middle",
  style: {
    fg: "yellow",
    bold: true,
  },
});

// Create progress bar box
const progressBox = grid.set(4, 0, 1, 12, blessed.box, {
  label: " Scanning Progress ",
  tags: true,
  border: {
    type: "line",
    fg: uiColor,
  },
  style: {
    fg: "white",
  },
});

// Create stats display
const stats = grid.set(5, 0, 2, 6, blessed.box, {
  label: " Stats ",
  tags: true,
  border: {
    type: "line",
    fg: uiColor,
  },
  style: {
    fg: "white",
  },
});

// Create event distribution display
const eventDistribution = grid.set(5, 6, 2, 6, blessed.box, {
  label: " Event Distribution ",
  tags: true,
  border: {
    type: "line",
    fg: uiColor,
  },
  style: {
    fg: "white",
  },
});

// Create current round info
const roundInfo = grid.set(7, 0, 2, 12, blessed.box, {
  label: " Current Round ",
  tags: true,
  border: {
    type: "line",
    fg: uiColor,
  },
  style: {
    fg: "white",
  },
});

// Create log window
const logWindow = grid.set(9, 0, 3, 12, contrib.log, {
  label: " Event Log ",
  tags: true,
  border: {
    type: "line",
    fg: uiColor,
  },
  style: {
    fg: "green",
  },
  bufferLength: 30,
});

// Exit on Escape, q, or Ctrl+C
screen.key(["escape", "q", "C-c"], function (ch, key) {
  return process.exit(0);
});

//=============================================================================
// UI UPDATE FUNCTIONS
//=============================================================================
// Update progress bar display
const updateProgressBar = (progress, label = "") => {
  const width = progressBox.width - 4;
  const filledWidth = Math.floor(width * progress);
  const emptyWidth = width - filledWidth;

  const filledBar = chalk.bgHex(uiColor)(" ".repeat(filledWidth));
  const emptyBar = chalk.bgBlack(" ".repeat(emptyWidth));

  progressBox.setContent(
    `${filledBar}${emptyBar} ${(progress * 100).toFixed(2)}% ${label}`
  );
  screen.render();
};

// Update event distribution display
const updateEventDistribution = (eventCounts) => {
  const totalEvents = eventCounts.Tapped + eventCounts.RoundEnded;

  if (totalEvents === 0) {
    eventDistribution.setContent("No events found yet.");
    return;
  }

  const tappedPercent = (eventCounts.Tapped / totalEvents) * 100;
  const roundEndedPercent = (eventCounts.RoundEnded / totalEvents) * 100;

  eventDistribution.setContent(
    `${chalk.hex(uiColor)("Tapped:".padEnd(12))} ${chalk.green("■".repeat(Math.floor(tappedPercent / 5)))} ${formatNumber(eventCounts.Tapped)} (${tappedPercent.toFixed(1)}%)\n` +
    `${chalk.hex(uiColor)("RoundEnded:".padEnd(12))} ${chalk.yellow("■".repeat(Math.floor(roundEndedPercent / 5)))} ${formatNumber(eventCounts.RoundEnded)} (${roundEndedPercent.toFixed(1)}%)`
  );
  screen.render();
};

// Update round info display
const updateRoundInfo = (currentRound, lastTapper, tapCost, lastWinner, prizeAmount, newEndTime) => {
  roundInfo.setContent(
    `${chalk.hex(uiColor)("Current Round:".padEnd(16))} ${chalk.white(currentRound || "N/A")}\n` +
    `${chalk.hex(uiColor)("Last Tapper:".padEnd(16))} ${chalk.green(formatAddress(lastTapper) || "N/A")}\n` +
    `${chalk.hex(uiColor)("Current Tap Cost:".padEnd(16))} ${chalk.cyan(formatEth(tapCost) || "N/A")}\n` +
    `${chalk.hex(uiColor)("Current End Time:".padEnd(16))} ${chalk.cyan(newEndTime ? new Date(Number(newEndTime) * 1000).toLocaleString() : "N/A")}\n` +
    `${chalk.hex(uiColor)("Last Winner:".padEnd(16))} ${chalk.yellow(formatAddress(lastWinner) || "N/A")}\n` +
    `${chalk.hex(uiColor)("Last Prize:".padEnd(16))} ${chalk.magenta(formatEth(prizeAmount) || "N/A")}`
  );
  screen.render();
};

// Update stats display
const updateStats = (currentBlock, height, eventCounts, startTime) => {
  const totalEvents = eventCounts.Tapped + eventCounts.RoundEnded;
  const progress = Math.min((currentBlock - CONFIG.startBlock) / (height - CONFIG.startBlock), 1);
  const seconds = (performance.now() - startTime) / 1000;
  const eventsPerSecond = (totalEvents / Math.max(seconds, 0.1)).toFixed(1);

  stats.setContent(
    `${chalk.hex(uiColor)("Current Block")}: ${formatNumber(currentBlock)}\n` +
    `${chalk.hex(uiColor)("Total Events")}: ${formatNumber(totalEvents)}\n` +
    `${chalk.hex(uiColor)("Tapped Events")}: ${formatNumber(eventCounts.Tapped)}\n` +
    `${chalk.hex(uiColor)("RoundEnded Events")}: ${formatNumber(eventCounts.RoundEnded)}\n` +
    `${chalk.hex(uiColor)("Elapsed Time")}: ${seconds.toFixed(1)}s\n` +
    `${chalk.hex(uiColor)("Speed")}: ${formatNumber(eventsPerSecond)} events/s`
  );
  screen.render();
};

const POLLING_INTERVAL = 200; //ms

//=============================================================================
// MAIN FUNCTION
//=============================================================================
async function main() {
  // Track metrics
  const eventCounts = {
    Tapped: 0,
    RoundEnded: 0
  };

  // Track game state
  let currentRound = null;
  let lastTapper = null;
  let tapCost = null;
  let lastWinner = null;
  let lastPrize = null;
  let newEndTime = null;

  // Performance tracking
  const startTime = performance.now();

  try {
    // Initialize UI
    logWindow.log(chalk.green("Starting Last Tap Game Event Tracker..."));
    screen.render();

    // Get chain height
    const height = await client.getHeight();
    logWindow.log(`Chain height: ${formatNumber(height)}`);

    // Create decoder - FIXED signatures to match actual events
    const decoder = Decoder.fromSignatures([
      "Tapped(uint256 indexed roundNumber,address indexed player,uint256 cost,uint256 newEndTime,uint256 timestamp)",
      "RoundEnded(uint256 indexed roundNumber,address indexed winner,uint256 prize,uint256 timestamp)",
    ]);

    // Initialize UI components
    updateProgressBar(0, `Block: ${formatNumber(CONFIG.startBlock)}/${formatNumber(height)}`);
    updateEventDistribution(eventCounts);
    updateRoundInfo(currentRound, lastTapper, tapCost, lastWinner, lastPrize, newEndTime);
    updateStats(CONFIG.startBlock, height, eventCounts, startTime);

    // Set up query - FIXED to include all necessary topic fields
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
          LogField.Topic1,  // roundNumber (indexed)
          LogField.Topic2,  // player/winner (indexed)
          LogField.Topic3   // Extra topic if needed
        ],
      },
      joinMode: JoinMode.JoinNothing,
    };

    // Stream events
    logWindow.log(chalk.yellow("Starting event stream..."));
    let stream = await client.stream(query, {});

    let currentBlock = CONFIG.startBlock;

    // Main event loop
    while (true) {
      const res = await stream.recv();

      // Check if we've reached chain tip
      if (res === null) {
        logWindow.log(chalk.cyan("Reached chain tip. Waiting for new blocks..."));

        // Wait for new blocks
        await new Promise(resolve => setTimeout(resolve, POLLING_INTERVAL));

        try {
          // Check for new blocks
          const newHeight = await client.getHeight();

          if (newHeight > height) {
            logWindow.log(chalk.green(`Chain advanced to ${formatNumber(newHeight)}`));

            // Restart stream
            await stream.close();
            stream = await client.stream(query, {});
          }
        } catch (err) {
          logWindow.log(chalk.red(`Error checking height: ${err.message}`));
        }

        continue;
      }

      // Process logs
      if (res.data && res.data.logs && res.data.logs.length > 0) {
        const decodedLogs = await decoder.decodeLogs(res.data.logs);

        for (let i = 0; i < decodedLogs.length; i++) {
          const decodedLog = decodedLogs[i];
          const rawLog = res.data.logs[i];

          if (decodedLog === null) continue;

          // Get event type
          const topic0 = rawLog.topics[0];
          const eventType = TOPIC_TO_NAME[topic0];

          if (eventType === "Tapped") {
            eventCounts.Tapped++;

            // FIXED: Extract data correctly based on event structure
            // Indexed parameters: roundNumber (topic1), player (topic2)
            // Non-indexed parameters in body: cost, newEndTime, timestamp
            const roundNumber = decodedLog.indexed[0]?.val?.toString();
            const tapper = decodedLog.indexed[1]?.val?.toString();
            const tapCostPaid = decodedLog.body[0]?.val?.toString();
            const endTime = decodedLog.body[1]?.val?.toString();
            const timestamp = decodedLog.body[2]?.val?.toString();

            const date = new Date(Number(timestamp) * 1000).toISOString();
            const timeString = date.split("T")[1].split(".")[0];

            // Update game state
            currentRound = roundNumber;
            lastTapper = tapper;
            tapCost = tapCostPaid;
            newEndTime = endTime;

            // Log the event
            logWindow.log(
              `${chalk.green("➤ Tapped")} | Round: ${chalk.cyan(roundNumber)} | Tapper: ${chalk.yellow(
                formatAddress(tapper)
              )} | Cost: ${chalk.magenta(formatEth(tapCostPaid))} | ${timeString}`
            );
          }
          else if (eventType === "RoundEnded") {
            eventCounts.RoundEnded++;

            // FIXED: Extract data correctly based on event structure
            // Indexed parameters: roundNumber (topic1), winner (topic2)
            // Non-indexed parameters in body: prize, timestamp
            const roundNumber = decodedLog.indexed[0]?.val?.toString();
            const winner = decodedLog.indexed[1]?.val?.toString();
            const prizeAmount = decodedLog.body[0]?.val?.toString();
            const timestamp = decodedLog.body[1]?.val?.toString();

            const date = new Date(Number(timestamp) * 1000).toISOString();
            const timeString = date.split("T")[1].split(".")[0];

            // Update game state
            lastWinner = winner;
            lastPrize = prizeAmount;
            currentRound = (parseInt(roundNumber) + 1).toString();

            // Log the event
            logWindow.log(
              `${chalk.yellow("★ Round Ended")} | Round: ${chalk.cyan(roundNumber)} | Winner: ${chalk.green(
                formatAddress(winner)
              )} | Prize: ${chalk.magenta(formatEth(prizeAmount))} | ${timeString}`
            );
          }
        }

        // Update UI components
        updateRoundInfo(currentRound, lastTapper, tapCost, lastWinner, lastPrize, newEndTime);
        updateEventDistribution(eventCounts);
      }

      // Update block position
      if (res.nextBlock) {
        currentBlock = res.nextBlock;
        query.fromBlock = currentBlock;

        // Calculate progress
        const progress = Math.min((currentBlock - CONFIG.startBlock) / (height - CONFIG.startBlock), 1);

        // Update UI
        updateProgressBar(progress, `Block: ${formatNumber(currentBlock)}/${formatNumber(height)}`);
        updateStats(currentBlock, height, eventCounts, startTime);
      }

      // Render screen
      screen.render();
    }
  } catch (error) {
    // Handle errors
    logWindow.log(chalk.red(`Error: ${error.message}`));

    if (error.stack) {
      logWindow.log(chalk.yellow(`Error location: ${error.stack.split('\n')[0]}`));
    }

    logWindow.log(chalk.yellow("Press Q to exit"));
    screen.render();

    // Keep UI alive
    while (true) {
      await new Promise(resolve => setTimeout(resolve, 10000));
    }
  }
}

// Start the application
main();