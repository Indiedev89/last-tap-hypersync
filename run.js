// last-tap-tracker-supabase.js
import { keccak256, toHex } from "viem";
import {
  HypersyncClient,
  LogField,
  JoinMode,
  Decoder,
} from "@envio-dev/hypersync-client";
import http from "http";
import { createClient } from "@supabase/supabase-js"; // Import Supabase client
import "dotenv/config"; // Import dotenv to load environment variables

// --- Configuration ---
const CONFIG = {
  contractAddress: "0x16ED00aC93b37B7481eD3CCfa2a87C342aCB816C",
  startBlock: 3258331,
  network: "megaethTestnet",
  logLevel: "event-only", // 'verbose', 'normal', 'event-only'
  supabaseUrl: process.env.SUPABASE_URL, // Get from environment variable
  supabaseServiceKey: process.env.SUPABASE_SERVICE_ROLE_KEY,
};

// Validate Supabase config
if (!CONFIG.supabaseUrl || !CONFIG.supabaseServiceKey) {
  console.error(
    "Error: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables are required."
  );
  process.exit(1);
}

// Log environment variables format check (without revealing full values)
console.log(`SUPABASE_URL format check: ${CONFIG.supabaseUrl?.startsWith('https://') ? 'OK' : 'INVALID'}`);
console.log(`SUPABASE_SERVICE_ROLE_KEY length check: ${CONFIG.supabaseServiceKey?.length > 30 ? 'OK' : 'INVALID'}`);

// Network URL mapping
const NETWORK_URLS = {
  ethereum: "https://eth.hypersync.xyz",
  arbitrum: "https://arbitrum.hypersync.xyz",
  optimism: "https://optimism.hypersync.xyz",
  megaethTestnet: "https://megaeth-testnet.hypersync.xyz",
};

// --- Initialize Clients ---
const hypersyncClient = HypersyncClient.new({
  url: NETWORK_URLS[CONFIG.network],
  timeout: 30000,
});

const supabase = createClient(CONFIG.supabaseUrl, CONFIG.supabaseServiceKey);

// --- Event Signatures and Topics ---
const TAPPED_TOPIC = keccak256(
  toHex("Tapped(address,uint256,uint256,uint256)")
);
const ROUND_ENDED_TOPIC = keccak256(
  toHex("RoundEnded(address,uint256,uint256,uint256)")
);
const topic0_list = [TAPPED_TOPIC, ROUND_ENDED_TOPIC];

const POLLING_INTERVAL = 200; // ms
const SUPABASE_RATE_LIMIT_DELAY = 100; // ms between operations

// --- Helper Functions ---
const formatAddress = (address) => {
  if (!address || address.length < 12) return address;
  return `${address.substring(0, 8)}...${address.substring(
    address.length - 4
  )}`;
};

const formatEth = (wei) => {
  try {
    // Handle potential BigInt or large string inputs
    const weiBigInt = BigInt(wei);
    const eth = Number((weiBigInt * 10000n) / 10n ** 18n) / 10000; // Calculate with BigInt then convert
    return eth.toFixed(4) + " ETH";
  } catch (e) {
    console.warn(`Error formatting ETH value: ${wei}`, e);
    return wei?.toString() || "N/A"; // Add nullish coalescing
  }
};

// Logging function
const log = (message, level = "normal") => {
  const timestamp = new Date().toISOString();
  const logMessage = `${timestamp} [${level.toUpperCase()}] ${message}`;

  if (level === "error") {
    console.error(logMessage);
    return;
  }

  if (
    CONFIG.logLevel === "event-only" &&
    level !== "event" &&
    level !== "startup" &&
    level !== "supabase"
  ) {
    return;
  }

  if (
    CONFIG.logLevel === "normal" ||
    level === "event" ||
    level === "startup" ||
    level === "supabase" ||
    level === "verbose" // Ensure verbose logs are shown if level is verbose
  ) {
    console.log(logMessage);
  }
};

// --- Metrics and State ---
let eventCounts = {
  Tapped: 0,
  RoundEnded: 0,
  SupabaseInserts: 0,
  SupabaseErrors: 0,
};
let currentRound = null;
let lastTapper = null;
let tapCost = null;
let lastWinner = null;
let lastPrize = null;
let currentBlock = CONFIG.startBlock;
let startTime = performance.now();

// --- Supabase Upsert Function ---
async function upsertEvent(tableName, eventData) {
  try {
    const { data, error } = await supabase.from(tableName).upsert(eventData, {
      onConflict: "transaction_hash,log_index",
    });

    if (error) {
      log(
        `Supabase upsert error (${tableName}): ${
          error.message || JSON.stringify(error)
        }`,
        "error"
      );
      log(`Failed Event Data: ${JSON.stringify(eventData)}`, "error");
      log(`Error Details: ${JSON.stringify(error || {})}`, "error");
      eventCounts.SupabaseErrors++;
    } else {
      log(
        `Successfully upserted event to ${tableName} (Tx: ${formatAddress(
          eventData.transaction_hash
        )}, Log: ${eventData.log_index})`,
        "supabase"
      );
      eventCounts.SupabaseInserts++;
    }

    return { success: !error, data, error };
  } catch (e) {
    // This will catch any exceptions not handled by the Supabase client
    log(`Exception in upsertEvent (${tableName}): ${e.message}`, "error");
    log(`Exception stack: ${e.stack}`, "error");
    eventCounts.SupabaseErrors++;
    return { success: false, error: e };
  }
}

// --- Supabase Upsert with Retry Function ---
async function upsertEventWithRetry(tableName, eventData, maxRetries = 3) {
  let attempts = 0;

  while (attempts < maxRetries) {
    attempts++;

    const result = await upsertEvent(tableName, eventData);

    if (result.success) {
      if (attempts > 1) {
        log(
          `Successfully upserted to ${tableName} after ${attempts} attempts`,
          "supabase"
        );
      }
      return result;
    }

    if (attempts < maxRetries) {
      const delay = Math.pow(2, attempts) * 500; // Exponential backoff
      log(
        `Retry ${attempts}/${maxRetries} for ${tableName} in ${delay}ms`,
        "supabase"
      );
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }

  log(`Failed to upsert to ${tableName} after ${maxRetries} attempts`, "error");
  return {
    success: false,
    error: { message: `Failed after ${maxRetries} attempts` },
  };
}

// --- Test Supabase Connection Function ---
async function testSupabaseConnection() {
  try {
    // Test basic query
    const { data, error } = await supabase
      .from("tapped_events")
      .select("count(*)", { count: "exact", head: true });

    if (error) {
      log(`Supabase connection test failed: ${error.message}`, "error");
      return false;
    }

    log(
      `Supabase connection test successful. Current count in tapped_events: ${data}`,
      "startup"
    );
    return true;
  } catch (e) {
    log(`Supabase connection test exception: ${e.message}`, "error");
    return false;
  }
}

// --- HTTP Server (for status/health check) ---
const server = http.createServer((req, res) => {
  const totalEvents = eventCounts.Tapped + eventCounts.RoundEnded;
  const uptime = ((performance.now() - startTime) / 1000).toFixed(0);

  const html = `
    <!DOCTYPE html>
    <html>
    <head>
      <title>Last Tap Tracker Status</title>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <style>
        body { font-family: sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; line-height: 1.6; background-color: #f0f0f0; color: #333;}
        h1 { color: #FF6B6B; border-bottom: 2px solid #FF6B6B; padding-bottom: 5px;}
        .stats { background: #fff; padding: 20px; border-radius: 8px; margin: 20px 0; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
        .stats p { margin: 10px 0; }
        strong { color: #555; }
        .event { margin: 10px 0; padding: 10px; border-left: 4px solid #FF6B6B; background-color: #fff; border-radius: 4px;}
        .footer { margin-top: 30px; text-align: center; font-size: 0.9em; color: #777; }
      </style>
    </head>
    <body>
      <h1>Last Tap Game Event Tracker</h1>
      <div class="stats">
        <p><strong>Status:</strong> <span style="color: green;">Running</span></p>
        <p><strong>Network:</strong> ${CONFIG.network}</p>
        <p><strong>Contract:</strong> ${CONFIG.contractAddress}</p>
        <p><strong>Uptime:</strong> ${uptime} seconds</p>
        <p><strong>Current Block:</strong> ${currentBlock}</p>
        <p><strong>Current Round:</strong> ${currentRound || "Unknown"}</p>
        <p><strong>Last Tapper:</strong> ${
          formatAddress(lastTapper) || "Unknown"
        }</p>
        <p><strong>Current Tap Cost:</strong> ${
          formatEth(tapCost) || "Unknown"
        }</p>
        <p><strong>Last Winner:</strong> ${
          formatAddress(lastWinner) || "Unknown"
        }</p>
        <p><strong>Last Prize:</strong> ${formatEth(lastPrize) || "Unknown"}</p>
        <p><strong>Events Processed:</strong> ${totalEvents} (${
    eventCounts.Tapped
  } Taps, ${eventCounts.RoundEnded} Round Ends)</p>
        <p><strong>Supabase Inserts:</strong> ${eventCounts.SupabaseInserts}</p>
        <p><strong>Supabase Errors:</strong> <span style="color: ${
          eventCounts.SupabaseErrors > 0 ? "red" : "inherit"
        };">${eventCounts.SupabaseErrors}</span></p>
      </div>
      <div class="footer">
        <p><em>Tracker is running headless. This page provides real-time status.</em></p>
      </div>
    </body>
    </html>
    `;
  res.writeHead(200, { "Content-Type": "text/html" });
  res.end(html);
});

server.listen(8080, "0.0.0.0", () => {
  // Listen on all interfaces for container environments
  log("Status web server running on port 8080", "startup");
});

// --- Main Function ---
async function main() {
  const runStartTime = performance.now(); // Use a separate start time for this run
  let lastTipReachedTime = 0;
  const chainTipReportInterval = 5 * 60 * 1000; // 5 minutes

  // Test Supabase connection at startup
  const connectionOk = await testSupabaseConnection();
  if (!connectionOk) {
    log(
      "Warning: Supabase connection test failed. Data storage may not work correctly.",
      "error"
    );
    // Optionally exit here if you want to fail on bad connections
    // process.exit(1);
  }

  try {
    log(`Starting Last Tap Tracker (Supabase Integrated)...`, "startup");
    log(
      `Network: ${CONFIG.network}, Contract: ${CONFIG.contractAddress}, Start Block: ${CONFIG.startBlock}`,
      "startup"
    );

    const height = await hypersyncClient.getHeight();
    log(`Initial chain height: ${height}`, "startup");

    const decoder = Decoder.fromSignatures([
      "Tapped(address indexed tapper, uint256 roundNumber, uint256 tapCostPaid, uint256 timestamp)",
      "RoundEnded(address indexed winner, uint256 prizeAmount, uint256 roundNumber, uint256 timestamp)",
    ]);

    // Define the query, requesting necessary fields including logIndex and transactionHash
    let query = {
      fromBlock: currentBlock, // Start from the current tracked block
      logs: [
        {
          address: [CONFIG.contractAddress],
          topics: [topic0_list],
        },
      ],
      fieldSelection: {
        log: [
          LogField.BlockNumber,
          LogField.TransactionHash, // Request Transaction Hash
          LogField.LogIndex, // Request Log Index
          LogField.Data,
          LogField.Topic0,
          LogField.Topic1, // Assuming indexed address is topic 1
          // Add Topic2, Topic3 if needed for other indexed fields
        ],
      },
      joinMode: JoinMode.JoinNothing,
    };

    log(`Starting event stream from block ${query.fromBlock}...`, "startup");
    let stream = await hypersyncClient.stream(query, {});

    let lastProgressLogBlock = currentBlock;
    let consecutiveChainTips = 0;

    while (true) {
      const res = await stream.recv();

      if (res === null) {
        // Reached chain tip
        const now = Date.now();
        consecutiveChainTips++;
        if (
          now - lastTipReachedTime > chainTipReportInterval ||
          consecutiveChainTips === 1
        ) {
          log(
            `Reached chain tip at block ${currentBlock}. Waiting for new blocks...`,
            "verbose"
          );
          lastTipReachedTime = now;
        }
        await new Promise((resolve) => setTimeout(resolve, POLLING_INTERVAL));

        // Optional: Check height again to potentially restart stream sooner
        try {
          const newHeight = await hypersyncClient.getHeight();
          if (newHeight > height) { // Check against the last known height from the stream start
            log(`Chain advanced to ${newHeight}. Re-querying...`, "verbose");
            await stream.close(); // Close the current stream
            query.fromBlock = currentBlock; // Ensure query starts from the correct block
            stream = await hypersyncClient.stream(query, {}); // Start new stream
            consecutiveChainTips = 0; // Reset counter
          }
        } catch (err) {
          log(`Error checking height while at tip: ${err.message}`, "error");
        }
        continue; // Go back to recv()
      }

      // Reset consecutive chain tips counter on receiving data
      consecutiveChainTips = 0;
      lastTipReachedTime = 0; // Reset tip timer

      // Process logs if they exist
      if (res.data && res.data.logs && res.data.logs.length > 0) {
        const decodedLogs = await decoder.decodeLogs(res.data.logs);

        for (let i = 0; i < decodedLogs.length; i++) {
          const decodedLog = decodedLogs[i];
          const rawLog = res.data.logs[i]; // Access the raw log data

          if (decodedLog === null) {
            log(
              `Skipping undecodable log at index ${i}, block ${rawLog.blockNumber}`,
              "verbose"
            );
            continue;
          }

          // --- Extract Common Fields ---
          const blockNumber = rawLog.blockNumber;
          const transactionHash = rawLog.transactionHash;
          const logIndex = rawLog.logIndex; // Extract logIndex

          if (
            transactionHash === undefined ||
            logIndex === undefined ||
            blockNumber === undefined
          ) {
            log(
              `Missing critical fields (txHash, logIndex, blockNumber) in log at index ${i}, block ${
                rawLog.blockNumber
              }. Skipping. Raw log: ${JSON.stringify(rawLog)}`,
              "error"
            );
            continue;
          }

          const topic0 = rawLog.topics[0];
          const eventType = topic0 === TAPPED_TOPIC ? "Tapped" : "RoundEnded";

          try {
            // Add try-catch around event processing
            if (eventType === "Tapped") {
              eventCounts.Tapped++;

              // Extract specific data
              const tapper = decodedLog.indexed[0]?.val?.toString();
              const roundNumber = decodedLog.body[0]?.val?.toString();
              const tapCostPaid = decodedLog.body[1]?.val?.toString();
              const timestampSeconds = decodedLog.body[2]?.val?.toString();
              const eventTimestamp = new Date(Number(timestampSeconds) * 1000);

              // Validate extracted data
              if (
                !tapper ||
                !roundNumber ||
                !tapCostPaid ||
                !timestampSeconds
              ) {
                log(
                  `Missing data in Tapped event: Tx ${formatAddress(
                    transactionHash
                  )}, Log ${logIndex}. Skipping.`,
                  "error"
                );
                continue;
              }

              // Update local state
              currentRound = roundNumber;
              lastTapper = tapper;
              tapCost = tapCostPaid;

              // Log locally
              log(
                `TAPPED | Blk: ${blockNumber} | Rnd: ${roundNumber} | Tapper: ${formatAddress(
                  tapper
                )} | Cost: ${formatEth(
                  tapCostPaid
                )} | ${eventTimestamp.toISOString()}`,
                "event"
              );

              // --- Prepare and Send to Supabase ---
              const tappedEventData = {
                block_number: Number(blockNumber),
                transaction_hash: String(transactionHash),
                log_index: Number(logIndex),
                tapper_address: String(tapper),
                round_number: String(roundNumber),
                tap_cost_paid: String(tapCostPaid),
                event_timestamp: eventTimestamp.toISOString(),
              };

              await upsertEventWithRetry("tapped_events", tappedEventData);

              // Add small delay to prevent rate limiting
              await new Promise(resolve => setTimeout(resolve, SUPABASE_RATE_LIMIT_DELAY));

            } else if (eventType === "RoundEnded") {
              eventCounts.RoundEnded++;

              // Extract specific data
              const winner = decodedLog.indexed[0]?.val?.toString();
              const prizeAmount = decodedLog.body[0]?.val?.toString();
              const roundNumber = decodedLog.body[1]?.val?.toString();
              const timestampSeconds = decodedLog.body[2]?.val?.toString();
              const eventTimestamp = new Date(Number(timestampSeconds) * 1000);

              // Validate extracted data
              if (
                !winner ||
                !prizeAmount ||
                !roundNumber ||
                !timestampSeconds
              ) {
                log(
                  `Missing data in RoundEnded event: Tx ${formatAddress(
                    transactionHash
                  )}, Log ${logIndex}. Skipping.`,
                  "error"
                );
                continue;
              }

              // Update local state
              lastWinner = winner;
              lastPrize = prizeAmount;
              // Assuming next round starts immediately for display purposes
              currentRound = (BigInt(roundNumber) + 1n).toString();

              // Log locally
              log(
                `ROUND END | Blk: ${blockNumber} | Rnd: ${roundNumber} | Winner: ${formatAddress(
                  winner
                )} | Prize: ${formatEth(
                  prizeAmount
                )} | ${eventTimestamp.toISOString()}`,
                "event"
              );

              // --- Prepare and Send to Supabase ---
              const roundEndedEventData = {
                block_number: Number(blockNumber),
                transaction_hash: String(transactionHash),
                log_index: Number(logIndex),
                winner_address: String(winner),
                prize_amount: String(prizeAmount),
                round_number: String(roundNumber),
                event_timestamp: eventTimestamp.toISOString(),
              };

              await upsertEventWithRetry("round_ended_events", roundEndedEventData);

              // Add small delay to prevent rate limiting
              await new Promise(resolve => setTimeout(resolve, SUPABASE_RATE_LIMIT_DELAY));
            }
          } catch (processingError) {
            log(
              `Error processing log: ${
                processingError.message
              }. Tx: ${formatAddress(
                transactionHash
              )}, Log: ${logIndex}. Skipping.`,
              "error"
            );
            log(`Processing Error Stack: ${processingError.stack}`, "error"); // Log stack trace
          }
        }
      }

      // Update block position for the next query iteration
      if (res.nextBlock) {
        const previousBlock = currentBlock;
        currentBlock = res.nextBlock;
        query.fromBlock = currentBlock; // IMPORTANT: Update query for potential restarts/reconnects

        // Log progress occasionally
        if (currentBlock - lastProgressLogBlock >= 10000) {
          const seconds = (performance.now() - runStartTime) / 1000;
          const totalEvents = eventCounts.Tapped + eventCounts.RoundEnded;
          log(
            `Progress: Block ${currentBlock} | ${totalEvents} events (${
              eventCounts.Tapped
            } Taps, ${eventCounts.RoundEnded} RoundEnds) | ${
              eventCounts.SupabaseInserts
            } DB inserts | ${seconds.toFixed(1)}s`,
            "normal"
          );
          lastProgressLogBlock = currentBlock;
        }
      } else if (res.data && res.data.logs && res.data.logs.length > 0) {
        // If we received data but no nextBlock (can happen at tip),
        // ensure we update currentBlock to the block number of the last log processed
        const lastLogBlock =
          res.data.logs[res.data.logs.length - 1]?.blockNumber;
        if (lastLogBlock && lastLogBlock >= currentBlock) {
          // Only update if it's forward progress. Add 1 because fromBlock is inclusive.
          currentBlock = lastLogBlock + 1;
          query.fromBlock = currentBlock;
          log(
            `Advanced currentBlock to ${currentBlock} based on last log received`,
            "verbose"
          );
        }
      }
    }
  } catch (error) {
    log(`Fatal error in main loop: ${error.message}`, "error");
    log(`Stack Trace: ${error.stack}`, "error"); // Log stack trace for debugging

    // Attempt graceful shutdown or specific error handling
    if (stream) {
      try {
        await stream.close();
        log("Hypersync stream closed.", "normal");
      } catch (closeError) {
        log(`Error closing Hypersync stream: ${closeError.message}`, "error");
      }
    }

    log("Attempting restart in 30 seconds...", "normal");
    await new Promise((resolve) => setTimeout(resolve, 30000));
    main(); // Restart the main function
  }
}

// --- Global Error Handlers ---
process.on("unhandledRejection", (reason, promise) => {
  log(
    `Unhandled Rejection at: ${promise}, reason: ${reason?.message || reason}`,
    "error"
  );
  if (reason instanceof Error) {
    log(`Unhandled Rejection Stack: ${reason.stack}`, "error");
  }
  // Consider exiting or specific recovery based on the error
  // process.exit(1); // Exit on unhandled rejection might be too aggressive
});

process.on("uncaughtException", (error) => {
  log(`Uncaught Exception: ${error.message}`, "error");
  log(`Uncaught Exception Stack: ${error.stack}`, "error");
  // It's generally recommended to exit cleanly after an uncaught exception
  process.exit(1);
});

// --- Start Application ---
main().catch((error) => {
  log(`Critical startup error: ${error.message}`, "error");
  log(`Startup Error Stack: ${error.stack}`, "error");
  // Optional: Delay before exiting or attempting restart
  log("Exiting due to critical startup error.", "error");
  process.exit(1); // Exit if main fails critically on initial start
});
