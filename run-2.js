// last-tap-tracker-supabase.js
import { keccak256, toHex } from "viem"; // Import crypto utils from viem for hashing event signatures
import {
  HypersyncClient,
  LogField,
  JoinMode,
  Decoder,
} from "@envio-dev/hypersync-client"; // Import Hypersync client components for blockchain data streaming
import http from "http"; // Import Node.js built-in HTTP module for the status server
import { createClient } from "@supabase/supabase-js"; // Import Supabase client for database interaction
import "dotenv/config"; // Import dotenv to load environment variables from a .env file

// --- Configuration ---
// Central configuration object holding key parameters for the indexer.
const CONFIG = {
  // The address of the smart contract emitting the events we want to track.
  contractAddress: "0x16ED00aC93b37B7481eD3CCfa2a87C342aCB816C",
  // The block number from which to start indexing events.
  startBlock: 3258331,
  // The target blockchain network (maps to a Hypersync endpoint).
  network: "megaethTestnet",
  // Controls the verbosity of console logging ('verbose', 'normal', 'event-only').
  logLevel: "event-only",
  // Supabase project URL, loaded from environment variables for security.
  supabaseUrl: process.env.SUPABASE_URL,
  // Supabase service role key, loaded from environment variables for security. Required for bypassing RLS.
  supabaseServiceKey: process.env.SUPABASE_SERVICE_ROLE_KEY,
};

// Validate that essential Supabase configuration variables are loaded from the environment.
if (!CONFIG.supabaseUrl || !CONFIG.supabaseServiceKey) {
  console.error(
    "Error: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables are required."
  );
  process.exit(1); // Exit if configuration is incomplete.
}

// Basic sanity checks for Supabase environment variables without logging the actual secrets.
console.log(`SUPABASE_URL format check: ${CONFIG.supabaseUrl?.startsWith('https://') ? 'OK' : 'INVALID'}`);
console.log(`SUPABASE_SERVICE_ROLE_KEY length check: ${CONFIG.supabaseServiceKey?.length > 30 ? 'OK' : 'INVALID'}`);


// Maps network names defined in CONFIG to their corresponding Hypersync API endpoints.
const NETWORK_URLS = {
  ethereum: "https://eth.hypersync.xyz",
  arbitrum: "https://arbitrum.hypersync.xyz",
  optimism: "https://optimism.hypersync.xyz",
  megaethTestnet: "https://megaeth-testnet.hypersync.xyz",
};

// --- Initialize Clients ---
// Creates a new Hypersync client instance configured with the URL for the target network.
const hypersyncClient = HypersyncClient.new({
  url: NETWORK_URLS[CONFIG.network], // Selects the correct Hypersync endpoint based on the config.
  timeout: 30000, // Sets a timeout for Hypersync requests (in milliseconds).
});

// Creates a new Supabase client instance using the URL and service key from config.
const supabase = createClient(CONFIG.supabaseUrl, CONFIG.supabaseServiceKey);

// --- Event Signatures and Topics ---
// Calculates the Keccak256 hash of the Tapped event signature. This is topic0 for this event.
const TAPPED_TOPIC = keccak256(
  toHex("Tapped(address,uint256,uint256,uint256)")
);
// Calculates the Keccak256 hash of the RoundEnded event signature. This is topic0 for this event.
const ROUND_ENDED_TOPIC = keccak256(
  toHex("RoundEnded(address,uint256,uint256,uint256)")
);
// List of topic0 hashes we are interested in. Hypersync uses this for efficient server-side filtering.
const topic0_list = [TAPPED_TOPIC, ROUND_ENDED_TOPIC];

// --- Constants ---
// Interval (in milliseconds) to wait before polling Hypersync again when at the chain tip.
const POLLING_INTERVAL = 200; // ms
// Small delay (in milliseconds) introduced between Supabase operations to avoid hitting rate limits.
const SUPABASE_RATE_LIMIT_DELAY = 100; // ms between operations

// --- Helper Functions ---

/**
 * @function formatAddress
 * @description Formats an Ethereum address for concise logging (e.g., 0x1234...abcd).
 * @param {string} address - The full Ethereum address string.
 * @returns {string} The formatted address or the original if invalid/too short.
 */
const formatAddress = (address) => {
  if (!address || address.length < 12) return address; // Basic validation
  return `${address.substring(0, 8)}...${address.substring(
    address.length - 4
  )}`;
};

/**
 * @function formatEth
 * @description Converts a Wei value (string or BigInt) to an ETH string, formatted to 4 decimal places.
 * @param {string | bigint} wei - The amount in Wei.
 * @returns {string} The formatted ETH string (e.g., "1.2345 ETH") or "N/A" on error.
 */
const formatEth = (wei) => {
  try {
    // Use BigInt for accurate calculations with potentially very large numbers.
    const weiBigInt = BigInt(wei);
    // Perform division using BigInt, then convert to Number for formatting.
    // Multiplying by 10000n before division preserves some decimal precision.
    const eth = Number((weiBigInt * 10000n) / 10n ** 18n) / 10000;
    return eth.toFixed(4) + " ETH";
  } catch (e) {
    // Log a warning if formatting fails (e.g., invalid input).
    console.warn(`Error formatting ETH value: ${wei}`, e);
    return wei?.toString() || "N/A"; // Return the original string or "N/A".
  }
};

/**
 * @function log
 * @description Centralized logging function with timestamping and level control based on CONFIG.logLevel.
 * @param {string} message - The message to log.
 * @param {string} [level='normal'] - The log level ('verbose', 'normal', 'event', 'startup', 'supabase', 'error').
 */
const log = (message, level = "normal") => {
  const timestamp = new Date().toISOString();
  const logMessage = `${timestamp} [${level.toUpperCase()}] ${message}`;

  if (level === "error") {
    console.error(logMessage); // Always log errors to stderr.
    return;
  }

  // Filter logs based on the configured logLevel.
  if (
    CONFIG.logLevel === "event-only" &&
    level !== "event" &&
    level !== "startup" &&
    level !== "supabase" // Allow essential levels even in 'event-only' mode
  ) {
    return; // Skip if level doesn't meet the 'event-only' criteria.
  }

  if (
    CONFIG.logLevel === "normal" || // Show normal, event, startup, supabase if level is normal
    level === "event" ||
    level === "startup" ||
    level === "supabase" ||
    CONFIG.logLevel === "verbose" // Show everything if level is verbose
  ) {
    console.log(logMessage); // Log to stdout if level criteria met.
  }
};

// --- Metrics and State ---
// Object to store counts of various events and operations for monitoring.
let eventCounts = {
  Tapped: 0,
  RoundEnded: 0,
  SupabaseInserts: 0,
  SupabaseErrors: 0,
};
// Stores the current round number derived from the latest event.
let currentRound = null;
// Stores the address of the last player who tapped.
let lastTapper = null;
// Stores the cost of the last tap event.
let tapCost = null;
// Stores the address of the winner of the last round.
let lastWinner = null;
// Stores the prize amount of the last round.
let lastPrize = null;
// Tracks the current block number the indexer is processing or waiting for. Initialized with startBlock.
let currentBlock = CONFIG.startBlock;
// Timestamp when the indexer process started, used for uptime calculation.
let startTime = performance.now();

// --- Supabase Upsert Function ---

/**
 * @function upsertEvent
 * @description Performs an 'upsert' operation (insert or update) into a specified Supabase table.
 *              Uses 'transaction_hash' and 'log_index' as conflict resolution keys.
 * @param {string} tableName - The name of the Supabase table (e.g., 'tapped_events').
 * @param {object} eventData - The data object to upsert. Column names should match Supabase schema.
 * @returns {Promise<{success: boolean, data: any, error: any}>} An object indicating success/failure and Supabase response.
 */
async function upsertEvent(tableName, eventData) {
  try {
    // Attempt to upsert the data into the specified table.
    const { data, error } = await supabase
      .from(tableName)
      .upsert(eventData, {
        // Specify the columns that define a unique constraint.
        // If a row with the same tx_hash and log_index exists, it will be updated; otherwise, inserted.
        onConflict: "transaction_hash,log_index",
      });

    if (error) {
      // Log detailed error information if the upsert fails.
      log(
        `Supabase upsert error (${tableName}): ${
          error.message || JSON.stringify(error) // Handle different error formats
        }`,
        "error"
      );
      log(`Failed Event Data: ${JSON.stringify(eventData)}`, "error");
      log(`Error Details: ${JSON.stringify(error || {})}`, "error");
      eventCounts.SupabaseErrors++;
      return { success: false, data: null, error };
    } else {
      // Log success message on successful upsert.
      log(
        `Successfully upserted event to ${tableName} (Tx: ${formatAddress(
          eventData.transaction_hash
        )}, Log: ${eventData.log_index})`,
        "supabase" // Use 'supabase' log level for DB operations.
      );
      eventCounts.SupabaseInserts++;
      return { success: true, data, error: null };
    }
  } catch (e) {
    // Catch any unexpected exceptions during the upsert process (e.g., network issues).
    log(`Exception in upsertEvent (${tableName}): ${e.message}`, "error");
    log(`Exception stack: ${e.stack}`, "error");
    eventCounts.SupabaseErrors++;
    return { success: false, error: e };
  }
}

// --- Supabase Upsert with Retry Function ---

/**
 * @function upsertEventWithRetry
 * @description Wraps `upsertEvent` with a retry mechanism using exponential backoff.
 * @param {string} tableName - The name of the Supabase table.
 * @param {object} eventData - The data object to upsert.
 * @param {number} [maxRetries=3] - The maximum number of retry attempts.
 * @returns {Promise<{success: boolean, data: any, error: any}>} Result of the upsert attempt.
 */
async function upsertEventWithRetry(tableName, eventData, maxRetries = 3) {
  let attempts = 0;

  while (attempts < maxRetries) {
    attempts++;
    const result = await upsertEvent(tableName, eventData); // Call the base upsert function.

    if (result.success) {
      if (attempts > 1) {
        // Log if the upsert succeeded after retrying.
        log(
          `Successfully upserted to ${tableName} after ${attempts} attempts`,
          "supabase"
        );
      }
      return result; // Return successful result immediately.
    }

    // If not successful and more retries are allowed:
    if (attempts < maxRetries) {
      // Calculate delay using exponential backoff (e.g., 500ms, 1000ms, 2000ms).
      const delay = Math.pow(2, attempts) * 500;
      log(
        `Retry ${attempts}/${maxRetries} for ${tableName} in ${delay}ms`,
        "supabase" // Log retry attempt.
      );
      // Wait for the calculated delay before the next attempt.
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }

  // If all retries fail, log the final failure.
  log(`Failed to upsert to ${tableName} after ${maxRetries} attempts`, "error");
  // Return a failure result.
  return {
    success: false,
    error: { message: `Failed after ${maxRetries} attempts` },
  };
}

// --- Test Supabase Connection Function ---

/**
 * @function testSupabaseConnection
 * @description Performs a simple query to check if the Supabase connection is working.
 * @returns {Promise<boolean>} True if the connection test is successful, false otherwise.
 */
async function testSupabaseConnection() {
  try {
    // Perform a lightweight query (count rows in 'tapped_events').
    // `head: true` makes it even lighter as it doesn't return the count value, just status.
    const { error, count } = await supabase
      .from("tapped_events") // Assumes this table exists. Might need a more generic test if not guaranteed.
      .select("*", { count: "exact", head: true }); // Use `count: 'exact'` for accuracy if `head: false`

    if (error) {
      // Log error if the test query fails.
      log(`Supabase connection test failed: ${error.message}`, "error");
      return false;
    }

    // Log success if the test query works.
    log(
      `Supabase connection test successful. Can query tapped_events.`, // Removed count display as head:true doesn't return it.
      "startup"
    );
    return true;
  } catch (e) {
    // Catch exceptions during the connection test (e.g., network issues, config errors).
    log(`Supabase connection test exception: ${e.message}`, "error");
    return false;
  }
}

// --- HTTP Server (for status/health check) ---
// Creates a simple HTTP server to provide status information about the indexer.
const server = http.createServer((req, res) => {
  const totalEvents = eventCounts.Tapped + eventCounts.RoundEnded;
  // Calculate uptime in seconds.
  const uptime = ((performance.now() - startTime) / 1000).toFixed(0);

  // Basic HTML response displaying current state and metrics.
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
        <p><strong>Current Block Being Processed:</strong> ${currentBlock}</p> {/* Clarified label */}
        <p><strong>Current Round (from latest event):</strong> ${currentRound || "Unknown"}</p> {/* Clarified label */}
        <p><strong>Last Tapper:</strong> ${
          formatAddress(lastTapper) || "Unknown"
        }</p>
        <p><strong>Current Tap Cost (from last tap):</strong> ${ // Clarified label
          formatEth(tapCost) || "Unknown"
        }</p>
        <p><strong>Last Winner:</strong> ${
          formatAddress(lastWinner) || "Unknown"
        }</p>
        <p><strong>Last Prize:</strong> ${formatEth(lastPrize) || "Unknown"}</p>
        <p><strong>Events Processed:</strong> ${totalEvents} (${
    eventCounts.Tapped
  } Taps, ${eventCounts.RoundEnded} Round Ends)</p>
        <p><strong>Supabase Upserts:</strong> ${eventCounts.SupabaseInserts}</p> {/* Changed label */}
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
  // Send the HTML response with a 200 OK status.
  res.writeHead(200, { "Content-Type": "text/html" });
  res.end(html);
});

// Starts the HTTP server, listening on port 8080 on all available network interfaces.
// '0.0.0.0' is important for accessibility within Docker containers or VMs.
server.listen(8080, "0.0.0.0", () => {
  log("Status web server running on port 8080", "startup");
});

// --- Main Function ---

/**
 * @function main
 * @description The core asynchronous function that sets up and runs the Hypersync event stream processing loop.
 */
async function main() {
  const runStartTime = performance.now(); // Track start time for progress logging within this run.
  let lastTipReachedTime = 0; // Timestamp when the chain tip was last reached.
  const chainTipReportInterval = 5 * 60 * 1000; // Log "at tip" message every 5 minutes.
  let stream = null; // Initialize stream variable outside try block for access in catch/finally.

  // Test Supabase connection before starting the main loop.
  const connectionOk = await testSupabaseConnection();
  if (!connectionOk) {
    // Log a warning but continue running; data storage might fail.
    // Consider exiting here (`process.exit(1)`) if Supabase connection is strictly required.
    log(
      "Warning: Supabase connection test failed. Data storage may not work correctly.",
      "error"
    );
  }

  try {
    log(`Starting Last Tap Tracker (Supabase Integrated)...`, "startup");
    log(
      `Network: ${CONFIG.network}, Contract: ${CONFIG.contractAddress}, Start Block: ${CONFIG.startBlock}`,
      "startup"
    );

    // Get the current height of the blockchain from Hypersync.
    const height = await hypersyncClient.getHeight();
    log(`Initial chain height: ${height}`, "startup");

    // Create a Decoder instance using the event signatures to parse log data.
    const decoder = Decoder.fromSignatures([
      "Tapped(address indexed tapper, uint256 roundNumber, uint256 tapCostPaid, uint256 timestamp)",
      "RoundEnded(address indexed winner, uint256 prizeAmount, uint256 roundNumber, uint256 timestamp)",
    ]);

    // Define the Hypersync query.
    let query = {
      // Start querying from the currentBlock (initially CONFIG.startBlock, then updated).
      fromBlock: currentBlock,
      // Specify the log filters.
      logs: [
        {
          // Only logs from our target contract address.
          address: [CONFIG.contractAddress],
          // Only logs matching the specified topic0 hashes (Tapped or RoundEnded events).
          topics: [topic0_list], // topic0_list = [TAPPED_TOPIC, ROUND_ENDED_TOPIC]
        },
      ],
      // Specify exactly which fields we need from the logs. This improves Hypersync performance.
      fieldSelection: {
        log: [
          LogField.BlockNumber,       // Block number the log is in.
          LogField.TransactionHash,   // Hash of the transaction that emitted the log.
          LogField.LogIndex,          // Index of the log within the block's logs. Crucial for unique identification.
          LogField.Data,              // Unindexed log data (needs decoding).
          LogField.Topic0,            // The event signature hash (used to identify event type).
          LogField.Topic1,            // First indexed topic (used for `tapper` in Tapped, `winner` in RoundEnded).
          // Add LogField.Topic2, LogField.Topic3 if other indexed fields were needed.
        ],
      },
      // Specify that we only need log data, not full transaction or block data.
      joinMode: JoinMode.JoinNothing,
    };

    log(`Starting event stream from block ${query.fromBlock}...`, "startup");
    // Initialize the Hypersync data stream with the defined query.
    stream = await hypersyncClient.stream(query, {});

    let lastProgressLogBlock = currentBlock; // Track the block number of the last progress log.
    let consecutiveChainTips = 0; // Counter for consecutive 'null' responses (indicates being at chain tip).

    // --- Main Processing Loop ---
    while (true) {
      // Wait for the next message (data chunk or null) from the Hypersync stream.
      const res = await stream.recv();

      // Check if the stream reached the tip of the chain (indicated by a null response).
      if (res === null) {
        const now = Date.now();
        consecutiveChainTips++;
        // Log that we're at the chain tip periodically or on the first occurrence.
        if (
          now - lastTipReachedTime > chainTipReportInterval ||
          consecutiveChainTips === 1 // Log immediately the first time
        ) {
          log(
            `Reached chain tip at block ${currentBlock}. Waiting for new blocks...`,
            "verbose" // Use verbose to avoid spamming logs when idle.
          );
          lastTipReachedTime = now;
        }
        // Wait for a short interval before checking the stream again.
        await new Promise((resolve) => setTimeout(resolve, POLLING_INTERVAL));

        // Optional optimization: Check chain height again. If it increased, restart the stream
        // immediately instead of waiting for the next `recv()` to return data. This can reduce latency.
        try {
          const newHeight = await hypersyncClient.getHeight();
          // Use `height` from the outer scope, which was the height when the stream *started*.
          // Compare `newHeight` to the block we've processed up to (`currentBlock`).
          if (newHeight >= currentBlock) { // If chain height is at or beyond our current block
            log(`Chain potentially advanced to ${newHeight}. Re-querying if needed by stream...`, "verbose");
            // Hypersync's stream implementation might handle this automatically upon the next `recv()`.
            // Explicitly restarting *could* be faster in some edge cases but adds complexity.
            // The current logic relies on the next `recv()` call to fetch new data if available.
            // To force restart:
            //   await stream.close();
            //   query.fromBlock = currentBlock; // Ensure query starts from the updated block
            //   stream = await hypersyncClient.stream(query, {});
            //   consecutiveChainTips = 0; // Reset counter
            //   lastTipReachedTime = 0;
          }
        } catch (err) {
           // Log errors during the optional height check but don't crash.
          log(`Error checking height while at tip: ${err.message}`, "error");
        }
        continue; // Go back to the start of the loop to call recv() again.
      }

      // If we received data (res is not null), reset the chain tip tracking variables.
      consecutiveChainTips = 0;
      lastTipReachedTime = 0;

      // Process the received data if it contains logs.
      if (res.data && res.data.logs && res.data.logs.length > 0) {
        // Decode the raw log data using the Decoder instance.
        const decodedLogs = await decoder.decodeLogs(res.data.logs);

        // Iterate through the decoded logs.
        for (let i = 0; i < decodedLogs.length; i++) {
          const decodedLog = decodedLogs[i];
          const rawLog = res.data.logs[i]; // Get the corresponding raw log for fields not in decodedLog.

          // Handle cases where a log couldn't be decoded (should be rare if signatures match).
          if (decodedLog === null) {
            log(
              `Skipping undecodable log at index ${i}, block ${rawLog.blockNumber}`,
              "verbose"
            );
            continue; // Skip to the next log.
          }

          // --- Extract Common Fields ---
          // These fields were requested in `fieldSelection`.
          const blockNumber = rawLog.blockNumber;
          const transactionHash = rawLog.transactionHash;
          const logIndex = rawLog.logIndex; // Log index within the block.

          // Validate essential identifiers. If missing, something is wrong with the Hypersync response or query.
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
            continue; // Skip this potentially corrupt log entry.
          }

          // Identify the event type based on the topic0 hash.
          const topic0 = rawLog.topics[0];
          const eventType = topic0 === TAPPED_TOPIC ? "Tapped" : "RoundEnded"; // Assumes only these two topics are possible due to query filter.

          // Wrap individual log processing in a try/catch to prevent one bad log from stopping the indexer.
          try {
            if (eventType === "Tapped") {
              eventCounts.Tapped++;

              // --- Extract Tapped Event Data ---
              // Indexed fields are in `decodedLog.indexed`, non-indexed in `decodedLog.body`.
              // `.val?.toString()` provides safe access and converts BigInts/Numbers to strings.
              const tapper = decodedLog.indexed[0]?.val?.toString(); // First indexed param (address tapper)
              const roundNumber = decodedLog.body[0]?.val?.toString();   // First body param (uint256 roundNumber)
              const tapCostPaid = decodedLog.body[1]?.val?.toString();   // Second body param (uint256 tapCostPaid)
              const timestampSeconds = decodedLog.body[2]?.val?.toString(); // Third body param (uint256 timestamp)
              // Convert UNIX timestamp (seconds) to a JavaScript Date object.
              const eventTimestamp = new Date(Number(timestampSeconds) * 1000);

              // Validate extracted data. Skip if essential fields are missing.
              if (
                !tapper ||
                !roundNumber ||
                !tapCostPaid ||
                !timestampSeconds
              ) {
                log(
                  `Missing data in decoded Tapped event: Tx ${formatAddress(
                    transactionHash
                  )}, Log ${logIndex}. Skipping. Decoded: ${JSON.stringify(decodedLog)}`, // Added decoded log for debugging
                  "error"
                );
                continue;
              }

              // Update the locally tracked state based on this event.
              currentRound = roundNumber;
              lastTapper = tapper;
              tapCost = tapCostPaid;

              // Log the processed event locally.
              log(
                `TAPPED | Blk: ${blockNumber} | Rnd: ${roundNumber} | Tapper: ${formatAddress(
                  tapper
                )} | Cost: ${formatEth(
                  tapCostPaid
                )} | ${eventTimestamp.toISOString()}`,
                "event" // Use 'event' log level.
              );

              // --- Prepare Data for Supabase ---
              // Map the extracted data to the Supabase table column names.
              // Ensure data types match the Supabase schema (e.g., use Number for numeric types if appropriate, but strings often safer for uint256).
              const tappedEventData = {
                block_number: Number(blockNumber),
                transaction_hash: String(transactionHash),
                log_index: Number(logIndex),
                tapper_address: String(tapper),
                round_number: String(roundNumber), // Store large numbers as strings
                tap_cost_paid: String(tapCostPaid), // Store large numbers as strings
                event_timestamp: eventTimestamp.toISOString(), // Use ISO string format for timestamps
              };

              // Upsert the event data to the 'tapped_events' table with retry logic.
              await upsertEventWithRetry("tapped_events", tappedEventData);

              // Introduce a small delay to avoid overwhelming Supabase.
              await new Promise(resolve => setTimeout(resolve, SUPABASE_RATE_LIMIT_DELAY));

            } else if (eventType === "RoundEnded") {
              eventCounts.RoundEnded++;

              // --- Extract RoundEnded Event Data ---
              const winner = decodedLog.indexed[0]?.val?.toString();     // First indexed param (address winner)
              const prizeAmount = decodedLog.body[0]?.val?.toString();   // First body param (uint256 prizeAmount)
              const roundNumber = decodedLog.body[1]?.val?.toString();   // Second body param (uint256 roundNumber)
              const timestampSeconds = decodedLog.body[2]?.val?.toString(); // Third body param (uint256 timestamp)
              const eventTimestamp = new Date(Number(timestampSeconds) * 1000);

              // Validate extracted data.
               if (
                !winner ||
                !prizeAmount ||
                !roundNumber ||
                !timestampSeconds
              ) {
                log(
                  `Missing data in decoded RoundEnded event: Tx ${formatAddress(
                    transactionHash
                  )}, Log ${logIndex}. Skipping. Decoded: ${JSON.stringify(decodedLog)}`, // Added decoded log for debugging
                  "error"
                );
                continue;
              }

              // Update local state.
              lastWinner = winner;
              lastPrize = prizeAmount;
              // Assume the round number displayed should be the *next* round after one ends.
              currentRound = (BigInt(roundNumber) + 1n).toString();

              // Log the processed event locally.
              log(
                `ROUND END | Blk: ${blockNumber} | Rnd: ${roundNumber} | Winner: ${formatAddress(
                  winner
                )} | Prize: ${formatEth(
                  prizeAmount
                )} | ${eventTimestamp.toISOString()}`,
                "event"
              );

              // --- Prepare Data for Supabase ---
              const roundEndedEventData = {
                block_number: Number(blockNumber),
                transaction_hash: String(transactionHash),
                log_index: Number(logIndex),
                winner_address: String(winner),
                prize_amount: String(prizeAmount), // Store large numbers as strings
                round_number: String(roundNumber), // Store large numbers as strings
                event_timestamp: eventTimestamp.toISOString(),
              };

              // Upsert the event data to the 'round_ended_events' table with retry logic.
              await upsertEventWithRetry("round_ended_events", roundEndedEventData);

              // Introduce a small delay.
               await new Promise(resolve => setTimeout(resolve, SUPABASE_RATE_LIMIT_DELAY));
            }
          } catch (processingError) {
            // Catch errors specifically related to processing a single log event.
            log(
              `Error processing log: ${
                processingError.message
              }. Tx: ${formatAddress(
                transactionHash
              )}, Log: ${logIndex}. Skipping event.`,
              "error"
            );
             log(`Processing Error Stack: ${processingError.stack}`, "error"); // Log stack trace for debugging.
             // Continue to the next log, do not stop the indexer.
          }
        } // End of log processing loop
      } // End of if(res.data.logs)

      // Update the current block number for the next iteration or stream restart.
      // `res.nextBlock` indicates the block number *after* the ones included in the current response.
      if (res.nextBlock) {
        currentBlock = res.nextBlock;
        // IMPORTANT: Update the query's fromBlock. If the stream disconnects and restarts,
        // it needs to know the correct block to resume from.
        query.fromBlock = currentBlock;

        // Log progress periodically based on block count.
        if (currentBlock - lastProgressLogBlock >= 10000) { // Log every 10k blocks
          const seconds = (performance.now() - runStartTime) / 1000;
          const totalEvents = eventCounts.Tapped + eventCounts.RoundEnded;
          log(
            `Progress: Reached block ${currentBlock} | ${totalEvents} events processed (${
              eventCounts.Tapped
            } Taps, ${eventCounts.RoundEnded} RoundEnds) | ${
              eventCounts.SupabaseInserts
            } DB inserts | ${seconds.toFixed(1)}s elapsed`,
            "normal"
          );
          lastProgressLogBlock = currentBlock;
        }
      } else if (res.data && res.data.logs && res.data.logs.length > 0) {
        // Handle edge case: Data received, but no `nextBlock` provided (might happen at the chain tip).
        // Update `currentBlock` to be one *after* the last processed log's block number.
        const lastLogBlock = res.data.logs[res.data.logs.length - 1]?.blockNumber;
        if (lastLogBlock && lastLogBlock >= currentBlock -1) { // Ensure it's actual progress or same block
          const nextBlockAfterLastLog = lastLogBlock + 1;
          if (nextBlockAfterLastLog > currentBlock) { // Only update if it moves forward
             currentBlock = nextBlockAfterLastLog;
             query.fromBlock = currentBlock; // Keep query's fromBlock consistent
             log(
               `Advanced currentBlock to ${currentBlock} based on last log received at tip`,
               "verbose"
             );
          }
        }
      }
    } // End of main processing while(true) loop
  } catch (error) {
    // Catch fatal errors in the main loop (e.g., stream connection failure, critical bugs).
    log(`Fatal error in main loop: ${error.message}`, "error");
    log(`Stack Trace: ${error.stack}`, "error"); // Log the full stack trace.

    // Attempt to gracefully close the Hypersync stream if it exists.
    if (stream) {
      try {
        await stream.close();
        log("Hypersync stream closed.", "normal");
      } catch (closeError) {
        log(`Error closing Hypersync stream: ${closeError.message}`, "error");
      }
    }

    // Implement a restart mechanism after a delay.
    log("Attempting restart in 30 seconds...", "normal");
    await new Promise((resolve) => setTimeout(resolve, 30000));
    main(); // Recursively call main() to restart the process. Be mindful of potential stack overflow on rapid failures.
  }
}

// --- Global Error Handlers ---
// Catch unhandled promise rejections (e.g., async operations without await or .catch).
process.on("unhandledRejection", (reason, promise) => {
  log(
    `Unhandled Rejection at: ${promise}, reason: ${reason?.message || reason}`,
    "error"
  );
  if (reason instanceof Error) {
    log(`Unhandled Rejection Stack: ${reason.stack}`, "error");
  }
  // Consider whether to exit or attempt recovery based on the error type.
  // Exiting might be safer: process.exit(1);
});

// Catch uncaught synchronous exceptions.
process.on("uncaughtException", (error) => {
  log(`Uncaught Exception: ${error.message}`, "error");
  log(`Uncaught Exception Stack: ${error.stack}`, "error");
  // It's generally recommended to exit after an uncaught exception, as the application state might be corrupt.
  process.exit(1);
});

// --- Start Application ---
// Execute the main function and catch any critical errors during the initial startup phase.
main().catch((error) => {
  log(`Critical startup error: ${error.message}`, "error");
  log(`Startup Error Stack: ${error.stack}`, "error");
  log("Exiting due to critical startup error.", "error");
  process.exit(1); // Exit if main() fails immediately on the first run.
});