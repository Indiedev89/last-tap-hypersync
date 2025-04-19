// last-tap-tracker-supabase-batch.js
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
  batchSize: 100, // Optional: Max number of events per Supabase batch (adjust as needed)
  maxRetries: 3, // Max retries for Supabase operations
  retryBaseDelay: 500, // Base delay for exponential backoff (ms)
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
  timeout: 30000, // Increased timeout for potentially larger responses
});

const supabase = createClient(CONFIG.supabaseUrl, CONFIG.supabaseServiceKey, {
    // Optional: Configure Supabase client further if needed
    // db: { schema: 'public' },
    // auth: { persistSession: false }
});

// --- Event Signatures and Topics ---
const TAPPED_TOPIC = keccak256(
  toHex("Tapped(address,uint256,uint256,uint256)")
);
const ROUND_ENDED_TOPIC = keccak256(
  toHex("RoundEnded(address,uint256,uint256,uint256)")
);
const topic0_list = [TAPPED_TOPIC, ROUND_ENDED_TOPIC];

const POLLING_INTERVAL = 200; // ms (Interval when at chain tip)

// --- Helper Functions ---
const formatAddress = (address) => {
  if (!address || address.length < 12) return address || "N/A";
  return `${address.substring(0, 8)}...${address.substring(
    address.length - 4
  )}`;
};

const formatEth = (wei) => {
  try {
    if (wei === null || wei === undefined) return "N/A";
    const weiBigInt = BigInt(wei);
    const eth = Number((weiBigInt * 10000n) / 10n ** 18n) / 10000;
    return eth.toFixed(4) + " ETH";
  } catch (e) {
    console.warn(`Error formatting ETH value: ${wei}`, e);
    return wei?.toString() || "N/A";
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
    level !== "supabase" || // Only show supabase logs if verbose or normal
    CONFIG.logLevel === "verbose"
  ) {
    console.log(logMessage);
  }
};

// --- Metrics and State ---
let eventCounts = {
  Tapped: 0,
  RoundEnded: 0,
  SupabaseBatchesSent: 0,
  SupabaseEventsUpserted: 0,
  SupabaseErrors: 0,
};
let currentRound = null;
let lastTapper = null;
let tapCost = null;
let lastWinner = null;
let lastPrize = null;
let currentBlock = CONFIG.startBlock;
let startTime = performance.now();

// --- Supabase Batch Upsert with Retry Function ---
/**
 * Upserts a batch of events to a specified Supabase table with retry logic.
 * @param {string} tableName - The name of the Supabase table.
 * @param {Array<object>} batchData - An array of event data objects to upsert.
 * @returns {Promise<{success: boolean, error: any | null}>} - Result of the batch operation.
 */
async function batchUpsertEventsWithRetry(tableName, batchData) {
  if (!batchData || batchData.length === 0) {
    return { success: true, error: null }; // Nothing to upsert
  }

  let attempts = 0;
  const batchSize = batchData.length; // Size of this specific batch

  while (attempts < CONFIG.maxRetries) {
    attempts++;
    try {
      const { error } = await supabase.from(tableName).upsert(batchData, {
        onConflict: "transaction_hash,log_index", // Ensure this matches your unique constraint
        // Consider `returning: 'minimal'` if you don't need the data back
      });

      if (error) {
        log(
          `Supabase batch upsert error (Attempt ${attempts}/${CONFIG.maxRetries}, Table: ${tableName}, Size: ${batchSize}): ${
            error.message || JSON.stringify(error)
          }`,
          "error"
        );
        log(`Error Details: ${JSON.stringify(error || {})}`, "error");
        // Optional: Log failed batch data (can be large)
        // if (attempts === CONFIG.maxRetries) {
        //   log(`Failed Batch Data (${tableName}): ${JSON.stringify(batchData)}`, "error");
        // }

        if (attempts < CONFIG.maxRetries) {
          const delay = Math.pow(2, attempts -1) * CONFIG.retryBaseDelay; // Exponential backoff
          log(
            `Retrying batch upsert to ${tableName} in ${delay}ms`,
            "supabase"
          );
          await new Promise(resolve => setTimeout(resolve, delay));
        } else {
          eventCounts.SupabaseErrors += batchSize; // Increment error count by batch size on final failure
          return { success: false, error };
        }
      } else {
        // Success
        log(
          `Successfully upserted batch of ${batchSize} events to ${tableName}${attempts > 1 ? ` after ${attempts} attempts` : ''}`,
          "supabase"
        );
        eventCounts.SupabaseBatchesSent++;
        eventCounts.SupabaseEventsUpserted += batchSize;
        return { success: true, error: null };
      }
    } catch (e) {
      // Catch unexpected errors during the upsert call
      log(
        `Exception during batch upsert (Attempt ${attempts}/${CONFIG.maxRetries}, Table: ${tableName}, Size: ${batchSize}): ${e.message}`,
        "error"
      );
      log(`Exception stack: ${e.stack}`, "error");

      if (attempts < CONFIG.maxRetries) {
        const delay = Math.pow(2, attempts - 1) * CONFIG.retryBaseDelay;
        log(
          `Retrying batch upsert to ${tableName} after exception in ${delay}ms`,
          "supabase"
        );
        await new Promise(resolve => setTimeout(resolve, delay));
      } else {
        eventCounts.SupabaseErrors += batchSize; // Increment error count by batch size on final failure
        return { success: false, error: e };
      }
    }
  }

  // Should theoretically not be reached if maxRetries > 0, but acts as a fallback
  log(`Failed to upsert batch to ${tableName} after ${CONFIG.maxRetries} attempts`, "error");
  return {
    success: false,
    error: { message: `Failed after ${CONFIG.maxRetries} attempts` },
  };
}


// --- Test Supabase Connection Function ---
async function testSupabaseConnection() {
  try {
    const { count, error } = await supabase
      .from("tapped_events") // Use a table you expect to exist
      .select("*", { count: "exact", head: true }); // More reliable way to check connection/perms

    if (error) {
      log(`Supabase connection test failed: ${error.message}`, "error");
      log(`Supabase error details: ${JSON.stringify(error)}`, "error");
      return false;
    }

    log(
      `Supabase connection test successful. Table 'tapped_events' accessible.`,
      "startup"
    );
    return true;
  } catch (e) {
    log(`Supabase connection test exception: ${e.message}`, "error");
    return false;
  }
}

// --- HTTP Server (for status/health check - unchanged) ---
const server = http.createServer((req, res) => {
  const totalEvents = eventCounts.Tapped + eventCounts.RoundEnded;
  const uptime = ((performance.now() - startTime) / 1000).toFixed(0);
  const eventsPerSecond = uptime > 0 ? (totalEvents / uptime).toFixed(2) : 0;
  const dbOpsPerSecond = uptime > 0 ? (eventCounts.SupabaseEventsUpserted / uptime).toFixed(2) : 0;


  const html = `
<!DOCTYPE html>
<html lang="en">
<head>
  <title>Last Tap Tracker Dashboard</title>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style>
    :root {
      --primary: #4F46E5;
      --primary-light: #818CF8;
      --primary-dark: #3730A3;
      --success: #10B981;
      --warning: #F59E0B;
      --danger: #EF4444;
      --dark: #1F2937;
      --light: #F9FAFB;
      --gray: #9CA3AF;
      --gray-light: #E5E7EB;
    }

    * {
      margin: 0;
      padding: 0;
      box-sizing: border-box;
    }

    body {
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
      line-height: 1.6;
      color: var(--dark);
      background-color: #F3F4F6;
      padding: 0;
      margin: 0;
    }

    .container {
      max-width: 1000px;
      margin: 0 auto;
      padding: 1rem;
    }

    .header {
      background-color: var(--primary);
      color: white;
      padding: 1.5rem 0;
      box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
      margin-bottom: 2rem;
    }

    .header h1 {
      font-size: 1.8rem;
      font-weight: 700;
      margin: 0;
      text-align: center;
    }

    .header p {
      opacity: 0.8;
      margin-top: 0.5rem;
      text-align: center;
    }

    .card {
      background: white;
      border-radius: 0.75rem;
      box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px 0 rgba(0, 0, 0, 0.06);
      padding: 1.5rem;
      margin-bottom: 1.5rem;
    }

    .card-header {
      display: flex;
      align-items: center;
      margin-bottom: 1rem;
      padding-bottom: 0.5rem;
      border-bottom: 1px solid var(--gray-light);
    }

    .card-header h2 {
      font-size: 1.25rem;
      font-weight: 600;
      color: var(--primary-dark);
      margin: 0;
    }

    .card-header .icon {
      margin-right: 0.75rem;
      color: var(--primary);
      font-size: 1.25rem;
    }

    .status-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); /* Responsive grid */
      gap: 1.5rem;
    }

    .stat-item {
      display: flex;
      flex-direction: column; /* Stack label and value */
      margin-bottom: 1rem;
    }

    .stat-label {
      font-weight: 500;
      color: var(--gray);
      font-size: 0.875rem; /* Smaller label */
      margin-bottom: 0.25rem;
    }

    .stat-value {
      color: var(--primary-dark);
      font-weight: 600;
      word-break: break-all; /* Prevent long addresses from breaking layout */
    }

    .status-indicator {
      display: inline-flex;
      align-items: center;
      padding: 0.25rem 0.75rem;
      border-radius: 9999px;
      font-size: 0.875rem;
      font-weight: 500;
    }

    .status-running {
      background-color: rgba(16, 185, 129, 0.1);
      color: var(--success);
    }

    .address {
      font-family: monospace;
      font-size: 0.9em;
      background-color: rgba(79, 70, 229, 0.1);
      padding: 0.1rem 0.3rem;
      border-radius: 0.25rem;
      color: var(--primary-dark);
    }

    .network-badge {
      display: inline-block;
      padding: 0.25rem 0.75rem;
      border-radius: 9999px;
      font-size: 0.75rem;
      font-weight: 500;
      background-color: var(--primary-light);
      color: white;
      margin-left: 0.75rem;
    }

    .info-section {
      display: grid; /* Use grid for info boxes */
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); /* Responsive */
      gap: 1rem;
      margin-bottom: 1rem;
    }

    .info-box {
      padding: 1rem;
      background-color: #F9FAFB;
      border-radius: 0.5rem;
      border-left: 4px solid var(--primary);
    }

     .info-box.error { /* Style for error box */
        border-left-color: var(--danger);
     }
     .info-box.error h3 {
        color: var(--danger);
     }
      .info-box.error p {
        color: var(--danger);
        font-weight: 700;
      }
       .info-box.success p {
        color: var(--success);
         font-weight: 700;
      }


    .info-box h3 {
      font-size: 0.875rem;
      color: var(--gray);
      margin-bottom: 0.5rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }

    .info-box p {
      font-size: 1.25rem;
      font-weight: 600;
      color: var(--primary-dark);
    }

    .refresh-notice {
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 0.875rem;
      color: var(--gray);
      margin-top: 1rem;
    }

    .pulse {
      display: inline-block;
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background-color: var(--success);
      margin-right: 0.5rem;
      animation: pulse 2s infinite;
    }

    @keyframes pulse {
      0% {
        box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.4);
      }
      70% {
        box-shadow: 0 0 0 10px rgba(16, 185, 129, 0);
      }
      100% {
        box-shadow: 0 0 0 0 rgba(16, 185, 129, 0);
      }
    }

    .footer {
      text-align: center;
      margin-top: 2rem;
      padding: 1rem 0;
      color: var(--gray);
      font-size: 0.875rem;
    }
  </style>
</head>
<body>
  <header class="header">
    <div class="container">
      <h1>Last Tap Game Event Tracker</h1>
      <p>Real-time monitoring dashboard (Batch Upsert Version)</p>
    </div>
  </header>

  <div class="container">
    <div class="card">
      <div class="card-header">
        <h2>System Status <span class="network-badge">${CONFIG.network}</span></h2>
      </div>

      <div class="info-section">
        <div class="info-box">
          <h3>Status</h3>
          <p><span class="status-indicator status-running">Running</span></p>
        </div>
        <div class="info-box">
          <h3>Uptime</h3>
          <p>${uptime} s</p>
        </div>
        <div class="info-box">
          <h3>Current Block</h3>
          <p>${currentBlock}</p>
        </div>
         <div class="info-box">
          <h3>Events/Sec</h3>
          <p>${eventsPerSecond}</p>
        </div>
         <div class="info-box">
          <h3>DB Ops/Sec</h3>
          <p>${dbOpsPerSecond}</p>
        </div>
      </div>

      <div class="card-header">
        <h2>Game State</h2>
      </div>

      <div class="status-grid">
        <div class="stat-item">
          <div class="stat-label">Current Round:</div>
          <div class="stat-value">${currentRound || "Unknown"}</div>
        </div>
        <div class="stat-item">
          <div class="stat-label">Last Tapper:</div>
          <div class="stat-value">
            ${lastTapper ? `<span class="address">${formatAddress(lastTapper)}</span>` : "Unknown"}
          </div>
        </div>
        <div class="stat-item">
          <div class="stat-label">Current Tap Cost:</div>
          <div class="stat-value">${formatEth(tapCost) || "Unknown"}</div>
        </div>
        <div class="stat-item">
          <div class="stat-label">Last Winner:</div>
          <div class="stat-value">
            ${lastWinner ? `<span class="address">${formatAddress(lastWinner)}</span>` : "Unknown"}
          </div>
        </div>
        <div class="stat-item">
          <div class="stat-label">Last Prize:</div>
          <div class="stat-value">${formatEth(lastPrize) || "Unknown"}</div>
        </div>
        <div class="stat-item">
          <div class="stat-label">Contract Address:</div>
          <div class="stat-value">
            <span class="address">${formatAddress(CONFIG.contractAddress)}</span>
          </div>
        </div>
      </div>
    </div>

    <div class="card events-card">
      <div class="card-header">
        <h2>Event & Database Statistics</h2>
      </div>

      <div class="info-section">
        <div class="info-box">
          <h3>Total Events Processed</h3>
          <p>${eventCounts.Tapped + eventCounts.RoundEnded}</p>
        </div>
        <div class="info-box">
          <h3>Taps</h3>
          <p>${eventCounts.Tapped}</p>
        </div>
        <div class="info-box">
          <h3>Round Ends</h3>
          <p>${eventCounts.RoundEnded}</p>
        </div>
        <div class="info-box">
          <h3>DB Batches Sent</h3>
           <p>${eventCounts.SupabaseBatchesSent}</p>
        </div>
        <div class="info-box">
          <h3>DB Events Upserted</h3>
          <p>${eventCounts.SupabaseEventsUpserted}</p>
        </div>
        <div class="info-box ${eventCounts.SupabaseErrors > 0 ? 'error' : 'success'}">
          <h3>DB Errors</h3>
          <p>${eventCounts.SupabaseErrors}</p>
        </div>
      </div>
    </div>

    <div class="refresh-notice">
      <span class="pulse"></span> Auto-refreshes on page reload
    </div>
  </div>

  <footer class="footer">
    <div class="container">
      <p>Last Tap Tracker • Running Headless • <span id="current-time"></span></p>
    </div>
  </footer>

  <script>
    function updateTime() {
      const timeElement = document.getElementById('current-time');
      if (timeElement) {
        timeElement.textContent = new Date().toLocaleString();
      }
    }
    updateTime();
    setInterval(updateTime, 1000);
  </script>
</body>
</html>
  `;
  res.writeHead(200, { "Content-Type": "text/html" });
  res.end(html);
});

server.listen(8080, "0.0.0.0", () => {
  log("Status web server running on port 8080", "startup");
});

// --- Main Function ---
async function main() {
  const runStartTime = performance.now();
  let lastTipReachedTime = 0;
  const chainTipReportInterval = 5 * 60 * 1000; // 5 minutes

  // Test Supabase connection at startup
  const connectionOk = await testSupabaseConnection();
  if (!connectionOk) {
    log(
      "CRITICAL: Supabase connection test failed. Exiting.",
      "error"
    );
    process.exit(1); // Exit if connection fails at start
  }

  try {
    log(`Starting Last Tap Tracker (Supabase Batch Upsert)...`, "startup");
    log(
      `Network: ${CONFIG.network}, Contract: ${CONFIG.contractAddress}, Start Block: ${CONFIG.startBlock}`,
      "startup"
    );

    let height = await hypersyncClient.getHeight();
    log(`Initial chain height: ${height}`, "startup");

    const decoder = Decoder.fromSignatures([
      "Tapped(address indexed tapper, uint256 roundNumber, uint256 tapCostPaid, uint256 timestamp)",
      "RoundEnded(address indexed winner, uint256 prizeAmount, uint256 roundNumber, uint256 timestamp)",
    ]);

    // Define the query
    let query = {
      fromBlock: currentBlock,
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
          LogField.LogIndex,
          LogField.Data,
          LogField.Topic0,
          LogField.Topic1, // Tapped: tapper, RoundEnded: winner
          // No other indexed fields in these events
        ],
      },
      // Consider JoinTransaction if you need tx fields like 'from' or 'value'
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

        // Optional: Check height again
        try {
          const newHeight = await hypersyncClient.getHeight();
          if (newHeight > height) {
            log(`Chain advanced to ${newHeight}. Re-querying...`, "verbose");
            await stream.close();
            height = newHeight; // Update known height
            query.fromBlock = currentBlock;
            stream = await hypersyncClient.stream(query, {});
            consecutiveChainTips = 0;
          }
        } catch (err) {
          log(`Error checking height while at tip: ${err.message}`, "error");
          // Consider more robust error handling here, maybe retry connection
        }
        continue;
      }

      // Reset consecutive chain tips counter on receiving data
      consecutiveChainTips = 0;
      lastTipReachedTime = 0;

      // --- Batch Processing Logic ---
      let tappedBatch = [];
      let roundEndedBatch = [];

      if (res.data && res.data.logs && res.data.logs.length > 0) {
        const decodedLogs = await decoder.decodeLogs(res.data.logs);

        for (let i = 0; i < decodedLogs.length; i++) {
          const decodedLog = decodedLogs[i];
          const rawLog = res.data.logs[i];

          if (decodedLog === null) {
            log(
              `Skipping undecodable log at index ${i}, block ${rawLog.blockNumber}`,
              "verbose"
            );
            continue;
          }

          const blockNumber = rawLog.blockNumber;
          const transactionHash = rawLog.transactionHash;
          const logIndex = rawLog.logIndex;

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
            if (eventType === "Tapped") {
              eventCounts.Tapped++;

              const tapper = decodedLog.indexed[0]?.val?.toString();
              const roundNumber = decodedLog.body[0]?.val?.toString();
              const tapCostPaid = decodedLog.body[1]?.val?.toString();
              const timestampSeconds = decodedLog.body[2]?.val?.toString();

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

              const eventTimestamp = new Date(Number(timestampSeconds) * 1000);
              currentRound = roundNumber;
              lastTapper = tapper;
              tapCost = tapCostPaid;

              log(
                `TAPPED | Blk: ${blockNumber} | Rnd: ${roundNumber} | Tapper: ${formatAddress(
                  tapper
                )} | Cost: ${formatEth(
                  tapCostPaid
                )} | ${eventTimestamp.toISOString()}`,
                "event"
              );

              // Add to batch instead of upserting immediately
              tappedBatch.push({
                block_number: Number(blockNumber),
                transaction_hash: String(transactionHash),
                log_index: Number(logIndex),
                tapper_address: String(tapper),
                round_number: String(roundNumber),
                tap_cost_paid: String(tapCostPaid),
                event_timestamp: eventTimestamp.toISOString(),
              });

            } else if (eventType === "RoundEnded") {
              eventCounts.RoundEnded++;

              const winner = decodedLog.indexed[0]?.val?.toString();
              const prizeAmount = decodedLog.body[0]?.val?.toString();
              const roundNumber = decodedLog.body[1]?.val?.toString();
              const timestampSeconds = decodedLog.body[2]?.val?.toString();


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

              const eventTimestamp = new Date(Number(timestampSeconds) * 1000);
              lastWinner = winner;
              lastPrize = prizeAmount;
              currentRound = (BigInt(roundNumber) + 1n).toString(); // Update for display

              log(
                `ROUND END | Blk: ${blockNumber} | Rnd: ${roundNumber} | Winner: ${formatAddress(
                  winner
                )} | Prize: ${formatEth(
                  prizeAmount
                )} | ${eventTimestamp.toISOString()}`,
                "event"
              );

              // Add to batch
              roundEndedBatch.push({
                block_number: Number(blockNumber),
                transaction_hash: String(transactionHash),
                log_index: Number(logIndex),
                winner_address: String(winner),
                prize_amount: String(prizeAmount),
                round_number: String(roundNumber),
                event_timestamp: eventTimestamp.toISOString(),
              });
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
            log(`Processing Error Stack: ${processingError.stack}`, "error");
          }
        } // End of log processing loop

        // --- Send Batches to Supabase ---
        // Use Promise.all to send batches concurrently (optional, can also do sequentially)
        const batchPromises = [];
        if (tappedBatch.length > 0) {
            batchPromises.push(batchUpsertEventsWithRetry("tapped_events", tappedBatch));
        }
        if (roundEndedBatch.length > 0) {
            batchPromises.push(batchUpsertEventsWithRetry("round_ended_events", roundEndedBatch));
        }

        if (batchPromises.length > 0) {
            const results = await Promise.all(batchPromises);
            // Optional: Check results for failures if needed for more granular error handling
            results.forEach(result => {
                if (!result.success) {
                    // Log final failure of a batch after retries
                    log(`Failed to upsert a batch after all retries. Error: ${JSON.stringify(result.error)}`, "error");
                    // Note: SupabaseErrors count is already incremented within the batch function
                }
            });
        }

      } // End of if(res.data...)

      // Update block position for the next query iteration
      if (res.nextBlock) {
        const previousBlock = currentBlock;
        currentBlock = res.nextBlock;
        query.fromBlock = currentBlock; // Update query state

        // Log progress occasionally
        if (currentBlock - lastProgressLogBlock >= 10000) {
          const seconds = (performance.now() - runStartTime) / 1000;
          const totalEvents = eventCounts.Tapped + eventCounts.RoundEnded;
          log(
            `Progress: Block ${currentBlock} | ${totalEvents} events processed | ${eventCounts.SupabaseEventsUpserted} DB upserts | ${eventCounts.SupabaseErrors} DB errors | ${seconds.toFixed(1)}s`,
            "normal"
          );
          lastProgressLogBlock = currentBlock;
        }
      } else if (res.data && res.data.logs && res.data.logs.length > 0) {
        // If data received but no nextBlock (at tip), update block based on last log
        const lastLogBlock =
          res.data.logs[res.data.logs.length - 1]?.blockNumber;
        if (lastLogBlock && lastLogBlock >= currentBlock) {
          currentBlock = lastLogBlock + 1; // Start next query from the block *after* the last one processed
          query.fromBlock = currentBlock;
          log(
            `Advanced currentBlock to ${currentBlock} based on last log received at chain tip`,
            "verbose"
          );
        }
      }
    } // End of while(true) loop
  } catch (error) {
    log(`Fatal error in main loop: ${error.message}`, "error");
    log(`Stack Trace: ${error.stack}`, "error");

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

// --- Global Error Handlers (Unchanged) ---
process.on("unhandledRejection", (reason, promise) => {
  log(
    `Unhandled Rejection at: ${promise}, reason: ${reason?.message || reason}`,
    "error"
  );
  if (reason instanceof Error) {
    log(`Unhandled Rejection Stack: ${reason.stack}`, "error");
  }
  // Consider more graceful shutdown or specific recovery based on the error
  // process.exit(1); // Might be too aggressive
});

process.on("uncaughtException", (error) => {
  log(`Uncaught Exception: ${error.message}`, "error");
  log(`Uncaught Exception Stack: ${error.stack}`, "error");
  // Recommended to exit cleanly after an uncaught exception
  process.exit(1);
});

// --- Start Application ---
main().catch((error) => {
  log(`Critical startup error: ${error.message}`, "error");
  log(`Startup Error Stack: ${error.stack}`, "error");
  log("Exiting due to critical startup error.", "error");
  process.exit(1);
});
