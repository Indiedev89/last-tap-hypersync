// last-tap-tracker-supabase-batch-dual-rpc.js
import { keccak256, toHex } from "viem";
import {
  HypersyncClient,
  LogField,
  JoinMode,
  Decoder,
} from "@envio-dev/hypersync-client";
import http from "http";
import { createClient } from "@supabase/supabase-js";
import pg from "pg";
const { Pool } = pg;
import "dotenv/config";

// --- Configuration ---
const CONFIG = {
  contractAddress: process.env.GAME_CONTRACT_ADDRESS,
  startBlock: 5507082,
  network: "megaethTestnet",
  logLevel: "event-only", // 'verbose', 'normal', 'event-only'
  supabaseUrl: process.env.SUPABASE_URL,
  supabaseServiceKey: process.env.SUPABASE_SERVICE_ROLE_KEY,
  supabaseDbConnectionString: process.env.SUPABASE_DB_CONNECTION_STRING,
  batchSize: 100,
  maxRetries: 20,
  retryBaseDelay: 500,
};

// --- Enhanced Network URL mapping with fallback RPCs ---
const NETWORK_URLS = {
  ethereum: ["https://eth.hypersync.xyz"],
  arbitrum: ["https://arbitrum.hypersync.xyz"],
  optimism: ["https://optimism.hypersync.xyz"],
  megaethTestnet: [
    "https://megaeth-testnet.hypersync.xyz",
    "https://6342.rpc.hypersync.xyz"
  ],
};

// Track which RPC we're currently using
let currentRpcIndex = 0;
let currentRpcUrl = null;

// --- Helper Functions ---
const log = (message, level = "normal") => {
  const timestamp = new Date().toISOString();
  const logMessage = `${timestamp} [${level.toUpperCase()}] ${message}`;

  if (level === "error") {
    console.error(logMessage);
    return;
  }

  if (
    CONFIG.logLevel === "verbose" ||
    level === "event" ||
    level === "startup" ||
    level === "supabase" ||
    level === "db_setup"
  ) {
    console.log(logMessage);
  } else if (CONFIG.logLevel === "normal" && level !== "verbose") {
    console.log(logMessage);
  } else if (
    CONFIG.logLevel === "event-only" &&
    (level === "event" || level === "startup" || level === "db_setup")
  ) {
    console.log(logMessage);
  }
};

const formatAddress = (address) => {
  if (!address || address.length < 12) return address || "N/A";
  return `${address.substring(0, 8)}...${address.substring(
    address.length - 4
  )}`;
};

const formatTokens = (wei) => {
  try {
    if (wei === null || wei === undefined) return "N/A";
    const weiBigInt = BigInt(wei);
    const eth = Number((weiBigInt * 10000n) / 10n ** 18n) / 10000;
    return eth.toFixed(4) + " TAP";
  } catch (e) {
    log(`Error formatting TAP value: ${wei} - ${e.message}`, "error");
    return wei?.toString() || "N/A";
  }
};

// --- RPC Management Functions ---
function getCurrentRpcUrl(network) {
  const urls = NETWORK_URLS[network];
  if (!urls || urls.length === 0) {
    throw new Error(`No RPC URLs configured for network: ${network}`);
  }

  currentRpcIndex = currentRpcIndex % urls.length;
  currentRpcUrl = urls[currentRpcIndex];
  return currentRpcUrl;
}

function switchToNextRpc(network) {
  const urls = NETWORK_URLS[network];
  if (!urls || urls.length <= 1) {
    return false;
  }

  const previousUrl = currentRpcUrl;
  currentRpcIndex = (currentRpcIndex + 1) % urls.length;
  currentRpcUrl = urls[currentRpcIndex];

  log(`🔄 Switching RPC from ${previousUrl} to ${currentRpcUrl}`, 'startup');
  return true;
}

// --- Validate Configuration ---
if (!CONFIG.supabaseUrl || !CONFIG.supabaseServiceKey) {
  log("Error: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables are required.", "error");
  process.exit(1);
}
if (!CONFIG.supabaseDbConnectionString) {
  log("Error: SUPABASE_DB_CONNECTION_STRING environment variable is required for initial setup.", "error");
  process.exit(1);
}

log(`SUPABASE_URL format check: ${CONFIG.supabaseUrl?.startsWith("https://") ? "OK" : "INVALID"}`, "startup");
log(`SUPABASE_SERVICE_ROLE_KEY length check: ${CONFIG.supabaseServiceKey?.length > 30 ? "OK" : "INVALID"}`, "startup");
log(`SUPABASE_DB_CONNECTION_STRING format check: ${CONFIG.supabaseDbConnectionString?.startsWith("postgresql://") ? "OK" : "INVALID"}`, "startup");
log(`🌐 Available RPCs for ${CONFIG.network}: ${NETWORK_URLS[CONFIG.network].join(', ')}`, 'startup');

// --- Enhanced Hypersync client creation with RPC fallback ---
async function createHypersyncClientWithFallback(maxRetries = 3) {
  const urls = NETWORK_URLS[CONFIG.network];
  if (!urls || urls.length === 0) {
    throw new Error(`No RPC URLs configured for network: ${CONFIG.network}`);
  }

  let totalAttempts = 0;
  const maxTotalAttempts = maxRetries * urls.length;

  while (totalAttempts < maxTotalAttempts) {
    const rpcUrl = getCurrentRpcUrl(CONFIG.network);
    totalAttempts++;

    try {
      log(`🔌 Attempting to connect to ${rpcUrl} (attempt ${totalAttempts}/${maxTotalAttempts})`, 'startup');

      const client = HypersyncClient.new({
        url: rpcUrl,
        bearerToken: process.env.HYPERSYNC_BEARER_TOKEN,
        timeout: 30000,
      });

      const height = await client.getHeight();
      log(`✅ Successfully connected to ${rpcUrl}. Chain height: ${height}`, 'startup');
      return { client, rpcUrl };

    } catch (error) {
      log(`❌ Failed to connect to ${rpcUrl}: ${error.message}`, 'error');

      const isConnectionError = error.message.includes('503') ||
                               error.message.includes('Service Unavailable') ||
                               error.message.includes('timeout') ||
                               error.message.includes('ECONNREFUSED') ||
                               error.message.includes('ENOTFOUND');

      if (isConnectionError && switchToNextRpc(CONFIG.network)) {
        log(`Connection error detected, trying next RPC...`, 'startup');
        continue;
      }

      if (totalAttempts < maxTotalAttempts) {
        const delay = Math.min(1000 * Math.pow(2, Math.floor(totalAttempts / urls.length)), 30000);
        log(`Retrying in ${delay}ms...`, 'startup');
        await new Promise(resolve => setTimeout(resolve, delay));

        switchToNextRpc(CONFIG.network);
      }
    }
  }

  throw new Error(`Failed to connect to any Hypersync RPC after ${maxTotalAttempts} attempts across ${urls.length} endpoints`);
}

async function createStreamWithFallback(client, query, maxRetries = 3) {
  let attempts = 0;

  while (attempts < maxRetries) {
    attempts++;
    try {
      log(`Creating stream (attempt ${attempts}/${maxRetries})...`, 'startup');
      const stream = await client.stream(query, {});
      log(`✅ Stream created successfully`, 'startup');
      return stream;
    } catch (error) {
      log(`❌ Stream creation failed (attempt ${attempts}/${maxRetries}): ${error.message}`, 'error');

      const isConnectionError = error.message.includes('503') ||
                               error.message.includes('Service Unavailable') ||
                               error.message.includes('timeout');

      if (isConnectionError && attempts === maxRetries) {
        throw new Error(`Stream creation failed after ${maxRetries} attempts: ${error.message}`);
      }

      if (attempts < maxRetries) {
        const delay = 1000 * attempts;
        log(`Retrying stream creation in ${delay}ms...`, 'startup');
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }

  throw new Error(`Failed to create stream after ${maxRetries} attempts`);
}

// --- Initialize Supabase Client ---
const supabase = createClient(CONFIG.supabaseUrl, CONFIG.supabaseServiceKey);

// --- Database Setup SQL ---
const SETUP_SQL = `
CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.tapped_events_2 (
    block_number BIGINT NOT NULL,
    transaction_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    round_number TEXT NOT NULL,
    player TEXT NOT NULL,
    cost TEXT NOT NULL,
    new_end_time TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    contract_address TEXT NOT NULL,
    PRIMARY KEY (transaction_hash, log_index)
);

CREATE TABLE IF NOT EXISTS public.round_ended_events_2 (
    block_number BIGINT NOT NULL,
    transaction_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    round_number TEXT NOT NULL,
    winner TEXT NOT NULL,
    prize TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    contract_address TEXT NOT NULL,
    PRIMARY KEY (transaction_hash, log_index)
);

CREATE INDEX IF NOT EXISTS idx_tapped_2_round_number ON public.tapped_events_2(round_number);
CREATE INDEX IF NOT EXISTS idx_tapped_2_player ON public.tapped_events_2(player);
CREATE INDEX IF NOT EXISTS idx_tapped_2_timestamp ON public.tapped_events_2(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_tapped_2_contract ON public.tapped_events_2(contract_address);
CREATE INDEX IF NOT EXISTS idx_round_ended_2_round ON public.round_ended_events_2(round_number);
CREATE INDEX IF NOT EXISTS idx_round_ended_2_winner ON public.round_ended_events_2(winner);
CREATE INDEX IF NOT EXISTS idx_round_ended_2_timestamp ON public.round_ended_events_2(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_round_ended_2_contract ON public.round_ended_events_2(contract_address);

GRANT USAGE ON SCHEMA public TO postgres, anon, authenticated, service_role;
GRANT SELECT ON TABLE public.tapped_events_2 TO anon, authenticated;
GRANT SELECT ON TABLE public.round_ended_events_2 TO anon, authenticated;
GRANT ALL PRIVILEGES ON TABLE public.tapped_events_2 TO postgres, service_role;
GRANT ALL PRIVILEGES ON TABLE public.round_ended_events_2 TO postgres, service_role;

ALTER TABLE public.tapped_events_2 ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.round_ended_events_2 ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Anon can read tapped_events_2" ON public.tapped_events_2;
DROP POLICY IF EXISTS "Service roles full access tapped_events_2" ON public.tapped_events_2;
DROP POLICY IF EXISTS "Anon can read round_ended_events_2" ON public.round_ended_events_2;
DROP POLICY IF EXISTS "Service roles full access round_ended_events_2" ON public.round_ended_events_2;

CREATE POLICY "Anon can read tapped_events_2" ON public.tapped_events_2 FOR SELECT TO anon USING (true);
CREATE POLICY "Service roles full access tapped_events_2" ON public.tapped_events_2 FOR ALL TO service_role, postgres USING (true) WITH CHECK (true);
CREATE POLICY "Anon can read round_ended_events_2" ON public.round_ended_events_2 FOR SELECT TO anon USING (true);
CREATE POLICY "Service roles full access round_ended_events_2" ON public.round_ended_events_2 FOR ALL TO service_role, postgres USING (true) WITH CHECK (true);

-- Views will be created later after we add contract_address to old tables
-- For now, just create the new tables

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'supabase_realtime') THEN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication_tables
        WHERE pubname = 'supabase_realtime' AND schemaname = 'public' AND tablename = 'tapped_events_2'
    ) THEN
        ALTER PUBLICATION supabase_realtime ADD TABLE public.tapped_events_2;
        RAISE NOTICE 'Added public.tapped_events_2 to supabase_realtime publication.';
    ELSE
        RAISE NOTICE 'public.tapped_events_2 already in supabase_realtime publication.';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_publication_tables
        WHERE pubname = 'supabase_realtime' AND schemaname = 'public' AND tablename = 'round_ended_events_2'
    ) THEN
        ALTER PUBLICATION supabase_realtime ADD TABLE public.round_ended_events_2;
        RAISE NOTICE 'Added public.round_ended_events_2 to supabase_realtime publication.';
    ELSE
        RAISE NOTICE 'public.round_ended_events_2 already in supabase_realtime publication.';
    END IF;
  ELSE
    RAISE WARNING 'Publication supabase_realtime does not exist.';
  END IF;
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE WARNING 'Insufficient privileges to modify publication supabase_realtime.';
  WHEN OTHERS THEN
    RAISE WARNING 'An error occurred during publication setup: SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
END
$$;
`;

async function runDatabaseSetup() {
  log("Starting database schema setup...", "db_setup");
  const pool = new Pool({
    connectionString: CONFIG.supabaseDbConnectionString,
  });
  let client;

  try {
    client = await pool.connect();
    log("Database connection successful for setup.", "db_setup");
    await client.query(SETUP_SQL);
    log("Database schema setup script executed successfully.", "db_setup");
  } catch (err) {
    log(`Database setup failed: ${err.message}`, "error");
    log(`SQL Error Details: ${err.stack}`, "error");
    log("Exiting due to critical database setup failure.", "error");
    process.exit(1);
  } finally {
    if (client) {
      client.release();
      log("Database setup client released.", "db_setup");
    }
    await pool.end();
    log("Database setup pool closed.", "db_setup");
  }
}

async function testSupabaseConnection() {
  try {
    const { error } = await supabase
      .from("tapped_events_2")
      .select("transaction_hash", { count: "exact", head: true })
      .limit(1);

    if (error) {
      log(`Supabase client connection/permission test failed: ${error.message}`, "error");
      log(`Supabase error details: ${JSON.stringify(error)}`, "error");
      return false;
    }
    log("Supabase client connection/permission test successful.", "startup");
    return true;
  } catch (e) {
    log(`Supabase client connection test exception: ${e.message}`, "error");
    return false;
  }
}

// --- Event Signatures and Topics ---
const TAPPED_TOPIC = keccak256(toHex("Tapped(uint256,address,uint256,uint256,uint256)"));
const ROUND_ENDED_TOPIC = keccak256(toHex("RoundEnded(uint256,address,uint256,uint256)"));
const topic0_list = [TAPPED_TOPIC, ROUND_ENDED_TOPIC];

const POLLING_INTERVAL = 200;

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
let hypersyncClient = null;

// --- Supabase Batch Upsert Function ---
async function batchUpsertEventsWithRetry(tableName, batchData) {
  if (!batchData || batchData.length === 0) {
    return { success: true, error: null };
  }
  let attempts = 0;
  const batchSize = batchData.length;

  while (attempts < CONFIG.maxRetries) {
    attempts++;
    try {
      const { error } = await supabase.from(tableName).upsert(batchData, {
        onConflict: ["transaction_hash", "log_index"], // Changed to array format
      });
      if (error) {
        log(`Supabase batch upsert error (Attempt ${attempts}/${CONFIG.maxRetries}, Table: ${tableName}, Size: ${batchSize}): ${error.message || JSON.stringify(error)}`, "error");
        if (attempts < CONFIG.maxRetries) {
          const delay = Math.pow(2, attempts - 1) * CONFIG.retryBaseDelay;
          log(`Retrying batch upsert to ${tableName} in ${delay}ms`, "supabase");
          await new Promise((resolve) => setTimeout(resolve, delay));
        } else {
          eventCounts.SupabaseErrors += batchSize;
          return { success: false, error };
        }
      } else {
        log(`Successfully upserted batch of ${batchSize} events to ${tableName}${attempts > 1 ? ` after ${attempts} attempts` : ""}`, "supabase");
        eventCounts.SupabaseBatchesSent++;
        eventCounts.SupabaseEventsUpserted += batchSize;
        return { success: true, error: null };
      }
    } catch (e) {
      log(`Exception during batch upsert (Attempt ${attempts}/${CONFIG.maxRetries}, Table: ${tableName}, Size: ${batchSize}): ${e.message}`, "error");
      if (attempts < CONFIG.maxRetries) {
        const delay = Math.pow(2, attempts - 1) * CONFIG.retryBaseDelay;
        log(`Retrying batch upsert to ${tableName} after exception in ${delay}ms`, "supabase");
        await new Promise((resolve) => setTimeout(resolve, delay));
      } else {
        eventCounts.SupabaseErrors += batchSize;
        return { success: false, error: e };
      }
    }
  }
  log(`Failed to upsert batch to ${tableName} after ${CONFIG.maxRetries} attempts`, "error");
  return { success: false, error: { message: `Failed after ${CONFIG.maxRetries} attempts` } };
}

// --- Simplified JSON Status Server ---
const server = http.createServer(async (req, res) => {
  // Health check for all RPCs
  if (req.url === '/health/rpcs') {
    const rpcStatuses = [];
    const availableRpcs = NETWORK_URLS[CONFIG.network];

    for (let i = 0; i < availableRpcs.length; i++) {
      const rpcUrl = availableRpcs[i];
      try {
        const testClient = HypersyncClient.new({
          url: rpcUrl,
          bearerToken: process.env.HYPERSYNC_BEARER_TOKEN,
          timeout: 10000,
        });

        const startTime = Date.now();
        const height = await testClient.getHeight();
        const responseTime = Date.now() - startTime;

        rpcStatuses.push({
          url: rpcUrl,
          status: 'healthy',
          height: height,
          responseTime: responseTime,
          active: i === currentRpcIndex
        });
      } catch (error) {
        rpcStatuses.push({
          url: rpcUrl,
          status: 'unhealthy',
          error: error.message,
          active: i === currentRpcIndex
        });
      }
    }

    const healthyCount = rpcStatuses.filter(rpc => rpc.status === 'healthy').length;
    const overallStatus = healthyCount > 0 ? 'healthy' : 'unhealthy';

    res.writeHead(overallStatus === 'healthy' ? 200 : 503, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      overall: overallStatus,
      network: CONFIG.network,
      healthyRpcs: healthyCount,
      totalRpcs: availableRpcs.length,
      currentRpc: currentRpcUrl,
      rpcs: rpcStatuses,
      timestamp: new Date().toISOString()
    }, null, 2));
    return;
  }

  // Single RPC health check
  if (req.url === '/health') {
    try {
      if (!hypersyncClient || !currentRpcUrl) {
        throw new Error('No active Hypersync client');
      }
      const height = await hypersyncClient.getHeight();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: 'healthy',
        hypersync: 'connected',
        rpc: currentRpcUrl,
        height: height,
        network: CONFIG.network
      }, null, 2));
    } catch (error) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: 'unhealthy',
        hypersync: 'disconnected',
        rpc: currentRpcUrl || 'none',
        error: error.message,
        network: CONFIG.network
      }, null, 2));
    }
    return;
  }

  // Default status endpoint
  const totalEvents = eventCounts.Tapped + eventCounts.RoundEnded;
  const uptimeSeconds = ((performance.now() - startTime) / 1000);
  const eventsPerSecond = uptimeSeconds > 0 ? (totalEvents / uptimeSeconds).toFixed(2) : "0.00";
  const dbOpsPerSecond = uptimeSeconds > 0 ? (eventCounts.SupabaseEventsUpserted / uptimeSeconds).toFixed(2) : "0.00";

  const status = {
    status: "running",
    uptimeSeconds: Math.floor(uptimeSeconds),
    network: CONFIG.network,
    contractAddress: CONFIG.contractAddress,
    currentBlock,
    processingRate: {
      eventsPerSecond,
      dbOpsPerSecond,
    },
    rpcStatus: {
      currentRpc: currentRpcUrl || "Not connected",
      availableRpcs: NETWORK_URLS[CONFIG.network],
    },
    gameState: {
      currentRound,
      lastTapper,
      tapCost,
      lastWinner,
      lastPrize,
    },
    eventStatistics: eventCounts,
  };

  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify(status, null, 2));
});

server.listen(8080, "0.0.0.0", () => {
  log("Status web server running on port 8080", "startup");
});

// --- Enhanced Main Function with Comprehensive RPC Fallback ---
async function main() {
  const runStartTime = performance.now();
  let lastTipReachedTime = 0;
  const chainTipReportInterval = 5 * 60 * 1000;
  let stream;
  let consecutiveFailures = 0;
  const maxConsecutiveFailures = 5;

  // Run Database Setup First
  await runDatabaseSetup();

  // Test Supabase connection
  const connectionOk = await testSupabaseConnection();
  if (!connectionOk) {
    log("CRITICAL: Supabase client connection test failed after setup. Exiting.", "error");
    process.exit(1);
  }

  // Main retry loop with RPC fallback
  while (true) {
    try {
      // Create Hypersync client with RPC fallback
      const clientResult = await createHypersyncClientWithFallback();
      hypersyncClient = clientResult.client;

      log(`🚀 Starting Last Tap Tracker using ${clientResult.rpcUrl}`, "startup");
      log(`Network: ${CONFIG.network}, Contract: ${CONFIG.contractAddress}, Start Block: ${CONFIG.startBlock}`, "startup");

      let height = await hypersyncClient.getHeight();
      log(`Initial chain height: ${height}`, "startup");

      const decoder = Decoder.fromSignatures([
        "Tapped(uint256 indexed roundNumber,address indexed player,uint256 cost,uint256 newEndTime,uint256 timestamp)",
        "RoundEnded(uint256 indexed roundNumber,address indexed winner,uint256 prize,uint256 timestamp)",
      ]);

      let query = {
        fromBlock: currentBlock,
        logs: [{ address: [CONFIG.contractAddress], topics: [topic0_list] }],
        fieldSelection: {
          log: [
            LogField.BlockNumber,
            LogField.TransactionHash,
            LogField.LogIndex,
            LogField.Data,
            LogField.Topic0,
            LogField.Topic1,
            LogField.Topic2,
          ],
        },
        joinMode: JoinMode.JoinNothing,
      };

      log(`Starting event stream from block ${query.fromBlock}...`, "startup");
      stream = await createStreamWithFallback(hypersyncClient, query);

      let lastProgressLogBlock = currentBlock;
      consecutiveFailures = 0;

      // Main event processing loop
      while (true) {
        try {
          const res = await stream.recv();

          if (res === null) {
            // Reached chain tip logic
            const now = Date.now();
            if (now - lastTipReachedTime > chainTipReportInterval) {
              log(`⏳ Reached chain tip at block ${currentBlock} on ${currentRpcUrl}. Waiting...`, "verbose");
              lastTipReachedTime = now;
            }
            await new Promise((resolve) => setTimeout(resolve, POLLING_INTERVAL));

            try {
              const newHeight = await hypersyncClient.getHeight();
              if (newHeight > height) {
                log(`📈 Chain advanced to ${newHeight}. Re-querying...`, "verbose");
                await stream.close();
                height = newHeight;
                query.fromBlock = currentBlock;
                stream = await createStreamWithFallback(hypersyncClient, query);
              }
            } catch (err) {
              log(`Error checking height: ${err.message}`, "error");
              throw err;
            }
            continue;
          }

          consecutiveFailures = 0;
          let tappedBatch = [];
          let roundEndedBatch = [];

          if (res.data && res.data.logs && res.data.logs.length > 0) {
            const decodedLogs = await decoder.decodeLogs(res.data.logs);

            for (let i = 0; i < decodedLogs.length; i++) {
              const decodedLog = decodedLogs[i];
              const rawLog = res.data.logs[i];
              if (decodedLog === null) {
                log(`Skipping undecodable log idx ${i}, blk ${rawLog.blockNumber}`, "verbose");
                continue;
              }

              const blockNumber = rawLog.blockNumber;
              const transactionHash = rawLog.transactionHash;
              const logIndex = rawLog.logIndex;
              if (transactionHash === undefined || logIndex === undefined || blockNumber === undefined) {
                log(`Missing critical fields in log idx ${i}, blk ${rawLog.blockNumber}. Skipping. Raw: ${JSON.stringify(rawLog)}`, "error");
                continue;
              }

              const topic0 = rawLog.topics[0];
              const eventType = topic0 === TAPPED_TOPIC ? "Tapped" : "RoundEnded";

              try {
                if (eventType === "Tapped") {
                  eventCounts.Tapped++;
                  const roundNumber = decodedLog.indexed[0]?.val?.toString();
                  const player = decodedLog.indexed[1]?.val?.toString();
                  const cost = decodedLog.body[0]?.val?.toString();
                  const newEndTime = decodedLog.body[1]?.val?.toString();
                  const timestamp = decodedLog.body[2]?.val?.toString();
                  if (!roundNumber || !player || !cost || !newEndTime || !timestamp) {
                    log(`Missing data in Tapped event: Tx ${formatAddress(transactionHash)}, Log ${logIndex}. Skipping.`, "error");
                    continue;
                  }

                  const eventTimestamp = new Date(Number(timestamp) * 1000);
                  currentRound = roundNumber;
                  lastTapper = player;
                  tapCost = cost;
                  log(`TAPPED | Blk: ${blockNumber} | Rnd: ${roundNumber} | Player: ${formatAddress(player)} | Cost: ${formatTokens(cost)} | ${eventTimestamp.toISOString()}`, "event");

                  tappedBatch.push({
                    block_number: Number(blockNumber),
                    transaction_hash: String(transactionHash),
                    log_index: Number(logIndex),
                    round_number: String(roundNumber),
                    player: String(player),
                    cost: String(cost),
                    new_end_time: String(newEndTime),
                    timestamp: String(timestamp),
                    event_timestamp: eventTimestamp.toISOString(),
                    contract_address: String(CONFIG.contractAddress),
                  });
                } else if (eventType === "RoundEnded") {
                  eventCounts.RoundEnded++;
                  const roundNumber = decodedLog.indexed[0]?.val?.toString();
                  const winner = decodedLog.indexed[1]?.val?.toString();
                  const prize = decodedLog.body[0]?.val?.toString();
                  const timestamp = decodedLog.body[1]?.val?.toString();
                  if (!roundNumber || !winner || !prize || !timestamp) {
                    log(`Missing data in RoundEnded event: Tx ${formatAddress(transactionHash)}, Log ${logIndex}. Skipping.`, "error");
                    continue;
                  }

                  const eventTimestamp = new Date(Number(timestamp) * 1000);
                  lastWinner = winner;
                  lastPrize = prize;
                  currentRound = (BigInt(roundNumber) + 1n).toString();
                  log(`ROUND END | Blk: ${blockNumber} | Rnd: ${roundNumber} | Winner: ${formatAddress(winner)} | Prize: ${formatTokens(prize)} | ${eventTimestamp.toISOString()}`, "event");

                  roundEndedBatch.push({
                    block_number: Number(blockNumber),
                    transaction_hash: String(transactionHash),
                    log_index: Number(logIndex),
                    round_number: String(roundNumber),
                    winner: String(winner),
                    prize: String(prize),
                    timestamp: String(timestamp),
                    event_timestamp: eventTimestamp.toISOString(),
                    contract_address: String(CONFIG.contractAddress),
                  });
                }
              } catch (processingError) {
                log(`Error processing log: ${processingError.message}. Tx: ${formatAddress(transactionHash)}, Log: ${logIndex}. Skipping. Stack: ${processingError.stack}`, "error");
              }
            }

            // Send Batches
            const batchPromises = [];
            if (tappedBatch.length > 0)
              batchPromises.push(batchUpsertEventsWithRetry("tapped_events_2", tappedBatch));
            if (roundEndedBatch.length > 0)
              batchPromises.push(batchUpsertEventsWithRetry("round_ended_events_2", roundEndedBatch));
            if (batchPromises.length > 0) {
              const results = await Promise.all(batchPromises);
              results.forEach((result) => {
                if (!result.success)
                  log(`Failed final upsert batch. Error: ${JSON.stringify(result.error)}`, "error");
              });
            }
          }

          // Update block position
          if (res.nextBlock) {
            const previousBlock = currentBlock;
            currentBlock = res.nextBlock;
            query.fromBlock = currentBlock;

            if (currentBlock - lastProgressLogBlock >= 10000) {
              const seconds = (performance.now() - runStartTime) / 1000;
              const totalEvents = eventCounts.Tapped + eventCounts.RoundEnded;
              log(`📊 Progress: Block ${currentBlock} | ${totalEvents} events | ${eventCounts.SupabaseEventsUpserted} DB upserts | ${eventCounts.SupabaseErrors} DB errors | ${seconds.toFixed(1)}s | RPC: ${currentRpcUrl}`, "normal");
              lastProgressLogBlock = currentBlock;
            }
          } else if (res.data && res.data.logs && res.data.logs.length > 0) {
            const lastLogBlock = res.data.logs[res.data.logs.length - 1]?.blockNumber;
            if (lastLogBlock && lastLogBlock >= currentBlock) {
              currentBlock = lastLogBlock + 1;
              query.fromBlock = currentBlock;
              log(`Advanced currentBlock to ${currentBlock} based on last log at tip`, "verbose");
            }
          }

        } catch (streamError) {
          consecutiveFailures++;
          log(`Stream error (${consecutiveFailures}/${maxConsecutiveFailures}): ${streamError.message}`, "error");

          if (consecutiveFailures >= maxConsecutiveFailures) {
            log(`Too many consecutive stream failures, breaking to retry with different RPC`, "error");
            throw streamError;
          }

          await new Promise(resolve => setTimeout(resolve, 1000));
        }
      }

    } catch (error) {
      consecutiveFailures++;
      log(`💥 Main loop error (${consecutiveFailures}): ${error.message}`, "error");

      const isConnectionError = error.message.includes('503') ||
                               error.message.includes('Service Unavailable') ||
                               error.message.includes('timeout') ||
                               error.message.includes('ECONNREFUSED') ||
                               error.message.includes('fetch');

      let retryDelay;

      if (isConnectionError) {
        log(`🔄 Connection error detected, will try different RPC endpoint`, "error");
        retryDelay = 5000;
        switchToNextRpc(CONFIG.network);
      } else {
        log(`⚠️ Non-connection error, using exponential backoff`, "error");
        retryDelay = Math.min(1000 * Math.pow(2, consecutiveFailures), 60000);
      }

      // Clean up existing connections
      if (stream) {
        try {
          await stream.close();
        } catch (closeError) {
          log(`Error closing stream: ${closeError.message}`, "error");
        }
        stream = null;
      }

      hypersyncClient = null;

      log(`⏱️ Retrying main loop in ${retryDelay}ms...`, "startup");
      await new Promise(resolve => setTimeout(resolve, retryDelay));
    }
  }
}

// --- Start the application ---
main().catch((error) => {
  log(`Unhandled error in main execution: ${error.message}`, "error");
  log(`Stack Trace: ${error.stack}`, "error");
  process.exit(1);
});