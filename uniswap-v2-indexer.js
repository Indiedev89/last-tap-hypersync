// uniswap-v2-indexer.js
import { keccak256, toHex } from "viem";
import {
  HypersyncClient,
  LogField,
  JoinMode,
  Decoder,
  BlockField,
  TransactionField,
} from "@envio-dev/hypersync-client";
import http from "http";
import { createClient } from "@supabase/supabase-js";
import pg from "pg";
const { Pool } = pg;
import "dotenv/config";

// --- Configuration ---
const UNISWAP_V2_PAIR_ADDRESS = process.env.UNISWAP_V2_PAIR_ADDRESS || "0x9f6A232C454743a31512C01FcD4A9873BDb3df71";
const TAP_TOKEN_ADDRESS = process.env.TAP_TOKEN_ADDRESS || "0xAb0d0B32dadAbcADD74aF2E87593c920C3070a81";
const WETH_TOKEN_ADDRESS = "0x4eB2Bd7beE16F38B1F4a0A5796Fffd028b6040e9";

const CONFIG = {
  uniswapV2PairAddress: UNISWAP_V2_PAIR_ADDRESS,
  tapTokenAddress: TAP_TOKEN_ADDRESS,
  wethTokenAddress: WETH_TOKEN_ADDRESS,
  startBlock: 3507082,
  network: "megaethTestnet",
  logLevel: "event-only", // 'verbose', 'normal', 'event-only'
  supabaseUrl: process.env.SUPABASE_URL,
  supabaseServiceKey: process.env.SUPABASE_SERVICE_ROLE_KEY,
  supabaseDbConnectionString: process.env.SUPABASE_DB_CONNECTION_STRING,
  batchSize: 200,
  maxRetries: 20,
  retryBaseDelay: 500,
};

// Determine which token is token0 and which is token1
const [token0, token1] = [CONFIG.wethTokenAddress, CONFIG.tapTokenAddress].sort((a, b) => a.localeCompare(b));
const IS_WETH_TOKEN0 = token0 === CONFIG.wethTokenAddress;

// --- Enhanced Network URL mapping with fallback RPCs ---
const NETWORK_URLS = {
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

const formatUnits = (value, decimals = 18) => {
    try {
        if (value === null || value === undefined) return "N/A";
        const valueBigInt = BigInt(value);
        const divisor = 10n ** BigInt(decimals);
        const scaled = valueBigInt * 1000000n / divisor;
        return (Number(scaled) / 1000000).toFixed(6);
    } catch(e) {
        log(`Error formatting units: ${value} - ${e.message}`, "error");
        return value?.toString() || "N/A";
    }
}

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

log(`Monitoring Uniswap V2 Pair: ${CONFIG.uniswapV2PairAddress}`, "startup");
log(`$TAP Token: ${CONFIG.tapTokenAddress}`, "startup");
log(`WETH Token: ${CONFIG.wethTokenAddress}`, "startup");
log(`Token Order: token0=${IS_WETH_TOKEN0 ? 'WETH' : '$TAP'}, token1=${IS_WETH_TOKEN0 ? '$TAP' : 'WETH'}`, 'startup');
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

CREATE TABLE IF NOT EXISTS public.swap_events (
    block_number BIGINT NOT NULL,
    transaction_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    sender TEXT NOT NULL,
    recipient TEXT NOT NULL,
    amount_weth_in NUMERIC(78, 0) NOT NULL,
    amount_tap_in NUMERIC(78, 0) NOT NULL,
    amount_weth_out NUMERIC(78, 0) NOT NULL,
    amount_tap_out NUMERIC(78, 0) NOT NULL,
    price_tap_in_weth NUMERIC(38, 18),
    event_timestamp TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (transaction_hash, log_index)
);

CREATE INDEX IF NOT EXISTS idx_swap_events_timestamp ON public.swap_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_swap_events_sender ON public.swap_events(sender);
CREATE INDEX IF NOT EXISTS idx_swap_events_recipient ON public.swap_events(recipient);
CREATE INDEX IF NOT EXISTS idx_swap_events_price ON public.swap_events(price_tap_in_weth);

GRANT USAGE ON SCHEMA public TO postgres, anon, authenticated, service_role;
GRANT SELECT ON TABLE public.swap_events TO anon, authenticated;
GRANT ALL PRIVILEGES ON TABLE public.swap_events TO postgres, service_role;

ALTER TABLE public.swap_events ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Anon can read swap_events" ON public.swap_events;
CREATE POLICY "Anon can read swap_events" ON public.swap_events FOR SELECT TO anon USING (true);

DROP POLICY IF EXISTS "Service roles full access swap_events" ON public.swap_events;
CREATE POLICY "Service roles full access swap_events" ON public.swap_events FOR ALL TO service_role, postgres USING (true) WITH CHECK (true);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'supabase_realtime') THEN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication_tables
        WHERE pubname = 'supabase_realtime' AND schemaname = 'public' AND tablename = 'swap_events'
    ) THEN
        ALTER PUBLICATION supabase_realtime ADD TABLE public.swap_events;
        RAISE NOTICE 'Added public.swap_events to supabase_realtime publication.';
    ELSE
        RAISE NOTICE 'public.swap_events already in supabase_realtime publication.';
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
      .from("swap_events")
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
const SWAP_TOPIC = keccak256(toHex("Swap(address,uint256,uint256,uint256,uint256,address)"));
const topic0_list = [SWAP_TOPIC];

const POLLING_INTERVAL = 2000;

// --- Metrics and State ---
let eventCounts = {
  Swap: 0,
  SupabaseBatchesSent: 0,
  SupabaseEventsUpserted: 0,
  SupabaseErrors: 0,
};
let latestPrice = 0;
let latestSwap = {};
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
        onConflict: "transaction_hash,log_index",
      });
      if (error) {
        throw error;
      } else {
        log(`Successfully upserted batch of ${batchSize} events to ${tableName}${attempts > 1 ? ` after ${attempts} attempts` : ""}`, "supabase");
        eventCounts.SupabaseBatchesSent++;
        eventCounts.SupabaseEventsUpserted += batchSize;
        return { success: true, error: null };
      }
    } catch (e) {
      log(`Supabase batch upsert error (Attempt ${attempts}/${CONFIG.maxRetries}, Table: ${tableName}, Size: ${batchSize}): ${e.message || JSON.stringify(e)}`, "error");
      if (attempts < CONFIG.maxRetries) {
        const delay = Math.pow(2, attempts - 1) * CONFIG.retryBaseDelay;
        log(`Retrying batch upsert to ${tableName} in ${delay}ms`, "supabase");
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
  const totalEvents = eventCounts.Swap;
  const uptimeSeconds = ((performance.now() - startTime) / 1000);
  const eventsPerSecond = uptimeSeconds > 0 ? (totalEvents / uptimeSeconds).toFixed(2) : "0.00";
  const dbOpsPerSecond = uptimeSeconds > 0 ? (eventCounts.SupabaseEventsUpserted / uptimeSeconds).toFixed(2) : "0.00";

  const status = {
    status: "running",
    uptimeSeconds: Math.floor(uptimeSeconds),
    network: CONFIG.network,
    contractAddress: CONFIG.uniswapV2PairAddress,
    currentBlock,
    processingRate: {
      eventsPerSecond,
      dbOpsPerSecond,
    },
    rpcStatus: {
      currentRpc: currentRpcUrl || "Not connected",
      availableRpcs: NETWORK_URLS[CONFIG.network],
    },
    swapInfo: {
      latestPrice_TAP_in_WETH: latestPrice,
      latestSwap,
    },
    eventStatistics: eventCounts,
  };

  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify(status, (key, value) =>
    typeof value === 'bigint' ? value.toString() : value,
  2));
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

      log(`🚀 Starting Uniswap V2 Indexer using ${clientResult.rpcUrl}`, "startup");
      log(`Network: ${CONFIG.network}, Pair Contract: ${CONFIG.uniswapV2PairAddress}, Start Block: ${CONFIG.startBlock}`, "startup");

      let height = await hypersyncClient.getHeight();
      log(`Initial chain height: ${height}`, "startup");

      const decoder = Decoder.fromSignatures([
        "Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)"
      ]);

      let query = {
        fromBlock: currentBlock,
        logs: [{ address: [CONFIG.uniswapV2PairAddress], topics: [topic0_list] }],
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
          block: [BlockField.Timestamp],
          transaction: [TransactionField.Hash] // Ensure transaction data is linked
        },
        joinMode: JoinMode.JoinAll, // Use the most robust join mode
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
          let swapBatch = [];

          if (res.data && res.data.logs && res.data.logs.length > 0) {
            const blockTimestampMap = new Map(
                res.data.blocks?.map(block => [block.number, block.timestamp]) || []
            );
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

              try {
                eventCounts.Swap++;
                const sender = decodedLog.indexed[0]?.val?.toString();
                const recipient = decodedLog.indexed[1]?.val?.toString();
                const amount0In = BigInt(decodedLog.body[0]?.val);
                const amount1In = BigInt(decodedLog.body[1]?.val);
                const amount0Out = BigInt(decodedLog.body[2]?.val);
                const amount1Out = BigInt(decodedLog.body[3]?.val);

                const blockTimestamp = blockTimestampMap.get(rawLog.blockNumber);
                if (blockTimestamp === undefined) {
                    const availableBlocks = Array.from(blockTimestampMap.keys());
                    log(`Could not find block data for log in block ${rawLog.blockNumber}, tx ${rawLog.transactionHash}. Available blocks in this batch: [${availableBlocks.join(', ')}]`, 'error');
                    continue;
                }
                const eventTimestamp = new Date(Number(blockTimestamp) * 1000);

                const amountWethIn = IS_WETH_TOKEN0 ? amount0In : amount1In;
                const amountTapIn = IS_WETH_TOKEN0 ? amount1In : amount0In;
                const amountWethOut = IS_WETH_TOKEN0 ? amount0Out : amount1Out;
                const amountTapOut = IS_WETH_TOKEN0 ? amount1Out : amount0Out;

                let price = 0;
                if (amountTapIn > 0n && amountWethOut > 0n) {
                    price = Number(amountWethOut * 10n**18n / amountTapIn) / 1e18;
                } else if (amountWethIn > 0n && amountTapOut > 0n) {
                    price = Number(amountWethIn * 10n**18n / amountTapOut) / 1e18;
                }
                latestPrice = price;

                latestSwap = {
                    tx: formatAddress(rawLog.transactionHash),
                    type: amountWethIn > 0n ? 'SELL $TAP' : 'BUY $TAP',
                    tapAmount: formatUnits(amountTapIn > 0n ? amountTapIn : amountTapOut),
                    wethAmount: formatUnits(amountWethIn > 0n ? amountWethIn : amountWethOut),
                    price: price.toFixed(8)
                };
                log(`SWAP | Blk: ${blockNumber} | ${latestSwap.type} | Price: ${latestSwap.price} WETH/$TAP`, "event");

                swapBatch.push({
                    block_number: Number(blockNumber),
                    transaction_hash: String(transactionHash),
                    log_index: Number(logIndex),
                    sender,
                    recipient,
                    amount_weth_in: amountWethIn.toString(),
                    amount_tap_in: amountTapIn.toString(),
                    amount_weth_out: amountWethOut.toString(),
                    amount_tap_out: amountTapOut.toString(),
                    price_tap_in_weth: price.toFixed(18),
                    event_timestamp: eventTimestamp.toISOString(),
                });

              } catch (processingError) {
                log(`Error processing log: ${processingError.message}. Tx: ${formatAddress(transactionHash)}, Log: ${logIndex}. Skipping. Stack: ${processingError.stack}`, "error");
              }
            }

            // Send Batches
            if (swapBatch.length > 0) {
              await batchUpsertEventsWithRetry("swap_events", swapBatch);
            }
          }

          // Update block position
          if (res.nextBlock) {
            currentBlock = res.nextBlock;
            query.fromBlock = currentBlock;

            if (currentBlock - lastProgressLogBlock >= 10000) {
              const seconds = (performance.now() - runStartTime) / 1000;
              const totalEvents = eventCounts.Swap;
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