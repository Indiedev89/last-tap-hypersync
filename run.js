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
import pg from "pg"; // Import the default export of 'pg'
const { Pool } = pg; // Destructure Pool from the default export
import "dotenv/config"; // Import dotenv to load environment variables

// --- Configuration ---
const CONFIG = {
  contractAddress: "0x16ED00aC93b37B7481eD3CCfa2a87C342aCB816C",
  startBlock: 3258331,
  network: "megaethTestnet",
  logLevel: "event-only", // 'verbose', 'normal', 'event-only'
  supabaseUrl: process.env.SUPABASE_URL,
  supabaseServiceKey: process.env.SUPABASE_SERVICE_ROLE_KEY,
  supabaseDbConnectionString: process.env.SUPABASE_DB_CONNECTION_STRING, // Get DB connection string
  batchSize: 100,
  maxRetries: 3,
  retryBaseDelay: 500,
};

// --- Helper Functions (Logging, Formatting - Unchanged) ---
// Logging function
const log = (message, level = "normal") => {
  const timestamp = new Date().toISOString();
  const logMessage = `${timestamp} [${level.toUpperCase()}] ${message}`;

  if (level === "error") {
    console.error(logMessage);
    return;
  }

  // Simplified logging logic for brevity (keep your original if preferred)
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

const formatEth = (wei) => {
  try {
    if (wei === null || wei === undefined) return "N/A";
    const weiBigInt = BigInt(wei);
    const eth = Number((weiBigInt * 10000n) / 10n ** 18n) / 10000;
    return eth.toFixed(4) + " ETH";
  } catch (e) {
    log(`Error formatting ETH value: ${wei} - ${e.message}`, "error");
    return wei?.toString() || "N/A";
  }
};

// --- Validate Configuration ---
if (!CONFIG.supabaseUrl || !CONFIG.supabaseServiceKey) {
  log(
    "Error: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables are required.",
    "error"
  );
  process.exit(1);
}
if (!CONFIG.supabaseDbConnectionString) {
  log(
    "Error: SUPABASE_DB_CONNECTION_STRING environment variable is required for initial setup.",
    "error"
  );
  process.exit(1);
}

// Log environment variables format check (without revealing full values)
log(
  `SUPABASE_URL format check: ${
    CONFIG.supabaseUrl?.startsWith("https://") ? "OK" : "INVALID"
  }`,
  "startup"
);
log(
  `SUPABASE_SERVICE_ROLE_KEY length check: ${
    CONFIG.supabaseServiceKey?.length > 30 ? "OK" : "INVALID"
  }`,
  "startup"
);
log(
  `SUPABASE_DB_CONNECTION_STRING format check: ${
    CONFIG.supabaseDbConnectionString?.startsWith("postgresql://")
      ? "OK"
      : "INVALID"
  }`,
  "startup"
);

// --- Network URL mapping (Unchanged) ---
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

// --- Database Setup SQL ---
const SETUP_SQL = `
-- Ensure public schema exists (usually does, but good practice)
CREATE SCHEMA IF NOT EXISTS public;

-- Create tapped_events table
CREATE TABLE IF NOT EXISTS public.tapped_events (
    block_number BIGINT NOT NULL,
    transaction_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    tapper_address TEXT NOT NULL,
    round_number TEXT NOT NULL,
    tap_cost_paid TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (transaction_hash, log_index)
);

-- Create round_ended_events table
CREATE TABLE IF NOT EXISTS public.round_ended_events (
    block_number BIGINT NOT NULL,
    transaction_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    winner_address TEXT NOT NULL,
    prize_amount TEXT NOT NULL,
    round_number TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (transaction_hash, log_index)
);

-- Add indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_tapped_round_number
ON public.tapped_events(round_number);

CREATE INDEX IF NOT EXISTS idx_tapped_tapper
ON public.tapped_events(tapper_address);

CREATE INDEX IF NOT EXISTS idx_tapped_timestamp
ON public.tapped_events(event_timestamp);

CREATE INDEX IF NOT EXISTS idx_round_ended_round
ON public.round_ended_events(round_number);

CREATE INDEX IF NOT EXISTS idx_round_ended_winner
ON public.round_ended_events(winner_address);

CREATE INDEX IF NOT EXISTS idx_round_ended_timestamp
ON public.round_ended_events(event_timestamp);

-- Grant usage on schema to roles if needed (anon, authenticated usually have it)
GRANT USAGE ON SCHEMA public TO postgres, anon, authenticated, service_role;

-- Grant select on tables to anon role
GRANT SELECT ON TABLE public.tapped_events TO anon, authenticated;
GRANT SELECT ON TABLE public.round_ended_events TO anon, authenticated;

-- Grant all privileges to postgres and service_role (service_role should already have it)
GRANT ALL PRIVILEGES ON TABLE public.tapped_events TO postgres, service_role;
GRANT ALL PRIVILEGES ON TABLE public.round_ended_events TO postgres, service_role;

-- Note: Supabase manages default privileges, explicit grants might be needed in specific scenarios.

-- Enable RLS (Important!)
ALTER TABLE public.tapped_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.round_ended_events ENABLE ROW LEVEL SECURITY;

-- Delete existing policies for anon/service_role before creating new ones to avoid errors if run multiple times
DROP POLICY IF EXISTS "Anon can read tapped_events" ON public.tapped_events;
DROP POLICY IF EXISTS "Service roles full access tapped_events" ON public.tapped_events;
DROP POLICY IF EXISTS "Anon can read round_ended_events" ON public.round_ended_events;
DROP POLICY IF EXISTS "Service roles full access round_ended_events" ON public.round_ended_events;

-- Create RLS Policies
-- Read access for anon
CREATE POLICY "Anon can read tapped_events"
ON public.tapped_events
FOR SELECT
TO anon -- Apply to anon role
USING (true); -- Allow reading all rows

-- Allow all access for service_role and postgres (Supabase Admin)
CREATE POLICY "Service roles full access tapped_events"
ON public.tapped_events
FOR ALL -- SELECT, INSERT, UPDATE, DELETE
TO service_role, postgres -- Apply to admin/service roles
USING (true) -- Allow access to all existing rows
WITH CHECK (true); -- Allow inserting/updating any row

-- Read access for anon
CREATE POLICY "Anon can read round_ended_events"
ON public.round_ended_events
FOR SELECT
TO anon
USING (true);

-- Allow all access for service_role and postgres
CREATE POLICY "Service roles full access round_ended_events"
ON public.round_ended_events
FOR ALL
TO service_role, postgres
USING (true)
WITH CHECK (true);


-- Realtime Publication Setup (Requires SUPERUSER or rds_superuser privileges usually)
-- Note: Running ALTER PUBLICATION might fail if the script's DB user doesn't have sufficient privileges.
-- It's often better to do this once via the Supabase Dashboard (Database -> Replication).
-- If you need the script to ensure it, handle potential errors gracefully.
DO $$
BEGIN
  -- Check if the publication exists
  IF EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'supabase_realtime') THEN

    -- Check and add tapped_events if not present
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication_tables
        WHERE pubname = 'supabase_realtime' AND schemaname = 'public' AND tablename = 'tapped_events'
    ) THEN
        ALTER PUBLICATION supabase_realtime ADD TABLE public.tapped_events;
        RAISE NOTICE 'Added public.tapped_events to supabase_realtime publication.';
    ELSE
        RAISE NOTICE 'public.tapped_events already in supabase_realtime publication.';
    END IF;

    -- Check and add round_ended_events if not present
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication_tables
        WHERE pubname = 'supabase_realtime' AND schemaname = 'public' AND tablename = 'round_ended_events'
    ) THEN
        ALTER PUBLICATION supabase_realtime ADD TABLE public.round_ended_events;
        RAISE NOTICE 'Added public.round_ended_events to supabase_realtime publication.';
    ELSE
        RAISE NOTICE 'public.round_ended_events already in supabase_realtime publication.';
    END IF;

  ELSE
    RAISE WARNING 'Publication supabase_realtime does not exist. Realtime will not function for these tables unless created manually.';
  END IF;
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE WARNING 'Insufficient privileges to modify publication supabase_realtime. Please enable realtime for tables manually in the Supabase dashboard if needed.';
  WHEN OTHERS THEN
    RAISE WARNING 'An error occurred during publication setup: SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
END
$$;
`;

// --- Database Setup Function ---
async function runDatabaseSetup() {
  log("Starting database schema setup...", "db_setup");
  const pool = new Pool({
    connectionString: CONFIG.supabaseDbConnectionString,
  });
  let client; // Declare client outside try block

  try {
    client = await pool.connect();
    log("Database connection successful for setup.", "db_setup");
    await client.query(SETUP_SQL);
    log("Database schema setup script executed successfully.", "db_setup");
  } catch (err) {
    log(`Database setup failed: ${err.message}`, "error");
    log(`SQL Error Details: ${err.stack}`, "error");
    // Decide if you want to exit if setup fails. Critical tables might require exit.
    log("Exiting due to critical database setup failure.", "error");
    process.exit(1);
  } finally {
    if (client) {
      client.release(); // Release the client back to the pool
      log("Database setup client released.", "db_setup");
    }
    await pool.end(); // Close the pool after setup is done
    log("Database setup pool closed.", "db_setup");
  }
}

// --- Event Signatures and Topics (Unchanged) ---
const TAPPED_TOPIC = keccak256(
  toHex("Tapped(address,uint256,uint256,uint256)")
);
const ROUND_ENDED_TOPIC = keccak256(
  toHex("RoundEnded(address,uint256,uint256,uint256)")
);
const topic0_list = [TAPPED_TOPIC, ROUND_ENDED_TOPIC];

const POLLING_INTERVAL = 200; // ms

// --- Metrics and State (Unchanged) ---
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

// --- Supabase Batch Upsert with Retry Function (Unchanged) ---
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
        log(
          `Supabase batch upsert error (Attempt ${attempts}/${
            CONFIG.maxRetries
          }, Table: ${tableName}, Size: ${batchSize}): ${
            error.message || JSON.stringify(error)
          }`,
          "error"
        );
        if (attempts < CONFIG.maxRetries) {
          const delay = Math.pow(2, attempts - 1) * CONFIG.retryBaseDelay;
          log(
            `Retrying batch upsert to ${tableName} in ${delay}ms`,
            "supabase"
          );
          await new Promise((resolve) => setTimeout(resolve, delay));
        } else {
          eventCounts.SupabaseErrors += batchSize;
          return { success: false, error };
        }
      } else {
        log(
          `Successfully upserted batch of ${batchSize} events to ${tableName}${
            attempts > 1 ? ` after ${attempts} attempts` : ""
          }`,
          "supabase"
        );
        eventCounts.SupabaseBatchesSent++;
        eventCounts.SupabaseEventsUpserted += batchSize;
        return { success: true, error: null };
      }
    } catch (e) {
      log(
        `Exception during batch upsert (Attempt ${attempts}/${CONFIG.maxRetries}, Table: ${tableName}, Size: ${batchSize}): ${e.message}`,
        "error"
      );
      if (attempts < CONFIG.maxRetries) {
        const delay = Math.pow(2, attempts - 1) * CONFIG.retryBaseDelay;
        log(
          `Retrying batch upsert to ${tableName} after exception in ${delay}ms`,
          "supabase"
        );
        await new Promise((resolve) => setTimeout(resolve, delay));
      } else {
        eventCounts.SupabaseErrors += batchSize;
        return { success: false, error: e };
      }
    }
  }
  log(
    `Failed to upsert batch to ${tableName} after ${CONFIG.maxRetries} attempts`,
    "error"
  );
  return {
    success: false,
    error: { message: `Failed after ${CONFIG.maxRetries} attempts` },
  };
}

// --- Test Supabase Connection Function (Now tests after setup) ---
async function testSupabaseConnection() {
  // This test now implicitly assumes the table exists because setup runs first.
  // It primarily checks if the service key allows selecting from the table.
  try {
    const { error } = await supabase
      .from("tapped_events")
      .select("transaction_hash", { count: "exact", head: true }) // Minimal select
      .limit(1); // Only need to know if we *can* select

    if (error) {
      log(
        `Supabase client connection/permission test failed: ${error.message}`,
        "error"
      );
      log(`Supabase error details: ${JSON.stringify(error)}`, "error");
      // If RLS is misconfigured or service key is wrong, this might fail.
      return false;
    }
    log("Supabase client connection/permission test successful.", "startup");
    return true;
  } catch (e) {
    log(`Supabase client connection test exception: ${e.message}`, "error");
    return false;
  }
}

// --- HTTP Server (Unchanged - Keep your HTML/CSS) ---
const server = http.createServer((req, res) => {
  const totalEvents = eventCounts.Tapped + eventCounts.RoundEnded;
  const uptime = ((performance.now() - startTime) / 1000).toFixed(0);
  const eventsPerSecond = uptime > 0 ? (totalEvents / uptime).toFixed(2) : 0;
  const dbOpsPerSecond =
    uptime > 0 ? (eventCounts.SupabaseEventsUpserted / uptime).toFixed(2) : 0;

  // --- Your existing HTML template ---
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
        <h2>System Status <span class="network-badge">${
          CONFIG.network
        }</span></h2>
















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
            ${
              lastTapper
                ? `<span class="address">${formatAddress(lastTapper)}</span>`
                : "Unknown"
            }
          </div>
        </div>
        <div class="stat-item">
          <div class="stat-label">Current Tap Cost:</div>
          <div class="stat-value">${formatEth(tapCost) || "Unknown"}</div>
        </div>
        <div class="stat-item">
          <div class="stat-label">Last Winner:</div>
          <div class="stat-value">
            ${
              lastWinner
                ? `<span class="address">${formatAddress(lastWinner)}</span>`
                : "Unknown"
            }
          </div>
        </div>
        <div class="stat-item">
          <div class="stat-label">Last Prize:</div>
          <div class="stat-value">${formatEth(lastPrize) || "Unknown"}</div>
        </div>
        <div class="stat-item">
          <div class="stat-label">Contract Address:</div>
          <div class="stat-value">
            <span class="address">${formatAddress(
              CONFIG.contractAddress
            )}</span>
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
        <div class="info-box ${
          eventCounts.SupabaseErrors > 0 ? "error" : "success"
        }">
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
  `; // --- End HTML ---

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

  // --- Run Database Setup First ---
  await runDatabaseSetup(); // Ensure tables/policies exist before proceeding

  // Test Supabase client connection/permissions AFTER setup
  const connectionOk = await testSupabaseConnection();
  if (!connectionOk) {
    log(
      "CRITICAL: Supabase client connection test failed after setup. Check service key permissions or RLS policies. Exiting.",
      "error"
    );
    process.exit(1); // Exit if client can't interact with tables
  }

  // --- Start Hypersync Processing ---
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
        ],
      },
      joinMode: JoinMode.JoinNothing,
    };

    log(`Starting event stream from block ${query.fromBlock}...`, "startup");
    let stream = await hypersyncClient.stream(query, {});

    let lastProgressLogBlock = currentBlock;
    let consecutiveChainTips = 0;

    // --- Main Event Loop (Largely Unchanged) ---
    while (true) {
      const res = await stream.recv();

      if (res === null) {
        // Reached chain tip logic (unchanged)
        const now = Date.now();
        consecutiveChainTips++;
        if (
          now - lastTipReachedTime > chainTipReportInterval ||
          consecutiveChainTips === 1
        ) {
          log(
            `Reached chain tip at block ${currentBlock}. Waiting...`,
            "verbose"
          );
          lastTipReachedTime = now;
        }
        await new Promise((resolve) => setTimeout(resolve, POLLING_INTERVAL));
        try {
          // Optional height check (unchanged)
          const newHeight = await hypersyncClient.getHeight();
          if (newHeight > height) {
            log(`Chain advanced to ${newHeight}. Re-querying...`, "verbose");
            await stream.close();
            height = newHeight;
            query.fromBlock = currentBlock;
            stream = await hypersyncClient.stream(query, {});
            consecutiveChainTips = 0;
          }
        } catch (err) {
          log(`Error checking height at tip: ${err.message}`, "error");
        }
        continue;
      }

      consecutiveChainTips = 0;
      lastTipReachedTime = 0;
      let tappedBatch = [];
      let roundEndedBatch = [];

      if (res.data && res.data.logs && res.data.logs.length > 0) {
        const decodedLogs = await decoder.decodeLogs(res.data.logs);

        for (let i = 0; i < decodedLogs.length; i++) {
          const decodedLog = decodedLogs[i];
          const rawLog = res.data.logs[i];
          if (decodedLog === null) {
            log(
              `Skipping undecodable log idx ${i}, blk ${rawLog.blockNumber}`,
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
              `Missing critical fields in log idx ${i}, blk ${
                rawLog.blockNumber
              }. Skipping. Raw: ${JSON.stringify(rawLog)}`,
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
              currentRound = (BigInt(roundNumber) + 1n).toString();
              log(
                `ROUND END | Blk: ${blockNumber} | Rnd: ${roundNumber} | Winner: ${formatAddress(
                  winner
                )} | Prize: ${formatEth(
                  prizeAmount
                )} | ${eventTimestamp.toISOString()}`,
                "event"
              );

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
              )}, Log: ${logIndex}. Skipping. Stack: ${processingError.stack}`,
              "error"
            );
          }
        } // End log loop

        // --- Send Batches (Unchanged) ---
        const batchPromises = [];
        if (tappedBatch.length > 0)
          batchPromises.push(
            batchUpsertEventsWithRetry("tapped_events", tappedBatch)
          );
        if (roundEndedBatch.length > 0)
          batchPromises.push(
            batchUpsertEventsWithRetry("round_ended_events", roundEndedBatch)
          );
        if (batchPromises.length > 0) {
          const results = await Promise.all(batchPromises);
          results.forEach((result) => {
            if (!result.success)
              log(
                `Failed final upsert batch. Error: ${JSON.stringify(
                  result.error
                )}`,
                "error"
              );
          });
        }
      } // End if(res.data...)

      // Update block position (unchanged)
      if (res.nextBlock) {
        const previousBlock = currentBlock;
        currentBlock = res.nextBlock;
        query.fromBlock = currentBlock;
        if (currentBlock - lastProgressLogBlock >= 10000) {
          // Progress log (unchanged)
          const seconds = (performance.now() - runStartTime) / 1000;
          const totalEvents = eventCounts.Tapped + eventCounts.RoundEnded;
          log(
            `Progress: Block ${currentBlock} | ${totalEvents} events | ${
              eventCounts.SupabaseEventsUpserted
            } DB upserts | ${
              eventCounts.SupabaseErrors
            } DB errors | ${seconds.toFixed(1)}s`,
            "normal"
          );
          lastProgressLogBlock = currentBlock;
        }
      } else if (res.data && res.data.logs && res.data.logs.length > 0) {
        // Block update at tip (unchanged)
        const lastLogBlock =
          res.data.logs[res.data.logs.length - 1]?.blockNumber;
        if (lastLogBlock && lastLogBlock >= currentBlock) {
          currentBlock = lastLogBlock + 1;
          query.fromBlock = currentBlock;
          log(
            `Advanced currentBlock to ${currentBlock} based on last log at tip`,
            "verbose"
          );
        }
      }
    } // End while(true) loop
  } catch (error) {
    // Fatal error handling (unchanged)
    log(`Fatal error in main loop: ${error.message}`, "error");
    log(`Stack Trace: ${error.stack}`, "error");
    if (stream) {
      try {
        await stream.close();
        log("Hypersync stream closed.", "normal");
      } catch (closeE) {
        log(`Error closing stream: ${closeE.message}`, "error");
      }
    }
    process.exit(1);
  }
}

// --- Start the application ---
main().catch((error) => {
  log(`Unhandled error in main execution: ${error.message}`, "error");
  log(`Stack Trace: ${error.stack}`, "error");
  process.exit(1);
});
