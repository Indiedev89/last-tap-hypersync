import { keccak256, toHex } from "viem";
import {
  HypersyncClient,
  LogField,
  JoinMode,
  BlockField,
  TransactionField,
  HexOutput,
  DataType,
  Decoder,
} from "@envio-dev/hypersync-client";

// Define ERC20 event signatures
const event_signatures = [
  "Transfer(address,address,uint256)",
  "Approval(address,address,uint256)",
];

// Create topic0 hashes from event signatures
const topic0_list = event_signatures.map((sig) => keccak256(toHex(sig)));

// Store individual topic hashes for easier comparison
const TRANSFER_TOPIC = topic0_list[0];
const APPROVAL_TOPIC = topic0_list[1];

// Create mapping from topic0 hash to event name
const topic0ToName = {};
topic0ToName[TRANSFER_TOPIC] = "Transfer";
topic0ToName[APPROVAL_TOPIC] = "Approval";

// Initialize Hypersync client
const client = HypersyncClient.new({
  url: "http://unichain.hypersync.xyz",
});

// Define query for Uniswap V3 events
let query = {
  fromBlock: 0,
  logs: [
    {
      topics: [topic0_list],
    },
  ],
  fieldSelection: {
    // block: [BlockField.Number, BlockField.Timestamp, BlockField.Hash],
    log: [
      // LogField.BlockNumber,
      // LogField.LogIndex,
      // LogField.TransactionIndex,
      // LogField.TransactionHash,
      LogField.Data,
      LogField.Address,
      LogField.Topic0,
      LogField.Topic1,
      LogField.Topic2,
      LogField.Topic3,
    ],
    // transaction: [
    //   TransactionField.From,
    //   TransactionField.To,
    //   TransactionField.Hash,
    //   TransactionField.Value,
    // ],
  },
  joinMode: JoinMode.JoinTransactions,
};

const main = async () => {
  console.log("Starting ERC20 event scan...");

  // Create decoder outside the loop for better performance
  const decoder = Decoder.fromSignatures([
    "Transfer(address indexed from, address indexed to, uint256 amount)",
    "Approval(address indexed owner, address indexed spender, uint256 amount)",
  ]);

  let totalEvents = 0;
  let totalTransfers = 0;
  let totalApprovals = 0;
  let totalTransferAmount = BigInt(0);
  let totalApprovalAmount = BigInt(0);
  const startTime = performance.now();

  // Start streaming events
  const stream = await client.stream(query, {});

  while (true) {
    const res = await stream.recv();

    // Exit if we've reached the end of the chain
    if (res === null) {
      console.log("Reached the tip of the blockchain");
      break;
    }

    // Count total events
    if (res.data && res.data.logs) {
      totalEvents += res.data.logs.length;

      // Decode logs
      const decodedLogs = await decoder.decodeLogs(res.data.logs);

      // Track if we've printed an event for this batch
      let printedEventThisBatch = false;

      // Process ERC20 events
      for (let i = 0; i < decodedLogs.length; i++) {
        const log = decodedLogs[i];
        // Skip invalid logs
        if (log === null) {
          continue;
        }

        // Access the decoded values directly without using JSON.stringify
        try {
          // Get the original raw log to access topic0
          const rawLog = res.data.logs[i];
          if (!rawLog || !rawLog.topics || !rawLog.topics[0]) {
            continue;
          }

          const topic0 = rawLog.topics[0];

          if (topic0 === TRANSFER_TOPIC) {
            // Get from and to from indexed parameters
            const from = log.indexed[0]?.val.toString() || "unknown";
            const to = log.indexed[1]?.val.toString() || "unknown";
            const amount = log.body[0]?.val || BigInt(0);

            totalTransfers++;
            totalTransferAmount += amount;

            // Print details for just the first transfer event in each batch
            if (!printedEventThisBatch) {
              console.log(
                "\nSample Transfer Event from Block " + res.nextBlock + ":"
              );
              console.log(`  From: ${from}`);
              console.log(`  To: ${to}`);
              console.log(`  Amount: ${amount.toString()}`);

              // Mark that we've printed an event for this batch
              printedEventThisBatch = true;
            }
          } else if (topic0 === APPROVAL_TOPIC) {
            // Get owner and spender from indexed parameters
            const owner = log.indexed[0]?.val.toString() || "unknown";
            const spender = log.indexed[1]?.val.toString() || "unknown";
            const amount = log.body[0]?.val || BigInt(0);

            totalApprovals++;
            totalApprovalAmount += amount;

            // Print details for the first approval event in each batch if no transfer was printed
            if (!printedEventThisBatch) {
              console.log(
                "\nSample Approval Event from Block " + res.nextBlock + ":"
              );
              console.log(`  Owner: ${owner}`);
              console.log(`  Spender: ${spender}`);
              console.log(`  Amount: ${amount.toString()}`);

              // Mark that we've printed an event for this batch
              printedEventThisBatch = true;
            }
          }
        } catch (error) {
          console.log("Error processing event:", error.message);
        }
      }
    }

    // Update query for next batch
    if (res.nextBlock) {
      query.fromBlock = res.nextBlock;
    }

    // Calculate and print simple progress metrics
    const seconds = (performance.now() - startTime) / 1000;

    console.log(
      `Block ${res.nextBlock} | ${totalEvents} events | ${seconds.toFixed(
        1
      )}s | ${(totalEvents / seconds).toFixed(1)} events/s`
    );
  }

  // Print final results
  const totalTime = (performance.now() - startTime) / 1000;
  console.log(
    `\nScan complete: ${totalEvents} events in ${totalTime.toFixed(1)} seconds`
  );
  console.log(`Total Transfer Events: ${totalTransfers}`);
  console.log(`Total Approval Events: ${totalApprovals}`);
  console.log(`Total Transfer Amount: ${totalTransferAmount.toString()}`);
  console.log(`Total Approval Amount: ${totalApprovalAmount.toString()}`);
};

main().catch((error) => {
  console.error("Error:", error);
  process.exit(1);
});
