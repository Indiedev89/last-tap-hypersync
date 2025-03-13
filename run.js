import { keccak256, toHex } from "viem";
import {
  HypersyncClient,
  Decoder,
  LogField,
  JoinMode,
  BlockField,
  TransactionField,
} from "@envio-dev/hypersync-client";

// Function to create a simple ASCII progress bar
const createProgressBar = (percentage, width = 40) => {
  const filledWidth = Math.round(width * percentage);
  const emptyWidth = width - filledWidth;
  const filledBar = "█".repeat(filledWidth);
  const emptyBar = "░".repeat(emptyWidth);
  return `[${filledBar}${emptyBar}] ${(percentage * 100).toFixed(2)}%`;
};

// Function to clear the console line
const clearLine = () => {
  process.stdout.write("\r\x1b[K");
};

const event_signatures = [
  "PoolCreated(address,address,uint24,int24,address)",
  "Burn(address,int24,int24,uint128,uint256,uint256)",
  "Initialize(uint160,int24)",
  "Mint(address,address,int24,int24,uint128,uint256,uint256)",
  "Swap(address,address,int256,int256,uint160,uint128,int24)",
];

const topic0_list = event_signatures.map((sig) => keccak256(toHex(sig)));

const client = HypersyncClient.new({
  url: "http://eth.hypersync.xyz",
});

let query = {
  fromBlock: 0,
  logs: [
    {
      // Get all events that have any of the topic0 values we want
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
  // Count events by type
  const eventCounts = {
    PoolCreated: 0,
    Burn: 0,
    Initialize: 0,
    Mint: 0,
    Swap: 0,
    Unknown: 0,
    Total: 0,
  };

  const startTime = performance.now();

  // Get current blockchain height
  const height = await client.getHeight();
  console.log(`Starting scan from block 0 to current height: ${height}`);

  // Create a decoder for the event signatures
  const decoder = Decoder.fromSignatures(event_signatures);

  // Start streaming events
  const stream = await client.stream(query, {});

  while (true) {
    const res = await stream.recv();

    // Quit if we reached the tip
    if (res === null) {
      console.log(`\nReached the tip of the blockchain!`);
      break;
    }

    // Update the logs count
    if (res.data && res.data.logs && res.data.logs.length > 0) {
      // Process logs based on their topic0 value
      res.data.logs.forEach((log) => {
        eventCounts.Total++;

        if (!log.topics || !log.topics[0]) {
          eventCounts.Unknown++;
          return;
        }

        const topic0 = log.topics[0];

        // Match topic0 to determine event type
        if (topic0 === topic0_list[0]) {
          eventCounts.PoolCreated++;
        } else if (topic0 === topic0_list[1]) {
          eventCounts.Burn++;
        } else if (topic0 === topic0_list[2]) {
          eventCounts.Initialize++;
        } else if (topic0 === topic0_list[3]) {
          eventCounts.Mint++;
        } else if (topic0 === topic0_list[4]) {
          eventCounts.Swap++;
        } else {
          eventCounts.Unknown++;
        }
      });

      // Figure out decoder!!
      const decodedLogs = await decoder.decodeLogs(res.data.logs);
      if (decodedLogs && decodedLogs.length > 0) {
        console.log(decodedLogs[0]);
      }
    }

    // Update the fromBlock for the next iteration
    if (res.nextBlock) {
      query.fromBlock = res.nextBlock;
    }

    const currentTime = performance.now();
    const seconds = (currentTime - startTime) / 1000;
    const logsPerSecond = (eventCounts.Total / seconds).toFixed(2);

    // Calculate progress percentage
    const progress = res.nextBlock / height;

    // Clear the previous line and print the progress bar
    clearLine();
    process.stdout.write(
      `${createProgressBar(progress)} Block: ${res.nextBlock}/${height} | ` +
        `Events: ${eventCounts.Total} | ` +
        `Time: ${seconds.toFixed(2)}s | ` +
        `Speed: ${logsPerSecond} events/s`
    );

    // Print event counts every 100 blocks (to avoid console spam)
    if (res.nextBlock % 100 === 0) {
      console.log(
        `\nEvent counts: PoolCreated=${eventCounts.PoolCreated}, Burn=${eventCounts.Burn}, Initialize=${eventCounts.Initialize}, Mint=${eventCounts.Mint}, Swap=${eventCounts.Swap}, Unknown=${eventCounts.Unknown}`
      );
    }
  }

  // Final summary
  console.log("\n--- Final Event Summary ---");
  console.log(`Total Events: ${eventCounts.Total}`);
  console.log(`PoolCreated: ${eventCounts.PoolCreated}`);
  console.log(`Initialize: ${eventCounts.Initialize}`);
  console.log(`Mint: ${eventCounts.Mint}`);
  console.log(`Burn: ${eventCounts.Burn}`);
  console.log(`Swap: ${eventCounts.Swap}`);
  console.log(`Unknown: ${eventCounts.Unknown}`);
  console.log(
    `Total processing time: ${((performance.now() - startTime) / 1000).toFixed(
      2
    )} seconds`
  );
};

main().catch((error) => {
  console.error("Error in main:", error);
  process.exit(1);
});
