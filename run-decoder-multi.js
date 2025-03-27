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

// Define the target address to track approvals for (without 0x prefix for topic filtering)
const TARGET_ADDRESS = "0x3EA751a05922A663AeabFA05a014daf8643D4a3D";
const TARGET_ADDRESS_NO_PREFIX = TARGET_ADDRESS.substring(2).toLowerCase();
// Address with padding for topic matching (addresses in topics are padded to 32 bytes)
const TARGET_ADDRESS_PADDED =
  "0x000000000000000000000000" + TARGET_ADDRESS_NO_PREFIX;

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
  url: "http://eth.hypersync.xyz",
});

// Define query for ERC20 events related to our target address
// Using proper topic filtering based on HyperSync documentation
let query = {
  fromBlock: 0,
  logs: [
    // Filter for Approval events where target address is the owner (topic1)
    {
      topics: [[APPROVAL_TOPIC], [TARGET_ADDRESS_PADDED], []],
    },
    // Filter for Transfer events where target address is from (topic1)
    {
      topics: [[TRANSFER_TOPIC], [TARGET_ADDRESS_PADDED], []],
    },
    // Also get Transfer events where target address is to (topic2)
    {
      topics: [[TRANSFER_TOPIC], [], [TARGET_ADDRESS_PADDED]],
    },
  ],
  // Also filter for transactions involving the target address
  transactions: [
    {
      from: [TARGET_ADDRESS],
    },
    {
      to: [TARGET_ADDRESS],
    },
  ],
  fieldSelection: {
    log: [
      LogField.BlockNumber,
      LogField.LogIndex,
      LogField.TransactionIndex,
      LogField.TransactionHash,
      LogField.Data,
      LogField.Address,
      LogField.Topic0,
      LogField.Topic1,
      LogField.Topic2,
      LogField.Topic3,
    ],
    transaction: [
      TransactionField.From, // To track which address initiated transactions
      TransactionField.To,
      TransactionField.Hash,
    ],
  },
  joinMode: JoinMode.JoinTransactions, // Ensure we get transaction data for each log
};

const main = async () => {
  console.log(`Starting approval tracking for address: ${TARGET_ADDRESS}...`);

  // Create decoder outside the loop for better performance
  const decoder = Decoder.fromSignatures([
    "Transfer(address indexed from, address indexed to, uint256 amount)",
    "Approval(address indexed owner, address indexed spender, uint256 amount)",
  ]);

  // Track approvals by token and spender
  // Format: { tokenAddress: { spenderAddress: { amount: BigInt, blockNumber: number } } }
  const approvals = {};

  // Track transfers that might be using approvals
  // Format: { tokenAddress: { spenderAddress: usedAmount } }
  const transfersUsingApprovals = {};

  let totalEvents = 0;
  const startTime = performance.now();

  // Track the tokens we've seen for final reporting
  const tokenAddresses = new Set();

  // Start streaming events
  const stream = await client.stream(query, {});

  while (true) {
    const res = await stream.recv();

    // Exit if we've reached the end of the chain
    if (res === null) {
      console.log("Reached the tip of the blockchain");
      break;
    }

    // Process events
    if (res.data && res.data.logs) {
      totalEvents += res.data.logs.length;

      // Decode logs
      const decodedLogs = await decoder.decodeLogs(res.data.logs);

      // Process ERC20 events
      for (let i = 0; i < decodedLogs.length; i++) {
        const log = decodedLogs[i];
        // Skip invalid logs
        if (log === null) {
          continue;
        }

        try {
          // Get the original raw log and transaction
          const rawLog = res.data.logs[i];
          if (!rawLog || !rawLog.topics || !rawLog.topics[0]) {
            continue;
          }

          const topic0 = rawLog.topics[0];
          const tokenAddress = rawLog.address.toLowerCase();
          tokenAddresses.add(tokenAddress);

          // Find corresponding transaction for this log
          const txHash = rawLog.transactionHash;
          const transaction = res.data.transactions?.find(
            (tx) => tx.hash === txHash
          );
          const txSender = transaction?.from?.toLowerCase() || null;

          if (topic0 === APPROVAL_TOPIC) {
            // Get owner and spender from indexed parameters
            const owner = log.indexed[0]?.val.toString().toLowerCase() || "";
            const spender = log.indexed[1]?.val.toString().toLowerCase() || "";
            const amount = log.body[0]?.val || BigInt(0);

            // Only track approvals where the target address is the owner
            if (owner === TARGET_ADDRESS.toLowerCase()) {
              // Initialize token in approvals map if needed
              if (!approvals[tokenAddress]) {
                approvals[tokenAddress] = {};
              }

              // Store latest approval with block number for chronological ordering
              approvals[tokenAddress][spender] = {
                amount,
                blockNumber: rawLog.blockNumber,
                txHash,
              };

              console.log(
                `New Approval at block ${
                  rawLog.blockNumber
                }: Token ${tokenAddress}, Spender ${spender}, Amount ${amount.toString()}`
              );
            }
          } else if (topic0 === TRANSFER_TOPIC) {
            // Get from and to from indexed parameters
            const from = log.indexed[0]?.val.toString().toLowerCase() || "";
            const to = log.indexed[1]?.val.toString().toLowerCase() || "";
            const amount = log.body[0]?.val || BigInt(0);

            // Track transfers where the target has approved a spender (from = target, to = any)
            if (from === TARGET_ADDRESS.toLowerCase()) {
              // Initialize token in transfers map if needed
              if (!transfersUsingApprovals[tokenAddress]) {
                transfersUsingApprovals[tokenAddress] = {};
              }

              // Check two cases:
              // 1. Transaction initiated by spender (typical approval usage)
              // 2. Transaction initiated by owner but sent to a contract with approval
              const isSpenderInitiated =
                txSender && txSender !== from.toLowerCase();
              const isOwnerInitiatedToSpender =
                txSender === from.toLowerCase() && transaction?.to;

              if (isSpenderInitiated) {
                // Track against the transaction sender (spender)
                if (!transfersUsingApprovals[tokenAddress][txSender]) {
                  transfersUsingApprovals[tokenAddress][txSender] = BigInt(0);
                }
                transfersUsingApprovals[tokenAddress][txSender] += amount;

                console.log(`Transfer using approval at block ${
                  rawLog.blockNumber
                }: 
                  Token: ${tokenAddress}, 
                  From: ${from}, 
                  To: ${to}, 
                  Amount: ${amount.toString()}, 
                  Tx Sender (Spender): ${txSender}`);
              } else if (isOwnerInitiatedToSpender) {
                // When owner initiates a transaction to a spender, check if the transaction.to
                // address matches any approved spender for this token
                const txTo = transaction.to.toLowerCase();

                // Check if txTo is an approved spender
                if (approvals[tokenAddress]?.[txTo]) {
                  if (!transfersUsingApprovals[tokenAddress][txTo]) {
                    transfersUsingApprovals[tokenAddress][txTo] = BigInt(0);
                  }
                  transfersUsingApprovals[tokenAddress][txTo] += amount;

                  console.log(`Owner-initiated transfer to spender at block ${
                    rawLog.blockNumber
                  }: 
                    Token: ${tokenAddress}, 
                    From: ${from}, 
                    To: ${to}, 
                    Amount: ${amount.toString()}, 
                    Tx Recipient (Spender): ${txTo}`);
                }
              }
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

  // Calculate and print outstanding approvals
  console.log("\n========== OUTSTANDING APPROVALS ==========");
  console.log(`ADDRESS: ${TARGET_ADDRESS}`);
  console.log("==========================================\n");

  let hasOutstandingApprovals = false;

  // Iterate through all tokens we've seen
  for (const tokenAddress of tokenAddresses) {
    let tokenHasApprovals = false;
    const tokenApprovals = approvals[tokenAddress] || {};

    // Process each spender for this token
    for (const spender in tokenApprovals) {
      const { amount: approvedAmount, blockNumber } = tokenApprovals[spender];

      // Get transferred amount (if any)
      const transferredAmount =
        transfersUsingApprovals[tokenAddress]?.[spender] || BigInt(0);

      // Calculate remaining approval
      let remainingApproval;
      let isUnlimited = false;

      // Check for unlimited approval (common value is 2^256-1)
      if (
        approvedAmount === BigInt(2) ** BigInt(256) - BigInt(1) ||
        approvedAmount ===
          BigInt(
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
          )
      ) {
        remainingApproval = approvedAmount;
        isUnlimited = true;
      } else {
        // For limited approvals, subtract used amount
        remainingApproval =
          approvedAmount > transferredAmount
            ? approvedAmount - transferredAmount
            : BigInt(0);
      }

      // Only display if there's a remaining approval
      if (remainingApproval > 0) {
        if (!tokenHasApprovals) {
          console.log(`\nTOKEN: ${tokenAddress}`);
          tokenHasApprovals = true;
          hasOutstandingApprovals = true;
        }

        console.log(`  SPENDER: ${spender}`);
        console.log(`  APPROVED AMOUNT: ${approvedAmount.toString()}`);
        console.log(`  USED AMOUNT: ${transferredAmount.toString()}`);
        console.log(`  REMAINING APPROVAL: ${remainingApproval.toString()}`);
        console.log(`  APPROVED AT BLOCK: ${blockNumber}`);
        console.log(`  TX HASH: ${tokenApprovals[spender].txHash}`);

        if (isUnlimited) {
          console.log(`  ⚠️ UNLIMITED APPROVAL`);
        }

        console.log("  ----------------------------------------");
      }
    }
  }

  if (!hasOutstandingApprovals) {
    console.log("\nNo outstanding approvals found.");
  }

  // Print final results
  const totalTime = (performance.now() - startTime) / 1000;
  console.log(
    `\nScan complete: ${totalEvents} events in ${totalTime.toFixed(1)} seconds`
  );
};

main().catch((error) => {
  console.error("Error:", error);
  process.exit(1);
});
