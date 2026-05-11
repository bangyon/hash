/**
 * HASH256.org Mining Bot v2.1
 * - ABI & hash logic fixed (solidityPackedKeccak256)
 * - Multi-threading (Worker Threads)
 * - Auto-retry on TX fail
 * - Auto gas wait
 * - File logging
 * - Telegram notifications
 */

const { ethers } = require("ethers");
const { Worker, isMainThread, parentPort, workerData } = require("worker_threads");
const os = require("os");
const fs = require("fs");
require("dotenv").config();

// ─── CONFIG ───────────────────────────────────────────────────────────────────
const CONFIG = {
  RPC_URL:       process.env.RPC_URL,
  PRIVATE_KEY:   process.env.PRIVATE_KEY,
  CONTRACT_ADDR: "0xAC7b5d06fa1e77D08aea40d46cB7C5923A87A0cc",
  THREADS:       os.cpus().length,
  BATCH_SIZE:    100000,
  GAS_LIMIT:     200000,
  MAX_GWEI:      50,
  GAS_WAIT_MS:   15000,
  MAX_RETRIES:   5,
  LOG_FILE:      "miner.log",
  TG_BOT_TOKEN:  process.env.TG_BOT_TOKEN || "",
  TG_CHAT_ID:    process.env.TG_CHAT_ID   || "",
};

const ABI = [
  "function getChallenge(address miner) view returns (bytes32)",
  "function miningState() view returns (uint256 era, uint256 reward, uint256 difficulty, uint256 minted, uint256 remaining, uint256 epoch, uint256 epochBlocksLeft_)",
  "function mine(uint256 nonce)",
];

// ─── WORKER (thread) ──────────────────────────────────────────────────────────
if (!isMainThread) {
  const { ethers } = require("ethers");
  const { challenge, difficulty, startNonce, batchSize } = workerData;
  const diff = BigInt(difficulty);
  let nonce = BigInt(startNonce);
  const end = nonce + BigInt(batchSize);

  for (; nonce < end; nonce++) {
    const hash = ethers.solidityPackedKeccak256(
      ["bytes32", "uint256"],
      [challenge, nonce]
    );
    if (BigInt(hash) < diff) {
      parentPort.postMessage({ found: true, nonce: nonce.toString(), hash });
      process.exit(0);
    }
  }

  parentPort.postMessage({ found: false });
  process.exit(0);
}

// ─── HELPERS ──────────────────────────────────────────────────────────────────
function log(msg) {
  const ts = new Date().toLocaleTimeString("id-ID");
  const line = `[${ts}] ${msg}`;
  console.log(line);
  fs.appendFileSync(CONFIG.LOG_FILE, line + "\n");
}

async function notify(msg) {
  if (!CONFIG.TG_BOT_TOKEN || !CONFIG.TG_CHAT_ID) return;
  try {
    await fetch(`https://api.telegram.org/bot${CONFIG.TG_BOT_TOKEN}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ chat_id: CONFIG.TG_CHAT_ID, text: msg }),
    });
  } catch (_) {}
}

const stats = { totalHashes: 0n, solutions: 0, startTime: Date.now() };

function hashRate() {
  const elapsed = (Date.now() - stats.startTime) / 1000;
  const hps = Number(stats.totalHashes) / elapsed;
  if (hps >= 1e6) return (hps / 1e6).toFixed(2) + " MH/s";
  if (hps >= 1e3) return (hps / 1e3).toFixed(2) + " KH/s";
  return hps.toFixed(0) + " H/s";
}

function runWorkerBatch(challenge, difficulty, baseNonce) {
  return new Promise((resolve) => {
    const threads = CONFIG.THREADS;
    const perThread = CONFIG.BATCH_SIZE;
    let completed = 0, foundResult = null;

    for (let i = 0; i < threads; i++) {
      const startNonce = (BigInt(baseNonce) + BigInt(i) * BigInt(perThread)).toString();
      const worker = new Worker(__filename, {
        workerData: {
          challenge,
          difficulty: difficulty.toString(),
          startNonce,
          batchSize: perThread,
        },
      });

      worker.on("message", (msg) => {
        if (msg.found && !foundResult) foundResult = msg;
        stats.totalHashes += BigInt(perThread);
        if (++completed === threads) resolve(foundResult);
      });

      worker.on("error", () => {
        if (++completed === threads) resolve(foundResult);
      });
    }
  });
}

async function submitSolution(contract, provider, nonce, retries = 0) {
  if (retries >= CONFIG.MAX_RETRIES) {
    log(`❌ Max retries reached, skipping nonce`);
    return false;
  }

  const feeData = await provider.getFeeData();
  const gwei    = Number(ethers.formatUnits(feeData.gasPrice, "gwei"));

  if (gwei > CONFIG.MAX_GWEI) {
    log(`⛽ Gas ${gwei.toFixed(1)} gwei terlalu tinggi, tunggu ${CONFIG.GAS_WAIT_MS / 1000}s...`);
    await new Promise(r => setTimeout(r, CONFIG.GAS_WAIT_MS));
    return submitSolution(contract, provider, nonce, retries + 1);
  }

  try {
    log(`📡 Submit — gas: ${gwei.toFixed(1)} gwei (attempt ${retries + 1})`);
    const tx = await contract.mine(BigInt(nonce), { gasLimit: CONFIG.GAS_LIMIT });
    log(`📨 TX: ${tx.hash}`);
    log(`🔗 https://etherscan.io/tx/${tx.hash}`);
    const receipt = await tx.wait();
    if (receipt.status === 1) {
      stats.solutions++;
      log(`✅ MINED! Block: ${receipt.blockNumber} | Total solutions: ${stats.solutions}`);
      await notify(`🎉 HASH256 Mined!\nBlock: ${receipt.blockNumber}\nTotal: ${stats.solutions}\nhttps://etherscan.io/tx/${tx.hash}`);
      return true;
    } else {
      log("❌ TX reverted");
      return false;
    }
  } catch (e) {
    log(`⚠️  Error (attempt ${retries + 1}): ${e.shortMessage || e.message.slice(0, 100)}`);
    await new Promise(r => setTimeout(r, 3000));
    return submitSolution(contract, provider, nonce, retries + 1);
  }
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────
async function main() {
  console.log("╔══════════════════════════════════════════════╗");
  console.log("║       HASH256.org Mining Bot v2.1            ║");
  console.log(`║       Threads: ${String(CONFIG.THREADS).padEnd(2)} | Batch: ${(CONFIG.BATCH_SIZE * CONFIG.THREADS / 1e3).toFixed(0)}K/round         ║`);
  console.log("╚══════════════════════════════════════════════╝\n");

  if (!CONFIG.RPC_URL || !CONFIG.PRIVATE_KEY) {
    console.error("❌ Isi RPC_URL dan PRIVATE_KEY di file .env dulu!");
    process.exit(1);
  }

  const provider = new ethers.JsonRpcProvider(CONFIG.RPC_URL);
  const wallet   = new ethers.Wallet(CONFIG.PRIVATE_KEY, provider);
  const contract = new ethers.Contract(CONFIG.CONTRACT_ADDR, ABI, wallet);
  const miner    = wallet.address;

  try {
    const block = await provider.getBlockNumber();
    const bal   = await provider.getBalance(miner);
    log(`✅ Connected — block #${block}`);
    log(`👛 Wallet    : ${miner}`);
    log(`💰 Balance   : ${ethers.formatEther(bal)} ETH`);
    log(`🖥️  Threads   : ${CONFIG.THREADS} cores`);
  } catch (e) {
    log(`❌ RPC error: ${e.message}`);
    process.exit(1);
  }

  let challenge, difficulty, nextRefresh = 0;

  const refreshState = async () => {
    try {
      const state = await contract.miningState();
      challenge   = await contract.getChallenge(miner);
      difficulty  = BigInt(state.difficulty.toString());

      log(`🔄 Challenge : ${challenge}`);
      log(`⚙️  Difficulty: ${difficulty.toString().slice(0, 20)}...`);
      log(`📊 Era: ${state.era} | Reward: ${ethers.formatUnits(state.reward, 18)} HASH | Minted: ${state.minted}\n`);
      nextRefresh = Date.now() + 60_000;
    } catch (e) {
      log(`❌ Contract error: ${e.shortMessage || e.message} — retry 30s...`);
      await new Promise(r => setTimeout(r, 30000));
      return refreshState();
    }
  };

  await refreshState();
  log(`⛏️  Mining started with ${CONFIG.THREADS} threads...\n`);

  let baseNonce = BigInt(Math.floor(Math.random() * 1_000_000_000));
  let logTimer  = Date.now();

  while (true) {
    if (Date.now() > nextRefresh) {
      const prev = challenge;
      await refreshState();
      if (challenge !== prev) {
        baseNonce = BigInt(Math.floor(Math.random() * 1_000_000_000));
      }
    }

    const result = await runWorkerBatch(challenge, difficulty, baseNonce);
    baseNonce += BigInt(CONFIG.BATCH_SIZE * CONFIG.THREADS);

    if (Date.now() - logTimer > 10000) {
      process.stdout.write(`\r⛏️  ${hashRate()} | ${(Number(stats.totalHashes) / 1e6).toFixed(1)}M hashes | Solutions: ${stats.solutions}   `);
      logTimer = Date.now();
    }

    if (result && result.found) {
      console.log("");
      log(`🎉 Solution! Nonce: ${result.nonce} | Hash: ${result.hash}`);
      const ok = await submitSolution(contract, provider, result.nonce);
      if (ok) nextRefresh = 0;
    }
  }
}

main().catch(e => {
  console.error("❌ Fatal:", e.message);
  process.exit(1);
});
