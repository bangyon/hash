/**
 * HASH256.org Mining Bot v3.0
 * Upgrades:
 * - Wider nonce range (0-1T)
 * - Challenge pre-check before submit
 * - Auto gas bump if TX pending too long
 * - Multi-instance coordinator (nonce partitioning)
 * - Epoch monitoring (instant reset on new challenge)
 * - Dual RPC (read + flashbots submit)
 */

const { ethers } = require("ethers");
const { Worker, isMainThread, parentPort, workerData } = require("worker_threads");
const os = require("os");
const fs = require("fs");
require("dotenv").config();

// ─── CONFIG ───────────────────────────────────────────────────────────────────
const CONFIG = {
  RPC_URL:        process.env.RPC_URL,
  FLASHBOTS_RPC:  process.env.FLASHBOTS_RPC || process.env.RPC_URL,
  PRIVATE_KEY:    process.env.PRIVATE_KEY,
  CONTRACT_ADDR:  "0xAC7b5d06fa1e77D08aea40d46cB7C5923A87A0cc",
  THREADS:        os.cpus().length,
  BATCH_SIZE:     100000,
  GAS_LIMIT:      200000,
  MAX_GWEI:       50,
  GAS_WAIT_MS:    15000,
  MAX_RETRIES:    5,
  GAS_BUMP_PCT:   120,       // bump gas 20% if pending too long
  GAS_BUMP_MS:    30000,     // bump after 30s pending
  LOG_FILE:       "miner.log",
  TG_BOT_TOKEN:   process.env.TG_BOT_TOKEN || "",
  TG_CHAT_ID:     process.env.TG_CHAT_ID   || "",

  // Multi-instance: set INSTANCE_ID dan TOTAL_INSTANCES di .env
  // misal instance 0 dari 3: INSTANCE_ID=0, TOTAL_INSTANCES=3
  INSTANCE_ID:       Number(process.env.INSTANCE_ID       || 0),
  TOTAL_INSTANCES:   Number(process.env.TOTAL_INSTANCES   || 1),
};

const ABI = [
  "function getChallenge(address miner) view returns (bytes32)",
  "function miningState() view returns (uint256 era, uint256 reward, uint256 difficulty, uint256 minted, uint256 remaining, uint256 epoch, uint256 epochBlocksLeft_)",
  "function mine(uint256 nonce)",
];

// ─── WORKER ───────────────────────────────────────────────────────────────────
if (!isMainThread) {
  const { ethers } = require("ethers");
  const { challenge, difficulty, startNonce, batchSize } = workerData;
  const diff = BigInt(difficulty);
  let nonce = BigInt(startNonce);
  const end = nonce + BigInt(batchSize);

  for (; nonce < end; nonce++) {
    const hash = ethers.solidityPackedKeccak256(["bytes32", "uint256"], [challenge, nonce]);
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

const stats = { totalHashes: 0n, solutions: 0, startTime: Date.now(), failed: 0 };

function hashRate() {
  const elapsed = (Date.now() - stats.startTime) / 1000;
  const hps = Number(stats.totalHashes) / elapsed;
  if (hps >= 1e6) return (hps / 1e6).toFixed(2) + " MH/s";
  if (hps >= 1e3) return (hps / 1e3).toFixed(2) + " KH/s";
  return hps.toFixed(0) + " H/s";
}

// Nonce partitioning untuk multi-instance
// Total nonce space: 1 Triliun, dibagi per instance
const NONCE_SPACE = 1_000_000_000_000n; // 1T
function getInstanceNonceStart() {
  const sliceSize = NONCE_SPACE / BigInt(CONFIG.TOTAL_INSTANCES);
  const base = sliceSize * BigInt(CONFIG.INSTANCE_ID);
  // random start dalam slice ini
  const rand = BigInt(Math.floor(Math.random() * Number(sliceSize)));
  return base + rand;
}

function runWorkerBatch(challenge, difficulty, baseNonce) {
  return new Promise((resolve) => {
    const threads = CONFIG.THREADS;
    const perThread = CONFIG.BATCH_SIZE;
    let completed = 0, foundResult = null;

    for (let i = 0; i < threads; i++) {
      const startNonce = (BigInt(baseNonce) + BigInt(i) * BigInt(perThread)).toString();
      const worker = new Worker(__filename, {
        workerData: { challenge, difficulty: difficulty.toString(), startNonce, batchSize: perThread },
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

// Submit dengan pre-check challenge + auto gas bump
async function submitSolution(readContract, submitContract, submitProvider, nonce, currentChallenge, retries = 0) {
  if (retries >= CONFIG.MAX_RETRIES) {
    log(`❌ Max retries reached, skipping`);
    stats.failed++;
    return false;
  }

  // Pre-check: pastikan challenge belum berubah
  try {
    const miner = await submitProvider.getSigner ? (await submitContract.runner.getAddress()) : submitContract.runner.address;
    const liveChallenge = await readContract.getChallenge(miner);
    if (liveChallenge !== currentChallenge) {
      log(`⚠️  Challenge sudah berubah sebelum submit — skip, mining ulang...`);
      return "challenge_changed";
    }
  } catch (_) {}

  // Cek gas
  const feeData = await submitProvider.getFeeData();
  const gwei    = Number(ethers.formatUnits(feeData.gasPrice, "gwei"));

  if (gwei > CONFIG.MAX_GWEI) {
    log(`⛽ Gas ${gwei.toFixed(1)} gwei terlalu tinggi, tunggu ${CONFIG.GAS_WAIT_MS / 1000}s...`);
    await new Promise(r => setTimeout(r, CONFIG.GAS_WAIT_MS));
    return submitSolution(readContract, submitContract, submitProvider, nonce, currentChallenge, retries + 1);
  }

  try {
    log(`📡 Submit — gas: ${gwei.toFixed(1)} gwei (attempt ${retries + 1})`);

    const gasPrice = feeData.gasPrice;
    const tx = await submitContract.mine(BigInt(nonce), {
      gasLimit: CONFIG.GAS_LIMIT,
      gasPrice,
    });
    log(`📨 TX: ${tx.hash}`);
    log(`🔗 https://etherscan.io/tx/${tx.hash}`);

    // Wait dengan auto gas bump
    const receipt = await Promise.race([
      tx.wait(),
      new Promise(async (_, reject) => {
        await new Promise(r => setTimeout(r, CONFIG.GAS_BUMP_MS));
        reject(new Error("TX_SLOW"));
      }),
    ]).catch(async (e) => {
      if (e.message === "TX_SLOW") {
        log(`⏱️  TX lambat, bump gas ${CONFIG.GAS_BUMP_PCT}%...`);
        const bumpedGas = gasPrice * BigInt(CONFIG.GAS_BUMP_PCT) / 100n;
        const newTx = await submitContract.mine(BigInt(nonce), {
          gasLimit: CONFIG.GAS_LIMIT,
          gasPrice: bumpedGas,
          nonce: tx.nonce, // same nonce = replace TX
        });
        log(`📨 Bumped TX: ${newTx.hash}`);
        return await newTx.wait();
      }
      throw e;
    });

    if (receipt && receipt.status === 1) {
      stats.solutions++;
      log(`✅ MINED! Block: ${receipt.blockNumber} | Solutions: ${stats.solutions} | Failed: ${stats.failed}`);
      await notify(`🎉 HASH256 Mined!\nBlock: ${receipt.blockNumber}\nSolutions: ${stats.solutions}\nhttps://etherscan.io/tx/${tx.hash}`);
      return true;
    } else {
      log("❌ TX reverted");
      stats.failed++;
      return false;
    }
  } catch (e) {
    log(`⚠️  Error (attempt ${retries + 1}): ${e.shortMessage || e.message.slice(0, 100)}`);
    await new Promise(r => setTimeout(r, 3000));
    return submitSolution(readContract, submitContract, submitProvider, nonce, currentChallenge, retries + 1);
  }
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────
async function main() {
  console.log("╔══════════════════════════════════════════════════╗");
  console.log("║        HASH256.org Mining Bot v3.0               ║");
  console.log(`║        Threads: ${String(CONFIG.THREADS).padEnd(2)} | Instance: ${CONFIG.INSTANCE_ID}/${CONFIG.TOTAL_INSTANCES}               ║`);
  console.log("╚══════════════════════════════════════════════════╝\n");

  if (!CONFIG.RPC_URL || !CONFIG.PRIVATE_KEY) {
    console.error("❌ Isi RPC_URL dan PRIVATE_KEY di file .env dulu!");
    process.exit(1);
  }

  // Dual provider
  const readProvider   = new ethers.JsonRpcProvider(CONFIG.RPC_URL);
  const submitProvider = new ethers.JsonRpcProvider(CONFIG.FLASHBOTS_RPC);
  const wallet         = new ethers.Wallet(CONFIG.PRIVATE_KEY, readProvider);
  const submitWallet   = new ethers.Wallet(CONFIG.PRIVATE_KEY, submitProvider);
  const readContract   = new ethers.Contract(CONFIG.CONTRACT_ADDR, ABI, wallet);
  const submitContract = new ethers.Contract(CONFIG.CONTRACT_ADDR, ABI, submitWallet);
  const miner          = wallet.address;

  try {
    const block = await readProvider.getBlockNumber();
    const bal   = await readProvider.getBalance(miner);
    log(`✅ Connected — block #${block}`);
    log(`👛 Wallet    : ${miner}`);
    log(`💰 Balance   : ${ethers.formatEther(bal)} ETH`);
    log(`🖥️  Threads   : ${CONFIG.THREADS} cores`);
    log(`📦 Instance  : ${CONFIG.INSTANCE_ID} of ${CONFIG.TOTAL_INSTANCES}`);
  } catch (e) {
    log(`❌ RPC error: ${e.message}`);
    process.exit(1);
  }

  let challenge, difficulty, currentEpoch, nextRefresh = 0;

  const refreshState = async () => {
    try {
      const state   = await readContract.miningState();
      const newChallenge = await readContract.getChallenge(miner);
      const newEpoch     = state.epoch.toString();

      if (newEpoch !== currentEpoch) {
        if (currentEpoch !== undefined) log(`🔔 Epoch baru! ${currentEpoch} → ${newEpoch}`);
        currentEpoch = newEpoch;
      }

      challenge  = newChallenge;
      difficulty = BigInt(state.difficulty.toString());

      log(`🔄 Challenge : ${challenge}`);
      log(`⚙️  Difficulty: ${difficulty.toString().slice(0, 20)}...`);
      log(`📊 Era: ${state.era} | Reward: ${ethers.formatUnits(state.reward, 18)} HASH | Epoch: ${currentEpoch}\n`);
      nextRefresh = Date.now() + 30_000; // refresh tiap 30s (lebih sering untuk detect epoch baru)
    } catch (e) {
      log(`❌ Contract error: ${e.shortMessage || e.message} — retry 15s...`);
      await new Promise(r => setTimeout(r, 15000));
      return refreshState();
    }
  };

  await refreshState();
  log(`⛏️  Mining started...\n`);

  let baseNonce = getInstanceNonceStart();
  let logTimer  = Date.now();

  while (true) {
    // Refresh challenge
    if (Date.now() > nextRefresh) {
      const prevChallenge = challenge;
      const prevEpoch     = currentEpoch;
      await refreshState();
      if (challenge !== prevChallenge || currentEpoch !== prevEpoch) {
        log(`🔄 Challenge/Epoch berubah — reset nonce`);
        baseNonce = getInstanceNonceStart();
      }
    }

    const result = await runWorkerBatch(challenge, difficulty, baseNonce);
    baseNonce += BigInt(CONFIG.BATCH_SIZE * CONFIG.THREADS);

    // Progress log
    if (Date.now() - logTimer > 10000) {
      process.stdout.write(
        `\r⛏️  ${hashRate()} | ${(Number(stats.totalHashes) / 1e6).toFixed(1)}M hashes | ✅ ${stats.solutions} | ❌ ${stats.failed}   `
      );
      logTimer = Date.now();
    }

    if (result && result.found) {
      console.log("");
      log(`🎉 Solution! Nonce: ${result.nonce} | Hash: ${result.hash}`);
      const ok = await submitSolution(readContract, submitContract, submitProvider, result.nonce, challenge);
      if (ok === true || ok === "challenge_changed") {
        nextRefresh = 0; // force refresh setelah submit
      }
    }
  }
}

main().catch(e => {
  console.error("❌ Fatal:", e.message);
  process.exit(1);
});
