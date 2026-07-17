'use strict';

const { exec } = require('child_process');
const storage = require('./storage');
const { computeDelaySeconds } = require('./backoff');

const WORKER_ID = process.env.QUEUECTL_WORKER_ID || `worker-${process.pid}`;
const POLL_INTERVAL_MS = Number(process.env.QUEUECTL_POLL_INTERVAL_MS || 500);

let stopping = false;
let currentJobId = null;

function nowIso() {
  return new Date().toISOString();
}

function log(msg) {
  console.log(`[${WORKER_ID}] ${msg}`);
}

function registerWorker() {
  storage.transaction((state) => {
    const existing = state.workers.find((w) => w.id === WORKER_ID);
    if (existing) {
      existing.pid = process.pid;
      existing.status = 'running';
      existing.started_at = nowIso();
    } else {
      state.workers.push({ id: WORKER_ID, pid: process.pid, status: 'running', started_at: nowIso() });
    }
  });
}

function updateWorkerStatus(status) {
  storage.transaction((state) => {
    const w = state.workers.find((w) => w.id === WORKER_ID);
    if (w) {
      w.status = status;
      w.updated_at = nowIso();
    }
  });
}

// Atomically finds and claims the oldest eligible job. This runs inside the
// cross-process storage lock, so two workers can never claim the same job -
// this is what prevents duplicate processing.
function claimNextJob() {
  return storage.transaction((state) => {
    const now = Date.now();
    const candidates = Object.values(state.jobs)
      .filter((j) => j.state === 'pending' || j.state === 'failed')
      .filter((j) => !j.next_run_at || Date.parse(j.next_run_at) <= now)
      .sort((a, b) => a.created_at.localeCompare(b.created_at));
    if (candidates.length === 0) return null;
    const job = candidates[0];
    job.state = 'processing';
    job.worker_id = WORKER_ID;
    job.updated_at = nowIso();
    return JSON.parse(JSON.stringify(job));
  });
}

function finishJob(id, outcome) {
  storage.transaction((state) => {
    const job = state.jobs[id];
    if (!job) return; // job vanished, nothing to do
    const base = state.config.backoff_base || 2;
    if (outcome.success) {
      job.state = 'completed';
      job.last_error = null;
    } else {
      job.attempts += 1;
      job.last_error = outcome.error;
      if (job.attempts >= job.max_retries) {
        job.state = 'dead'; // moved to Dead Letter Queue
      } else {
        job.state = 'failed';
        const delaySec = computeDelaySeconds(base, job.attempts);
        job.next_run_at = new Date(Date.now() + delaySec * 1000).toISOString();
      }
    }
    job.worker_id = null;
    job.updated_at = nowIso();
  });
}

function runCommand(command) {
  return new Promise((resolve) => {
    exec(command, (error, stdout, stderr) => {
      if (error) {
        resolve({ success: false, error: error.message, stdout, stderr });
      } else {
        resolve({ success: true, stdout, stderr });
      }
    });
  });
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function loop() {
  registerWorker();
  log(`started (pid ${process.pid})`);
  while (!stopping) {
    let job;
    try {
      job = claimNextJob();
    } catch (e) {
      log(`error claiming job: ${e.message}`);
      await sleep(POLL_INTERVAL_MS);
      continue;
    }
    if (!job) {
      await sleep(POLL_INTERVAL_MS);
      continue;
    }
    currentJobId = job.id;
    log(`running job "${job.id}": ${job.command}`);
    const outcome = await runCommand(job.command);
    finishJob(job.id, outcome);
    log(`job "${job.id}" ${outcome.success ? 'completed' : `failed (${outcome.error})`}`);
    currentJobId = null;
  }
  updateWorkerStatus('stopped');
  log('stopped gracefully');
  process.exit(0);
}

process.on('SIGTERM', () => {
  log(`received SIGTERM, finishing current job (${currentJobId || 'none'}) before exit...`);
  stopping = true;
});
process.on('SIGINT', () => {
  stopping = true;
});

loop();
