'use strict';

const fs = require('fs');
const path = require('path');

const DATA_DIR = process.env.QUEUECTL_DATA_DIR || path.join(__dirname, '..', 'data');
const STATE_FILE = path.join(DATA_DIR, 'state.json');
const LOCK_DIR = path.join(DATA_DIR, '.lock');

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
}

function defaultState() {
  return {
    jobs: {},
    config: { backoff_base: 2, max_retries: 3 },
    workers: [],
  };
}

function readStateRaw() {
  ensureDataDir();
  if (!fs.existsSync(STATE_FILE)) {
    const state = defaultState();
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
    return state;
  }
  const raw = fs.readFileSync(STATE_FILE, 'utf8');
  if (!raw.trim()) return defaultState();
  try {
    const parsed = JSON.parse(raw);
    // Backfill in case of older/partial state files
    return { ...defaultState(), ...parsed };
  } catch (e) {
    // Corrupted file: back it up rather than silently losing data.
    fs.copyFileSync(STATE_FILE, STATE_FILE + '.corrupt.' + Date.now());
    return defaultState();
  }
}

function writeStateRaw(state) {
  ensureDataDir();
  const tmp = STATE_FILE + '.tmp-' + process.pid;
  fs.writeFileSync(tmp, JSON.stringify(state, null, 2));
  fs.renameSync(tmp, STATE_FILE); // atomic on POSIX filesystems
}

function sleepSync(ms) {
  const sab = new SharedArrayBuffer(4);
  const view = new Int32Array(sab);
  Atomics.wait(view, 0, 0, ms);
}

// mkdirSync is atomic (EEXIST if it already exists), so we use a lock
// *directory* as a cross-process mutex. This is what makes it safe for
// multiple worker processes to claim jobs without racing each other.
function acquireLock(timeoutMs = 15000, retryDelayMs = 20) {
  ensureDataDir();
  const start = Date.now();
  while (true) {
    try {
      fs.mkdirSync(LOCK_DIR);
      return;
    } catch (e) {
      if (e.code !== 'EEXIST') throw e;
      // Break stale locks (e.g. a worker was killed -9 mid-transaction).
      try {
        const stat = fs.statSync(LOCK_DIR);
        if (Date.now() - stat.mtimeMs > timeoutMs) {
          fs.rmdirSync(LOCK_DIR);
          continue;
        }
      } catch (_) { /* lock disappeared, loop and retry */ }
      if (Date.now() - start > timeoutMs) {
        throw new Error('Timed out waiting for storage lock');
      }
      sleepSync(retryDelayMs + Math.floor(Math.random() * 25));
    }
  }
}

function releaseLock() {
  try {
    fs.rmdirSync(LOCK_DIR);
  } catch (_) { /* already released */ }
}

/**
 * Run fn(state) inside an exclusive cross-process lock. fn may mutate
 * `state` in place and/or return a value. The mutated state is persisted
 * before the lock is released.
 */
function transaction(fn) {
  acquireLock();
  try {
    const state = readStateRaw();
    const result = fn(state);
    writeStateRaw(state);
    return result;
  } finally {
    releaseLock();
  }
}

function readState() {
  return readStateRaw();
}

module.exports = { transaction, readState, DATA_DIR, STATE_FILE };
