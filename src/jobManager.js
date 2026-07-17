'use strict';

const storage = require('./storage');

const VALID_STATES = ['pending', 'processing', 'completed', 'failed', 'dead'];

function nowIso() {
  return new Date().toISOString();
}

function enqueueJob(input) {
  return storage.transaction((state) => {
    if (!input || typeof input !== 'object') {
      throw new Error('Job payload must be a JSON object');
    }
    if (!input.id || typeof input.id !== 'string') {
      throw new Error('Job must have a string "id" field');
    }
    if (!input.command || typeof input.command !== 'string') {
      throw new Error('Job must have a string "command" field');
    }
    if (state.jobs[input.id]) {
      throw new Error(`Job with id "${input.id}" already exists`);
    }
    const maxRetries = input.max_retries !== undefined ? Number(input.max_retries) : state.config.max_retries;
    const job = {
      id: input.id,
      command: input.command,
      state: 'pending',
      attempts: 0,
      max_retries: maxRetries,
      created_at: nowIso(),
      updated_at: nowIso(),
      next_run_at: input.run_at || nowIso(), // supports delayed/scheduled jobs
      worker_id: null,
      last_error: null,
    };
    state.jobs[input.id] = job;
    return job;
  });
}

function listJobs(filterState) {
  if (filterState && !VALID_STATES.includes(filterState)) {
    throw new Error(`Invalid state "${filterState}". Must be one of: ${VALID_STATES.join(', ')}`);
  }
  const state = storage.readState();
  let jobs = Object.values(state.jobs);
  if (filterState) jobs = jobs.filter((j) => j.state === filterState);
  jobs.sort((a, b) => a.created_at.localeCompare(b.created_at));
  return jobs;
}

function getStatus() {
  const state = storage.readState();
  const counts = { pending: 0, processing: 0, completed: 0, failed: 0, dead: 0 };
  Object.values(state.jobs).forEach((j) => {
    counts[j.state] = (counts[j.state] || 0) + 1;
  });
  return {
    counts,
    total: Object.keys(state.jobs).length,
    active_workers: state.workers.filter((w) => w.status === 'running').length,
    workers: state.workers,
    config: state.config,
  };
}

function listDLQ() {
  return listJobs('dead');
}

function retryFromDLQ(id) {
  return storage.transaction((state) => {
    const job = state.jobs[id];
    if (!job) throw new Error(`Job "${id}" not found`);
    if (job.state !== 'dead') {
      throw new Error(`Job "${id}" is not in the DLQ (current state: ${job.state})`);
    }
    job.state = 'pending';
    job.attempts = 0;
    job.next_run_at = nowIso();
    job.last_error = null;
    job.updated_at = nowIso();
    return job;
  });
}

function setConfig(key, value) {
  return storage.transaction((state) => {
    state.config[key] = value;
    return state.config;
  });
}

function getConfig() {
  return storage.readState().config;
}

module.exports = {
  enqueueJob,
  listJobs,
  getStatus,
  listDLQ,
  retryFromDLQ,
  setConfig,
  getConfig,
  VALID_STATES,
};
