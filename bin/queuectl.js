#!/usr/bin/env node
'use strict';

const path = require('path');
const { spawn } = require('child_process');
const jobManager = require('../src/jobManager');
const storage = require('../src/storage');

const args = process.argv.slice(2);
const cmd = args[0];

function printHelp() {
  console.log(`
queuectl - CLI-based background job queue system

Usage:
  queuectl enqueue '<json>'                 Add a new job to the queue
  queuectl worker start [--count N]         Start N worker processes (default 1)
  queuectl worker stop                      Stop all running workers gracefully
  queuectl status                           Show summary of job states & workers
  queuectl list [--state <state>]           List jobs, optionally filtered by state
  queuectl dlq list                         List jobs in the Dead Letter Queue
  queuectl dlq retry <job_id>               Retry a job from the DLQ
  queuectl config set <key> <value>         Set config (max-retries, backoff-base)
  queuectl config get                       Show current configuration
  queuectl help                             Show this help message

Examples:
  queuectl enqueue '{"id":"job1","command":"sleep 2"}'
  queuectl enqueue '{"id":"job2","command":"exit 1","max_retries":2}'
  queuectl worker start --count 3
  queuectl list --state pending
  queuectl dlq retry job2
`);
}

function parseFlags(rest) {
  const flags = {};
  for (let i = 0; i < rest.length; i++) {
    if (rest[i].startsWith('--')) {
      const key = rest[i].slice(2);
      const hasValue = rest[i + 1] !== undefined && !rest[i + 1].startsWith('--');
      flags[key] = hasValue ? rest[i + 1] : true;
      if (hasValue) i++;
    }
  }
  return flags;
}

function fail(err) {
  console.error(`Error: ${err.message || err}`);
  process.exit(1);
}

function main() {
  switch (cmd) {
    case 'enqueue': {
      const jsonArg = args[1];
      if (!jsonArg) return fail(new Error('Usage: queuectl enqueue \'{"id":"job1","command":"..."}\''));
      let input;
      try {
        input = JSON.parse(jsonArg);
      } catch (e) {
        return fail(new Error('Invalid JSON: ' + e.message));
      }
      try {
        const job = jobManager.enqueueJob(input);
        console.log(`Enqueued job "${job.id}" (state: ${job.state}, max_retries: ${job.max_retries})`);
      } catch (e) {
        fail(e);
      }
      break;
    }

    case 'worker': {
      const sub = args[1];
      if (sub === 'start') {
        const flags = parseFlags(args.slice(2));
        const count = parseInt(flags.count || '1', 10);
        if (!Number.isInteger(count) || count < 1) return fail(new Error('--count must be a positive integer'));
        const fs = require('fs');
        const logDir = path.join(storage.DATA_DIR, 'logs');
        if (!fs.existsSync(logDir)) fs.mkdirSync(logDir, { recursive: true });
        for (let i = 0; i < count; i++) {
          const workerId = `worker-${Date.now()}-${i}-${Math.random().toString(36).slice(2, 6)}`;
          const logFile = path.join(logDir, `${workerId}.log`);
          const out = fs.openSync(logFile, 'a');
          const child = spawn(process.execPath, [path.join(__dirname, '..', 'src', 'worker.js')], {
            env: { ...process.env, QUEUECTL_WORKER_ID: workerId },
            detached: true,
            stdio: ['ignore', out, out],
          });
          child.unref();
          console.log(`Started worker "${workerId}" (pid ${child.pid}) - logs: ${logFile}`);
        }
      } else if (sub === 'stop') {
        const state = storage.readState();
        const running = state.workers.filter((w) => w.status === 'running');
        if (running.length === 0) {
          console.log('No running workers.');
          break;
        }
        running.forEach((w) => {
          try {
            process.kill(w.pid, 'SIGTERM');
            console.log(`Sent SIGTERM to worker "${w.id}" (pid ${w.pid})`);
          } catch (e) {
            console.log(`Worker "${w.id}" (pid ${w.pid}) already stopped`);
          }
        });
        console.log('Workers will finish their current job before exiting.');
      } else {
        console.log('Usage: queuectl worker <start|stop> [--count N]');
        process.exit(1);
      }
      break;
    }

    case 'status': {
      const s = jobManager.getStatus();
      console.log('Job counts:');
      Object.entries(s.counts).forEach(([k, v]) => console.log(`  ${k.padEnd(12)} ${v}`));
      console.log(`Total jobs:     ${s.total}`);
      console.log(`Active workers: ${s.active_workers}`);
      if (s.workers.length) {
        console.log('Workers:');
        s.workers.forEach((w) => console.log(`  ${w.id} (pid ${w.pid}) - ${w.status}`));
      }
      console.log(`Config: ${JSON.stringify(s.config)}`);
      break;
    }

    case 'list': {
      const flags = parseFlags(args.slice(1));
      try {
        const jobs = jobManager.listJobs(flags.state);
        if (jobs.length === 0) {
          console.log('No jobs found.');
          break;
        }
        jobs.forEach((j) => {
          console.log(`${j.id}\t${j.state}\tattempts=${j.attempts}/${j.max_retries}\t"${j.command}"`);
        });
      } catch (e) {
        fail(e);
      }
      break;
    }

    case 'dlq': {
      const sub = args[1];
      if (sub === 'list') {
        const jobs = jobManager.listDLQ();
        if (jobs.length === 0) {
          console.log('DLQ is empty.');
          break;
        }
        jobs.forEach((j) => console.log(`${j.id}\tattempts=${j.attempts}\tlast_error=${j.last_error}`));
      } else if (sub === 'retry') {
        const id = args[2];
        if (!id) return fail(new Error('Usage: queuectl dlq retry <job_id>'));
        try {
          jobManager.retryFromDLQ(id);
          console.log(`Job "${id}" re-queued from DLQ.`);
        } catch (e) {
          fail(e);
        }
      } else {
        console.log('Usage: queuectl dlq <list|retry> [job_id]');
        process.exit(1);
      }
      break;
    }

    case 'config': {
      const sub = args[1];
      if (sub === 'set') {
        const keyRaw = args[2];
        const value = args[3];
        if (!keyRaw || value === undefined) return fail(new Error('Usage: queuectl config set <key> <value>'));
        const keyMap = { 'max-retries': 'max_retries', 'backoff-base': 'backoff_base' };
        const key = keyMap[keyRaw] || keyRaw;
        const parsedValue = isNaN(Number(value)) ? value : Number(value);
        jobManager.setConfig(key, parsedValue);
        console.log(`Config updated: ${key} = ${parsedValue}`);
      } else if (sub === 'get' || !sub) {
        console.log(JSON.stringify(jobManager.getConfig(), null, 2));
      } else {
        console.log('Usage: queuectl config <set|get> [key] [value]');
        process.exit(1);
      }
      break;
    }

    case 'help':
    case undefined:
    case '--help':
    case '-h':
      printHelp();
      break;

    default:
      console.log(`Unknown command: ${cmd}`);
      printHelp();
      process.exit(1);
  }
}

main();
