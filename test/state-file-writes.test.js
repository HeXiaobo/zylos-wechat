#!/usr/bin/env node
/**
 * State files are written only when their content changes, and the accounts
 * directory is cleared of abandoned atomic-write temporary files at startup.
 *
 * Uses the real Poller, AccountStore and RuntimeHealthStore on a temporary data
 * directory. A completed atomic write renames a new file into place, so a
 * changed inode means the file was rewritten.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  symlinkSync,
  utimesSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { AccountStore } from '../src/lib/account-store.js';
import { Poller } from '../src/lib/poller.js';
import { RuntimeHealthStore } from '../src/lib/runtime-health-store.js';

const ID = 'acct-im-bot';

function makeDataDir(t) {
  const dir = mkdtempSync(join(tmpdir(), 'wechat-state-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  return dir;
}

function inode(path) {
  return existsSync(path) ? statSync(path).ino : null;
}

function abortError() {
  const err = new Error('aborted');
  err.name = 'AbortError';
  return err;
}

/**
 * Runs a Poller against scripted getUpdates steps. Each step may run `before`
 * (to inspect or change the disk) and then returns `response` or throws.
 * After the last step the poller is stopped. Returns the cursor passed to each
 * call and the errors the poller emitted.
 */
async function runPoller(store, steps) {
  const sentBufs = [];
  const errors = [];
  let index = 0;
  let poller;
  const client = {
    async getUpdates(buf) {
      sentBufs.push(buf);
      const step = steps[index++];
      if (!step) {
        poller.stop();
        throw abortError();
      }
      step.before?.();
      return step.response;
    },
  };
  poller = new Poller({ client, accountStore: store, accountId: ID, normalizedId: ID });
  poller.on('error', (err) => errors.push(err));
  await poller.start();
  return { sentBufs, errors };
}

test('sync.json is not rewritten while getUpdates returns the same cursor', async (t) => {
  const dataDir = makeDataDir(t);
  const store = new AccountStore(dataDir);
  await store.init();
  await store.saveSyncState(ID, 'B1');
  const syncPath = join(dataDir, 'accounts', `${ID}.sync.json`);
  const initial = inode(syncPath);

  const seen = [];
  const look = () => seen.push(inode(syncPath));
  const { sentBufs, errors } = await runPoller(store, [
    { response: { get_updates_buf: 'B1', msgs: [] } },
    { before: look, response: { get_updates_buf: 'B1' } },
    { before: look, response: {} },
    { before: look, response: { get_updates_buf: 'B2', msgs: [] } },
    { before: look, response: { get_updates_buf: 'B2' } },
  ]);
  look();

  assert.deepEqual(errors, []);
  assert.deepEqual(sentBufs, ['B1', 'B1', 'B1', 'B1', 'B2', 'B2']);
  // Three unchanged cursors (and one response without a cursor): no write.
  assert.equal(seen[0], initial);
  assert.equal(seen[1], initial);
  assert.equal(seen[2], initial);
  // The changed cursor is written once, then left alone.
  assert.notEqual(seen[3], initial);
  assert.equal(seen[4], seen[3]);
  assert.deepEqual(JSON.parse(readFileSync(syncPath, 'utf8')), { get_updates_buf: 'B2' });
});

test('a cursor whose write failed is written by a later poll that returns it again', async (t) => {
  const dataDir = makeDataDir(t);
  const store = new AccountStore(dataDir);
  await store.init();
  await store.saveSyncState(ID, 'B1');
  const syncPath = join(dataDir, 'accounts', `${ID}.sync.json`);

  const { errors } = await runPoller(store, [
    {
      // A directory at the target path makes the rename fail.
      before: () => {
        rmSync(syncPath);
        mkdirSync(syncPath);
      },
      response: { get_updates_buf: 'B2' },
    },
    { before: () => rmSync(syncPath, { recursive: true }), response: { get_updates_buf: 'B2' } },
  ]);

  assert.equal(errors.length, 1);
  assert.deepEqual(JSON.parse(readFileSync(syncPath, 'utf8')), { get_updates_buf: 'B2' });
});

test('runtime.json is written only when a field other than updatedAt changes', async (t) => {
  const dataDir = makeDataDir(t);
  const health = new RuntimeHealthStore(dataDir);
  const runtimePath = join(dataDir, 'accounts', `${ID}.runtime.json`);
  // The periodic replyability refresh sends this patch every minute.
  const refresh = { accountId: 'raw-id', replyability: 'needs_user_message', lastContextAt: null };

  const first = await health.upsert(ID, refresh);
  const firstInode = inode(runtimePath);
  assert.notEqual(firstInode, null);

  await new Promise((resolve) => setTimeout(resolve, 5));
  const second = await health.upsert(ID, refresh);
  assert.equal(inode(runtimePath), firstInode);
  assert.equal(second.updatedAt, first.updatedAt);
  assert.deepEqual(await health.load(ID), first);

  await new Promise((resolve) => setTimeout(resolve, 5));
  const changed = await health.upsert(ID, {
    ...refresh,
    replyability: 'replyable',
    lastContextAt: '2026-11-15T00:00:00.000Z',
  });
  assert.notEqual(inode(runtimePath), firstInode);
  assert.notEqual(changed.updatedAt, first.updatedAt);
  assert.equal((await health.load(ID)).replyability, 'replyable');
});

test('runtime.json without the current fields is rewritten even if the patch adds nothing', async (t) => {
  const dataDir = makeDataDir(t);
  const health = new RuntimeHealthStore(dataDir);
  const runtimePath = join(dataDir, 'accounts', `${ID}.runtime.json`);
  mkdirSync(join(dataDir, 'accounts'));
  writeFileSync(runtimePath, `${JSON.stringify({ loginHealth: 'healthy', updatedAt: 'x' })}\n`);
  const before = inode(runtimePath);

  const next = await health.upsert(ID, {});
  assert.notEqual(inode(runtimePath), before);
  assert.equal(next.normalizedAccountId, ID);
  assert.equal(next.loginHealth, 'healthy');
});

test('startup cleanup removes only abandoned temporary files in this store\'s format', async (t) => {
  const dataDir = makeDataDir(t);
  const accountsDir = join(dataDir, 'accounts');
  mkdirSync(accountsDir);
  const exited = spawnSync(process.execPath, ['-e', '']);
  const deadPid = exited.pid;
  const livePid = process.ppid; // running, and not this process
  const ts = Date.now();
  const hourAgo = new Date(Date.now() - 60 * 60_000);

  const removable = [
    `${ID}.sync.json.${deadPid}.${ts}.tmp`, // writer exited (0-byte, as left mid-write)
    `${ID}.runtime.json.${deadPid}.${ts}.tmp`,
    `${ID}.json.${livePid}.${ts - 3_600_000}.tmp`, // pid now running, file an hour old
  ];
  const kept = [
    `${ID}.json`,
    `${ID}.sync.json`,
    `${ID}.runtime.json`,
    `${ID}.sync.json.${livePid}.${ts}.tmp`, // a write that may still be in progress
    `${ID}.sync.json.${process.pid}.${ts}.tmp`, // this process's own fresh write
    `${ID}.runtime.json.1.${ts}.tmp`, // pid 1 runs (as another user unless the test runs as root)
    `${ID}.sync.json.tmp`,
    `${ID}.sync.json.${deadPid}.tmp`,
    `${ID}.txt.${deadPid}.${ts}.tmp`,
    'notes.tmp',
  ];
  for (const name of [...removable, ...kept]) {
    writeFileSync(join(accountsDir, name), name.endsWith('.tmp') && name.includes(String(deadPid)) ? '' : '{}\n');
  }
  utimesSync(join(accountsDir, removable[2]), hourAgo, hourAgo);

  // Matching names that are not regular files.
  const dirName = `other.json.${deadPid}.${ts}.tmp`;
  mkdirSync(join(accountsDir, dirName));
  const target = join(dataDir, 'keep-target.json');
  writeFileSync(target, '{}\n');
  const linkName = `link.json.${deadPid}.${ts}.tmp`;
  symlinkSync(target, join(accountsDir, linkName));
  // Same format outside the accounts directory.
  const outside = `accounts.json.${deadPid}.${ts}.tmp`;
  writeFileSync(join(dataDir, outside), '');

  const store = new AccountStore(dataDir);
  await store.init();
  const removed = await store.removeStaleTempFiles();

  assert.deepEqual([...removed].sort(), [...removable].sort());
  assert.deepEqual(
    readdirSync(accountsDir).sort(),
    [...kept, dirName, linkName].sort(),
  );
  assert.ok(existsSync(target));
  assert.ok(existsSync(join(dataDir, outside)));
});

test('startup cleanup with no accounts directory removes nothing', async (t) => {
  const dataDir = makeDataDir(t);
  const store = new AccountStore(dataDir);
  assert.deepEqual(await store.removeStaleTempFiles(), []);
});
