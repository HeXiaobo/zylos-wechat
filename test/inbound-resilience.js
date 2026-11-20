#!/usr/bin/env node
/**
 * Inbound delivery must not depend on the runtime health file.
 *
 * A full disk (ENOSPC) makes the runtime.json write fail. Before this fix the
 * write ran first in handleMessages(), so the throw escaped the handler: the
 * message never reached C4, nothing was logged, and nothing was marked seen.
 *
 * Uses the real RuntimeHealthStore on a temporary data directory so the disk
 * error goes through writeJsonAtomic exactly as it does in production.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { createInboundHandler } from '../src/lib/inbound-handler.js';
import { RuntimeHealthStore } from '../src/lib/runtime-health-store.js';

const ACCOUNT_ID = 'acct-raw@im.bot';
const NORMALIZED_ID = 'acct-normalized';
const USER_ID = 'f7c1@im.wechat';

function makeDataDir(t) {
  const dir = mkdtempSync(join(tmpdir(), 'wechat-inbound-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  return dir;
}

function enospcError() {
  const err = new Error('ENOSPC: no space left on device, write');
  err.code = 'ENOSPC';
  return err;
}

/** RuntimeHealthStore whose writes always fail with ENOSPC. */
function fullDiskHealthStore(dataDir) {
  const store = new RuntimeHealthStore(dataDir);
  const upsert = store.upsert.bind(store);
  store.upsert = async () => {
    throw enospcError();
  };
  store.upsertReal = upsert;
  return store;
}

function captureLogger() {
  const lines = { debug: [], info: [], warn: [], error: [] };
  const record = (level) => (...args) => lines[level].push(args.join(' '));
  return {
    lines,
    logger: { debug: record('debug'), info: record('info'), warn: record('warn'), error: record('error') },
  };
}

function makeHandler({ runtimeHealth, logger, sent, allow = () => true }) {
  return createInboundHandler({
    runtimeHealth,
    getLogger: () => logger,
    getConfig: () => ({ c4ReceiveScript: '/tmp/c4-receive.js' }),
    sendToC4: async (payload) => {
      sent.push(payload);
      return '{"ok":true}';
    },
    getClient: () => null,
    getTypingManager: () => undefined,
    isDmAllowed: allow,
    mediaDir: '/tmp/does-not-matter',
    latestContextState: () => ({ replyability: 'replyable', lastContextAt: '2026-11-20T00:00:00.000Z' }),
  });
}

function textMessage(id, text = 'hello') {
  return {
    message_id: id,
    message_type: 1,
    message_state: 2,
    from_user_id: USER_ID,
    context_token: `ctx-${id}`,
    item_list: [{ type: 1, text_item: { text } }],
  };
}

test('a message still reaches C4 when the runtime.json write fails with ENOSPC', async (t) => {
  const dataDir = makeDataDir(t);
  const runtimeHealth = fullDiskHealthStore(dataDir);
  const { lines, logger } = captureLogger();
  const sent = [];
  const handler = makeHandler({ runtimeHealth, logger, sent });

  await handler.handleMessages([textMessage('m-1', 'disk full')], ACCOUNT_ID, NORMALIZED_ID);

  assert.equal(sent.length, 1, 'the message must be delivered to C4');
  assert.equal(sent[0].channel, 'wechat');
  assert.equal(sent[0].endpoint, `${NORMALIZED_ID}|to:${USER_ID}`);
  assert.match(sent[0].content, /disk full/);
  // The failure stays visible, once, with the error name.
  assert.equal(lines.warn.length, 1);
  assert.match(lines.warn[0], /ENOSPC/);
  assert.match(lines.warn[0], /runtime health write skipped/);
  // Delivery is not an error.
  assert.deepEqual(lines.error, []);
});

test('delivery is ordered before the state write', async (t) => {
  const dataDir = makeDataDir(t);
  const order = [];
  const runtimeHealth = new RuntimeHealthStore(dataDir);
  const realUpsert = runtimeHealth.upsert.bind(runtimeHealth);
  runtimeHealth.upsert = async (id, patch) => {
    order.push('upsert');
    return realUpsert(id, patch);
  };
  const { logger } = captureLogger();
  const sent = [];
  const handler = createInboundHandler({
    runtimeHealth,
    getLogger: () => logger,
    getConfig: () => ({ c4ReceiveScript: '/tmp/c4-receive.js' }),
    sendToC4: async (payload) => {
      order.push('sendToC4');
      sent.push(payload);
      return '{"ok":true}';
    },
    getClient: () => null,
    getTypingManager: () => undefined,
    isDmAllowed: () => true,
    mediaDir: '/tmp/does-not-matter',
    latestContextState: () => ({ replyability: 'replyable', lastContextAt: null }),
  });

  await handler.handleMessages([textMessage('m-order')], ACCOUNT_ID, NORMALIZED_ID);

  assert.deepEqual(order, ['sendToC4', 'upsert']);
  assert.equal(sent.length, 1);
  assert.equal((await runtimeHealth.load(NORMALIZED_ID)).lastInboundAt !== null, true);
});

test('one failed state write does not stop the rest of the batch', async (t) => {
  const dataDir = makeDataDir(t);
  const runtimeHealth = fullDiskHealthStore(dataDir);
  const { lines, logger } = captureLogger();
  const sent = [];
  const handler = makeHandler({ runtimeHealth, logger, sent });

  await handler.handleMessages(
    [textMessage('m-a', 'first'), textMessage('m-b', 'second'), textMessage('m-c', 'third')],
    ACCOUNT_ID,
    NORMALIZED_ID
  );

  assert.equal(sent.length, 3, 'every message in the batch is delivered');
  assert.deepEqual(sent.map((p) => p.content), [
    `[WeChat DM] ${USER_ID} said: first`,
    `[WeChat DM] ${USER_ID} said: second`,
    `[WeChat DM] ${USER_ID} said: third`,
  ]);
  // One warning per failed write, no error lines.
  assert.equal(lines.warn.length, 3);
  assert.deepEqual(lines.error, []);
});

test('messages are still deduplicated across batches when the state write fails', async (t) => {
  const dataDir = makeDataDir(t);
  const runtimeHealth = fullDiskHealthStore(dataDir);
  const { logger } = captureLogger();
  const sent = [];
  const handler = makeHandler({ runtimeHealth, logger, sent });
  const msg = textMessage('m-dup');

  await handler.handleMessages([msg], ACCOUNT_ID, NORMALIZED_ID);
  await handler.handleMessages([msg], ACCOUNT_ID, NORMALIZED_ID);

  assert.equal(sent.length, 1);
});

test('a C4 failure is logged and still does not throw when the state write also fails', async (t) => {
  const dataDir = makeDataDir(t);
  const runtimeHealth = fullDiskHealthStore(dataDir);
  const { lines, logger } = captureLogger();
  const handler = createInboundHandler({
    runtimeHealth,
    getLogger: () => logger,
    getConfig: () => ({ c4ReceiveScript: '/tmp/c4-receive.js' }),
    sendToC4: async () => {
      throw new Error('c4 receive failed');
    },
    getClient: () => null,
    getTypingManager: () => undefined,
    isDmAllowed: () => true,
    mediaDir: '/tmp/does-not-matter',
    latestContextState: () => ({ replyability: 'replyable', lastContextAt: null }),
  });

  await handler.handleMessages([textMessage('m-c4-fail')], ACCOUNT_ID, NORMALIZED_ID);

  assert.equal(lines.error.length, 1);
  assert.match(lines.error[0], /C4 dispatch failed/);
  assert.equal(lines.warn.length, 1);
  assert.match(lines.warn[0], /ENOSPC/);
});

test('syncAccountReplyability keeps a failed write out of the caller', async (t) => {
  const dataDir = makeDataDir(t);
  const runtimeHealth = new RuntimeHealthStore(dataDir);
  await runtimeHealth.upsertReal?.(NORMALIZED_ID, { accountId: ACCOUNT_ID });
  await runtimeHealth.upsert(NORMALIZED_ID, { accountId: ACCOUNT_ID });

  const { lines, logger } = captureLogger();
  const handler = makeHandler({ runtimeHealth, logger, sent: [] });
  runtimeHealth.upsert = async () => {
    throw enospcError();
  };

  const result = await handler.syncAccountReplyability(NORMALIZED_ID, ACCOUNT_ID);

  assert.equal(result, null);
  assert.equal(lines.warn.length, 1);
  assert.match(lines.warn[0], /ENOSPC/);
  assert.match(lines.warn[0], new RegExp(NORMALIZED_ID));
});

test('syncAccountReplyability returns null without touching disk when no runtime file exists', async (t) => {
  const dataDir = makeDataDir(t);
  const runtimeHealth = new RuntimeHealthStore(dataDir);
  const { lines, logger } = captureLogger();
  const handler = makeHandler({ runtimeHealth, logger, sent: [] });

  assert.equal(await handler.syncAccountReplyability('missing-account', ACCOUNT_ID), null);
  assert.deepEqual(lines.warn, []);
  assert.deepEqual(readdirSync(dataDir), []);
});

test('a real filesystem fault on the state directory does not block delivery', async (t) => {
  const dataDir = makeDataDir(t);
  // Not a stub: a regular file where `accounts/` must be makes every write to
  // runtime.json fail inside writeJsonAtomic for real.
  writeFileSync(join(dataDir, 'accounts'), 'blocker\n');
  const runtimeHealth = new RuntimeHealthStore(dataDir);
  const { lines, logger } = captureLogger();
  const sent = [];
  const handler = makeHandler({ runtimeHealth, logger, sent });

  await handler.handleMessages(
    [textMessage('f-1', 'first'), textMessage('f-2', 'second')],
    ACCOUNT_ID,
    NORMALIZED_ID
  );

  assert.equal(sent.length, 2);
  assert.equal(sent[0].content.includes('first'), true);
  assert.equal(lines.warn.length, 2);
  assert.match(lines.warn[0], /runtime health write skipped for acct-normalized/);
  assert.match(lines.warn[0], /Error /);
  assert.deepEqual(lines.error, []);
});

test('a non-allowlisted sender is dropped before delivery but the state write failure is contained', async (t) => {
  const dataDir = makeDataDir(t);
  const runtimeHealth = fullDiskHealthStore(dataDir);
  const { lines, logger } = captureLogger();
  const sent = [];
  const handler = makeHandler({ runtimeHealth, logger, sent, allow: () => false });

  await handler.handleMessages([textMessage('m-blocked')], ACCOUNT_ID, NORMALIZED_ID);

  assert.equal(sent.length, 0, 'allowlist still blocks delivery');
  assert.equal(lines.warn.length, 1);
  assert.match(lines.warn[0], /ENOSPC/);
});
