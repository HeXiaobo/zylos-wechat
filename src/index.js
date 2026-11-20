#!/usr/bin/env node
import { join } from 'node:path';
import { ensureDirs, getConfig, paths, stopWatching, watchConfig } from './lib/config.js';
import { createLogger } from './lib/logger.js';
import { sendToC4 } from './lib/bridge.js';
import { AccountManager } from './lib/account-manager.js';
import { ContextTokenStore } from './lib/context-tokens.js';
import { TypingManager } from './lib/typing.js';
import { WeChatApiClient } from './lib/api-client.js';
import { RuntimeHealthStore } from './lib/runtime-health-store.js';
import { LoginSessionStore } from './lib/login-session-store.js';
import { AdminServer } from './lib/admin-server.js';
import { createInboundHandler, describeError, safeRuntimeHealthWrite } from './lib/inbound-handler.js';

let runtimeConfig = getConfig();
let logger = createLogger(runtimeConfig.logLevel);

if (!runtimeConfig.enabled) {
  logger.info('component disabled by config');
  process.exit(0);
}

ensureDirs();

const dataDir = paths.dataDir;
const manager = new AccountManager(dataDir, {
  cdnBaseUrl: runtimeConfig.wechat.cdnBaseUrl,
  channelVersion: runtimeConfig.wechat.channelVersion,
  appId: runtimeConfig.wechat.appId,
  botAgent: runtimeConfig.wechat.botAgent,
  logger,
});
const contextTokens = new ContextTokenStore({
  persistPath: join(dataDir, 'context-tokens.json'),
});
const runtimeHealth = new RuntimeHealthStore(dataDir);
const loginSessions = new LoginSessionStore({
  dataDir,
  logger,
  qrClientFactory: () =>
    new WeChatApiClient({
      baseUrl: runtimeConfig.wechat.apiBase,
      cdnBaseUrl: runtimeConfig.wechat.cdnBaseUrl,
      channelVersion: runtimeConfig.wechat.channelVersion,
      appId: runtimeConfig.wechat.appId,
      botAgent: runtimeConfig.wechat.botAgent,
      logger,
    }),
});
const typingManagers = new Map(); // raw accountId -> TypingManager

const ACCOUNT_RECONCILE_INTERVAL_MS = 10_000;
const REPLYABILITY_REFRESH_INTERVAL_MS = 60_000;
let shuttingDown = false;
let reconcileTimer = null;
let replyabilityTimer = null;
let adminServer = null;

function isDmAllowed(userId) {
  if (runtimeConfig.dmPolicy !== 'allowlist') return true;
  return runtimeConfig.dmAllowFrom.includes(userId);
}

function latestContextState(normalizedId, rawAccountId) {
  const latest = contextTokens.latestTimestampForAccount(normalizedId, [rawAccountId]);
  if (!latest) {
    return {
      replyability: 'needs_user_message',
      lastContextAt: null,
    };
  }
  return {
    replyability: 'replyable',
    lastContextAt: new Date(latest).toISOString(),
  };
}

const inboundHandler = createInboundHandler({
  runtimeHealth,
  getLogger: () => logger,
  getConfig: () => runtimeConfig,
  sendToC4,
  getClient: (normalizedId) => manager.getClient(normalizedId),
  getTypingManager: (accountId) => typingManagers.get(accountId),
  isDmAllowed,
  mediaDir: paths.mediaDir,
  latestContextState,
  setContextToken: (normalizedId, fromUserId, token) =>
    contextTokens.set(normalizedId, fromUserId, token),
});

const { handleMessages, syncAccountReplyability } = inboundHandler;

async function main() {
  logger.info('zylos-wechat starting...');
  logger.info('data dir:', dataDir);

  watchConfig((nextConfig) => {
    runtimeConfig = nextConfig;
    logger = createLogger(runtimeConfig.logLevel);
    logger.info('config reloaded');

    if (!runtimeConfig.enabled) {
      logger.info('component disabled in config, shutting down');
      shutdown('CONFIG_DISABLED');
    }
  });

  await manager.init();
  const removedTempFiles = await manager.store.removeStaleTempFiles();
  if (removedTempFiles.length > 0) {
    logger.info(`removed ${removedTempFiles.length} stale temporary file(s) from accounts/`);
  }
  await loginSessions.init();

  manager.on('error', (err, acctId, normalizedId) => {
    logger.error(`[${acctId}] error:`, err.message);
    if (normalizedId) {
      void safeRuntimeHealthWrite(runtimeHealth, logger, normalizedId, () => ({
        accountId: acctId,
        loginHealth: 'degraded',
        lastPollAt: new Date().toISOString(),
        lastPollErrorCode: 'WECHAT_POLL_ERROR',
        lastPollErrorMessage: err.message,
        ...latestContextState(normalizedId, acctId),
      }));
    }
  });

  manager.on('session-expired', (acctId, normalizedId) => {
    logger.warn(`[${acctId}] session expired - paused for 60 minutes`);
    if (normalizedId) {
      void safeRuntimeHealthWrite(runtimeHealth, logger, normalizedId, () => ({
        accountId: acctId,
        loginHealth: 'reauth_required',
        lastPollAt: new Date().toISOString(),
        lastPollErrorCode: 'WECHAT_SESSION_EXPIRED',
        lastPollErrorMessage: 'Session expired and requires QR re-auth',
        replyability: 'unknown',
      }));
    }
  });

  manager.on('connected', (acctId, normalizedId) => {
    logger.info(`[${acctId}] polling started`);
    const client = manager.getClient(normalizedId);
    if (client) {
      typingManagers.set(acctId, new TypingManager(client));
      client.notifyStart().catch((err) => {
        logger.debug(`[${acctId}] notifyStart failed:`, err.message);
      });
    }
    void manager.store.loadCredentials(normalizedId).then((creds) => {
      return safeRuntimeHealthWrite(runtimeHealth, logger, normalizedId, () => ({
        accountId: acctId,
        userId: creds?.userId || null,
        savedAt: creds?.savedAt || null,
        loginHealth: 'healthy',
        lastPollAt: new Date().toISOString(),
        lastPollErrorCode: null,
        lastPollErrorMessage: null,
        ...latestContextState(normalizedId, acctId),
      }));
    }).catch((err) => {
      logger.warn(`[${acctId}] runtime health refresh failed (${describeError(err)}):`, err.message);
    });
  });

  manager.on('disconnected', (acctId, normalizedId) => {
    logger.info(`[${acctId}] polling stopped`);
    const typingMgr = typingManagers.get(acctId);
    if (typingMgr) {
      typingMgr.stopAll();
      typingManagers.delete(acctId);
    }
    if (normalizedId) {
      void safeRuntimeHealthWrite(runtimeHealth, logger, normalizedId, () => ({
        accountId: acctId,
        loginHealth: 'degraded',
        lastPollAt: new Date().toISOString(),
        ...latestContextState(normalizedId, acctId),
      }));
    }
  });

  await manager.startAll(handleMessages);

  reconcileTimer = setInterval(() => {
    if (shuttingDown) return;

    manager.reconcile(handleMessages).catch((err) => {
      logger.error('account reconcile failed:', err.message);
    });
  }, ACCOUNT_RECONCILE_INTERVAL_MS);
  reconcileTimer.unref?.();

  replyabilityTimer = setInterval(() => {
    if (shuttingDown) return;
    contextTokens.cleanup();
    for (const account of manager.listAccounts()) {
      void syncAccountReplyability(account.normalizedId, account.accountId);
    }
  }, REPLYABILITY_REFRESH_INTERVAL_MS);
  replyabilityTimer.unref?.();

  if (runtimeConfig.admin?.enabled !== false) {
    adminServer = new AdminServer({
      host: runtimeConfig.admin.host,
      port: runtimeConfig.admin.port,
      tokenPath: paths.adminTokenFile,
      logger,
      getConfig: () => runtimeConfig,
      accountManager: manager,
      accountStore: manager.store,
      contextTokens,
      loginSessions,
      runtimeHealth,
      reconcileAccounts: () => manager.reconcileCurrent(),
      typingManagers,
    });
    await adminServer.start();
  }

  const accounts = manager.listAccounts();
  if (accounts.length === 0) {
    logger.info('no accounts configured - waiting for QR login via admin CLI');
  } else {
    logger.info(`${accounts.length} account(s) active`);
  }
}

function shutdown(sig) {
  if (shuttingDown) return;
  shuttingDown = true;

  logger.info(`received ${sig}, shutting down...`);
  stopWatching();
  if (reconcileTimer) {
    clearInterval(reconcileTimer);
    reconcileTimer = null;
  }
  if (replyabilityTimer) {
    clearInterval(replyabilityTimer);
    replyabilityTimer = null;
  }
  contextTokens.flush();
  loginSessions.stop();
  void adminServer?.close().catch(() => {});

  for (const typingMgr of typingManagers.values()) {
    typingMgr.stopAll();
  }

  const notifyPromises = manager.listAccounts().map((acct) => {
    const client = manager.getClient(acct.normalizedId);
    return client?.notifyStop({ timeout: 3_000 }).catch((err) => {
      logger.debug(`[${acct.accountId}] notifyStop failed:`, err.message);
    });
  });

  Promise.allSettled(notifyPromises).then(() => {
    return manager.stopAll();
  }).then(() => {
    logger.info('all pollers stopped');
    process.exit(0);
  }).catch(() => {
    process.exit(1);
  });

  setTimeout(() => process.exit(0), 5_000);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

main().catch((err) => {
  logger.error('fatal:', err.message);
  process.exit(1);
});
