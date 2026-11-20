/**
 * Inbound message handling.
 *
 * Delivery to C4 comes first. The runtime health file is bookkeeping: a failed
 * state write (ENOSPC on a full disk, permissions, a read-only volume) must
 * never stop an inbound message from reaching C4 and must never abort the rest
 * of the batch. Each failed write is reported as a single warning line that
 * carries the error name, so the failure stays visible without a stack trace
 * per message.
 */

import {
  downloadInboundMedia,
  extractTextBody,
  formatInboundContent,
  pickInboundMediaItem,
} from './inbound-media.js';

const DEDUPE_MAX = 10_000;

/** `Error ENOSPC` for an fs error; `Error` when there is no code. */
export function describeError(error) {
  const name = error?.name || 'Error';
  const code = typeof error?.code === 'string' ? error.code : null;
  if (!code || code === name) return name;
  return `${name} ${code}`;
}

/**
 * Run `runtimeHealth.upsert` and swallow write failures.
 *
 * `buildPatch` runs inside the same guard so that computing the patch can never
 * break inbound delivery either. Returns the written record, or null when the
 * write failed.
 */
export async function safeRuntimeHealthWrite(runtimeHealth, logger, normalizedId, buildPatch) {
  try {
    return await runtimeHealth.upsert(normalizedId, buildPatch());
  } catch (error) {
    logger?.warn?.(
      `runtime health write skipped for ${normalizedId} (${describeError(error)}): ${error?.message}`
    );
    return null;
  }
}

/**
 * @param {object} deps
 * @param {object} deps.runtimeHealth - RuntimeHealthStore (or compatible)
 * @param {() => object} deps.getLogger
 * @param {() => object} deps.getConfig
 * @param {(payload: object) => Promise<string>} deps.sendToC4
 * @param {(normalizedId: string) => object | null} deps.getClient
 * @param {(accountId: string) => object | undefined} deps.getTypingManager
 * @param {(userId: string) => boolean} deps.isDmAllowed
 * @param {string} deps.mediaDir
 * @param {(normalizedId: string, rawAccountId: string) => object} deps.latestContextState
 */
export function createInboundHandler(deps) {
  const {
    runtimeHealth,
    getLogger,
    getConfig,
    sendToC4,
    getClient,
    getTypingManager,
    isDmAllowed,
    mediaDir,
    latestContextState,
  } = deps;

  const seenMessages = new Set();

  function markSeen(key) {
    seenMessages.add(key);
    if (seenMessages.size > DEDUPE_MAX) {
      const recent = [...seenMessages].slice(-Math.floor(DEDUPE_MAX * 0.9));
      seenMessages.clear();
      for (const value of recent) {
        seenMessages.add(value);
      }
    }
  }

  const isSeen = (key) => seenMessages.has(key);

  function writeHealth(normalizedId, buildPatch) {
    return safeRuntimeHealthWrite(runtimeHealth, getLogger(), normalizedId, buildPatch);
  }

  async function deliver(msg, accountId, normalizedId, fromUserId) {
    const logger = getLogger();
    const client = getClient(normalizedId);
    const mediaItem = pickInboundMediaItem(msg.item_list);
    const mediaResult = mediaItem && client
      ? await downloadInboundMedia(client, mediaItem, {
          mediaDir,
          logger,
          label: 'inbound',
        })
      : {};

    const text = extractTextBody(msg.item_list);
    const endpoint = fromUserId ? `${normalizedId}|to:${fromUserId}` : normalizedId;
    const content = mediaItem || text
      ? formatInboundContent(msg, mediaResult)
      : `[WeChat DM] ${fromUserId} said: ${text}`;

    const typingMgr = getTypingManager(accountId);
    if (typingMgr && fromUserId) {
      typingMgr.startTyping(fromUserId, msg.context_token).catch(() => {});
    }

    try {
      await sendToC4({
        scriptPath: getConfig().c4ReceiveScript,
        channel: 'wechat',
        endpoint,
        content,
        logger,
      });
    } catch (err) {
      logger.error('C4 dispatch failed:', err.message);
      if (typingMgr && fromUserId) {
        typingMgr.stopTyping(fromUserId).catch(() => {});
      }
    }
  }

  function recordInbound(normalizedId, accountId, receivedAt) {
    return writeHealth(normalizedId, () => ({
      accountId,
      loginHealth: 'healthy',
      lastPollAt: receivedAt,
      lastPollErrorCode: null,
      lastPollErrorMessage: null,
      lastInboundAt: receivedAt,
      ...latestContextState(normalizedId, accountId),
    }));
  }

  async function handleMessages(msgs, accountId, normalizedId) {
    const logger = getLogger();

    for (const msg of msgs) {
      if (msg.message_type === 2) continue;
      if (msg.message_state !== undefined && msg.message_state !== 2) continue;

      const msgId = String(msg.message_id || msg.seq || '');
      if (!msgId) continue;

      const dedupeKey = `${accountId}:${msgId}`;
      if (isSeen(dedupeKey)) continue;

      const fromUserId = msg.from_user_id || '';
      const receivedAt = new Date().toISOString();

      if (msg.context_token && fromUserId) {
        deps.setContextToken?.(normalizedId, fromUserId, msg.context_token);
      }

      if (!isDmAllowed(fromUserId)) {
        logger.debug('drop from non-allowlisted user:', fromUserId);
        await recordInbound(normalizedId, accountId, receivedAt);
        markSeen(dedupeKey);
        continue;
      }

      // Deliver before touching the state file: a full disk then costs the
      // health record, never the message.
      await deliver(msg, accountId, normalizedId, fromUserId);
      await recordInbound(normalizedId, accountId, receivedAt);
      markSeen(dedupeKey);
    }
  }

  async function syncAccountReplyability(normalizedId, rawAccountId) {
    const runtime = await runtimeHealth.load(normalizedId);
    if (!runtime) return null;

    return writeHealth(normalizedId, () => {
      const contextState = latestContextState(normalizedId, rawAccountId);
      return {
        accountId: rawAccountId,
        replyability: contextState.replyability,
        lastContextAt: contextState.lastContextAt,
      };
    });
  }

  return { handleMessages, syncAccountReplyability, markSeen, isSeen };
}
