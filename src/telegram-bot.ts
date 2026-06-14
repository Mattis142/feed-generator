/**
 * telegram-bot.ts
 *
 * Enhanced Telegram bot for the BSKYALGO feed generator.
 * Supports toggling individual alert types via inline keyboard buttons,
 * without needing to push a code update.
 *
 * Alert types:
 *  - errors        : Server / pipeline errors (always-critical, hard to disable)
 *  - warnings      : Non-critical warnings (e.g. bad post records)
 *  - batch_start   : Batch pipeline started
 *  - batch_finish  : Batch pipeline finished (with summary stats)
 *  - batch_error   : An error occurred mid-batch for a specific user
 *  - feed_deploy   : Feed redeployed / published
 */

import fs from 'fs'
import path from 'path'
import dotenv from 'dotenv'
import { Database } from './db'

dotenv.config()

const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN
const CHAT_ID = process.env.TELEGRAM_CHAT_ID
const HOSTNAME = process.env.FEEDGEN_HOSTNAME || 'feed.mattis-kabella.com'

let db: Database | null = null

// ─── Persistent alert preferences ────────────────────────────────────────────

export type AlertType = 'errors' | 'warnings' | 'batch_start' | 'batch_finish' | 'batch_error' | 'feed_deploy'

const PREFS_FILE = path.join(process.cwd(), 'data', 'telegram_prefs.json')

const DEFAULT_PREFS: Record<AlertType, boolean> = {
    errors: true,
    warnings: false,
    batch_start: true,
    batch_finish: true,
    batch_error: true,
    feed_deploy: true,
}

const ALERT_LABELS: Record<AlertType, string> = {
    errors: '❌ Errors',
    warnings: '⚠️ Warnings',
    batch_start: '🚀 Batch Start',
    batch_finish: '✅ Batch Finish',
    batch_error: '🔥 Batch Errors',
    feed_deploy: '📡 Feed Deploy',
}

function loadPrefs(): Record<AlertType, boolean> {
    try {
        fs.mkdirSync(path.dirname(PREFS_FILE), { recursive: true })
        if (fs.existsSync(PREFS_FILE)) {
            const raw = fs.readFileSync(PREFS_FILE, 'utf-8')
            return { ...DEFAULT_PREFS, ...JSON.parse(raw) }
        }
    } catch (e) {
        // ignore
    }
    return { ...DEFAULT_PREFS }
}

function savePrefs(prefs: Record<AlertType, boolean>) {
    try {
        fs.mkdirSync(path.dirname(PREFS_FILE), { recursive: true })
        fs.writeFileSync(PREFS_FILE, JSON.stringify(prefs, null, 2))
    } catch (e) {
        console.error('[TelegramBot] Failed to save prefs:', e)
    }
}

let prefs = loadPrefs()

// ─── Telegram API helpers ─────────────────────────────────────────────────────

async function apiCall(method: string, body: object): Promise<any> {
    if (!BOT_TOKEN) return null
    try {
        const res = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/${method}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        })
        const data = await res.json() as any
        if (!data.ok) {
            console.error(`[TelegramBot] API error (${method}):`, data.description)
        }
        return data
    } catch (err) {
        console.error(`[TelegramBot] Network error (${method}):`, err)
        return null
    }
}

async function sendMessage(text: string, extra: object = {}): Promise<any> {
    if (!BOT_TOKEN || !CHAT_ID) return null
    return apiCall('sendMessage', {
        chat_id: CHAT_ID,
        text: text.slice(0, 4096),
        parse_mode: 'Markdown',
        ...extra,
    })
}

async function editMessage(messageId: number, text: string, extra: object = {}): Promise<any> {
    if (!BOT_TOKEN || !CHAT_ID) return null
    return apiCall('editMessageText', {
        chat_id: CHAT_ID,
        message_id: messageId,
        text: text.slice(0, 4096),
        parse_mode: 'Markdown',
        ...extra,
    })
}

async function answerCallbackQuery(callbackQueryId: string, text?: string): Promise<any> {
    return apiCall('answerCallbackQuery', {
        callback_query_id: callbackQueryId,
        text,
        show_alert: false,
    })
}

// ─── Inline keyboard builder ──────────────────────────────────────────────────

function buildSettingsKeyboard(): object {
    const alertTypes: AlertType[] = ['errors', 'warnings', 'batch_start', 'batch_finish', 'batch_error', 'feed_deploy']

    const buttons = alertTypes.map(type => [{
        text: `${prefs[type] ? '🟢' : '🔴'} ${ALERT_LABELS[type]}`,
        callback_data: `toggle:${type}`,
    }])

    // Add a refresh/close row
    buttons.push([
        { text: '🔄 Refresh', callback_data: 'settings:refresh' },
        { text: '❌ Close', callback_data: 'settings:close' },
    ])

    return { inline_keyboard: buttons }
}

function buildSettingsText(): string {
    const lines = (Object.keys(ALERT_LABELS) as AlertType[]).map(type =>
        `${prefs[type] ? '🟢' : '🔴'} ${ALERT_LABELS[type]}`
    )
    return `*⚙️ Alert Settings — ${HOSTNAME}*\n\nTap a button to toggle an alert type on or off. Changes take effect immediately.\n\n${lines.join('\n')}`
}

// ─── Public notification functions ────────────────────────────────────────────

export async function notify(type: AlertType, message: string): Promise<void> {
    prefs = loadPrefs() // Always load fresh from disk
    if (!prefs[type]) return
    await sendMessage(message)
}

export async function notifyError(context: string, err: any): Promise<void> {
    const errorMessage = err instanceof Error ? err.stack || err.message : String(err)
    const msg = `❌ *Error on ${HOSTNAME}*\n\n*Context:* ${context}\n\n\`\`\`\n${errorMessage.slice(0, 1500)}\n\`\`\``
    await notify('errors', msg)
}

export async function notifyWarning(context: string, detail: string): Promise<void> {
    const msg = `⚠️ *Warning on ${HOSTNAME}*\n\n*Context:* ${context}\n${detail.slice(0, 1000)}`
    await notify('warnings', msg)
}

export async function notifyBatchStart(userCount: number): Promise<void> {
    const msg = `🚀 *Batch pipeline started* — ${HOSTNAME}\n\nProcessing *${userCount}* user(s)\n_${new Date().toUTCString()}_`
    await notify('batch_start', msg)
}

export async function notifyBatchFinish(stats: {
    userCount: number
    totalCandidates: number
    durationMs: number
    errors: number
}): Promise<void> {
    const mins = Math.round(stats.durationMs / 60000)
    const msg = [
        `✅ *Batch pipeline finished* — ${HOSTNAME}`,
        '',
        `👥 Users processed: *${stats.userCount}*`,
        `📦 Total candidates stored: *${stats.totalCandidates}*`,
        `⏱ Duration: *${mins} min*`,
        stats.errors > 0 ? `⚠️ Errors: *${stats.errors}*` : `🎉 No errors`,
        '',
        `_${new Date().toUTCString()}_`,
    ].join('\n')
    await notify('batch_finish', msg)
}

export async function notifyBatchUserError(userDid: string, err: any): Promise<void> {
    const errorMessage = err instanceof Error ? err.message : String(err)
    const msg = `🔥 *Batch error for user* — ${HOSTNAME}\n\n\`${userDid.slice(0, 30)}...\`\n\n\`\`\`\n${errorMessage.slice(0, 800)}\n\`\`\``
    await notify('batch_error', msg)
}

export async function notifyDeploy(): Promise<void> {
    const msg = `📡 *Feed deployed* — ${HOSTNAME}\n\n_Feed generator record re-published to Bluesky_\n_${new Date().toUTCString()}_`
    await notify('feed_deploy', msg)
}

// ─── Handle resolution cache ──────────────────────────────────────────────────

const handleCache = new Map<string, string>()

async function resolveHandle(did: string): Promise<string> {
    if (handleCache.has(did)) return handleCache.get(did)!
    
    if (did === 'did:plc:c7m7glv2pfjwvgmtkt6kej5w') return 'mattis-kabella.com'
    if (did === process.env.FEEDGEN_PUBLISHER_DID) return process.env.BSKY_HANDLE || did.slice(-8)

    try {
        const controller = new AbortController()
        const timeoutId = setTimeout(() => controller.abort(), 3000)
        const res = await fetch(`https://plc.directory/${did}`, { signal: controller.signal })
        clearTimeout(timeoutId)

        if (res.ok) {
            const data = await res.json() as any
            if (data.alsoKnownAs && data.alsoKnownAs[0]) {
                const handle = data.alsoKnownAs[0].replace('at://', '')
                handleCache.set(did, handle)
                return handle
            }
        }
    } catch (err) {
        // ignore and fallback
    }
    
    const short = did.slice(-8)
    handleCache.set(did, short)
    return short
}

// ─── Command handlers ─────────────────────────────────────────────────────────

async function handleStart(): Promise<void> {
    const msg = [
        `👋 *BSKYALGO Feed Monitor* — ${HOSTNAME}`,
        '',
        'I keep you informed about your Bluesky feed generator.',
        '',
        '*Commands:*',
        '/status — current system status',
        '/batches — view semantic batch consumption & time left',
        '/settings — toggle alert types on/off',
        '/help — show this message',
    ].join('\n')
    await sendMessage(msg)
}

async function handleHelp(): Promise<void> {
    return handleStart()
}

async function handleStatus(): Promise<void> {
    const enabledAlerts = (Object.keys(prefs) as AlertType[])
        .filter(k => prefs[k])
        .map(k => ALERT_LABELS[k])
        .join(', ') || 'none'

    const msg = [
        `📊 *Status — ${HOSTNAME}*`,
        '',
        `🕐 Server time: \`${new Date().toUTCString()}\``,
        `✅ Bot is online and polling`,
        '',
        `*Active alerts:* ${enabledAlerts}`,
        '',
        `Use /batches or /monitor to view candidate batch consumption stats.`,
    ].join('\n')
    await sendMessage(msg)
}

async function handleSettings(): Promise<void> {
    await sendMessage(buildSettingsText(), {
        reply_markup: buildSettingsKeyboard(),
    })
}

async function handleBatches(): Promise<void> {
    if (!db) {
        await sendMessage('⚠️ Database connection is not available.')
        return
    }

    try {
        const nowMs = Date.now()
        const BATCH_TTL_HOURS = 12
        const cutoff = new Date(nowMs - BATCH_TTL_HOURS * 60 * 60 * 1000).toISOString()

        const activeUsers = await db
            .selectFrom('user_candidate_batch')
            .select('userDid')
            .distinct()
            .where('generatedAt', '>', cutoff)
            .execute()

        if (activeUsers.length === 0) {
            await sendMessage('📊 *Semantic Batch Monitor*\n\nNo active batches found in the last 12 hours.')
            return
        }

        const lines: string[] = ['📊 *Semantic Batch Monitor*\n']

        for (const user of activeUsers) {
            const userDid = user.userDid
            const handle = await resolveHandle(userDid)

            const batchRows = await db
                .selectFrom('user_candidate_batch')
                .selectAll()
                .where('userDid', '=', userDid)
                .where('generatedAt', '>', cutoff)
                .execute()

            if (batchRows.length === 0) continue

            const uniqueBatchRowsMap: Record<string, typeof batchRows[0]> = {}
            let latestGeneratedAt = new Date(0)
            for (const row of batchRows) {
                const genTime = new Date(row.generatedAt)
                if (genTime > latestGeneratedAt) {
                    latestGeneratedAt = genTime
                }
                if (!uniqueBatchRowsMap[row.uri] || row.generatedAt > uniqueBatchRowsMap[row.uri].generatedAt) {
                    uniqueBatchRowsMap[row.uri] = row
                }
            }

            const totalBatchCandidates = Object.keys(uniqueBatchRowsMap).length

            const fatigueLookback = new Date(nowMs - 7 * 24 * 60 * 60 * 1000).toISOString()
            const seenPosts = await db
                .selectFrom('user_seen_post')
                .select('uri')
                .where('userDid', '=', userDid)
                .where('seenAt', '>', fatigueLookback)
                .execute()

            const seenUris = new Set(seenPosts.map(sp => sp.uri))
            let totalServedFromBatches = 0
            for (const uri of Object.keys(uniqueBatchRowsMap)) {
                if (seenUris.has(uri)) {
                    totalServedFromBatches++
                }
            }

            const consumptionRatio = totalBatchCandidates > 0
                ? (totalServedFromBatches / totalBatchCandidates) * 100
                : 0

            const remaining = totalBatchCandidates - totalServedFromBatches

            const expiresAt = new Date(latestGeneratedAt.getTime() + BATCH_TTL_HOURS * 60 * 60 * 1000)
            const msLeft = expiresAt.getTime() - nowMs
            const minutesLeft = Math.max(0, Math.floor(msLeft / 60000))
            const hoursLeft = Math.floor(minutesLeft / 60)
            const remMin = minutesLeft % 60
            const timeLeftStr = minutesLeft <= 0 ? 'Expired' : `${hoursLeft}h ${remMin}m`

            const isRegenerating = consumptionRatio >= 50
            const statusEmoji = isRegenerating ? '🔄 REGEN TRIGGERED' : '✅ HEALTHY'

            lines.push([
                `👤 *${handle}*`,
                `├ 📦 Total candidates: *${totalBatchCandidates}*`,
                `├ 😋 Consumed: *${totalServedFromBatches}* (${consumptionRatio.toFixed(1)}%)`,
                `├ 📥 Remaining: *${remaining}*`,
                `├ 🕒 Generated: *${latestGeneratedAt.toLocaleTimeString('en-US', { hour12: false, timeZone: 'UTC' })} UTC*`,
                `├ ⏳ Expires in: *${timeLeftStr}*`,
                `└ 🩺 Status: *${statusEmoji}*`,
                ''
            ].join('\n'))
        }

        lines.push(`_Updated: ${new Date().toLocaleTimeString('en-US', { hour12: false })}_`)
        await sendMessage(lines.join('\n'))
    } catch (err) {
        console.error('[TelegramBot] Error handling /batches command:', err)
        await sendMessage(`❌ *Error fetching batch status:*\n\n\`\`\`\n${err instanceof Error ? err.message : String(err)}\n\`\`\``)
    }
}

// ─── Update polling loop ──────────────────────────────────────────────────────

let lastUpdateId = 0

async function pollUpdates(): Promise<void> {
    const data = await apiCall('getUpdates', {
        offset: lastUpdateId + 1,
        timeout: 30,
        allowed_updates: ['message', 'callback_query'],
    })

    if (!data?.ok || !data.result) return

    for (const update of data.result) {
        lastUpdateId = update.update_id

        if (update.message) {
            const text: string = update.message.text || ''
            const cmd = text.split(' ')[0].toLowerCase()

            if (cmd === '/start') await handleStart()
            else if (cmd === '/help') await handleHelp()
            else if (cmd === '/status') await handleStatus()
            else if (cmd === '/settings') await handleSettings()
            else if (cmd === '/batches' || cmd === '/monitor') await handleBatches()
        }

        if (update.callback_query) {
            const cq = update.callback_query
            const [action, arg] = (cq.data || '').split(':')

            if (action === 'toggle' && arg) {
                const type = arg as AlertType
                if (type in prefs) {
                    prefs[type] = !prefs[type]
                    savePrefs(prefs)

                    // Update the message in-place
                    await editMessage(cq.message.message_id, buildSettingsText(), {
                        reply_markup: buildSettingsKeyboard(),
                    })
                    await answerCallbackQuery(cq.id, `${ALERT_LABELS[type as AlertType]} ${prefs[type] ? 'enabled ✅' : 'disabled 🔴'}`)
                }
            } else if (action === 'settings' && arg === 'refresh') {
                await editMessage(cq.message.message_id, buildSettingsText(), {
                    reply_markup: buildSettingsKeyboard(),
                })
                await answerCallbackQuery(cq.id, 'Refreshed')
            } else if (action === 'settings' && arg === 'close') {
                await apiCall('deleteMessage', {
                    chat_id: CHAT_ID,
                    message_id: cq.message.message_id,
                })
                await answerCallbackQuery(cq.id)
            }
        }
    }
}

// ─── Bot startup ──────────────────────────────────────────────────────────────

export function startTelegramBot(database: Database): void {
    db = database
    if (!BOT_TOKEN || !CHAT_ID) {
        console.log('[TelegramBot] No BOT_TOKEN or CHAT_ID — bot disabled')
        return
    }

    console.log('[TelegramBot] Starting polling...')

    const poll = async () => {
        try {
            await pollUpdates()
        } catch (err) {
            // Polling errors are non-fatal — just log and retry
            console.error('[TelegramBot] Polling error:', err)
        }
        setTimeout(poll, 1000) // poll every second
    }

    setTimeout(poll, 2000) // initial delay to let server warm up
    console.log('[TelegramBot] Bot is running. Send /settings to configure alerts.')
}
