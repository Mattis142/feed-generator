import dotenv from 'dotenv'

dotenv.config()

const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN
const CHAT_ID = process.env.TELEGRAM_CHAT_ID

export const notifyTelegram = async (message: string) => {
    if (!BOT_TOKEN || !CHAT_ID) return

    try {
        const url = `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`
        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                chat_id: CHAT_ID,
                text: message.slice(0, 4000), // Telegram has a 4096 char limit
                parse_mode: 'Markdown',
            }),
        })

        if (!response.ok) {
            console.error('Failed to send Telegram notification:', await response.text())
        }
    } catch (err) {
        console.error('Error sending Telegram notification:', err)
    }
}

export const logger = {
    info: (msg: string, ...args: any[]) => {
        console.log(`[INFO] ${msg}`, ...args)
    },
    warn: (msg: string, ...args: any[]) => {
        console.warn(`[WARN] ${msg}`, ...args)
    },
    error: (msg: string, err?: any, ...args: any[]) => {
        const errorMessage = err instanceof Error ? err.stack || err.message : String(err)
        console.error(`[ERROR] ${msg}`, errorMessage, ...args)

        // Send to Telegram
        const hostname = process.env.FEEDGEN_HOSTNAME || 'unknown-host'
        const telegramMessage = `❌ *Error on ${hostname}*\n\n*Context:* ${msg}\n\n\`\`\`\n${errorMessage}\n\`\`\``
        notifyTelegram(telegramMessage).catch(console.error)
    },
}
