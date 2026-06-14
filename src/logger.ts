import dotenv from 'dotenv'
import { notifyError as tgNotifyError, notify } from './telegram-bot'

dotenv.config()

// Kept for backwards-compat call sites outside logger
export const notifyTelegram = async (message: string) => {
    await notify('warnings', message)
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
        tgNotifyError(msg, err).catch(console.error)
    },
}
