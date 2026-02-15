import { createDb } from '../src/db'
import dotenv from 'dotenv'
import path from 'path'
import fs from 'fs'
import { sql } from 'kysely'

const ENV_PATH = path.join(__dirname, '../.env')
dotenv.config({ path: ENV_PATH })

const dbLocation = process.env.FEEDGEN_SQLITE_LOCATION || 'db.sqlite'
const db = createDb(dbLocation)

const REFRESH_INTERVAL_MS = 2000

async function monitor() {
    console.clear()

    while (true) {
        try {
            const stats = await db.selectFrom('user_candidate_batch as uc')
                .select([
                    'uc.userDid',
                    sql<number>`count(distinct uc.uri)`.as('total'),
                    (eb) => eb.fn.max('uc.generatedAt').as('last_gen')
                ])
                .groupBy('uc.userDid')
                .execute()

            const results: any[] = []

            for (const row of stats) {
                // Get unique items from this batch that have been served
                const consumedResult = await db.selectFrom('user_candidate_batch as b')
                    .innerJoin('user_served_post as s', (join) =>
                        join.onRef('b.uri', '=', 's.uri')
                            .onRef('b.userDid', '=', 's.userDid')
                    )
                    .select(({ fn }) => [
                        sql<number>`count(distinct b.uri)`.as('count')
                    ])
                    .where('b.userDid', '=', row.userDid as string)
                    .executeTakeFirst()

                const total = Number(row.total)
                const count = Number((consumedResult as any)?.count || 0)
                const ratio = total > 0 ? (count / total) * 100 : 0

                // Trigger logic: ratio >= 50%
                const isRegenerating = ratio >= 50

                results.push({
                    user: (row.userDid as string).slice(-8),
                    total,
                    consumed: count,
                    ratio: ratio.toFixed(1) + '%',
                    lastGen: new Date(row.last_gen as string).toLocaleTimeString(),
                    status: isRegenerating ? '🔄 REGEN TRIGGERED' : '✅ HEALTHY'
                })
            }

            // Move cursor to top left instead of clearing for less flicker
            process.stdout.write('\x1b[H')
            console.log('📊 Semantic Batch Monitor (Live)')
            console.log(`Updated: ${new Date().toLocaleTimeString()}`)
            console.log('----------------------------------------------------')

            if (results.length > 0) {
                console.table(results)
            } else {
                console.log('No active batches found in user_candidate_batch.')
            }

            console.log('\n(Press Ctrl+C to stop monitor)')
            console.log('⚠️ ALERT: You must restart `npm run turbo` manually to apply fallback fixes!')

        } catch (err) {
            if (!(err as any).message?.includes('database is locked')) {
                console.error('Monitor error:', err)
            }
        }

        await new Promise(resolve => setTimeout(resolve, REFRESH_INTERVAL_MS))
    }
}

console.log('Starting monitor...')
monitor().catch(console.error)
