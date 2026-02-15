import { createDb } from '../src/db'
import dotenv from 'dotenv'

async function run() {
    dotenv.config()
    const db = createDb(process.env.FEEDGEN_SQLITE_LOCATION ?? ':memory:')

    const batches = await db
        .selectFrom('user_candidate_batch')
        .select(['userDid', 'batchId', 'generatedAt'])
        .distinct()
        .execute()

    console.log(`\n=== Ready Batches ===`)
    if (batches.length === 0) {
        console.log('No batches found! Pipeline may have failed or not run yet.')
    } else {
        for (const batch of batches) {
            const count = await db
                .selectFrom('user_candidate_batch')
                .select(db.fn.count('uri').as('count'))
                .where('batchId', '=', batch.batchId)
                .executeTakeFirst()

            console.log(`User: ${batch.userDid}`)
            console.log(`  Batch ID: ${batch.batchId}`)
            console.log(`  Generated: ${batch.generatedAt}`)
            console.log(`  Candidates: ${Number(count?.count)}`)
            console.log('---')
        }
    }
}

run()
