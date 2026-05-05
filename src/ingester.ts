import dotenv from 'dotenv'
import { createDb } from './db'
import { JetstreamSubscription } from './subscription'
import { migrateToLatest } from './db'
import { logger } from './logger'

const run = async () => {
    dotenv.config()

    const postgresConnectionString = (process.env.USE_REMOTE_DB === 'true' && process.env.POSTGRES_CONNECTION_STRING_REMOTE) ? process.env.POSTGRES_CONNECTION_STRING_REMOTE : (process.env.POSTGRES_CONNECTION_STRING ?? 'postgresql://bsky:bskypassword@localhost:5432/repo')
    const subscriptionEndpoint = process.env.FEEDGEN_SUBSCRIPTION_ENDPOINT ?? 'wss://bsky.network'
    const publisherDid = process.env.FEEDGEN_PUBLISHER_DID ?? 'did:example:alice'
    const whitelist = (process.env.FEEDGEN_WHITELIST ?? '').split(',').filter(Boolean)
    const subscriptionReconnectDelay = parseInt(process.env.FEEDGEN_SUBSCRIPTION_RECONNECT_DELAY || '3000', 10)

    console.log('🤖 Starting Feed Generator Ingester...')
    logger.info('Starting Feed Generator Ingester...')

    const db = createDb(postgresConnectionString)
    await migrateToLatest(db)

    // Configure high-performance global like/repost tracking
    const subscription = new JetstreamSubscription(db, subscriptionEndpoint, {
        // We want all post edits, likes, and reposts globally
        wantedCollections: ['app.bsky.feed.post', 'app.bsky.feed.like', 'app.bsky.feed.repost'],
        // Empty wantedDids means subscribe to ALL users
        wantedDids: [],
        // Only apply fatigue tracking / personal graph tracking for our users
        trackedDids: new Set([publisherDid, ...whitelist]),
    })

    subscription.run(subscriptionReconnectDelay)

    // Refresh tracked DIDs for graph interaction tracking
    const refreshTrackedDids = async () => {
        try {
            const allUsersToTrack = [publisherDid, ...whitelist]

            const allLayer1Dids = await db.selectFrom('graph_follow')
                .select('followee')
                .where('follower', 'in', allUsersToTrack)
                .execute()

            const allTasteTwins = await db.selectFrom('taste_reputation')
                .select('similarUserDid')
                .where('userDid', 'in', allUsersToTrack)
                .where('reputationScore', '>', 1.1)
                .execute()

            const didsToTrack = new Set<string>()
            allLayer1Dids.forEach(r => didsToTrack.add(r.followee))
            allTasteTwins.forEach(r => didsToTrack.add(r.similarUserDid))
            allUsersToTrack.forEach(did => didsToTrack.add(did))

            subscription.trackedInteractionDids = didsToTrack
            console.log(`[Ingester] Updated tracked interaction DIDs. Watching ${didsToTrack.size} DIDs for graph interactions.`)
        } catch (err) {
            logger.error('Failed to refresh tracked DIDs:', err)
        }
    }

    // Start background database pruning (every 24 hours to reduce contention)
    setInterval(() => cleanupDatabase(db), 24 * 60 * 60 * 1000)
    // Refresh graph followers every 15 mins
    setInterval(() => refreshTrackedDids(), 15 * 60 * 1000)

    // Run graph refresh on startup
    setTimeout(() => refreshTrackedDids(), 5000)

    logger.info('Ingester running and subscribed to global firehose.')
}

async function cleanupDatabase(db: any) {
    try {
        logger.info('Running background Postgres cleanup...')

        // Prune very old feed_debug_logs
        await db.deleteFrom('feed_debug_log')
            .where('servedAt', '<', new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString())
            .execute()

        // Prune very old served/seen history (over 7 days)
        const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString()
        await db.deleteFrom('user_served_post')
            .where('servedAt', '<', sevenDaysAgo)
            .execute()
        await db.deleteFrom('user_seen_post')
            .where('seenAt', '<', sevenDaysAgo)
            .execute()

        // --- Tiered post pruning (batched to prevent deadlocks with concurrent inserts) ---
        // We use DELETE WHERE uri IN (SELECT uri ... LIMIT N) so only a fixed number
        // of rows are locked at once, allowing ingester writes to proceed between batches.
        const threeDaysAgo = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString()
        const sevenDaysAgoPost = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString()
        const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString()

        let totalDeleted = 0

        const runBatchPurger = async (fn: () => Promise<number>) => {
            let deletedInTier = 0
            while (true) {
                const count = await fn()
                deletedInTier += count
                if (count < 5000) break
                await new Promise(resolve => setTimeout(resolve, 500))
            }
            return deletedInTier
        }

        // Tier 1: posts older than 3 days with zero engagement (original rule)
        const tier1 = await runBatchPurger(() => deletePostsBatched(db, threeDaysAgo, 0, 0, 5000))
        totalDeleted += tier1

        // Small pause between tiers to reduce contention
        await new Promise(resolve => setTimeout(resolve, 500))

        // Tier 2: posts older than 7 days with low engagement (< 5 likes, < 2 reposts)
        // Cuts the long tail that the zero-engagement filter misses
        const tier2 = await runBatchPurger(() => deletePostsBatched(db, sevenDaysAgoPost, 5, 2, 5000))
        totalDeleted += tier2

        await new Promise(resolve => setTimeout(resolve, 500))

        // Tier 3: posts older than 30 days — delete everything.
        // The algorithm never looks back more than 30 days, so this data is useless.
        const tier3 = await runBatchPurger(() => deletePostsBatchedAll(db, thirtyDaysAgo, 5000))
        totalDeleted += tier3

        logger.info(`Cleanup complete. Pruned posts: ${totalDeleted} (t1:${tier1}, t2:${tier2}, t3:${tier3})`)
    } catch (err) {
        logger.error('Failed during Postgres background cleanup:', err)
    }
}

/**
 * Batched delete for posts with zero engagement older than cutoff.
 * Uses a subquery with LIMIT to lock only a fixed number of rows at a time,
 * preventing deadlocks with concurrent ingester inserts.
 */
async function deletePostsBatched(db: any, cutoff: string, maxLikes: number, maxReposts: number, batchSize: number): Promise<number> {
    try {
        const result = await db.deleteFrom('post')
            .where('uri', 'in', (eb: any) =>
                eb.selectFrom('post')
                    .select('uri')
                    .where('indexedAt', '<', cutoff)
                    .where('likeCount', '<=', maxLikes)
                    .where('repostCount', '<=', maxReposts)
                    // Never prune posts that are currently in an active semantic batch for ANY user
                    .where(eb.not(eb.exists(
                        eb.selectFrom('user_candidate_batch').select('uri').whereRef('user_candidate_batch.uri', '=', 'post.uri')
                    )))
                    .limit(batchSize)
            )
            .executeTakeFirst()
        return Number(result?.numDeletedRows ?? 0)
    } catch (err: any) {
        if (err?.code === '40P01') { // Postgres deadlock error code
            logger.warn('Deadlock during post cleanup (tier), will retry next cycle')
            return 0
        }
        throw err
    }
}

/**
 * Batched delete for ALL posts older than cutoff (regardless of engagement).
 * Used for the 30-day full sweep.
 */
async function deletePostsBatchedAll(db: any, cutoff: string, batchSize: number): Promise<number> {
    try {
        const result = await db.deleteFrom('post')
            .where('uri', 'in', (eb: any) =>
                eb.selectFrom('post')
                    .select('uri')
                    .where('indexedAt', '<', cutoff)
                    // Never prune posts that are currently in an active semantic batch for ANY user
                    .where(eb.not(eb.exists(
                        eb.selectFrom('user_candidate_batch').select('uri').whereRef('user_candidate_batch.uri', '=', 'post.uri')
                    )))
                    .limit(batchSize)
            )
            .executeTakeFirst()
        return Number(result?.numDeletedRows ?? 0)
    } catch (err: any) {
        if (err?.code === '40P01') { // Postgres deadlock error code
            logger.warn('Deadlock during post cleanup (30d sweep), will retry next cycle')
            return 0
        }
        throw err
    }
}

run()
