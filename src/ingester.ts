import dotenv from 'dotenv'
import { createDb } from './db'
import { JetstreamSubscription } from './subscription'
import { migrateToLatest } from './db'
import { logger } from './logger'

const run = async () => {
    dotenv.config()

    const postgresConnectionString = process.env.POSTGRES_CONNECTION_STRING ?? 'postgresql://bsky:bskypassword@localhost:5432/repo'
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

    // Start background database pruning (every 30 mins)
    setInterval(() => cleanupDatabase(db), 30 * 60 * 1000)
    // Refresh graph followers every 15 mins
    setInterval(() => refreshTrackedDids(), 15 * 60 * 1000)

    // Run once on startup
    setTimeout(() => cleanupDatabase(db), 5000)
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

        // To prevent unbounded growth of global posts, delete old posts 
        // that have very low engagement (or all old posts if disk gets tight,
        // but here we sweep posts older than 3 days with no engagement)
        const threeDaysAgo = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString()

        // Postgres specific DELETE using kysely
        const deletedPosts = await db.deleteFrom('post')
            .where('indexedAt', '<', threeDaysAgo)
            .where('likeCount', '=', 0)
            .where('repostCount', '=', 0)
            // Only delete if NO ONE we track follows them (this prevents deleting content meant for timeline)
            //.where('author', 'not in', (eb) => eb.selectFrom('graph_follow').select('followee')) 
            .executeTakeFirst()

        logger.info(`Cleanup complete. Cleaned empty global posts: ${deletedPosts.numDeletedRows}`)
    } catch (err) {
        logger.error('Failed during Postgres background cleanup:', err)
    }
}

run()
