import { QueryParams } from '../lexicon/types/app/bsky/feed/getFeedSkeleton'
import { AppContext } from '../config'

export const shortname = 'social-graph'

export const handler = async (
    ctx: AppContext,
    params: QueryParams,
    requesterDid: string,
) => {
    // 1. Fetch user's graph (Layer 1 and Layer 2)
    const layer1Rows = await ctx.db
        .selectFrom('graph_follow')
        .select('followee')
        .where('follower', '=', requesterDid)
        .execute()
    const layer1Dids = new Set(layer1Rows.map(r => r.followee))

    const layer2Rows = await ctx.db
        .selectFrom('graph_follow')
        .select('followee')
        .where('follower', 'in', Array.from(layer1Dids).length > 0 ? Array.from(layer1Dids) : ['dummy'])
        .execute()
    const layer2Dids = new Set(layer2Rows.map(r => r.followee))

    // Mutuals check
    const mutualRows = await ctx.db
        .selectFrom('graph_follow')
        .select('follower')
        .where('followee', '=', requesterDid)
        .where('follower', 'in', Array.from(layer1Dids).length > 0 ? Array.from(layer1Dids) : ['dummy'])
        .execute()
    const mutualDids = new Set(mutualRows.map(r => r.follower))

    // 1.5. Bubble Influence Computation with Caching
    const cacheExpiry = 24 * 60 * 60 * 1000 // 24 hours
    const now = new Date().toISOString()

    // Check if we have fresh cache
    const cachedInfluential = await ctx.db
        .selectFrom('user_influential_l2')
        .select(['l2Did', 'influenceScore', 'l1FollowerCount', 'updatedAt'])
        .where('userDid', '=', requesterDid)
        .execute()

    let influentialL2: Array<{ did: string; score: number; l1Count: number }> = []
    const needsRefresh = cachedInfluential.length === 0 ||
        (new Date(now).getTime() - new Date(cachedInfluential[0]?.updatedAt || 0).getTime() > cacheExpiry)

    if (needsRefresh) {
        console.log(`[BubbleInfluence] Computing influential L2 for ${requesterDid.slice(0, 10)}...`)

        // Compute L2 bubble influence scores
        const l2FollowerCounts = await ctx.db
            .selectFrom('graph_follow')
            .select(['followee', ctx.db.fn.count<number>('follower').as('l1_count')])
            .where('follower', 'in', Array.from(layer1Dids).length > 0 ? Array.from(layer1Dids) : ['dummy'])
            .groupBy('followee')
            .having(ctx.db.fn.count<number>('follower'), '>=', 2) // At least 2 L1 followers for relevance
            .execute()

        // Get total follower counts for L2 accounts
        const l2Dids = l2FollowerCounts.map(r => r.followee)
        const l2TotalFollowers = await ctx.db
            .selectFrom('graph_follow')
            .select(['followee', ctx.db.fn.count<number>('follower').as('total_count')])
            .where('followee', 'in', l2Dids.length > 0 ? l2Dids : ['dummy'])
            .groupBy('followee')
            .execute()

        const totalMap = new Map(l2TotalFollowers.map(t => [t.followee, Number(t.total_count)]))

        // Compute influence: (L1_count / sqrt(total_followers)) * L1_count
        influentialL2 = l2FollowerCounts
            .map(l2 => {
                const total = totalMap.get(l2.followee) || 1
                const l1Count = Number(l2.l1_count)
                const influenceScore = (l1Count / Math.sqrt(total)) * l1Count
                return { did: l2.followee, score: influenceScore, l1Count }
            })
            .sort((a, b) => b.score - a.score)
            .slice(0, 100) // Top 100 influential L2s

        // Update cache
        await ctx.db.deleteFrom('user_influential_l2').where('userDid', '=', requesterDid).execute()
        if (influentialL2.length > 0) {
            await ctx.db
                .insertInto('user_influential_l2')
                .values(influentialL2.map(i => ({
                    userDid: requesterDid,
                    l2Did: i.did,
                    influenceScore: i.score,
                    l1FollowerCount: i.l1Count,
                    updatedAt: now,
                })))
                .execute()
        }
        console.log(`[BubbleInfluence] Cached ${influentialL2.length} influential L2 accounts`)
    } else {
        // Use cached data
        influentialL2 = cachedInfluential.map(c => ({
            did: c.l2Did,
            score: c.influenceScore,
            l1Count: c.l1FollowerCount,
        }))
    }

    const influentialL2Dids = new Set(influentialL2.map(i => i.did))

    // 1.7. Personal Interaction Scope (Layer 0)
    // Track accounts the user has directly liked or replied to (Top 100 most recent unique authors)
    const personalInteractions = await ctx.db
        .selectFrom('graph_interaction')
        .select(['target', 'type'])
        .where('actor', '=', requesterDid)
        .orderBy('indexedAt', 'desc')
        .limit(200) // Fetch more to find 100 unique authors
        .execute()

    // Extract DIDs from interaction targets (uris)
    const interactedDids = new Set<string>()
    for (const int of personalInteractions) {
        if (interactedDids.size >= 100) break
        const parts = int.target.replace('at://', '').split('/')
        if (parts[0]?.startsWith('did:')) {
            interactedDids.add(parts[0])
        }
    }

    // 1.8. Serving Fatigue (Memory)
    // Fetch URIs served to this user in the last 60 minutes
    const fatigueLookback = new Date(Date.now() - 60 * 60 * 1000).toISOString()
    const servedPosts = await ctx.db
        .selectFrom('user_served_post')
        .select(['uri'])
        .where('userDid', '=', requesterDid)
        .where('servedAt', '>', fatigueLookback)
        .execute()

    const servedCountMap: Record<string, number> = {}
    servedPosts.forEach(sp => {
        servedCountMap[sp.uri] = (servedCountMap[sp.uri] || 0) + 1
    })

    // 2.5. Discovery & Scoring Pipeline with SMART Fallback
    const stages = [
        { lookback: 48, threshold: 200 },
        { lookback: 48, threshold: 100 },
        { lookback: 96, threshold: 100 },
        { lookback: 96, threshold: 0 },
    ]

    let finalScoredPosts: Array<{ post: any; score: number }> = []
    let posts: any[] = []

    for (const stage of stages) {
        const lookbackTime = new Date(Date.now() - stage.lookback * 60 * 60 * 1000).toISOString()

        // Pre-filter: Find posts with strong network proof (5+ L1 likes)
        const strongNetworkProofResults = await ctx.db
            .selectFrom('graph_interaction')
            .select(['target', ctx.db.fn.count<number>('actor').as('like_count')])
            .where('actor', 'in', Array.from(layer1Dids).length > 0 ? Array.from(layer1Dids) : ['dummy'])
            .where('type', '=', 'like')
            .where('indexedAt', '>', lookbackTime)
            .groupBy('target')
            .having(ctx.db.fn.count<number>('actor'), '>=', 5)
            .execute()

        const strongNetworkProofUris = strongNetworkProofResults.map(r => r.target)

        let query = ctx.db
            .selectFrom('post')
            .selectAll()
            .where('indexedAt', '>', lookbackTime)
            .where((eb) => eb.or([
                eb('author', 'in', Array.from(layer1Dids).length > 0 ? Array.from(layer1Dids) : ['dummy']),
                eb('author', 'in', Array.from(layer2Dids).length > 0 ? Array.from(layer2Dids) : ['dummy']),
                eb('author', 'in', Array.from(interactedDids).length > 0 ? Array.from(interactedDids) : ['dummy']), // Interaction Discovery
                eb('likeCount', '>', 3),
                eb('uri', 'in', strongNetworkProofUris.length > 0 ? strongNetworkProofUris : ['dummy'])
            ]))
            .orderBy('indexedAt', 'desc')
            .limit(1000)

        if (params.cursor) {
            const timeStr = new Date(parseInt(params.cursor, 10)).toISOString()
            query = query.where('post.indexedAt', '<', timeStr)
        }

        posts = await query.execute()
        if (posts.length === 0 && stage.lookback === 48) continue

        // Fetch interactions for scoring
        const postUris = posts.map(p => p.uri)
        const interactions = await ctx.db
            .selectFrom('graph_interaction')
            .select(['target', 'type', 'actor'])
            .where('target', 'in', postUris.length > 0 ? postUris : ['dummy'])
            .where((eb) => eb.or([
                eb('actor', 'in', Array.from(layer1Dids).length > 0 ? Array.from(layer1Dids) : ['dummy']),
                eb('actor', 'in', Array.from(influentialL2Dids).length > 0 ? Array.from(influentialL2Dids) : ['dummy'])
            ]))
            .execute()

        const networkEffortMap: Record<string, { likes: number, reposts: number, actors: Set<string> }> = {}
        interactions.forEach(int => {
            if (!networkEffortMap[int.target]) {
                networkEffortMap[int.target] = { likes: 0, reposts: 0, actors: new Set() }
            }
            if (int.type === 'like') networkEffortMap[int.target].likes++
            if (int.type === 'repost') networkEffortMap[int.target].reposts++
            networkEffortMap[int.target].actors.add(int.actor)
        })

        // Scoring
        const candidateScoredPosts = posts.map((post) => {
            let score = 0
            const ageInHours = (Date.now() - new Date(post.indexedAt).getTime()) / (1000 * 60 * 60)

            // Base recency decay (6h half-life)
            const recencyMultiplier = Math.pow(0.5, ageInHours / 6)
            score += 10 * recencyMultiplier

            // Gentle Tier Decay (48h half-life)
            const tierDecay = Math.pow(0.5, ageInHours / 48)

            const isLayer1 = layer1Dids.has(post.author)
            const isLayer2 = layer2Dids.has(post.author)
            const isInteracted = interactedDids.has(post.author)
            const isMutual = mutualDids.has(post.author)

            if (isLayer1) {
                score += 2000 * tierDecay
                if (isMutual) score += 500 * tierDecay
            } else if (isInteracted) {
                score += 1500 * tierDecay
            } else if (isLayer2) {
                score += 500 * tierDecay
            } else {
                score += 50 * tierDecay
            }

            const networkInteractions = networkEffortMap[post.uri]
            if (networkInteractions) {
                const count = networkInteractions.likes + networkInteractions.reposts
                score += Math.round(Math.pow(count, 1.5) * 200)
            }

            score += (post.likeCount || 0) * 15 + (post.repostCount || 0) * 30

            const hasAnyEngagement = (post.likeCount || 0) > 0 || (post.repostCount || 0) > 0 || networkInteractions
            if (!hasAnyEngagement && ageInHours < 2) score -= 400
            if (post.replyParent) score -= 1000
            if (!isLayer1 && !isLayer2 && !isInteracted && !networkInteractions) score -= 1000

            // Serving Fatigue Penalty (Harsh rotation)
            const seenCount = servedCountMap[post.uri] || 0
            if (seenCount > 0) {
                score -= 1000 * seenCount
            }

            // Entropic Shuffling: Add random jitter (+0-800 pts)
            score += Math.random() * 800

            return { post, score: Math.round(score) }
        })

        // Filter and Dedup
        const filtered = candidateScoredPosts.filter(sp => {
            if (sp.post.replyParent && !layer1Dids.has(sp.post.author) && !layer2Dids.has(sp.post.author)) return false
            return sp.score > stage.threshold
        })

        // Relaxed Dedup: Allow up to 2 posts from the same thread
        const threadCounts: Record<string, number> = {}
        const finalPool: Array<{ post: any; score: number }> = []

        // Sort by score first to pick the best items from each thread
        const sortedCandidatePool = filtered.sort((a, b) => b.score - a.score)

        for (const sp of sortedCandidatePool) {
            const threadKey = sp.post.replyRoot || sp.post.uri
            threadCounts[threadKey] = (threadCounts[threadKey] || 0) + 1
            if (threadCounts[threadKey] <= 2) {
                finalPool.push(sp)
            }
        }

        finalScoredPosts = finalPool.sort((a, b) => b.score - a.score)

        // If we found enough content, we stop
        if (finalScoredPosts.length >= 10) {
            console.log(`[Algo] SMART Fallback: Found ${finalScoredPosts.length} posts at stage (lookback: ${stage.lookback}h, threshold: ${stage.threshold})`)
            break
        }
    }

    const limit = params.limit || 30
    const feed = finalScoredPosts.slice(0, limit).map((p) => ({
        post: p.post.uri,
    }))

    let cursor: string | undefined
    const last = posts.at(-1)
    if (last && posts.length === 1000) {
        cursor = new Date(last.indexedAt).getTime().toString(10)
    }

    return {
        cursor,
        feed,
    }
}
