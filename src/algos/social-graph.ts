import { QueryParams } from '../lexicon/types/app/bsky/feed/getFeedSkeleton'
import { AppContext } from '../config'
import { Database } from '../db'
import { getTasteSimilarUsers, getPostsLikedBySimilarUsers, updateTasteReputation } from './taste-similarity'

// Account diversity function to prevent same author from appearing consecutively
function applyAccountDiversity(posts: Array<{ post: any; score: number; signals?: any; repostUri?: string }>): Array<{ post: any; score: number; signals?: any; repostUri?: string }> {
    if (posts.length <= 1) return posts

    const result: Array<{ post: any; score: number; signals?: any; repostUri?: string }> = []
    const usedAuthors = new Set<string>()
    const remainingPosts = [...posts]

    // Take the highest scoring post first
    result.push(remainingPosts.shift()!)
    usedAuthors.add(result[0].post.author)

    // Greedy approach: always pick the highest scoring post from a different author
    while (remainingPosts.length > 0 && result.length < posts.length) {
        let bestIndex = -1
        let bestScore = -Infinity

        // Find the highest scoring post from a different author
        for (let i = 0; i < remainingPosts.length; i++) {
            const post = remainingPosts[i]
            if (!usedAuthors.has(post.post.author) && post.score > bestScore) {
                bestIndex = i
                bestScore = post.score
            }
        }

        if (bestIndex === -1) {
            // No posts from different authors available, take the highest scoring remaining post
            const bestRemaining = remainingPosts.shift()!
            result.push(bestRemaining)
            usedAuthors.add(bestRemaining.post.author)
        } else {
            // Found a post from a different author
            const selected = remainingPosts.splice(bestIndex, 1)[0]
            result.push(selected)
            usedAuthors.add(selected.post.author)

            // Reset used authors every 3 posts to allow some repetition but not consecutive
            if (result.length % 3 === 0) {
                usedAuthors.clear()
                // Add the last 2 authors to prevent immediate repetition
                if (result.length >= 2) {
                    usedAuthors.add(result[result.length - 1].post.author)
                    usedAuthors.add(result[result.length - 2].post.author)
                }
            }
        }
    }

    // Add any remaining posts
    result.push(...remainingPosts)

    console.log(`[Account Diversity] Reordered ${posts.length} posts to prevent consecutive authors`)

    // Final safety: if diversity logic accidentally killed the pool size, return the original
    return result.length < posts.length * 0.5 ? posts : result
}

export const shortname = 'social-graph'

export type HandlerOptions = {
    batchMode?: boolean  // If true: looser thresholds, no fatigue, return full pool
}

export const handler = async (
    ctx: AppContext,
    params: QueryParams,
    requesterDid: string,
    options: HandlerOptions = {},
) => {
    const { batchMode = false } = options

    const now = new Date().toISOString()

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
    const cacheExpiry = 72 * 60 * 60 * 1000 // Increased to 72 hours to reduce regeneration churn

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

    // 1.8. InteractionSeen Fatigue (Memory) + User Interaction Tracking
    // Fetch posts the user has actually SEEN in the last 6 hours for accurate fatigue tracking
    // Note: We use user_seen_post based on InteractionSeen API for precise view tracking
    const fatigueLookback = new Date(Date.now() - 6 * 60 * 60 * 1000).toISOString()
    const seenPosts = await ctx.db
        .selectFrom('user_seen_post')
        .select(['uri', 'seenAt'])
        .where('userDid', '=', requesterDid)
        .where('seenAt', '>', fatigueLookback)
        .execute()

    // Fetch posts the user has already interacted with (likes, reposts, replies)
    const userInteractions = await ctx.db
        .selectFrom('graph_interaction')
        .select(['target', 'type'])
        .where('actor', '=', requesterDid)
        .where('type', 'in', ['like', 'repost', 'reply'])
        .execute()

    const seenCountMap: Record<string, number> = {}
    const seenTimeMap: Record<string, string> = {}
    const userInteractionMap: Record<string, string> = {} // Track interaction type

    seenPosts.forEach(sp => {
        seenCountMap[sp.uri] = (seenCountMap[sp.uri] || 0) + 1
        seenTimeMap[sp.uri] = sp.seenAt
    })

    userInteractions.forEach(ui => {
        userInteractionMap[ui.target] = ui.type
    })

    // 2.0. Taste Similarity Analysis - Find users with similar tastes (Increased to 100 for better discovery)
    const tasteSimilarUsers = await getTasteSimilarUsers(ctx, requesterDid, 100)
    console.log(`[Taste Similarity] Found ${tasteSimilarUsers.length} taste-similar users`)

    // Get posts liked by taste-similar users in the last 72 hours
    const tasteSimilarPosts = await getPostsLikedBySimilarUsers(ctx, requesterDid, tasteSimilarUsers, 72)
    const tasteSimilarPostMap = new Map<string, { boostScore: number; similarUserDids: string[] }>()
    tasteSimilarPosts.forEach(tsp => {
        tasteSimilarPostMap.set(tsp.postUri, {
            boostScore: tsp.boostScore,
            similarUserDids: tsp.similarUserDids
        })
    })
    console.log(`[Taste Similarity] Found ${tasteSimilarPosts.length} posts liked by similar users`)

    // 2.2. Calculate Media Preference Ratio (Recent 100 likes)
    const recentLikes = await ctx.db
        .selectFrom('graph_interaction')
        .innerJoin('post', 'post.uri', 'graph_interaction.target')
        .select(['post.hasImage', 'post.hasVideo', 'post.hasExternal'])
        .where('graph_interaction.actor', '=', requesterDid)
        .where('graph_interaction.type', '=', 'like')
        .orderBy('graph_interaction.indexedAt', 'desc')
        .limit(100)
        .execute()

    let imageCount = 0
    let videoCount = 0
    recentLikes.forEach(l => {
        if (l.hasImage) imageCount++
        if (l.hasVideo) videoCount++
    })
    const imageRatio = recentLikes.length > 0 ? imageCount / recentLikes.length : 0.25
    const videoRatio = recentLikes.length > 0 ? videoCount / recentLikes.length : 0.25
    console.log(`[Media Preference] User ${requesterDid.slice(0, 10)} has image ratio: ${imageRatio.toFixed(2)} (${imageCount}/${recentLikes.length}) and video ratio: ${videoRatio.toFixed(2)} (${videoCount}/${recentLikes.length})`)

    // 2.5. Discovery & Scoring Pipeline: Global Mix (Anti-Chronological)
    const lookback72h = new Date(Date.now() - 72 * 60 * 60 * 1000).toISOString()
    const lookback7d = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString()
    const lookback30d = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString()

    // bucket 1: Fresh Activity (Last 72 hours, limited discovery + social graph)
    // Multi-factor scoring with randomized weights for diversity
    const bucket1Raw = await ctx.db
        .selectFrom('post')
        .selectAll()
        .where('indexedAt', '>', lookback72h)
        .where((eb) => eb.or([
            eb('author', 'in', Array.from(layer1Dids).length > 0 ? Array.from(layer1Dids) : ['dummy']),
            eb('author', 'in', Array.from(layer2Dids).length > 0 ? Array.from(layer2Dids) : ['dummy']),
            eb('author', 'in', Array.from(interactedDids).length > 0 ? Array.from(interactedDids) : ['dummy']),
            eb('likeCount', '>', batchMode ? 0 : 2) // Looser in batch mode
        ]))
        .limit(batchMode ? 3000 : 1200)
        .execute()

    // Apply multi-factor scoring with jitter
    // Apply multi-factor scoring with jitter
    // CRITICAL: Jitter must be seeded by requesterDid to prevent "Same Order" bug across accounts
    const userSeed = requesterDid.split('').reduce((acc, char) => acc + char.charCodeAt(0), 0)
    const seed = (Date.now() + userSeed) % 1000
    const randomFactor1 = (seed * 9301 + 49297) % 233280 / 233280 // 0-1
    const randomFactor2 = (seed * 233280 + 9301) % 49297 / 49297 // 0-1  
    const randomFactor3 = (seed * 49297 + 233280) % 9301 / 9301 // 0-1

    const bucket1 = bucket1Raw
        .map(post => {
            const ageInHours = (Date.now() - new Date(post.indexedAt).getTime()) / (1000 * 60 * 60)
            const likes = post.likeCount || 0
            const likeVelocity = ageInHours > 0 ? likes / ageInHours : likes // likes per hour

            // Three scoring factors with increased randomization (0.5-2.0x)
            const likeScore = likes * (0.5 + randomFactor1 * 1.5) // 0.5-2.0x
            const timeScore = (1000 / (ageInHours + 1)) * (0.5 + randomFactor2 * 1.5) // 0.5-2.0x
            const velocityScore = likeVelocity * 50 * (0.5 + randomFactor3 * 1.5) // 0.5-2.0x

            return { post, score: likeScore + timeScore + velocityScore }
        })
        .sort((a, b) => b.score - a.score)
        .slice(0, 1200) // Increased for infinite scroll
        .map(item => item.post)

    // bucket 1.5: Bridge Activity (3-7 days, medium engagement)
    const bucket1_5Raw = await ctx.db
        .selectFrom('post')
        .selectAll()
        .where('indexedAt', '>', lookback7d)
        .where('indexedAt', '<=', lookback72h)
        .where((eb) => eb.or([
            eb('author', 'in', Array.from(layer1Dids).length > 0 ? Array.from(layer1Dids) : ['dummy']),
            eb('author', 'in', Array.from(layer2Dids).length > 0 ? Array.from(layer2Dids) : ['dummy']),
            eb('author', 'in', Array.from(interactedDids).length > 0 ? Array.from(interactedDids) : ['dummy']),
            eb('likeCount', '>', 1) // RESTORED: Limited global discovery
        ]))
        .limit(600) // Increased for infinite scroll
        .execute()

    // Different random factors for bridge bucket
    const seed1_5 = (Date.now() + 500) % 1000
    const randomFactor1_5_1 = (seed1_5 * 9301 + 49297) % 233280 / 233280
    const randomFactor1_5_2 = (seed1_5 * 233280 + 9301) % 49297 / 49297
    const randomFactor1_5_3 = (seed1_5 * 49297 + 233280) % 9301 / 9301

    const bucket1_5 = bucket1_5Raw
        .map(post => {
            const ageInHours = (Date.now() - new Date(post.indexedAt).getTime()) / (1000 * 60 * 60)
            const likes = post.likeCount || 0
            const likeVelocity = ageInHours > 0 ? likes / ageInHours : likes

            // Balanced scoring for bridge content with increased randomization (0.4-2.2x)
            const likeScore = likes * (0.4 + randomFactor1_5_1 * 1.8) // 0.4-2.2x
            const timeScore = (700 / (ageInHours + 1)) * (0.4 + randomFactor1_5_2 * 1.8) // 0.4-2.2x
            const velocityScore = likeVelocity * 35 * (0.4 + randomFactor1_5_3 * 1.8) // 0.4-2.2x

            return { post, score: likeScore + timeScore + velocityScore }
        })
        .sort((a, b) => b.score - a.score)
        .slice(0, 600)
        .map(item => item.post)

    // bucket 2: Global Gems (Last 30 days, any post with high engagement)
    // RESTORED: The user wants discovery. We will sandbox it in the Scoring phase.
    let bucket2Raw: typeof bucket1Raw = []

    // 1. Fetch "Global" candidates (Discovery)
    const globalGemsRaw = await ctx.db
        .selectFrom('post')
        .selectAll()
        .where('indexedAt', '>', lookback30d)
        .where('likeCount', '>', batchMode ? 0 : 1) // Looser in batch mode
        .limit(batchMode ? 5000 : 1000)
        .execute()

    // 2. Fetch "Taste" candidates (Personalized Discovery)
    let tasteRaw: typeof bucket1Raw = []
    if (tasteSimilarPostMap.size > 0) {
        const tasteUris = Array.from(tasteSimilarPostMap.keys()).slice(0, 2000) // Increased limit
        if (tasteUris.length > 0) {
            tasteRaw = await ctx.db
                .selectFrom('post')
                .selectAll()
                .where('uri', 'in', tasteUris)
                .limit(2000) // Increased limit
                .execute()
        }
    }

    // Merge them
    bucket2Raw = [...globalGemsRaw, ...tasteRaw]

    // Different random factors for bucket 2
    const seed2 = (Date.now() + 1000) % 1000
    const randomFactor4 = (seed2 * 9301 + 49297) % 233280 / 233280
    const randomFactor5 = (seed2 * 233280 + 9301) % 49297 / 49297
    const randomFactor6 = (seed2 * 49297 + 233280) % 9301 / 9301

    const bucket2 = bucket2Raw
        .map(post => {
            const ageInHours = (Date.now() - new Date(post.indexedAt).getTime()) / (1000 * 60 * 60)
            const likes = post.likeCount || 0
            const likeVelocity = ageInHours > 0 ? likes / ageInHours : likes

            // Different weight balance for global gems with increased randomization (0.3-2.7x)
            const likeScore = likes * (0.3 + randomFactor4 * 2.4) // 0.3-2.7x
            const timeScore = (500 / (ageInHours + 1)) * (0.3 + randomFactor5 * 2.4) // 0.3-2.7x
            const velocityScore = likeVelocity * 30 * (0.3 + randomFactor6 * 2.4) // 0.3-2.7x

            return { post, score: likeScore + timeScore + velocityScore }
        })
        .sort((a, b) => b.score - a.score)
        .slice(0, batchMode ? 3000 : 1600) // Increased for infinite scroll
        .map(item => item.post)

    // bucket 3: Bubble Highlights (Last 30 days, from closest circle only)
    const bucket3Raw = await ctx.db
        .selectFrom('post')
        .selectAll()
        .where('indexedAt', '>', lookback30d)
        .where((eb) => eb.or([
            eb('author', 'in', Array.from(layer1Dids).length > 0 ? Array.from(layer1Dids) : ['dummy']),
            eb('author', 'in', Array.from(interactedDids).length > 0 ? Array.from(interactedDids) : ['dummy'])
        ]))
        .limit(800) // Increased for infinite scroll
        .execute()

    // Different random factors for bucket 3
    const seed3 = (Date.now() + 2000) % 1000
    const randomFactor7 = (seed3 * 9301 + 49297) % 233280 / 233280
    const randomFactor8 = (seed3 * 233280 + 9301) % 49297 / 49297
    const randomFactor9 = (seed3 * 49297 + 233280) % 9301 / 9301

    const bucket3 = bucket3Raw
        .map(post => {
            const ageInHours = (Date.now() - new Date(post.indexedAt).getTime()) / (1000 * 60 * 60)
            const likes = post.likeCount || 0
            const likeVelocity = ageInHours > 0 ? likes / ageInHours : likes

            // Balanced weights for bubble highlights with increased randomization (0.6-1.8x)
            const likeScore = likes * (0.6 + randomFactor7 * 1.2) // 0.6-1.8x
            const timeScore = (800 / (ageInHours + 1)) * (0.6 + randomFactor8 * 1.2) // 0.6-1.8x
            const velocityScore = likeVelocity * 40 * (0.6 + randomFactor9 * 1.2) // 0.6-1.8x

            return { post, score: likeScore + timeScore + velocityScore }
        })
        .sort((a, b) => b.score - a.score)
        .slice(0, 800)
        .map(item => item.post)

    const [posts1, posts1_5, posts2, posts3] = [bucket1, bucket1_5, bucket2, bucket3]

    // Merge and deduplicate
    const allPosts = [...posts1, ...posts1_5, ...posts2, ...posts3]
    const uniquePosts = Array.from(new Map(allPosts.map(p => [p.uri, p])).values())

    console.log(`[Algo] Harvesting completed: ${uniquePosts.length} unique candidates (B1:${posts1.length}, B1.5:${posts1_5.length}, B2:${posts2.length}, B3:${posts3.length})`)

    // Fetch interactions for scoring
    const postUris = uniquePosts.map(p => p.uri)
    const interactions = await ctx.db
        .selectFrom('graph_interaction')
        .select(['target', 'type', 'actor', 'interactionUri'])
        .where('target', 'in', postUris.length > 0 ? postUris : ['dummy'])
        .where((eb) => eb.or([
            eb('actor', 'in', Array.from(layer1Dids).length > 0 ? Array.from(layer1Dids) : ['dummy']),
            eb('actor', 'in', Array.from(influentialL2Dids).length > 0 ? Array.from(influentialL2Dids) : ['dummy'])
        ]))
        .execute()

    const networkEffortMap: Record<string, { likes: number, reposts: number, actors: Set<string>, repostUri?: string }> = {}
    interactions.forEach(int => {
        if (!networkEffortMap[int.target]) {
            networkEffortMap[int.target] = { likes: 0, reposts: 0, actors: new Set() }
        }
        if (int.type === 'like') networkEffortMap[int.target].likes++
        if (int.type === 'repost') {
            networkEffortMap[int.target].reposts++
            // Track the repost URI if it's from a Layer 1 follow (direct friend)
            if (int.interactionUri && layer1Dids.has(int.actor) && !networkEffortMap[int.target].repostUri) {
                networkEffortMap[int.target].repostUri = int.interactionUri
            }
        }
        networkEffortMap[int.target].actors.add(int.actor)
    })

    // Advanced Reply Analysis
    // Group replies by their root post to identify conversation clusters
    const replyClusters: Record<string, Array<{ post: any, author: string, isLayer1: boolean, isLayer2: boolean, isInteracted: boolean, isMutual: boolean }>> = {}
    const replyToRootMap: Record<string, string> = {}

    // Self-reply chain detection
    const selfReplyChains: Record<string, { author: string, replyCount: number, chainDepth: number }> = {}

    uniquePosts.forEach(post => {
        if (post.replyRoot) {
            replyToRootMap[post.uri] = post.replyRoot
            if (!replyClusters[post.replyRoot]) {
                replyClusters[post.replyRoot] = []
            }
            replyClusters[post.replyRoot].push({
                post,
                author: post.author,
                isLayer1: layer1Dids.has(post.author),
                isLayer2: layer2Dids.has(post.author),
                isInteracted: interactedDids.has(post.author),
                isMutual: mutualDids.has(post.author)
            })

            // Track self-reply chains
            if (!selfReplyChains[post.replyRoot]) {
                selfReplyChains[post.replyRoot] = { author: post.author, replyCount: 0, chainDepth: 0 }
            }

            // Check if this is a self-reply (same author as any previous reply in this thread)
            const cluster = replyClusters[post.replyRoot]
            const hasSelfReply = cluster.some(r => r.author === post.author && r.post.uri !== post.uri)

            if (hasSelfReply || post.author === selfReplyChains[post.replyRoot].author) {
                selfReplyChains[post.replyRoot].replyCount++
                selfReplyChains[post.replyRoot].author = post.author
                // Calculate chain depth (how many consecutive self-replies)
                const sortedReplies = cluster
                    .filter(r => r.author === post.author)
                    .sort((a, b) => new Date(a.post.indexedAt).getTime() - new Date(b.post.indexedAt).getTime())

                let maxDepth = 0
                let currentDepth = 0
                for (let i = 0; i < sortedReplies.length; i++) {
                    if (i === 0 || sortedReplies[i].post.replyParent === sortedReplies[i - 1].post.uri) {
                        currentDepth++
                    } else {
                        currentDepth = 1
                    }
                    maxDepth = Math.max(maxDepth, currentDepth)
                }
                selfReplyChains[post.replyRoot].chainDepth = maxDepth
            }
        }
    })

    // Identify which reply clusters have multiple social graph participants
    const multiPersonReplies = new Set<string>()
    const opBoostMap: Record<string, number> = {} // Track OP boost amounts

    Object.entries(replyClusters).forEach(([rootPost, replies]) => {
        const socialGraphReplies = replies.filter(r => r.isLayer1 || r.isLayer2 || r.isInteracted)
        if (socialGraphReplies.length >= 2) {
            multiPersonReplies.add(rootPost)

            // Calculate OP boost based on social graph engagement
            const layer1Replies = socialGraphReplies.filter(r => r.isLayer1).length
            const layer2Replies = socialGraphReplies.filter(r => r.isLayer2).length
            const mutualReplies = socialGraphReplies.filter(r => r.isMutual).length

            // Boost the OP for generating social graph conversation
            let opBoost = 0
            opBoost += layer1Replies * 150  // Each Layer 1 reply = +150 to OP
            opBoost += layer2Replies * 75   // Each Layer 2 reply = +75 to OP  
            opBoost += mutualReplies * 200  // Each mutual reply = +200 to OP

            // Additional boost for high-engagement conversations
            if (socialGraphReplies.length >= 3) {
                opBoost += 300 // Conversation starter bonus
            }
            if (socialGraphReplies.length >= 5) {
                opBoost += 500 // Popular conversation bonus
            }

            opBoostMap[rootPost] = opBoost
        }
    })

    console.log(`[Reply Analysis] Found ${Object.keys(replyClusters).length} conversation clusters, ${multiPersonReplies.size} with multi-person social graph participation`)
    console.log(`[OP Boost] Calculated boosts for ${Object.keys(opBoostMap).length} original posts`)
    console.log(`[Self-Reply Chains] Found ${Object.keys(selfReplyChains).length} potential self-reply chains`)
    console.log(`[User Interactions] Found ${userInteractions.length} user interactions (${userInteractions.filter(ui => ui.type === 'like').length} likes, ${userInteractions.filter(ui => ui.type === 'repost').length} reposts, ${userInteractions.filter(ui => ui.type === 'reply').length} replies)`)
    console.log(`[InteractionSeen Fatigue] Found ${seenPosts.length} recently seen posts for ${requesterDid.slice(0, 10)}...`)

    // Load user keywords for interest boosting
    const userKeywords = await ctx.db
        .selectFrom('user_keyword')
        .select(['keyword', 'score'])
        .where('userDid', '=', requesterDid)
        .execute()

    const keywordMap = new Map<string, number>()
    userKeywords.forEach(kw => keywordMap.set(kw.keyword.toLowerCase(), kw.score))
    console.log(`[Interest Keywords] Loaded ${keywordMap.size} keywords for user`)

    // 2.6. Taste Similarity Analysis - MOVED UP
    // (Previously here)

    // 1.9. User Author Fatigue Tracking
    // Fetch current author fatigue data for the user
    const authorFatigueData = await ctx.db
        .selectFrom('user_author_fatigue')
        .selectAll()
        .where('userDid', '=', requesterDid)
        .execute()

    const authorFatigueMap: Record<string, {
        serveCount: number
        fatigueScore: number
        affinityScore: number
        interactionWeight: number
        lastServedAt: string
        lastInteractionAt: string | null
        interactionCount: number
    }> = {}

    authorFatigueData.forEach(af => {
        authorFatigueMap[af.authorDid] = {
            serveCount: af.serveCount,
            fatigueScore: af.fatigueScore,
            affinityScore: af.affinityScore,
            interactionWeight: af.interactionWeight,
            lastServedAt: af.lastServedAt,
            lastInteractionAt: af.lastInteractionAt,
            interactionCount: af.interactionCount
        }
    })

    console.log(`[Author Fatigue] Loaded fatigue data for ${authorFatigueData.length} authors`)

    // Update reputation based on user's recent interactions with recommended posts
    // This is a simplified version - in production you'd want more sophisticated tracking
    const recentUserInteractions = await ctx.db
        .selectFrom('graph_interaction')
        .select(['target', 'type', 'indexedAt'])
        .where('actor', '=', requesterDid)
        .where('indexedAt', '>', new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString()) // Last 24 hours
        .execute()

    for (const interaction of recentUserInteractions) {
        const tasteData = tasteSimilarPostMap.get(interaction.target)
        if (tasteData) {
            // User interacted with a post recommended by taste-similar users
            const action = interaction.type === 'like' ? 'served_liked' : 'served_ignored'
            for (const similarUserDid of tasteData.similarUserDids) {
                await updateTasteReputation(ctx, requesterDid, similarUserDid, action)
            }
        }
    }

    // Scoring
    const scoredPosts = uniquePosts.map((post) => {
        let score = 0
        const signals: Record<string, number> = {}
        const ageInHours = (Date.now() - new Date(post.indexedAt).getTime()) / (1000 * 60 * 60)

        // Base recency decay (24h half-life)
        const recencyMultiplier = Math.pow(0.5, ageInHours / 24)
        const recencyBase = 10 * recencyMultiplier
        score += recencyBase
        signals['recency'] = Math.round(recencyBase)

        // Slower Tier Decay (336h / 2-week half-life)
        const tierDecay = Math.pow(0.5, ageInHours / 336)

        const isLayer1 = layer1Dids.has(post.author)
        const isLayer2 = layer2Dids.has(post.author)
        const isInteracted = interactedDids.has(post.author)
        const isMutual = mutualDids.has(post.author)

        const authorAffinity = authorFatigueMap[post.author]?.affinityScore || 1.0

        if (isLayer1) {
            const mutualBoost = isMutual ? 2.5 : 1.0
            const l1Boost = 3000 * tierDecay * mutualBoost * (0.8 + authorAffinity * 0.2) // Scale by affinity
            score += l1Boost
            signals['layer1'] = Math.round(l1Boost)
            if (authorAffinity > 1.2) signals['affinity_boost'] = Math.round(l1Boost * (authorAffinity - 1) * 0.2 / (0.8 + authorAffinity * 0.2))
        } else if (isInteracted) {
            const interactedBoost = 1500 * tierDecay * (0.8 + authorAffinity * 0.2)
            score += interactedBoost
            signals['interacted'] = Math.round(interactedBoost)
        } else if (isLayer2) {
            const l2Boost = 500 * tierDecay * (0.9 + authorAffinity * 0.1)
            score += l2Boost
            signals['layer2'] = Math.round(l2Boost)
        } else {
            const coldBoost = 50 * tierDecay
            score += coldBoost
            signals['cold_score'] = Math.round(coldBoost)
        }

        const networkInteractions = networkEffortMap[post.uri]
        if (networkInteractions) {
            const count = networkInteractions.likes + networkInteractions.reposts
            const effortBoost = Math.round(Math.pow(count, 1.5) * 200)
            score += effortBoost
            signals['network_effort'] = effortBoost
        }

        const engagementBoost = (post.likeCount || 0) * 15 + (post.repostCount || 0) * 30
        score += engagementBoost
        signals['engagement'] = engagementBoost

        // DISCOVERY ENGINE: Keywords and Taste Similarity
        const isOutsideSocialGraph = !isLayer1 && !isLayer2 && !isInteracted
        const tasteData = tasteSimilarPostMap.get(post.uri)
        let hasKeywordMatch = false
        let keywordBoost = 0
        let tasteBoost = 0
        let discoveryMatch = false

        // 1. Taste Similarity Boost
        if (tasteData) {
            // Significant boost to overcome sandbox penalty for "sole reason" of taste agreement
            tasteBoost = tasteData.boostScore * 2500 // Increased from 1500
            const consensusMultiplier = Math.min(4.0, 1 + (tasteData.similarUserDids.length - 1) * 0.8) // More aggressive consensus boost
            tasteBoost *= consensusMultiplier
            discoveryMatch = true
        }

        // 2. Keyword Boost (Whole Word Matching only to prevent false positives like "post" in "posters")
        if (post.text && keywordMap.size > 0) {
            const textLower = post.text.toLowerCase()
            for (const [keyword, kwWeight] of keywordMap.entries()) {
                // Use regex for whole-word boundary check
                const regex = new RegExp(`\\b${keyword.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&')}\\b`, 'i')
                if (regex.test(textLower)) {
                    hasKeywordMatch = true
                    discoveryMatch = true
                    // Multiplier: increased for discovery punch-through
                    const multiplier = isOutsideSocialGraph ? (batchMode ? 800 : 1200) : 100
                    keywordBoost += kwWeight * multiplier
                }
            }
        }

        // 3. Apply Boosts and Discovery Sandboxing
        if (isOutsideSocialGraph) {
            // SANDBOX PENALTY: Always apply to "Cold" posts to ensure they need real merit to pass
            const viralSafety = (post.likeCount || 0) > 50
            const basePenalty = viralSafety ? -1500 : (batchMode ? -2000 : -4000) // Slightly reduced penalty for viral stuff
            score += basePenalty
            signals['sandbox_penalty'] = basePenalty

            // MEDIA PREFERENCE PENALTY: Penalize if post doesn't match user's media preference
            const hasImage = post.hasImage
            const hasVideo = post.hasVideo
            const imageMismatch = hasImage && imageRatio < 0.2
            const videoMismatch = hasVideo && videoRatio < 0.2
            const mediaMismatch = imageMismatch || videoMismatch
            if (mediaMismatch) {
                const mediaPenalty = -1500
                score += mediaPenalty
                signals['media_mismatch'] = mediaPenalty
            }

            // Apply Discovery Boosts on top of the penalty
            if (hasKeywordMatch) {
                score += keywordBoost
                signals['keyword_discovery'] = Math.round(keywordBoost)
            }
            if (tasteData) {
                score += tasteBoost
                signals['taste_discovery'] = Math.round(tasteBoost)
            }
        } else {
            // Inside social graph - apply standard boosts
            score += keywordBoost + tasteBoost
            if (keywordBoost !== 0) signals['keyword_boost'] = Math.round(keywordBoost)
            if (tasteBoost > 0) signals['taste_boost'] = Math.round(tasteBoost)
        }

        // OP Boost: Give original posts a small boost to encourage thread starts
        if (!post.replyParent && score > 0) {
            const opBoost = Math.min(300, Math.round(score * 0.1)) // Max 300, or 10% of score
            score += opBoost
            signals['op_boost'] = opBoost
            // Only log significant OP boosts (> 1000)
            if (opBoost > 1000) {
                console.log(`[OP Boost] Applied +${opBoost} boost to OP: ${post.uri.slice(-10)}`)
            }
        }

        // Nested Conversation Boost: If this post is a reply but also generated conversation, give smaller boost
        if (post.replyParent && opBoostMap[post.uri]) {
            const nestedBoost = Math.round(opBoostMap[post.uri] * 0.3)
            score += nestedBoost
            signals['nested_convo_boost'] = nestedBoost
            console.log(`[Nested Boost] Applied +${nestedBoost} boost to nested conversation: ${post.uri.slice(-10)}`)
        }

        // Advanced Reply Scoring System
        if (post.replyParent) {
            const replyRoot = post.replyRoot || post.replyParent
            const isInMultiPersonConversation = multiPersonReplies.has(replyRoot)
            const replyCluster = replyClusters[replyRoot] || []
            const socialGraphRepliesInCluster = replyCluster.filter(r => r.isLayer1 || r.isLayer2 || r.isInteracted)

            // Base reply penalty - replies start lower than original posts
            const baseReplyPenalty = -800
            score += baseReplyPenalty
            signals['reply_base_penalty'] = baseReplyPenalty

            // Boost 1: Mutuals get significant boost for their replies
            if (isMutual) {
                const mutualReplyBoost = 600
                score += mutualReplyBoost
                signals['reply_mutual_boost'] = mutualReplyBoost
            }

            // Boost 2: Popular replies (high engagement) get boost
            const replyEngagement = (post.likeCount || 0) + (post.repostCount || 0) * 2
            if (replyEngagement >= 5) {
                score += 300
                signals['reply_popularity_high'] = 300
            } else if (replyEngagement >= 2) {
                score += 100
                signals['reply_popularity_med'] = 100
            }

            // Boost 3: Replies from people in your social graph
            if (isLayer1) {
                score += 400
                signals['reply_l1_boost'] = 400
            } else if (isInteracted) {
                score += 200
                signals['reply_interacted_boost'] = 200
            } else if (isLayer2) {
                score += 100
                signals['reply_l2_boost'] = 100
            }

            // Penalty 1: Heavy penalty for replies in multi-person conversations (to reduce repetition)
            if (isInMultiPersonConversation && socialGraphRepliesInCluster.length >= 2) {
                // If this is one of many replies from your social graph to the same thread
                const mySocialGraphReplies = socialGraphRepliesInCluster.filter(r => r.author === post.author)
                if (mySocialGraphReplies.length > 0) {
                    score -= 400
                    signals['reply_convo_repetition_penalty'] = -400
                }

                // Additional penalty based on how many people from your graph replied to this thread
                const conversationPenalty = -Math.min(socialGraphRepliesInCluster.length * 100, 500)
                score += conversationPenalty
                signals['reply_convo_crowd_penalty'] = conversationPenalty
            }

            // Penalty 2: Replies to very old posts get extra penalty (less relevant)
            const parentPostAge = uniquePosts.find(p => p.uri === post.replyParent)
            if (parentPostAge) {
                const parentAgeInHours = (Date.now() - new Date(parentPostAge.indexedAt).getTime()) / (1000 * 60 * 60)
                if (parentAgeInHours > 24) {
                    const oldReplyPenalty = -Math.min(parentAgeInHours * 5, 300)
                    score += oldReplyPenalty
                    signals['reply_old_parent_penalty'] = Math.round(oldReplyPenalty)
                }
            }

            // Bonus: Replies that generate network interaction get boost
            const replyNetworkInteractions = networkEffortMap[post.uri]
            if (replyNetworkInteractions && replyNetworkInteractions.actors.size > 0) {
                const networkReplyBoost = replyNetworkInteractions.actors.size * 50
                score += networkReplyBoost
                signals['reply_network_boost'] = networkReplyBoost
            }
        }

        const hasAnyEngagement = (post.likeCount || 0) > 0 || (post.repostCount || 0) > 0 || networkInteractions
        if (!hasAnyEngagement && ageInHours < 1) {
            score -= 500
            signals['ghost_penalty'] = -500
        }
        if (!post.replyParent && !isLayer1 && !isLayer2 && !isInteracted && !networkInteractions && ageInHours > 24) {
            score -= 1000
            signals['cold_unknown_penalty'] = -1000
        }

        // User Interaction Penalty (Already liked/reposted/replied) - STRONGER
        const userInteraction = userInteractionMap[post.uri]
        if (userInteraction) {
            // HEAVIER penalties for already interacted content
            if (userInteraction === 'like') {
                score -= 8000
                signals['status_already_liked'] = -8000
            } else if (userInteraction === 'repost') {
                score -= 6000
                signals['status_already_reposted'] = -6000
            } else if (userInteraction === 'reply') {
                score -= 5000
                signals['status_already_replied'] = -5000
            }
        }

        // InteractionSeen Fatigue Penalty - Permanent -50% multiplier per view
        const seenCount = seenCountMap[post.uri] || 0
        const lastSeenAt = seenTimeMap[post.uri]
        let fatiguePenalty = 0

        if (seenCount > 0 && lastSeenAt && !batchMode) {
            // Apply permanent -50% multiplier per view
            const seenMultiplier = Math.pow(0.5, seenCount) // 0.5^seenCount
            score *= seenMultiplier
            fatiguePenalty = Math.round(score * (1 - seenMultiplier)) // Track penalty amount
            signals['seen_fatigue_multiplier'] = Math.round(seenMultiplier * 1000) / 1000
            signals['seen_fatigue_penalty'] = fatiguePenalty

            // Log significant fatigue penalties
            if (seenCount >= 2) {
                console.log(`[InteractionSeen Fatigue] Applied ${Math.round(seenMultiplier * 100)}% score (${seenCount} views): ${post.uri.slice(-10)}`)
            }
        }

        // Ensure posts can NEVER fully recover from over-serving penalties
        const authorFatigue = authorFatigueMap[post.author]
        if (authorFatigue) {
            let authorFatiguePenalty = 0

            // Anti-fatigue bonus for negative scores (authors you engage with frequently)
            if (authorFatigue.fatigueScore < 0) {
                authorFatiguePenalty = Math.round(Math.abs(authorFatigue.fatigueScore) * 50) // Bonus points
            }
            // Base fatigue penalty based on fatigue score (0-100 scale)
            else if (authorFatigue.fatigueScore > 40) { // Increased threshold from 20 to 40
                // Linear penalty instead of exponential to be "a lot less aggressive"
                authorFatiguePenalty = -Math.round((authorFatigue.fatigueScore - 30) * 80)

                // Extra penalty if no recent interaction with this author
                const hoursSinceLastInteraction = authorFatigue.lastInteractionAt
                    ? (Date.now() - new Date(authorFatigue.lastInteractionAt).getTime()) / (1000 * 60 * 60)
                    : 999 // Very large number if no interaction

                if (hoursSinceLastInteraction > 72) { // No interaction in 3 days
                    authorFatiguePenalty *= 1.5
                } else if (hoursSinceLastInteraction > 24) { // No interaction in 1 day
                    authorFatiguePenalty *= 1.2
                }

                // Reduce penalty if author has high engagement on this specific post
                const postEngagement = (post.likeCount || 0) + (post.repostCount || 0) * 2
                if (postEngagement >= 10) {
                    authorFatiguePenalty *= 0.3 // High engagement reduces penalty by 70%
                } else if (postEngagement >= 5) {
                    authorFatiguePenalty *= 0.5 // Medium engagement reduces penalty by 50%
                } else if (postEngagement >= 2) {
                    authorFatiguePenalty *= 0.7 // Low engagement reduces penalty by 30%
                }

                authorFatiguePenalty = Math.round(authorFatiguePenalty)
                score += authorFatiguePenalty
                signals['author_fatigue'] = authorFatiguePenalty
                // Silenced log spam: only log if it's truly massive (> 20000)
                if (Math.abs(authorFatiguePenalty) > 20000) {
                    console.log(`[Author Fatigue] ${authorFatiguePenalty} penalty for author ${post.author.slice(-10)} (fatigue: ${authorFatigue.fatigueScore.toFixed(1)})`)
                }
            }
        }

        // Self-Reply Chain Penalty
        if (post.replyRoot && selfReplyChains[post.replyRoot]) {
            const chainData = selfReplyChains[post.replyRoot]

            // Only penalize if this author is the one creating the self-reply chain
            if (chainData.author === post.author) {
                let selfReplyPenalty = 0

                // Penalty based on chain depth (consecutive self-replies)
                if (chainData.chainDepth >= 3) {
                    selfReplyPenalty = -2000 // Heavy penalty for deep chains
                } else if (chainData.chainDepth >= 2) {
                    selfReplyPenalty = -1000 // Moderate penalty for short chains
                }

                // Additional penalty based on total self-reply count in thread
                if (chainData.replyCount >= 5) {
                    selfReplyPenalty -= 1000 // Extra penalty for spammy threads
                } else if (chainData.replyCount >= 3) {
                    selfReplyPenalty -= 500 // Moderate extra penalty
                }

                // Reduce penalty for high engagement self-replies (they might be valuable updates)
                const postEngagement = (post.likeCount || 0) + (post.repostCount || 0) * 2
                if (postEngagement >= 5) {
                    selfReplyPenalty *= 0.5 // Reduce penalty for engaging self-replies
                } else if (postEngagement >= 2) {
                    selfReplyPenalty *= 0.7 // Slightly reduce penalty
                }

                if (selfReplyPenalty !== 0) {
                    selfReplyPenalty = Math.round(selfReplyPenalty)
                    score += selfReplyPenalty
                    signals['self_reply_chain_penalty'] = selfReplyPenalty
                    // Only log extreme self-reply penalties (> 2500)
                    if (Math.abs(selfReplyPenalty) > 2500) {
                        console.log(`[Self-Reply Chain] ${selfReplyPenalty} penalty for self-reply (depth: ${chainData.chainDepth}): ${post.uri.slice(-10)}`)
                    }
                }
            }
        }

        // Permanent scar system removed - replaced with -50% multiplier per view above

        // Deterministic jitter for variety - seed it with User DID to ensure unique order per user
        const salt = post.uri + requesterDid

        // JITTER NERF: If this is an outside post with no keyword or taste match, slash the jitter range
        // This prevents "unlucky" users from seeing random global content just because it got a high roll.
        let jitterRange = 1200
        if (isOutsideSocialGraph && !discoveryMatch) {
            jitterRange = 300 // 75% reduction in "luck" potential
        }

        const jitter = (salt.split('').reduce((a, c) => ((a << 5) - a + c.charCodeAt(0)) | 0, 0) % jitterRange + jitterRange) % jitterRange
        score += jitter
        signals['jitter'] = jitter

        return { post, score: Math.round(score), signals, repostUri: networkInteractions?.repostUri }
    })

    // Filter and Dedup with Advanced Reply Logic
    const filtered = scoredPosts.filter(sp => {
        // HARD FILTER: Completely remove posts the user has already liked
        const userInteraction = userInteractionMap[sp.post.uri]
        if (userInteraction === 'like') {
            return false // Never show liked posts again
        }

        // HARD FILTER: Completely remove posts with 0 engagement that have been seen 3+ times
        const seenCount = seenCountMap[sp.post.uri] || 0
        const hasZeroEngagement = (sp.post.likeCount || 0) === 0 && (sp.post.repostCount || 0) === 0
        if (hasZeroEngagement && seenCount >= 3) {
            // Only log extreme zero engagement removals (> 15 serves)
            if (seenCount >= 15) {
                console.log(`[Zero Engagement Filter] Removing post seen ${seenCount} times with 0 engagement: ${sp.post.uri.slice(-10)}`)
            }
            return false // Never show zero-engagement posts after 3 views
        }

        // Always allow original posts through standard criteria
        // Relaxed from -100 to -5000: We want the diversity/dedup logic to handle truncation, not a hard score floor.
        if (!sp.post.replyParent) {
            return sp.score > -5000
        }

        // Advanced reply filtering
        const replyRoot = sp.post.replyRoot || sp.post.replyParent
        const isInMultiPersonConversation = multiPersonReplies.has(replyRoot)
        const replyCluster = replyClusters[replyRoot] || []
        const socialGraphRepliesInCluster = replyCluster.filter(r => r.isLayer1 || r.isLayer2 || r.isInteracted)

        const isLayer1 = layer1Dids.has(sp.post.author)
        const isLayer2 = layer2Dids.has(sp.post.author)
        const isInteracted = interactedDids.has(sp.post.author)
        const isMutual = mutualDids.has(sp.post.author)

        // Rule 1: Always allow replies from mutuals if they have decent engagement
        if (isMutual && (sp.post.likeCount || 0) + (sp.post.repostCount || 0) >= 1) {
            return sp.score > -200
        }

        // Rule 2: Allow replies from Layer 1 if they're popular or from important conversations
        if (isLayer1) {
            const replyEngagement = (sp.post.likeCount || 0) + (sp.post.repostCount || 0) * 2
            if (replyEngagement >= 3) {
                return sp.score > -150 // Popular replies from Layer 1
            }
        }

        // Rule 3: Heavy filtering for replies in multi-person conversations to prevent repetition
        if (isInMultiPersonConversation && socialGraphRepliesInCluster.length >= 3) {
            // Only allow the highest-scoring reply from each conversation cluster
            const clusterReplies = scoredPosts.filter(s =>
                s.post.replyRoot === replyRoot || s.post.replyParent === replyRoot
            )
            const highestScoringReply = clusterReplies.reduce((max, current) =>
                current.score > max.score ? current : max
            )
            return sp.post.uri === highestScoringReply.post.uri && sp.score > -100
        }

        // Rule 4: Standard reply filtering for other cases
        if (isLayer1 || isLayer2 || isInteracted) {
            return sp.score > -2000
        }

        // Rule 5: Default case - require higher score for unknown replies
        return sp.score > -1000
    })

    // Advanced Thread Deduplication with Reply Intelligence
    const threadCounts: Record<string, number> = {}
    const conversationCounts: Record<string, number> = {} // Track conversation clusters
    const finalPool: Array<{ post: any; score: number; signals?: any; repostUri?: string }> = []

    // Sort by score first
    const sortedCandidatePool = filtered.sort((a, b) => b.score - a.score)

    for (const sp of sortedCandidatePool) {
        const threadKey = sp.post.replyRoot || sp.post.uri
        const conversationKey = sp.post.replyRoot || 'no_conversation'

        threadCounts[threadKey] = (threadCounts[threadKey] || 0) + 1
        conversationCounts[conversationKey] = (conversationCounts[conversationKey] || 0) + 1

        // Rule 1: Allow up to 2 posts from the same thread for original posts
        if (!sp.post.replyParent && threadCounts[threadKey] <= 2) {
            finalPool.push(sp)
        }

        // Rule 2: For replies, be much more restrictive to prevent conversation flooding
        else if (sp.post.replyParent) {
            const isLayer1 = layer1Dids.has(sp.post.author)
            const isLayer2 = layer2Dids.has(sp.post.author)
            const isInteracted = interactedDids.has(sp.post.author)
            const isMutual = mutualDids.has(sp.post.author)

            // Allow replies from mutuals more liberally
            if (isMutual && conversationCounts[conversationKey] <= 3) {
                finalPool.push(sp)
            }
            // Allow very popular replies from Layer 1
            else if (isLayer1 && (sp.post.likeCount || 0) + (sp.post.repostCount || 0) >= 5 && conversationCounts[conversationKey] <= 2) {
                finalPool.push(sp)
            }
            // Allow one high-quality reply per conversation from others in social graph
            else if ((isLayer1 || isLayer2 || isInteracted) && conversationCounts[conversationKey] <= 1 && sp.score > 100) {
                finalPool.push(sp)
            }
            // Rarely allow replies from unknown people unless they're exceptional
            else if (!isLayer1 && !isLayer2 && !isInteracted && sp.score > 500 && conversationCounts[conversationKey] <= 1) {
                finalPool.push(sp)
            }
        }
    }

    // Deterministic sort for stable pagination: score desc, then indexedAt desc, then uri
    finalPool.sort((a, b) => {
        if (b.score !== a.score) return b.score - a.score
        const tA = new Date(a.post.indexedAt).getTime()
        const tB = new Date(b.post.indexedAt).getTime()
        if (tB !== tA) return tB - tA
        return a.post.uri.localeCompare(b.post.uri)
    })

    // --- BATCH MODE: Return the full scored pool with post data for embedding ---
    if (batchMode) {
        console.log(`[Batch Mode] Returning ${finalPool.length} scored candidates (no fatigue/diversity applied)`)
        return {
            feed: finalPool.map((p) => {
                const item: any = { post: p.post.uri }
                if (p.repostUri && !layer1Dids.has(p.post.author)) {
                    item.reason = {
                        $type: 'app.bsky.feed.defs#skeletonReasonRepost',
                        repost: p.repostUri
                    }
                }
                return item
            }),
            cursor: undefined,
            // Attach full post data + scores for the batch pipeline to use
            _batchData: finalPool.map((p) => ({
                uri: p.post.uri,
                text: p.post.text,
                author: p.post.author,
                indexedAt: p.post.indexedAt,
                likeCount: p.post.likeCount,
                repostCount: p.post.repostCount,
                hasImage: p.post.hasImage,
                hasVideo: p.post.hasVideo,
                score: p.score,
                signals: p.signals,
            })),
        }
    }

    // --- SERVE MODE: Normal feed serving with diversity and pagination ---

    // Apply account diversity: prevent same author from appearing consecutively
    const diversifiedPool = applyAccountDiversity(finalPool)

    const limit = Math.min(params.limit || 30, 100)
    let slicePool = diversifiedPool

    // Cursor-based pagination (opaque: score::timestamp::uri) — next page = items that sort after cursor
    if (params.cursor) {
        const parts = params.cursor.split('::')
        if (parts.length >= 3) {
            const cursorScore = Number(parts[0])
            const cursorTime = Number(parts[1])
            const cursorUri = parts.slice(2).join('::')
            slicePool = diversifiedPool.filter((sp) => {
                const score = sp.score
                const t = new Date(sp.post.indexedAt).getTime()
                if (score < cursorScore) return true
                if (score > cursorScore) return false
                if (t < cursorTime) return true
                if (t > cursorTime) return false
                return sp.post.uri.localeCompare(cursorUri) < 0
            })
        }
    }

    const page = slicePool.slice(0, limit)

    // Save debug logs for posts that made it to the final feed (as requested)
    if (page.length > 0) {
        const debugEntries = page.map(p => ({
            userDid: requesterDid,
            uri: p.post.uri,
            score: p.score,
            signals: JSON.stringify(p.signals || {}),
            servedAt: now,
        }))

        // Debug logging disabled to reduce WAL growth
        // await ctx.db
        //     .insertInto('feed_debug_log')
        //     .values(debugEntries)
        //     .execute()
        //     .catch(err => console.error(`[Debug Log Error] ${err}`))
    }

    const feed = page.map((p) => {
        const item: any = { post: p.post.uri }
        if (p.repostUri && !layer1Dids.has(p.post.author)) {
            item.reason = {
                $type: 'app.bsky.feed.defs#skeletonReasonRepost',
                repost: p.repostUri
            }
        }
        return item
    })
    const last = page[page.length - 1]
    const cursor =
        last && page.length === limit
            ? `${last.score}::${new Date(last.post.indexedAt).getTime()}::${last.post.uri}`
            : undefined

    // Log final feed composition for monitoring
    const replyCount = page.filter(p => p.post.replyParent).length
    const originalCount = page.filter(p => !p.post.replyParent).length
    const previouslySeenCount = page.filter(p => seenCountMap[p.post.uri] > 0).length
    const filteredOutCount = scoredPosts.length - filtered.length
    console.log(`[Feed Composition] Final feed: ${page.length} items (${originalCount} original, ${replyCount} replies, ${previouslySeenCount} previously seen)`)
    console.log(`[Filtering] Removed ${filteredOutCount} posts (${userInteractions.filter(ui => ui.type === 'like').length} liked posts filtered out, ${previouslySeenCount} seen posts penalized)`)

    return {
        feed,
        cursor,
    }
}

// Update author fatigue when posts are served to users
export async function updateAuthorFatigueOnServe(
    ctx: AppContext | { db: Database },
    userDid: string,
    authorDid: string,
    postUri: string
) {
    const now = new Date().toISOString()

    // Use upsert to handle race conditions
    const existing = await ctx.db
        .selectFrom('user_author_fatigue')
        .selectAll()
        .where('userDid', '=', userDid)
        .where('authorDid', '=', authorDid)
        .executeTakeFirst()

    if (existing) {
        // Update existing record
        const newServeCount = existing.serveCount + 1
        const hoursSinceLastServe = (Date.now() - new Date(existing.lastServedAt).getTime()) / (1000 * 60 * 60)

        // Calculate new fatigue score, affinity and weight
        let newFatigueScore = existing.fatigueScore
        let newAffinityScore = existing.affinityScore
        let newInteractionWeight = existing.interactionWeight

        // Passive decay on serve (seeing an author without interacting slightly cools the connection)
        newAffinityScore -= 0.05
        newInteractionWeight *= 0.98

        // Increase fatigue based on serve count
        if (newServeCount <= 3) {
            newFatigueScore += 3 // Reduced: very gentle increase
        } else if (newServeCount <= 6) {
            newFatigueScore += 5 // Reduced
        } else {
            newFatigueScore += 8 // Reduced: max increment is much lower
        }

        // Apply time-based decay (fatigue slowly recovers over time)
        if (hoursSinceLastServe > 48) { // 2 days
            newFatigueScore *= 0.7 // 30% recovery
            newAffinityScore *= 0.95 // slight cooling over long absence
        } else if (hoursSinceLastServe > 24) { // 1 day
            newFatigueScore *= 0.85 // 15% recovery
        }

        // Cap scores
        newFatigueScore = Math.min(100, Math.max(-100, newFatigueScore))
        newAffinityScore = Math.min(10, Math.max(0.1, newAffinityScore))

        await ctx.db
            .updateTable('user_author_fatigue')
            .set({
                serveCount: newServeCount,
                lastServedAt: now,
                fatigueScore: newFatigueScore,
                affinityScore: newAffinityScore,
                interactionWeight: newInteractionWeight,
                updatedAt: now
            })
            .where('userDid', '=', userDid)
            .where('authorDid', '=', authorDid)
            .execute()

        console.log(`[Author Fatigue Update] Served: ${authorDid.slice(-10)} to ${userDid.slice(-10)} (serves: ${newServeCount}, fatigue: ${newFatigueScore.toFixed(1)})`)
    } else {
        // Create new fatigue record — use ON CONFLICT to handle concurrent inserts safely
        await ctx.db
            .insertInto('user_author_fatigue')
            .values({
                userDid,
                authorDid,
                serveCount: 1,
                lastServedAt: now,
                fatigueScore: 10, // Starting fatigue score
                affinityScore: 1.0, // Starting base affinity
                interactionWeight: 0.1, // Initial weight
                lastInteractionAt: null,
                interactionCount: 0,
                updatedAt: now
            })
            .onConflict((oc) => oc
                .columns(['userDid', 'authorDid'])
                .doUpdateSet((eb) => ({
                    serveCount: eb('excluded.serveCount', '+', eb.ref('user_author_fatigue.serveCount')),
                    lastServedAt: eb.ref('excluded.lastServedAt'),
                    fatigueScore: eb('user_author_fatigue.fatigueScore', '+', 10),
                    updatedAt: eb.ref('excluded.updatedAt'),
                }))
            )
            .execute()

        console.log(`[Author Fatigue Create] New record: ${authorDid.slice(-10)} for ${userDid.slice(-10)}`)
    }
}

// Update author fatigue when user interacts with an author
export async function updateAuthorFatigueOnInteraction(
    ctx: AppContext | { db: Database },
    userDid: string,
    postUri: string,
    interactionType: 'like' | 'repost' | 'reply'
) {
    // Extract author DID from post URI
    const parts = postUri.replace('at://', '').split('/')
    if (parts[0]?.startsWith('did:')) {
        const authorDid = parts[0]
        const now = new Date().toISOString()

        // Check if fatigue record exists
        const existing = await ctx.db
            .selectFrom('user_author_fatigue')
            .selectAll()
            .where('userDid', '=', userDid)
            .where('authorDid', '=', authorDid)
            .executeTakeFirst()

        if (existing) {
            // Update existing record - interaction reduces fatigue and boosts affinity
            const newInteractionCount = existing.interactionCount + 1
            let newFatigueScore = existing.fatigueScore
            let newAffinityScore = existing.affinityScore
            let newInteractionWeight = existing.interactionWeight

            // Reduce fatigue and boost affinity based on interaction type
            if (interactionType === 'like') {
                newFatigueScore -= 25
                newAffinityScore += 0.8
                newInteractionWeight += 1.0
            } else if (interactionType === 'repost') {
                newFatigueScore -= 30
                newAffinityScore += 1.2
                newInteractionWeight += 2.0
            } else if (interactionType === 'reply') {
                newFatigueScore -= 20
                newAffinityScore += 0.5
                newInteractionWeight += 0.5
            }

            // Extra reduction if this user hasn't interacted with this author recently
            const hoursSinceLastInteraction = existing.lastInteractionAt
                ? (Date.now() - new Date(existing.lastInteractionAt).getTime()) / (1000 * 60 * 60)
                : 999

            if (hoursSinceLastInteraction > 72) { // First interaction in 3+ days
                newFatigueScore -= 15 // Bonus fatigue reduction
                newAffinityScore += 0.5 // Bonus affinity
            }

            // Cap scores
            newFatigueScore = Math.min(100, Math.max(-100, newFatigueScore))
            newAffinityScore = Math.min(10, Math.max(0.1, newAffinityScore))

            await ctx.db
                .updateTable('user_author_fatigue')
                .set({
                    interactionCount: newInteractionCount,
                    lastInteractionAt: now,
                    fatigueScore: newFatigueScore,
                    affinityScore: newAffinityScore,
                    interactionWeight: newInteractionWeight,
                    updatedAt: now
                })
                .where('userDid', '=', userDid)
                .where('authorDid', '=', authorDid)
                .execute()

            console.log(`[Author Fatigue Recovery] ${interactionType}: ${authorDid.slice(-10)} by ${userDid.slice(-10)} (fatigue: ${existing.fatigueScore.toFixed(1)} → ${newFatigueScore.toFixed(1)})`)
        } else {
            // Create new fatigue record with positive interaction — use ON CONFLICT to handle concurrent inserts safely
            const fatigueReduction = interactionType === 'like' ? 25 : interactionType === 'repost' ? 30 : 20

            await ctx.db
                .insertInto('user_author_fatigue')
                .values({
                    userDid,
                    authorDid,
                    serveCount: 0,
                    lastServedAt: now,
                    fatigueScore: 0, // Start with no fatigue since user interacted positively
                    affinityScore: 2.0, // Interaction boosts initial affinity
                    interactionWeight: 1.5, // Initial weight for active author
                    lastInteractionAt: now,
                    interactionCount: 1,
                    updatedAt: now
                })
                .onConflict((oc) => oc
                    .columns(['userDid', 'authorDid'])
                    .doUpdateSet((eb) => ({
                        interactionCount: eb('user_author_fatigue.interactionCount', '+', 1),
                        lastInteractionAt: eb.ref('excluded.lastInteractionAt'),
                        fatigueScore: eb('user_author_fatigue.fatigueScore', '-', fatigueReduction),
                        affinityScore: eb('user_author_fatigue.affinityScore', '+', 0.5),
                        interactionWeight: eb('user_author_fatigue.interactionWeight', '+', 1.0),
                        updatedAt: eb.ref('excluded.updatedAt'),
                    }))
                )
                .execute()

            console.log(`[Author Fatigue Create] New record with interaction: ${authorDid.slice(-10)} for ${userDid.slice(-10)}`)
        }
    }
}

// Update affinity when a post is seen but not interacted with (Passive decay)
export async function updateAffinityOnSeen(
    ctx: AppContext | { db: Database },
    userDid: string,
    postUri: string
) {
    const parts = postUri.replace('at://', '').split('/')
    if (parts[0]?.startsWith('did:')) {
        const authorDid = parts[0]

        await ctx.db
            .updateTable('user_author_fatigue')
            .set((eb) => ({
                affinityScore: eb('affinityScore', '-', 0.02), // Very small decay per seen post
                updatedAt: new Date().toISOString()
            }))
            .where('userDid', '=', userDid)
            .where('authorDid', '=', authorDid)
            .where('affinityScore', '>', 0.1)
            .execute()
    }
}

// Handle explicit \"Show More/Less Like This\" feedback
export async function handleInteractionFeedback(
    ctx: AppContext,
    userDid: string,
    postUri: string,
    type: 'more' | 'less',
    strength: 'strong' | 'weak' = 'strong'
) {
    const now = new Date().toISOString()

    // 1. Get post content and author
    const post = await ctx.db
        .selectFrom('post')
        .selectAll()
        .where('uri', '=', postUri)
        .executeTakeFirst()

    if (!post) return

    // Define magnitudes based on strength
    // Symmetrical for 'strong' (Buttons), lighter for 'weak' (Likes/dislikes)
    const affinityAdj = strength === 'strong' ? 5.0 : 1.0
    const fatigueAdj = strength === 'strong' ? 60 : 20
    const keywordAdj = strength === 'strong' ? 0.7 : 0.2

    // 2. Adjust Author Affinity
    const finalAffinityAdj = type === 'more' ? affinityAdj : -affinityAdj
    const finalFatigueAdj = type === 'more' ? -fatigueAdj : fatigueAdj

    console.log(`[Feedback] Adjusting author ${post.author} (Strength: ${strength}): affinity ${finalAffinityAdj >= 0 ? '+' : ''}${finalAffinityAdj}, fatigue ${finalFatigueAdj >= 0 ? '+' : ''}${finalFatigueAdj}`)

    await ctx.db
        .updateTable('user_author_fatigue')
        .set((eb) => ({
            affinityScore: eb('affinityScore', '+', finalAffinityAdj),
            fatigueScore: eb('fatigueScore', '+', finalFatigueAdj),
            updatedAt: now
        }))
        .where('userDid', '=', userDid)
        .where('authorDid', '=', post.author)
        .execute()

    // 3. Adjust Keyword Scores (-1.0 to 1.0 range)
    if (post.text) {
        const words = post.text.toLowerCase().split(/\s+/).filter(w => w.length > 4)
        const adjustment = type === 'more' ? keywordAdj : -keywordAdj
        console.log(`[Feedback] Adjusting ${words.length} keywords by ${adjustment}`)

        const restrictedKeywords = new Set(['adult', 'porn', 'nsfw', 'pornography', 'xxx', 'hentai', 'furry'])

        for (const word of words) {
            const cleanWord = word.replace(/[^\w]/g, '')
            if (cleanWord.length < 4 || restrictedKeywords.has(cleanWord)) continue

            const existing = await ctx.db
                .selectFrom('user_keyword')
                .select(['score'])
                .where('userDid', '=', userDid)
                .where('keyword', '=', cleanWord)
                .executeTakeFirst()

            if (existing) {
                const newScore = Math.min(1.0, Math.max(-1.0, existing.score + adjustment))
                await ctx.db
                    .updateTable('user_keyword')
                    .set({ score: newScore, updatedAt: now })
                    .where('userDid', '=', userDid)
                    .where('keyword', '=', cleanWord)
                    .execute()
            } else {
                // Add new keywords for both positive and negative feedback
                await ctx.db
                    .insertInto('user_keyword')
                    .values({ userDid, keyword: cleanWord, score: adjustment, updatedAt: now })
                    .execute()
            }
        }
    }

    // 4. Adjust Taste Reputation
    // Find who liked this post and adjust their reputation in this user's eyes
    // ALWAYS fetch from API (cap 50) to discover new "taste twins" regardless of social graph
    const { GraphBuilder } = await import('../services/graph-builder')
    const graphBuilder = new GraphBuilder(ctx.db)
    let likerDids = await graphBuilder.getPostLikers(postUri, 50)

    if (likerDids.length === 0) {
        // Fallback to indexed likers if API fails or returns nothing
        const similarUsers = await ctx.db
            .selectFrom('graph_interaction')
            .select('actor')
            .where('target', '=', postUri)
            .where('type', '=', 'like')
            .execute()
        likerDids = similarUsers.map(sim => sim.actor)
    }

    const reputationAction = strength === 'strong'
        ? (type === 'more' ? 'explicit_more' : 'explicit_less')
        : (type === 'more' ? 'served_liked' : 'served_ignored')

    console.log(`[Interaction Feedback] Updating reputation for ${likerDids.length} users (Action: ${reputationAction}, Strength: ${strength})`)

    for (const did of likerDids) {
        // Don't update reputation for the user themselves
        if (did === userDid) continue
        await updateTasteReputation(ctx, userDid, did, reputationAction)
    }

    console.log(`[Interaction Feedback] ${strength.toUpperCase()} ${type.toUpperCase()} processed for ${postUri.slice(-10)}`)
}
