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

    // 1.8. Serving Fatigue (Memory) + User Interaction Tracking
    // Fetch URIs served to this user in the last 6 hours for better fatigue tracking
    const fatigueLookback = new Date(Date.now() - 6 * 60 * 60 * 1000).toISOString()
    const servedPosts = await ctx.db
        .selectFrom('user_served_post')
        .select(['uri', 'servedAt'])
        .where('userDid', '=', requesterDid)
        .where('servedAt', '>', fatigueLookback)
        .execute()

    // Fetch posts the user has already interacted with (likes, reposts, replies)
    const userInteractions = await ctx.db
        .selectFrom('graph_interaction')
        .select(['target', 'type'])
        .where('actor', '=', requesterDid)
        .where('type', 'in', ['like', 'repost', 'reply'])
        .execute()

    const servedCountMap: Record<string, number> = {}
    const servedTimeMap: Record<string, string> = {}
    const userInteractionMap: Record<string, string> = {} // Track interaction type
    
    servedPosts.forEach(sp => {
        servedCountMap[sp.uri] = (servedCountMap[sp.uri] || 0) + 1
        servedTimeMap[sp.uri] = sp.servedAt
    })
    
    userInteractions.forEach(ui => {
        userInteractionMap[ui.target] = ui.type
    })

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
            eb('likeCount', '>', 2)
        ]))
        .limit(400)
        .execute()

    // Apply multi-factor scoring with jitter
    const seed = Date.now() % 1000
    const randomFactor1 = (seed * 9301 + 49297) % 233280 / 233280 // 0-1
    const randomFactor2 = (seed * 233280 + 9301) % 49297 / 49297 // 0-1  
    const randomFactor3 = (seed * 49297 + 233280) % 9301 / 9301 // 0-1

    const bucket1 = bucket1Raw
        .map(post => {
            const ageInHours = (Date.now() - new Date(post.indexedAt).getTime()) / (1000 * 60 * 60)
            const likes = post.likeCount || 0
            const likeVelocity = ageInHours > 0 ? likes / ageInHours : likes // likes per hour
            
            // Three scoring factors
            const likeScore = likes * (0.8 + randomFactor1 * 0.4) // 0.8-1.2x
            const timeScore = (1000 / (ageInHours + 1)) * (0.8 + randomFactor2 * 0.4) // 0.8-1.2x
            const velocityScore = likeVelocity * 50 * (0.8 + randomFactor3 * 0.4) // 0.8-1.2x
            
            return { post, score: likeScore + timeScore + velocityScore }
        })
        .sort((a, b) => b.score - a.score)
        .slice(0, 400)
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
            eb('likeCount', '>', 1)
        ]))
        .limit(200)
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
            
            // Balanced scoring for bridge content
            const likeScore = likes * (0.9 + randomFactor1_5_1 * 0.4) // 0.9-1.3x
            const timeScore = (700 / (ageInHours + 1)) * (0.7 + randomFactor1_5_2 * 0.4) // 0.7-1.1x
            const velocityScore = likeVelocity * 35 * (0.7 + randomFactor1_5_3 * 0.4) // 0.7-1.1x
            
            return { post, score: likeScore + timeScore + velocityScore }
        })
        .sort((a, b) => b.score - a.score)
        .slice(0, 200)
        .map(item => item.post)

    // bucket 2: Global Gems (Last 30 days, any post with high engagement)
    const bucket2Raw = await ctx.db
        .selectFrom('post')
        .selectAll()
        .where('indexedAt', '>', lookback30d)
        .where('likeCount', '>', 1)
        .limit(400)
        .execute()

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
            
            // Different weight balance for global gems (more emphasis on likes)
            const likeScore = likes * (1.2 + randomFactor4 * 0.6) // 1.2-1.8x
            const timeScore = (500 / (ageInHours + 1)) * (0.6 + randomFactor5 * 0.4) // 0.6-1.0x
            const velocityScore = likeVelocity * 30 * (0.6 + randomFactor6 * 0.4) // 0.6-1.0x
            
            return { post, score: likeScore + timeScore + velocityScore }
        })
        .sort((a, b) => b.score - a.score)
        .slice(0, 400)
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
        .limit(400)
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
            
            // Balanced weights for bubble highlights
            const likeScore = likes * (1.0 + randomFactor7 * 0.4) // 1.0-1.4x
            const timeScore = (800 / (ageInHours + 1)) * (0.8 + randomFactor8 * 0.4) // 0.8-1.2x
            const velocityScore = likeVelocity * 40 * (0.8 + randomFactor9 * 0.4) // 0.8-1.2x
            
            return { post, score: likeScore + timeScore + velocityScore }
        })
        .sort((a, b) => b.score - a.score)
        .slice(0, 400)
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

    // Advanced Reply Analysis
    // Group replies by their root post to identify conversation clusters
    const replyClusters: Record<string, Array<{ post: any, author: string, isLayer1: boolean, isLayer2: boolean, isInteracted: boolean, isMutual: boolean }>> = {}
    const replyToRootMap: Record<string, string> = {}
    
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
    console.log(`[User Interactions] Found ${userInteractions.length} user interactions (${userInteractions.filter(ui => ui.type === 'like').length} likes, ${userInteractions.filter(ui => ui.type === 'repost').length} reposts, ${userInteractions.filter(ui => ui.type === 'reply').length} replies)`)
    console.log(`[Serving Fatigue] Found ${servedPosts.length} recently served posts for ${requesterDid.slice(0, 10)}...`)

    // Scoring
    const scoredPosts = uniquePosts.map((post) => {
        let score = 0
        const ageInHours = (Date.now() - new Date(post.indexedAt).getTime()) / (1000 * 60 * 60)

        // Base recency decay (24h half-life)
        const recencyMultiplier = Math.pow(0.5, ageInHours / 24)
        score += 10 * recencyMultiplier

        // Slower Tier Decay (336h / 2-week half-life)
        const tierDecay = Math.pow(0.5, ageInHours / 336)

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

        // OP Boost: If this post is an original post that generated social graph conversation, boost it
        if (!post.replyParent && opBoostMap[post.uri]) {
            score += opBoostMap[post.uri]
            console.log(`[OP Boost] Applied +${opBoostMap[post.uri]} boost to OP: ${post.uri.slice(-10)}`)
        }
        
        // Nested Conversation Boost: If this post is a reply but also generated conversation, give smaller boost
        if (post.replyParent && opBoostMap[post.uri]) {
            score += Math.round(opBoostMap[post.uri] * 0.3) // 30% of OP boost for nested conversations
            console.log(`[Nested Boost] Applied +${Math.round(opBoostMap[post.uri] * 0.3)} boost to nested conversation: ${post.uri.slice(-10)}`)
        }

        // Advanced Reply Scoring System
        if (post.replyParent) {
            const isReply = true
            const replyRoot = post.replyRoot || post.replyParent
            const isInMultiPersonConversation = multiPersonReplies.has(replyRoot)
            const replyCluster = replyClusters[replyRoot] || []
            const socialGraphRepliesInCluster = replyCluster.filter(r => r.isLayer1 || r.isLayer2 || r.isInteracted)
            
            // Base reply penalty - replies start lower than original posts
            score -= 800
            
            // Boost 1: Mutuals get significant boost for their replies
            if (isMutual) {
                score += 600 // Strong boost for mutual replies
            }
            
            // Boost 2: Popular replies (high engagement) get boost
            const replyEngagement = (post.likeCount || 0) + (post.repostCount || 0) * 2
            if (replyEngagement >= 5) {
                score += 300 // Popular reply boost
            } else if (replyEngagement >= 2) {
                score += 100 // Moderately popular reply boost
            }
            
            // Boost 3: Replies from people in your social graph
            if (isLayer1) {
                score += 400 // Strong boost for Layer 1 replies
            } else if (isInteracted) {
                score += 200 // Medium boost for interacted users
            } else if (isLayer2) {
                score += 100 // Small boost for Layer 2 replies
            }
            
            // Penalty 1: Heavy penalty for replies in multi-person conversations (to reduce repetition)
            if (isInMultiPersonConversation && socialGraphRepliesInCluster.length >= 2) {
                // If this is one of many replies from your social graph to the same thread
                const mySocialGraphReplies = socialGraphRepliesInCluster.filter(r => r.author === post.author)
                if (mySocialGraphReplies.length > 0) {
                    score -= 400 // Heavy penalty to prevent seeing same topic repeatedly
                }
                
                // Additional penalty based on how many people from your graph replied to this thread
                const conversationPenalty = Math.min(socialGraphRepliesInCluster.length * 100, 500)
                score -= conversationPenalty
            }
            
            // Penalty 2: Replies to very old posts get extra penalty (less relevant)
            const parentPostAge = uniquePosts.find(p => p.uri === post.replyParent)
            if (parentPostAge) {
                const parentAgeInHours = (Date.now() - new Date(parentPostAge.indexedAt).getTime()) / (1000 * 60 * 60)
                if (parentAgeInHours > 24) {
                    score -= Math.min(parentAgeInHours * 5, 300) // Increasing penalty for replying to old content
                }
            }
            
            // Bonus: Replies that generate network interaction get boost
            const replyNetworkInteractions = networkEffortMap[post.uri]
            if (replyNetworkInteractions && replyNetworkInteractions.actors.size > 0) {
                score += replyNetworkInteractions.actors.size * 50
            }
        }

        const hasAnyEngagement = (post.likeCount || 0) > 0 || (post.repostCount || 0) > 0 || networkInteractions
        if (!hasAnyEngagement && ageInHours < 1) score -= 200 // Reduced penalty
        if (!post.replyParent && !isLayer1 && !isLayer2 && !isInteracted && !networkInteractions && ageInHours > 24) score -= 500 // Only penalize older unknown content

        // User Interaction Penalty (Already liked/reposted/replied)
        const userInteraction = userInteractionMap[post.uri]
        if (userInteraction) {
            // Heavy penalties for already interacted content
            if (userInteraction === 'like') {
                score -= 5000 // Heaviest penalty for liked posts
            } else if (userInteraction === 'repost') {
                score -= 4000 // Heavy penalty for reposted posts  
            } else if (userInteraction === 'reply') {
                score -= 3000 // Moderate penalty for replied posts (might want to see replies to your reply)
            }
        }

        // Serving Fatigue Penalty (Progressive decay)
        const seenCount = servedCountMap[post.uri] || 0
        const lastServedAt = servedTimeMap[post.uri]
        let fatiguePenalty = 0
        
        if (seenCount > 0 && lastServedAt) {
            const hoursSinceLastServe = (Date.now() - new Date(lastServedAt).getTime()) / (1000 * 60 * 60)
            
            // Progressive penalty: 1st serve=0, 2nd=-200, 3rd=-400, 4th=-800, 5th=-1600
            if (seenCount >= 2) {
                fatiguePenalty = -200 * Math.pow(2, seenCount - 2)
            }
            
            // Time-based recovery: penalty reduces over time
            const recoveryFactor = Math.max(0.1, 1 - (hoursSinceLastServe / 6)) // Full recovery after 6 hours
            fatiguePenalty = Math.round(fatiguePenalty * recoveryFactor)
            
            // Extra penalty for very recent re-serves (within 30 minutes)
            if (hoursSinceLastServe < 0.5) {
                fatiguePenalty -= 300
            }
        }
        
        if (fatiguePenalty !== 0) {
            score += fatiguePenalty
        }

        // Deterministic jitter for variety while keeping stable order for pagination
        const jitter = (post.uri.split('').reduce((a, c) => ((a << 5) - a + c.charCodeAt(0)) | 0, 0) % 1200 + 1200) % 1200
        score += jitter

        return { post, score: Math.round(score) }
    })

    // Filter and Dedup with Advanced Reply Logic
    const filtered = scoredPosts.filter(sp => {
        // HARD FILTER: Completely remove posts the user has already liked
        const userInteraction = userInteractionMap[sp.post.uri]
        if (userInteraction === 'like') {
            return false // Never show liked posts again
        }
        
        // Always allow original posts through standard criteria
        if (!sp.post.replyParent) {
            return sp.score > -100
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
            return sp.score > -50
        }
        
        // Rule 5: Default case - require higher score for unknown replies
        return sp.score > 200
    })

    // Advanced Thread Deduplication with Reply Intelligence
    const threadCounts: Record<string, number> = {}
    const conversationCounts: Record<string, number> = {} // Track conversation clusters
    const finalPool: Array<{ post: any; score: number }> = []

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

    const limit = Math.min(params.limit || 30, 100)
    let slicePool = finalPool

    // Cursor-based pagination (opaque: score::timestamp::uri) — next page = items that sort after cursor
    if (params.cursor) {
        const parts = params.cursor.split('::')
        if (parts.length >= 3) {
            const cursorScore = Number(parts[0])
            const cursorTime = Number(parts[1])
            const cursorUri = parts.slice(2).join('::')
            slicePool = finalPool.filter((sp) => {
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
    const feed = page.map((p) => ({ post: p.post.uri }))
    const last = page[page.length - 1]
    const cursor =
        last && page.length === limit
            ? `${last.score}::${new Date(last.post.indexedAt).getTime()}::${last.post.uri}`
            : undefined

    // Log final feed composition for monitoring
    const replyCount = page.filter(p => p.post.replyParent).length
    const originalCount = page.filter(p => !p.post.replyParent).length
    const previouslyServedCount = page.filter(p => servedCountMap[p.post.uri] > 0).length
    const filteredOutCount = scoredPosts.length - filtered.length
    console.log(`[Feed Composition] Final feed: ${page.length} items (${originalCount} original, ${replyCount} replies, ${previouslyServedCount} previously served)`)
    console.log(`[Filtering] Removed ${filteredOutCount} posts (${userInteractions.filter(ui => ui.type === 'like').length} liked posts filtered out)`)

    return {
        feed,
        cursor,
    }
}
