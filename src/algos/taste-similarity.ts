import { AppContext } from '../config'
import { Database } from '../db'
import { AtpAgent } from '@atproto/api'
import { logger } from '../logger'

const publicAgent = new AtpAgent({ service: 'https://public.api.bsky.app' })

// Track taste similarity between users based on co-liked posts
export async function updateTasteSimilarity(
  ctx: AppContext | { db: Database },
  userDid: string,
  postUri: string,
  interactionType: 'like' | 'dislike' | 'ignore'
) {
  if (interactionType !== 'like') return // Only track likes for similarity

  const now = new Date().toISOString()

  // Find other users who also liked this post
  const coLikers = await ctx.db
    .selectFrom('graph_interaction')
    .select('actor')
    .where('target', '=', postUri)
    .where('type', '=', 'like')
    .where('actor', '!=', userDid)
    .execute()

  for (const coLiker of coLikers) {
    const similarUserDid = coLiker.actor

    // Use upsert to handle race conditions
    try {
      await ctx.db
        .insertInto('taste_similarity')
        .values({
          userDid,
          similarUserDid,
          agreementCount: 1,
          totalCoLikedPosts: 1,
          lastAgreementAt: now,
          updatedAt: now
        })
        .onConflict((oc) => oc
          .columns(['userDid', 'similarUserDid'])
          .doUpdateSet({
            agreementCount: (eb) => eb('taste_similarity.agreementCount', '+', 1),
            totalCoLikedPosts: (eb) => eb('taste_similarity.totalCoLikedPosts', '+', 1),
            lastAgreementAt: now,
            updatedAt: now
          })
        )
        .execute()
    } catch (err: any) {
      if (err.code === 'SQLITE_CONSTRAINT_PRIMARYKEY' || err.code === 'SQLITE_CONSTRAINT_UNIQUE') {
        // Record was updated by another process, which is fine
        console.log(`[Taste Similarity] Race condition handled for ${userDid.slice(0, 10)} and ${similarUserDid.slice(0, 10)}`)
      } else {
        logger.error(`[Taste Similarity] Error updating similarity for ${userDid.slice(0, 10)}:`, err)
      }
    }

    // Update reputation score
    await updateTasteReputation(ctx, userDid, similarUserDid, 'agreement')
  }

  // EXTERNAL DISCOVERY: Find co-likers from the wider network via API
  // This allows us to find "Taste Twins" we aren't currently tracking
  try {
    const res = await publicAgent.getLikes({ uri: postUri, limit: 100 })
    if (res.success) {
      const externalDids = res.data.likes
        .map(l => l.actor.did)
        .filter(did => did !== userDid)

      for (const extDid of externalDids) {
        // Just directly update/create reputation for these users
        // This makes them eligible for firehose tracking in the next refresh
        await updateTasteReputation(ctx, userDid, extDid, 'agreement')
      }

      if (externalDids.length > 0) {
        console.log(`[Taste Discovery] Found ${externalDids.length} external co-likers for discovery on ${postUri.slice(-10)}`)
      }
    }
  } catch (err) {
    // Silently fail API discovery if rate limited or network error
    logger.error(`[Taste Discovery] API discovery failed for ${postUri.slice(-10)}`)
  }
}

// Update reputation scores based on continued agreement/disagreement
export async function updateTasteReputation(
  ctx: AppContext | { db: Database },
  userDid: string,
  similarUserDid: string,
  action: 'agreement' | 'disagreement' | 'served_ignored' | 'served_liked' | 'explicit_more' | 'explicit_less'
) {
  const now = new Date().toISOString()

  let existing = await ctx.db
    .selectFrom('taste_reputation')
    .selectAll()
    .where('userDid', '=', userDid)
    .where('similarUserDid', '=', similarUserDid)
    .executeTakeFirst()

  if (!existing) {
    // Create new reputation record with a try-catch for race conditions
    try {
      const initialScore = action === 'agreement' ? 1.2 : 0.8
      await ctx.db
        .insertInto('taste_reputation')
        .values({
          userDid,
          similarUserDid,
          reputationScore: initialScore,
          agreementHistory: action === 'agreement' ? 1 : (action === 'disagreement' ? -1 : 0),
          lastSeenAt: now,
          decayRate: 0.95, // Default decay rate
          updatedAt: now
        })
        .execute()
      return
    } catch (err: any) {
      if (err.code === 'SQLITE_CONSTRAINT_PRIMARYKEY' || err.code === 'SQLITE_CONSTRAINT_UNIQUE') {
        // Someone else inserted it, just continue to update
        console.log(`[Taste Reputation] Race condition during insert, falling back to update for ${userDid.slice(0, 10)}`)
        // Re-fetch to get the record for update
        const reFetched = await ctx.db
          .selectFrom('taste_reputation')
          .selectAll()
          .where('userDid', '=', userDid)
          .where('similarUserDid', '=', similarUserDid)
          .executeTakeFirst()
        if (!reFetched) return // Shoudn't happen

        // continue below with reFetched logic
        // We'll jump to the update part by re-setting variables
        existing = reFetched as any
      } else {
        throw err
      }
    }
  }

  // Double check existing again because we might have re-fetched in catch
  if (!existing) return

  let newScore = existing.reputationScore
  let newHistory = existing.agreementHistory
  let newDecayRate = existing.decayRate

  // Calculate time since last update
  const hoursSinceUpdate = (Date.now() - new Date(existing.updatedAt).getTime()) / (1000 * 60 * 60)

  // Apply decay based on time passed
  const decayMultiplier = Math.pow(existing.decayRate, hoursSinceUpdate / 24) // Decay per day
  newScore *= decayMultiplier

  // Update based on action
  switch (action) {
    case 'agreement':
      newScore = Math.min(3.0, newScore * 1.15) // Boost reputation, max 3.0
      newHistory += 1
      newDecayRate = Math.min(0.99, newDecayRate * 1.02) // Slow down decay (preserve more)
      break
    case 'disagreement':
      newScore = Math.max(0.1, newScore * 0.85) // Reduce reputation, min 0.1
      newHistory -= 1
      newDecayRate = Math.max(0.80, newDecayRate * 0.95) // Speed up decay (forget faster)
      break
    case 'explicit_more':
      // Very strong boost for explicit interest
      newScore = Math.min(5.0, newScore * 1.6) // Higher boost for explicit "Move more"
      newHistory += 3.0
      newDecayRate = Math.min(0.999, newDecayRate * 1.1) // Significantly slow decay for "taste twins"
      break
    case 'explicit_less':
      // Very strong penalty for bad recommendations (the inverse of requestMore)
      newScore = Math.max(0.001, newScore * 0.1) // HEAVY cut for punishing likers of bad content
      newHistory -= 5.0
      newDecayRate = Math.max(0.5, newDecayRate * 0.5) // Forget them EXTREMELY quickly
      break
    case 'served_liked':
      newScore = Math.min(3.0, newScore * 1.05) // Small boost for successful recommendations
      newHistory += 0.5
      newDecayRate = Math.min(0.99, newDecayRate * 1.01)
      break
    case 'served_ignored':
      newScore = Math.max(0.1, newScore * 0.95) // Small penalty for ignored recommendations
      newHistory -= 0.25
      newDecayRate = Math.max(0.85, newDecayRate * 0.99)
      break
  }

  await ctx.db
    .updateTable('taste_reputation')
    .set({
      reputationScore: newScore,
      agreementHistory: newHistory,
      lastSeenAt: now,
      decayRate: newDecayRate,
      updatedAt: now
    })
    .where('userDid', '=', userDid)
    .where('similarUserDid', '=', similarUserDid)
    .execute()
}

// Get taste-similar users with their reputation scores
export async function getTasteSimilarUsers(
  ctx: AppContext | { db: Database },
  userDid: string,
  limit: number = 50
): Promise<Array<{ userDid: string; reputationScore: number; agreementCount: number }>> {
  
  // 1. Fetch user's recent likes for activity decay
  const recentLikes = await ctx.db
    .selectFrom('graph_interaction')
    .select('indexedAt')
    .where('actor', '=', userDid)
    .where('type', '=', 'like')
    .orderBy('indexedAt', 'desc')
    .limit(500)
    .execute()

  // 2. Fetch all twins
  const results = await ctx.db
    .selectFrom('taste_reputation')
    .leftJoin('taste_similarity', (join) =>
      join.onRef('taste_reputation.similarUserDid', '=', 'taste_similarity.similarUserDid')
        .on('taste_similarity.userDid', '=', userDid)
    )
    .select([
      'taste_reputation.similarUserDid as userDid',
      'taste_reputation.reputationScore',
      'taste_reputation.updatedAt',
      'taste_reputation.decayRate',
      'taste_similarity.agreementCount'
    ])
    .where('taste_reputation.userDid', '=', userDid)
    .execute()

  // 3. Calculate dynamic decay
  const decayedTwins: Array<{ userDid: string; reputationScore: number; agreementCount: number }> = []

  for (const r of results) {
    const hoursSinceUpdate = (Date.now() - new Date(r.updatedAt).getTime()) / (1000 * 60 * 60)
    let decayedScore = r.reputationScore

    if (r.reputationScore > 1.0) {
      // Positive Twin: Activity decay + Relaxed Time decay
      const likesSinceUpdate = recentLikes.filter(l => new Date(l.indexedAt).getTime() > new Date(r.updatedAt).getTime()).length
      const activityMultiplier = Math.pow(0.95, likesSinceUpdate / 50) // More lenient: 5% per 50 likes
      const timeMultiplier = Math.pow(0.98, hoursSinceUpdate / 24)
      
      const distance = r.reputationScore - 1.0
      const newDistance = distance * activityMultiplier * timeMultiplier
      decayedScore = 1.0 + newDistance
    } else if (r.reputationScore < 1.0) {
      // Negative Twin: Time decay only, -5% per day
      const timeMultiplier = Math.pow(0.95, hoursSinceUpdate / 24)
      
      const distance = 1.0 - r.reputationScore
      const newDistance = distance * timeMultiplier
      decayedScore = 1.0 - newDistance
    }

    decayedTwins.push({
      userDid: r.userDid,
      reputationScore: decayedScore,
      agreementCount: r.agreementCount ?? 1
    })
  }

  // 4. Separate positive and negative twins
  const positiveTwins = decayedTwins.filter(t => t.reputationScore > 1.1)
    .sort((a, b) => b.reputationScore - a.reputationScore)
    .slice(0, limit)

  const negativeTwins = decayedTwins.filter(t => t.reputationScore < 0.9)
    .sort((a, b) => a.reputationScore - b.reputationScore) // Lowest first
    .slice(0, limit)

  return [...positiveTwins, ...negativeTwins]
}

// Get posts liked by taste-similar users
export async function getPostsLikedBySimilarUsers(
  ctx: AppContext | { db: Database },
  userDid: string,
  similarUsers: Array<{ userDid: string; reputationScore: number }>,
  timeLimitHours: number = 72
): Promise<Array<{ postUri: string; boostScore: number; similarUserDids: string[] }>> {
  if (similarUsers.length === 0) return []

  const timeLimit = new Date(Date.now() - timeLimitHours * 60 * 60 * 1000).toISOString()
  const similarUserDids = similarUsers.map(u => u.userDid)

  // Get posts liked by similar users in the time window
  const likedPosts = await ctx.db
    .selectFrom('graph_interaction')
    .select(['target', 'actor'])
    .where('type', '=', 'like')
    .where('actor', 'in', similarUserDids)
    .where('indexedAt', '>', timeLimit)
    .execute()

  // Group posts by URI and calculate boost scores
  const postMap: Record<string, { similarUserDids: string[]; totalScore: number }> = {}

  for (const like of likedPosts) {
    const postUri = like.target
    const similarUser = similarUsers.find(u => u.userDid === like.actor)

    if (!similarUser) continue

    if (!postMap[postUri]) {
      postMap[postUri] = { similarUserDids: [], totalScore: 0 }
    }

    postMap[postUri].similarUserDids.push(like.actor)
    // Center the reputation score so that < 1.0 becomes a penalty, and > 1.0 becomes a boost
    postMap[postUri].totalScore += (similarUser.reputationScore - 1.0)
  }

  // Convert to array and sort by boost score (both positive and negative)
  return Object.entries(postMap)
    .map(([postUri, data]) => ({
      postUri,
      boostScore: data.totalScore,
      similarUserDids: data.similarUserDids
    }))
    .sort((a, b) => b.boostScore - a.boostScore)
}

// Clean up old taste similarity data (call this periodically)
export async function cleanupOldTasteData(ctx: AppContext | { db: Database }, daysToKeep: number = 90) {
  const cutoffDate = new Date(Date.now() - daysToKeep * 24 * 60 * 60 * 1000).toISOString()

  // Delete old similarity records with low agreement counts
  await ctx.db
    .deleteFrom('taste_similarity')
    .where('updatedAt', '<', cutoffDate)
    .where('agreementCount', '<', 3)
    .execute()

  // For reputation records, instead of doing expensive math in memory for millions of rows
  // which blocks the Node.js event loop and locks the database, we rely on the fact that
  // time decay mathematically destroys ALL scores after ~120 days. 
  // Positive twins (1% decay) drop by 70%, negative twins (5% decay) drop by 99%.
  // Therefore, we can safely and instantly delete ALL records older than 120 days.
  const hardCutoffDate = new Date(Date.now() - 120 * 24 * 60 * 60 * 1000).toISOString()
  
  await ctx.db
    .deleteFrom('taste_reputation')
    .where('updatedAt', '<', hardCutoffDate)
    .execute()
}
