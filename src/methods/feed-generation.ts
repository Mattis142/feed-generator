import { InvalidRequestError, AuthRequiredError } from '@atproto/xrpc-server'
import { Server } from '../lexicon'
import { AppContext } from '../config'
import algos from '../algos'
import { validateAuth } from '../auth'
import { AtUri } from '@atproto/syntax'
import { GraphBuilder } from '../services/graph-builder'
import { updateAuthorFatigueOnServe } from '../algos/social-graph'

export default function (server: Server, ctx: AppContext) {
  server.app.bsky.feed.getFeedSkeleton(async ({ params, req }) => {
    console.log(`Received getFeedSkeleton request for ${params.feed} from ${req.headers['x-forwarded-for'] || req.socket.remoteAddress}`)
    try {
      const feedUri = new AtUri(params.feed)
      const algo = algos[feedUri.rkey]
      if (
        feedUri.hostname !== ctx.cfg.publisherDid ||
        feedUri.collection !== 'app.bsky.feed.generator' ||
        !algo
      ) {
        throw new InvalidRequestError(
          'Unsupported algorithm',
          'UnsupportedAlgorithm',
        )
      }

      const requesterDid = await validateAuth(
        req,
        ctx.cfg.serviceDid,
        ctx.didResolver,
      )
      console.log(`Authenticated requester: ${requesterDid}`)

      // Check whitelist
      if (ctx.cfg.whitelist.length > 0 && !ctx.cfg.whitelist.includes(requesterDid)) {
        console.log(`Access denied for ${requesterDid}`)
        throw new InvalidRequestError(
          'This feed is restricted to whitelisted users only.',
          'AccountRestricted',
        )
      }

      // Trigger background graph build
      const graphBuilder = new GraphBuilder(ctx.db)
      graphBuilder.buildUserGraph(requesterDid).catch((err) => {
        console.error(`Background graph build failed for ${requesterDid}`, err)
      })

      // --- Semantic Batch Serve Mode ---
      // Try to serve from pre-computed candidate batches first.
      // Falls back to running the full pipeline if no batches exist (cold start).
      const body = await serveFromBatchesOrFallback(ctx, algo, params, requesterDid)

      // Record served posts for fatigue memory (background)
      const servedUris = body.feed.map(f => f.post)
      if (servedUris.length > 0) {
        // Record post-level serving
        ctx.db.insertInto('user_served_post')
          .values(servedUris.map(uri => ({
            userDid: requesterDid,
            uri,
            servedAt: new Date().toISOString(),
          })))
          .execute()
          .catch(err => console.error('Failed to record served posts', err))

        // Record author-level fatigue tracking (background)
        servedUris.forEach(async (uri) => {
          try {
            const parts = uri.replace('at://', '').split('/')
            if (parts[0]?.startsWith('did:')) {
              const authorDid = parts[0]
              await updateAuthorFatigueOnServe(ctx, requesterDid, authorDid, uri)
            }
          } catch (err) {
            console.error('Failed to update author fatigue on serve:', err)
          }
        })
      }

      console.log(`Sucessfully generated feed for ${requesterDid} with ${body.feed.length} items`)
      return {
        encoding: 'application/json',
        body: body,
      }
    } catch (err) {
      console.error(`Error in getFeedSkeleton for ${params.feed}:`, err)
      throw err
    }
  })
}

/**
 * Attempt to serve from pre-computed semantic batches.
 * If no non-expired batches exist for the user, falls back to the full pipeline.
 */
async function serveFromBatchesOrFallback(
  ctx: AppContext,
  algo: (ctx: AppContext, params: any, requesterDid: string) => Promise<any>,
  params: any,
  requesterDid: string,
): Promise<{ feed: Array<{ post: string }>; cursor?: string }> {
  const extractAuthor = (uri: string): string => {
    const parts = uri.replace('at://', '').split('/')
    return parts[0] || ''
  }

  const now = Date.now()
  const BATCH_TTL_HOURS = 12

  // Load all non-expired candidate batch rows for this user
  const cutoff = new Date(now - BATCH_TTL_HOURS * 60 * 60 * 1000).toISOString()
  const batchRows = await ctx.db
    .selectFrom('user_candidate_batch')
    .selectAll()
    .where('userDid', '=', requesterDid)
    .where('generatedAt', '>', cutoff)
    .execute()

  // Cold start fallback: no batches → run full pipeline synchronously
  if (batchRows.length === 0) {
    console.log(`[Semantic Serve] No batches for ${requesterDid.slice(0, 15)}..., falling back to full pipeline`)
    return algo(ctx, params, requesterDid)
  }

  // --- Deduplicate and calculate scores ---
  const uniqueBatchRowsMap: Record<string, typeof batchRows[0]> = {}
  for (const row of batchRows) {
    if (!uniqueBatchRowsMap[row.uri] || row.generatedAt > uniqueBatchRowsMap[row.uri].generatedAt) {
      uniqueBatchRowsMap[row.uri] = row
    }
  }

  const decayed = Object.values(uniqueBatchRowsMap).map((row) => {
    const batchAgeHours = (now - new Date(row.generatedAt).getTime()) / (1000 * 60 * 60)
    const impactMultiplier = Math.max(0, 1 - (batchAgeHours / BATCH_TTL_HOURS))

    // Combined score: pipelineScore weight + semantic score weight with decay
    // Semantic matches are valuable, but must not overwhelm the social graph (scaled down from 5000)
    const effectiveScore = row.pipelineScore * 0.3 + row.semanticScore * 1800 * impactMultiplier

    return {
      uri: row.uri,
      effectiveScore,
      semanticScore: row.semanticScore,
      pipelineScore: row.pipelineScore,
      centroidId: row.centroidId,
      batchId: row.batchId,
      impactMultiplier,
    }
  })

  // --- Real-time filters ---

  // 1. Filter already-liked posts
  const likedPosts = await ctx.db
    .selectFrom('graph_interaction')
    .select('target')
    .where('actor', '=', requesterDid)
    .where('type', 'in', ['like', 'repost', 'reply'])
    .execute()
  const likedUris = new Set(likedPosts.map(r => r.target))

  // 2. InteractionSeen fatigue: posts seen recently get penalized
  const fatigueLookback = new Date(now - 7 * 24 * 60 * 60 * 1000).toISOString()
  const seenPosts = await ctx.db
    .selectFrom('user_seen_post')
    .select(['uri', 'seenAt'])
    .where('userDid', '=', requesterDid)
    .where('seenAt', '>', fatigueLookback)
    .execute()

  const seenCountMap: Record<string, number> = {}
  seenPosts.forEach(sp => {
    seenCountMap[sp.uri] = (seenCountMap[sp.uri] || 0) + 1
  })

  // 3. Author fatigue data
  const authorFatigueData = await ctx.db
    .selectFrom('user_author_fatigue')
    .selectAll()
    .where('userDid', '=', requesterDid)
    .execute()

  const authorFatigueMap: Record<string, number> = {}
  authorFatigueData.forEach(af => {
    authorFatigueMap[af.authorDid] = af.fatigueScore
  })

  // Apply real-time scoring adjustments
  const filtered = decayed
    .filter(c => {
      // Remove already-liked
      if (likedUris.has(c.uri)) return false
      // Remove zero-impact batches
      if (c.impactMultiplier <= 0) return false
      return true
    })
    .map(c => {
      let adjustedScore = c.effectiveScore
      const author = extractAuthor(c.uri)

      // 1. InteractionSeen fatigue penalty - Permanent -80% multiplier per view, with hard cutoff after 3 views
      const seenCount = seenCountMap[c.uri] || 0
      if (seenCount > 0) {
        if (seenCount >= 3) {
          // Hard cutoff: never serve after 3 views regardless of score
          adjustedScore = -501
        } else {
          // Apply permanent -80% multiplier per view
          const seenMultiplier = Math.pow(0.2, seenCount) // 0.2^seenCount
          adjustedScore *= seenMultiplier
        }
      }

      // 2. Author fatigue penalty - Soft penalty
      const fatigueScore = authorFatigueMap[author] || 0
      if (fatigueScore > 0) {
        // High fatigue (100) = -1200 score (significant but allows very high matches to pass)
        adjustedScore -= (fatigueScore / 100) * 1200
      }

      return { ...c, author, adjustedScore }
    })
    // Relaxed filter: Allow slightly fatigued posts to pass if they are still "close"
    .filter(c => c.adjustedScore > -500)

  // Sort by adjusted score
  filtered.sort((a, b) => b.adjustedScore - a.adjustedScore)

  // --- Account diversity: prevent same author from dominating ---
  const diversified: typeof filtered = []
  const recentAuthors: string[] = []

  for (const candidate of filtered) {
    // Account diversity: don't allow same author in last 2 slots (relaxed from 3)
    if (recentAuthors.slice(-2).includes(candidate.author)) continue

    diversified.push(candidate)
    recentAuthors.push(candidate.author)
  }

  console.log(`[Semantic Serve] After filters: ${filtered.length} → ${diversified.length} (${likedUris.size} liked filtered, serving fatigue applied)`)

  // --- Intersplicing: If batch is running low, supplement with live results ---
  const MIN_POSTS_THRESHOLD = 20
  let finalResults = diversified

  if (diversified.length < MIN_POSTS_THRESHOLD) {
    console.log(`[Semantic Serve] Batch running low (${diversified.length} items), intersplicing with live pipeline...`)
    const liveResult = await algo(ctx, params, requesterDid)

    // Base score for live items
    // If we have a cursor, we MUST ensure items start at or below the cursor score
    const currentCursorScore = params.cursor ? Number(params.cursor.split('::')[0]) : 1001
    const baseLiveScore = Math.min(1000, currentCursorScore - 1)

    const liveItems = liveResult.feed
      // Deduplicate: if it's already a HIGH-SCORING positive item in our batch, ignore live version
      .filter(item => {
        const batchItem = decayed.find(d => d.uri === item.post)
        if (batchItem && batchItem.effectiveScore > 0) return false
        return !likedUris.has(item.post)
      })
      .map((item, index) => ({
        uri: item.post,
        // Live items start at the baseLiveScore and decline slowly
        adjustedScore: baseLiveScore - (index * 5),
        author: extractAuthor(item.post),
        // Defaults for logging/metrics
        semanticScore: 0,
        pipelineScore: 0,
        impactMultiplier: 1,
        centroidId: -1,
        batchId: 'live_interspliced'
      }))

    // Merge and re-sort
    // @ts-ignore
    finalResults = [...diversified, ...liveItems].sort((a, b) => b.adjustedScore - a.adjustedScore)
    console.log(`[Semantic Serve] Interspliced ${liveItems.length} live posts. Total: ${finalResults.length}`)
  }

  // --- Fallback if all else fails ---
  if (finalResults.length === 0) {
    console.log(`[Semantic Serve] All sources exhausted, falling back to pure live pipeline`)
    return algo(ctx, params, requesterDid)
  }

  // --- Pagination ---
  const limit = Math.min(params.limit || 30, 100)
  let slicePool = finalResults

  if (params.cursor) {
    const parts = params.cursor.split('::')
    if (parts.length >= 2) {
      const cursorScore = Number(parts[0])
      const cursorUri = parts.slice(1).join('::')
      // @ts-ignore
      slicePool = finalResults.filter(c => {
        if (c.adjustedScore < cursorScore) return true
        if (c.adjustedScore > cursorScore) return false
        return c.uri.localeCompare(cursorUri) < 0
      })
    }
  }

  const page = slicePool.slice(0, limit)

  const semanticCount = page.filter(p => (p as any).batchId !== 'live_interspliced').length
  const liveCount = page.length - semanticCount

  // Log semantic serve metrics
  const avgSemantic = page.length > 0
    ? (page.reduce((sum, c) => sum + ((c as any).semanticScore || 0), 0) / page.length).toFixed(3)
    : '0'
  const avgImpact = page.length > 0
    ? (page.reduce((sum, c) => sum + ((c as any).impactMultiplier || 0), 0) / page.length).toFixed(3)
    : '0'
  console.log(`[Semantic Serve] Serving ${page.length} items (${semanticCount} semantic, ${liveCount} live) - avg semantic: ${avgSemantic}, avg impact: ${avgImpact}`)

  // Log debug entries for served semantic posts
  if (page.length > 0) {
    const debugEntries = page.map(p => ({
      userDid: requesterDid,
      uri: p.uri,
      score: Math.round(p.adjustedScore),
      signals: JSON.stringify({
        semanticScore: (p as any).semanticScore || 0,
        pipelineScore: (p as any).pipelineScore || 0,
        impactMultiplier: Number(((p as any).impactMultiplier || 1).toFixed(3)),
        centroidId: (p as any).centroidId || -1,
        batchId: (p as any).batchId || 'live_interspliced',
        source: (p as any).batchId === 'live_interspliced' ? 'live_interspliced' : 'semantic_batch',
      }),
      servedAt: new Date().toISOString(),
    }))

    // Debug logging disabled to reduce WAL growth
    // ctx.db
    //   .insertInto('feed_debug_log')
    //   .values(debugEntries)
    //   .execute()
    //   .catch(err => console.error(`[Semantic Debug Log Error] ${err}`))
  }

  const feed = page.map(c => ({ post: c.uri }))
  const last = page[page.length - 1]
  const cursor =
    last && page.length === limit
      ? `${last.adjustedScore}::${last.uri}`
      : undefined

  // Check batch consumption: use deduplicated counts for accurate trigger
  const totalBatchCandidates = decayed.length
  const totalServedFromBatches = Object.keys(seenCountMap).filter(uri =>
    uniqueBatchRowsMap[uri] !== undefined
  ).length
  const consumptionRatio = totalBatchCandidates > 0
    ? totalServedFromBatches / totalBatchCandidates
    : 0

  if (consumptionRatio >= 0.5) {
    console.log(`[Semantic Serve] ⚠️ Batch consumption at ${(consumptionRatio * 100).toFixed(0)}% — triggering early regeneration`)
    if (ctx.triggerBatchPipeline) {
      ctx.triggerBatchPipeline(true) // Priority trigger!
    }
  }

  return { feed, cursor }
}
