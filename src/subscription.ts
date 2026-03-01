import { Database } from './db'
import WebSocket from 'ws'
import { updateTasteSimilarity } from './algos/taste-similarity'
import { updateAuthorFatigueOnInteraction } from './algos/social-graph'
import { logger } from './logger'

export type JetstreamConfig = {
  wantedCollections: string[]
  wantedDids?: string[]
  trackedDids?: Set<string> // Users whose fatigue/interactions we actually want to track
}

export class JetstreamSubscription {
  public db: Database
  public service: string
  public sub: WebSocket | undefined
  public cursor: number | undefined
  public config: JetstreamConfig

  public trackedInteractionDids = new Set<string>()

  // Batching state
  private pendingLikes = new Map<string, number>()
  private pendingReposts = new Map<string, number>()
  private pendingPosts: any[] = []
  private pendingPostDeletes = new Set<string>()
  private pendingReplyCounts = new Map<string, number>()
  private pendingGraphInteractions: any[] = []
  private batchInterval: NodeJS.Timeout | null = null
  private isFlushing = false

  constructor(db: Database, service: string, config: JetstreamConfig) {
    this.db = db
    this.service = service
    this.config = config
    this.batchInterval = setInterval(() => this.flushBatch(), 5000)
  }

  private async flushBatch() {
    if (this.isFlushing) return
    if (this.pendingLikes.size === 0 && this.pendingReposts.size === 0 && this.pendingPosts.length === 0 && this.pendingPostDeletes.size === 0 && this.pendingReplyCounts.size === 0 && this.pendingGraphInteractions.length === 0) return

    this.isFlushing = true

    const likesToFlush = new Map(this.pendingLikes)
    const repostsToFlush = new Map(this.pendingReposts)
    const replyCountsToFlush = new Map(this.pendingReplyCounts)
    const postsToFlush = [...this.pendingPosts]
    const deletesToFlush = new Set(this.pendingPostDeletes)
    const interactionsToFlush = [...this.pendingGraphInteractions]

    this.pendingLikes.clear()
    this.pendingReposts.clear()
    this.pendingReplyCounts.clear()
    this.pendingPosts = []
    this.pendingPostDeletes.clear()
    this.pendingGraphInteractions = []

    try {
      await this.db.transaction().execute(async (trx) => {
        // Insert posts
        if (postsToFlush.length > 0) {
          for (let i = 0; i < postsToFlush.length; i += 500) {
            await trx.insertInto('post')
              .values(postsToFlush.slice(i, i + 500))
              .onConflict((oc) => oc.doNothing())
              .execute()
          }
        }

        // Delete posts
        if (deletesToFlush.size > 0) {
          const deleteArray = Array.from(deletesToFlush)
          for (let i = 0; i < deleteArray.length; i += 500) {
            await trx.deleteFrom('post').where('uri', 'in', deleteArray.slice(i, i + 500)).execute()
          }
        }

        // Aggregate all updates perfectly per URI
        const postUpdates = new Map<string, { likes: number, reposts: number, replies: number }>()
        for (const [uri, count] of likesToFlush.entries()) {
          const u = postUpdates.get(uri) || { likes: 0, reposts: 0, replies: 0 }
          u.likes += count
          postUpdates.set(uri, u)
        }
        for (const [uri, count] of repostsToFlush.entries()) {
          const u = postUpdates.get(uri) || { likes: 0, reposts: 0, replies: 0 }
          u.reposts += count
          postUpdates.set(uri, u)
        }
        for (const [uri, count] of replyCountsToFlush.entries()) {
          const u = postUpdates.get(uri) || { likes: 0, reposts: 0, replies: 0 }
          u.replies += count
          postUpdates.set(uri, u)
        }

        const sortedUpdates = Array.from(postUpdates.entries()).sort((a, b) => a[0].localeCompare(b[0]))

        for (const [uri, counts] of sortedUpdates) {
          await trx.updateTable('post')
            .set((eb) => {
              const updates: any = {}
              if (counts.likes > 0) updates.likeCount = eb('likeCount', '+', counts.likes)
              if (counts.reposts > 0) updates.repostCount = eb('repostCount', '+', counts.reposts)
              if (counts.replies > 0) updates.replyCount = eb('replyCount', '+', counts.replies)
              return updates
            })
            .where('uri', '=', uri)
            .execute()
        }

        // Insert interactions
        if (interactionsToFlush.length > 0) {
          for (let i = 0; i < interactionsToFlush.length; i += 500) {
            await trx.insertInto('graph_interaction')
              .values(interactionsToFlush.slice(i, i + 500))
              .onConflict((oc) => oc.doNothing())
              .execute()
          }
        }
      })
      console.log(`[Batch Flush] Posts: ${postsToFlush.length}, Deletes: ${deletesToFlush.size}, Likes: ${likesToFlush.size}, Reposts: ${repostsToFlush.size}, Interactions: ${interactionsToFlush.length}`)
      logger.info(`Batch Flush completed. Posts: ${postsToFlush.length}, Deletes: ${deletesToFlush.size}, Likes: ${likesToFlush.size}`)

      if (this.cursor) {
        await this.updateCursor(this.cursor)
      }
    } catch (err) {
      logger.error('Failed to flush to DB:', err)
      // Retry logic (simplified)
      for (const p of postsToFlush) this.pendingPosts.push(p)
      for (const d of deletesToFlush) this.pendingPostDeletes.add(d)
      for (const i of interactionsToFlush) this.pendingGraphInteractions.push(i)
      for (const [uri, count] of likesToFlush.entries()) this.pendingLikes.set(uri, (this.pendingLikes.get(uri) || 0) + count)
      for (const [uri, count] of repostsToFlush.entries()) this.pendingReposts.set(uri, (this.pendingReposts.get(uri) || 0) + count)
      for (const [uri, count] of replyCountsToFlush.entries()) this.pendingReplyCounts.set(uri, (this.pendingReplyCounts.get(uri) || 0) + count)
    } finally {
      this.isFlushing = false
    }
  }

  async run(subscriptionReconnectDelay: number) {
    try {
      const res = await this.db
        .selectFrom('sub_state')
        .selectAll()
        .where('service', '=', this.service)
        .executeTakeFirst()

      if (res) {
        this.cursor = res.cursor
      }

      const urlObj = new URL(this.service)
      this.config.wantedCollections.forEach((c) =>
        urlObj.searchParams.append('wantedCollections', c),
      )
      // Never put DIDs in URL to avoid "400 Bad Request" (URL too long)
      // Instead, we use requireHello=true and send them via a WebSocket message
      urlObj.searchParams.append('requireHello', 'true')

      if (this.cursor) {
        urlObj.searchParams.append('cursor', this.cursor.toString())
      }

      const urlStr = urlObj.toString()
      logger.info(`Connecting to Jetstream at: ${urlStr}`)

      this.sub = new WebSocket(urlStr)

      this.sub.on('open', () => {
        logger.info(`Jetstream connected (${this.config.wantedCollections.join(', ')}). Sending options update...`)
        this.sendOptionsUpdate()
      })

      this.sub.on('message', async (data: WebSocket.Data) => {
        try {
          const event = JSON.parse(data.toString())
          await this.handleEvent(event)
        } catch (err) {
          logger.error('Error handling Jetstream event', err)
        }
      })

      this.sub.on('error', (err) => {
        logger.error('Jetstream WebSocket error:', err)
      })

      this.sub.on('close', () => {
        logger.warn('Jetstream connection closed. Reconnecting...')
        setTimeout(() => this.run(subscriptionReconnectDelay), subscriptionReconnectDelay)
      })
    } catch (err) {
      logger.error('Error starting Jetstream subscription', err)
      setTimeout(() => this.run(subscriptionReconnectDelay), subscriptionReconnectDelay)
    }
  }

  async handleEvent(evt: any) {
    if (evt.kind !== 'commit') return
    const { operation, collection, rkey, record, cid } = evt.commit
    const did = evt.did
    const uri = `at://${did}/${collection}/${rkey}`

    if (collection === 'app.bsky.feed.post') {
      if (operation === 'create') {
        const replyRoot = record.reply?.root?.uri || null
        const replyParent = record.reply?.parent?.uri || null
        // PostgreSQL strictly rejects \u0000 (null bytes) in strings, which Bluesky permits
        const text = record.text?.replace(/\u0000/g, '') || null

        // Extract media flags
        const embed = record.embed
        const hasImage = embed?.$type === 'app.bsky.embed.images' || (embed?.$type === 'app.bsky.embed.recordWithMedia' && embed?.media?.$type === 'app.bsky.embed.images')
        const hasVideo = embed?.$type === 'app.bsky.embed.video' || (embed?.$type === 'app.bsky.embed.recordWithMedia' && embed?.media?.$type === 'app.bsky.embed.video')
        const hasExternal = embed?.$type === 'app.bsky.embed.external'

        this.pendingPosts.push({
          uri,
          cid,
          author: did,
          indexedAt: new Date().toISOString(),
          likeCount: 0,
          replyCount: 0,
          repostCount: 0,
          replyRoot,
          replyParent,
          text,
          hasImage,
          hasVideo,
          hasExternal,
        })

        if (replyParent) {
          this.pendingReplyCounts.set(replyParent, (this.pendingReplyCounts.get(replyParent) || 0) + 1)
        }
      } else if (operation === 'delete') {
        this.pendingPostDeletes.add(uri)
      }
    } else if (collection === 'app.bsky.feed.like') {
      if (operation === 'create') {
        const subjectUri = record.subject?.uri
        if (subjectUri) {
          // Batch it into memory instead of hitting the DB instantly
          this.pendingLikes.set(subjectUri, (this.pendingLikes.get(subjectUri) || 0) + 1)

          if (this.trackedInteractionDids.has(did)) {
            this.pendingGraphInteractions.push({
              actor: did,
              target: subjectUri,
              type: 'like',
              weight: 1,
              indexedAt: new Date().toISOString(),
              interactionUri: uri,
            })
          }

          // Update taste similarity for all users
          try {
            const ctx = { db: this.db }
            // ONLY track similarity for our own users
            if (!this.config.trackedDids || this.config.trackedDids.has(did)) {
              await updateTasteSimilarity(ctx, did, subjectUri, 'like')
            }
          } catch (err) {
            console.error('[Taste Similarity] Failed to update taste similarity:', err)
          }

          // Update author fatigue for like interactions
          try {
            const ctx = { db: this.db }
            // ONLY track fatigue for our own users, not everyone we follow
            if (!this.config.trackedDids || this.config.trackedDids.has(did)) {
              await updateAuthorFatigueOnInteraction(ctx, did, subjectUri, 'like')
            }
          } catch (err) {
            console.error('[Author Fatigue] Failed to update fatigue for like:', err)
          }
        }
      }
    } else if (collection === 'app.bsky.feed.repost') {
      if (operation === 'create') {
        const subjectUri = record.subject?.uri
        if (subjectUri) {
          // Batch it into memory
          this.pendingReposts.set(subjectUri, (this.pendingReposts.get(subjectUri) || 0) + 1)

          if (this.trackedInteractionDids.has(did)) {
            this.pendingGraphInteractions.push({
              actor: did,
              target: subjectUri,
              type: 'repost',
              weight: 2,
              indexedAt: new Date().toISOString(),
              interactionUri: uri,
            })
          }

          // Update author fatigue for repost interactions
          try {
            const ctx = { db: this.db }
            // ONLY track fatigue for our own users, not everyone we follow
            if (!this.config.trackedDids || this.config.trackedDids.has(did)) {
              await updateAuthorFatigueOnInteraction(ctx, did, subjectUri, 'repost')
            }
          } catch (err) {
            console.error('[Author Fatigue] Failed to update fatigue for repost:', err)
          }
        }
      }
    } else if (collection === 'app.bsky.feed.post' && operation === 'create' && record.reply) {
      // Handle reply interactions (when someone replies to a post)
      const replyParent = record.reply?.parent?.uri
      if (replyParent) {
        if (this.trackedInteractionDids.has(did)) {
          this.pendingGraphInteractions.push({
            actor: did,
            target: replyParent,
            type: 'reply',
            weight: 1,
            indexedAt: new Date().toISOString(),
            interactionUri: uri,
          })
        }

        // Update author fatigue for reply interactions
        try {
          const ctx = { db: this.db }
          // ONLY track fatigue for our own users, not everyone we follow
          if (!this.config.trackedDids || this.config.trackedDids.has(did)) {
            await updateAuthorFatigueOnInteraction(ctx, did, replyParent, 'reply')
          }
        } catch (err) {
          console.error('[Author Fatigue] Failed to update fatigue for reply:', err)
        }
      }
    }

    if (evt.time_us) {
      this.cursor = evt.time_us
    }
  }

  async updateCursor(cursor: number) {
    await this.db
      .insertInto('sub_state')
      .values({ service: this.service, cursor })
      .onConflict((oc) => oc.column('service').doUpdateSet({ cursor }))
      .execute()
  }

  public sendOptionsUpdate() {
    if (!this.sub || this.sub.readyState !== WebSocket.OPEN) return
    const message = {
      type: 'options_update',
      payload: {
        wantedCollections: this.config.wantedCollections,
        wantedDids: this.config.wantedDids || [],
        maxMessageSizeBytes: 0, // 0 = no limit, explicitly matching documentation example
      },
    }
    this.sub.send(JSON.stringify(message))
  }

  public async updateOptions(config: Partial<JetstreamConfig>) {
    this.config = { ...this.config, ...config }
    this.sendOptionsUpdate()
  }

  async stop() {
    if (this.batchInterval) {
      clearInterval(this.batchInterval)
      await this.flushBatch()
    }
    if (this.sub) {
      // Remove listeners to prevent 'error' or 'close' from firing during planned termination
      this.sub.removeAllListeners()
      this.sub.on('error', () => { }) // Swallow any terminal errors

      if (this.sub.readyState === WebSocket.CONNECTING || this.sub.readyState === WebSocket.OPEN) {
        try {
          this.sub.terminate()
        } catch (e) {
          // Ignore state-change race conditions
        }
      }
      this.sub = undefined
    }
  }
}
