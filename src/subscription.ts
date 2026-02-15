import { Database } from './db'
import WebSocket from 'ws'
import { updateTasteSimilarity } from './algos/taste-similarity'
import { updateAuthorFatigueOnInteraction } from './algos/social-graph'

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

  constructor(db: Database, service: string, config: JetstreamConfig) {
    this.db = db
    this.service = service
    this.config = config
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
      console.log(`Connecting to Jetstream at: ${urlStr}`)

      this.sub = new WebSocket(urlStr)

      this.sub.on('open', () => {
        console.log(`Jetstream connected (${this.config.wantedCollections.join(', ')}). Sending options update...`)
        this.sendOptionsUpdate()
      })

      this.sub.on('message', async (data: WebSocket.Data) => {
        try {
          const event = JSON.parse(data.toString())
          await this.handleEvent(event)
        } catch (err) {
          console.error('Error handling Jetstream event', err)
        }
      })

      this.sub.on('error', (err) => {
        console.error('Jetstream WebSocket error:', err)
      })

      this.sub.on('close', () => {
        console.log('Jetstream connection closed. Reconnecting...')
        setTimeout(() => this.run(subscriptionReconnectDelay), subscriptionReconnectDelay)
      })
    } catch (err) {
      console.error('Error starting Jetstream subscription', err)
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
        const text = record.text || null

        // Extract media flags
        const embed = record.embed
        const hasImage = embed?.$type === 'app.bsky.embed.images' || (embed?.$type === 'app.bsky.embed.recordWithMedia' && embed?.media?.$type === 'app.bsky.embed.images')
        const hasVideo = embed?.$type === 'app.bsky.embed.video' || (embed?.$type === 'app.bsky.embed.recordWithMedia' && embed?.media?.$type === 'app.bsky.embed.video')
        const hasExternal = embed?.$type === 'app.bsky.embed.external'

        await this.db
          .insertInto('post')
          .values({
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
            hasImage: hasImage ? 1 : 0,
            hasVideo: hasVideo ? 1 : 0,
            hasExternal: hasExternal ? 1 : 0,
          })
          .onConflict((oc) => oc.doNothing())
          .execute()

        if (replyParent) {
          await this.db
            .updateTable('post')
            .set((eb) => ({
              replyCount: eb('replyCount', '+', 1),
            }))
            .where('uri', '=', replyParent)
            .execute()
        }
      } else if (operation === 'delete') {
        await this.db.deleteFrom('post').where('uri', '=', uri).execute()
      }
    } else if (collection === 'app.bsky.feed.like') {
      if (operation === 'create') {
        const subjectUri = record.subject?.uri
        if (subjectUri) {
          await this.db
            .updateTable('post')
            .set((eb) => ({
              likeCount: eb('likeCount', '+', 1),
            }))
            .where('uri', '=', subjectUri)
            .execute()

          await this.db.insertInto('graph_interaction').values({
            actor: did,
            target: subjectUri,
            type: 'like',
            weight: 1,
            indexedAt: new Date().toISOString(),
          })
            .onConflict((oc) => oc.doNothing())
            .execute()

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
          await this.db
            .updateTable('post')
            .set((eb) => ({
              repostCount: eb('repostCount', '+', 1),
            }))
            .where('uri', '=', subjectUri)
            .execute()

          await this.db.insertInto('graph_interaction').values({
            actor: did,
            target: subjectUri,
            type: 'repost',
            weight: 2,
            indexedAt: new Date().toISOString(),
          })
            .onConflict((oc) => oc.doNothing())
            .execute()

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
        await this.db.insertInto('graph_interaction').values({
          actor: did,
          target: replyParent,
          type: 'reply',
          weight: 1,
          indexedAt: new Date().toISOString(),
        })
          .onConflict((oc) => oc.doNothing())
          .execute()

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
      await this.updateCursor(evt.time_us)
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
