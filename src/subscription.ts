import { Database } from './db'
import WebSocket from 'ws'

export type JetstreamConfig = {
  wantedCollections: string[]
  wantedDids?: string[]
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
      if (this.config.wantedDids && this.config.wantedDids.length > 0) {
        this.config.wantedDids.forEach((d) =>
          urlObj.searchParams.append('wantedDids', d),
        )
      }
      if (this.cursor) {
        urlObj.searchParams.append('cursor', this.cursor.toString())
      }

      const urlStr = urlObj.toString()
      console.log(`Connecting to Jetstream at: ${urlStr}`)

      this.sub = new WebSocket(urlStr)

      this.sub.on('open', () => {
        console.log(`Jetstream connected (${this.config.wantedCollections.join(', ')})`)
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

  async stop() {
    if (this.sub) {
      this.sub.terminate()
    }
  }
}
