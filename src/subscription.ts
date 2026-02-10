import { Database } from './db'
import WebSocket from 'ws'

export class JetstreamSubscription {
  public db: Database
  public service: string
  public sub: WebSocket | undefined
  public cursor: number | undefined
  public intervalId: NodeJS.Timeout | undefined

  constructor(db: Database, service: string) {
    this.db = db
    this.service = service
  }

  async run(subscriptionReconnectDelay: number) {
    try {
      // Get the last cursor from the database
      const res = await this.db
        .selectFrom('sub_state')
        .selectAll()
        .where('service', '=', this.service)
        .executeTakeFirst()

      if (res) {
        this.cursor = res.cursor
      }

      // Construct the Jetstream URL
      // The service URL in .env is likely "wss://jetstream2.us-east.bsky.network/subscribe"
      // We need to append query parameters.
      let urlStr = this.service
      if (!urlStr.includes('?')) {
        urlStr += '?'
      } else {
        urlStr += '&'
      }

      const params = new URLSearchParams()
      params.append('wantedCollections', 'app.bsky.feed.post')
      if (this.cursor) {
        params.append('cursor', this.cursor.toString())
      }

      // If service url ended with ?, append params without leading ? or &.
      // Simplest is to use URL object if service is a valid URL base.
      try {
        const urlObj = new URL(this.service)
        urlObj.searchParams.append('wantedCollections', 'app.bsky.feed.post')
        if (this.cursor) {
          urlObj.searchParams.append('cursor', this.cursor.toString())
        }
        urlStr = urlObj.toString()
      } catch (e) {
        // Fallback if strictly not a URL (though it should be)
        urlStr = `${this.service}?wantedCollections=app.bsky.feed.post`
        if (this.cursor) urlStr += `&cursor=${this.cursor}`
      }

      console.log(`Connecting to Jetstream at: ${urlStr}`)

      this.sub = new WebSocket(urlStr)

      this.sub.on('open', () => {
        console.log('Jetstream connected!')
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
    // We only care about commits
    if (evt.kind !== 'commit') return

    const { operation, collection, rkey, record, cid } = evt.commit

    // Check collection (redundant if filtered by server, but good practice)
    if (collection !== 'app.bsky.feed.post') return

    const did = evt.did
    const uri = `at://${did}/${collection}/${rkey}`

    if (operation === 'create') {
      // Log the text as requested (optional fun)
      if (record?.text) {
        console.log(`${did}: ${record.text}`)
      }

      // Filter logic: "alf" (case-insensitive)
      if (record?.text && record.text.toLowerCase().includes('alf')) {
        console.log(`Found ALF post! ${uri}`)
        const post = {
          uri: uri,
          cid: cid,
          indexedAt: new Date().toISOString(),
        }
        await this.db
          .insertInto('post')
          .values(post)
          .onConflict((oc) => oc.doNothing())
          .execute()
      }
    } else if (operation === 'delete') {
      await this.db
        .deleteFrom('post')
        .where('uri', '=', uri)
        .execute()
    }

    // Update cursor logic
    if (evt.time_us) {
      // Update cursor in DB occasionally (e.g. every 100th event or similar)
      // For now, simpler: update in memory, and maybe flush to DB?
      // Or just write to DB. Jetstream is fast, writing every event might be slow.
      // But let's assume SQLite is fast enough for now or rely on the previous logic's pattern.
      // Previous logic updated every 20 sequences.
      // We can use a modulo check if time_us was sequential, but it's not.
      // Let's just update for now. If perf is bad, we optimize.

      await this.updateCursor(evt.time_us)
    }
  }

  async updateCursor(cursor: number) {
    // Upsert the cursor
    const result = await this.db
      .updateTable('sub_state')
      .set({ cursor })
      .where('service', '=', this.service)
      .execute()

    // If no row was updated, insert it
    if (result.length === 0 || result[0].numUpdatedRows === BigInt(0)) {
      // Double check existence to be safe or just use insert on conflict
      // However, we can use insert ... on conflict update
      await this.db
        .insertInto('sub_state')
        .values({ service: this.service, cursor })
        .onConflict((oc) => oc.column('service').doUpdateSet({ cursor }))
        .execute()
    }
  }
}
