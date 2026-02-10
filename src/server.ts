import http from 'http'
import events from 'events'
import express from 'express'
import { DidResolver, MemoryCache } from '@atproto/identity'
import { createServer } from './lexicon'
import feedGeneration from './methods/feed-generation'
import describeGenerator from './methods/describe-generator'
import { createDb, Database, migrateToLatest } from './db'
import { JetstreamSubscription } from './subscription'
import { AppContext, Config } from './config'
import wellKnown from './well-known'
import { GraphBuilder } from './services/graph-builder'

export class FeedGenerator {
  public app: express.Application
  public server?: http.Server
  public db: Database
  public firehoses: JetstreamSubscription[]
  public cfg: Config

  constructor(
    app: express.Application,
    db: Database,
    firehoses: JetstreamSubscription[],
    cfg: Config,
  ) {
    this.app = app
    this.db = db
    this.firehoses = firehoses
    this.cfg = cfg
  }

  static create(cfg: Config) {
    const app = express()
    const db = createDb(cfg.sqliteLocation)

    // Connection A: Global Posts
    const globalPosts = new JetstreamSubscription(db, cfg.subscriptionEndpoint, {
      wantedCollections: ['app.bsky.feed.post'],
    })

    // Connection B: Graph Interactions (Initially empty dids, will be updated)
    const graphInteractions = new JetstreamSubscription(db, cfg.subscriptionEndpoint, {
      wantedCollections: ['app.bsky.feed.like', 'app.bsky.feed.repost'],
      wantedDids: [],
    })

    const didCache = new MemoryCache()
    const didResolver = new DidResolver({
      plcUrl: 'https://plc.directory',
      didCache,
    })

    const server = createServer({
      validateResponse: true,
      payload: {
        jsonLimit: 100 * 1024, // 100kb
        textLimit: 100 * 1024, // 100kb
        blobLimit: 5 * 1024 * 1024, // 5mb
      },
    })
    const ctx: AppContext = {
      db,
      didResolver,
      cfg,
    }
    feedGeneration(server, ctx)
    describeGenerator(server, ctx)
    app.use(server.xrpc.router)
    app.use(wellKnown(ctx))

    return new FeedGenerator(app, db, [globalPosts, graphInteractions], cfg)
  }

  async start(): Promise<http.Server> {
    await migrateToLatest(this.db)

    // Initial run of firehoses
    for (const firehose of this.firehoses) {
      firehose.run(this.cfg.subscriptionReconnectDelay)
    }

    this.server = this.app.listen(this.cfg.port, this.cfg.listenhost)
    await events.once(this.server, 'listening')

    // Start background refresh for wantedDids
    this.refreshWantedDids().catch(err => {
      console.error('Initial refreshWantedDids failed', err)
    })
    setInterval(() => {
      this.refreshWantedDids().catch(err => {
        console.error('Periodic refreshWantedDids failed', err)
      })
    }, 24 * 60 * 60 * 1000)

    // Start background cleanup for low-relevance posts
    this.cleanupPosts().catch(err => {
      console.error('Initial cleanupPosts failed', err)
    })
    setInterval(() => {
      this.cleanupPosts().catch(err => {
        console.error('Periodic cleanupPosts failed', err)
      })
    }, 30 * 60 * 1000) // Every 30 minutes

    return this.server
  }

  private async cleanupPosts() {
    try {
      console.log('Running background cleanup for low-relevance posts...')
      const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString()

      // Delete posts older than 1h that aren't from anyone's followed graph and have low engagement
      const deleted = await this.db.deleteFrom('post')
        .where('indexedAt', '<', oneHourAgo)
        .where('likeCount', '<', 2)
        .where('repostCount', '=', 0)
        .where('author', 'not in', (eb) => eb.selectFrom('graph_follow').select('followee'))
        .executeTakeFirst()

      console.log(`Pruned ${deleted.numDeletedRows} low-relevance posts.`)

      // Cleanup served posts older than 24h
      const oneDayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString()
      const cleanedServed = await this.db.deleteFrom('user_served_post')
        .where('servedAt', '<', oneDayAgo)
        .executeTakeFirst()

      if (cleanedServed.numDeletedRows > 0) {
        console.log(`Pruned ${cleanedServed.numDeletedRows} old serving history entries.`)
      }
    } catch (err) {
      console.error('Failed to cleanup posts', err)
    }
  }

  private async refreshWantedDids() {
    try {
      console.log('Refreshing wantedDids for graph subscription...')
      const graphBuilder = new GraphBuilder(this.db)

      // For now, we refresh for the publisher DID (the owner)
      // In a multi-user system, we'd iterate all whitelisted users.
      const dids = await graphBuilder.getWantedDids(this.cfg.publisherDid)

      // Update the second firehose (Connection B)
      const graphFirehose = this.firehoses[1]
      if (graphFirehose) {
        // Jetstream URL length is limited. We'll track interactions for Layer 1 only,
        // plus the publisher and any whitelisted users.
        const layer1Dids = await this.db.selectFrom('graph_follow')
          .select('followee')
          .where('follower', '=', this.cfg.publisherDid)
          .execute()

        const didsToTrack = new Set(layer1Dids.map(r => r.followee))
        didsToTrack.add(this.cfg.publisherDid) // Always track the owner
        this.cfg.whitelist.forEach(did => didsToTrack.add(did)) // Track whitelisted users

        graphFirehose.config.wantedDids = Array.from(didsToTrack)
        console.log(`Updated graph firehose with ${didsToTrack.size} Layer 1 DIDs. Restarting...`)
        await graphFirehose.stop()
        graphFirehose.run(this.cfg.subscriptionReconnectDelay)
      }
    } catch (err) {
      console.error('Failed to refresh wantedDids', err)
    }
  }
}

export default FeedGenerator
