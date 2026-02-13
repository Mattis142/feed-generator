import http from 'http'
import events from 'events'
import express from 'express'
import { spawn } from 'child_process'
import path from 'path'
import { DidResolver, MemoryCache } from '@atproto/identity'
import { createServer } from './lexicon'
import feedGeneration from './methods/feed-generation'
import describeGenerator from './methods/describe-generator'
import sendInteractions from './methods/send-interactions'
import { createDb, Database, migrateToLatest } from './db'
import { JetstreamSubscription } from './subscription'
import { AppContext, Config } from './config'
import wellKnown from './well-known'
import { GraphBuilder } from './services/graph-builder'
import { cleanupOldTasteData } from './algos/taste-similarity'

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
      timeout: 10000, // 10 second timeout for PLC directory
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
    sendInteractions(server, ctx)
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

    // Start daily keywords extraction job
    this.runDailyKeywords().catch(err => {
      console.error('Initial runDailyKeywords failed', err)
    })
    setInterval(() => {
      this.runDailyKeywords().catch(err => {
        console.error('Periodic runDailyKeywords failed', err)
      })
    }, 24 * 60 * 60 * 1000) // Every 24 hours

    // Start background cleanup for taste similarity data
    this.cleanupTasteData().catch(err => {
      console.error('Initial cleanupTasteData failed', err)
    })
    setInterval(() => {
      this.cleanupTasteData().catch(err => {
        console.error('Periodic cleanupTasteData failed', err)
      })
    }, 7 * 24 * 60 * 60 * 1000) // Every 7 days

    return this.server
  }

  private async cleanupPosts() {
    try {
      console.log('Running background cleanup for low-relevance posts...')
      // Keep at least 7 days of data for better temporal diversity
      const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString()

      // Delete posts older than 7 days that have zero engagement and aren't followed
      const deleted = await this.db.deleteFrom('post')
        .where('indexedAt', '<', sevenDaysAgo)
        .where('likeCount', '=', 0)
        .where('repostCount', '=', 0)
        .where('author', 'not in', (eb) => eb.selectFrom('graph_follow').select('followee'))
        .executeTakeFirst()

      console.log(`Pruned ${deleted.numDeletedRows} low-relevance posts.`)

      // Cleanup served posts older than 6 hours (extended from 24h for better fatigue tracking)
      const sixHoursAgo = new Date(Date.now() - 6 * 60 * 60 * 1000).toISOString()
      const cleanedServed = await this.db.deleteFrom('user_served_post')
        .where('servedAt', '<', sixHoursAgo)
        .executeTakeFirst()

      // Cleanup seen posts older than 8 hours (longer retention for better fatigue tracking)
      const eightHoursAgo = new Date(Date.now() - 8 * 60 * 60 * 1000).toISOString()
      const cleanedSeen = await this.db.deleteFrom('user_seen_post')
        .where('seenAt', '<', eightHoursAgo)
        .executeTakeFirst()

      if (cleanedServed.numDeletedRows > 0) {
        console.log(`Pruned ${cleanedServed.numDeletedRows} old serving history entries.`)
      }
      if (cleanedSeen.numDeletedRows > 0) {
        console.log(`Pruned ${cleanedSeen.numDeletedRows} old seen history entries.`)
      }
    } catch (err) {
      console.error('Failed to cleanup posts', err)
    }
  }

  private async refreshWantedDids() {
    try {
      console.log('Refreshing wantedDids for graph subscription...')
      const graphBuilder = new GraphBuilder(this.db)

      // Build graphs for all whitelisted users
      const allUsersToTrack = [this.cfg.publisherDid, ...this.cfg.whitelist]
      const allDids = new Set<string>()

      for (const userDid of allUsersToTrack) {
        const userDids = await graphBuilder.getWantedDids(userDid)
        userDids.forEach(did => allDids.add(did))
      }

      // Update the second firehose (Connection B)
      const graphFirehose = this.firehoses[1]
      if (graphFirehose) {
        // Track Layer 1 follows for ALL users
        const allLayer1Dids = await this.db.selectFrom('graph_follow')
          .select('followee')
          .where('follower', 'in', allUsersToTrack)
          .execute()

        // Track high-reputation Taste Twins for ALL users to index their behaviors
        const allTasteTwins = await this.db.selectFrom('taste_reputation')
          .select('similarUserDid')
          .where('userDid', 'in', allUsersToTrack)
          .where('reputationScore', '>', 1.1) // Track anyone with positive reputation
          .execute()

        const didsToTrack = new Set(allLayer1Dids.map(r => r.followee))
        allTasteTwins.forEach(r => didsToTrack.add(r.similarUserDid))
        allUsersToTrack.forEach(did => didsToTrack.add(did)) // Track all users themselves

        console.log(`Updating graph firehose with ${didsToTrack.size} tracked DIDs (including taste twins)...`)
        await graphFirehose.updateOptions({ wantedDids: Array.from(didsToTrack) })
      }
    } catch (err) {
      console.error('Failed to refresh wantedDids', err)
    }
  }

  private async runDailyKeywords() {
    try {
      console.log('Running daily keywords extraction job...')
      // Run the daily keywords script as a child process to avoid blocking the main server
      const scriptPath = path.join(__dirname, '../scripts/daily-keywords.ts')
      const child = spawn('ts-node', [scriptPath], {
        stdio: 'inherit',
        env: { ...process.env }
      })

      child.on('error', (err: Error) => {
        console.error('Failed to spawn daily keywords process:', err)
      })

      child.on('exit', (code: number) => {
        if (code === 0) {
          console.log('Daily keywords extraction completed successfully')
        } else {
          console.error(`Daily keywords extraction exited with code ${code}`)
        }
      })
    } catch (err) {
      console.error('Failed to run daily keywords job', err)
    }
  }

  private async cleanupTasteData() {
    try {
      console.log('Running background cleanup for taste similarity data...')
      await cleanupOldTasteData({ db: this.db }, 90) // Keep 90 days of data
      console.log('Taste similarity data cleanup completed')
    } catch (err) {
      console.error('Failed to cleanup taste data', err)
    }
  }
}

export default FeedGenerator
