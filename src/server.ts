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
import { qdrantDB } from './db/qdrant'

export class FeedGenerator {
  public app: express.Application
  public server?: http.Server
  public db: Database
  public cfg: Config
  private isPipelineRunning = false
  private lastPipelineRunAt = 0

  constructor(
    app: express.Application,
    db: Database,
    cfg: Config,
  ) {
    this.app = app
    this.db = db
    this.cfg = cfg
  }

  static create(cfg: Config) {
    const app = express()
    const db = createDb(cfg.postgresConnectionString)

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
      triggerBatchPipeline: (priority?: boolean) => {
        // This will be properly set in start()
      }
    }
    feedGeneration(server, ctx)
    describeGenerator(server, ctx)
    sendInteractions(server, ctx)
    app.use(server.xrpc.router)
    app.use(wellKnown(ctx))

    const generator = new FeedGenerator(app, db, cfg)
    ctx.triggerBatchPipeline = (priority?: boolean) => generator.runBatchPipeline(priority)
    return generator
  }

  async start(): Promise<http.Server> {
    await migrateToLatest(this.db)

    // Initialize Qdrant semantic feed collections
    try {
      await qdrantDB.ensureFeedCollections()
    } catch (err) {
      console.error('Failed to initialize Qdrant feed collections (non-fatal):', err)
    }

    this.server = this.app.listen(this.cfg.port, this.cfg.listenhost)
    await events.once(this.server, 'listening')

    // Start background refresh for wantedDids (For API context)
    this.refreshWantedDids().catch(err => {
      console.error('Initial refreshWantedDids failed', err)
    })
    setInterval(() => {
      this.refreshWantedDids().catch(err => {
        console.error('Periodic refreshWantedDids failed', err)
      })
    }, 30 * 60 * 1000)

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

    // Start batch pipeline for semantic embeddings (every 90 minutes)
    // Delay initial run by 2 minutes to let the server stabilize
    setTimeout(() => {
      this.runBatchPipeline().catch(err => {
        console.error('Initial runBatchPipeline failed', err)
      })
    }, 2 * 60 * 1000)
    setInterval(() => {
      this.runBatchPipeline().catch(err => {
        console.error('Periodic runBatchPipeline failed', err)
      })
    }, 90 * 60 * 1000) // Every 90 minutes

    // Cleanup expired semantic batches (every 30 minutes)
    setInterval(() => {
      this.cleanupExpiredBatches().catch(err => {
        console.error('Periodic cleanupExpiredBatches failed', err)
      })
    }, 30 * 60 * 1000) // Every 30 minutes

    // Cleanup debug logs (every 5 minutes)
    this.cleanupDebugLogs().catch(err => {
      console.error('Initial cleanupDebugLogs failed', err)
    })
    setInterval(() => {
      this.cleanupDebugLogs().catch(err => {
        console.error('Periodic cleanupDebugLogs failed', err)
      })
    }, 5 * 60 * 1000) // Every 5 minutes

    return this.server
  }

  private async refreshWantedDids() {
    try {
      console.log('Refreshing user graphs...')
      const graphBuilder = new GraphBuilder(this.db)

      // Build graphs for all whitelisted users
      const allUsersToTrack = [this.cfg.publisherDid, ...this.cfg.whitelist]

      for (const userDid of allUsersToTrack) {
        // Just trigger the graph resolution, which writes to DB
        await graphBuilder.getWantedDids(userDid)
      }
    } catch (err) {
      console.error('Failed to refresh user graphs', err)
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

  public async runBatchPipeline(forcePriority = false) {
    if (this.isPipelineRunning) {
      console.log('[Batch Pipeline] Already running, skipping trigger.')
      return
    }

    // Cooldown: don't run more than once every 10 minutes unless prioritized
    const now = Date.now()
    if (!forcePriority && (now - this.lastPipelineRunAt < 10 * 60 * 1000)) {
      console.log('[Batch Pipeline] Recently run, skipping trigger.')
      return
    }

    try {
      this.isPipelineRunning = true
      this.lastPipelineRunAt = now
      console.log('Running semantic batch pipeline...')
      const scriptPath = path.join(__dirname, '../scripts/batch-pipeline.ts')
      const child = spawn('ts-node', [scriptPath], {
        stdio: 'inherit',
        env: { ...process.env }
      })

      child.on('error', (err: Error) => {
        this.isPipelineRunning = false
        console.error('Failed to spawn batch pipeline process:', err)
      })

      child.on('exit', (code: number) => {
        this.isPipelineRunning = false
        if (code === 0) {
          console.log('Semantic batch pipeline completed successfully')
        } else {
          console.error(`Semantic batch pipeline exited with code ${code}`)
        }
      })
    } catch (err) {
      this.isPipelineRunning = false
      console.error('Failed to run batch pipeline', err)
    }
  }

  private async cleanupExpiredBatches() {
    try {
      const expiredThreshold = new Date(Date.now() - 12 * 60 * 60 * 1000).toISOString()

      // Fetch URIs that are about to be deleted (for Qdrant cleanup)
      const expiredRows = await this.db
        .selectFrom('user_candidate_batch')
        .select('uri')
        .where('generatedAt', '<', expiredThreshold)
        .execute()

      const expiredUris = [...new Set(expiredRows.map(r => r.uri))]

      // Delete expired SQLite rows
      const deleted = await this.db
        .deleteFrom('user_candidate_batch')
        .where('generatedAt', '<', expiredThreshold)
        .executeTakeFirst()

      if (deleted.numDeletedRows > 0) {
        console.log(`[Batch Cleanup] Removed ${deleted.numDeletedRows} expired candidate batch rows`)

        // Find orphaned URIs (no longer referenced by any remaining batch)
        if (expiredUris.length > 0) {
          const stillReferenced = await this.db
            .selectFrom('user_candidate_batch')
            .select('uri')
            .where('uri', 'in', expiredUris)
            .execute()
          const stillReferencedSet = new Set(stillReferenced.map(r => r.uri))
          const orphanedUris = expiredUris.filter(u => !stillReferencedSet.has(u))

          // Delete orphaned points from Qdrant
          if (orphanedUris.length > 0) {
            try {
              await qdrantDB.getClient().delete('feed_post_embeddings', {
                filter: {
                  must: [{
                    key: 'uri',
                    match: { any: orphanedUris.slice(0, 500) } // Batch limit
                  }]
                }
              })
              console.log(`[Batch Cleanup] Removed ${orphanedUris.length} orphaned Qdrant points`)
            } catch (err) {
              console.error('[Batch Cleanup] Failed to cleanup Qdrant points:', err)
            }
          }
        }
      }
    } catch (err) {
      console.error('Failed to cleanup expired batches', err)
    }
  }

  private async cleanupDebugLogs() {
    // Debug logging disabled - no cleanup needed
    // try {
    //   const debugRetentionLimit = new Date(Date.now() - 5 * 60 * 1000).toISOString()
    //   const deleted = await this.db
    //     .deleteFrom('feed_debug_log')
    //     .where('servedAt', '<', debugRetentionLimit)
    //     .executeTakeFirst()

    //   if (deleted.numDeletedRows > 0) {
    //     console.log(`[Debug Cleanup] Removed ${deleted.numDeletedRows} old debug log entries`)
    //   }
    // } catch (err) {
    //   console.error('Failed to cleanup debug logs', err)
    // }
  }
}

export default FeedGenerator
