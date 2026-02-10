import { InvalidRequestError, AuthRequiredError } from '@atproto/xrpc-server'
import { Server } from '../lexicon'
import { AppContext } from '../config'
import algos from '../algos'
import { validateAuth } from '../auth'
import { AtUri } from '@atproto/syntax'
import { GraphBuilder } from '../services/graph-builder'

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

      const body = await algo(ctx, params, requesterDid)

      // Record served posts for fatigue memory (background)
      const servedUris = body.feed.map(f => f.post)
      if (servedUris.length > 0) {
        ctx.db.insertInto('user_served_post')
          .values(servedUris.map(uri => ({
            userDid: requesterDid,
            uri,
            servedAt: new Date().toISOString(),
          })))
          .execute()
          .catch(err => console.error('Failed to record served posts', err))
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
