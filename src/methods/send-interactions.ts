import { Server } from '../lexicon'
import { AppContext } from '../config'
import { InputSchema } from '../lexicon/types/app/bsky/feed/sendInteractions'
import { validateAuth } from '../auth'
import { logger } from '../logger'

export default function (server: Server, ctx: AppContext) {
  server.app.bsky.feed.sendInteractions(async ({ input, req }) => {
    // Get the actual user DID from authentication
    const userDid = await validateAuth(req, ctx.cfg.serviceDid, ctx.didResolver)

    // Extract interactions from input
    const interactions = (input as any).body?.interactions || (input as any).interactions

    if (interactions && interactions.length > 0) {
      // Store seen interactions in the database
      const now = new Date().toISOString()
      const seenRecords = interactions
        .filter((interaction: any) =>
          interaction.event === 'app.bsky.feed.defs#interactionSeen' &&
          interaction.item
        )
        .map((interaction: any) => ({
          userDid: userDid,
          uri: interaction.item,
          seenAt: now,
        }))

      if (seenRecords.length > 0) {
        try {
          // Sync insert seen interactions (crucial for fatigue)
          await ctx.db
            .insertInto('user_seen_post')
            .values(seenRecords)
            .execute()


          // Background processing for everything else to keep response fast
          const processBackgroundInteractions = async () => {
            try {
              // Affinity updates
              const { updateAffinityOnSeen } = await import('../algos/social-graph')
              for (const record of seenRecords) {
                await updateAffinityOnSeen(ctx, userDid, record.uri)
              }

              // Explicit feedback
              const feedbackInteractions = interactions.filter((interaction: any) =>
                ['app.bsky.feed.defs#interactionLike', 'app.bsky.feed.defs#requestLess', 'app.bsky.feed.defs#requestMore'].includes(interaction.event)
              )

              if (feedbackInteractions.length > 0) {
                const { handleInteractionFeedback } = await import('../algos/social-graph')
                
                // Hard-filter and Like tracking: Insert explicit interactions into graph
                const explicitGraphInserts = feedbackInteractions
                  .filter((i: any) =>
                    i.event === 'app.bsky.feed.defs#requestLess' ||
                    i.event === 'app.bsky.feed.defs#interactionLike' ||
                    i.event === 'app.bsky.feed.defs#requestMore'
                  )
                  .map((i: any) => ({
                    actor: userDid,
                    target: i.item,
                    type: (i.event === 'app.bsky.feed.defs#requestLess'
                      ? 'hide'
                      : i.event === 'app.bsky.feed.defs#requestMore'
                        ? 'requestMore'
                        : 'like') as any,
                    weight: 1,
                    indexedAt: now
                  }))
                  
                if (explicitGraphInserts.length > 0) {
                  await ctx.db.insertInto('graph_interaction')
                    .values(explicitGraphInserts)
                    .onConflict((oc) => oc.doNothing())
                    .execute()
                    .catch(e => logger.error('[Interactions] Failed to insert explicit graph interactions', e))
                }

                for (const interaction of feedbackInteractions) {
                  const isPositive = interaction.event === 'app.bsky.feed.defs#interactionLike' || interaction.event === 'app.bsky.feed.defs#requestMore'
                  const strength = interaction.event.includes('request') ? 'strong' : 'weak'
                  await handleInteractionFeedback(ctx, userDid, interaction.item, isPositive ? 'more' : 'less', strength)
                }
              }
            } catch (err) {
              logger.error('[Interactions] Background processing failed:', err)
            }
          }
          processBackgroundInteractions() // No await: immediate return

        } catch (error) {
          logger.error('[Interactions] Failed to store seen interactions:', error)
        }
      }
    }

    return {
      encoding: 'application/json',
      body: {},
    }
  })
}
