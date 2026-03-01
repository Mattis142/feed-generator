import { Server } from '../lexicon'
import { AppContext } from '../config'
import { InputSchema } from '../lexicon/types/app/bsky/feed/sendInteractions'
import { validateAuth } from '../auth'

export default function (server: Server, ctx: AppContext) {
  server.app.bsky.feed.sendInteractions(async ({ input, req }) => {
    console.log('Received interactions request - full input:', JSON.stringify(input, null, 2))

    // Get the actual user DID from authentication
    const userDid = await validateAuth(req, ctx.cfg.serviceDid, ctx.didResolver)

    // The actual data is nested in input.body.interactions
    const interactions = (input as any).body?.interactions
    console.log('Received interactions - interactions field:', interactions)

    if (interactions && interactions.length > 0) {
      console.log(`✅ Received ${interactions.length} interactions:`)
      interactions.forEach((interaction: any, index: number) => {
        console.log(`  ${index + 1}. Event: ${interaction.event}, Item: ${interaction.item}`)
      })

      // Store seen interactions in the database
      const now = new Date().toISOString()
      const seenRecords = interactions
        .filter((interaction: any) =>
          interaction.event === 'app.bsky.feed.defs#interactionSeen' &&
          interaction.item
        )
        .map((interaction: any) => ({
          userDid: userDid, // Use actual user DID
          uri: interaction.item,
          seenAt: now,
        }))

      if (seenRecords.length > 0) {
        try {
          // Batch insert seen interactions
          await ctx.db
            .insertInto('user_seen_post')
            .values(seenRecords)
            .onConflict((oc) => oc.columns(['userDid', 'uri']).doNothing())
            .execute()

          console.log(`[Seen Interactions] Stored ${seenRecords.length} seen posts for user ${userDid.slice(0, 10)}...`)

          // Trigger affinity decay for seen but ignored posts
          const { updateAffinityOnSeen } = await import('../algos/social-graph')
          for (const record of seenRecords) {
            await updateAffinityOnSeen(ctx, userDid, record.uri)
          }
        } catch (error) {
          console.error('[Seen Interactions] Failed to store seen interactions:', error)
        }
      }

      // Handle explicit feedback (Like/Dislike "Show Less/More")
      const feedbackInteractions = interactions.filter((interaction: any) =>
        interaction.event === 'app.bsky.feed.defs#interactionLike' ||
        interaction.event === 'app.bsky.feed.defs#interactionDislike' ||
        interaction.event === 'app.bsky.feed.defs#requestLess' ||
        interaction.event === 'app.bsky.feed.defs#requestMore'
      )

      // Log clickthrough and share interactions (without processing)
      const clickthroughAndShareInteractions = interactions.filter((interaction: any) =>
        interaction.event === 'app.bsky.feed.defs#interactionShare' ||
        interaction.event === 'app.bsky.feed.defs#clickthroughItem' ||
        interaction.event === 'app.bsky.feed.defs#clickthroughAuthor' ||
        interaction.event === 'app.bsky.feed.defs#clickthroughReposter' ||
        interaction.event === 'app.bsky.feed.defs#clickthroughEmbed'
      )

      if (clickthroughAndShareInteractions.length > 0) {
        console.log(`[Clickthrough/Share] Received ${clickthroughAndShareInteractions.length} interactions:`)
        clickthroughAndShareInteractions.forEach((interaction: any, index: number) => {
          console.log(`  ${index + 1}. Event: ${interaction.event}, Item: ${interaction.item}`)
        })
      }

      if (feedbackInteractions.length > 0) {
        const { handleInteractionFeedback } = await import('../algos/social-graph')
        for (const interaction of feedbackInteractions) {
          const isPositive =
            interaction.event === 'app.bsky.feed.defs#interactionLike' ||
            interaction.event === 'app.bsky.feed.defs#requestMore'

          const strength =
            interaction.event === 'app.bsky.feed.defs#requestMore' ||
              interaction.event === 'app.bsky.feed.defs#requestLess'
              ? 'strong'
              : 'weak'

          await handleInteractionFeedback(
            ctx,
            userDid,
            interaction.item,
            isPositive ? 'more' : 'less',
            strength
          )
          console.log(`[Feedback] Processed ${strength} ${isPositive ? 'more' : 'less'} for ${interaction.item}`)
        }
      }
    }

    return {
      encoding: 'application/json',
      body: {},
    }
  })
}
