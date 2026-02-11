import { Server } from '../lexicon'
import { AppContext } from '../config'
import { InputSchema } from '../lexicon/types/app/bsky/feed/sendInteractions'

export default function (server: Server, ctx: AppContext) {
  server.app.bsky.feed.sendInteractions(async ({ input }) => {
    // For now, we just accept the interactions without doing anything with them
    // This declares that the feed supports the sendInteractions API
    console.log('Received interactions request - full input:', JSON.stringify(input, null, 2))
    
    // The actual data is nested in input.body.interactions
    const interactions = (input as any).body?.interactions
    console.log('Received interactions - interactions field:', interactions)
    
    if (interactions) {
      console.log(`✅ Received ${interactions.length} interactions:`)
      interactions.forEach((interaction: any, index: number) => {
        console.log(`  ${index + 1}. Event: ${interaction.event}, Item: ${interaction.item}`)
      })
    }
    
    return {
      encoding: 'application/json',
      body: {},
    }
  })
}
