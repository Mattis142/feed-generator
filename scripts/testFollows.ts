import { BskyAgent } from '@atproto/api'

const run = async () => {
    // Try public AppView instead of the PDS
    const agent = new BskyAgent({ service: 'https://public.api.bsky.app' })
    const userDid = 'did:plc:c7m7glv2pfjwvgmtkt6kej5w'

    try {
        console.log(`Testing getFollows for ${userDid}...`)
        const res = await agent.getFollows({ actor: userDid, limit: 10 })
        console.log(`Success! Found ${res.data.follows.length} follows.`)
        res.data.follows.forEach(f => console.log(`- ${f.handle} (${f.did})`))
    } catch (e) {
        console.error('Failed to fetch follows unauthenticated:', e)
    }
}

run()
