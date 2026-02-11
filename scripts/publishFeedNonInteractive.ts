import dotenv from 'dotenv'
import { AtpAgent, AppBskyFeedDefs } from '@atproto/api'
import { ids } from '../src/lexicon/lexicons'

const run = async () => {
    dotenv.config()

    const handle = process.env.BSKY_HANDLE
    const password = process.env.BSKY_PASSWORD
    const recordName = process.env.FEED_RECORD_NAME || 'social-graph'
    const displayName = process.env.FEED_DISPLAY_NAME || 'Social Graph Feed'
    const description = process.env.FEED_DESCRIPTION || 'A custom feed based on your social graph.'
    const service = process.env.BSKY_SERVICE || 'https://bsky.social'

    if (!handle || !password) {
        throw new Error('Please provide BSKY_HANDLE and BSKY_PASSWORD in .env or as environment variables')
    }

    if (!process.env.FEEDGEN_HOSTNAME) {
        throw new Error('Please provide FEEDGEN_HOSTNAME in the .env file')
    }

    const feedGenDid = process.env.FEEDGEN_SERVICE_DID ?? `did:web:${process.env.FEEDGEN_HOSTNAME}`

    const agent = new AtpAgent({ service })
    await agent.login({ identifier: handle, password })

    await agent.api.com.atproto.repo.putRecord({
        repo: agent.session?.did ?? '',
        collection: ids.AppBskyFeedGenerator,
        rkey: recordName,
        record: {
            did: feedGenDid,
            displayName: displayName,
            description: description,
            acceptsInteractions: true,
            createdAt: new Date().toISOString(),
            contentMode: AppBskyFeedDefs.CONTENTMODEUNSPECIFIED,
        },
    })

    console.log('All done 🎉')
}

run().catch(console.error)
