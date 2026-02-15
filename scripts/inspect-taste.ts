import dotenv from 'dotenv'
import { qdrantDB } from '../src/db/qdrant'
import { createDb } from '../src/db'

async function inspectTaste(userDid: string, clusterId?: number) {
    dotenv.config()
    const db = createDb(process.env.FEEDGEN_SQLITE_LOCATION ?? ':memory:')

    console.log(`\n🔍 Inspecting tastes for: ${userDid}`)

    // 1. Fetch search centroids
    const profileResult = await qdrantDB.getClient().scroll('feed_user_profiles', {
        filter: {
            must: [
                { key: 'userDid', match: { value: userDid } },
                ...(clusterId !== undefined ? [{ key: 'clusterId', match: { value: clusterId } }] : [])
            ]
        },
        limit: 10,
        with_payload: true,
        with_vector: true,
    })

    if (profileResult.points.length === 0) {
        console.log('❌ No interests found for this user.')
        return
    }

    for (const point of profileResult.points) {
        const p = point.payload as any
        console.log(`\n==================================================`)
        console.log(`📂 CATEGORY: Cluster ${p.clusterId}`)
        console.log(`📊 STRENGTH: ${(p.weight * 100).toFixed(1)}% (based on ${p.postCount} recent likes)`)
        console.log(`==================================================`)

        // 2. Search for the most representative posts for this vector
        const matches = await qdrantDB.getClient().search('feed_post_embeddings', {
            vector: point.vector as number[],
            limit: 5,
            filter: {
                must: [{ key: 'discoveredBy', match: { value: userDid } }]
            },
            with_payload: true
        })

        console.log('Top Representative Posts:')
        for (const match of matches) {
            const payload = match.payload as any
            const uri = payload.uri as string

            // Convert URI to bsky.app link
            // at://did:plc:abc/app.bsky.feed.post/123 -> https://bsky.app/profile/did:plc:abc/post/123
            const link = uri.replace('at://', 'https://bsky.app/profile/').replace('/app.bsky.feed.post/', '/post/')

            // Fetch text from local DB
            const post = await db.selectFrom('post')
                .select(['text', 'author'])
                .where('uri', '=', uri)
                .executeTakeFirst()

            if (post) {
                console.log(`  [Score: ${match.score.toFixed(3)}]`)
                console.log(`  "${post.text?.replace(/\n/g, ' ').slice(0, 80)}..."`)
                console.log(`  🔗 Link: ${link}`)
                console.log(`  ------------------------------------------`)
            } else {
                console.log(`  [Score: ${match.score.toFixed(3)}]`)
                console.log(`  🔗 Link: ${link}`)
                console.log(`  ------------------------------------------`)
            }
        }
    }
}

// Example usage: inspectTaste('did:plc:usniyyo2axaoe4mauxeyd4qn')
const targetDid = process.argv[2]
if (targetDid) {
    inspectTaste(targetDid).catch(console.error)
} else {
    console.log('Please provide a userDid as an argument.')
}
