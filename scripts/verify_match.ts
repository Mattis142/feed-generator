import { AtpAgent } from '@atproto/api'
import { qdrantDB } from '../src/db/qdrant'
import { writeFile, readFile } from 'fs/promises'
import { join } from 'path'
import { exec } from 'child_process'
import { promisify } from 'util'
import * as dotenv from 'dotenv'

dotenv.config()

const execAsync = promisify(exec)

async function run() {
    // 1. Constants
    const postsToCheck = [
        { handle: 'nnaly.bsky.social', rkey: '3mem5gam4322f', label: 'Reported 1 (nnaly)' },
        { handle: 'candyhearse.bsky.social', rkey: '3meo552pi3k2v', label: 'Reported 2 (candyhearse)' },
        { handle: 'oughta.bsky.social', rkey: '3meovoghy2n2y', label: 'Reported 3 (oughta - porn)' },
        { handle: 'neoforme.me', rkey: '3meofkm65ss2l', label: 'Reported 4 (neoforme - porn)' }
    ]

    const agent = new AtpAgent({ service: 'https://public.api.bsky.app' })

    // 2. Fetch all User Profiles from Qdrant
    console.log('Fetching all user profiles from Qdrant...')
    const allProfilesResult = await qdrantDB.getClient().scroll('feed_user_profiles', {
        with_payload: true,
        with_vector: true,
        limit: 100
    })

    // Group centroids by User DID
    const userProfiles: Record<string, Array<{ vector: number[], weight: number, clusterId: number }>> = {}

    for (const point of allProfilesResult.points) {
        const payload = point.payload
        if (!payload) continue
        const did = payload.userDid as string
        if (!did) continue

        if (!userProfiles[did]) userProfiles[did] = []
        userProfiles[did].push({
            vector: point.vector as number[],
            weight: (payload.weight as number) || 1.0,
            clusterId: (payload.clusterId as number) || 0
        })
    }

    const userDids = Object.keys(userProfiles)
    console.log(`Found ${userDids.length} users with profiles: ${userDids.join(', ')}`)

    // 3. Process Each Post
    for (const postInfo of postsToCheck) {
        console.log(`\n\n=== Processing ${postInfo.label} ===`)
        let postAuthorDid = ''
        try {
            const resolved = await agent.resolveHandle({ handle: postInfo.handle })
            postAuthorDid = resolved.data.did
        } catch (e) {
            console.error(`Failed to resolve handle ${postInfo.handle}`)
            continue
        }

        const postUri = `at://${postAuthorDid}/app.bsky.feed.post/${postInfo.rkey}`

        // Fetch Post Details
        const res = await agent.getPosts({ uris: [postUri] })
        if (!res.success || res.data.posts.length === 0) {
            console.error(`Failed to fetch post ${postUri}`)
            continue
        }

        const postView = res.data.posts[0]
        const record = postView.record as any

        const image_urls: string[] = []
        const alt_text: string[] = []

        if (postView.embed && postView.embed.images) {
            // @ts-ignore
            postView.embed.images.forEach((img: any) => {
                if (img.fullsize) image_urls.push(img.fullsize)
                if (img.alt) alt_text.push(img.alt)
            })
        }

        const postData = [{
            uri: postView.uri,
            text: record.text || '',
            image_urls,
            alt_text
        }]

        console.log(`Fetched post content for ${postInfo.label}`)
        console.log(`Text: "${record.text?.slice(0, 50)}..."`)

        // Run Embedding
        const tempDir = join(process.cwd(), 'temp_batch')
        await execAsync(`mkdir -p ${tempDir}`)
        const inputPath = join(tempDir, `verify_input_${postInfo.rkey}.json`)
        const outputPath = join(tempDir, `verify_output_${postInfo.rkey}.json`)

        await writeFile(inputPath, JSON.stringify(postData))

        const modelPath = join(process.cwd(), 'models', 'mobileclip2_s2.pt')
        const scriptPath = join(process.cwd(), 'scripts', 'embed_posts.py')

        try {
            await execAsync(`python3 "${scriptPath}" "${inputPath}" "${outputPath}" --model-path "${modelPath}"`)
        } catch (e: any) {
            console.error('Embedding failed:', e.message)
            continue
        }

        const embeddingResult = JSON.parse(await readFile(outputPath, 'utf-8'))
        const vector = embeddingResult[0].vector

        // Compare against ALL users
        console.log(`\n--- Similarity Scores for ${postInfo.label} ---`)

        for (const userDid of userDids) {
            const centroids = userProfiles[userDid]
            let maxSimilarity = -1
            let bestCluster = -1

            for (const centroid of centroids) {
                // Cosine Similarity
                const dotProduct = vector.reduce((sum: number, val: number, i: number) => sum + val * centroid.vector[i], 0)
                const magA = Math.sqrt(vector.reduce((sum: number, val: number) => sum + val * val, 0))
                const magB = Math.sqrt(centroid.vector.reduce((sum: number, val: number) => sum + val * val, 0))
                const similarity = dotProduct / (magA * magB)

                if (similarity > maxSimilarity) {
                    maxSimilarity = similarity
                    bestCluster = centroid.clusterId
                }
            }

            const decision = maxSimilarity > 0.3 ? "MATCH ✅" : "NO MATCH ❌"
            const userLabel = userDid.startsWith('did:plc:us') ? 'FURRY (4qn)' : 'PROFESSIONAL (j5w)' // HEURISTIC based on user input
            console.log(`${userLabel} (${userDid.slice(0, 8)}...): Score ${maxSimilarity.toFixed(4)} -> ${decision}`)
        }
    }
}

run().catch(console.error)
