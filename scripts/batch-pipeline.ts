import dotenv from 'dotenv'
import { createDb, migrateToLatest } from '../src/db'
import { handler } from '../src/algos/social-graph'
import { AppContext } from '../src/config'
import { qdrantDB } from '../src/db/qdrant'
import { writeFile, mkdir, readFile, unlink } from 'fs/promises'
import { join } from 'path'
import { exec } from 'child_process'
import { promisify } from 'util'
import crypto from 'crypto'
import { AtpAgent } from '@atproto/api'
import { DidResolver, MemoryCache } from '@atproto/identity'

const execAsync = promisify(exec)

/**
 * Batch Pipeline Orchestrator
 * 
 * Runs the full feed pipeline in batch mode, embeds candidates,
 * builds user profiles, computes semantic matches, and stores
 * pre-computed candidate batches.
 * 
 * Steps:
 * 1. Run existing social-graph pipeline in batch mode (looser params, no fatigue)
 * 2. Embed candidate texts via Python MobileCLIP2-S2 service
 * 3. Build/refresh user profile (HDBSCAN multi-centroid)
 * 4. Query Qdrant for semantically similar posts per centroid
 * 5. Store top 300 candidates in user_candidate_batch table
 */

async function run() {
    dotenv.config()

    const postgresConnectionString = process.env.POSTGRES_CONNECTION_STRING ?? 'postgresql://bsky:bskypassword@localhost:5432/repo'
    const db = createDb(postgresConnectionString)
    await migrateToLatest(db)

    // Initialize Qdrant collections
    try {
        await qdrantDB.ensureFeedCollections()
    } catch (err) {
        console.error('[Batch Pipeline] Failed to initialize Qdrant collections:', err)
        console.error('[Batch Pipeline] Make sure Qdrant is running: docker compose up -d qdrant')
        process.exit(1)
    }

    const tempDir = join(process.cwd(), 'temp_batch')
    try {
        await mkdir(tempDir, { recursive: true })
    } catch (err) {
        // Directory might already exist
    }

    // Build AppContext for the pipeline
    const hostname = process.env.FEEDGEN_HOSTNAME ?? 'localhost'
    const serviceDid = process.env.FEEDGEN_SERVICE_DID ?? `did:web:${hostname}`
    const publisherDid = process.env.FEEDGEN_PUBLISHER_DID ?? ''
    const whitelistStr = process.env.FEEDGEN_WHITELIST ?? ''
    const whitelist = whitelistStr ? whitelistStr.split(',').map(d => d.trim()) : []

    const didCache = new MemoryCache()
    const didResolver = new DidResolver({
        plcUrl: 'https://plc.directory',
        didCache,
    })

    const ctx: AppContext = {
        db,
        didResolver,
        cfg: {
            port: 0, // Not serving
            listenhost: '',
            hostname,
            postgresConnectionString,
            subscriptionEndpoint: '',
            subscriptionReconnectDelay: 3000,
            serviceDid,
            publisherDid,
            whitelist,
        },
    }

    // Get active users (whitelisted users)
    let activeUsers: string[] = []
    if (whitelist.length > 0) {
        activeUsers = whitelist
    } else {
        // Fallback: get users who have been served posts recently
        const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString()
        const rows = await db
            .selectFrom('user_served_post')
            .select('userDid')
            .distinct()
            .where('servedAt', '>', sevenDaysAgo)
            .execute()
        activeUsers = rows.map(r => r.userDid)
    }

    console.log(`[Batch Pipeline] Processing ${activeUsers.length} active users`)

    // Process each user
    for (const userDid of activeUsers) {
        console.log(`\n[Batch Pipeline] ========== User: ${userDid.slice(0, 20)}... ==========`)

        try {
            // --- Step 1: Run existing pipeline in batch mode ---
            console.log('[Batch Pipeline] Step 1: Running pipeline in batch mode...')
            const result = await handler(ctx, { feed: '', limit: 2000 }, userDid, { batchMode: true }) as any

            const batchData = result._batchData
            if (!batchData || batchData.length === 0) {
                console.log('[Batch Pipeline] No candidates from pipeline, skipping user')
                continue
            }
            console.log(`[Batch Pipeline] Got ${batchData.length} candidates from pipeline`)

            // --- Step 2: Embed candidate texts ---
            console.log('[Batch Pipeline] Step 2: Embedding candidate texts...')

            // Filter to posts with text that haven't been embedded yet
            const postsWithText = batchData.filter((p: any) => p.text && p.text.trim().length > 10)

            // Check which posts are already in Qdrant
            const existingUris = new Set<string>()
            try {
                // Batch check in groups of 100
                for (let i = 0; i < postsWithText.length; i += 100) {
                    const batch = postsWithText.slice(i, i + 100)
                    const uris = batch.map((p: any) => p.uri)

                    const scrollResult = await qdrantDB.getClient().scroll('feed_post_embeddings', {
                        filter: {
                            must: [
                                { key: 'uri', match: { any: uris } },
                                { key: 'discoveredBy', match: { value: userDid } }
                            ]
                        },
                        limit: 100,
                        with_payload: ['uri'],
                        with_vector: false,
                    })

                    for (const point of scrollResult.points) {
                        existingUris.add(point.payload?.uri as string)
                    }
                }
            } catch (err) {
                console.log('[Batch Pipeline] Qdrant scroll failed (collection may be empty), embedding all posts')
            }

            const postsToEmbed = postsWithText.filter((p: any) => !existingUris.has(p.uri))
            console.log(`[Batch Pipeline] ${postsWithText.length} posts with text, ${existingUris.size} already embedded, ${postsToEmbed.length} to embed`)

            if (postsToEmbed.length > 0) {
                // Optimize: Only fetch posts with images from AppView
                // Use DB text by default, only fetch image metadata
                console.log('[Batch Pipeline] Optimizing AppView fetching for image metadata only...')

                // Separate posts by image content
                const postsWithImages = postsToEmbed.filter((p: any) => p.hasImage === 1)
                const postsWithoutImages = postsToEmbed.filter((p: any) => p.hasImage !== 1)

                // Check for posts without text (rare fallback case)
                const postsNeedingTextCheck = postsWithoutImages.filter((p: any) => !p.text || p.text.trim().length === 0)
                const postsWithValidText = postsWithoutImages.filter((p: any) => p.text && p.text.trim().length > 0)

                console.log(`[Batch Pipeline] Posts analysis: ${postsWithImages.length} with images, ${postsWithValidText.length} with text only, ${postsNeedingTextCheck.length} need text fetch`)

                const richPosts: any[] = []

                // Add posts with valid text and no images immediately (no AppView needed)
                for (const post of postsWithValidText) {
                    richPosts.push({
                        uri: post.uri,
                        text: post.text,
                        image_urls: [],
                        alt_text: []
                    })
                }

                // Only fetch posts with images or missing text from AppView
                const postsToFetch = [...postsWithImages, ...postsNeedingTextCheck]

                if (postsToFetch.length > 0) {
                    console.log(`[Batch Pipeline] Fetching ${postsToFetch.length} posts from AppView (image metadata + text fallback)...`)
                    const publicAgent = new AtpAgent({ service: 'https://public.api.bsky.app' })

                    // Fetch in batches of 25 (AppView limit)
                    const urisToFetch = postsToFetch.map((p: any) => p.uri)
                    for (let i = 0; i < urisToFetch.length; i += 25) {
                        const batchUris = urisToFetch.slice(i, i + 25)
                        try {
                            const res = await publicAgent.getPosts({ uris: batchUris })
                            if (res.success) {
                                for (const postView of res.data.posts) {
                                    const record = postView.record as any
                                    const image_urls: string[] = []
                                    const alt_text: string[] = []

                                    if (postView.embed) {
                                        // Handle images
                                        if (postView.embed.images) {
                                            // @ts-ignore
                                            postView.embed.images.forEach((img: any) => {
                                                if (img.fullsize) image_urls.push(img.fullsize)
                                                else if (img.thumb) image_urls.push(img.thumb)
                                                if (img.alt) alt_text.push(img.alt)
                                            })
                                        }
                                        // Handle record embeds with images (external, etc)
                                        // ... basic support for now
                                    }

                                    // Find the original post data to get DB text
                                    const originalPost = postsToFetch.find(p => p.uri === postView.uri)

                                    richPosts.push({
                                        uri: postView.uri,
                                        text: originalPost?.text || record.text || '',  // Prefer DB text, fallback to AppView
                                        image_urls,
                                        alt_text
                                    })
                                }
                            }
                        } catch (err) {
                            console.error(`[Batch Pipeline] Failed to fetch post details for batch ${i}:`, err)
                        }
                    }
                }

                // Write enriched posts to temp file for Python script
                const inputPath = join(tempDir, `embed_input_${userDid.replace(/:/g, '_')}.json`)
                const outputPath = join(tempDir, `embed_output_${userDid.replace(/:/g, '_')}.json`)

                await writeFile(inputPath, JSON.stringify(richPosts))

                // Run Python embedding script
                const modelPath = join(process.cwd(), 'models', 'mobileclip2_s2.pt')
                const scriptPath = join(process.cwd(), 'scripts', 'embed_posts.py')

                try {
                    const { stdout, stderr } = await execAsync(
                        `python3 "${scriptPath}" "${inputPath}" "${outputPath}" --model-path "${modelPath}" --batch-size 32`,
                        { maxBuffer: 50 * 1024 * 1024 } // 50MB buffer for large outputs
                    )
                    if (stderr) console.log(`[Batch Pipeline] Python stderr: ${stderr.slice(0, 500)}`)

                    // Read embeddings and upsert to Qdrant
                    const embeddingsRaw = await readFile(outputPath, 'utf-8')
                    const embeddings: Array<{ uri: string; vector: number[] }> = JSON.parse(embeddingsRaw)

                    // Build a map from uri to post data for payloads
                    const postMap = new Map<string, any>()
                    for (const p of batchData) {
                        postMap.set(p.uri, p)
                    }

                    // Upsert to Qdrant in batches of 100
                    const points = embeddings
                        .filter(e => e.vector && e.vector.length === 512 && e.vector.some(v => v !== 0))
                        .map((e, idx) => {
                            const post = postMap.get(e.uri)
                            return {
                                id: hashUri(`${userDid}:${e.uri}`), // Isolated ID per user
                                vector: e.vector,
                                payload: {
                                    uri: e.uri,
                                    author: post?.author || '',
                                    indexedAt: post?.indexedAt || '',
                                    likeCount: post?.likeCount || 0,
                                    discoveredBy: userDid, // THE SIGNATURE
                                },
                            }
                        })

                    for (let i = 0; i < points.length; i += 100) {
                        const batch = points.slice(i, i + 100)
                        await qdrantDB.getClient().upsert('feed_post_embeddings', {
                            points: batch,
                        })
                    }
                    console.log(`[Batch Pipeline] Embedded and stored ${points.length} post vectors in Qdrant`)

                    // Cleanup temp files
                    await unlink(inputPath).catch(() => { })
                    await unlink(outputPath).catch(() => { })

                } catch (err) {
                    console.error('[Batch Pipeline] Embedding failed:', err)
                    // Continue anyway — we can still use existing embeddings
                }
            }

            // --- Step 3: Build/refresh user profile ---
            console.log('[Batch Pipeline] Step 3: Building user profile...')

            // Fetch user's recent interactions (piggybacking on daily-keywords pattern)
            const repoAgent = new AtpAgent({ service: 'https://bsky.social' })
            const publicAgent = new AtpAgent({ service: 'https://public.api.bsky.app' })

            // Get liked post URIs from our local DB first (faster than API)
            const threeDaysAgo = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString()
            const recentLikes = await db
                .selectFrom('graph_interaction')
                .select(['target', 'type'])
                .where('actor', '=', userDid)
                .where('type', 'in', ['like', 'repost'])
                .where('indexedAt', '>', threeDaysAgo)
                .limit(200)
                .execute()

            // Also get explicit feedback interactions
            const recentFeedback = await db
                .selectFrom('graph_interaction')
                .select(['target', 'type', 'weight'])
                .where('actor', '=', userDid)
                .where('type', 'in', ['requestMore', 'requestLess'] as any[])
                .where('indexedAt', '>', new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString())
                .limit(100)
                .execute()

            // Retrieve embeddings for liked posts from Qdrant
            const likedUris = [...recentLikes.map(l => l.target), ...recentFeedback.map(f => f.target)]
            const uniqueLikedUris = [...new Set(likedUris)]
            const likedEmbeddings: Array<{ vector: number[]; weight: number; interactionType: string }> = []
            const foundUris = new Set<string>()

            if (uniqueLikedUris.length > 0) {
                // First pass: check which liked posts are already embedded in Qdrant
                try {
                    for (let i = 0; i < uniqueLikedUris.length; i += 100) {
                        const batch = uniqueLikedUris.slice(i, i + 100)
                        const scrollResult = await qdrantDB.getClient().scroll('feed_post_embeddings', {
                            filter: {
                                must: [
                                    { key: 'uri', match: { any: batch } },
                                    { key: 'discoveredBy', match: { value: userDid } }
                                ]
                            },
                            limit: 100,
                            with_payload: ['uri'],
                            with_vector: true,
                        })

                        for (const point of scrollResult.points) {
                            const uri = point.payload?.uri as string
                            const vector = point.vector as number[]
                            if (!vector || vector.length !== 512) continue
                            foundUris.add(uri)

                            const like = recentLikes.find(l => l.target === uri)
                            const feedback = recentFeedback.find(f => f.target === uri)
                            let interactionType = 'like'
                            if (feedback) {
                                interactionType = (feedback.type as string) === 'requestMore' ? 'requestMore' : 'requestLess'
                            } else if (like) {
                                interactionType = like.type
                            }

                            likedEmbeddings.push({ vector, weight: 1.0, interactionType })
                        }
                    }
                } catch (err) {
                    console.error('[Batch Pipeline] Failed to fetch liked embeddings from Qdrant:', err)
                }

                // Second pass: embed liked posts that AREN'T already in Qdrant
                const missingUris = uniqueLikedUris.filter(u => !foundUris.has(u))
                if (missingUris.length > 0) {
                    console.log(`[Batch Pipeline] ${missingUris.length} liked posts not yet embedded — embedding now...`)

                    // Fetch text for missing liked posts from local post table
                    const missingPosts = await db
                        .selectFrom('post')
                        .select(['uri', 'text', 'author', 'indexedAt', 'likeCount'])
                        .where('uri', 'in', missingUris)
                        .execute()

                    // Enrich with AppView data for images
                    console.log(`[Batch Pipeline] Fetching full post details for ${missingPosts.length} liked posts...`)
                    const richLikedPosts: any[] = []

                    // Batch fetch from AppView
                    const urisToFetch = missingPosts.map((p: any) => p.uri)
                    for (let i = 0; i < urisToFetch.length; i += 25) {
                        const batchUris = urisToFetch.slice(i, i + 25)
                        try {
                            const res = await publicAgent.getPosts({ uris: batchUris })
                            if (res.success) {
                                for (const postView of res.data.posts) {
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

                                    richLikedPosts.push({
                                        uri: postView.uri,
                                        text: record.text || '',
                                        image_urls,
                                        alt_text
                                    })
                                }
                            }
                        } catch (err) {
                            console.error(`[Batch Pipeline] Failed to fetch liked post details batch ${i}:`, err)
                        }
                    }

                    if (richLikedPosts.length > 0) {
                        // Embed them using the same Python script
                        const likeEmbedInputPath = join(tempDir, `like_embed_input_${userDid.replace(/:/g, '_')}.json`)
                        const likeEmbedOutputPath = join(tempDir, `like_embed_output_${userDid.replace(/:/g, '_')}.json`)

                        await writeFile(likeEmbedInputPath, JSON.stringify(richLikedPosts))

                        try {
                            const modelPath = join(process.cwd(), 'models', 'mobileclip2_s2.pt')
                            const { stderr: likeStderr } = await execAsync(
                                `python3 "${join(process.cwd(), 'scripts', 'embed_posts.py')}" "${likeEmbedInputPath}" "${likeEmbedOutputPath}" --model-path "${modelPath}" --batch-size 32`
                            )
                            if (likeStderr) console.log('[Batch Pipeline] Liked-post embed stderr:', likeStderr.trim().split('\n').slice(-2).join(' | '))

                            const likeEmbedResult = JSON.parse(await readFile(likeEmbedOutputPath, 'utf-8'))

                            // Store in Qdrant
                            const likePoints = likeEmbedResult
                                .filter((r: any) => r.vector && r.vector.length === 512)
                                .map((r: any) => {
                                    const postData = missingPosts.find(p => p.uri === r.uri)
                                    // Generate deterministic UUID from URI + UserDID for isolation
                                    const hash = crypto.createHash('md5').update(`${userDid}:${r.uri}`).digest('hex')
                                    const uuid = `${hash.substring(0, 8)}-${hash.substring(8, 12)}-${hash.substring(12, 16)}-${hash.substring(16, 20)}-${hash.substring(20)}`

                                    return {
                                        id: uuid,
                                        vector: r.vector,
                                        payload: {
                                            uri: r.uri,
                                            author: postData?.author || '',
                                            indexedAt: postData?.indexedAt || '',
                                            likeCount: postData?.likeCount || 0,
                                            discoveredBy: userDid, // THE SIGNATURE
                                        }
                                    }
                                })

                            if (likePoints.length > 0) {
                                for (let i = 0; i < likePoints.length; i += 100) {
                                    await qdrantDB.getClient().upsert('feed_post_embeddings', {
                                        points: likePoints.slice(i, i + 100),
                                    })
                                }
                                console.log(`[Batch Pipeline] Embedded and stored ${likePoints.length} liked-post vectors in Qdrant`)
                            }

                            // Add to likedEmbeddings for profile building
                            for (const r of likeEmbedResult) {
                                if (!r.vector || r.vector.length !== 512) continue
                                const like = recentLikes.find(l => l.target === r.uri)
                                const feedback = recentFeedback.find(f => f.target === r.uri)
                                let interactionType = 'like'
                                if (feedback) {
                                    interactionType = (feedback.type as string) === 'requestMore' ? 'requestMore' : 'requestLess'
                                } else if (like) {
                                    interactionType = like.type
                                }
                                likedEmbeddings.push({ vector: r.vector, weight: 1.0, interactionType })
                            }
                        } catch (err) {
                            console.error('[Batch Pipeline] Failed to embed liked posts:', err)
                        }

                        // Cleanup temp files
                        await unlink(likeEmbedInputPath).catch(() => { })
                        await unlink(likeEmbedOutputPath).catch(() => { })
                    }
                }
            }

            console.log(`[Batch Pipeline] Total liked-post embeddings for profile: ${likedEmbeddings.length} (${foundUris.size} cached, ${likedEmbeddings.length - foundUris.size} newly embedded)`)

            if (likedEmbeddings.length >= 3) {
                // Run HDBSCAN clustering via Python
                const profileInputPath = join(tempDir, `profile_input_${userDid.replace(/:/g, '_')}.json`)
                const profileOutputPath = join(tempDir, `profile_output_${userDid.replace(/:/g, '_')}.json`)

                await writeFile(profileInputPath, JSON.stringify(likedEmbeddings))

                const profileScript = join(process.cwd(), 'scripts', 'build_user_profile.py')
                try {
                    const { stdout, stderr } = await execAsync(
                        `python3 "${profileScript}" "${profileInputPath}" "${profileOutputPath}"`,
                        { maxBuffer: 10 * 1024 * 1024 }
                    )
                    if (stderr) console.log(`[Batch Pipeline] Profile stderr: ${stderr.slice(0, 500)}`)

                    // Read centroids and upsert to Qdrant
                    const centroidsRaw = await readFile(profileOutputPath, 'utf-8')
                    const centroids: Array<{ clusterId: number; centroid: number[]; weight: number; postCount: number }> = JSON.parse(centroidsRaw)

                    // Delete old profile points for this user
                    try {
                        await qdrantDB.getClient().delete('feed_user_profiles', {
                            filter: {
                                must: [{
                                    key: 'userDid',
                                    match: { value: userDid }
                                }]
                            }
                        })
                    } catch (err) {
                        // May fail if no points exist yet
                    }

                    // Insert new centroids
                    const profilePoints = centroids.map((c, idx) => ({
                        id: hashUri(`${userDid}:profile:${c.clusterId}`),
                        vector: c.centroid,
                        payload: {
                            userDid,
                            clusterId: c.clusterId,
                            weight: c.weight,
                            postCount: c.postCount,
                            updatedAt: new Date().toISOString(),
                        },
                    }))

                    if (profilePoints.length > 0) {
                        await qdrantDB.getClient().upsert('feed_user_profiles', {
                            points: profilePoints,
                        })
                    }

                    console.log(`[Batch Pipeline] Stored ${centroids.length} interest centroids for user`)
                    for (const c of centroids) {
                        console.log(`  Cluster ${c.clusterId}: ${c.postCount} posts, weight=${c.weight.toFixed(3)}`)
                    }

                    // Cleanup temp files
                    await unlink(profileInputPath).catch(() => { })
                    await unlink(profileOutputPath).catch(() => { })

                } catch (err) {
                    console.error('[Batch Pipeline] Profile building failed:', err)
                }
            } else {
                console.log('[Batch Pipeline] Too few interaction embeddings for profile building, skipping')
            }

            // --- Step 4: Query Qdrant for semantically similar posts ---
            console.log('[Batch Pipeline] Step 4: Querying semantic matches...')

            // Load user centroids from Qdrant
            let userCentroids: Array<{ vector: number[]; weight: number; clusterId: number }> = []
            try {
                const profileResult = await qdrantDB.getClient().scroll('feed_user_profiles', {
                    filter: {
                        must: [{
                            key: 'userDid',
                            match: { value: userDid }
                        }]
                    },
                    limit: 5,
                    with_payload: true,
                    with_vector: true,
                })

                userCentroids = profileResult.points.map(p => ({
                    vector: p.vector as number[],
                    weight: (p.payload?.weight as number) || 1.0,
                    clusterId: (p.payload?.clusterId as number) || 0,
                }))
            } catch (err) {
                console.log('[Batch Pipeline] No user profile found, skipping semantic matching')
            }

            if (userCentroids.length === 0) {
                console.log('[Batch Pipeline] No centroids available, skipping semantic matching')
                continue
            }

            // Get already-liked URIs and heavily-served URIs to exclude from results
            const likedUriSet = new Set(likedUris)

            // NEW: Also exclude posts that have been seen 3+ times to ensure fresh candidates
            const heavilySeenRows = await db.selectFrom('user_seen_post')
                .select('uri')
                .where('userDid', '=', userDid)
                .groupBy('uri')
                .having((eb) => eb.fn.count('uri'), '>=', 3)
                .execute()
            const heavilySeenUris = new Set(heavilySeenRows.map(r => r.uri))
            console.log(`[Batch Pipeline] Excluding ${heavilySeenUris.size} heavily seen posts from candidate search`)

            // NEW: Get authors with low taste reputation to exclude them
            const badReputationRows = await db.selectFrom('taste_reputation')
                .select('similarUserDid')
                .where('userDid', '=', userDid)
                .where('reputationScore', '<', 0.1)
                .execute()
            const excludedAuthors = new Set(badReputationRows.map(r => r.similarUserDid))
            console.log(`[Batch Pipeline] Excluding ${excludedAuthors.size} authors with low reputation`)

            // Query for similar posts per centroid
            const semanticCandidates: Array<{
                uri: string;
                semanticScore: number;
                centroidId: number;
                pipelineScore: number;
            }> = []

            // Build a pipeline score map
            const pipelineScoreMap = new Map<string, number>()
            for (const p of batchData) {
                pipelineScoreMap.set(p.uri, p.score)
            }

            for (const centroid of userCentroids) {
                // QUADRUPLE the search capacity to ensure we find enough new content
                const searchLimit = Math.round(400 * centroid.weight) + 200
                try {
                    const searchResult = await qdrantDB.getClient().search('feed_post_embeddings', {
                        vector: centroid.vector,
                        limit: searchLimit,
                        score_threshold: 0.25, // Slightly looser threshold for broader discovery
                        filter: {
                            must: [{
                                key: 'discoveredBy',
                                match: { value: userDid }
                            }]
                        },
                        with_payload: ['uri', 'author'],
                    })

                    for (const hit of searchResult) {
                        const uri = hit.payload?.uri as string
                        const author = hit.payload?.author as string
                        if (!uri || likedUriSet.has(uri) || heavilySeenUris.has(uri)) continue
                        if (author && excludedAuthors.has(author)) continue

                        // NEW: If the post was NOT in our social graph pipeline, apply the Discovery Sandbox Penalty
                        // This prevents unexpected "global" content from bypassing filters
                        let pipelineScore = pipelineScoreMap.get(uri) ?? -4000 // Standard sandbox penalty for new discovery

                        semanticCandidates.push({
                            uri,
                            semanticScore: hit.score,
                            centroidId: centroid.clusterId,
                            pipelineScore,
                        })
                    }
                } catch (err) {
                    console.error(`[Batch Pipeline] Semantic search failed for centroid ${centroid.clusterId}:`, err)
                }
            }

            // Deduplicate — keep the highest score per URI
            const deduped = new Map<string, typeof semanticCandidates[0]>()
            for (const c of semanticCandidates) {
                const existing = deduped.get(c.uri)
                if (!existing || c.semanticScore > existing.semanticScore) {
                    deduped.set(c.uri, c)
                }
            }

            // Sort by semantic score and take top 1500 (increased from 600 for high-load scrolling)
            const topCandidates = Array.from(deduped.values())
                .sort((a, b) => b.semanticScore - a.semanticScore)
                .slice(0, 1500)

            console.log(`[Batch Pipeline] Found ${semanticCandidates.length} raw matches, ${deduped.size} unique, keeping top ${topCandidates.length}`)

            // --- Step 5: Store batch in SQLite ---
            console.log('[Batch Pipeline] Step 5: Storing candidate batch...')

            const batchId = generateBatchId()
            const generatedAt = new Date().toISOString()

            if (topCandidates.length > 0) {
                // Insert in batches of 50 to avoid SQLite variable limits
                for (let i = 0; i < topCandidates.length; i += 50) {
                    const batch = topCandidates.slice(i, i + 50)
                    let retries = 3
                    while (retries > 0) {
                        try {
                            await db
                                .insertInto('user_candidate_batch')
                                .values(batch.map(c => ({
                                    userDid,
                                    uri: c.uri,
                                    semanticScore: c.semanticScore,
                                    pipelineScore: c.pipelineScore,
                                    centroidId: c.centroidId,
                                    batchId,
                                    generatedAt,
                                })))
                                .execute()
                            break // Success
                        } catch (err: any) {
                            if (err.code === 'SQLITE_BUSY' && retries > 1) {
                                console.log(`[Batch Pipeline] Database busy, retrying in 1s... (${retries - 1} left)`)
                                await new Promise(resolve => setTimeout(resolve, 1000))
                                retries--
                            } else {
                                throw err
                            }
                        }
                    }
                }

                console.log(`[Batch Pipeline] Stored ${topCandidates.length} candidates (batch: ${batchId.slice(0, 8)})`)
            }

            // --- Cleanup expired batches (older than 12 hours, impact = 0) ---
            const expiredThreshold = new Date(Date.now() - 12 * 60 * 60 * 1000).toISOString()
            const deleted = await db
                .deleteFrom('user_candidate_batch')
                .where('userDid', '=', userDid)
                .where('generatedAt', '<', expiredThreshold)
                .execute()
            console.log(`[Batch Pipeline] Cleaned up expired batches for user`)

        } catch (err) {
            console.error(`[Batch Pipeline] Error processing user ${userDid}:`, err)
            continue
        }
    }

    console.log('\n[Batch Pipeline] Completed')
    process.exit(0)
}

/**
 * Hash a URI to a deterministic numeric ID for Qdrant.
 * Uses a simple but collision-resistant hash.
 */
function hashUri(uri: string): number {
    let hash = 0
    for (let i = 0; i < uri.length; i++) {
        const chr = uri.charCodeAt(i)
        hash = ((hash << 5) - hash) + chr
        hash |= 0 // Convert to 32bit integer
    }
    // Ensure positive and within safe integer range
    return Math.abs(hash) % (Number.MAX_SAFE_INTEGER - 1) + 1
}

/**
 * Generate a short batch ID (8 hex chars)
 */
function generateBatchId(): string {
    const bytes = new Uint8Array(4)
    // Simple deterministic-ish ID from timestamp + random
    const now = Date.now()
    bytes[0] = now & 0xff
    bytes[1] = (now >> 8) & 0xff
    bytes[2] = (now >> 16) & 0xff
    bytes[3] = Math.floor(Math.random() * 256)
    return Array.from(bytes).map(b => b.toString(16).padStart(2, '0')).join('')
}

run().catch((err) => {
    console.error('Fatal error:', err)
    process.exit(1)
})
