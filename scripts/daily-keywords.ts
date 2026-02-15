import dotenv from 'dotenv'
import { AtpAgent } from '@atproto/api'
import { createDb, migrateToLatest } from '../src/db'
import { writeFile, mkdir } from 'fs/promises'
import { join } from 'path'
import { exec } from 'child_process'
import { promisify } from 'util'
import { sql } from 'kysely'

const execAsync = promisify(exec)

const run = async () => {
  dotenv.config()

  const sqliteLocation = process.env.FEEDGEN_SQLITE_LOCATION ?? ':memory:'
  const db = createDb(sqliteLocation)
  await migrateToLatest(db)

  // Get active users (users who have been served posts in the last 7 days)
  const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString()
  const activeUsers = await db
    .selectFrom('user_served_post')
    .select('userDid')
    .distinct()
    .where('servedAt', '>', sevenDaysAgo)
    .execute()

  console.log(`Found ${activeUsers.length} active users`)

  // Create temp directory for corpus files
  const tempDir = join(process.cwd(), 'temp_keywords')
  try {
    await mkdir(tempDir, { recursive: true })
  } catch (err) {
    // Directory might already exist
  }

  // Get background corpus (random 1000 posts with text)
  const backgroundPosts = await db
    .selectFrom('post')
    .select(['text'])
    .where('text', 'is not', null)
    .orderBy(sql`(RANDOM())`)
    .limit(1000)
    .execute()

  const backgroundTexts = backgroundPosts
    .map((p: any) => p.text as string | null)
    .filter((text): text is string => text !== null && text.trim().length > 0)

  const backgroundCorpusPath = join(tempDir, 'background_corpus.txt')
  await writeFile(backgroundCorpusPath, backgroundTexts.join('\n\n'), 'utf-8')
  console.log(`Created background corpus with ${backgroundTexts.length} posts`)

  // Create ATP agent for API calls - use PDS for listRecords, public API for getPosts
  const agent = new AtpAgent({
    service: process.env.FEEDGEN_PDS_URL ?? 'https://bsky.social',
  })

  // Authenticate if credentials are available
  if (process.env.BSKY_HANDLE && process.env.BSKY_PASSWORD) {
    console.log('Authenticating with Bluesky...')
    await agent.login({
      identifier: process.env.BSKY_HANDLE,
      password: process.env.BSKY_PASSWORD,
    })
    console.log('Authentication successful')
  }

  // Create unauthenticated agent for repo access (listRecords doesn't require auth)
  const repoAgent = new AtpAgent({
    service: 'https://bsky.social',
  })

  // Create separate agent for public API calls
  const publicAgent = new AtpAgent({
    service: 'https://public.api.bsky.app',
  })

  // Process each user
  for (const user of activeUsers) {
    const userDid = user.userDid
    console.log(`\nProcessing user: ${userDid.slice(0, 20)}...`)

    try {
      // Check if keywords already exist for this user
      const existingKeywordsCount = await db
        .selectFrom('user_keyword')
        .select([db.fn.count<number>('keyword').as('count')])
        .where('userDid', '=', userDid)
        .executeTakeFirst()

      const hasExistingKeywords = existingKeywordsCount && Number(existingKeywordsCount.count) > 0

      if (hasExistingKeywords) {
        console.log(`  User already has ${existingKeywordsCount?.count} keywords, skipping regeneration`)
        continue
      }

      // Clear existing keywords for fresh start (only if we're regenerating)
      await db.deleteFrom('user_keyword').where('userDid', '=', userDid).execute()
      console.log('  Cleared existing keywords')

      // Check if this is first run for this user (no keywords exist)
      const isFirstRun = true // Always treat as first run when regenerating

      // Fetch liked posts via listRecords (no auth required)
      const likedTexts: string[] = []
      let imageLikesCount = 0
      let videoLikesCount = 0
      let totalLikesWithMediaInfo = 0
      let cursor: string | undefined = undefined
      let fetchedCount = 0
      // On first run, fetch ALL likes (no limit). Otherwise, fetch last 100.
      const maxLikes = isFirstRun ? Number.MAX_SAFE_INTEGER : 100

      console.log(`  ${isFirstRun ? 'First run' : 'Regular run'}: ${isFirstRun ? 'fetching ALL past likes' : 'fetching last 100 likes'}`)

      while (true) {
        const response = await repoAgent.api.com.atproto.repo.listRecords({
          repo: userDid,
          collection: 'app.bsky.feed.like',
          limit: 100, // API limit per request
          cursor,
        })

        if (!response.data.records || response.data.records.length === 0) break

        // Extract post URIs from like records and hydrate them
        const postUris = response.data.records
          .map((record: any) => record.value?.subject?.uri)
          .filter((uri): uri is string => uri && uri.includes('app.bsky.feed.post'))

        console.log(`  Found ${postUris.length} post URIs in this batch`)

        // Fetch posts to get their text content using getPosts on public API (no auth required)
        for (const uri of postUris) {
          try {
            const postResponse = await publicAgent.api.app.bsky.feed.getPosts({
              uris: [uri],
            })

            if (postResponse.data.posts && postResponse.data.posts.length > 0) {
              const post = postResponse.data.posts[0]
              const text = (post as any)?.record?.text as string | undefined
              if (text && text.trim().length > 0) {
                likedTexts.push(text)
              }

              // New: Extract media info for preference tracking
              const embed = (post as any)?.record?.embed
              const hasImage = embed?.$type === 'app.bsky.embed.images' ||
                (embed?.$type === 'app.bsky.embed.recordWithMedia' &&
                  embed?.media?.$type === 'app.bsky.embed.images')
              const hasVideo = embed?.$type === 'app.bsky.embed.video' ||
                (embed?.$type === 'app.bsky.embed.recordWithMedia' &&
                  embed?.media?.$type === 'app.bsky.embed.video')

              if (hasImage) imageLikesCount++
              if (hasVideo) videoLikesCount++
              totalLikesWithMediaInfo++
            }
          } catch (postErr) {
            console.log(`  Failed to fetch post ${uri}: ${postErr}`)
            // Skip posts that can't be fetched (deleted, private, etc.)
            continue
          }
        }

        fetchedCount += response.data.records.length
        cursor = response.data.cursor

        // On first run, continue until no more pages. Otherwise, stop after maxLikes.
        if (!cursor) break // No more pages
        if (!isFirstRun && fetchedCount >= maxLikes) break // Reached limit on regular run
      }

      if (likedTexts.length === 0) {
        console.log(`  No liked posts with text found for user`)
        continue
      }

      console.log(`  Found ${likedTexts.length} liked posts with text`)
      const historicalImageRatio = totalLikesWithMediaInfo > 0 ? imageLikesCount / totalLikesWithMediaInfo : 0.25
      const historicalVideoRatio = totalLikesWithMediaInfo > 0 ? videoLikesCount / totalLikesWithMediaInfo : 0.25
      console.log(`  Historical image ratio: ${historicalImageRatio.toFixed(2)} (${imageLikesCount}/${totalLikesWithMediaInfo} posts)`)
      console.log(`  Historical video ratio: ${historicalVideoRatio.toFixed(2)} (${videoLikesCount}/${totalLikesWithMediaInfo} posts)`)

      // Write liked texts to file
      const likedCorpusPath = join(tempDir, `liked_${userDid.replace(/:/g, '_')}.txt`)
      await writeFile(likedCorpusPath, likedTexts.join('\n\n'), 'utf-8')

      // Call Python script for YAKE + TF-IDF
      const pythonScript = join(process.cwd(), 'scripts', 'extract_keywords.py')
      const { stdout, stderr } = await execAsync(
        `python3 "${pythonScript}" "${likedCorpusPath}" "${backgroundCorpusPath}"`
      )

      if (stderr && !stderr.includes('WARNING')) {
        console.error(`  Python script error: ${stderr}`)
        continue
      }

      // Parse output: keyword,score per line
      const keywordScores: Array<{ keyword: string; score: number }> = []
      for (const line of stdout.trim().split('\n')) {
        if (!line.trim()) continue
        const [keyword, scoreStr] = line.split('\t')
        if (keyword && scoreStr) {
          const score = parseFloat(scoreStr)
          if (!isNaN(score) && score > 0) {
            keywordScores.push({ keyword: keyword.toLowerCase().trim(), score })
          }
        }
      }

      if (keywordScores.length === 0) {
        console.log(`  No keywords extracted`)
        continue
      }

      console.log(`  Extracted ${keywordScores.length} keywords`)

      // Load existing keywords for this user
      const existingKeywords = await db
        .selectFrom('user_keyword')
        .select(['keyword', 'score'])
        .where('userDid', '=', userDid)
        .execute()

      const existingMap = new Map<string, number>()
      existingKeywords.forEach(k => existingMap.set(k.keyword, k.score))

      const now = new Date().toISOString()
      const seenKeywords = new Set<string>()

      // Merge new keywords and apply parabolic decay curve
      for (const { keyword, score } of keywordScores) {
        seenKeywords.add(keyword)
        const existingScore = existingMap.get(keyword)
        
        let decayFactor
        if (existingScore !== undefined) {
          // Parabolic decay: extremes decay slower, middle decays faster
          // Peak decay at middle (around 0), slower at extremes (-1 and +1)
          const absScore = Math.abs(existingScore)
          const parabolicFactor = 1 - (1 - absScore) * (1 - absScore) // 0.0 to 1.0
          const maxDecay = 0.15 // 15% peak decay at middle
          const minDecay = 0.03 // 3% decay at extremes
          decayFactor = 1 - (minDecay + (maxDecay - minDecay) * parabolicFactor)
          
          const newScore = decayFactor * existingScore + score
          // Update existing keyword
          await db
            .updateTable('user_keyword')
            .set({
              score: newScore,
              updatedAt: now,
            })
            .where('userDid', '=', userDid)
            .where('keyword', '=', keyword)
            .execute()
        } else {
          // New keyword - no decay
          decayFactor = 1.0
          await db
            .insertInto('user_keyword')
            .values({ userDid, keyword, score, updatedAt: now })
            .execute()
        }
      }

      // Apply passive decay to keywords not seen today
      for (const [keyword, oldScore] of existingMap.entries()) {
        if (!seenKeywords.has(keyword)) {
          // Apply same parabolic decay curve
          const absScore = Math.abs(oldScore)
          const parabolicFactor = 1 - (1 - absScore) * (1 - absScore) // 0.0 to 1.0
          const maxDecay = 0.15 // 15% peak decay at middle
          const minDecay = 0.03 // 3% decay at extremes
          const decayFactor = 1 - (minDecay + (maxDecay - minDecay) * parabolicFactor)
          
          const newScore = oldScore * decayFactor
          if (Math.abs(newScore) < 0.1) {
            // Prune: delete keywords below threshold
            await db
              .deleteFrom('user_keyword')
              .where('userDid', '=', userDid)
              .where('keyword', '=', keyword)
              .execute()
          } else {
            // Update with decayed score
            await db
              .updateTable('user_keyword')
              .set({
                score: newScore,
                updatedAt: now,
              })
              .where('userDid', '=', userDid)
              .where('keyword', '=', keyword)
              .execute()
          }
        }
      }

      console.log(`  Updated keywords for user`)

    } catch (err) {
      console.error(`  Error processing user ${userDid}:`, err)
      continue
    }
  }

  console.log('\nDaily keywords job completed')
  process.exit(0)
}

run().catch((err) => {
  console.error('Fatal error:', err)
  process.exit(1)
})
