import { BskyAgent } from '@atproto/api'
import { Database } from '../db'

export class GraphBuilder {
    public db: Database
    public agent: BskyAgent

    constructor(db: Database) {
        this.db = db
        // Use public AppView for unauthenticated follows fetching
        this.agent = new BskyAgent({ service: 'https://public.api.bsky.app' })
    }

    async buildUserGraph(userDid: string) {
        console.log(`[GraphBuilder] Starting graph build for ${userDid}...`)

        // 1. Check if we already built this graph recently (last 24h)
        const lastUpdate = await this.db
            .selectFrom('graph_meta')
            .selectAll()
            .where('key', '=', `graph_last_update_${userDid}`)
            .executeTakeFirst()

        if (lastUpdate &&
            new Date(lastUpdate.updatedAt).getTime() > Date.now() - 24 * 60 * 60 * 1000) {
            console.log(`Graph for ${userDid} is fresh enough.`)
            return
        }

        // 2. Fetch Layer 1 (Follows)
        let follows: string[] = []
        let cursor: string | undefined
        try {
            do {
                const res = await this.agent.getFollows({ actor: userDid, cursor })
                follows.push(...res.data.follows.map(f => f.did))
                cursor = res.data.cursor
            } while (cursor)
            console.log(`[GraphBuilder] Found ${follows.length} direct follows (Layer 1) for ${userDid}`)
        } catch (e) {
            console.error(`[GraphBuilder] Failed to fetch follows for ${userDid}`, e)
        }

        // Store Layer 1
        for (const followee of follows) {
            await this.db.insertInto('graph_follow').values({
                follower: userDid,
                followee: followee,
                indexedAt: new Date().toISOString(),
            }).onConflict(oc => oc.doNothing()).execute()
        }

        // 3. Fetch Layer 2 (Follows of Follows) - BACKGROUND / LAZY
        // We do this sequentially but could be optimized. 
        // WARNING: Rate limits.
        console.log(`Fetching Layer 2 for ${follows.length} users...`)

        // We'll use a small subset or just do it slowly for this implementation
        // In a real app, this should be a queue task.
        let l2Count = 0
        for (const fDid of follows) {
            try {
                let l2Follows: string[] = []
                // Just fetch first 100 to avoid hitting rate limits too hard immediately
                const res = await this.agent.getFollows({ actor: fDid, limit: 100 })
                l2Follows = res.data.follows.map(f => f.did)
                l2Count += l2Follows.length

                for (const l2f of l2Follows) {
                    await this.db.insertInto('graph_follow').values({
                        follower: fDid,
                        followee: l2f,
                        indexedAt: new Date().toISOString(),
                    }).onConflict(oc => oc.doNothing()).execute()
                }
            } catch (e) {
                console.warn(`Could not fetch follows for Layer 2 user ${fDid}`, e)
                // Sleep a bit if rate limited?
                await new Promise(r => setTimeout(r, 100))
            }
        }

        // Update meta
        await this.db.insertInto('graph_meta').values({
            key: `graph_last_update_${userDid}`,
            value: 'done',
            updatedAt: new Date().toISOString(),
        }).onConflict(oc => oc.column('key').doUpdateSet({ updatedAt: new Date().toISOString() })).execute()

        console.log(`Finished building graph for ${userDid}.`)
    }

    async getWantedDids(userDid: string): Promise<string[]> {
        // Return User + Follows + Follows of Follows
        const layer1 = await this.db
            .selectFrom('graph_follow')
            .select('followee')
            .where('follower', '=', userDid)
            .execute()

        const l1Dids = layer1.map(r => r.followee)

        if (l1Dids.length === 0) return [userDid]

        const layer2 = await this.db
            .selectFrom('graph_follow')
            .select('followee')
            .where('follower', 'in', l1Dids)
            .execute()

        const l2Dids = layer2.map(r => r.followee)

        return Array.from(new Set([userDid, ...l1Dids, ...l2Dids]))
    }
}
