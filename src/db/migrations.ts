import { Kysely, Migration, MigrationProvider, sql } from 'kysely'

const migrations: Record<string, Migration> = {}

export const migrationProvider: MigrationProvider = {
  async getMigrations() {
    return migrations
  },
}

migrations['001'] = {
  async up(db: Kysely<any>) {
    await db.schema
      .createTable('post')
      .addColumn('uri', 'varchar', (col) => col.primaryKey())
      .addColumn('cid', 'varchar', (col) => col.notNull())
      .addColumn('indexedAt', 'varchar', (col) => col.notNull())
      .execute()
    await db.schema
      .createTable('sub_state')
      .addColumn('service', 'varchar', (col) => col.primaryKey())
      .addColumn('cursor', 'bigint', (col) => col.notNull())
      .execute()
  },
  async down(db: Kysely<any>) {
    await db.schema.dropTable('post').execute()
    await db.schema.dropTable('sub_state').execute()
  },
}

migrations['002'] = {
  async up(db: Kysely<any>) {
    await db.schema
      .alterTable('post')
      .addColumn('author', 'varchar', (col) => col.notNull().defaultTo(''))
      .execute()
    await db.schema
      .alterTable('post')
      .addColumn('likeCount', 'integer', (col) => col.notNull().defaultTo(0))
      .execute()
    await db.schema
      .alterTable('post')
      .addColumn('replyCount', 'integer', (col) => col.notNull().defaultTo(0))
      .execute()
    await db.schema
      .alterTable('post')
      .addColumn('repostCount', 'integer', (col) => col.notNull().defaultTo(0))
      .execute()
    await db.schema
      .alterTable('post')
      .addColumn('replyRoot', 'varchar')
      .execute()
    await db.schema
      .alterTable('post')
      .addColumn('replyParent', 'varchar')
      .execute()
    await db.schema
      .createTable('graph_follow')
      .addColumn('follower', 'varchar', (col) => col.notNull())
      .addColumn('followee', 'varchar', (col) => col.notNull())
      .addColumn('indexedAt', 'varchar', (col) => col.notNull())
      .addPrimaryKeyConstraint('graph_follow_pk', ['follower', 'followee'])
      .execute()
    await db.schema
      .createTable('graph_interaction')
      .addColumn('id', 'serial', (col) => col.primaryKey())
      .addColumn('actor', 'varchar', (col) => col.notNull())
      .addColumn('target', 'varchar', (col) => col.notNull())
      .addColumn('type', 'varchar', (col) => col.notNull())
      .addColumn('weight', 'integer', (col) => col.notNull())
      .addColumn('indexedAt', 'varchar', (col) => col.notNull())
      .execute()
    await db.schema
      .createTable('graph_meta')
      .addColumn('key', 'varchar', (col) => col.primaryKey())
      .addColumn('value', 'varchar', (col) => col.notNull())
      .addColumn('updatedAt', 'varchar', (col) => col.notNull())
      .execute()
  },
  async down(db: Kysely<any>) {
    await db.schema.dropTable('graph_follow').execute()
    await db.schema.dropTable('graph_interaction').execute()
    await db.schema.dropTable('graph_meta').execute()
    await db.schema
      .alterTable('post')
      .dropColumn('author')
      .dropColumn('likeCount')
      .dropColumn('replyCount')
      .dropColumn('repostCount')
      .dropColumn('replyRoot')
      .dropColumn('replyParent')
      .execute()
  },
}

migrations['003'] = {
  async up(db: Kysely<any>) {
    await db.schema
      .createIndex('post_author_idx')
      .on('post')
      .column('author')
      .execute()
    await db.schema
      .createIndex('post_indexedAt_idx')
      .on('post')
      .column('indexedAt')
      .execute()
    await db.schema
      .createIndex('post_likeCount_idx')
      .on('post')
      .column('likeCount')
      .execute()
    await db.schema
      .createIndex('graph_follow_follower_idx')
      .on('graph_follow')
      .column('follower')
      .execute()
  },
  async down(db: Kysely<any>) {
    await db.schema.dropIndex('post_author_idx').execute()
    await db.schema.dropIndex('post_indexedAt_idx').execute()
    await db.schema.dropIndex('post_likeCount_idx').execute()
    await db.schema.dropIndex('graph_follow_follower_idx').execute()
  },
}

migrations['004'] = {
  async up(db: Kysely<any>) {
    await db.schema
      .createIndex('graph_interaction_target_idx')
      .on('graph_interaction')
      .column('target')
      .execute()
    await db.schema
      .createIndex('graph_interaction_actor_idx')
      .on('graph_interaction')
      .column('actor')
      .execute()
  },
  async down(db: Kysely<any>) {
    await db.schema.dropIndex('graph_interaction_target_idx').execute()
    await db.schema.dropIndex('graph_interaction_actor_idx').execute()
  },
}

migrations['005'] = {
  async up(db: Kysely<any>) {
    await db.schema
      .createTable('user_influential_l2')
      .addColumn('userDid', 'varchar', (col) => col.notNull())
      .addColumn('l2Did', 'varchar', (col) => col.notNull())
      .addColumn('influenceScore', 'real', (col) => col.notNull())
      .addColumn('l1FollowerCount', 'integer', (col) => col.notNull())
      .addColumn('updatedAt', 'varchar', (col) => col.notNull())
      .addPrimaryKeyConstraint('user_influential_l2_pk', ['userDid', 'l2Did'])
      .execute()
    await db.schema
      .createIndex('user_influential_l2_userDid_idx')
      .on('user_influential_l2')
      .column('userDid')
      .execute()
    await db.schema
      .createIndex('user_influential_l2_score_idx')
      .on('user_influential_l2')
      .columns(['userDid', 'influenceScore'])
      .execute()
  },
  async down(db: Kysely<any>) {
    await db.schema.dropIndex('user_influential_l2_userDid_idx').execute()
    await db.schema.dropIndex('user_influential_l2_score_idx').execute()
    await db.schema.dropTable('user_influential_l2').execute()
  },
}

migrations['006'] = {
  async up(db: Kysely<any>) {
    // 1. Deduplicate graph_interaction before applying unique constraint
    await sql`DELETE FROM graph_interaction 
          WHERE id NOT IN (
            SELECT MAX(id) 
            FROM graph_interaction 
            GROUP BY actor, target, type
          )`.execute(db)

    // 2. Create the unique index
    await db.schema
      .createIndex('graph_interaction_unique_idx')
      .on('graph_interaction')
      .columns(['actor', 'target', 'type'])
      .unique()
      .execute()
  },
  async down(db: Kysely<any>) {
    await db.schema.dropIndex('graph_interaction_unique_idx').execute()
  },
}

migrations['007'] = {
  async up(db: Kysely<any>) {
    await db.schema
      .createTable('user_served_post')
      .addColumn('userDid', 'varchar', (col) => col.notNull())
      .addColumn('uri', 'varchar', (col) => col.notNull())
      .addColumn('servedAt', 'varchar', (col) => col.notNull())
      .execute()

    await db.schema
      .createIndex('user_served_post_user_uri_idx')
      .on('user_served_post')
      .columns(['userDid', 'uri'])
      .execute()

    await db.schema
      .createIndex('user_served_post_servedAt_idx')
      .on('user_served_post')
      .column('servedAt')
      .execute()
  },
  async down(db: Kysely<any>) {
    await db.schema.dropTable('user_served_post').execute()
  },
}

migrations['008'] = {
  async up(db: Kysely<any>) {
    await db.schema
      .alterTable('post')
      .addColumn('text', 'text')
      .execute()
  },
  async down(db: Kysely<any>) {
    await db.schema
      .alterTable('post')
      .dropColumn('text')
      .execute()
  },
}

migrations['009'] = {
  async up(db: Kysely<any>) {
    await db.schema
      .createTable('user_keyword')
      .addColumn('userDid', 'varchar', (col) => col.notNull())
      .addColumn('keyword', 'varchar', (col) => col.notNull())
      .addColumn('score', 'real', (col) => col.notNull())
      .addColumn('updatedAt', 'varchar', (col) => col.notNull())
      .addPrimaryKeyConstraint('user_keyword_pk', ['userDid', 'keyword'])
      .execute()

    await db.schema
      .createIndex('user_keyword_userDid_idx')
      .on('user_keyword')
      .column('userDid')
      .execute()
  },
  async down(db: Kysely<any>) {
    await db.schema.dropIndex('user_keyword_userDid_idx').execute()
    await db.schema.dropTable('user_keyword').execute()
  },
}

migrations['010'] = {
  async up(db: Kysely<any>) {
    await db.schema
      .createTable('user_seen_post')
      .addColumn('userDid', 'varchar', (col) => col.notNull())
      .addColumn('uri', 'varchar', (col) => col.notNull())
      .addColumn('seenAt', 'varchar', (col) => col.notNull())
      .execute()

    await db.schema
      .createIndex('user_seen_post_user_uri_idx')
      .on('user_seen_post')
      .columns(['userDid', 'uri'])
      .execute()

    await db.schema
      .createIndex('user_seen_post_seenAt_idx')
      .on('user_seen_post')
      .column('seenAt')
      .execute()
  },
  async down(db: Kysely<any>) {
    await db.schema.dropTable('user_seen_post').execute()
  },
}

migrations['011'] = {
  async up(db: Kysely<any>) {
    await db.schema
      .createTable('taste_similarity')
      .addColumn('userDid', 'varchar', (col) => col.notNull())
      .addColumn('similarUserDid', 'varchar', (col) => col.notNull())
      .addColumn('agreementCount', 'integer', (col) => col.notNull().defaultTo(0))
      .addColumn('totalCoLikedPosts', 'integer', (col) => col.notNull().defaultTo(0))
      .addColumn('lastAgreementAt', 'varchar', (col) => col.notNull())
      .addColumn('updatedAt', 'varchar', (col) => col.notNull())
      .addPrimaryKeyConstraint('taste_similarity_pk', ['userDid', 'similarUserDid'])
      .execute()

    await db.schema
      .createIndex('taste_similarity_userDid_idx')
      .on('taste_similarity')
      .column('userDid')
      .execute()

    await db.schema
      .createIndex('taste_similarity_agreement_idx')
      .on('taste_similarity')
      .columns(['userDid', 'agreementCount'])
      .execute()
  },
  async down(db: Kysely<any>) {
    await db.schema.dropIndex('taste_similarity_userDid_idx').execute()
    await db.schema.dropIndex('taste_similarity_agreement_idx').execute()
    await db.schema.dropTable('taste_similarity').execute()
  },
}

migrations['012'] = {
  async up(db: Kysely<any>) {
    await db.schema
      .createTable('taste_reputation')
      .addColumn('userDid', 'varchar', (col) => col.notNull())
      .addColumn('similarUserDid', 'varchar', (col) => col.notNull())
      .addColumn('reputationScore', 'real', (col) => col.notNull().defaultTo(1.0))
      .addColumn('agreementHistory', 'integer', (col) => col.notNull().defaultTo(0))
      .addColumn('lastSeenAt', 'varchar', (col) => col.notNull())
      .addColumn('decayRate', 'real', (col) => col.notNull().defaultTo(0.95))
      .addColumn('updatedAt', 'varchar', (col) => col.notNull())
      .addPrimaryKeyConstraint('taste_reputation_pk', ['userDid', 'similarUserDid'])
      .execute()

    await db.schema
      .createIndex('taste_reputation_userDid_idx')
      .on('taste_reputation')
      .column('userDid')
      .execute()

    await db.schema
      .createIndex('taste_reputation_score_idx')
      .on('taste_reputation')
      .columns(['userDid', 'reputationScore'])
      .execute()
  },
  async down(db: Kysely<any>) {
    await db.schema.dropIndex('taste_reputation_userDid_idx').execute()
    await db.schema.dropIndex('taste_reputation_score_idx').execute()
    await db.schema.dropTable('taste_reputation').execute()
  },
}

migrations['013'] = {
  async up(db: Kysely<any>) {
    await db.schema
      .createTable('user_author_fatigue')
      .addColumn('userDid', 'varchar', (col) => col.notNull())
      .addColumn('authorDid', 'varchar', (col) => col.notNull())
      .addColumn('serveCount', 'integer', (col) => col.notNull().defaultTo(0))
      .addColumn('lastServedAt', 'varchar', (col) => col.notNull())
      .addColumn('fatigueScore', 'real', (col) => col.notNull().defaultTo(0))
      .addColumn('lastInteractionAt', 'varchar')
      .addColumn('interactionCount', 'integer', (col) => col.notNull().defaultTo(0))
      .addColumn('updatedAt', 'varchar', (col) => col.notNull())
      .addPrimaryKeyConstraint('user_author_fatigue_pk', ['userDid', 'authorDid'])
      .execute()

    await db.schema
      .createIndex('user_author_fatigue_userDid_idx')
      .on('user_author_fatigue')
      .column('userDid')
      .execute()

    await db.schema
      .createIndex('user_author_fatigue_fatigue_idx')
      .on('user_author_fatigue')
      .columns(['userDid', 'fatigueScore'])
      .execute()
  },
  async down(db: Kysely<any>) {
    await db.schema.dropIndex('user_author_fatigue_userDid_idx').execute()
    await db.schema.dropIndex('user_author_fatigue_fatigue_idx').execute()
    await db.schema.dropTable('user_author_fatigue').execute()
  },
}

migrations['014'] = {
  async up(db: Kysely<any>) {
    await db.schema
      .createTable('feed_debug_log')
      .addColumn('userDid', 'varchar', (col) => col.notNull())
      .addColumn('uri', 'varchar', (col) => col.notNull())
      .addColumn('score', 'integer', (col) => col.notNull())
      .addColumn('signals', 'text', (col) => col.notNull()) // JSON breakdown
      .addColumn('servedAt', 'varchar', (col) => col.notNull())
      .execute()

    await db.schema
      .createIndex('feed_debug_log_user_idx')
      .on('feed_debug_log')
      .column('userDid')
      .execute()

    await db.schema
      .createIndex('feed_debug_log_servedAt_idx')
      .on('feed_debug_log')
      .column('servedAt')
      .execute()
  },
  async down(db: Kysely<any>) {
    await db.schema.dropTable('feed_debug_log').execute()
  },
}
migrations['015'] = {
  async up(db: Kysely<any>) {
    await db.schema
      .alterTable('post')
      .addColumn('hasImage', 'boolean', (col) => col.defaultTo(false))
      .execute()
    await db.schema
      .alterTable('post')
      .addColumn('hasVideo', 'boolean', (col) => col.defaultTo(false))
      .execute()
    await db.schema
      .alterTable('post')
      .addColumn('hasExternal', 'boolean', (col) => col.defaultTo(false))
      .execute()
  },
  async down(db: Kysely<any>) {
    await db.schema
      .alterTable('post')
      .dropColumn('hasImage')
      .execute()
    await db.schema
      .alterTable('post')
      .dropColumn('hasVideo')
      .execute()
    await db.schema
      .alterTable('post')
      .dropColumn('hasExternal')
      .execute()
  },
}

migrations['016'] = {
  async up(db: Kysely<any>) {
    await db.schema
      .alterTable('user_author_fatigue')
      .addColumn('affinityScore', 'double precision', (col) => col.defaultTo(1.0))
      .execute()
    await db.schema
      .alterTable('user_author_fatigue')
      .addColumn('interactionWeight', 'double precision', (col) => col.defaultTo(0.0))
      .execute()
  },
  async down(db: Kysely<any>) {
    await db.schema
      .alterTable('user_author_fatigue')
      .dropColumn('affinityScore')
      .execute()
    await db.schema
      .alterTable('user_author_fatigue')
      .dropColumn('interactionWeight')
      .execute()
  },
}

migrations['017'] = {
  async up(db: Kysely<any>) {
    await db.schema
      .createTable('user_candidate_batch')
      .addColumn('userDid', 'varchar', (col) => col.notNull())
      .addColumn('uri', 'varchar', (col) => col.notNull())
      .addColumn('semanticScore', 'double precision', (col) => col.notNull())
      .addColumn('pipelineScore', 'double precision', (col) => col.notNull().defaultTo(0))
      .addColumn('centroidId', 'integer', (col) => col.notNull().defaultTo(0))
      .addColumn('batchId', 'varchar', (col) => col.notNull())
      .addColumn('generatedAt', 'varchar', (col) => col.notNull())
      .execute()

    // Index for reading batches by user, ordered by recency
    await db.schema
      .createIndex('idx_candidate_batch_user_time')
      .on('user_candidate_batch')
      .columns(['userDid', 'generatedAt'])
      .execute()

    // Index for reading batches by user, ordered by score
    await db.schema
      .createIndex('idx_candidate_batch_user_score')
      .on('user_candidate_batch')
      .columns(['userDid', 'semanticScore'])
      .execute()

    // Index for cleanup by batch age
    await db.schema
      .createIndex('idx_candidate_batch_generated')
      .on('user_candidate_batch')
      .columns(['generatedAt'])
      .execute()
  },
  async down(db: Kysely<any>) {
    await db.schema.dropTable('user_candidate_batch').execute()
  },
}
migrations['018'] = {
  async up(db: Kysely<any>) {
    await db.schema
      .alterTable('taste_reputation')
      .alterColumn('agreementHistory', (col) => col.setDataType('double precision'))
      .execute()
  },
  async down(db: Kysely<any>) {
    await db.schema
      .alterTable('taste_reputation')
      .alterColumn('agreementHistory', (col) => col.setDataType('integer'))
      .execute()
  },
}

migrations['019'] = {
  async up(db: Kysely<any>) {
    await db.schema
      .alterTable('graph_interaction')
      .addColumn('interactionUri', 'varchar')
      .execute()
  },
  async down(db: Kysely<any>) {
    await db.schema
      .alterTable('graph_interaction')
      .dropColumn('interactionUri')
      .execute()
  },
}
