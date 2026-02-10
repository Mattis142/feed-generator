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
      .addColumn('cursor', 'integer', (col) => col.notNull())
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
      .addColumn('id', 'integer', (col) => col.primaryKey().autoIncrement())
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
          WHERE rowid NOT IN (
            SELECT MAX(rowid) 
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
