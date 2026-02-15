import { QdrantClient } from '@qdrant/js-client-rest';

export class QdrantDatabase {
  private client: QdrantClient;

  constructor() {
    this.client = new QdrantClient({
      url: 'http://127.0.0.1:6333'
    });
  }

  async initialize(): Promise<void> {
    try {
      // Test connection
      const collections = await this.client.getCollections();
      console.log('Connected to Qdrant successfully');
      console.log('Existing collections:', collections.collections.map(c => c.name));
    } catch (error) {
      console.error('Failed to connect to Qdrant:', error);
      throw error;
    }
  }

  getClient(): QdrantClient {
    return this.client;
  }

  /**
   * Ensure the semantic feed collections exist in Qdrant.
   * These are SEPARATE from the existing atlas_data collection.
   */
  async ensureFeedCollections(): Promise<void> {
    const collections = await this.client.getCollections();
    const existingNames = new Set(collections.collections.map(c => c.name));

    // Collection for post text embeddings (MobileCLIP2-S2 = 512-dim)
    if (!existingNames.has('feed_post_embeddings')) {
      await this.client.createCollection('feed_post_embeddings', {
        vectors: {
          size: 512,
          distance: 'Cosine'
        },
        optimizers_config: {
          default_segment_number: 2, // Good for moderate dataset sizes
        },
      });
      console.log('[Qdrant] Created collection: feed_post_embeddings (512-dim, Cosine)');

      // Create payload indexes for filtering
      await this.client.createPayloadIndex('feed_post_embeddings', {
        field_name: 'author',
        field_schema: 'keyword',
      });
      await this.client.createPayloadIndex('feed_post_embeddings', {
        field_name: 'uri',
        field_schema: 'keyword',
      });
      await this.client.createPayloadIndex('feed_post_embeddings', {
        field_name: 'indexedAt',
        field_schema: 'keyword',
      });
      await this.client.createPayloadIndex('feed_post_embeddings', {
        field_name: 'likeCount',
        field_schema: 'integer',
      });
    } else {
      console.log('[Qdrant] Collection feed_post_embeddings already exists');
    }

    // Collection for user interest profile centroids (multi-centroid per user)
    if (!existingNames.has('feed_user_profiles')) {
      await this.client.createCollection('feed_user_profiles', {
        vectors: {
          size: 512,
          distance: 'Cosine'
        },
      });
      console.log('[Qdrant] Created collection: feed_user_profiles (512-dim, Cosine)');

      // Create payload indexes
      await this.client.createPayloadIndex('feed_user_profiles', {
        field_name: 'userDid',
        field_schema: 'keyword',
      });
      await this.client.createPayloadIndex('feed_user_profiles', {
        field_name: 'clusterId',
        field_schema: 'integer',
      });
      await this.client.createPayloadIndex('feed_user_profiles', {
        field_name: 'updatedAt',
        field_schema: 'keyword',
      });
    } else {
      console.log('[Qdrant] Collection feed_user_profiles already exists');
    }
  }

  async createCollection(name: string, vectorSize: number = 1536): Promise<void> {
    try {
      await this.client.createCollection(name, {
        vectors: {
          size: vectorSize,
          distance: 'Cosine'
        }
      });
      console.log(`Collection '${name}' created successfully`);
    } catch (error) {
      console.error(`Failed to create collection '${name}':`, error);
      throw error;
    }
  }

  async listCollections(): Promise<void> {
    try {
      const collections = await this.client.getCollections();
      console.log('Available collections:');
      collections.collections.forEach(collection => {
        console.log(`- ${collection.name}`);
      });
    } catch (error) {
      console.error('Failed to list collections:', error);
      throw error;
    }
  }
}

export const qdrantDB = new QdrantDatabase();
