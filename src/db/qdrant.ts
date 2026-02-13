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
      console.log('Existing collections:', collections.collections);
    } catch (error) {
      console.error('Failed to connect to Qdrant:', error);
      throw error;
    }
  }

  getClient(): QdrantClient {
    return this.client;
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
