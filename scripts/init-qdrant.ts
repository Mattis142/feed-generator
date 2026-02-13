import dotenv from 'dotenv';
import { qdrantDB } from '../src/db/qdrant';

dotenv.config();

async function initializeQdrant() {
  try {
    console.log('Initializing Qdrant database...');
    
    // Test connection and list existing collections
    await qdrantDB.initialize();
    await qdrantDB.listCollections();
    
    console.log('Qdrant database initialized successfully!');
  } catch (error) {
    console.error('Failed to initialize Qdrant database:', error);
    process.exit(1);
  }
}

initializeQdrant();
