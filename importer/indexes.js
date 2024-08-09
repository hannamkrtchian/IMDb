import { poolPromise } from './database.js';

export async function createIndexes() {
  try {
    const pool = await poolPromise;
    const request = pool.request();
    request.timeout = 60000; 

    await request.query(`CREATE INDEX idx_moviePrimaryTitle ON movie (primaryTitle);`);
    await request.query(`CREATE INDEX idx_movieStartYear ON movie (startYear);`);
    await request.query(`CREATE INDEX idx_personPrimaryName ON person (primaryName);`);
    
    console.log('Indexes created successfully.');
    return { message: 'Indexes created successfully' };
  } catch (error) {
    console.error('Error creating Indexes:', error);
    throw error;
  }
}
