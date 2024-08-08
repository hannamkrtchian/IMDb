import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import pkg from 'papaparse';
import sql from 'mssql';
import { poolPromise } from '../database.js';

const Papa = pkg;
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const dataDir = path.join(__dirname, '../../data');

export async function populateGenreTables() {
    try {
      const pool = await poolPromise;
      const filePath = path.join(dataDir, 'title.basics.tsv');
  
      // Read stream
      const fileStream = fs.createReadStream(filePath, { encoding: 'utf8' });
  
      const batchSize = 50;
      let batch = [];
      let rowCount = 0;
  
      return new Promise((resolve, reject) => {
        const parser = Papa.parse(fileStream, {
          header: true,
          skipEmptyLines: true,
          step: async (results, parser) => {
            const row = results.data;
            let { tconst, genres } = row;

            if (!tconst.startsWith('tt')) {
              tconst = 'tt' + tconst;
            }   
  
            const genreList = genres === '\\N' ? [] : genres.split(',');
  
            batch.push({ tconst, genreList });
  
            if (batch.length >= batchSize) {
              parser.pause();
              try {
                await insertBatch(pool, batch);
                rowCount += batch.length;
                batch = [];
                parser.resume();
              } catch (error) {
                console.error('Error processing batch:', error);
                reject(error);
              }
            }
          },
          complete: async () => {
            if (batch.length > 0) {
              try {
                await insertBatch(pool, batch);
                rowCount += batch.length;
              } catch (error) {
                console.error('Error processing final batch:', error);
                reject(error);
              }
            }
            console.log(`Genre and movieGenre tables populated successfully with ${rowCount} records.`);
            resolve({ message: `Genres and Movie Genres tables populated successfully with ${rowCount} records` });
          },
          error: (error) => {
            console.error('Error parsing file:', error);
            reject(error);
          },
        });
      });
    } catch (error) {
      console.error('Error populating genre tables:', error);
      throw error;
    }
}
  
async function insertBatch(pool, batch) {
    const genreTable = new sql.Table('genre');
    genreTable.create = false;
    genreTable.columns.add('genreName', sql.VarChar(255), { nullable: true });

    const movieGenreTable = new sql.Table('movieGenre');
    movieGenreTable.create = false;
    movieGenreTable.columns.add('tconst', sql.VarChar(255), { nullable: false });
    movieGenreTable.columns.add('genreId', sql.Int, { nullable: false });

    const newGenres = [];
    
    const genreMap = new Map();

    // Fetch existing genres to avoid duplicates
    const existingGenres = await pool.request().query('SELECT genreId, genreName FROM genre');
    existingGenres.recordset.forEach(row => {
      genreMap.set(row.genreName, row.genreId);
    });

    for (const row of batch) {
        for (const genre of row.genreList) {
            if (!genreMap.has(genre) && !newGenres.some(g => g.genreName === genre)) {
                newGenres.push({ genreName: genre });
            }
        }
    }

    // Insert new genres
    if (newGenres.length > 0) {
      newGenres.forEach(genre => {
          genreTable.rows.add(genre.genreName);
      });

      try {
          const request = pool.request();
          await request.bulk(genreTable);
      } catch (error) {
          console.error('Error inserting genres:', error);
          throw error;
      }
    }

    // Insert into movieGenre table
    for (const row of batch) {
      for (const genre of row.genreList) {
        const result = await pool.request().query(`SELECT genreId FROM genre WHERE genreName='${genre}'`);
        if (result.recordset.length > 0) {
          const genreId = result.recordset[0].genreId;
          movieGenreTable.rows.add(row.tconst, genreId);
        } else {
          console.error(`Genre ${genre} not found in database`);
        }
      }
    }
    
    try {
      const request = pool.request();
      await request.bulk(movieGenreTable);
    } catch (error) {
      if (error.code === 'EREQUEST' && error.originalError && error.originalError.info && error.originalError.info.message.includes('Violation of PRIMARY KEY constraint')) {
        console.warn('Duplicate key error detected, skipping duplicate rows.');
      } else {
        console.error('Error inserting batch into movieGenre:', error);
        throw error;
      }
    }
}
  