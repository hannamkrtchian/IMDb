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

export async function populatePersonTable() {
  try {
    const pool = await poolPromise;
    const filePath = path.join(dataDir, 'name.basics.tsv');

    // Read stream
    const fileStream = fs.createReadStream(filePath, { encoding: 'utf8' });

    const batchSize = 10000;
    let batch = [];
    let rowCount = 0;

    return new Promise((resolve, reject) => {
      const parser = Papa.parse(fileStream, {
        header: true,
        skipEmptyLines: true,
        step: async (results, parser) => {
          const row = results.data;
          const { nconst, primaryName, birthYear, deathYear } = row;

          // Convert '\\N' to null
          const birthYearValue = birthYear === '\\N' ? null : parseInt(birthYear, 10);
          const deathYearValue = deathYear === '\\N' ? null : parseInt(deathYear, 10);

          // If string is too long
          const primaryNameValue = primaryName.length > 255 ? primaryName.substring(0, 255) : primaryName;

          batch.push({ nconst, primaryName: primaryNameValue, birthYear: birthYearValue, deathYear: deathYearValue });

          if (batch.length >= batchSize) {
            parser.pause();
            try {
              await insertBatch(pool, batch);
              rowCount += batch.length;
              batch = [];
              parser.resume();
            } catch (error) {
              console.error('Error inserting batch:', error);
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
              console.error('Error inserting final batch:', error);
              reject(error);
            }
          }
          console.log(`Person table populated successfully with ${rowCount} records.`);
          resolve({ message: `Person table populated successfully with ${rowCount} records` });
        },
        error: (error) => {
          console.error('Error parsing file:', error);
          reject(error);
        },
      });
    });
  } catch (error) {
    console.error('Error populating person table:', error);
    throw error;
  }
}

async function insertBatch(pool, batch) {
  const table = new sql.Table('person');
  table.create = false;

  table.columns.add('nconst', sql.VarChar(255), { nullable: false });
  table.columns.add('primaryName', sql.VarChar(255), { nullable: true });
  table.columns.add('birthYear', sql.Int, { nullable: true });
  table.columns.add('deathYear', sql.Int, { nullable: true });

  batch.forEach(row => {
    table.rows.add(row.nconst, row.primaryName, row.birthYear, row.deathYear);
  });

  const request = pool.request();
  try {
    await request.bulk(table);
  } catch (error) {
    console.error('Error inserting batch:', error);
  }
}
