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

export async function populateMovieTable() {
  try {
    const pool = await poolPromise;
    const filePath = path.join(dataDir, 'title.basics.tsv');

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
          let { tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes } = results.data;

          // Convert '\\N' to null
          originalTitle = originalTitle === '\\N' ? null : originalTitle;
          startYear = startYear === '\\N' ? null : startYear;
          endYear = endYear === '\\N' ? null : endYear;
          runtimeMinutes = runtimeMinutes === '\\N' ? null : runtimeMinutes;
          isAdult = isNaN(parseInt(isAdult, 10)) ? 0 : parseInt(isAdult, 10);

          // If string is too long
          titleType = titleType.length > 255 ? titleType.substring(0, 255) : titleType;
          primaryTitle = primaryTitle.length > 255 ? primaryTitle.substring(0, 255) : primaryTitle;
          originalTitle = originalTitle?.length > 255 ? originalTitle.substring(0, 255) : originalTitle;

          batch.push({ tconst, titleType, primaryTitle, originalTitle: originalTitle, isAdult, startYear, endYear, runtimeMinutes });

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
          console.log(`Movie table populated successfully with ${rowCount} records.`);
          resolve({ message: `Movie table populated successfully with ${rowCount} records` });
        },
        error: (error) => {
          console.error('Error parsing file:', error);
          reject(error);
        },
      });
    });
  } catch (error) {
    console.error('Error populating movie table:', error);
    throw error;
  }
}

async function insertBatch(pool, batch) {
  const table = new sql.Table('movie');
  table.create = false;

  table.columns.add('tconst', sql.VarChar(255), { nullable: false });
  table.columns.add('titleType', sql.VarChar(255), { nullable: true });
  table.columns.add('primaryTitle', sql.VarChar(255), { nullable: true });
  table.columns.add('originalTitle', sql.VarChar(255), { nullable: true });
  table.columns.add('isAdult', sql.TinyInt, { nullable: true });
  table.columns.add('startYear', sql.Int, { nullable: true });
  table.columns.add('endYear', sql.Int, { nullable: true });
  table.columns.add('runtimeMinutes', sql.Int, { nullable: true });

  batch.forEach(row => {
    table.rows.add(row.tconst, row.titleType, row.primaryTitle, row.originalTitle, row.isAdult, row.startYear, row.endYear, row.runtimeMinutes);
  });

  const request = pool.request();
  try {
    await request.bulk(table);
  } catch (error) {
    console.error('Error inserting batch:', error);
  }
}
