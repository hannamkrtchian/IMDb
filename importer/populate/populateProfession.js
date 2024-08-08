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

export async function populateProfessionTables() {
    try {
      const pool = await poolPromise;
      const filePath = path.join(dataDir, 'name.basics.tsv');
  
      //  await pool.request().query(`IF NOT EXISTS (SELECT name FROM sysindexes WHERE name = 'idx_professionName')
      //    CREATE INDEX idx_professionName ON profession (professionName);`);
      
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
            let { nconst, primaryProfession } = row;

            if (!nconst.startsWith('nm')) {
              nconst = 'nm' + nconst;
            }   
  
            const professionList = primaryProfession === '\\N' ? [] : primaryProfession.split(',');
  
            batch.push({ nconst, professionList });
  
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
            console.log(`Profession and personProfession tables populated successfully with ${rowCount} records.`);
            resolve({ message: `Professions and personProfessions tables populated successfully with ${rowCount} records` });
          },
          error: (error) => {
            console.error('Error parsing file:', error);
            reject(error);
          },
        });
      });
    } catch (error) {
      console.error('Error populating profession tables:', error);
      throw error;
    }
}
  
async function insertBatch(pool, batch) {
    const professionTable = new sql.Table('profession');
    professionTable.create = false;
    professionTable.columns.add('professionName', sql.VarChar(255), { nullable: true });

    const personProfessionTable = new sql.Table('personProfession');
    personProfessionTable.create = false;
    personProfessionTable.columns.add('nconst', sql.VarChar(255), { nullable: false });
    personProfessionTable.columns.add('professionId', sql.Int, { nullable: false });

    const newprofessions = [];
    
    const professionMap = new Map();

    // Fetch existing professions to avoid duplicates
    const existingprofessions = await pool.request().query('SELECT professionId, professionName FROM profession');
    existingprofessions.recordset.forEach(row => {
      professionMap.set(row.professionName, row.professionId);
    });

    for (const row of batch) {
        for (const profession of row.professionList) {
            if (!professionMap.has(profession) && !newprofessions.some(g => g.professionName === profession)) {
                newprofessions.push({ professionName: profession });
            }
        }
    }

    // Insert new professions
    if (newprofessions.length > 0) {
      newprofessions.forEach(profession => {
          professionTable.rows.add(profession.professionName);
      });

      try {
          const request = pool.request();
          await request.bulk(professionTable);
      } catch (error) {
          console.error('Error inserting professions:', error);
          throw error;
      }
    }

    // Insert into personProfession table
    for (const row of batch) {
      for (const profession of row.professionList) {
        const result = await pool.request().query(`SELECT professionId FROM profession WHERE professionName='${profession}'`);
        if (result.recordset.length > 0) {
          const professionId = result.recordset[0].professionId;
          console.log(`Adding to personProfessionTable: nconst = ${row.nconst}, professionId = ${professionId}`);
          personProfessionTable.rows.add(row.nconst, professionId);
        } else {
          console.error(`profession ${profession} not found in database`);
        }
      }
    }

    console.log(`Final personProfessionTable rows: ${personProfessionTable.rows.length}`);
    
    try {
      const request = pool.request();
      await request.bulk(personProfessionTable);
    } catch (error) {
      if (error.code === 'EREQUEST' && error.originalError && error.originalError.info && error.originalError.info.message.includes('Violation of PRIMARY KEY constraint')) {
        console.warn('Duplicate key error detected, skipping duplicate rows.');
      } else {
        console.error('Error inserting batch into personProfession:', error);
        throw error;
      }
    }
}