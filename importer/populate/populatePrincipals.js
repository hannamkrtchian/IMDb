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

export async function populatePrincipalTable() {
    try {
        const pool = await poolPromise;

        // Array of file names to process (split large file)
        const fileNames = ['principals_aa', 'principals_ab', 'principals_ac', 'principals_ad', 'principals_ae', 'principals_af'];

        for (const fileName of fileNames) {
            const filePath = path.join(dataDir, fileName);

            await processFile(pool, filePath);
        }
    } catch (error) {
        console.error('Error populating principal table:', error);
        throw error;
    }
}

async function processFile(pool, filePath) {
    return new Promise((resolve, reject) => {
        const fileStream = fs.createReadStream(filePath, { encoding: 'utf8' });

        const batchSize = 100;
        let batch = [];
        let rowCount = 0;

        const parser = Papa.parse(fileStream, {
            header: true,
            skipEmptyLines: true,
            step: async (results, parser) => {
                let { tconst, ordering, nconst, category, job } = results.data;

                // Convert '\\N' to null
                category = category === '\\N' ? null : category;
                job = job === '\\N' ? null : job;

                // If string is too long
                category = category?.length > 255 ? category.substring(0, 255) : category;
                job = job?.length > 255 ? job.substring(0, 255) : job;

                batch.push({ tconst, ordering, nconst, category, job });

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
                console.log(`Processed ${rowCount} records from ${filePath}.`);
                resolve();
            },
            error: (error) => {
                console.error('Error parsing file:', error);
                reject(error);
            },
        });
    });
}

async function insertBatch(pool, batch) {
    const table = new sql.Table('principal');
    table.create = false;

    table.columns.add('tconst', sql.VarChar(255), { nullable: false });
    table.columns.add('ordering', sql.Int, { nullable: false });
    table.columns.add('nconst', sql.VarChar(255), { nullable: true });
    table.columns.add('category', sql.VarChar(255), { nullable: true });
    table.columns.add('job', sql.VarChar(255), { nullable: true });

    batch.forEach(row => {
        table.rows.add(row.tconst, row.ordering, row.nconst, row.category, row.job);
    });

    const request = pool.request();
    try {
        await request.bulk(table);
    } catch (error) {
        console.error('Error inserting batch:', error);
    }
}