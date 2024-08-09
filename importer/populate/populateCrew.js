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

export async function populateCrewTable() {
    try {
        const pool = await poolPromise;
        const filePath = path.join(dataDir, 'title.crew.tsv');

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
                    let { tconst, directors, writers } = row;

                    if (!tconst.startsWith('tt')) {
                        tconst = 'tt' + tconst;
                    }

                    const directorList = directors === '\\N' ? [] : directors.split(',');
                    const writerList = writers === '\\N' ? [] : writers.split(',');

                    batch.push({ tconst, directorList, writerList });

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
                    console.log(`Director and writer table populated successfully with ${rowCount} records.`);
                    resolve({ message: `Director and writer table populated successfully with ${rowCount} records` });
                },
                error: (error) => {
                    console.error('Error parsing file:', error);
                    reject(error);
                },
            });
        });
    } catch (error) {
        console.error('Error populating director and writer tables:', error);
        throw error;
    }
}

async function insertBatch(pool, batch) {
    // Insert directors
    const directorTable = new sql.Table('director');
    directorTable.create = false;
    directorTable.columns.add('tconst', sql.VarChar(255), { nullable: false });
    directorTable.columns.add('nconst', sql.VarChar(255), { nullable: false });

    for (const row of batch) {
        for (const director of row.directorList) {
            directorTable.rows.add(row.tconst, director);
        }
    }

    try {
        const request = pool.request();
        await request.bulk(directorTable);
    } catch (error) {
        if (error.code === 'EREQUEST' && error.originalError && error.originalError.info && error.originalError.info.message.includes('Violation of PRIMARY KEY constraint')) {
            console.warn('Duplicate key error detected, skipping duplicate rows.');
        } else {
            console.error('Error inserting batch into directorTable:', error);
            throw error;
        }
    }

    // Insert writers
    const writerTable = new sql.Table('writer');
    writerTable.create = false;
    writerTable.columns.add('tconst', sql.VarChar(255), { nullable: false });
    writerTable.columns.add('nconst', sql.VarChar(255), { nullable: false });

    for (const row of batch) {
        for (const writer of row.writerList) {
            writerTable.rows.add(row.tconst, writer);
        }
    }

    try {
        const request = pool.request();
        await request.bulk(writerTable);
    } catch (error) {
        if (error.code === 'EREQUEST' && error.originalError && error.originalError.info && error.originalError.info.message.includes('Violation of PRIMARY KEY constraint')) {
            console.warn('Duplicate key error detected, skipping duplicate rows.');
        } else {
            console.error('Error inserting batch into writerTable:', error);
            throw error;
        }
    }
}