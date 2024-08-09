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

export async function populateKnownForTable() {
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
                    let { nconst, knownForTitles } = row;

                    if (!nconst.startsWith('nm')) {
                        nconst = 'nm' + nconst;
                    }

                    const knownForList = knownForTitles === '\\N' ? [] : knownForTitles.split(',');

                    batch.push({ nconst, knownForList });

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
                    console.log(`KnownFor table populated successfully with ${rowCount} records.`);
                    resolve({ message: `KnownFor table populated successfully with ${rowCount} records` });
                },
                error: (error) => {
                    console.error('Error parsing file:', error);
                    reject(error);
                },
            });
        });
    } catch (error) {
        console.error('Error populating knownFor tables:', error);
        throw error;
    }
}

async function insertBatch(pool, batch) {
    const knownForTable = new sql.Table('knownFor');
    knownForTable.create = false;
    knownForTable.columns.add('nconst', sql.VarChar(255), { nullable: false });
    knownForTable.columns.add('tconst', sql.VarChar(255), { nullable: false });

    for (const row of batch) {
        for (const knownFor of row.knownForList) {
            knownForTable.rows.add(row.nconst, knownFor);
        }
    }

    try {
        const request = pool.request();
        await request.bulk(knownForTable);
    } catch (error) {
        if (error.code === 'EREQUEST' && error.originalError && error.originalError.info && error.originalError.info.message.includes('Violation of PRIMARY KEY constraint')) {
            console.warn('Duplicate key error detected, skipping duplicate rows.');
        } else {
            console.error('Error inserting batch into knownForTable:', error);
            throw error;
        }
    }
}