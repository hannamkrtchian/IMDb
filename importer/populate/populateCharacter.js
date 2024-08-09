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

let currentCharacterId = 1;

export async function populateCharacterTables() {
    try {
        const pool = await poolPromise;

        // Files to process (split)
        const fileNames = ['principals_aa', 'principals_ab', 'principals_ac', 'principals_ad', 'principals_ae', 'principals_af'];

        for (const fileName of fileNames) {
            const filePath = path.join(dataDir, fileName);
            await processCharacterFile(pool, filePath);
        }
    } catch (error) {
        console.error('Error populating character tables:', error);
        throw error;
    }
}

async function processCharacterFile(pool, filePath) {
    return new Promise((resolve, reject) => {
        const fileStream = fs.createReadStream(filePath, { encoding: 'utf8' });

        const batchSize = 50;
        let batch = [];
        let rowCount = 0;

        const parser = Papa.parse(fileStream, {
            header: true,
            skipEmptyLines: true,
            step: async (results, parser) => {
                const row = results.data;
                let { tconst, ordering, characters } = row;

                if (!tconst.startsWith('tt')) {
                    tconst = 'tt' + tconst;
                }

                let characterList = [];
                try {
                    characterList = JSON.parse(characters);
                } catch (error) {
                    characterList = [];
                }
                const rowCharacterMap = new Map();

                characterList.forEach(characterName => {
                    characterName = characterName.trim();
                    rowCharacterMap.set(characterName, currentCharacterId);
                    currentCharacterId += 1;
                });

                batch.push({ tconst, ordering, characterMap: rowCharacterMap });

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
                console.log(`Character and principalCharacter tables populated successfully with ${rowCount} records.`);
                resolve({ message: `Character and principalCharacter tables populated successfully with ${rowCount} records` });
            },
            error: (error) => {
                console.error('Error parsing file:', error);
                reject(error);
            },
        });
    });
}

async function insertBatch(pool, batch) {
    const characterTable = new sql.Table('character');
    characterTable.create = false;
    characterTable.columns.add('characterId', sql.Int, { nullable: false });
    characterTable.columns.add('characterName', sql.VarChar(255), { nullable: true });

    for (const row of batch) {
        for (const [characterName, characterId] of row.characterMap) {
            characterTable.rows.add(characterId, characterName);
        }
    }

    try {
        const request = pool.request();
        await request.bulk(characterTable);
        console.log(`Inserted characters into the character table.`);
    } catch (error) {
        console.error('Error inserting characters:', error);
        throw error;
    }

    const principalCharacterTable = new sql.Table('principalCharacter');
    principalCharacterTable.create = false;
    principalCharacterTable.columns.add('tconst', sql.VarChar(255), { nullable: false });
    principalCharacterTable.columns.add('ordering', sql.Int, { nullable: false });
    principalCharacterTable.columns.add('characterId', sql.Int, { nullable: false });

    for (const row of batch) {
        for (const characterId of row.characterMap.values()) {
            principalCharacterTable.rows.add(row.tconst, row.ordering, characterId);
        }
    }

    try {
        const request = pool.request();
        await request.bulk(principalCharacterTable);
    } catch (error) {
        if (error.code === 'EREQUEST' && error.originalError && error.originalError.info && error.originalError.info.message.includes('Violation of PRIMARY KEY constraint')) {
            console.warn('Duplicate key error detected, skipping duplicate rows.');
        } else {
            console.error('Error inserting batch into principalCharacter:', error);
            throw error;
        }
    }
}