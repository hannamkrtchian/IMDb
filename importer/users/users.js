import sql from 'mssql';
import crypto from 'crypto';
import { poolPromise } from '../database.js';
import { storeSecret } from './vault.js';
import { masterConfig } from '../config.js';

export async function createUsers() {
  try {
    const pool = await poolPromise;

    // Manueel in ssms de logins & users aangemaakt, dus code gecomment

    // const generatedPasswordWebDev = crypto.randomBytes(16).toString('hex');
    // const generatedPasswordDataAnalyst = crypto.randomBytes(16).toString('hex');

    // const masterPool = await sql.connect(masterConfig).then(pool => {
    //   console.log('Connected to SQL Server master DB');
    //   return pool;
    // }).catch(err => {
    //   console.error('Master Database Connection Failed! Bad Config: ', err);
    //   throw err;
    // });

    // await masterPool.request()
    //   .query(`
    //       CREATE LOGIN WebDev 
    //       WITH PASSWORD = '${generatedPasswordWebDev}';
    //       CREATE USER WebDev FOR LOGIN WebDev;

    //       CREATE LOGIN DataAnalyst 
    //       WITH PASSWORD = '${generatedPasswordDataAnalyst}';
    //       CREATE USER DataAnalyst FOR LOGIN DataAnalyst;
    //   `);

    await pool.request().query(`
      ALTER ROLE ReadViews ADD MEMBER WebDev;
  
      ALTER ROLE InsertAndModify ADD MEMBER DataAnalyst;
    `);

    // await storeSecret("WebDevPassword", generatedPasswordWebDev);
    // await storeSecret("DataAnalystPassword", generatedPasswordDataAnalyst);

    console.log(`Users created successfully.`);
    return { message: 'Users created successfully' };
  } catch (error) {
    console.error('Error creating users:', error);
    throw error;
  }
}