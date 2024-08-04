import config from './config.js';
import sql from 'mssql';

const poolPromise = sql.connect(config).then(pool => {
  console.log('Connected to SQL Server');
  return pool;
}).catch(err => {
  console.error('Database Connection Failed! Bad Config: ', err);
  throw err;
});

export default class Database {
  async executeQuery(query) {
    try {
      const pool = await poolPromise;
      const result = await pool.request().query(query);
      return result.recordset;
    } catch (error) {
      console.error('Error executing query:', error);
      throw error;
    }
  }
}

export { poolPromise };
