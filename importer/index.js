import express from 'express';
import Database from './database.js';
import { createTables, deleteTables, getTables } from './tables.js';

const port = process.env.PORT || 3000;
const app = express();

const database = new Database();

app.get('/', async (req, res) => {
  try {
    const result = await database.executeQuery('SELECT GETDATE() AS CurrentTime');
    res.send(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.get('/create-tables', async (req, res) => {
  try {
    const result = await createTables();
    res.send(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.get('/delete-tables', async (req, res) => {
  try {
    const result = await deleteTables();
    res.send(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.get('/get-tables', async (req, res) => {
  try {
    const result = await getTables();
    res.send(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});