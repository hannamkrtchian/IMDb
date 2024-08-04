import express from 'express';
import Database from './database.js';

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

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
