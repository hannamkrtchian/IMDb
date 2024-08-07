import express from 'express';
import Database from './database.js';
import { createTables, deleteTables, getTables } from './tables.js';
import { populatePersonTable } from './populate/populatePerson.js';
import { populateMovieTable } from './populate/populateMovie.js';
import { populateGenreTables } from './populate/populateGenre.js';
import { populateProfessionTables } from './populate/populateProfession.js';
import { populateKnownForTable } from './populate/populateKnownFor.js';
import { populateCrewTable } from './populate/populateCrew.js';
import { populatePrincipalTable } from './populate/populatePrincipals.js';
import { populateCharacterTables } from './populate/populateCharacter.js';
import { createIndexes } from './indexes.js';

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


// TABLES
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


// DATA
app.get('/populate-person-table', async (req, res) => {
  try {
    const result = await populatePersonTable();
    res.send(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.get('/populate-movie-table', async (req, res) => {
  try {
    const result = await populateMovieTable();
    res.send(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.get('/populate-genre-table', async (req, res) => {
  try {
    const result = await populateGenreTables();
    res.send(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.get('/populate-profession-table', async (req, res) => {
  try {
    const result = await populateProfessionTables();
    res.send(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.get('/populate-knownFor-table', async (req, res) => {
  try {
    const result = await populateKnownForTable();
    res.send(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.get('/populate-crew-table', async (req, res) => {
  try {
    const result = await populateCrewTable();
    res.send(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.get('/populate-principal-table', async (req, res) => {
  try {
    const result = await populatePrincipalTable();
    res.send(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.get('/populate-character-table', async (req, res) => {
  try {
    const result = await populateCharacterTables();
    res.send(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});


// INDEXES
app.get('/create-indexes', async (req, res) => {
  try {
    const result = await createIndexes();
    res.send(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
