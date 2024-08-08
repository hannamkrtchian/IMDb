import { poolPromise } from './database.js';

export async function createTables() {
  try {
    const pool = await poolPromise;

    const tableCreationQuery = `
      CREATE TABLE movie (
        tconst VARCHAR(255) PRIMARY KEY,
        titleType VARCHAR(255),
        primaryTitle VARCHAR(255),
        originalTitle VARCHAR(255),
        isAdult TINYINT,
        startYear INT,
        endYear INT,
        runtimeMinutes INT
      );

      CREATE TABLE genre (
        genreId INT IDENTITY(1,1) PRIMARY KEY,
        genreName VARCHAR(255) UNIQUE
      );

      CREATE TABLE movieGenre (
        tconst VARCHAR(255),
        genreId INT,
        PRIMARY KEY (tconst, genreId),
        FOREIGN KEY (tconst) REFERENCES movie(tconst),
        FOREIGN KEY (genreId) REFERENCES genre(genreId)
      );

      CREATE TABLE person (
        nconst VARCHAR(255) PRIMARY KEY,
        primaryName VARCHAR(255),
        birthYear INT,
        deathYear INT
      );

      CREATE TABLE writer (
        tconst VARCHAR(255),
        nconst VARCHAR(255),
        FOREIGN KEY (tconst) REFERENCES movie(tconst),
        FOREIGN KEY (nconst) REFERENCES person(nconst)
      );

      CREATE TABLE director (
        tconst VARCHAR(255),
        nconst VARCHAR(255),
        FOREIGN KEY (tconst) REFERENCES movie(tconst),
        FOREIGN KEY (nconst) REFERENCES person(nconst)
      );

      CREATE TABLE principal (
        tconst VARCHAR(255),
        ordering INT,
        nconst VARCHAR(255),
        category VARCHAR(255),
        job VARCHAR(255),
        PRIMARY KEY (tconst, ordering),
        FOREIGN KEY (tconst) REFERENCES movie(tconst),
        FOREIGN KEY (nconst) REFERENCES person(nconst)
      );

      CREATE TABLE character (
        characterId VARCHAR(255) PRIMARY KEY,
        characterName VARCHAR(255)
      );

      CREATE TABLE principalCharacter (
        tconst VARCHAR(255),
        ordering INT,
        characterId VARCHAR(255),
        FOREIGN KEY (tconst, ordering) REFERENCES principal(tconst, ordering),
        FOREIGN KEY (characterId) REFERENCES character(characterId)
      );

      CREATE TABLE knownFor (
        nconst VARCHAR(255),
        tconst VARCHAR(255),
        FOREIGN KEY (nconst) REFERENCES person(nconst),
        FOREIGN KEY (tconst) REFERENCES movie(tconst)
      );

      CREATE TABLE profession (
        professionId VARCHAR(255) PRIMARY KEY,
        professionName VARCHAR(255)
      );

      CREATE TABLE personProfession (
        nconst VARCHAR(255),
        professionId VARCHAR(255),
        FOREIGN KEY (nconst) REFERENCES person(nconst),
        FOREIGN KEY (professionId) REFERENCES profession(professionId)
      );
    `;

    await pool.request().query(tableCreationQuery);
    console.log('Tables created successfully.');
    return { message: 'Tables created successfully' };
  } catch (error) {
    console.error('Error creating tables:', error);
    throw error;
  }
}

export async function deleteTables() {
    try {
      const pool = await poolPromise;
  
      const tableDeletionQuery = `
        DROP TABLE IF EXISTS movieGenre;
        DROP TABLE IF EXISTS writer;
        DROP TABLE IF EXISTS director;
        DROP TABLE IF EXISTS principalCharacter;
        DROP TABLE IF EXISTS principal;
        DROP TABLE IF EXISTS knownFor;
        DROP TABLE IF EXISTS personProfession;
        DROP TABLE IF EXISTS movie;
        DROP TABLE IF EXISTS genre;
        DROP TABLE IF EXISTS character;
        DROP TABLE IF EXISTS person;
        DROP TABLE IF EXISTS profession;
      `;
  
      await pool.request().query(tableDeletionQuery);
      console.log('Tables deleted successfully.');
      return { message: 'Tables deleted successfully' };
    } catch (error) {
      console.error('Error deleting tables:', error);
      throw error;
    }
}

export async function getTables() {
    try {
      const pool = await poolPromise;
  
      const getTablesQuery = `
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
      `;
  
      const result = await pool.request().query(getTablesQuery);
      return result.recordset;
    } catch (error) {
      console.error('Error fetching table names:', error);
      throw error;
    }
  }
