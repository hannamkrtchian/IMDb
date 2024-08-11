import { poolPromise } from './database.js';

export async function createTrigger() {
  try {
    const pool = await poolPromise;

    await pool.request().query(`
        CREATE TRIGGER trg_MovieUpdate
            ON movie AFTER UPDATE
            AS
        BEGIN
            DECLARE @tconst VARCHAR(255), @primaryTitle VARCHAR(255), @column VARCHAR(255), @oldValue VARCHAR(255), @newValue VARCHAR(255);

            SELECT @tconst = i.tconst, @primaryTitle = i.primaryTitle
            FROM inserted i;

            IF UPDATE(primaryTitle)
            BEGIN
                SELECT @oldValue = d.primaryTitle, @newValue = i.primaryTitle
                FROM deleted d, inserted i
                WHERE d.tconst = i.tconst;

                INSERT INTO updatedMovies (tconst, primaryTitle, updatedColumn, oldValue, newValue, updateTime)
                VALUES (@tconst, @primaryTitle, 'primaryTitle', @oldValue, @newValue, GETDATE());
            END

            IF UPDATE(startYear)
            BEGIN
                SELECT @oldValue = CAST(d.startYear AS VARCHAR(255)), @newValue = CAST(i.startYear AS VARCHAR(255))
                FROM deleted d, inserted i
                WHERE d.tconst = i.tconst;

                INSERT INTO updatedMovies (tconst, primaryTitle, updatedColumn, oldValue, newValue, updateTime)
                VALUES (@tconst, @primaryTitle, 'startYear', @oldValue, @newValue, GETDATE());
            END

            IF UPDATE(endYear)
            BEGIN
                SELECT @oldValue = CAST(d.endYear AS VARCHAR(255)), @newValue = CAST(i.endYear AS VARCHAR(255))
                FROM deleted d, inserted i
                WHERE d.tconst = i.tconst;

                INSERT INTO updatedMovies (tconst, primaryTitle, updatedColumn, oldValue, newValue, updateTime)
                VALUES (@tconst, @primaryTitle, 'endYear', @oldValue, @newValue, GETDATE());
            END
        END;
      `);

    console.log('Trigger created successfully.');
    return { message: 'Trigger created successfully' };
  } catch (error) {
    console.error('Error creating trigger:', error);
    throw error;
  }
}