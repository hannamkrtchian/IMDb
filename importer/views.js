import { poolPromise } from './database.js';

export async function createViews() {
  try {
    const pool = await poolPromise;

    await pool.request().query(`
        CREATE VIEW MovieDirectorsWriters AS
        SELECT
            m.tconst,
            m.primaryTitle,
            m.startYear,
            'Director' AS Role,
            p.nconst,
            p.primaryName
        FROM
            movie m
        JOIN
            director d ON m.tconst = d.tconst
        JOIN
            person p ON d.nconst = p.nconst
        UNION ALL
        SELECT
            m.tconst,
            m.primaryTitle,
            m.startYear,
            'Writer' AS Role,
            p.nconst,
            p.primaryName
        FROM
            movie m
        JOIN
            writer w ON m.tconst = w.tconst
        JOIN
            person p ON w.nconst = p.nconst;
      `);


      await pool.request().query(`
        CREATE VIEW MovieGenresView AS
        SELECT
            m.tconst,
            m.primaryTitle,
            g.genreName
        FROM
            movie m
        JOIN
            movieGenre mg ON m.tconst = mg.tconst
        JOIN
            genre g ON mg.genreId = g.genreId;
      `);


      await pool.request().query(`
        CREATE VIEW AllMovieInfoView AS
        SELECT
            m.tconst,
            m.titleType,
            m.primaryTitle,
            m.originalTitle,
            m.isAdult,
            m.startYear,
            m.endYear,
            m.runtimeMinutes,
            g.genreName,
            p.primaryName,
            p.birthYear,
            p.deathYear,
            pr.category,
            pr.job
        FROM
            movie m
        LEFT JOIN
            movieGenre mg ON m.tconst = mg.tconst
        LEFT JOIN
            genre g ON mg.genreId = g.genreId
        LEFT JOIN
            principal pr ON m.tconst = pr.tconst
        LEFT JOIN
            person p ON pr.nconst = p.nconst;
      `);
        

      await pool.request().query(`
        CREATE VIEW PersonProfessionsView AS
        SELECT
            p.nconst,
            p.primaryName,
            pr.professionName
        FROM
            person p
        JOIN
            personProfession pp ON p.nconst = pp.nconst
        JOIN
            profession pr ON pp.professionId = pr.professionId;
      `);

    console.log('Views created successfully.');
    return { message: 'Views created successfully' };
  } catch (error) {
    console.error('Error creating views:', error);
    throw error;
  }
}
