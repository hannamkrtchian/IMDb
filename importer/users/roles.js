import { poolPromise } from '../database.js';

export async function createRoles() {
  try {
    const pool = await poolPromise;

    await pool.request().query(`
        CREATE ROLE ReadViews;

        GRANT SELECT ON OBJECT::dbo.MovieDirectorsWriters TO ReadViews;
        GRANT SELECT ON OBJECT::dbo.MovieGenresView TO ReadViews;
        GRANT SELECT ON OBJECT::dbo.AllMovieInfoView TO ReadViews;
        GRANT SELECT ON OBJECT::dbo.PersonProfessionsView TO ReadViews;


        CREATE ROLE InsertAndModify;

        GRANT INSERT, UPDATE ON SCHEMA::dbo TO InsertAndModify;
        DENY INSERT, UPDATE ON OBJECT::dbo.updatedMovies TO InsertAndModify;
    `);

    console.log('Roles created successfully.');
    return { message: 'Roles created successfully' };
  } catch (error) {
    console.error('Error creating roles:', error);
    throw error;
  }
}