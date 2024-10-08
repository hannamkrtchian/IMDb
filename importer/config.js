import * as dotenv from 'dotenv';
dotenv.config();

const config = {
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    server: process.env.DB_HOST,
    database: process.env.DB_NAME,
    options: {
        encrypt: true,
        trustServerCertificate: false,
        connectTimeout: 30000
    },
    port: 1433
};

export const masterConfig = {
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    server: process.env.DB_HOST,
    database: process.env.DB_NAME_SERVER,
    options: {
        encrypt: true,
        trustServerCertificate: false,
        connectTimeout: 30000
    },
    port: 1433
};

export default config;