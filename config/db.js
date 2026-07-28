
require("dotenv").config();
const { Pool } = require("pg");

const pool = new Pool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    port: parseInt(process.env.DB_PORT),

    ssl: false,
});

// pool.on("connect", () => {
//     console.log("PostgreSQL Connected");
// });

pool.on("connect", (client) => {
    console.log("New PostgreSQL connection created");
    console.log("Total connections:", pool.totalCount);
    console.log("Idle connections:", pool.idleCount);
    console.log("Waiting requests:", pool.waitingCount);
});

pool.on("error", (err) => {
    console.log("DB Error", err);
});

module.exports = pool;