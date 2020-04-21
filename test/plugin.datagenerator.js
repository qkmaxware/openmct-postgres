const pgp = require("pg-promise")();

var pool = pgp({
    host: process.env.OPENMCT_POSTGRES_HOST || "localhost",
    port: parseInt(process.env.OPENMCT_POSTGRES_PORT) || 5432,

    database: process.env.OPENMCT_POSTGRES_DATABASE,
    user: process.env.OPENMCT_POSTGRES_USERNAME,
    password: process.env.OPENMCT_POSTGRES_PASSWORD,
});

var timer = setInterval(() => {
    pool.none("INSERT INTO telemetry (\"timestamp\", \"attribute\", \"value\") VALUES (NOW(), 'test', random() * 25)")
    .then((data) => { })
    .catch((err) => { console.error(err); })
}, 5 * 1000);