const pgp = require("pg-promise")();
const express = require("express");
const ws = require("express-ws");

module.exports = function install(app) {
    var router = new express.Router();
    var path = "/data/postgres";

    var pool = null;
    if (process.env.OPENMCT_POSTGRES_USERNAME && process.env.OPENMCT_POSTGRES_DATABASE && process.env.OPENMCT_POSTGRES_PASSWORD) {
        pool = pgp({
            host: process.env.OPENMCT_POSTGRES_HOST || "localhost",
            port: parseInt(process.env.OPENMCT_POSTGRES_PORT) || 5432,

            database: process.env.OPENMCT_POSTGRES_DATABASE,
            user: process.env.OPENMCT_POSTGRES_USERNAME,
            password: process.env.OPENMCT_POSTGRES_PASSWORD,
        });
    }

    function Listener(config) {
        // https://github.com/vitaly-t/pg-promise/wiki/Robust-Listeners
        if (!config.handler) {
            config.handler = {};
        }
        var onNotification = config.handler.notify || function(data) {}
        var onDisconnect = config.handler.lost || function(err) {}
    
        this.connection = config.connection;
        var channel = config.channel
        var maxAttempts = 10;
        var reconnectionTime = 5 * 1000;
        var connection = null;

        function removeListeners(client) {
            client.removeListener('notification', onNotification);
        }

        function setListeners(client) {
            client.on('notification', onNotification);
            return connection.none('LISTEN $1~', [ channel ])
                .catch(error => {
                    console.log("pg-listener failed to attach" + error);
                });
        }

        function onConnectionLost (err, e) {
            connection = null;
            removeListeners(e.client);
            reconnect(reconnectionTime, maxAttempts)
                .then((obj) => {})
                .catch((_) => {
                    onDisconnect(err);
                })
        }

        function reconnect(delay, attempts) {
            return new Promise((resolve, reject) => {
                setTimeout(() => {
                    pool
                    .connect({direct: true, onLost: onConnectionLost})
                    .then((obj => {
                        connection = obj;
                        resolve(obj);
                        return setListeners(obj.client);
                    }))
                    .catch(err => {
                        if (--attempts) {
                            reconnect(delay, attempts)
                            .then(resolve)
                            .catch(reject);
                        } else {
                            reject(error);
                        }
                    });
                },delay );
            });
        }

        this.listen = function() {
            reconnect(reconnectionTime, maxAttempts)
                .then(obj => {

                })
                .catch((err) => {
                    onDisconnect(err);
                })
        }

        this.stop = function() {
            if (connection) {
                connection.done();
            }
        }

        return this;
    }

    function SqlTelemetry(config) {
        this.table = config.table;

        function getWhereClause(offset) {
            if (!config.filters) {
                return { sql: "", values: [] };
            }
            var sql = ""; 
            var values = [];
            var first = true;
            for (var column in config.filters) {
                var value = config.filters[column];
                if (!first) {
                    sql += " AND ";
                } else {
                    first = false;
                }
                sql += "$" + (offset++) + ":name=$" + (offset++);
                values.push(column);
                values.push(value);
            }

            return { sql: sql, values: values };
        }

        function clean(rows) {
            if (rows.length > 0){
                for (var i = 0; i < rows.length; i++) {
                    if (rows[i].timestamp instanceof Date) {
                        rows[i].timestamp = rows[i].timestamp.getTime() - (rows[i].timestamp.getTimezoneOffset() * 60000);
                    }
                }
            } 
            return rows;
        }

        // start and end are always date objects
        this.between = function(start, end) {
            var where = getWhereClause(6);
            var sql = where.sql;
            if (where.values.length > 0) {
                sql = " AND " + sql;
            }
            return pool.query(
                "SELECT $1:name AS \"timestamp\", $2:name AS \"value\" FROM $3:name WHERE $1:name >= $4 AND $1:name <= $5" + sql,
                [ config.timestamp, config.value, config.table, start.toUTCString(), end.toUTCString() ].concat(where.values)
            ).then(clean);
        }

        this.latest = function(count) {
            var where = getWhereClause(5);
            var sql = where.sql;
            if (where.values.length > 0) {
                sql = "WHERE " + sql;
            }
            return pool.query(
                "SELECT $1:name AS \"timestamp\", $2:name AS \"value\" FROM $3:name " + sql + " ORDER BY $1:name DESC LIMIT $4",
                [ config.timestamp, config.value, config.table, count || 1 ].concat(where.values)
            ).then(clean);
        }

        this.latestBetween = function(start, end, count) {
            var where = getWhereClause(7);
            var sql = where.sql;
            if (where.values.length > 0) {
                sql = " AND " + sql;
            }
            return pool.query(
                "SELECT $1:name AS \"timestamp\", $2:name AS \"value\" FROM $3:name WHERE $1:name >= $4 AND $1:name <= $5 " + sql + " ORDER BY $1:name DESC LIMIT $6",
                [ config.timestamp, config.value, config.table, start.toUTCString(), end.toUTCString(), count || 1 ].concat(where.values)
            ).then(clean);
        }

        this.minmax = function(start, end) {
            var where = getWhereClause(6);
            var sql = where.sql;
            if (where.values.length > 0) {
                sql = " AND " + sql;
            }
            return pool.query(
                "SELECT MAX($2:name) AS \"max\", MIN($2:name) AS \"min\" FROM $3:name WHERE $1:name >= $4 AND $1:name <= $5" + sql,
                [ config.timestamp, config.value, config.table, start.toUTCString(), end.toUTCString() ].concat(where.values)
            ).then(function(rows) {
                var row = rows[0];
                return [
                    { timestamp: start, value: row.min },
                    { timestamp: end, value: row.max }
                ]
            }).then(clean);
        }

        return this;
    }

    function ping(callback) {
        if (pool !== null) {
            pool.query("SELECT 1", [])
            .then(function(data) {
                if (data.length >= 1) {
                    callback(true);
                } else {
                    callback(false);
                }
            })
            .catch(function(err) {
                callback(false);
            });
        } else {
            callback(false);
        }
    }

    function query(req, res, request) {
        // Safety check
        if (pool == null) {
            res.json([]);
            return;
        }

        var telemetry = new SqlTelemetry({
            table: req.params.table,
            timestamp: req.params.timeColumn,
            value: req.params.valueColumn,
            filters: req.query,
        });

        var data = null;
        if (request) {
            switch (request.type) {
                case "latest": {
                    if (request.start && request.end) {
                        data = telemetry.latestBetween(new Date(request.start), new Date(request.end), request.size || 1);
                    } else {
                        data = telemetry.latest(request.size || 1);
                    }
                } break;
                case "minmax": {
                    data = telemetry.minmax(new Date(request.start), new Date(request.end));
                } break;
            }
        } else {
            data = telemetry.latest(1);
        }
        if (data == null) {
            res.json([]);
            return;
        } else {
            data.then(function(rows) {
                // convert Date to date integer
                res.json(rows);
            }).catch(function(error) {
                res.json([]);
            });
        }
    }

    router.get("/ping", function(req, res) {
        ping((available) => { res.json(available); });
    });

    router.ws("/listen/:table/:timeColumn/:valueColumn", function(ws, req) {
        if (!pool) {
            ws.close();
            return;
        }

        // Get resource sql object
        var telemetry = new SqlTelemetry({
            table: req.params.table,
            timestamp: req.params.timeColumn,
            value: req.params.valueColumn,
            filters: req.query,
        });
        function notify(point) {
            ws.send(JSON.stringify(point));
        }

        function onMessage (message) {
            telemetry.latest(1).then((rows) => {
                if (rows.length > 0)
                    notify(rows[0]); // get the first data-point
            });
        }

        // set up handlers for if the client sends messages
        var handlers = {
            request: onMessage
        }

        // set up database listener
        var listener = new Listener({
            connection: pool,
            channel: "openmct." + telemetry.table,
            handler: {
                notify: onMessage,
                lost: function(err) {
                    
                }
            }
        });

        // start listening
        listener.listen();

        ws.on('message', function(message) {
            var data = JSON.parse(message);
            if (data.type && handlers[data.type]) {
                handlers[data.type](message.payload);
            }
        });

        ws.on('close', function() {
            // stop listening
            listener.stop();
        });
    });

    router.get("/query/:table/:timeColumn/:valueColumn", function(req, res) {
        if (!pool) {
            res.json([]);
            return;
        }

        query(req, res, null);
    });
    router.post("/query/:table/:timeColumn/:valueColumn", function(req, res) {
        if (!pool) {
            res.json([]);
            return;
        }

        if (!req.body) {
            query(req, res, {
                strategy: "latest",
                size: 1
            });
            return;
        } else {
            query(req, res, req.body);
        }
    });

    app.use(path, router);
}