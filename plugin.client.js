const postgres = function() {
    const package = "postgres";
    const telemetryType = package + ".telemetry";
    console.log(telemetryType);
    var connected = "s-status-on";
    var connectedString = "postgres server available";
    var disconnected = "s-status-off";
    var disconnectedString = "postgres server unavailable";

    var postgresProxyUrl = "/data/postgres";

    function checkDbStatus(callback) {
        http.get(postgresProxyUrl + "/ping")
        .then(function(result){
            if (callback && typeof(result.data) === "boolean") {
                callback(result.data);
            }
        })
        .catch(function(e) {
            if (callback) {
                callback(false);
            } 
        });
    }

    function getUrl(domain, op) {
        var table = encodeURIComponent(domain["table"]);
        var time = encodeURIComponent(domain["time.column"]);
        var value = encodeURIComponent(domain["value.column"]);
        var clause = encodeURIComponent(domain["where"] || "");
        var url = `${postgresProxyUrl}/${op}/${table}/${time}/${value}?${clause}`;
        return url;
    }

    function queryDb(domain, request) {
        var url = getUrl(domain, "query");
        return http.post(url, request).then(function(response) {
            if (Array.isArray(response.data)) {
                return response.data;
            } else {
                return [];
            }
        });
    }

    function postgresTelemetryProvider() {
        this.supportsRequest = function(domain) {
            return domain.type === telemetryType;
        }
        this.supportsSubscribe = function(domain) {
            return domain.type === telemetryType;
        }
        this.request = function(domain, request) {
            return queryDb(domain, request);
        }
        this.subscribe = function(domain, callback, options) {
            var url = getUrl(domain, "listen");
            var socket = new WebSocket(location.origin.replace(/^http/, 'ws') + url);
            socket.onmessage = function(event) {
                try {
                    var point = JSON.parse(event.data);
                    if (callback) {
                        callback(point);
                    } 
                } catch (e) {
                    console.error(e);
                }
            }
            
            return function unsubscribe() {
                socket.close();
            }
        }
        return this;
    }

    return function install (openmct) {
        var PING_DELAY_SECONDS = 5 * 1000;

        // Database status indicator
        var db_indicator = openmct.indicators.simpleIndicator();
        db_indicator.iconClass("icon-database-in-brackets");
        db_indicator.statusClass(disconnected);
        db_indicator.text(disconnectedString);
        openmct.indicators.add(db_indicator);

        // Regular updates to the status indicator
        setInterval(function() {
            checkDbStatus ((available) => {
                if (available) {
                    db_indicator.text(connectedString);
                    db_indicator.statusClass(connected);
                } else {
                    db_indicator.text(disconnectedString);
                    db_indicator.statusClass(disconnected);
                }
            });
        }, PING_DELAY_SECONDS)

        // Custom type for postgres query strings
        openmct.types.addType(telemetryType, {
            name: 'Postgres Telemetry',
            description: 'Telemetry provided by a connection to a PostgreSQL server',
            cssClass: 'icon-telemetry',
            creatable: true,
            initialize: function(model) {
                var telemetry_definition = {
                    values: [
                        {
                            key: "value",               // unique identifier for this field.
                            source: "value",            // identifies the property of a datum where this value is stored. default "key"
                            name: "Value",              // a human readable label for this field. default "key"
                            units: model.units || "None",// the units of this value
                            format: "float",            // a specific format identifier
                            hints: {                    // Hints allow views to intelligently select relevant attributes for display
                                range: 1
                            }
                        },
                        {
                            key: "utc",                 // must match the key of the active time system
                            source: "timestamp",
                            name: "Timestamp",
                            format: "utc",
                            hints: {
                                domain: 1
                            }
                        }
                    ]
                }
                model.telemetry = telemetry_definition;
            },
            form: [
                {
                    "key": "time.column",
                    "name": "Timestamp Column Name",
                    "control": "textfield",
                    "value": "timestamp",
                    "required": true,
                    "cssClass": "l-input-lg"
                },
                {
                    "key": "table",
                    "name": "From",
                    "control": "textfield",
                    "required": true,
                    "cssClass": "l-input-lg"
                },
                {
                    "key": "value.column",
                    "name": "Select",
                    "value": "value",
                    "control": "textfield",
                    "required": true,
                    "cssClass": "l-input-lg"
                },
                {
                    "key": "units",
                    "name": "Units",
                    "control": "textfield",
                    "required": false,
                    "cssClass": "l-input-lg"
                },
                {
                    "key": "where",
                    "name": "Where",
                    "control": "textfield",
                    "required": false,
                    "cssClass": "l-input-lg"
                },
            ]
        });

        // register telemetry provider
        openmct.telemetry.addProvider(new postgresTelemetryProvider());

        console.log("postgres plugin installed");
    }
}