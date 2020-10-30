const oracledb = require('oracledb');
const dbConfig = require('./dbconfig.js');
const parquet = require('parquetjs');
const snowflake = require('snowflake-sdk');

const kickboxKey = MY_KICKBOX_KEY;
const kickbox = require('kickbox').client(kickboxKey).kickbox();

function clean(obj) {
    const propNames = Object.getOwnPropertyNames(obj);
    for (let i = 0; i < propNames.length; i++) {
        const propName = propNames[i];
        if (obj[propName] === null || obj[propName] === undefined) {
            delete obj[propName];
        }
    }
}

async function run() {

    let connection;
    let typeDef;
    let rs;
    let row;
    let writer;

    try {
        // Get a non-pooled connection
        connection = await oracledb.getConnection(dbConfig);

        // The statement to execute
        const sql = "select * from CONTACTS where EMAIL is not null";

        let result;

        // Optional Object Output Format
        result = await connection.execute(
            sql,
            [], // A bind parameter is needed to disambiguate the following options parameter and avoid ORA-01036
            {
                outFormat: oracledb.OUT_FORMAT_OBJECT,     // outFormat can be OBJECT or ARRAY.  The default is ARRAY
                prefetchRows: 100,                       // internal buffer allocation size for tuning
                fetchArraySize: 100,                        // internal buffer allocation size for tuning
                extendedMetaData: true,
                resultSet: true
            }
        );

        let tableSchema = '', thisType = {};

        for (const s of result.metaData) {
            switch (s.dbTypeName) {
                case "VARCHAR2":
                    thisType = 'UTF8';
                    break;
                case 'CHAR':
                    thisType = 'UTF8';
                    break;
                case 'NUMBER':
                    if (s.scale == 0) {
                        if (s.precision > 0 && s.precision < 11) {
                            thisType = 'INT32';
                        } else {
                            thisType = 'INT64';
                        }
                    } else {
                        thisType = 'DOUBLE';
                    }
                    break;
                case 'DATE':
                    thisType = 'TIMESTAMP_MILLIS';
                    break;
                default:
                    console.error("Unexpected Attribute Type for: " + s.name + ". Defaulting to String.");
                    thisType = 'UTF8';
                    break;
            }


            tableSchema = tableSchema + ' "' + s.name + '": { "type" : "' + thisType + '", "optional": "true", "compression": "SNAPPY" },';
            typeDef = {};

        }

        tableSchema = '{' + tableSchema.substr(0, tableSchema.length - 1) + '}';

        const schema = new parquet.ParquetSchema(JSON.parse(tableSchema));
        writer = await parquet.ParquetWriter.openFile(schema, 'dw.parquet');

        rs = result.resultSet;

        while (row = await rs.getRow()) {
            clean(row);
            await writer.appendRow(row);
            kickbox.verify(row.CONTACT_EMAIL, function (err, response) {
                // Let's see some results
                console.log(response.body);
              });        }
    } catch (err) {
        console.error(err);
    } finally {
        if (rs) {
            try {
                await rs.close();
            } catch (err) {
                console.error(err);
            }
        }
        if (connection) {
            try {
                await connection.close();
            } catch (err) {
                console.error(err);
            }
        }
        if (writer) {
            try {
                await writer.close();
            } catch (err) {
                console.error(err);
            }
        }
    }
}

run();

/*
var connection = snowflake.createConnection({
    account: ACCOUNT_ID,
    username: USER_NAME,
    password: PASSWORD
}
);

connection.connect(
    function (err, conn) {
        if (err) {
            console.error('Unable to connect: ' + err.message);
        }
        else {
            console.log('Successfully connected to Snowflake.');
            // Optional: store the connection ID.
            connection_ID = conn.getId();
        }
    }
);

connection.execute({
    sqlText: 'select 1.123456789123456789123456789 as "c1"',
    fetchAsString: ['Number'],
    complete: function(err, stmt, rows) {
      if (err) {
        console.error('Failed to execute statement due to the following error: ' + err.message);
      } else {
        console.log('c1: ' + rows[0].c1); // c1: 1.123456789123456789123456789
      }
    }
  });

connection.destroy(
    function (err, conn) {
        if (err) {
            console.error('Unable to disconnect: ' + err.message);
        } else {
            console.log('Disconnected connection with id: ' + connection.getId());
        }
    });
*/


