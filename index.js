import pg from 'pg';
const { Pool } = pg;

// Create a PostgreSQL connection pool
const pool = new Pool({
    user: process.env.PG_USER || 'postgres',
    host: process.env.PG_HOST || 'localhost',
    database: process.env.PG_DATABASE || 'testdb',
    password: process.env.PG_PASSWORD || 'password',
    port: process.env.PG_PORT || 5432
});

/**
 * Executes a query on the PostgreSQL database.
 * @param {string} text - The SQL query text.
 * @param {Array} [params=[]] - The query parameters.
 * @returns {Promise<Array>} - The query result rows.
 */
async function query(text, params = []) {
    const client = await pool.connect();
    try {
        const res = await client.query(text, params);
        return res.rows;
    } finally {
        client.release();
    }
}

/**
 * Inserts data into a table.
 * @param {string} table - The table name.
 * @param {Object} data - An object with column-value pairs.
 * @returns {Promise<Object>} - The inserted row.
 */
async function insert(table, data) {
    const keys = [];
    const values = [];
    for (const [key, value] of Object.entries(data)) {
        keys.push(key);
        values.push(value);
    }
    const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');

    const queryText = `INSERT INTO ${table} (${keys.join(', ')}) VALUES (${placeholders}) RETURNING *;`;
    const result = await query(queryText, values);
    return result[0];
}

/**
 * Selects data from a table.
 * @param {string} table - The table name.
 * @param {Object} [where={}] - An object with column-value pairs for the WHERE clause.
 * @returns {Promise<Array>} - The selected rows.
 */
async function select(table, where = {}) {
    const keys = Object.keys(where);
    const values = Object.values(where);
    const conditions = keys.map((key, i) => `${key} = $${i + 1}`).join(' AND ');
    const queryText = `SELECT * FROM ${table}${conditions ? ' WHERE ' + conditions : ''};`;
    const result = await query(queryText, values);
    return result;
}
async function create(table, columns) {
    const columnDefinitions = Object.entries(columns)
        .map(([key, value]) => `${key} ${value}`)
        .join(', ');

    const queryText = `CREATE TABLE IF NOT EXISTS ${table} (${columnDefinitions});`;
    
    return await query(queryText);
}
/**
 * 
 * @param {string} table - Table name 
 * @param {Array} changes - Array of changes to be made
 * @returns {Promise<Array>} - The result of the query execution.
 */
async function alterTable(table, changes) {
    const alterations = [];

    // Handling ADD columns
    if (changes.add) {
        for (const [column, type] of Object.entries(changes.add)) {
            alterations.push(`ADD COLUMN ${column} ${type}`);
        }
    }

    // Handling DROP columns
    if (changes.drop) {
        for (const column of changes.drop) {
            alterations.push(`DROP COLUMN ${column}`);
        }
    }

    // Handling MODIFY columns (changing data type)
    if (changes.modify) {
        for (const [column, newType] of Object.entries(changes.modify)) {
            alterations.push(`ALTER COLUMN ${column} TYPE ${newType}`);
        }
    }

    // If no changes, return early
    if (alterations.length === 0) {
        throw new Error("No valid alterations provided.");
    }

    // Constructing the final query
    const queryText = `ALTER TABLE ${table} ${alterations.join(', ')};`;

    return await query(queryText);
}
//Creates database
async function createDB(database) {
    // Basic validation (you can extend this further)
    if (!/^[a-zA-Z0-9_]+$/.test(database)) {
        throw new Error('Invalid database name');
    }

    const newDatabase = `CREATE DATABASE ${database}`;
    return await query(newDatabase);
}
/**
 * Drops a table or a database.
 * @param {string} type - Type of the object to drop ("table" or "database").
 * @param {string} name - The name of the table or database.
 * @returns {Promise} - Result of the query execution.
 * @throws {Error} - Throws error if invalid type is provided.
 */
async function drop(type, name) {
    // Basic input validation (you can adjust this regex as needed)
    if (!/^[a-zA-Z0-9_]+$/.test(name)) {
        throw new Error('Invalid name provided. Only alphanumeric characters and underscores are allowed.');
    }

    if (type === 'table') {
        return await query(`DROP TABLE IF EXISTS ${name}`);
    } else if (type === 'database') {
        return await query(`DROP DATABASE IF EXISTS ${name}`);
    }
    
    if (type === 'column') {
        throw new Error("Columns cannot be dropped using this function, use the alterTable function to drop columns.");
    } else {
        throw new Error(`Invalid type (${type}) provided. Use "table" or "database".`);
    }
}
async function update(table, data, where) {
    const setString = Object.keys(data)
        .map((key, i) => `${key} = $${i + 1}`)
        .join(', ');
    const whereString = Object.keys(where)
        .map((key, i) => `${key} = $${i + 1 + Object.keys(data).length}`)
        .join(' AND ');
    const values = [...Object.values(data), ...Object.values(where)];
    const queryText = `UPDATE ${table} SET ${setString} WHERE ${whereString};`;
    await query(queryText, values);
}
async function insertBulk(table, dataArray) {
    if (dataArray.length === 0) throw new Error("No data provided for bulk insert.");
    
    const keys = Object.keys(dataArray[0]);
    const values = dataArray.map(obj => keys.map(k => obj[k]));
    
    const placeholders = values
        .map((row, i) => `(${row.map((_, j) => `$${i * keys.length + j + 1}`).join(', ')})`)
        .join(', ');

    const queryText = `INSERT INTO ${table} (${keys.join(', ')}) VALUES ${placeholders} RETURNING *;`;
    return await query(queryText, values.flat());
}
async function copy(table, file, format, header) {
    if(format==='csv'){
        if(header){
            return await query(`COPY ${table} FROM '${file}' WITH (FORMAT csv, HEADER true)`);
        }else{
            return await query( `COPY ${table} FROM '${file}' WITH (FORMAT csv, HEADER false)`);
        }
    }
    
}


// Exported functions
async function updateTable(table, data, where) {
    try {
        return await update(table, data, where);
    } catch (error) {
        console.error("Database update error:", error);
        throw new Error("Failed to update the table.");
    }
    
}
async function createTable(table,columns){
    try {
        return await create(table,columns);
    } catch (error) {
        console.error("Database query error:", error);
        throw new Error("Failed to execute database query.");
    }
}
async function insertIntoTable(table, data) {
    try {
        if (Array.isArray(data)) {
            return await insertBulk(table, data);
        }
        return await insert(table, data);
    } catch (error) {
        console.error(`Error inserting into '${table}':`, error);
        throw new Error(`Failed to insert into '${table}'.`);
    }
}
async function selectFromTable(table, where = {}) {
    try {
        return await select(table, where);
    } catch (error) {
        console.error("Database query error:", error);
        throw new Error("Failed to execute database query.");
    }
}
async function dropTable(name) {
    try{
        return await drop('table', name);
    } catch (error) {
        console.error("Database query error:", error);
        throw new Error("Failed to execute database query.");
    }
}

async function dropDatabase(name) {
    try{
        return await drop('database', name);
    } catch (error) {
        console.error("Database query error:", error);
        throw new Error("Failed to execute database query.");
    }
}
async function dropColumn(table, column) {
    // Convert single column to an array if it's not already one
    const columns = Array.isArray(column) ? column : [column];

    return await alterTable(table, { drop: columns });
}
async function createDatabase(name) {
    try{
        return await createDB(name);
    } catch (error) {
        console.error("Database query error:", error);
        throw new Error("Failed to execute database query.");
    }
}
async function modifyColumns(table, columns) {
    try{
        return await alterTable(table, { modify: columns });
    }catch (error){
        console.error("Database query error:", error);
        throw new Error("Failed to execute database query.");
    }
}
async function addColumns(table, columns) {
    try{
        return await alterTable(table, { add: columns });
    }catch (error){
        console.error(`Failed to add '${columns}' to '${table}':`, error);
        throw new Error(`Failed to add '${columns}' to '${table}'.`);
    }
}
//Export
export {
    createTable,
    createDatabase,
    dropTable,
    dropDatabase,
    dropColumn,
    addColumns,
    modifyColumns,
    selectFromTable,
    insertIntoTable,
    updateTable
};
