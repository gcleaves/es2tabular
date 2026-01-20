import duckdb from 'duckdb';
import path from 'path';
import fs from 'fs';

/**
 * DuckDB Service - Server-side DuckDB with persistent storage
 */
class DuckDBService {
  constructor(dataDir) {
    this.dataDir = dataDir;
    this.dbPath = path.join(dataDir, 'es2tabular.duckdb');
    this.db = null;
    this.connection = null;
    this.initialized = false;
  }

  /**
   * Initialize DuckDB with persistent database
   */
  async init() {
    if (this.initialized) return;

    return new Promise((resolve, reject) => {
      // Create database with persistent file
      this.db = new duckdb.Database(this.dbPath, (err) => {
        if (err) {
          console.error('Failed to open DuckDB database:', err);
          reject(err);
          return;
        }

        this.connection = this.db.connect();
        
        // Install and load httpfs extension for S3 support
        this.connection.run(`
          INSTALL httpfs;
          LOAD httpfs;
        `, (err) => {
          if (err) {
            console.warn('httpfs extension not available:', err.message);
          }
          
          this.initialized = true;
          console.log(`DuckDB initialized with database: ${this.dbPath}`);
          resolve();
        });
      });
    });
  }

  /**
   * Execute a SQL query and return results
   * @param {string} sql - SQL query to execute
   * @returns {Promise<{columns: string[], rows: object[], rowCount: number}>}
   */
  async query(sql) {
    if (!this.initialized) {
      await this.init();
    }

    return new Promise((resolve, reject) => {
      this.connection.all(sql, (err, rows) => {
        if (err) {
          reject(err);
          return;
        }

        // Extract column names from first row or empty array
        const columns = rows.length > 0 ? Object.keys(rows[0]) : [];
        
        // Convert BigInt values to numbers/strings for JSON serialization
        const serializedRows = rows.map(row => {
          const newRow = {};
          for (const [key, value] of Object.entries(row)) {
            if (typeof value === 'bigint') {
              // Convert to number if safe, otherwise string
              newRow[key] = Number.isSafeInteger(Number(value)) ? Number(value) : value.toString();
            } else if (value instanceof Date) {
              newRow[key] = value.toISOString();
            } else if (Buffer.isBuffer(value)) {
              newRow[key] = value.toString('base64');
            } else {
              newRow[key] = value;
            }
          }
          return newRow;
        });
        
        resolve({
          columns,
          rows: serializedRows,
          rowCount: serializedRows.length
        });
      });
    });
  }

  /**
   * Execute a SQL statement without returning results (for DDL, etc.)
   * @param {string} sql - SQL statement to execute
   */
  async run(sql) {
    if (!this.initialized) {
      await this.init();
    }

    return new Promise((resolve, reject) => {
      this.connection.run(sql, (err) => {
        if (err) {
          reject(err);
          return;
        }
        resolve();
      });
    });
  }

  /**
   * Load a CSV file into a table
   * @param {string} csvPath - Path to CSV file (local or S3 URL)
   * @param {string} tableName - Name for the table
   * @param {object} options - Additional options
   */
  async loadCsv(csvPath, tableName, options = {}) {
    if (!this.initialized) {
      await this.init();
    }

    // Sanitize table name
    const safeTableName = tableName.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase();
    
    // Build the CREATE TABLE statement
    const createOrReplace = options.replace !== false ? 'CREATE OR REPLACE TABLE' : 'CREATE TABLE IF NOT EXISTS';
    
    const sql = `${createOrReplace} ${safeTableName} AS SELECT * FROM read_csv_auto('${csvPath}')`;
    
    await this.run(sql);
    
    // Get row count
    const result = await this.query(`SELECT COUNT(*) as count FROM ${safeTableName}`);
    const rawCount = result.rows[0]?.count;
    const rowCount = typeof rawCount === 'bigint' ? Number(rawCount) : (rawCount || 0);
    
    // Get column info
    const schemaResult = await this.query(`DESCRIBE ${safeTableName}`);
    const columns = schemaResult.rows.map(r => ({
      name: r.column_name,
      type: r.column_type
    }));

    return {
      tableName: safeTableName,
      rowCount,
      columns
    };
  }

  /**
   * List all tables in the database
   */
  async listTables() {
    if (!this.initialized) {
      await this.init();
    }

    const result = await this.query(`
      SELECT table_name, 
             (SELECT COUNT(*) FROM information_schema.columns c WHERE c.table_name = t.table_name) as column_count
      FROM information_schema.tables t
      WHERE table_schema = 'main'
      ORDER BY table_name
    `);

    // Get row counts for each table
    const tables = [];
    for (const row of result.rows) {
      try {
        const countResult = await this.query(`SELECT COUNT(*) as count FROM "${row.table_name}"`);
        const rowCount = countResult.rows[0]?.count;
        tables.push({
          name: row.table_name,
          columnCount: typeof row.column_count === 'bigint' ? Number(row.column_count) : row.column_count,
          rowCount: typeof rowCount === 'bigint' ? Number(rowCount) : (rowCount || 0)
        });
      } catch (e) {
        tables.push({
          name: row.table_name,
          columnCount: typeof row.column_count === 'bigint' ? Number(row.column_count) : row.column_count,
          rowCount: null
        });
      }
    }

    return tables;
  }

  /**
   * Get schema for a specific table
   * @param {string} tableName - Table name
   */
  async describeTable(tableName) {
    if (!this.initialized) {
      await this.init();
    }

    const result = await this.query(`DESCRIBE "${tableName}"`);
    return result.rows.map(r => ({
      name: r.column_name,
      type: r.column_type,
      nullable: r.null === 'YES'
    }));
  }

  /**
   * Drop a table
   * @param {string} tableName - Table name
   */
  async dropTable(tableName) {
    if (!this.initialized) {
      await this.init();
    }

    const safeTableName = tableName.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase();
    await this.run(`DROP TABLE IF EXISTS "${safeTableName}"`);
    return { success: true, tableName: safeTableName };
  }

  /**
   * Configure S3 credentials
   * @param {object} config - S3 configuration
   */
  async configureS3(config) {
    if (!this.initialized) {
      await this.init();
    }

    const statements = [];
    
    if (config.accessKeyId) {
      statements.push(`SET s3_access_key_id='${config.accessKeyId}'`);
    }
    if (config.secretAccessKey) {
      statements.push(`SET s3_secret_access_key='${config.secretAccessKey}'`);
    }
    if (config.region) {
      statements.push(`SET s3_region='${config.region}'`);
    }
    if (config.endpoint) {
      statements.push(`SET s3_endpoint='${config.endpoint}'`);
    }
    if (config.sessionToken) {
      statements.push(`SET s3_session_token='${config.sessionToken}'`);
    }

    for (const sql of statements) {
      await this.run(sql);
    }

    return { success: true };
  }

  /**
   * Close the database connection
   */
  async close() {
    if (this.connection) {
      this.connection.close();
    }
    if (this.db) {
      this.db.close();
    }
    this.initialized = false;
  }
}

export { DuckDBService };
