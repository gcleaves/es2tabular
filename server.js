import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import morgan from 'morgan';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { KibanaClient } from './lib/kibana-client.js';
import { esToTable, tableToCSV } from './index.js';
import { DuckDBService } from './lib/duckdb-service.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;
const BASE_PATH = process.env.BASE_PATH || '';

// Auth configuration
const AUTH_REQUIRED = process.env.AUTH_REQUIRED === 'true';
const AUTH_ALLOWED_DOMAIN = process.env.AUTH_ALLOWED_DOMAIN || 'mcpinsight.com';
const AUTH_HEADER = 'x-auth-request-preferred-username';

// Create a router for all routes
const router = express.Router();

// Middleware
app.use(cors());
app.use(express.json({ limit: '50mb' }));

// Request logging - custom format to include x- headers
morgan.token('x-headers', (req) => {
  const xHeaders = Object.entries(req.headers)
    .filter(([key]) => key.startsWith('x-'))
    .map(([key, value]) => `${key}=${value}`)
    .join(', ');
  return xHeaders || '-';
});
app.use(morgan(':method :url :status :response-time ms [:x-headers]'));

// Paths that bypass authentication (health checks, etc.)
const AUTH_BYPASS_PATHS = ['/api/duckdb/status'];

// Domain authentication middleware
const domainAuthMiddleware = (req, res, next) => {
  // Skip auth for health check endpoints
  if (AUTH_BYPASS_PATHS.includes(req.path)) {
    return next();
  }

  if (!AUTH_REQUIRED) {
    return next();
  }

  const username = req.headers[AUTH_HEADER];

  if (!username) {
    console.warn(`Auth: Missing ${AUTH_HEADER} header`);
    return res.status(401).json({ 
      error: 'Unauthorized', 
      message: 'Authentication required' 
    });
  }

  const domain = username.split('@')[1];
  if (!domain || domain.toLowerCase() !== AUTH_ALLOWED_DOMAIN.toLowerCase()) {
    console.warn(`Auth: User ${username} denied - domain not in allowed list`);
    return res.status(403).json({ 
      error: 'Forbidden', 
      message: `Access restricted to @${AUTH_ALLOWED_DOMAIN} users` 
    });
  }

  // Attach user info to request for downstream use
  req.authUser = username;
  next();
};

// Apply auth middleware to the router
router.use(domainAuthMiddleware);

// Create base data directory if it doesn't exist
const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

// Initialize Kibana client
const kibanaClient = new KibanaClient();

// Map to store DuckDB service instances per user
const duckdbInstances = new Map();

/**
 * Get user-specific data directory
 * @param {string} username - Username (email)
 * @returns {string} Path to user's data directory
 */
function getUserDataDir(username) {
  if (!username) {
    // Fallback to shared directory if no username (when auth is disabled)
    return DATA_DIR;
  }
  // Sanitize username for filesystem (replace @ and other special chars)
  const safeUsername = username.replace(/[^a-zA-Z0-9._-]/g, '_');
  const userDir = path.join(DATA_DIR, safeUsername);
  if (!fs.existsSync(userDir)) {
    fs.mkdirSync(userDir, { recursive: true });
  }
  return userDir;
}

/**
 * Get or create DuckDB service instance for a user
 * @param {string} username - Username (email)
 * @returns {DuckDBService} DuckDB service instance
 */
function getDuckDBService(username) {
  if (!username) {
    // Fallback to shared instance if no username (when auth is disabled)
    if (!duckdbInstances.has('')) {
      const sharedDir = getUserDataDir('');
      const service = new DuckDBService(sharedDir);
      duckdbInstances.set('', service);
    }
    return duckdbInstances.get('');
  }

  if (!duckdbInstances.has(username)) {
    const userDir = getUserDataDir(username);
    const service = new DuckDBService(userDir);
    duckdbInstances.set(username, service);
  }
  return duckdbInstances.get(username);
}

/**
 * API Route: Execute Elasticsearch query
 * POST /api/query
 * Body: { index: string, query: object }
 */
router.post('/api/query', async (req, res) => {
  try {
    const { index, query } = req.body;
    const username = req.authUser || '';

    if (!index) {
      return res.status(400).json({ error: 'Index is required' });
    }

    if (!query) {
      return res.status(400).json({ error: 'Query is required' });
    }

    console.log(`Executing query on index: ${index} for user: ${username || 'anonymous'}`);
    
    // Execute query via Kibana
    const esResponse = await kibanaClient.search(index, query);

    // Check if response has aggregations
    const hasAggregations = esResponse.aggregations && Object.keys(esResponse.aggregations).length > 0;

    // Generate descriptive filename from aggregation names + timestamp
    const now = new Date();
    const timestamp = now.toISOString().replace(/[-:]/g, '').replace(/\.\d{3}Z$/, '').replace('T', 'T');
    let namePart = 'query';
    if (hasAggregations) {
      // Extract unique aggregation names recursively
      const aggNames = new Set();
      const extractAggNames = (obj) => {
        if (!obj || typeof obj !== 'object') return;
        for (const key of Object.keys(obj)) {
          if (obj[key] && typeof obj[key] === 'object' && obj[key].buckets !== undefined) {
            aggNames.add(key);
            // Check for nested aggregations in first bucket only (to get structure, not duplicates)
            const buckets = Array.isArray(obj[key].buckets) ? obj[key].buckets : Object.values(obj[key].buckets || {});
            if (buckets.length > 0) {
              extractAggNames(buckets[0]);
            }
          }
        }
      };
      extractAggNames(esResponse.aggregations);
      if (aggNames.size > 0) {
        namePart = [...aggNames].join('_').replace(/[^a-zA-Z0-9_]/g, '');
        // Limit filename length (leave room for timestamp and extension)
        if (namePart.length > 100) {
          namePart = namePart.substring(0, 100);
        }
      }
    }
    const filename = `${namePart}_${timestamp}.json`;
    const userDataDir = getUserDataDir(username);
    const filepath = path.join(userDataDir, filename);

    // Save JSON response locally
    fs.writeFileSync(filepath, JSON.stringify(esResponse, null, 2), 'utf8');
    
    res.json({
      success: true,
      filename,
      filepath: `/api/files/${filename}`,
      hasAggregations,
      hits: esResponse.hits?.hits?.length || 0,
      total: esResponse.hits?.total?.value || esResponse.hits?.total || 0,
    });
  } catch (error) {
    console.error('Error executing query:', error);
    res.status(500).json({ 
      error: 'Failed to execute query',
      message: error.message 
    });
  }
});

/**
 * API Route: Convert JSON file to CSV
 * POST /api/convert
 * Body: { filename: string, aggregationName?: string }
 */
router.post('/api/convert', async (req, res) => {
  try {
    const { filename, aggregationName } = req.body;
    const username = req.authUser || '';

    if (!filename) {
      return res.status(400).json({ error: 'Filename is required' });
    }

    const userDataDir = getUserDataDir(username);
    const filepath = path.join(userDataDir, filename);
    
    if (!fs.existsSync(filepath)) {
      return res.status(404).json({ error: 'File not found' });
    }

    // Read JSON file
    const esOutput = JSON.parse(fs.readFileSync(filepath, 'utf8'));

    // Convert to table
    const options = aggregationName ? { aggregationName } : {};
    const table = esToTable(esOutput, options);

    if (table.length === 0) {
      return res.status(400).json({ 
        error: 'No data to convert. Make sure the query includes aggregations.' 
      });
    }

    // Convert to CSV
    const csv = tableToCSV(table);

    // Generate descriptive filename from column headers + timestamp
    const columns = Object.keys(table[0] || {});
    let columnPart = columns.join('_').replace(/[^a-zA-Z0-9_]/g, '');
    // Limit filename length (leave room for timestamp and extension)
    if (columnPart.length > 100) {
      columnPart = columnPart.substring(0, 100);
    }
    const now = new Date();
    const timestamp = now.toISOString().replace(/[-:]/g, '').replace(/\.\d{3}Z$/, '').replace('T', 'T');
    const csvFilename = `${columnPart}_${timestamp}.csv`;
    const csvFilepath = path.join(userDataDir, csvFilename);
    fs.writeFileSync(csvFilepath, csv, 'utf8');

    res.json({
      success: true,
      csvFilename,
      csvFilepath: `/api/files/${csvFilename}`,
      rows: table.length,
      columns: Object.keys(table[0] || {}),
    });
  } catch (error) {
    console.error('Error converting to CSV:', error);
    res.status(500).json({ 
      error: 'Failed to convert to CSV',
      message: error.message 
    });
  }
});

/**
 * API Route: Get file list
 * GET /api/files
 */
router.get('/api/files', (req, res) => {
  try {
    const username = req.authUser || '';
    const userDataDir = getUserDataDir(username);
    
    const files = fs.readdirSync(userDataDir)
      .filter(filename => {
        // Filter out directories and hidden files
        const filepath = path.join(userDataDir, filename);
        return fs.statSync(filepath).isFile() && !filename.startsWith('.');
      })
      .map(filename => {
        const filepath = path.join(userDataDir, filename);
        const stats = fs.statSync(filepath);
        // Use mtime (modification time) as primary, fallback for containers where birthtime may not be reliable
        const created = stats.birthtime && stats.birthtime.getTime() > 0 ? stats.birthtime : stats.mtime;
        return {
          filename,
          size: stats.size,
          created,
          modified: stats.mtime,
          url: `/api/files/${filename}`,
        };
      })
      .sort((a, b) => new Date(b.modified) - new Date(a.modified));

    res.json({ files });
  } catch (error) {
    console.error('Error listing files:', error);
    res.status(500).json({ 
      error: 'Failed to list files',
      message: error.message 
    });
  }
});

/**
 * API Route: Download file
 * GET /api/files/:filename
 */
router.get('/api/files/:filename', (req, res) => {
  try {
    const { filename } = req.params;
    const username = req.authUser || '';
    const userDataDir = getUserDataDir(username);
    const filepath = path.join(userDataDir, filename);

    // Prevent path traversal
    if (!filepath.startsWith(userDataDir)) {
      return res.status(403).json({ error: 'Access denied' });
    }

    if (!fs.existsSync(filepath)) {
      return res.status(404).json({ error: 'File not found' });
    }

    // Set appropriate content type
    const ext = path.extname(filename).toLowerCase();
    const contentType = ext === '.csv' ? 'text/csv' : 'application/json';
    
    res.setHeader('Content-Type', contentType);
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    
    // Prevent caching to ensure fresh downloads
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
    
    const fileStream = fs.createReadStream(filepath);
    fileStream.pipe(res);
  } catch (error) {
    console.error('Error downloading file:', error);
    res.status(500).json({ 
      error: 'Failed to download file',
      message: error.message 
    });
  }
});

/**
 * API Route: Rename file
 * PUT /api/files/:filename
 * Body: { newFilename: string }
 */
router.put('/api/files/:filename', (req, res) => {
  try {
    const { filename } = req.params;
    const { newFilename } = req.body;
    const username = req.authUser || '';

    if (!newFilename) {
      return res.status(400).json({ error: 'New filename is required' });
    }

    // Validate new filename (no path traversal, valid characters)
    if (newFilename.includes('/') || newFilename.includes('\\') || newFilename.includes('..')) {
      return res.status(400).json({ error: 'Invalid filename' });
    }

    const userDataDir = getUserDataDir(username);
    const oldPath = path.join(userDataDir, filename);
    const newPath = path.join(userDataDir, newFilename);

    // Prevent path traversal
    if (!oldPath.startsWith(userDataDir) || !newPath.startsWith(userDataDir)) {
      return res.status(403).json({ error: 'Access denied' });
    }

    if (!fs.existsSync(oldPath)) {
      return res.status(404).json({ error: 'File not found' });
    }

    if (fs.existsSync(newPath)) {
      return res.status(409).json({ error: 'A file with that name already exists' });
    }

    fs.renameSync(oldPath, newPath);
    res.json({ 
      success: true, 
      message: 'File renamed',
      oldFilename: filename,
      newFilename: newFilename
    });
  } catch (error) {
    console.error('Error renaming file:', error);
    res.status(500).json({ 
      error: 'Failed to rename file',
      message: error.message 
    });
  }
});

/**
 * API Route: Delete file
 * DELETE /api/files/:filename
 */
router.delete('/api/files/:filename', (req, res) => {
  try {
    const { filename } = req.params;
    const username = req.authUser || '';
    const userDataDir = getUserDataDir(username);
    const filepath = path.join(userDataDir, filename);

    // Prevent path traversal
    if (!filepath.startsWith(userDataDir)) {
      return res.status(403).json({ error: 'Access denied' });
    }

    if (!fs.existsSync(filepath)) {
      return res.status(404).json({ error: 'File not found' });
    }

    fs.unlinkSync(filepath);
    res.json({ success: true, message: 'File deleted' });
  } catch (error) {
    console.error('Error deleting file:', error);
    res.status(500).json({ 
      error: 'Failed to delete file',
      message: error.message 
    });
  }
});

// ============================================
// DuckDB API Routes
// ============================================

/**
 * API Route: Execute DuckDB SQL query
 * POST /api/duckdb/query
 * Body: { sql: string }
 */
router.post('/api/duckdb/query', async (req, res) => {
  try {
    const { sql } = req.body;
    const username = req.authUser || '';

    if (!sql) {
      return res.status(400).json({ error: 'SQL query is required' });
    }

    const duckdb = getDuckDBService(username);
    if (!duckdb.initialized) {
      await duckdb.init();
    }

    const startTime = Date.now();
    const result = await duckdb.query(sql);
    const executionTime = Date.now() - startTime;

    res.json({
      success: true,
      columns: result.columns,
      rows: result.rows,
      rowCount: result.rowCount,
      executionTime
    });
  } catch (error) {
    console.error('DuckDB query error:', error);
    res.status(500).json({
      error: 'Query execution failed',
      message: error.message
    });
  }
});

/**
 * API Route: Load CSV into DuckDB table
 * POST /api/duckdb/load
 * Body: { path: string, tableName?: string }
 * path can be a local filename (from data dir) or S3 URL
 */
router.post('/api/duckdb/load', async (req, res) => {
  try {
    const { path: csvPath, tableName } = req.body;
    const username = req.authUser || '';

    if (!csvPath) {
      return res.status(400).json({ error: 'CSV path is required' });
    }

    const duckdb = getDuckDBService(username);
    if (!duckdb.initialized) {
      await duckdb.init();
    }

    // Determine if it's a local file or remote URL
    let fullPath = csvPath;
    if (!csvPath.startsWith('s3://') && !csvPath.startsWith('http://') && !csvPath.startsWith('https://')) {
      // Local file - resolve relative to user's data directory
      const userDataDir = getUserDataDir(username);
      fullPath = path.join(userDataDir, csvPath);
      
      // Prevent path traversal
      if (!fullPath.startsWith(userDataDir)) {
        return res.status(403).json({ error: 'Access denied' });
      }
      
      if (!fs.existsSync(fullPath)) {
        return res.status(404).json({ error: 'File not found' });
      }
    }

    // Generate table name if not provided
    const name = tableName || path.basename(csvPath, '.csv').replace(/[^a-zA-Z0-9_]/g, '_');

    const result = await duckdb.loadCsv(fullPath, name);

    res.json({
      success: true,
      ...result
    });
  } catch (error) {
    console.error('DuckDB load error:', error);
    res.status(500).json({
      error: 'Failed to load CSV',
      message: error.message
    });
  }
});

/**
 * API Route: List DuckDB tables
 * GET /api/duckdb/tables
 */
router.get('/api/duckdb/tables', async (req, res) => {
  try {
    const username = req.authUser || '';
    const duckdb = getDuckDBService(username);
    if (!duckdb.initialized) {
      await duckdb.init();
    }
    const tables = await duckdb.listTables();
    res.json({ success: true, tables });
  } catch (error) {
    console.error('DuckDB list tables error:', error);
    res.status(500).json({
      error: 'Failed to list tables',
      message: error.message
    });
  }
});

/**
 * API Route: Get table schema
 * GET /api/duckdb/tables/:tableName
 */
router.get('/api/duckdb/tables/:tableName', async (req, res) => {
  try {
    const { tableName } = req.params;
    const username = req.authUser || '';
    const duckdb = getDuckDBService(username);
    if (!duckdb.initialized) {
      await duckdb.init();
    }
    const schema = await duckdb.describeTable(tableName);
    res.json({ success: true, tableName, schema });
  } catch (error) {
    console.error('DuckDB describe table error:', error);
    res.status(500).json({
      error: 'Failed to describe table',
      message: error.message
    });
  }
});

/**
 * API Route: Drop a DuckDB table
 * DELETE /api/duckdb/tables/:tableName
 */
router.delete('/api/duckdb/tables/:tableName', async (req, res) => {
  try {
    const { tableName } = req.params;
    const username = req.authUser || '';
    const duckdb = getDuckDBService(username);
    if (!duckdb.initialized) {
      await duckdb.init();
    }
    await duckdb.dropTable(tableName);
    res.json({ success: true, message: `Table ${tableName} dropped` });
  } catch (error) {
    console.error('DuckDB drop table error:', error);
    res.status(500).json({
      error: 'Failed to drop table',
      message: error.message
    });
  }
});

/**
 * API Route: Configure S3 credentials
 * POST /api/duckdb/s3-config
 * Body: { accessKeyId, secretAccessKey, region?, endpoint?, sessionToken? }
 */
router.post('/api/duckdb/s3-config', async (req, res) => {
  try {
    const config = req.body;
    const username = req.authUser || '';
    const duckdb = getDuckDBService(username);
    if (!duckdb.initialized) {
      await duckdb.init();
    }
    await duckdb.configureS3(config);
    res.json({ success: true, message: 'S3 configuration updated' });
  } catch (error) {
    console.error('DuckDB S3 config error:', error);
    res.status(500).json({
      error: 'Failed to configure S3',
      message: error.message
    });
  }
});

/**
 * API Route: Get DuckDB status
 * GET /api/duckdb/status
 */
router.get('/api/duckdb/status', async (req, res) => {
  try {
    const username = req.authUser || '';
    const duckdb = getDuckDBService(username);
    if (!duckdb.initialized) {
      await duckdb.init();
    }
    const tables = await duckdb.listTables();
    res.json({
      success: true,
      initialized: duckdb.initialized,
      dbPath: duckdb.dbPath,
      tableCount: tables.length
    });
  } catch (error) {
    const username = req.authUser || '';
    const duckdb = getDuckDBService(username);
    res.json({
      success: false,
      initialized: duckdb.initialized,
      error: error.message
    });
  }
});

// Serve static files through the router
router.use(express.static(path.join(__dirname, 'public')));

// Mount the router at the base path
app.use(BASE_PATH, router);

// Start server
app.listen(PORT, () => {
  console.log(`ES2Tabular server running on http://localhost:${PORT}${BASE_PATH}`);
  console.log(`Kibana: ${kibanaClient.baseUrl}`);
  console.log(`Base data directory: ${DATA_DIR}`);
  console.log(`Per-user data directories: ${DATA_DIR}/<username>/`);
  console.log(`Per-user DuckDB databases: ${DATA_DIR}/<username>/es2tabular.duckdb`);
  console.log(`Auth: ${AUTH_REQUIRED ? `enabled (domain: @${AUTH_ALLOWED_DOMAIN})` : 'disabled'}`);
});
