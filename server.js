import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { KibanaClient } from './lib/kibana-client.js';
import { esToTable, tableToCSV } from './index.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.static('public'));

// Create data directory if it doesn't exist
const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

// Initialize Kibana client
const kibanaClient = new KibanaClient();

/**
 * API Route: Execute Elasticsearch query
 * POST /api/query
 * Body: { index: string, query: object }
 */
app.post('/api/query', async (req, res) => {
  try {
    const { index, query } = req.body;

    if (!index) {
      return res.status(400).json({ error: 'Index is required' });
    }

    if (!query) {
      return res.status(400).json({ error: 'Query is required' });
    }

    console.log(`Executing query on index: ${index}`);
    
    // Execute query via Kibana
    const esResponse = await kibanaClient.search(index, query);

    // Generate unique filename
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const filename = `query-${timestamp}.json`;
    const filepath = path.join(DATA_DIR, filename);

    // Save JSON response locally
    fs.writeFileSync(filepath, JSON.stringify(esResponse, null, 2), 'utf8');

    // Check if response has aggregations
    const hasAggregations = esResponse.aggregations && Object.keys(esResponse.aggregations).length > 0;
    
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
app.post('/api/convert', async (req, res) => {
  try {
    const { filename, aggregationName } = req.body;

    if (!filename) {
      return res.status(400).json({ error: 'Filename is required' });
    }

    const filepath = path.join(DATA_DIR, filename);
    
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

    // Save CSV file
    const csvFilename = filename.replace('.json', '.csv');
    const csvFilepath = path.join(DATA_DIR, csvFilename);
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
app.get('/api/files', (req, res) => {
  try {
    const files = fs.readdirSync(DATA_DIR)
      .map(filename => {
        const filepath = path.join(DATA_DIR, filename);
        const stats = fs.statSync(filepath);
        return {
          filename,
          size: stats.size,
          created: stats.birthtime,
          modified: stats.mtime,
          url: `/api/files/${filename}`,
        };
      })
      .sort((a, b) => b.created - a.created);

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
app.get('/api/files/:filename', (req, res) => {
  try {
    const { filename } = req.params;
    const filepath = path.join(DATA_DIR, filename);

    if (!fs.existsSync(filepath)) {
      return res.status(404).json({ error: 'File not found' });
    }

    // Set appropriate content type
    const ext = path.extname(filename).toLowerCase();
    const contentType = ext === '.csv' ? 'text/csv' : 'application/json';
    
    res.setHeader('Content-Type', contentType);
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    
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
 * API Route: Delete file
 * DELETE /api/files/:filename
 */
app.delete('/api/files/:filename', (req, res) => {
  try {
    const { filename } = req.params;
    const filepath = path.join(DATA_DIR, filename);

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

// Start server
app.listen(PORT, () => {
  console.log(`ES2Tabular server running on http://localhost:${PORT}`);
  console.log(`Kibana: ${kibanaClient.baseUrl}`);
  console.log(`Data directory: ${DATA_DIR}`);
});
