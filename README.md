# ES2Tabular

A Node.js tool to convert Elasticsearch aggregation query output into tabular format (CSV). Includes both a web interface and a programmatic API.

## Features

- Automatically detects aggregation structure
- Handles nested aggregations (filters, terms, etc.)
- Converts to CSV format
- Preserves all data from aggregation buckets
- Web interface with Alpine.js for easy query execution
- RESTful API for programmatic access
- Executes queries via Kibana Console Proxy API

## Installation

```bash
npm install
```

## Web Interface

Start the server:

```bash
npm start
```

Then open your browser to `http://localhost:3000`

The web interface allows you to:
- Enter Elasticsearch index patterns
- Execute aggregation queries
- View query results
- Convert results to CSV format
- Download CSV files
- Manage stored query results

### Environment Variables

Configure Kibana connection via environment variables. You can either:

**Option 1: Use a `.env` file (recommended)**

Create a `.env` file in the project root:

```bash
cp env.example .env
```

Then edit `.env` with your configuration:

```bash
KIBANA_HOST=localhost          # Kibana host (default: localhost)
KIBANA_PORT=5601               # Kibana port (default: 5601)
KIBANA_PROTOCOL=http           # Protocol (default: http)
KIBANA_USERNAME=your_username  # Optional: Kibana username
KIBANA_PASSWORD=your_password  # Optional: Kibana password
KIBANA_AUTH_TOKEN=token        # Optional: Base64 auth token or Bearer token
PORT=3000                      # Server port (default: 3000)
```

**Option 2: Set environment variables directly**

```bash
export KIBANA_HOST=localhost
export KIBANA_PORT=5601
export KIBANA_USERNAME=your_username
export KIBANA_PASSWORD=your_password
npm start
```

## Usage

### Basic Usage

```javascript
import { esToTable, tableToCSV, convertFile } from './index.js';
import fs from 'fs';

// Read ES output
const esOutput = JSON.parse(fs.readFileSync('example_output/output01.json', 'utf8'));

// Convert to table
const table = esToTable(esOutput);

// Convert to CSV
const csv = tableToCSV(table);
console.log(csv);

// Or convert directly from file
convertFile('example_output/output01.json', 'output.csv');
```

### API

#### `esToTable(esOutput, options)`

Converts Elasticsearch aggregation output to an array of row objects.

**Parameters:**
- `esOutput` (Object): The Elasticsearch aggregation response
- `options` (Object, optional):
  - `aggregationName` (String): Name of the aggregation to process (defaults to first aggregation)

**Returns:** Array of objects, where each object represents a row with column names as keys.

#### `tableToCSV(table, options)`

Converts table data to CSV format.

**Parameters:**
- `table` (Array): Array of row objects
- `options` (Object, optional):
  - `delimiter` (String): CSV delimiter (default: ',')
  - `includeHeaders` (Boolean): Include header row (default: true)

**Returns:** CSV string

#### `convertFile(inputPath, outputPath, options)`

Converts an ES output JSON file directly to CSV.

**Parameters:**
- `inputPath` (String): Path to input JSON file
- `outputPath` (String): Path to output CSV file
- `options` (Object, optional): Same as `esToTable` options

**Returns:** Object with `table` and `csv` properties

## Example

For the example output structure:
- Filters aggregation (`uIpAnalysis_breakdown`) with buckets: `with_uIpAnalysis`, `without_uIpAnalysis`
- Nested terms aggregation (`by_client`) grouping by `client_id`

The output will have columns:
- `level_1`: The filter bucket name (with_uIpAnalysis/without_uIpAnalysis)
- `key`: The client_id value
- `doc_count`: The document count for that combination

## REST API

The API is available at `/api/*` endpoints:

### POST `/api/query`

Execute an Elasticsearch query via Kibana.

**Request Body:**
```json
{
  "index": "my-index-*",
  "query": {
    "size": 0,
    "aggs": {
      "my_aggregation": {
        "terms": {
          "field": "field_name",
          "size": 10
        }
      }
    }
  }
}
```

**Response:**
```json
{
  "success": true,
  "filename": "query-2024-01-01T12-00-00-000Z.json",
  "filepath": "/api/files/query-2024-01-01T12-00-00-000Z.json",
  "hasAggregations": true,
  "hits": 0,
  "total": 1000
}
```

### POST `/api/convert`

Convert a stored JSON file to CSV.

**Request Body:**
```json
{
  "filename": "query-2024-01-01T12-00-00-000Z.json",
  "aggregationName": "optional_agg_name"
}
```

**Response:**
```json
{
  "success": true,
  "csvFilename": "query-2024-01-01T12-00-00-000Z.csv",
  "csvFilepath": "/api/files/query-2024-01-01T12-00-00-000Z.csv",
  "rows": 74,
  "columns": ["level_1", "key", "doc_count"]
}
```

### GET `/api/files`

List all stored files.

**Response:**
```json
{
  "files": [
    {
      "filename": "query-2024-01-01T12-00-00-000Z.json",
      "size": 12345,
      "created": "2024-01-01T12:00:00.000Z",
      "modified": "2024-01-01T12:00:00.000Z",
      "url": "/api/files/query-2024-01-01T12-00-00-000Z.json"
    }
  ]
}
```

### GET `/api/files/:filename`

Download a file (JSON or CSV).

### DELETE `/api/files/:filename`

Delete a stored file.

## Programmatic Usage

### Using the Kibana Client

```javascript
import { KibanaClient } from './lib/kibana-client.js';

const client = new KibanaClient({
  host: 'localhost',
  port: 5601,
  username: 'user',
  password: 'pass'
});

const result = await client.search('my-index-*', {
  size: 0,
  aggs: {
    my_agg: {
      terms: { field: 'field_name' }
    }
  }
});
```

## Testing

```bash
npm test
```

This will process `example/output01.json` and generate `example/output01.csv`.
