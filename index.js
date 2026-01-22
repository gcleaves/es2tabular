import fs from 'fs';

/**
 * Converts Elasticsearch aggregation output to tabular format
 * @param {Object} esOutput - The Elasticsearch aggregation response
 * @param {Object} options - Configuration options
 * @returns {Array<Object>} Array of row objects with column names as keys
 */
export function esToTable(esOutput, options = {}) {
  const { aggregations } = esOutput;
  
  if (!aggregations) {
    throw new Error('No aggregations found in Elasticsearch output');
  }

  // Find the first aggregation (or use specified aggregation name)
  const aggName = options.aggregationName || Object.keys(aggregations)[0];
  const aggregation = aggregations[aggName];

  if (!aggregation) {
    throw new Error(`Aggregation "${aggName}" not found`);
  }

  // Pass the top-level aggregation name and set filter column name
  const processOptions = {
    ...options,
    topLevelAggregationName: aggName,
    filterColumnName: options.filterColumnName || aggName
  };

  return processAggregation(aggregation, processOptions);
}

/**
 * Recursively processes an aggregation structure
 * @param {Object} agg - The aggregation object
 * @param {Object} options - Options
 * @param {Array} path - Array of {column, value} objects representing the path
 */
function processAggregation(agg, options = {}, path = []) {
  const results = [];

  // Handle filters aggregation (buckets object)
  if (agg.buckets && typeof agg.buckets === 'object' && !Array.isArray(agg.buckets)) {
    // This is a filters aggregation
    // For filters, we use the current aggregation name if available (for nested filters),
    // otherwise fall back to filterColumnName (top-level) or a generic 'filter'
    const bucketNames = Object.keys(agg.buckets);
    
    for (const bucketName of bucketNames) {
      const bucket = agg.buckets[bucketName];
      // For filters aggregation, use a descriptive column name
      // Prefer currentAggregationName for nested filters aggregations
      const columnName = options.currentAggregationName || options.filterColumnName || 'filter';
      const newPath = [...path, { column: columnName, value: bucketName }];
      
      // Check if this bucket has nested aggregations
      const nestedAggs = findNestedAggregations(bucket);
      
      if (nestedAggs.length > 0) {
        // Process nested aggregations, passing the aggregation name
        for (const { name, aggregation } of nestedAggs) {
          const nestedOptions = { ...options, currentAggregationName: name };
          const nestedResults = processAggregation(aggregation, nestedOptions, newPath);
          results.push(...nestedResults);
        }
      } else {
        // Leaf bucket - create a row
        const row = createRowFromBucket(bucket, newPath);
        results.push(row);
      }
    }
  }
  // Handle terms aggregation (buckets array)
  else if (agg.buckets && Array.isArray(agg.buckets)) {
    // This is a terms aggregation
    // The aggregation name should be passed from parent, but if not, we'll use a default
    const aggregationName = options.currentAggregationName || options.topLevelAggregationName || options.aggregationName || 'aggregation';
    
    for (const bucket of agg.buckets) {
      // Include the bucket's key in the path with the aggregation name as column
      // Prefer key_as_string over key if both exist
      const bucketKey = getBucketKey(bucket);
      const newPath = bucketKey !== undefined 
        ? [...path, { column: aggregationName, value: bucketKey }]
        : [...path];
      
      // Check if this bucket has nested aggregations
      const nestedAggs = findNestedAggregations(bucket);
      
      if (nestedAggs.length > 0) {
        // Process nested aggregations, passing the aggregation name as currentAggregationName
        for (const { name, aggregation } of nestedAggs) {
          const nestedOptions = { ...options, currentAggregationName: name };
          const nestedResults = processAggregation(aggregation, nestedOptions, newPath);
          results.push(...nestedResults);
        }
      } else {
        // Leaf bucket - create a row
        const row = createRowFromBucket(bucket, newPath);
        results.push(row);
      }
    }
  }

  return results;
}

/**
 * Gets the preferred key value from a bucket
 * Prefers key_as_string over key if both exist
 * @param {Object} bucket - The bucket object
 * @returns {*} The key value (key_as_string if available, otherwise key)
 */
function getBucketKey(bucket) {
  if (bucket.key_as_string !== undefined) {
    return bucket.key_as_string;
  }
  return bucket.key;
}

/**
 * Finds nested aggregations in a bucket
 * Returns array of {name, aggregation} objects
 */
function findNestedAggregations(bucket) {
  const nestedAggs = [];
  
  // Look for nested aggregation structures
  for (const key in bucket) {
    if (key !== 'key' && key !== 'key_as_string' && key !== 'doc_count' && key !== 'doc_count_error_upper_bound' && 
        key !== 'sum_other_doc_count' && typeof bucket[key] === 'object' && bucket[key] !== null) {
      // Check if this looks like an aggregation (has buckets property)
      const obj = bucket[key];
      if (obj.buckets !== undefined) {
        nestedAggs.push({ name: key, aggregation: obj });
      }
    }
  }
  
  return nestedAggs;
}

/**
 * Creates a row object from a bucket
 * @param {Object} bucket - The bucket object
 * @param {Array} path - Array of {column, value} objects
 */
function createRowFromBucket(bucket, path) {
  const row = {};
  
  // Add path elements as columns using the column names from path
  for (const pathItem of path) {
    if (pathItem && typeof pathItem === 'object' && pathItem.column && pathItem.value !== undefined) {
      row[pathItem.column] = pathItem.value;
    } else if (typeof pathItem === 'string') {
      // Fallback for old format (shouldn't happen, but handle gracefully)
      row[`level_${path.length}`] = pathItem;
    }
  }
  
  // Add bucket key if present and not already in path
  // (for leaf buckets in terms aggregations, the key is already in the path)
  // Prefer key_as_string over key if both exist
  const bucketKey = getBucketKey(bucket);
  if (bucketKey !== undefined) {
    const lastPathItem = path.length > 0 ? path[path.length - 1] : null;
    if (!lastPathItem || lastPathItem.value !== bucketKey) {
      // Key not in path, add it with a generic column name
      row['key'] = bucketKey;
    }
  }
  
  // Add doc_count if present
  if (bucket.doc_count !== undefined) {
    row['doc_count'] = bucket.doc_count;
  }
  
  return row;
}

/**
 * Converts table data to CSV format
 */
export function tableToCSV(table, options = {}) {
  if (table.length === 0) {
    return '';
  }

  const headers = Object.keys(table[0]);
  const delimiter = options.delimiter || ',';
  const includeHeaders = options.includeHeaders !== false;

  let csv = '';

  // Add headers
  if (includeHeaders) {
    csv += headers.map(h => escapeCSV(h)).join(delimiter) + '\n';
  }

  // Add rows
  for (const row of table) {
    csv += headers.map(h => escapeCSV(row[h] !== undefined && row[h] !== null ? row[h] : '')).join(delimiter) + '\n';
  }

  return csv;
}

/**
 * Escapes a value for CSV
 */
function escapeCSV(value) {
  if (value === null || value === undefined) {
    return '';
  }
  
  const str = String(value);
  
  // If contains comma, newline, or quote, wrap in quotes and escape quotes
  if (str.includes(',') || str.includes('\n') || str.includes('"')) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  
  return str;
}

/**
 * Main function to convert ES output file to CSV
 */
export function convertFile(inputPath, outputPath, options = {}) {
  const esOutput = JSON.parse(fs.readFileSync(inputPath, 'utf8'));
  const table = esToTable(esOutput, options);
  const csv = tableToCSV(table, options);
  
  if (outputPath) {
    fs.writeFileSync(outputPath, csv, 'utf8');
    console.log(`Converted ${inputPath} to ${outputPath}`);
    console.log(`Generated ${table.length} rows`);
  }
  
  return { table, csv };
}
