import fs from 'fs';

/**
 * Converts Elasticsearch output to tabular format.
 * Supports (1) aggregation responses and (2) non-aggregated search hits.
 * For hits: columns are _id plus each _source field; non-scalar fields are JSON strings.
 *
 * @param {Object} esOutput - The Elasticsearch response (aggregations or hits)
 * @param {Object} options - Configuration options
 * @returns {Array<Object>} Array of row objects with column names as keys
 */
export function esToTable(esOutput, options = {}) {
  const { aggregations } = esOutput;
  const hits = esOutput.hits?.hits;

  if (aggregations && Object.keys(aggregations).length > 0) {
    const aggName = options.aggregationName || Object.keys(aggregations)[0];
    const aggregation = aggregations[aggName];
    if (!aggregation) {
      throw new Error(`Aggregation "${aggName}" not found`);
    }
    const processOptions = {
      ...options,
      topLevelAggregationName: aggName,
      filterColumnName: options.filterColumnName || aggName
    };
    return processAggregation(aggregation, processOptions);
  }

  if (Array.isArray(hits) && hits.length > 0) {
    return hitsToTable(hits);
  }

  throw new Error('No aggregations or hits found in Elasticsearch output');
}

/**
 * True if value is scalar (null, boolean, number, string).
 */
function isScalar(v) {
  return v === null || typeof v !== 'object';
}

/**
 * Convert a value to a table cell: scalars as-is, non-scalars as JSON string.
 */
function valueToCell(v) {
  if (isScalar(v)) return v;
  return JSON.stringify(v);
}

/**
 * Build tabular rows from raw search hits.
 * Columns: _id, then each _source key (first-seen order). Non-scalar fields as JSON.
 *
 * @param {Array<Object>} hits - hits.hits from ES response
 * @returns {Array<Object>} Array of row objects
 */
function hitsToTable(hits) {
  const colList = ['_id'];
  const seen = new Set(['_id']);
  for (const h of hits) {
    const src = h._source || {};
    for (const k of Object.keys(src)) {
      if (!seen.has(k)) {
        seen.add(k);
        colList.push(k);
      }
    }
  }

  const rows = hits.map((hit) => {
    const row = {};
    row._id = hit._id;
    const src = hit._source || {};
    for (const c of colList) {
      if (c === '_id') continue;
      const v = src[c];
      row[c] = v === undefined ? '' : valueToCell(v);
    }
    return row;
  });

  return rows;
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
    
    // Add "_other_" row if there are documents not captured in top N buckets
    if (agg.sum_other_doc_count > 0) {
      const otherPath = [...path, { column: aggregationName, value: '_other_' }];
      const otherRow = createRowFromBucket({ doc_count: agg.sum_other_doc_count }, otherPath);
      results.push(otherRow);
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
 * Finds metric aggregations in a bucket (filter aggs, value_count, sum, avg, etc.)
 * These are objects with doc_count or value but no buckets property.
 * Returns array of {name, value} objects.
 */
function findMetricAggregations(bucket) {
  const metrics = [];
  const skipKeys = new Set(['key', 'key_as_string', 'doc_count', 'doc_count_error_upper_bound', 'sum_other_doc_count']);
  
  for (const key in bucket) {
    if (skipKeys.has(key)) continue;
    const obj = bucket[key];
    if (obj && typeof obj === 'object' && obj !== null && !Array.isArray(obj)) {
      // It's an object - check if it's a metric aggregation (no buckets)
      if (obj.buckets === undefined) {
        // This is a metric aggregation (filter, value_count, sum, avg, cardinality, etc.)
        // Extract the most relevant value
        if (obj.doc_count !== undefined) {
          // Filter aggregation
          metrics.push({ name: key, value: obj.doc_count });
        } else if (obj.value !== undefined) {
          // Value metric (sum, avg, min, max, cardinality, value_count, etc.)
          metrics.push({ name: key, value: obj.value });
        }
      }
    }
  }
  
  return metrics;
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
  
  // Add metric aggregations (filter aggs, sum, avg, etc.) as columns
  const metrics = findMetricAggregations(bucket);
  for (const { name, value } of metrics) {
    row[name] = value;
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
