import { esToTable, tableToCSV, convertFile } from './index.js';
import fs from 'fs';

// Test with the example aggregation output
console.log('Testing es2tabular with example/output01.json (aggregations)\n');

const esOutput = JSON.parse(fs.readFileSync('example/output01.json', 'utf8'));

// Convert to table
const table = esToTable(esOutput);

console.log('Table structure:');
console.log('Number of rows:', table.length);
console.log('Columns:', Object.keys(table[0] || {}));
console.log('\nFirst 5 rows:');
console.log(JSON.stringify(table.slice(0, 5), null, 2));

console.log('\n--- CSV Output (first 10 rows) ---');
const csv = tableToCSV(table);
const csvLines = csv.split('\n');
console.log(csvLines.slice(0, 11).join('\n'));

// Test file conversion
console.log('\n--- Converting to CSV file ---');
convertFile('example/output01.json', 'example/output01.csv');

// Test raw (non-aggregated) hits
console.log('\n--- Testing raw hits (example/raw.json) ---');
const raw = JSON.parse(fs.readFileSync('example/raw.json', 'utf8'));
const rawTable = esToTable(raw);
console.log('Rows:', rawTable.length, 'Columns:', Object.keys(rawTable[0] || {}));
console.log('fraud_report sample:', rawTable[0].fraud_report);
convertFile('example/raw.json', 'example/raw.csv');
console.log('Raw hits conversion OK.');

// Test filter aggregations (filter agg without buckets, just doc_count)
console.log('\n--- Testing filter aggregations (example/filter.json) ---');
const filter = JSON.parse(fs.readFileSync('example/filter.json', 'utf8'));
const filterTable = esToTable(filter);
console.log('Rows:', filterTable.length, 'Columns:', Object.keys(filterTable[0] || {}));
console.log('has_static_canvas_history sample:', filterTable[2].has_static_canvas_history);
convertFile('example/filter.json', 'example/filter.csv');
console.log('Filter aggregations conversion OK.');

// Test sum_other_doc_count (_other_ rows)
console.log('\n--- Testing _other_ rows (example/other.json) ---');
const other = JSON.parse(fs.readFileSync('example/other.json', 'utf8'));
const otherTable = esToTable(other);
const otherRows = otherTable.filter(r => r.status === '_other_');
console.log('Rows:', otherTable.length, '_other_ rows:', otherRows.length);
console.log('_other_ doc_count sample:', otherRows[0]?.doc_count);
convertFile('example/other.json', 'example/other.csv');
console.log('_other_ rows conversion OK.');
