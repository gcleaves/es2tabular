import { esToTable, tableToCSV, convertFile } from './index.js';
import fs from 'fs';

// Test with the example output
console.log('Testing es2tabular with example/output01.json\n');

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
