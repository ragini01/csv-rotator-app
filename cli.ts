import * as fs from 'fs';
import * as csv from 'csv-parser';
import { Transform } from 'stream';
import { stringify } from 'csv-stringify';
import * as fastcsv from 'fast-csv';

interface InputRow {
  id: string;
  json: string;
}

interface OutputRow {
  id: string;
  json: string;
  is_valid: boolean;
}

function rotateTable(row: InputRow): OutputRow[] {
  const id = row.id;
  let isValid = true;

  try {
    const jsonArray = JSON.parse(row.json);
    const rotatedArray = rotateArray(jsonArray);
    const rotatedJson = JSON.stringify(rotatedArray);
    return [{ id, json: rotatedJson, is_valid: true }];
  } catch (e) {
    isValid = false;
  }

  return [{ id, json: "[]", is_valid: isValid }];
}

function rotateArray(arr: any[]): any[] {
  const newArr = [];
  const numRows = arr.length;
  const numCols = Math.max(...arr.map((row) => row.length));

  for (let j = 0; j < numCols; j++) {
    const newRow = [];
    for (let i = 0; i < numRows; i++) {
      newRow.push(arr[i][j]);
    }
    newArr.push(newRow);
  }

  return newArr;
}

const inputFilePath = process.argv[2];
const outputFilePath = process.argv[3];

const inputStream = fs.createReadStream(inputFilePath).pipe(csv({ headers: true }));

const transformer = new Transform({ objectMode: true, transform: (row, _, done) => done(null, rotateTable(row)) });

const csvStringifier = stringify({ header: true });

const outputStream = fastcsv.format({ headers: true }).pipe(process.stdout);

inputStream.pipe(transformer).pipe(csvStringifier).pipe(outputStream);
