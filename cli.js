"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var fs = require("fs");
var csv = require("csv-parser");
var stream_1 = require("stream");
var csv_stringify_1 = require("csv-stringify");
var fastcsv = require("fast-csv");
function rotateTable(row) {
    var id = row.id;
    var isValid = true;
    try {
        var jsonArray = JSON.parse(row.json);
        var rotatedArray = rotateArray(jsonArray);
        var rotatedJson = JSON.stringify(rotatedArray);
        return [{ id: id, json: rotatedJson, is_valid: true }];
    }
    catch (e) {
        isValid = false;
    }
    return [{ id: id, json: "[]", is_valid: isValid }];
}
function rotateArray(arr) {
    var newArr = [];
    var numRows = arr.length;
    var numCols = Math.max.apply(Math, arr.map(function (row) { return row.length; }));
    for (var j = 0; j < numCols; j++) {
        var newRow = [];
        for (var i = 0; i < numRows; i++) {
            newRow.push(arr[i][j]);
        }
        newArr.push(newRow);
    }
    return newArr;
}
var inputFilePath = process.argv[2];
var outputFilePath = process.argv[3];
var inputStream = fs.createReadStream(inputFilePath).pipe(csv({ headers: true }));
var transformer = new stream_1.Transform({ objectMode: true, transform: function (row, _, done) { return done(null, rotateTable(row)); } });
var csvStringifier = (0, csv_stringify_1.stringify)({ header: true });
var outputStream = fastcsv.format({ headers: true }).pipe(process.stdout);
inputStream.pipe(transformer).pipe(csvStringifier).pipe(outputStream);
