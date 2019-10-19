import fs from 'fs';
import _ from 'lodash';
import nodeWARC from 'node-warc';
import path from 'path';
import fsExtra from 'fs-extra';
import { exec } from 'child_process';

const sizeof = require('object-sizeof');

const {
  WET_FILES_FOLDER, TEMP_FILES_FOLDER, PAGE_TABLE, WET_FILES_PER_BATCH,
} = process.env;
const batchSize = parseInt(WET_FILES_PER_BATCH, 10);
const lexicon = {};
let termIdCounter = 0;
const tempFilePath = path.join(process.cwd(), TEMP_FILES_FOLDER);
const pageTablePath = path.join(process.cwd(), PAGE_TABLE);
let docIdCounter = 0;
let fetchComplete = null;
let fetchError = null;
let buffer = null;
let pageTable = null;
const vetWord = (word) => {
  const results = word.match(/^[a-zA-Z]*$/);
  if (!results) {
    return null;
  }
  const matchedWord = results[0].toLowerCase();
  if (matchedWord.length < 3 || matchedWord.length > 10) {
    return null;
  }
  if (!matchedWord.match(/(?:(.)(?<=^(?:(?!\1).)*\1)(?=(?:(?!\1).)*$).*?){3,}/)) {
    return null;
  }
  let termId = lexicon[matchedWord];
  if (!termId) {
    termIdCounter += 1;
    termId = termIdCounter;
    lexicon[matchedWord] = termId;
  }
  return termId;
};

const parseFile = (file, index, batch) => new Promise((resolve, reject) => {
  console.log('starting warc file', batch * batchSize + index, 'time:', new Date());
  const WARCParser = new nodeWARC.AutoWARCParser(file);
  WARCParser.on('record', (record) => {
    const { warcHeader } = record;
    if (warcHeader['WARC-Type'] !== 'conversion') {
      return;
    }
    const content = record.content.toString();
    const terms = content.trim().split(/\s+/);
    const vettedTerms = _.filter(_.map(terms, vetWord), Boolean);
    const termSet = [...(new Set(vettedTerms))];
    const frequency = {};
    _.each(termSet, (word) => { frequency[word] = 0; });
    _.each(vettedTerms, (word) => { frequency[word] += 1; });
    _.each(termSet, (term) => {
      const termBuffer = Buffer.allocUnsafe(4);
      termBuffer.writeUInt32BE(term);
      const docIdBuffer = Buffer.allocUnsafe(4);
      docIdBuffer.writeUInt32BE(docIdCounter);
      const freqBuffer = Buffer.allocUnsafe(2);
      freqBuffer.writeUInt16BE(frequency[term]);
      buffer.write(Buffer.concat([termBuffer, docIdBuffer, freqBuffer]));
    });
    pageTable.write(`${docIdCounter} ${warcHeader['WARC-Target-URI']}\n`);
    docIdCounter += 1;
  });
  WARCParser.on('done', () => {
    console.log('done with a warcfile:', batch * batchSize + index, 'time:', new Date());
    process.nextTick(() => {
      buffer.uncork();
      buffer.cork();
    });
    process.nextTick(() => {
      pageTable.uncork();
      pageTable.cork();
    });
    resolve(WARCParser);
  });
  WARCParser.start();
});

const parseFilesRec = (files, index, batch) => {
  const file = files[index];
  if (!file) {
    buffer.end();
    pageTable.end();
    buffer = null;
    pageTable = null;
    fetchComplete(lexicon);
    return;
  }
  parseFile(file, index, batch).then((WARCParser) => {
    WARCParser = null;
    parseFilesRec(files, index + 1, batch);
  });
};
const tokenize = async (batch) => {
  fs.readdir(WET_FILES_FOLDER, (dirReadErr, fileNames) => {
    if (dirReadErr) {
      console.log(dirReadErr);
      return;
    }
    console.log('startTime', new Date());
    const filePaths = _.map(fileNames, (fileName) => path.join(WET_FILES_FOLDER, fileName));
    pageTable = fs.createWriteStream(pageTablePath, { flags: 'a+' });
    buffer = fs.createWriteStream(path.join(tempFilePath, `batch_${batch.toString()}`));
    buffer.cork();
    pageTable.cork();
    parseFilesRec(filePaths, 0, batch);
  });
  return new Promise((resolve, reject) => {
    fetchComplete = resolve;
    fetchError = reject;
  });
};

export default { tokenize };
