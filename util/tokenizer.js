import fs from 'fs';
import _ from 'lodash';
import nodeWARC from 'node-warc';
import path from 'path';
import Document from './database/Document';

const {
  WET_FILES_FOLDER, TEMP_FILES_FOLDER, PAGE_TABLE, WET_FILES_PER_BATCH,
} = process.env;
const batchSize = parseInt(WET_FILES_PER_BATCH, 10);
const lexicon = {};
let termIdCounter = 0;
const tempFilePath = path.join(process.cwd(), TEMP_FILES_FOLDER);
const pageTablePath = path.join(process.cwd(), PAGE_TABLE);
let docIdCounter = 0;
let tokenizingComplete = null;
let tokenizingError = null;
let buffer = null;
let pageTable = null;
/**
 *
 * @param {String} term: individual term vetted to see if it qualifies for tokenzing
 * @description: this functiontakes an individual term and check if it is completley
 * comprised of ASCII characters. hence it allows all latin alphabet using languages
 * it also adds other restrictions to the term such that term length should be at between 3 and 10
 * and term should contain atleast 3 unique letters.
 * If term qualifies this function makes an entry in the lexicon and returns the termId
 */
const vetWord = (term) => {
  const results = term.match(/^[a-zA-Z]*$/);
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
/**
 *
 * @param {String} file: file name to parsed and tokenized
 * @param {Number} index: index of the file
 * @param {Number} batch: number of the bacth that the file belongs to.
 * @description: this functions reads compressed WET_file in compressed format and
 *  passes it to the WARCParser, which genrates an un-compressed stream of records
 *  then each reacord is split into terms and term-frequency for the given record is calculated
 *  the <termID, docId, freq> posting is then written to temfile in binary format using streams
 */

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
      const termBuffer = Buffer.alloc(4);
      termBuffer.writeUInt32BE(term);
      const docIdBuffer = Buffer.alloc(4);
      docIdBuffer.writeUInt32BE(docIdCounter);
      const freqBuffer = Buffer.alloc(2);
      freqBuffer.writeUInt16BE(frequency[term]);
      buffer.write(Buffer.concat([termBuffer, docIdBuffer, freqBuffer]));
    });
    const newDocument = new Document({ docId: docIdCounter, content });
    newDocument.save();
    pageTable.write(`${docIdCounter} ${warcHeader['WARC-Target-URI']} ${terms.length}\n`);
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

/**
 *
 * @param {Array} files: WET_files array
 * @param {Number} index: index of WET file to be tokenized
 * @param {Number} batch: the number of current batch being processed
 * @description: this function tokenizes each WET_file in a batch, and upon completion takes care
 * of cleaning operations such as closing the streams and idicating caller that all files have been procesed
 */
const parseFilesRec = (files, index, batch) => {
  const file = files[index];
  if (!file) {
    buffer.end();
    pageTable.end();
    buffer = null;
    pageTable = null;
    tokenizingComplete(lexicon);
    return;
  }
  parseFile(file, index, batch).then(() => {
    parseFilesRec(files, index + 1, batch);
  });
};
/**
 * @param {Number}: indicates what batch we are currently processing
 * @description: this function reads WET files in compressed format from the WET_FILES_FOLDER
 *  and calls tokenizer function on them.
 *  It creates a page table entry which maps a urlto its docId
 *  It creates a temp file for each batch where unsorted postings are stored
 */
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
    tokenizingComplete = resolve;
    tokenizingError = reject;
  });
};

export default { tokenize };
