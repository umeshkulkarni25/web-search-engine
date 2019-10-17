import fs from 'fs';
import path from 'path';
import nodeWARC from 'node-warc';
import _ from 'lodash';

const { WET_FILES_FOLDER, PAGE_TABLE } = process.env;
const pageTablePath = path.join(process.cwd(), PAGE_TABLE);
const pageTable = fs.createWriteStream(pageTablePath);

let docIdCounter = 0;
let fetchComplete = null;
let fetchError = null;

const lexicon = {};
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
  return matchedWord;
};
const parseFile = (files, index) => {
  const file = files[index];
  if (!file) {
    fetchComplete();
    return;
  }
  const WARCParser = new nodeWARC.AutoWARCParser(file);
  WARCParser.on('record', (record) => {
    const { warcHeader } = record;
    if (warcHeader['WARC-Type'] !== 'conversion') {
      return;
    }
    const content = record.content.toString();
    const terms = content.trim().split(/\s+/);
    const vettedTerms = _.filter(_.map(terms, vetWord), Boolean);
    _.each(vettedTerms, (term) => { lexicon[term] = true; });
    pageTable.write(`${docIdCounter} ${warcHeader['WARC-Target-URI']}\n`);
    docIdCounter += 1;
  });
  WARCParser.on('done', () => {
    console.log('done with a warcfile:', index, 'time:', new Date());
    console.log('total-docs:', docIdCounter);
    console.log('lexicon-size:', Object.keys(lexicon).length);
    parseFile(files, index + 1);
  });
  WARCParser.start();
};
const build = () => {
  fs.readdir(WET_FILES_FOLDER, (dirReadErr, fileNames) => {
    if (dirReadErr) {
      console.log(dirReadErr);
      return;
    }
    console.log('startTime', new Date());
    const filePaths = _.map(fileNames, (fileName) => path.join(WET_FILES_FOLDER, fileName));

    parseFile(filePaths, 0, 0);
  });
  return new Promise((resolve, reject) => {
    fetchComplete = resolve;
    fetchError = reject;
  });
};

export default { build };
