import fs from 'fs';
import _ from 'lodash';
import nodeWARC from 'node-warc';
import path from 'path';
import fsExtra from 'fs-extra';

const { WET_FILES_FOLDER, TEMP_FILES_FOLDER } = process.env;
const lexicon = {};
let termIdCounter = 0;
const tempFilePath = path.join(process.cwd(), TEMP_FILES_FOLDER);
let docIdCounter = 0;
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

const parseFile = (files, index, offset) => {
  const file = files[index];

  if (!file) {
    console.log('endTime', new Date());
    return;
  }
  console.log('starting warc file', offset + index);
  const buffer = fs.createWriteStream(path.join(tempFilePath, (offset + index).toString()));
  buffer.cork();
  const WARCParser = new nodeWARC.AutoWARCParser(file);
  WARCParser.on('record', (record) => {
    const content = record.content.toString();
    const words = content.trim().split(/\s+/);
    const vettedWords = _.filter(_.map(words, vetWord), Boolean);
    const wordSet = [...(new Set(vettedWords))];
    const frequency = {};
    _.each(wordSet, (word) => { frequency[word] = 0; });
    _.each(vettedWords, (word) => { frequency[word] += 1; });
    const postings = [];
    _.each(wordSet, (word) => {
      postings.push(`${word} ${docIdCounter} ${frequency[word]}\n`);
    });
    buffer.write(postings.join(''));
    docIdCounter += 1;
  });
  WARCParser.on('done', () => {
    console.log('done with a warcfile:', offset + index, 'time:', new Date());
    console.log('total-docs:', docIdCounter);
    console.log('lexicon-size:', Object.keys(lexicon).length);
    process.nextTick(() => {
      buffer.uncork();
      buffer.end();
    });
    parseFile(files, index + 1, offset);
  });
  WARCParser.start();
};

const parse = () => {
  fs.readdir(WET_FILES_FOLDER, (dirReadErr, fileNames) => {
    if (dirReadErr) {
      console.log(dirReadErr);
      return;
    }
    console.log('startTime', new Date());
    const filePaths = _.map(fileNames, (fileName) => path.join(WET_FILES_FOLDER, fileName));
    fsExtra.removeSync(tempFilePath);
    fsExtra.mkdirSync(tempFilePath);
    parseFile(filePaths, 0, 0);
  });
};

export default { parse };
