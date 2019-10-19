import { exec } from 'child_process';
import fs from 'fs';
import path from 'path';
import fsExtra from 'fs-extra';
import _ from 'lodash';

const IC = require('compress-integers');

const { TEMP_FILES_FOLDER, BOOKMARKED_TEMP_FILES_FOLDER } = process.env;
let recordsGroupByTermId = [];
let docIdsGroupedByTermId = [];
let frequencyGroupedByTermId = [];
let docIds = [];
let freqs = [];
let currentTerm = null;
const readFileRec = (fd, content, position, bookmarkedTemp, resolve) => {
  fs.read(fd, content, 0, content.length, position, (err, bytesRead, content) => {
    if (bytesRead === 0) {
      console.log('read file', new Date());
      bookmarkedTemp.end();
      currentTerm = null;
      resolve();
      return;
    }
    if (!currentTerm) {
      currentTerm = content.readUInt32BE(0);
    }
    for (let j = 0; j < bytesRead; j += 10) {
      const record = content.slice(j, j + 10);
      const termId = record.readUInt32BE(0);
      const docId = record.readUInt32BE(4);
      const freq = record.readUInt16BE(8);
      if (termId === currentTerm) {
        recordsGroupByTermId.push(record.slice(4));
        docIdsGroupedByTermId.push(record.slice(4, 8));
        frequencyGroupedByTermId.push(record.slice(8, 10));
        docIds.push(docId);
        freqs.push(freq);
      } else {
        const termBuffer = Buffer.allocUnsafe(4);
        termBuffer.writeUInt32BE(currentTerm);
        const docIdBuffer = Buffer.concat(docIdsGroupedByTermId);
        const frequencyBuffer = Buffer.concat(frequencyGroupedByTermId);
        const listBuffer = Buffer.concat([docIdBuffer, frequencyBuffer]);
        const lengthBuffer = Buffer.allocUnsafe(4);
        IC.compress(docIds, { order: 1 });
        lengthBuffer.writeUInt32BE(listBuffer.length);
        bookmarkedTemp.write(Buffer.concat([termBuffer, lengthBuffer, listBuffer]));
        currentTerm = termId;
        recordsGroupByTermId = [record.slice(4)];
        docIdsGroupedByTermId = [record.slice(4, 8)];
        frequencyGroupedByTermId = [record.slice(8, 10)];
        docIds = [docId];
        freqs = [freq];
      }
    }
    readFileRec(fd, content, position + bytesRead, bookmarkedTemp, resolve);
  });
};


const sort = (files, index) => new Promise((resolve, reject) => {
  const file = files[index];
  if (!file) {
    console.log('temp file sorting done');
    resolve();
    return;
  }
  exec(`bsort -k 4 -r 10 ${path.join(process.cwd(), TEMP_FILES_FOLDER, file)}`, async (err) => {
    if (err) {
      console.log(`error occured in sorting ${file}`);
    }
    console.log(`bookmarking ${file}`, new Date());
    const fd = fs.openSync(path.join(process.cwd(), TEMP_FILES_FOLDER, file));
    const bookmarkedTemp = fs.createWriteStream(path.join(process.cwd(), BOOKMARKED_TEMP_FILES_FOLDER, `${file}_bookmarked`));
    const bufferSize = 1000 * 10000;
    const content = Buffer.alloc(bufferSize);
    const fileSorted = new Promise((resolve, reject) => {
      readFileRec(fd, content, 0, bookmarkedTemp, resolve);
    });
    fileSorted.then(() => {
      sort(files, index + 1);
    });
  });
});

const start = () => {
  fs.readdir(TEMP_FILES_FOLDER, async (dirReadErr, fileNames) => {
    if (dirReadErr) {
      console.log(dirReadErr);
      return;
    }
    if (fs.existsSync(BOOKMARKED_TEMP_FILES_FOLDER)) {
      fsExtra.removeSync(BOOKMARKED_TEMP_FILES_FOLDER);
    }

    fsExtra.mkdirSync(BOOKMARKED_TEMP_FILES_FOLDER);
    await sort(fileNames, 0);
  });
};

export default { start };
