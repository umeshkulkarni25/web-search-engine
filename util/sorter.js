import { exec } from 'child_process';
import fs from 'fs';
import path from 'path';
import fsExtra from 'fs-extra';
import _ from 'lodash';

const { TEMP_FILES_FOLDER, BOOKMARKED_TEMP_FILES_FOLDER } = process.env;
let currentTerm = null; // current term by which the groupBy is being performed
let taskComplete = null;
let taskErrored = null;

/**
 *
 * @param {Object} fd: file descriptor of the file being bookmarked
 * @param {Object} content: buffer for reading chucks of the file at a time
 * @param {Number} position: postinion pointer in the file from where next buffer read is to be performed
 * @param {*} bookmarkedTemp: bookmark file stream
 * @param {*} resolve: Promise to indicate a file has been bookmarked completely
 * @description: this function reads chuks of sorted binary file at a time and bookmarks them
 *  and calls itself recursively to read and book mark next chunk.
 *  bookmarking is essenitally a group-by operation by termIds
 *  var-Byte compreesing is applied to docId block as well as frequecy blocks
 */
const readFileRec = (fd, content, position, bookmarkedTemp, resolve) => {
  let postings = [];
  fs.read(fd, content, 0, content.length, position, (err, bytesRead, content) => {
    if (bytesRead === 0) {
      bookmarkedTemp.end();
      currentTerm = null;
      resolve();
      return;
    }
    if (!currentTerm) {
      currentTerm = content.readUInt32BE(0);
    }
    for (let recordIndex = 0; recordIndex < bytesRead; recordIndex += 10) {
      const record = content.slice(recordIndex, recordIndex + 10);
      const termId = record.readUInt32BE(0);
      const posting = record.slice(4);
      if (termId === currentTerm) {
        postings.push(posting);
      } else {
        const termBuffer = Buffer.allocUnsafe(4);
        termBuffer.writeUInt32BE(currentTerm);
        const postingsBuffer = Buffer.concat(postings);
        const blockLengthBuffer = Buffer.allocUnsafe(4);
        blockLengthBuffer.writeUInt32BE(postingsBuffer.length);
        bookmarkedTemp.write(Buffer.concat([termBuffer, blockLengthBuffer, postingsBuffer]));
        currentTerm = termId;
        postings = [record.slice(4)];
      }
    }
    readFileRec(fd, content, position + bytesRead, bookmarkedTemp, resolve);
  });
};

/**
 *
 * @param {Array} files: Array of binar temp files
 * @param {*} index: index of the temp file to be sorted and bookmarked
 * @description: this functions applies bsort to temp postings one by one
 *  After sorting is done the sorted files are read using a buffer and bookmarked noting down
 *  where partial inverted lists for a termId start and end
 */
const sort = (files, index) => {
  const file = files[index];
  if (!file) {
    console.log('temp file sorting done');
    taskComplete();
    return;
  }
  exec(`bsort -k 4 -r 10 ${path.join(process.cwd(), TEMP_FILES_FOLDER, file)}`, async (err) => {
    if (err) {
      console.log(`error occured in sorting ${file}`);
    }
    console.log(`bookmarking ${file}`, new Date());
    const fd = fs.openSync(path.join(process.cwd(), TEMP_FILES_FOLDER, file));
    const bookmarkedTemp = fs.createWriteStream(path.join(process.cwd(), BOOKMARKED_TEMP_FILES_FOLDER, `${file}_bookmarked`));
    const bufferSize = 1000 * 5000;
    const content = Buffer.alloc(bufferSize);
    const fileSorted = new Promise((resolve, reject) => {
      readFileRec(fd, content, 0, bookmarkedTemp, resolve);
    });
    fileSorted.then(() => {
      console.log(`bookmarked ${file}`, new Date());
      sort(files, index + 1);
    });
  });
};
/**
 * @description: function to start the sorter functionality, the sorter picks up unsorted
 *  postings from TEMP_FILES_FOLDER and applies bsort to them.
 *  after be sorting it bookmarks the sorted files,
 *  it notes down where in the sorted file sorted postings for temp files start and end.
 *  the sorted postings are stored in all the docIds followed by all the frequecies.
 *  Also varbyte compression is applied to these docIds and frequecy blocks
 *  Sorting is run only after all bacthes have been tokenized.
 */
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
    sort(fileNames, 0);
  });
  return new Promise((resolve, reject) => {
    taskComplete = resolve;
    taskErrored = reject;
  });
};

export default { start };
