import fs from 'fs';
import path from 'path';
import _ from 'lodash';

const { BOOKMARKED_TEMP_FILES_FOLDER, LEXICON } = process.env;
const fileDescriptors = []; //  file descripter for bookmarked files
let lexicon = null; // lexicon in array format
// writeStream for file which maps terms to pointer offsets in the binary index
const termToPointer = fs.createWriteStream(path.join(process.cwd(), 'lexicon_with_byte_offset'));
// writeStream for final binary index
const indexStream = fs.createWriteStream(path.join(process.cwd(), 'index'));
const store = []; // holds block header from bookmarked files
let taskComplete = null;
let taskErorred = null;
/**
 * @description: reads the lexicon created by the tokenizer.
 */
const readLexicon = () => new Promise((resolve, reject) => {
  let serializedLexicon = ''; // lexicon read as a string
  const lexiconPath = path.join(process.cwd(), LEXICON);
  fs.createReadStream(lexiconPath)
    .on('data', (chunk) => {
      serializedLexicon += chunk;
    })
    .on('close', () => {
      lexicon = serializedLexicon.split('\n');
      lexicon.pop(); // get rid of last \n
      resolve();
    })
    .on('error', (err) => {
      console.log(err);
      reject();
    });
});


/**
 * @description: reads the next header available in the bookmarked files
 *  it looks up the store to see which fileHeaders need to to refilled and
 *  reads headers only form those files.
 *  in createIndex once the header gets used it is set to null and that is
 *  the cue for refill function to fetch the next header.
 */
const refillHeaders = () => {
  _.each(fileDescriptors, (fd, fdIndex) => {
    if (!store[fdIndex]) {
      const buffer = Buffer.alloc(12);
      fs.readSync(fd, buffer, 0, buffer.length, null);
      const termId = buffer.readUInt32BE(0);
      const lengthOfBlock = buffer.readUInt32BE(4);
      const numberOfDocs = buffer.readUInt32BE(8);
      store[fdIndex] = {
        termId, lengthOfBlock, numberOfDocs, fdIndex,
      };
    }
  });
};

const dump = (termHeaders, index, termIndexBuffers, numberOfDocs, resolve, reject) => {
  const termHeader = termHeaders[index];
  if (!termHeader) {
    resolve({ termIndexBuffers, numberOfDocs });
    return;
  }
  const buffer = Buffer.alloc(termHeader.lengthOfBlock);
  fs.read(fileDescriptors[termHeader.fdIndex], buffer, 0, termHeader.lengthOfBlock, null, (err, chunk) => {
    termIndexBuffers.push(buffer);
    // set used header entry to null so it can be filled up again in next iteration
    store[termHeader.fdIndex] = null;
    dump(termHeaders, index + 1, termIndexBuffers, numberOfDocs + termHeader.numberOfDocs, resolve, reject);
  });
};
/**
 *
 * @param {Object} termDescriptor: contains termId and length of its docId and frequency blocks.
 * @param {*} offsetInInvertedIndex: the pointer in the inverted list where the postings should be inserted
 * @description: this function picks up all the blocks for a given termId from all the bookmarked-temp files
 *  and writes it to final index and provided offset
 */
const createIndex = (termDescriptors, index, offsetInInvertedIndex) => {
  const termDescriptor = termDescriptors[index];
  if (!termDescriptor) {
    console.log('index built!!!');
    taskComplete();
    return;
  }

  refillHeaders(); // get the headers for current termId
  const termHeaders = _.filter(store, ['termId', parseInt(termDescriptor.split(' ')[1], 10)]);
  // read the block for docIds and frequency from all the files and write them to a index stream
  new Promise((resolve, reject) => {
    dump(termHeaders, 0, [], 0, resolve, reject);
  }).then(({ termIndexBuffers, numberOfDocs }) => {
    const termInvertedIndex = Buffer.concat(termIndexBuffers);
    indexStream.write(termInvertedIndex);
    termToPointer.write(`${termDescriptor.split(' ')[0]}, ${offsetInInvertedIndex + termInvertedIndex.length}, ${numberOfDocs}\n`);
    createIndex(termDescriptors, index + 1, offsetInInvertedIndex + termInvertedIndex.length);
  });
};
/**
 * @description: this function reads the bookmarked files form BOOKMARKED_TEMP_FILES_FOLDER
 *  and merges the bookmarked docId and frequcy blocks to create the final index.
 *  it reads the lexicon that was created by the tokenizer which in sorted format,
 *  goes over the bookmarked postings which are also sorted by termIds and merges them
 *  in an IO efficient manner
 */
const start = async () => {
  fs.readdir(path.join(process.cwd(), BOOKMARKED_TEMP_FILES_FOLDER), async (err, files) => {
    if (err) {
      taskErorred();
    }
    await readLexicon();
    _.each(files, (file) => {
      const completeFilePath = path.join(process.cwd(), BOOKMARKED_TEMP_FILES_FOLDER, file);
      fileDescriptors.push(fs.openSync(completeFilePath));
    });
    console.log('started building index');
    createIndex(lexicon, 0, 0);
  });
  return new Promise((resolve, reject) => {
    taskComplete = resolve;
    taskErorred = reject;
  });
};

export default { start };
