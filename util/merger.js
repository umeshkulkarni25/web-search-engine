import fs from 'fs';
import path from 'path';
import _ from 'lodash';

const { BOOKMARKED_TEMP_FILES_FOLDER, LEXICON } = process.env;
const fileDescriptors = []; //  file descripter for bookmarked files
let lexicon = null; // lexicon in array format
const store = []; // holds block header from bookmarked files
let taskComplete = null;
let taskErorred = null;
// writeStream for file which maps terms to pointer offsets in the binary index
let termToPointer = null;
// writeStream for final binary index
let indexStream = null;

/**
 * @param {Number} num: number to be var-byte encodeds
 * @returns {Array} array of numbers in decimal format to be converted in binary by the caller
 */
const varByteEncode = (num) => {
  const code = [];
  while (true) {
    code.unshift(num % 128);
    num = Math.floor(num / 128);
    if (num === 0) {
      break;
    }
  }
  code[code.length - 1] += 128;
  return code;
};

/**
 * @param {Array} list: array of integers to which var-byte ecoding is applied to
 * @returns {Buffer}: var-byte encoded list in binary format
 * @description: this function helps doing var-byte encoding for the docIds and frequecy blockss
 *  and concats all the individual var-bytes into one binary array block
 */
const varByteCompression = (list) => {
  let compressedList = [];
  _.each(list, (num) => {
    compressedList.push(varByteEncode(num));
  });
  compressedList = compressedList.flat();
  return Buffer.from(compressedList);
};

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
      const buffer = Buffer.alloc(8);
      fs.readSync(fd, buffer, 0, buffer.length, null);
      const termId = buffer.readUInt32BE(0);
      const lengthOfBlock = buffer.readUInt32BE(4);
      store[fdIndex] = {
        termId, lengthOfBlock, fdIndex,
      };
    }
  });
};

const dump = (termHeaders, index, termIndexBuffers, resolve, reject) => {
  const termHeader = termHeaders[index];
  if (!termHeader) {
    resolve({ termIndexBuffers });
    return;
  }
  const buffer = Buffer.alloc(termHeader.lengthOfBlock);
  fs.read(fileDescriptors[termHeader.fdIndex], buffer, 0, termHeader.lengthOfBlock, null, (err, chunk) => {
    termIndexBuffers.push(buffer);
    // set used header entry to null so it can be filled up again in next iteration
    store[termHeader.fdIndex] = null;
    dump(termHeaders, index + 1, termIndexBuffers, resolve, reject);
  });
};
/**
 *
 * @param {Object} termDescriptor: contains termId and length of its docId and frequency blocks.
 * @param {*} offsetInInvertedIndex: the pointer in the inverted list where the postings should be inserted
 * @description: this function picks up all the blocks for a given termId from all the bookmarked-temp files
 *  and writes it to final index and provided offset
 */
const createIndex = (termDescriptors, index, offsetInvertedIndex) => {
  const termDescriptor = termDescriptors[index];
  if (!termDescriptor) {
    console.log('index built!!!', new Date());
    taskComplete();
    return;
  }

  refillHeaders(); // get the headers for current termId
  const termHeaders = _.filter(store, ['termId', parseInt(termDescriptor.split(' ')[1], 10)]);
  // read the block for docIds and frequency from all the files and write them to a index stream
  new Promise((resolve, reject) => {
    dump(termHeaders, 0, [], resolve, reject);
  }).then(({ termIndexBuffers, numberOfDocs }) => {
    const termInvertedIndex = Buffer.concat(termIndexBuffers);
    const postings = [];
    for (let postingIndex = 0; postingIndex < termInvertedIndex.length; postingIndex += 6) {
      const posting = termInvertedIndex.slice(postingIndex, postingIndex + 6);
      const docId = posting.readUInt32BE(0);
      const freq = posting.readUInt16BE(4);
      postings.push([docId, freq]);
    }
    const sortedPostings = _.sortBy(postings, (posting) => posting[0]);
    const [sortedDocIds, freqs] = _.reduce(sortedPostings, (unzippedPostings, posting) => {
      unzippedPostings[0].push(posting[0]);
      unzippedPostings[1].push(posting[1]);
      return unzippedPostings;
    },
    [[], []]);
    let previousDocId = 0;
    const sortedDocIdsWithDiff = _.map(sortedDocIds, (currentDocId) => {
      const diff = currentDocId - previousDocId;
      previousDocId = currentDocId;
      return diff;
    });
    const docIdsBuffer = varByteCompression(sortedDocIdsWithDiff);
    const compressedInvertedList = Buffer.concat([docIdsBuffer, varByteCompression(freqs)]);
    indexStream.write(compressedInvertedList);
    termToPointer.write(`${termDescriptor.split(' ')[0]}, ${offsetInvertedIndex + docIdsBuffer.length}, ${offsetInvertedIndex + compressedInvertedList.length}, ${sortedDocIds.length}\n`);
    createIndex(termDescriptors, index + 1, offsetInvertedIndex + compressedInvertedList.length);
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
  // writeStream for file which maps terms to pointer offsets in the binary index
  termToPointer = fs.createWriteStream(path.join(process.cwd(), 'lexicon_with_byte_offset'));
  // writeStream for final binary index
  indexStream = fs.createWriteStream(path.join(process.cwd(), 'index'));
  fs.readdir(path.join(process.cwd(), BOOKMARKED_TEMP_FILES_FOLDER), async (err, files) => {
    if (err) {
      taskErorred();
    }
    await readLexicon();
    _.each(files, (file) => {
      const completeFilePath = path.join(process.cwd(), BOOKMARKED_TEMP_FILES_FOLDER, file);
      fileDescriptors.push(fs.openSync(completeFilePath));
    });
    console.log('started building index', new Date());
    createIndex(lexicon, 0, 0);
  });
  return new Promise((resolve, reject) => {
    taskComplete = resolve;
    taskErorred = reject;
  });
};

export default { start };
