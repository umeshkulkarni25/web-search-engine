import fs from 'fs';
import path from 'path';
import _ from 'lodash';

const { BOOKMARKED_TEMP_FILES_FOLDER, LEXICON } = process.env;
const fileDescriptors = [];
let serializedLexicon = '';
let lexicon = null;
const wordToPointer = fs.createWriteStream(path.join(process.cwd(), 'wordTopointer'));
const indexStream = fs.createWriteStream(path.join(process.cwd(), 'index'));
const readLexicon = () => new Promise((resolve, reject) => {
  fs.createReadStream(path.join(process.cwd(), LEXICON))
    .on('data', (chunk) => {
      serializedLexicon += chunk;
    })
    .on('close', () => {
      lexicon = serializedLexicon.split('\n');
      console.log(lexicon.pop()); // get rid of last \n
      resolve();
    })
    .on('error', (err) => {
      console.log(err);
      reject();
    });
});

const store = [];
const refillHeaders = () => {
  _.each(fileDescriptors, (fd, fdIndex) => {
    if (!store[fdIndex]) {
      const buffer = Buffer.alloc(8);
      const header = fs.readSync(fd, buffer, 0, buffer.length, null);
      const termId = buffer.readUInt32BE(0);
      const lengthOfBlock = buffer.readUInt32BE(4);
      store[fdIndex] = { termId, lengthOfBlock, fdIndex };
    }
  });
};
const createIndex = (termDescriptor, offsetInInvertedIndex) => {
  const termIndexBuffers = [];
  refillHeaders();
  const termHeaders = _.filter(store, ['termId', parseInt(termDescriptor.split(' ')[1], 10)]);
  _.each(termHeaders, (termHeader) => {
    const buffer = Buffer.alloc(termHeader.lengthOfBlock);
    fs.readSync(fileDescriptors[termHeader.fdIndex], buffer, 0, termHeader.lengthOfBlock, null);
    termIndexBuffers.push(buffer);
    store[termHeader.fdIndex] = null;
  });
  const termInvertedIndex = Buffer.concat(termIndexBuffers);
  indexStream.write(termInvertedIndex);
  wordToPointer.write(`${termDescriptor.split(' ')[0]}, ${offsetInInvertedIndex + termInvertedIndex.length}\n`);
  return termInvertedIndex.length;
};

const start = () => {
  fs.readdir(path.join(process.cwd(), BOOKMARKED_TEMP_FILES_FOLDER), async (err, files) => {
    if (err) {
      process.exit();
    }
    await readLexicon();
    _.each(files, (file) => {
      const completeFilePath = path.join(process.cwd(), BOOKMARKED_TEMP_FILES_FOLDER, file);
      fileDescriptors.push(fs.openSync(completeFilePath));
    });
    let offsetInInvertedIndex = 0;
    for (let index = 0; index < lexicon.length; index += 1) {
      if (index === lexicon.length - 1) {
        console.log('hi');
      }
      offsetInInvertedIndex += createIndex(lexicon[index], offsetInInvertedIndex);
    }
  });
};


export default { start };
