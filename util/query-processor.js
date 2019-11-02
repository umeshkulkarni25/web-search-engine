import fs from 'fs';
import path from 'path';
import _ from 'lodash';

const { LEXICON_WITH_BYTE_OFFSET, PAGE_TABLE } = process.env;


const openIndex = () => new Promise((resolve, reject) => {
  fs.open(path.join(process.cwd(), 'index'), (err, fd) => {
    if (err) {
      reject(err);
      return;
    }
    resolve(fd);
  });
});

const readBlock = (fd, [position, blockLength]) => new Promise((resolve, reject) => {
  fs.read(fd, Buffer.alloc(blockLength), 0, blockLength, position, (err, bytesRead, buffer) => {
    if (err || bytesRead !== blockLength) {
      reject();
      return;
    }
    resolve(buffer);
  });
});

const readLexicon = () => new Promise((resolve, reject) => {
  let serializedLexicon = ''; // lexicon read as a string
  const lexiconPath = path.join(process.cwd(), LEXICON_WITH_BYTE_OFFSET);
  fs.createReadStream(lexiconPath)
    .on('data', (chunk) => {
      serializedLexicon += chunk;
    })
    .on('close', () => {
      resolve(serializedLexicon);
    })
    .on('error', (err) => {
      reject(err);
    });
});

const parseLexicon = () => new Promise((resolve, reject) => {
  readLexicon()
    .then((serializedLexicon) => {
      const lexicon = {};
      const semiParsedLexicon = _.map(serializedLexicon.split('\n'), (entry) => {
        const parsedEntry = entry.split(', ');
        return [parsedEntry[0], parseInt(parsedEntry[1], 10), parseInt(parsedEntry[2], 10)];
      });
      _.each(semiParsedLexicon, ([term, endOfTermIndex, numberOfDocs], index) => {
        const previousEntry = semiParsedLexicon[index - 1];
        const startOfTermIndex = previousEntry ? previousEntry[1] : 0;
        lexicon[term] = [
          startOfTermIndex,
          endOfTermIndex - startOfTermIndex,
          numberOfDocs,
        ];
      });
      resolve(lexicon);
    })
    .catch((err) => {
      reject(err);
    });
});

const readPageTable = () => new Promise((resolve, reject) => {
  let serializedPageTable = ''; // lexicon read as a string
  const pageTablePath = path.join(process.cwd(), PAGE_TABLE);
  fs.createReadStream(pageTablePath)
    .on('data', (chunk) => {
      serializedPageTable += chunk;
    })
    .on('close', () => {
      resolve(serializedPageTable);
    })
    .on('error', (err) => {
      reject(err);
    });
});

const parsePageTable = () => new Promise((resolve, reject) => {
  readPageTable()
    .then((serializedPageTable) => {
      const semiParsedPageTable = serializedPageTable.split('\n');
      semiParsedPageTable.pop(); // get rid of last \n
      const pageTable = {};
      let accumilatedDocLength = 0;
      _.each(semiParsedPageTable, (entry) => {
        const parsedEntry = entry.split(' ');
        const docLength = parseInt(parsedEntry[2], 10);
        accumilatedDocLength += docLength;
        pageTable[parsedEntry[0]] = [parsedEntry[1], docLength];
      });
      const avgLengthOfDocInCollection = accumilatedDocLength / semiParsedPageTable.length;
      const totalNumberOfDocsInCollection = semiParsedPageTable.length;
      resolve({ pageTable, avgLengthOfDocInCollection, totalNumberOfDocsInCollection });
    })
    .catch((err) => {
      reject(err);
    });
});

const varByteDecode = (block, numberOfDocs) => {
  let num = 0;
  let i = 0;
  const docIds = []; const freqs = [];

  while (docIds.length < numberOfDocs) {
    const byte = block.readUInt8(i);
    if (byte < 128) {
      num = (num + byte) * 128;
    } else {
      num += byte - 128;
      docIds.push(num);
      num = 0;
    }
    i += 1;
  }
  while (freqs.length < numberOfDocs) {
    const byte = block.readUInt8(i);
    if (byte < 128) {
      num = num * 128 + byte * 128;
    } else {
      num += byte - 128;
      freqs.push(num);
      num = 0;
    }
    i += 1;
  }
  return [docIds, freqs, numberOfDocs];
};

const nextGEQ = (list, listIndex, docId) => {
  while (list[listIndex] < docId) {
    listIndex += 1;
  }
  return [list[listIndex], listIndex + 1];
};

const BM25 = (totalNumberOfDocsInCollection, termFreqs, numberOfDocsWithTerm, docLength, avgLengthOfDocInCollection) => {
  const k_1 = 1.2;
  const b = 0.75;
  const K = k_1 * ((1 - b) + b * (docLength / avgLengthOfDocInCollection));
  let score = 0;
  _.each(termFreqs, (termFreq, index) => {
    score
      += Math.log((totalNumberOfDocsInCollection - numberOfDocsWithTerm[index] + 0.5) / (numberOfDocsWithTerm[index] + 0.5))
      * (((k_1 + 1) * termFreq) / (K + termFreq));
  });
  return score;
};
const DAAT = (decodedTermBlocks, pageTable, avgLengthOfDocInCollection, totalNumberOfDocsInCollection) => {
  let sortedDecodedTermBlocks = _.sortBy(decodedTermBlocks, (termBlock) => termBlock[2]);
  sortedDecodedTermBlocks = _.map(sortedDecodedTermBlocks, (block) => { block.push(0); return block; });
  const shortestTermBlock = sortedDecodedTermBlocks[0];
  let dId = 0;
  const intersection = [];
  let results = [];
  while (shortestTermBlock[3] < shortestTermBlock[0].length) {
    const result = nextGEQ(shortestTermBlock[0], shortestTermBlock[3], dId);
    shortestTermBlock[3] = result[1];
    dId = result[0];
    let i = 1;
    for (i; i < sortedDecodedTermBlocks.length; i += 1) {
      const block = sortedDecodedTermBlocks[i];
      const geqResult = nextGEQ(block[0], block[3], dId);
      block[3] = geqResult[1];
      const nextGEQDId = geqResult[0];
      if (nextGEQDId > dId) {
        dId = nextGEQDId;
        break;
      }
    }
    if (i === decodedTermBlocks.length) {
      intersection.push(dId);
      if (!dId) { break; }
      const docLength = pageTable[dId][1];
      const termFreqs = _.map(sortedDecodedTermBlocks, (block) => block[1][block[3] - 1]);
      const numberOfDocsWithTerm = _.map(sortedDecodedTermBlocks, (block) => block[2]);
      const score = BM25(totalNumberOfDocsInCollection, termFreqs, numberOfDocsWithTerm, docLength, avgLengthOfDocInCollection);
      let index = 0;
      while (results[index] && (results[index][0] > score)) {
        index += 1;
      }
      results.splice(index + 1, 1, [score, pageTable[dId][0]]);
      if (results.length > 10) {
        results = results.slice(0, 10);
      }
    }
  }
  console.log(results);
};

const processQuery = (query, lexicon, pageTable, indexFd, avgLengthOfDocInCollection, totalNumberOfDocsInCollection) => {
  const terms = query.split(' ');
  const termIndexOffsets = _.map(terms, (term) => lexicon[term]);
  Promise
    .all(_.map(termIndexOffsets, (termIndexOffset) => readBlock(indexFd, termIndexOffset)))
    .then((termBlocks) => {
      const decodedTermBlocks = _.map(termBlocks, (block, index) => varByteDecode(block, termIndexOffsets[index][2]));
      DAAT(decodedTermBlocks, pageTable, avgLengthOfDocInCollection, totalNumberOfDocsInCollection);
    });
};

const start = () => {
  Promise
    .all([parseLexicon(), parsePageTable(), openIndex()])
    .then(([lexicon, { pageTable, avgLengthOfDocInCollection, totalNumberOfDocsInCollection }, indexFd]) => {
    //
      console.log('accepting queries now');
      const stdin = process.openStdin();

      stdin.addListener('data', (d) => {
        processQuery(d.toString().trim(), lexicon, pageTable, indexFd, avgLengthOfDocInCollection, totalNumberOfDocsInCollection);
      });
    });
};

export default { start };
