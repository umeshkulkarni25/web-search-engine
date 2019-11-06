import fs from 'fs';
import path from 'path';
import _ from 'lodash';
import Document from './database/Document';

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

const readBlock = (fd, position, blockLength) => new Promise((resolve, reject) => {
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
        return [parsedEntry[0], parseInt(parsedEntry[1], 10), parseInt(parsedEntry[2], 10), parseInt(parsedEntry[3], 10)];
      });
      _.each(semiParsedLexicon, ([term, endOfDocIdBlock, endOfFreqBlock, numberOfDocs], index) => {
        const previousEntry = semiParsedLexicon[index - 1];
        const startOfTermIndex = previousEntry ? previousEntry[2] : 0;
        lexicon[term] = [
          startOfTermIndex,
          endOfDocIdBlock,
          endOfFreqBlock,
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

const varByteDecode = (varByteCompressedBlock, position) => {
  let num = 0;
  let index = position;
  while (true) {
    const byte = varByteCompressedBlock.readUInt8(index);
    if (byte < 128) {
      num = (num + byte) * 128;
    } else {
      num += byte - 128;
      break;
    }
    index += 1;
  }
  return [num, index + 1];
};

const nextGEQ = (list, docId) => {
  while (true) {
    if (list.numberOfDocIdsRead >= list.numberOfDocIds) {
      break;
    }
    const [diff, docIdPointer] = varByteDecode(list.compressedDocIds, list.compressedDocIdPointer);
    list.lastReadDocId += diff;
    list.numberOfDocIdsRead += 1;
    list.compressedDocIdPointer = docIdPointer;
    if (list.lastReadDocId >= docId) {
      break;
    }
  }
  return list.lastReadDocId;
};

const getFreq = (list) => {
  while (list.numberOfFreqsRead < list.numberOfDocIdsRead) {
    const [diff, freqPointer] = varByteDecode(list.compressedFreqs, list.compressedFreqsPointer);
    list.lastReadFreq += diff;
    list.numberOfFreqsRead += 1;
    list.compressedFreqsPointer = freqPointer;
  }
  return list.lastReadFreq;
};

const BM25 = (totalNumberOfDocsInCollection, termFreqs, numberOfDocsWithTerm, docLength, avgLengthOfDocInCollection) => {
  const k_1 = 1.2;
  const b = 0.75;
  const K = k_1 * ((1 - b) + b * (docLength / avgLengthOfDocInCollection));
  let score = 0;
  _.each(termFreqs, (termFreq, index) => {
    if (!termFreq) { termFreq = 0; }
    score
      += Math.log((totalNumberOfDocsInCollection - numberOfDocsWithTerm[index] + 0.5) / (numberOfDocsWithTerm[index] + 0.5))
      * (((k_1 + 1) * termFreq) / (K + termFreq));
  });
  return score;
};
const DAAT = (lists, pageTable, avgLengthOfDocInCollection, totalNumberOfDocsInCollection) => {
  const sortedListsByNumberOfDocs = _.sortBy(lists, (list) => list.numberOfDocIds);
  const shortestList = sortedListsByNumberOfDocs[0];
  let dId = 0;
  let results = [];
  let reachedEndOfOneOfTheLists = false;
  while (!reachedEndOfOneOfTheLists) {
    dId = nextGEQ(shortestList, dId);
    if (shortestList.numberOfDocIds === shortestList.numberOfDocIdsRead) {
      reachedEndOfOneOfTheLists = true;
    }
    let docIdInIntersection = true;
    for (let listIndex = 1; listIndex < sortedListsByNumberOfDocs.length; listIndex += 1) {
      const list = sortedListsByNumberOfDocs[listIndex];
      const nextGEQDId = nextGEQ(list, dId);
      if (list.numberOfDocIds === list.numberOfDocIdsRead) {
        reachedEndOfOneOfTheLists = true;
      }
      if (nextGEQDId > dId) {
        dId = nextGEQDId;
        docIdInIntersection = false;
        break;
      }
    }
    if (docIdInIntersection) {
      const docLength = pageTable[dId][1];
      const termFreqs = _.map(sortedListsByNumberOfDocs, (list) => getFreq(list));
      const numberOfDocsWithTerm = _.map(sortedListsByNumberOfDocs, (list) => list.numberOfDocIds);
      const score = BM25(totalNumberOfDocsInCollection, termFreqs, numberOfDocsWithTerm, docLength, avgLengthOfDocInCollection);
      let index = 0;
      while (results[index] && (results[index][0] > score)) {
        index += 1;
      }
      results.splice(index, 0, [score, dId, termFreqs]);
      if (results.length > 10) {
        results = results.slice(0, 10);
      }
    }
    if (reachedEndOfOneOfTheLists) {
      break;
    }
  }
  return results;
};

const refillDocIds = (docIdsCollection, lists) => _.reduce(lists, (acc, list) => {
  const docId = nextGEQ(list, 0);
  if (_.has(acc, docId)) {
    acc[docId].push(list);
  } else {
    acc[docId] = [list];
  }
  return acc;
}, docIdsCollection);

const getNumberOfEmptyLists = (lists) => _.reduce(lists, (numberOfEmptyLists, list) => (list.numberOfDocIdsRead === list.numberOfDocIds ? numberOfEmptyLists + 1 : numberOfEmptyLists), 0);

const conjuctive = (lists, pageTable, avgLengthOfDocInCollection, totalNumberOfDocsInCollection) => {
  let groupByDocIds = refillDocIds({}, lists);
  let results = [];
  let numberOfEmptyLists = getNumberOfEmptyLists(lists);
  while (numberOfEmptyLists < lists.length) {
    const sortedDocIds = _.sortBy(_.map(_.keys(groupByDocIds), (docId) => parseInt(docId, 10)));
    const docLength = pageTable[sortedDocIds[0]][1];
    const termFreqs = _.map(groupByDocIds[sortedDocIds[0]], (list) => getFreq(list));
    const numberOfDocsWithTerm = _.map(groupByDocIds[sortedDocIds[0]], (list) => list.numberOfDocIds);
    const score = BM25(totalNumberOfDocsInCollection, termFreqs, numberOfDocsWithTerm, docLength, avgLengthOfDocInCollection);
    let index = 0;
    while (results[index] && (results[index][0] > score)) {
      index += 1;
    }
    results.splice(index, 0, [score, sortedDocIds[0], termFreqs]);
    if (results.length > 10) {
      results = results.slice(0, 10);
    }
    groupByDocIds = refillDocIds(groupByDocIds, groupByDocIds[sortedDocIds[0]]);
    numberOfEmptyLists = getNumberOfEmptyLists(lists);
    delete groupByDocIds[sortedDocIds[0]];
  }
  console.log(results);
  return results;
};
const openLists = (fd, listHeaders) => Promise.all(_.map(listHeaders, (listHeader) => readBlock(fd, listHeader[0], listHeader[2] - listHeader[0])));
const processQuery = (query, lexicon, pageTable, indexFd, avgLengthOfDocInCollection, totalNumberOfDocsInCollection) => {
  const terms = query.split(' ');
  const listHeaders = _.map(terms, (term) => lexicon[term]);
  openLists(indexFd, listHeaders)
    .then((listBuffers) => {
      const lists = [];
      _.each(listBuffers, (listBuffer, index) => {
        const listHeader = listHeaders[index];
        const docIdBlockLength = listHeader[1] - listHeader[0];
        lists.push({
          term: terms[index],
          compressedDocIds: listBuffer.slice(0, docIdBlockLength),
          compressedFreqs: listBuffer.slice(docIdBlockLength),
          numberOfDocIds: listHeader[3],
          compressedFreqsPointer: 0,
          numberOfDocIdsRead: 0,
          numberOfFreqsRead: 0,
          lastReadDocId: 0,
          lastReadFreq: 0,
          compressedDocIdPointer: 0,
        });
      });
      //  const results = DAAT(lists, pageTable, avgLengthOfDocInCollection, totalNumberOfDocsInCollection);
      const results = conjuctive(lists, pageTable, avgLengthOfDocInCollection, totalNumberOfDocsInCollection);
      console.log(results);
      const snippets = [];
      _.each(results, (result) => {
        snippets.push(Document.findOne({ docId: result[1] }));
      });
      Promise
        .all(snippets)
        .then((docs) => {
          _.each(docs, (doc, index) => {
            const content = doc.content.toLowerCase();
            let finalSnippet = '';
            _.each(terms, (term) => {
              const termIndex = content.indexOf(term);
              if (termIndex > 0) {
                finalSnippet = `${finalSnippet}...${content.slice(termIndex - 30, termIndex + 30)}`;
              }
            });
            console.log({
              bm25: results[index][0],
              termFreqs: results[index][3],
              url: pageTable[results[index][1]][0],
              snippet: finalSnippet,
            });
          });
        });
    });
};

const start = () => {
  Promise
    .all([parseLexicon(), parsePageTable(), openIndex()])
    .then(([lexicon, { pageTable, avgLengthOfDocInCollection, totalNumberOfDocsInCollection }, indexFd]) => {
      console.log('accepting queries now');
      const stdin = process.openStdin();
      stdin.addListener('data', (query) => {
        processQuery(query.toString().trim().toLowerCase(), lexicon, pageTable, indexFd, avgLengthOfDocInCollection, totalNumberOfDocsInCollection);
      });
      // processQuery('mua kim', lexicon, pageTable, indexFd, avgLengthOfDocInCollection, totalNumberOfDocsInCollection);
    });
};

export default { start };
