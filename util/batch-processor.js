import _ from 'lodash';
import fs from 'fs';
import path from 'path';
import tokenizer from './tokenizer';
import commonCrawl from './common-crawl';

let taskComplete = null;
let taskErorred = null;
const { NUMBER_OF_BATCHES } = process.env;

/**
 *
 * @param {Object} lexicon: hashtable that maps terms to termIds
 * @description this functions writes lexicon to disk and uses Node streams to do it in an
 * IO efficient way
 */
const serializeLexicon = (lexicon) => {
  const lexiconStream = fs.createWriteStream(path.join(process.cwd(), 'lexicon'));
  _.each(Object.keys(lexicon), (term) => {
    lexiconStream.write(`${term} ${lexicon[term]}\n`);
  });
  lexiconStream.end();
};

const fetchAndTokenize = async (batch, lexicon) => {
  if (batch >= NUMBER_OF_BATCHES) {
    taskComplete(lexicon);
    return;
  }
  await commonCrawl.fetchWETFiles(batch); // download WET files for this batch
  tokenizer.tokenize(batch)
    .then((updatedLexicon) => {
      serializeLexicon(lexicon);
      fetchAndTokenize(batch + 1, updatedLexicon);
    })
    .catch((err) => {
      taskErorred();
    });
};

const start = () => {
  fetchAndTokenize(0, {});
  return new Promise((resolve, reject) => {
    taskComplete = resolve;
    taskErorred = reject;
  });
};

export default { start };
