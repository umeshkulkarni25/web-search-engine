import fs from 'fs';
import path from 'path';
import _ from 'lodash';
import tokenizer from './util/tokenizer';
import commonCrawl from './util/common-crawl';

const { FETCH_WET_FILES } = process.env;
const batches = 4;
const start = async (batch, lexicon) => {
  if (batch >= batches) {
    const lexiconStream = fs.createWriteStream(path.join(process.cwd(), 'lexicon'));
    _.forEach(Object.keys(lexicon), (term) => {
      lexiconStream.write(`${term} ${lexicon[term]}\n`);
    });
    return;
  }
  if (FETCH_WET_FILES === 'true') {
    try {
      await commonCrawl.fetchWETFiles(batch);
    } catch (err) {
      console.log(err);
      process.exit();
    }
  }
  lexicon = await tokenizer.tokenize(batch);
  start(batch + 1, lexicon);
};

start(1);
