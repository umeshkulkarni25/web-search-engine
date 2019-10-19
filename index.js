import fs from 'fs';
import path from 'path';
import _ from 'lodash';
import tokenizer from './util/tokenizer';
import commonCrawl from './util/common-crawl';
import sorter from './util/sorter';
import merger from './util/merger';

const { NUMBER_OF_BATCHES } = process.env;

const start = async (batch, lexicon) => {
  if (batch >= NUMBER_OF_BATCHES) {
    return;
  }

  try {
    await commonCrawl.fetchWETFiles(batch);
  } catch (err) {
    console.log(err);
    process.exit();
  }

  lexicon = await tokenizer.tokenize(batch);
  const lexiconStream = fs.createWriteStream(path.join(process.cwd(), 'lexicon'));
  _.each(Object.keys(lexicon), (term) => {
    lexiconStream.write(`${term} ${lexicon[term]}\n`);
  });
  console.log(`finsihed batch${batch}`);
  start(batch + 1, lexicon);
};
sorter.start();
