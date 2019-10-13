import parser from './util/tokenizer';
import commonCrawl from './util/common-crawl';

const { FETCH_WET_FILES } = process.env;

const start = async () => {
  if (FETCH_WET_FILES === 'true') {
    try {
      await commonCrawl.fetchWETFiles();
    } catch (err) {
      console.log(err);
      process.exit();
    }
  }
  parser.parse();
};

start();
