import commonCrawl from './util/common-crawl';

const { FETCH_WET_FILES } = process.env;

if (FETCH_WET_FILES === 'true') {
  commonCrawl.fetchWETFiles();
}
