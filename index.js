
import batchProcessor from './util/batch-processor';
import sorter from './util/sorter';
import merger from './util/merger';
import queryProcessor from './util/query-processor';
import './util/database';

const { BUILD_INDEX } = process.env;
/**
 * @description this function is the main flow of the code, it fetches and tokenizes files in
 *  number batches defined in environment variables. then sorts the intermidiate
 *  postings and merges them to get the final index structure
 */
const start = async () => {
  if (BUILD_INDEX === 'true') {
    // await batchProcessor.start();
    // await sorter.start();
    await merger.start();
    queryProcessor.start();
  } else {
    queryProcessor.start();
  }
};

start();
