
import batchProcessor from './util/batch-processor';
import sorter from './util/sorter';
import merger from './util/merger';
import queryProcessor from './util/query-processor';

/**
 *
 * @param {Number} batch: indicates number of bacth we are in
 * @param {Object} lexicon: hashtable that maps terms to termIds, gets passed from each batch to next
 * @param {Object} resolve: Promise to indicate all batches are done
 * @param {Object} reject: Promise to indicate an error has occured
 */


/**
 * @description this function is the main flow of the code, it fetches and tokenizes files in
 *  number batches defined in environment variables. then sorts the intermidiate
 *  postings and merges them to get the final index structure
 */
const start = async () => {
  // await batchProcessor.start();
  // await sorter.start();
  // await merger.start();
  queryProcessor.start();
};

start();
