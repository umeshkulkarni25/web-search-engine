import zlib from 'zlib';
import { Transform } from 'stream';
import axios from 'axios';
import fs from 'fs';
import fsExtra from 'fs-extra';
import path from 'path';
import _ from 'lodash';

const {
  WET_PATH_URL, WET_PATH_PREFIX, WET_FILES_FOLDER, WET_FILES_PER_BATCH, DOWNLOAD_WORKERS,
} = process.env;

const workerLimit = parseInt(DOWNLOAD_WORKERS, 10); // total workers allowed for simultaneous downloadings
let workers = workerLimit;
const WETDirPath = path.join(process.cwd(), WET_FILES_FOLDER);
/**
 *
 * @param {Array} WETUrls: array of all WET_urls fetched from common crawl
 * @param {Number} index: index of WET_File to be fetched
 * @param {Number} batch: Number indicating current batch
 */
const fetch = (WETUrls, index, batch, resolve, reject) => {
  // check if all the files are downloaded for a given worker
  if (index >= (batch + 1) * WET_FILES_PER_BATCH) {
    workers -= 1;
    if (workers <= 0) {
      console.log('fetch complete');
      resolve(fs.readdirSync(WETDirPath));
    }
    return;
  }
  // fetch and pipe the response to disk
  const WETUrl = WETUrls[index];
  axios({ url: WETUrl, responseType: 'stream' })
    .then((response) => {
      const compressedFileName = WETUrl.split('/').pop();
      response.data
        .pipe(fs.createWriteStream(path.join(WETDirPath, compressedFileName)))
        .on('finish', () => {
          // on finish, call next fetch on next file for the given worker
          fetch(WETUrls, index + workerLimit, batch, resolve, reject);
        });
    })
    .catch((err) => {
      reject(err);
    });
};

/**
 *
 * @param {Number} batch: indicates what batch the code is downloading.
 * @returns {Promise}: to indicate all files in this batch are downloaded.
 * @description: this function fetches the WET files descriptor then collects the correct paths
 *  based on what batch we are in and the downloads 5 files simultaneously.
 *  Uncompressing for wet.paths.gz is done using pipes on the fly.
 *  The actual WET files are stored in compressed format on the disk.
 */
const fetchWETFiles = (batch) => new Promise((resolve, reject) => {
  let WETPaths = '';
  axios({ url: WET_PATH_URL, responseType: 'stream' }).then((response) => {
    response.data
      .pipe(zlib.createGunzip()) // pipe response into an unzipping stream
      .pipe(new Transform({ // transform stram to collect the paths
        objectMode: true,
        transform: (chunk, encoding, done) => {
          WETPaths += chunk.toString();
          done();
        },
        flush: () => {
          console.log('starting to fetch WARC files');
          console.log('target:', WET_FILES_PER_BATCH);
          console.log('destination', WETDirPath);
          /**
         * clear WET files directory from previous batch this helps us go through
         *  hundreds of WET files without overrunning the hard-disk space
         * */
          if (fs.existsSync(WETDirPath)) {
            fsExtra.removeSync(WETDirPath);
          }
          fsExtra.mkdirSync(WETDirPath);
          // download WET files for this batch with 5 simultenious downloads
          const WETUrls = WETPaths.split('\n').map((directortPath) => `${WET_PATH_PREFIX}${directortPath}`);
          workers = workerLimit;
          _.each(new Array(workerLimit).fill(0), (data, workerIndex) => {
            fetch(WETUrls, batch * WET_FILES_PER_BATCH + workerIndex, batch, resolve, reject);
          });
        },
      }));
  });
});

export default {
  fetchWETFiles,
};
