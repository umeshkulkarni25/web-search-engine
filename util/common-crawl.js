import zlib from 'zlib';
import { Transform } from 'stream';
import axios from 'axios';
import fs from 'fs';
import fsExtra from 'fs-extra';
import path from 'path';


let fetchComplete = null;
let fetchError = null;
const workerLimit = 5;
let workers = 5;
const {
  WET_PATH_URL, WET_PATH_PREFIX, WET_FILES_FOLDER, WET_FILES_PER_BATCH,
} = process.env;
const WETDirPath = path.join(process.cwd(), WET_FILES_FOLDER);

const fetch = (WETUrls, index, batch) => {
  if (index >= (batch + 1) * WET_FILES_PER_BATCH) {
    workers -= 1;
    if (workers <= 0) {
      console.log('fetch complete');
      fetchComplete(fs.readdirSync(WETDirPath));
    }
    return;
  }
  const WETUrl = WETUrls[index];
  if (WETUrl === WET_PATH_PREFIX) {
    fetch(WETUrls, index + 1);
  }
  axios({ url: WETUrl, responseType: 'stream' })
    .then((response) => {
      const compressedFileName = WETUrl.split('/').pop();
      response.data
        .pipe(fs.createWriteStream(path.join(WETDirPath, compressedFileName)))
        .on('finish', () => { fetch(WETUrls, index + workerLimit, batch); });
    })
    .catch((err) => {
      fetchError(err);
    });
};

const fetchWETFiles = async (batch) => {
  let WETPaths = '';
  const { data } = await axios({ url: WET_PATH_URL, responseType: 'stream' });
  data
    .pipe(zlib.createGunzip())
    .pipe(new Transform({
      objectMode: true,
      transform: (chunk, encoding, done) => {
        WETPaths += chunk.toString();
        done();
      },
      flush: () => {
        console.log('starting to fetch WARC files');
        console.log('target:', WET_FILES_PER_BATCH);
        console.log('destination', WETDirPath);
        fsExtra.removeSync(WETDirPath);
        fsExtra.mkdirSync(WETDirPath);
        workers = 5;
        const WETUrls = WETPaths.split('\n').map((directortPath) => `${WET_PATH_PREFIX}${directortPath}`);
        fetch(WETUrls, batch * WET_FILES_PER_BATCH + 0, batch);
        fetch(WETUrls, batch * WET_FILES_PER_BATCH + 1, batch);
        fetch(WETUrls, batch * WET_FILES_PER_BATCH + 2, batch);
        fetch(WETUrls, batch * WET_FILES_PER_BATCH + 3, batch);
        fetch(WETUrls, batch * WET_FILES_PER_BATCH + 4, batch);
      },
    }));

  return new Promise((resolve, reject) => {
    fetchComplete = resolve;
    fetchError = reject;
  });
};

export default {
  fetchWETFiles,
};
