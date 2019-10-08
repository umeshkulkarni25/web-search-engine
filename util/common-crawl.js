import zlib from 'zlib';
import { Transform } from 'stream';
import axios from 'axios';
import fs from 'fs';
import path from 'path';

const {
  WET_PATH_URL, WET_PATH_PREFIX, PROJECT_FOLDER, WET_FILES_FOLDER,
} = process.env;
const WETDirPath = path.join('..', PROJECT_FOLDER, WET_FILES_FOLDER);

const fetch = (WETUrls, index) => {
  if (index === WETUrls.length) {
    console.log('fetch complete');
    return;
  }
  const WETUrl = WETUrls[index];
  if (WETUrl === WET_PATH_PREFIX) {
    fetch(WETUrls, ++index);
  }
  axios({ url: WETUrl, responseType: 'stream' }).then((response) => {
    const compressedFileName = WETUrl.split('/').pop();
    const unCompressedFileName = compressedFileName.substring(0, compressedFileName.lastIndexOf('.'));

    response.data
      .pipe(zlib.createGunzip())
      .pipe(fs.createWriteStream(path.join(WETDirPath, unCompressedFileName)))
      .on('finish', () => { fetch(WETUrls, ++index); });
  });
};

const fetchWETFiles = async () => {
  let WETUrls = [];
  const { data } = await axios({ url: WET_PATH_URL, responseType: 'stream' });
  data
    .pipe(zlib.createGunzip())
    .pipe(new Transform({
      objectMode: true,
      transform: (chunk, encoding, done) => {
        const WETUrlsFromChunk = chunk.toString().split('\n').map((directortPath) => `${WET_PATH_PREFIX}${directortPath}`);
        WETUrls = WETUrls.concat(WETUrlsFromChunk);
        done();
      },
      flush() {
        fs.mkdirSync('WET_Files');
        fetch(WETUrls.splice(0, 25), 0);
        fetch(WETUrls.splice(0, 25), 0);
        fetch(WETUrls.splice(0, 25), 0);
        fetch(WETUrls.splice(0, 25), 0);
      },
    }));
};

export default {
  fetchWETFiles,
};
