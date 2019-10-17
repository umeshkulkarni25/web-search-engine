// import { exec } from 'child_process';
// import fs from 'fs';
// import path from 'path';
// import fsExtra from 'fs-extra';
// import _ from 'lodash';
// import chokidar from 'chokidar';

// const { TEMP_FILES_FOLDER, SORTED_TOKENS_DIR } = process.env;
// const tokenFileDir = path.join(process.cwd(), TEMP_FILES_FOLDER);
// const sortedTokenFileDir = path.join(process.cwd(), SORTED_TOKENS_DIR);

// const sort = (files, index) => {
//   const file = files[index];
//   if (!file) {
//     console.log('all files sorted');
//     return;
//   }
//   const sortCommand = `sort -k1,1n ${path.join(tokenFileDir, file)} > ${path.join(sortedTokenFileDir, file)}`;
//   exec(sortCommand, (err) => {
//     if (err) {
//       console.log(err);
//       process.exit();
//     }
//     fs.unlinkSync(path.join(tokenFileDir, file));
//     sort(files, index + 1);
//   });
// };

// const watch = () => {
//   if (fs.existsSync(sortedTokenFileDir)) {
//     fsExtra.rmdirSync(sortedTokenFileDir);
//   }
//   fsExtra.mkdirSync(sortedTokenFileDir);

//   const watcher = chokidar.watch(tokenFileDir, {
//     persistent: true,
//   });
//   watcher
//     .on('add', (filePath) => console.log(`File ${filePath} has been added`));
// };

// export default {
//   watch,
// };
