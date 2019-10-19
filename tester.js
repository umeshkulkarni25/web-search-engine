const fs = require('fs');

const fd = fs.openSync('./temp/batch_1');
let offset = 0;
const content = Buffer.alloc(50);
const position = 0;
while (true) {
  const bytesRead = fs.readSync(fd, content, 0, 50, position);
  if (bytesRead === 0) {
    break;
  }
  offset = 0;
  while (offset < bytesRead) {
    console.log(content.readUInt32BE(offset + 0), content.readUInt32BE(offset + 4), content.readUInt16BE(offset + 8));
    offset += 10;
  }
}


// while (true) {
//   if (!fs.readSync(fd, content, 0, 8, null)) {
//     break;
//   }
//   console.log(content.readUInt32BE(0), content.readUInt32BE(4));
//   const length = content.readUInt32BE(4);
//   const buff = Buffer.alloc(length);
//   fs.readSync(fd, buff, 0, length, null);
//   // console.log(buff.readUInt32BE(0, 6), buff.readUInt16BE(4, 6));
//   // console.log(buff.readUInt32BE(6, 10), buff.readUInt16BE(10, 12));
//   // console.log(buff.readUInt32BE(12), buff.readUInt16BE(16));
//   // console.log(buff.readUInt32BE(18), buff.readUInt16BE(28));
//   break;
// }

// // fs.readSync(fd, content, 0, 8, ((position + content.readUInt32BE(4)) * 6));
// // console.log(content.readUInt32BE(0), content.readUInt32BE(4));
