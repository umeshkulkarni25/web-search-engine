const fs = require('fs');

const fd = fs.openSync('./temp/batch_1');
let offset = 0;
const content = Buffer.alloc(50);
fs.readSync(fd, content, 0, 50, null);
while (offset < content.length) {
  console.log(content.readUInt32BE(offset + 0), content.readUInt32BE(offset + 4), content.readUInt16BE(offset + 8));
  offset += 10;
}
fs.readSync(fd, content, 0, 50, null);
while (offset < content.length) {
  console.log(content.readUInt32BE(offset + 0), content.readUInt32BE(offset + 4), content.readUInt16BE(offset + 8));
  offset += 10;
}
