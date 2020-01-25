const s3 = require('s3');
const path = require('path');
const task = require('./commands/task')

const deployFile = require('./../config/deploy')

console.log(deployFile(),'test');

module.exports = task('upload', () => Promise.resolve()
  .then(() => Uploader)
);
const Uploader = new Promise((resolve, reject) => {
  
})
