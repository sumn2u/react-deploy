# react-deploy

[![GitHub issues](https://img.shields.io/github/issues/sumn2u/react-deploy.svg)](https://github.com/sumn2u/react-deploy/issues) [![GitHub forks](https://img.shields.io/github/forks/sumn2u/react-deploy.svg)](https://github.com/sumn2u/react-deploy/network) [![GitHub stars](https://img.shields.io/github/stars/sumn2u/react-deploy.svg)](https://github.com/sumn2u/react-deploy/stargazers) [![GitHub license](https://img.shields.io/github/license/sumn2u/react-deploy.svg)](https://github.com/sumn2u/react-deploy/blob/master/LICENSE) [![Build Status](https://travis-ci.org/sumn2u/react-deploy.svg?branch=master)](https://travis-ci.org/sumn2u/react-deploy) [![Twitter](https://img.shields.io/twitter/url/https/github.com/sumn2u/react-deploy.svg?style=social)](https://twitter.com/intent/tweet?text=Wow:&url=https%3A%2F%2Fgithub.com%2Fsumn2u%2Freact-deploy)

> Create React App deployment to S3 bucket along with app versioning and activation features.

![upload revisions](img/display-revisions.png)

> Activate any revisions

![show revisions](img/showrevisions.png)


This package doesn't build the app,use webpack or create-react-app to build the application.

## Table of Contents

- [Install](#install)
- [Usage](#usage)
- [Contribute](#contribute)
- [License](#license)

## Install

```sh
npm i react-deploy -S

# or

yarn add react-deploy

```

## Usage
> task.js
```
/*
 * Minimalistic script runner. Usage example:
 *
 *     node tools/deploy.js
 *     Starting 'deploy'...
 *     Starting 'build'...
 *     Finished 'build' in 3212ms
 *     Finished 'deploy' in 582ms
 */

 function run (task, action, ...args) {
   const command = process.argv[2]
   const taskName = command && !command.startsWith('-') ? `${task}:${command}` : task
   const start = new Date()
   process.stdout.write(`Starting '${taskName}'...\n`)
   return Promise.resolve().then(() => action(...args)).then(() => {
     process.stdout.write(`Finished '${taskName}' after ${new Date().getTime() - start.getTime()}ms\n`)
   }, err => process.stderr.write(`${err.stack}\n`))
 }

 process.nextTick(() => require.main.exports())
 module.exports = (task, action) => run.bind(undefined, task, action)
```

> deploy.js
```
const s3 = require('react-deploy')
const task = require('./task')

module.exports = task('upload', () => Promise.resolve()
  .then(() => {
    Uploader
  })
)
const Uploader = new Promise((resolve, reject) => {
  const client = s3.createClient({
  s3Options: {
      accessKeyId: 'AWS_KEY',
      secretAccessKey: 'AWS_SECRET_ACCESSKEY',
      region: 'REGION',
      sslEnabled: true,
      Bucket:'BUCKETNAME'
    },
  })
  const uploader = client.uploadDir({
    localDir: 'DISTRIBUTIONFOLDER', //dist
    deleteRemoved: false,
    s3Params: {
      Bucket: 'BUCKETNAME'
    },
  })

  // on deploy  create a finger print
    client.createRevision()

  // display revisions
   client.displayRevisions()

  // activate the new value
   client.activateRevisions('index:a00a13d')

   uploader.on('error', reject)
   uploader.on('end', resolve)
})

```


## Contribute

Contributors are welcome.

Small note: If editing the README, please conform to the [standard-readme](https://github.com/RichardLitt/standard-readme) specification.

## License

MIT © sumn2u
