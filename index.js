let AWS = require('aws-sdk')
let EventEmitter = require('events').EventEmitter
let fs = require('graceful-fs')
let url = require('url')
let rimraf = require('rimraf')
let findit = require('findit2')
let Pend = require('pend')
let path = require('path')
// let crypto = require('crypto')
let mkdirp = require('mkdirp')
let assert = require('assert')
let MultipartETag = require('./lib/multipart_etag')
let fdSlicer = require('fd-slicer')
let mime = require('mime')
let StreamSink = require('streamsink')
let PassThrough = require('stream').PassThrough
let ScmTable = require('./lib/scm-table')
const generate = require('./lib/git-commit')
const RSVP = require('rsvp')

let MAX_PUTOBJECT_SIZE = 5 * 1024 * 1024 * 1024
let MAX_DELETE_COUNT = 1000
let MAX_MULTIPART_COUNT = 10000
let MIN_MULTIPART_SIZE = 5 * 1024 * 1024

let TO_UNIX_RE = new RegExp(quotemeta(path.sep), 'g')

exports.createClient = function (options) {
  return new Client(options)
}

exports.getPublicUrl = getPublicUrl
exports.getPublicUrlHttp = getPublicUrlHttp

exports.Client = Client
exports.MultipartETag = MultipartETag
exports.AWS = AWS

exports.MAX_PUTOBJECT_SIZE = MAX_PUTOBJECT_SIZE
exports.MAX_DELETE_COUNT = MAX_DELETE_COUNT
exports.MAX_MULTIPART_COUNT = MAX_MULTIPART_COUNT
exports.MIN_MULTIPART_SIZE = MIN_MULTIPART_SIZE

function Client (options) {
  options = options || {}
  this.s3 = options.s3Client || new AWS.S3(options.s3Options)
  this.s3Pend = new Pend()
  this.s3Pend.max = options.maxAsyncS3 || 20
  this.s3RetryCount = options.s3RetryCount || 3
  this.s3RetryDelay = options.s3RetryDelay || 1000
  this.multipartUploadThreshold = options.multipartUploadThreshold || (20 * 1024 * 1024)
  this.multipartUploadSize = options.multipartUploadSize || (15 * 1024 * 1024)
  this.multipartDownloadThreshold = options.multipartDownloadThreshold || (20 * 1024 * 1024)
  this.multipartDownloadSize = options.multipartDownloadSize || (15 * 1024 * 1024)

  if (this.multipartUploadThreshold < MIN_MULTIPART_SIZE) {
    throw new Error('Minimum multipartUploadThreshold is 5MB.')
  }
  if (this.multipartUploadThreshold > MAX_PUTOBJECT_SIZE) {
    throw new Error('Maximum multipartUploadThreshold is 5GB.')
  }
  if (this.multipartUploadSize < MIN_MULTIPART_SIZE) {
    throw new Error('Minimum multipartUploadSize is 5MB.')
  }
  if (this.multipartUploadSize > MAX_PUTOBJECT_SIZE) {
    throw new Error('Maximum multipartUploadSize is 5GB.')
  }
}

Client.prototype.deleteObjects = function (s3Params) {
  let self = this
  let ee = new EventEmitter()

  let params = {
    Bucket: s3Params.Bucket,
    Delete: extend({}, s3Params.Delete),
    MFA: s3Params.MFA
  }
  let slices = chunkArray(params.Delete.Objects, MAX_DELETE_COUNT)
  // let errorOccurred = false
  let pend = new Pend()

  ee.progressAmount = 0
  ee.progressTotal = params.Delete.Objects.length

  slices.forEach(uploadSlice)
  pend.wait(function (err) {
    if (err) {
      ee.emit('error', err)
      return
    }
    ee.emit('end')
  })
  return ee

  function uploadSlice (slice) {
    pend.go(function (cb) {
      doWithRetry(tryDeletingObjects, self.s3RetryCount, self.s3RetryDelay, function (err, data) {
        if (err) {
          cb(err)
        } else {
          ee.progressAmount += slice.length
          ee.emit('progress')
          ee.emit('data', data)
          cb()
        }
      })
    })

    function tryDeletingObjects (cb) {
      self.s3Pend.go(function (pendCb) {
        params.Delete.Objects = slice
        self.s3.deleteObjects(params, function (err, data) {
          pendCb()
          cb(err, data)
        })
      })
    }
  }
}

// display revisons
Client.prototype.displayRevisions = function () {
  let self = this
  let params = {
    Bucket: self.s3.config.Bucket,
    Prefix: 'index:',
    MaxKeys: 10
  }

  let revisions = new RSVP.Promise((resolve, reject) => {
    let s3revisionL = []
    // console.log(s3, 's3');

    return self.s3.listObjects(params, (err, data) => {
      if (err) {
        reject(err)
      }
      // needs to get files from s3
      data.Contents.forEach((obj) => {
        s3revisionL.push({revisionKey: obj.Key.replace(/.html/g, ''), revisionDate: obj.LastModified})
        // console.log(obj.Key, obj.LastModified, 'this is s3')
      })
      resolve(s3revisionL)
    })
  })

  revisions.then((revisions) => {
    const table = new ScmTable(this, revisions)
    table.display()
  })
}

// activate revisions

// upload revisions files
Client.prototype.createRevision = function () {
  generate().then((res) => {
    let self = this
    let newParams = {
      Bucket: self.s3.config.Bucket,
      CopySource: `${self.s3.config.Bucket}/index.html`,
      ContentType: 'text/html',
      ACL: 'public-read-write',
      Metadata: {
        'revision': res.revisionKey,
        'updated': (new Date()).toDateString()
      },
      MetadataDirective: 'COPY',
      Key: `index:${res.revisionKey}.html`
    }

    // console.log(newParams,'newParams');
    const uploadFile = new Promise((resolve, reject) => {
      self.s3.copyObject(newParams, (copyErr, copyData) => {
        if (copyErr) {
          reject(copyErr)
        } else {
          resolve(copyData)
        }
      })
    })

    uploadFile.then((rest) => {
      console.log(`Revision created successfully `)
    })
  })
}

//create revision for service worker
Client.prototype.serviceWorker = function () {
  generate().then((res) => {
    let self = this
    let newParams = {
      Bucket: self.s3.config.Bucket,
      CopySource: `${self.s3.config.Bucket}/service-worker.js`,
      ContentType: 'application/javascript',
      ACL: 'public-read-write',
      Metadata: {
        'revision': res.revisionKey,
        'updated': (new Date()).toDateString()
      },
      MetadataDirective: 'COPY',
      Key: `service-worker:${res.revisionKey}.js`
    }

    // console.log(newParams,'newParams');
    const uploadFile = new Promise((resolve, reject) => {
      self.s3.copyObject(newParams, (copyErr, copyData) => {
        if (copyErr) {
          reject(copyErr)
        } else {
          resolve(copyData)
        }
      })
    })

    uploadFile.then((rest) => {
      console.log(`Revision for service worker created successfully `)
    })
  })
}

// activate revision
Client.prototype.activateServiceWorkerRevisions = function (activate) {
  let self = this
  let params = {
    Bucket: self.s3.config.Bucket,
    Prefix: `${activate}.js`,
    MaxKeys: 1
  }

  if (!activate) return console.log('Please provide activation key')
  let actKey = activate.replace(/service-worker:/g, '')

  console.log(`Activating service-worker file of key ${actKey}`)

  let revisions = new RSVP.Promise((resolve, reject) => {
    return self.s3.listObjects(params, (err, data) => {
      if (err) {
        reject(err)
      }
      // needs to get files from s3
      if (data.Contents.length) {
        // let file = data.Contents[0]
        let newParams = {
          Bucket: self.s3.config.Bucket,
          CopySource: `${self.s3.config.Bucket}/${activate}.js`,
          ContentType: 'application/javascript',
          ACL: 'public-read-write',
          Metadata: {
            'revision': actKey,
            'updated': (new Date()).toDateString()
          },
          MetadataDirective: 'REPLACE',
          Key: 'service-worker.js'
        }

        self.s3.copyObject(newParams, (copyErr, copyData) => {
          if (copyErr) {
          //  console.log(copyErr)
            reject(copyErr)
          } else {
                // console.log('Copied: ', params.Key)
            resolve(copyData)
          }
        })
      }
    })
  })
// now see the promise
  revisions.then((revision) => {
    console.log(`Revision activated successfully`)
  //  let revisionKey  = revision.revisionKey.replace(/index:/g,'').replace(/.html:/g,'')
  //  if(revision == actKey){
  //    //@TODO save and activate
  //  }else{
  //    console.log(`cannot find file of key ${actKey}`)
  //  }
  })
}

// activate revision
Client.prototype.activateRevisions = function (activate) {
  let self = this
  let params = {
    Bucket: self.s3.config.Bucket,
    Prefix: `${activate}.html`,
    MaxKeys: 1
  }

  if (!activate) return console.log('Please provide activation key')
  let actKey = activate.replace(/index:/g, '')

  console.log(`Activating index file of key ${actKey}`)

  let revisions = new RSVP.Promise((resolve, reject) => {
    return self.s3.listObjects(params, (err, data) => {
      if (err) {
        reject(err)
      }
      // needs to get files from s3
      if (data.Contents.length) {
        // let file = data.Contents[0]
        // console.log('====================================');
        // console.log("data contains length");
        // console.log('====================================');
        let newParams = {
          Bucket: self.s3.config.Bucket,
          CopySource: `${self.s3.config.Bucket}/${activate}.html`,
          ContentType: 'text/html',
          ACL: 'public-read-write',
          Metadata: {
            'revision': actKey,
            'updated': (new Date()).toDateString()
          },
          MetadataDirective: 'REPLACE',
          Key: 'index.html'
        }

        self.s3.copyObject(newParams, (copyErr, copyData) => {
          if (copyErr) {
          //  console.log(copyErr)
            reject(copyErr)
          } else {
                // console.log('Copied: ', params.Key)
            resolve(copyData)
          }
        })
      }
    })
  })
// now see the promise
  revisions.then((revision) => {
    console.log(`Revision activated successfully`)
  //  let revisionKey  = revision.revisionKey.replace(/index:/g,'').replace(/.html:/g,'')
  //  if(revision == actKey){
  //    //@TODO save and activate
  //  }else{
  //    console.log(`cannot find file of key ${actKey}`)
  //  }
  })
}
Client.prototype.uploadFile = function (params) {
  let self = this
  let uploader = new EventEmitter()
  uploader.progressMd5Amount = 0
  uploader.progressAmount = 0
  uploader.progressTotal = 0
  uploader.abort = handleAbort
  uploader.getPublicUrl = function () {
    return getPublicUrl(s3Params.Bucket, s3Params.Key, self.s3.config.region, self.s3.config.endpoint)
  }
  uploader.getPublicUrlHttp = function () {
    return getPublicUrlHttp(s3Params.Bucket, s3Params.Key, self.s3.config.endpoint)
  }

  let localFile = params.localFile
  let localFileStat = null
  let s3Params = extend({}, params.s3Params)
  if (s3Params.ContentType === undefined) {
    let defaultContentType = params.defaultContentType || 'application/octet-stream'
    s3Params.ContentType = mime.lookup(localFile, defaultContentType)
  }
  let fatalError = false
  let localFileSlicer = null
  let parts = []

  openFile()

  return uploader

  function handleError (err) {
    if (localFileSlicer) {
      localFileSlicer.unref()
      localFileSlicer = null
    }
    if (fatalError) return
    fatalError = true
    uploader.emit('error', err)
  }

  function handleAbort () {
    fatalError = true
  }

  function openFile () {
    fs.open(localFile, 'r', function (err, fd) {
      if (err) return handleError(err)
      localFileSlicer = fdSlicer.createFromFd(fd, {autoClose: true})
      localFileSlicer.on('error', handleError)
      localFileSlicer.on('close', function () {
        uploader.emit('fileClosed')
      })

      // keep an extra reference alive until we decide that we're completely
      // done with the file
      localFileSlicer.ref()

      uploader.emit('fileOpened', localFileSlicer)

      fs.fstat(fd, function (err, stat) {
        if (err) return handleError(err)
        localFileStat = stat
        uploader.progressTotal = stat.size
        startPuttingObject()
      })
    })
  }

  function startPuttingObject () {
    if (localFileStat.size >= self.multipartUploadThreshold) {
      let multipartUploadSize = self.multipartUploadSize
      let partsRequiredCount = Math.ceil(localFileStat.size / multipartUploadSize)
      if (partsRequiredCount > MAX_MULTIPART_COUNT) {
        multipartUploadSize = smallestPartSizeFromFileSize(localFileStat.size)
      }
      if (multipartUploadSize > MAX_PUTOBJECT_SIZE) {
        let err = new Error('File size exceeds maximum object size: ' + localFile)
        err.retryable = false
        handleError(err)
        return
      }
      startMultipartUpload(multipartUploadSize)
    } else {
      doWithRetry(tryPuttingObject, self.s3RetryCount, self.s3RetryDelay, onPutObjectDone)
    }

    function onPutObjectDone (err, data) {
      if (fatalError) return
      if (err) return handleError(err)
      if (localFileSlicer) {
        localFileSlicer.unref()
        localFileSlicer = null
      }
      uploader.emit('end', data)
    }
  }

  function startMultipartUpload (multipartUploadSize) {
    doWithRetry(tryCreateMultipartUpload, self.s3RetryCount, self.s3RetryDelay, function (err, data) {
      if (fatalError) return
      if (err) return handleError(err)
      uploader.emit('data', data)
      s3Params = {
        Bucket: s3Params.Bucket,
        Key: encodeSpecialCharacters(s3Params.Key),
        SSECustomerAlgorithm: s3Params.SSECustomerAlgorithm,
        SSECustomerKey: s3Params.SSECustomerKey,
        SSECustomerKeyMD5: s3Params.SSECustomerKeyMD5
      }
      queueAllParts(data.UploadId, multipartUploadSize)
    })
  }

  function queueAllParts (uploadId, multipartUploadSize) {
    let cursor = 0
    let nextPartNumber = 1
    let pend = new Pend()
    while (cursor < localFileStat.size) {
      let start = cursor
      let end = cursor + multipartUploadSize
      if (end > localFileStat.size) {
        end = localFileStat.size
      }
      cursor = end
      let part = {
        ETag: null,
        PartNumber: nextPartNumber++
      }
      parts.push(part)
      pend.go(makeUploadPartFn(start, end, part, uploadId))
    }
    pend.wait(function (err) {
      if (fatalError) return
      if (err) return handleError(err)
      completeMultipartUpload()
    })
  }

  function makeUploadPartFn (start, end, part, uploadId) {
    return function (cb) {
      doWithRetry(tryUploadPart, self.s3RetryCount, self.s3RetryDelay, function (err, data) {
        if (fatalError) return
        if (err) return handleError(err)
        uploader.emit('part', data)
        cb()
      })
    }

    function tryUploadPart (cb) {
      if (fatalError) return
      self.s3Pend.go(function (pendCb) {
        if (fatalError) {
          pendCb()
          return
        }
        let inStream = localFileSlicer.createReadStream({start: start, end: end})
        let errorOccurred = false
        inStream.on('error', function (err) {
          if (fatalError || errorOccurred) return
          handleError(err)
        })
        s3Params.ContentLength = end - start
        s3Params.PartNumber = part.PartNumber
        s3Params.UploadId = uploadId

        let multipartETag = new MultipartETag({size: s3Params.ContentLength, count: 1})
        let prevBytes = 0
        let overallDelta = 0
        let pend = new Pend()
        let haveETag = pend.hold()
        multipartETag.on('progress', function () {
          if (fatalError || errorOccurred) return
          let delta = multipartETag.bytes - prevBytes
          prevBytes = multipartETag.bytes
          uploader.progressAmount += delta
          overallDelta += delta
          uploader.emit('progress')
        })
        multipartETag.on('end', function () {
          if (fatalError || errorOccurred) return
          let delta = multipartETag.bytes - prevBytes
          uploader.progressAmount += delta
          uploader.progressTotal += (end - start) - multipartETag.bytes
          uploader.emit('progress')
          haveETag()
        })
        inStream.pipe(multipartETag)
        s3Params.Body = multipartETag

        self.s3.uploadPart(extend({}, s3Params), function (err, data) {
          pendCb()
          if (fatalError || errorOccurred) return
          if (err) {
            errorOccurred = true
            uploader.progressAmount -= overallDelta
            cb(err)
            return
          }
          pend.wait(function () {
            if (fatalError) return
            if (!compareMultipartETag(data.ETag, multipartETag)) {
              errorOccurred = true
              uploader.progressAmount -= overallDelta
              cb(new Error('ETag does not match MD5 checksum'))
              return
            }
            part.ETag = data.ETag
            cb(null, data)
          })
        })
      })
    }
  }

  function completeMultipartUpload () {
    localFileSlicer.unref()
    localFileSlicer = null
    doWithRetry(tryCompleteMultipartUpload, self.s3RetryCount, self.s3RetryDelay, function (err, data) {
      if (fatalError) return
      if (err) return handleError(err)
      uploader.emit('end', data)
    })
  }

  function tryCompleteMultipartUpload (cb) {
    if (fatalError) return
    self.s3Pend.go(function (pendCb) {
      if (fatalError) {
        pendCb()
        return
      }
      s3Params = {
        Bucket: s3Params.Bucket,
        Key: s3Params.Key,
        UploadId: s3Params.UploadId,
        MultipartUpload: {
          Parts: parts
        }
      }
      self.s3.completeMultipartUpload(s3Params, function (err, data) {
        pendCb()
        if (fatalError) return
        cb(err, data)
      })
    })
  }

  function tryCreateMultipartUpload (cb) {
    if (fatalError) return
    self.s3Pend.go(function (pendCb) {
      if (fatalError) return pendCb()
      self.s3.createMultipartUpload(s3Params, function (err, data) {
        pendCb()
        if (fatalError) return
        cb(err, data)
      })
    })
  }

  function tryPuttingObject (cb) {
    self.s3Pend.go(function (pendCb) {
      if (fatalError) return pendCb()
      let inStream = localFileSlicer.createReadStream()
      inStream.on('error', handleError)
      let pend = new Pend()
      let multipartETag = new MultipartETag({size: localFileStat.size, count: 1})
      pend.go(function (cb) {
        multipartETag.on('end', function () {
          if (fatalError) return
          uploader.progressAmount = multipartETag.bytes
          uploader.progressTotal = multipartETag.bytes
          uploader.emit('progress')
          localFileStat.size = multipartETag.bytes
          localFileStat.multipartETag = multipartETag
          cb()
        })
      })
      multipartETag.on('progress', function () {
        if (fatalError) return
        uploader.progressAmount = multipartETag.bytes
        uploader.emit('progress')
      })
      s3Params.ContentLength = localFileStat.size
      uploader.progressAmount = 0
      inStream.pipe(multipartETag)
      s3Params.Body = multipartETag

      self.s3.putObject(s3Params, function (err, data) {
        pendCb()
        if (fatalError) return
        if (err) {
          cb(err)
          return
        }
        pend.wait(function () {
          if (fatalError) return
          if (!compareMultipartETag(data.ETag, localFileStat.multipartETag)) {
            cb(new Error('ETag does not match MD5 checksum'))
            return
          }
          cb(null, data)
        })
      })
    })
  }
}

Client.prototype.downloadFile = function (params) {
  let self = this
  let downloader = new EventEmitter()
  let localFile = params.localFile
  let s3Params = extend({}, params.s3Params)

  let dirPath = path.dirname(localFile)
  downloader.progressAmount = 0
  mkdirp(dirPath, function (err) {
    if (err) {
      downloader.emit('error', err)
      return
    }

    doWithRetry(doDownloadWithPend, self.s3RetryCount, self.s3RetryDelay, function (err) {
      if (err) {
        downloader.emit('error', err)
        return
      }
      downloader.emit('end')
    })
  })

  return downloader

  function doDownloadWithPend (cb) {
    self.s3Pend.go(function (pendCb) {
      doTheDownload(function (err) {
        pendCb()
        cb(err)
      })
    })
  }

  function doTheDownload (cb) {
    let request = self.s3.getObject(s3Params)
    let errorOccurred = false
    let hashCheckPend = new Pend()

    request.on('httpHeaders', function (statusCode, headers, resp) {
      if (statusCode >= 300) {
        handleError(new Error('http status code ' + statusCode))
        return
      }
      if (headers['content-length'] === undefined) {
        let outStream = fs.createWriteStream(localFile)
        outStream.on('error', handleError)
        downloader.progressTotal = 0
        downloader.progressAmount = -1
        request.on('httpData', function (chunk) {
          downloader.progressTotal += chunk.length
          downloader.progressAmount += chunk.length
          downloader.emit('progress')
          outStream.write(chunk)
        })

        request.on('httpDone', function () {
          if (errorOccurred) return
          downloader.progressAmount += 1
          downloader.emit('progress')
          outStream.end()
          cb()
        })
      } else {
        let contentLength = parseInt(headers['content-length'], 10)
        downloader.progressTotal = contentLength
        downloader.progressAmount = 0
        downloader.emit('progress')
        downloader.emit('httpHeaders', statusCode, headers, resp)
        let eTag = cleanETag(headers.etag)
        let eTagCount = getETagCount(eTag)

        let outStream = fs.createWriteStream(localFile)
        let multipartETag = new MultipartETag({size: contentLength, count: eTagCount})
        let httpStream = resp.httpResponse.createUnbufferedStream()

        httpStream.on('error', handleError)
        outStream.on('error', handleError)

        hashCheckPend.go(function (cb) {
          multipartETag.on('end', function () {
            if (multipartETag.bytes !== contentLength) {
              handleError(new Error('Downloaded size does not match Content-Length'))
              return
            }
            if (eTagCount === 1 && !multipartETag.anyMatch(eTag)) {
              handleError(new Error('ETag does not match MD5 checksum'))
              return
            }
            cb()
          })
        })
        multipartETag.on('progress', function () {
          downloader.progressAmount = multipartETag.bytes
          downloader.emit('progress')
        })
        outStream.on('close', function () {
          if (errorOccurred) return
          hashCheckPend.wait(cb)
        })

        httpStream.pipe(multipartETag)
        httpStream.pipe(outStream)
        multipartETag.resume()
      }
    })

    request.send(handleError)

    function handleError (err) {
      if (!err) return
      if (errorOccurred) return
      errorOccurred = true
      cb(err)
    }
  }
}

/* params:
 *  - recursive: false
 *  - s3Params:
 *    - Bucket: params.s3Params.Bucket,
 *    - Delimiter: null,
 *    - Marker: null,
 *    - MaxKeys: null,
 *    - Prefix: prefix,
 */
Client.prototype.listObjects = function (params) {
  let self = this
  let ee = new EventEmitter()
  let s3Details = extend({}, params.s3Params)
  let recursive = !!params.recursive
  let abort = false

  ee.progressAmount = 0
  ee.objectsFound = 0
  ee.dirsFound = 0
  findAllS3Objects(s3Details.Marker, s3Details.Prefix, function (err, data) {
    if (err) {
      ee.emit('error', err)
      return
    }
    ee.emit('end')
  })

  ee.abort = function () {
    abort = true
  }

  return ee

  function findAllS3Objects (marker, prefix, cb) {
    if (abort) return
    doWithRetry(listObjects, self.s3RetryCount, self.s3RetryDelay, function (err, data) {
      if (abort) return
      if (err) return cb(err)

      ee.progressAmount += 1
      ee.objectsFound += data.Contents.length
      ee.dirsFound += data.CommonPrefixes.length
      ee.emit('progress')
      ee.emit('data', data)

      let pend = new Pend()

      if (recursive) {
        data.CommonPrefixes.forEach(recurse)
        data.CommonPrefixes = []
      }

      if (data.IsTruncated) {
        pend.go(findNext1000)
      }

      pend.wait(function (err) {
        cb(err)
      })

      function findNext1000 (cb) {
        let nextMarker = data.NextMarker || data.Contents[data.Contents.length - 1].Key
        findAllS3Objects(nextMarker, prefix, cb)
      }

      function recurse (dirObj) {
        let prefix = dirObj.Prefix
        pend.go(function (cb) {
          findAllS3Objects(null, prefix, cb)
        })
      }
    })

    function listObjects (cb) {
      if (abort) return
      self.s3Pend.go(function (pendCb) {
        if (abort) {
          pendCb()
          return
        }
        s3Details.Marker = marker
        s3Details.Prefix = prefix
        self.s3.listObjects(s3Details, function (err, data) {
          pendCb()
          if (abort) return
          cb(err, data)
        })
      })
    }
  }
}

/* params:
 * - deleteRemoved - delete s3 objects with no corresponding local file. default false
 * - localDir - path on local file system to sync
 * - s3Params:
 *   - Bucket (required)
 *   - Key (required)
 */
Client.prototype.uploadDir = function (params) {
  return syncDir(this, params, true)
}

Client.prototype.downloadDir = function (params) {
  return syncDir(this, params, false)
}

Client.prototype.deleteDir = function (s3Params) {
  let self = this
  let ee = new EventEmitter()
  let bucket = s3Params.Bucket
  let mfa = s3Params.MFA
  let listObjectsParams = {
    recursive: true,
    s3Params: {
      Bucket: bucket,
      Prefix: s3Params.Prefix
    }
  }
  let finder = self.listObjects(listObjectsParams)
  let pend = new Pend()
  ee.progressAmount = 0
  ee.progressTotal = 0
  finder.on('error', function (err) {
    ee.emit('error', err)
  })
  finder.on('data', function (objects) {
    ee.progressTotal += objects.Contents.length
    ee.emit('progress')
    if (objects.Contents.length > 0) {
      pend.go(deleteThem)
    }

    function deleteThem (cb) {
      let params = {
        Bucket: bucket,
        Delete: {
          Objects: objects.Contents.map(keyOnly),
          Quiet: true
        },
        MFA: mfa
      }
      let deleter = self.deleteObjects(params)
      deleter.on('error', function (err) {
        finder.abort()
        ee.emit('error', err)
      })
      deleter.on('end', function () {
        ee.progressAmount += objects.Contents.length
        ee.emit('progress')
        cb()
      })
    }
  })
  finder.on('end', function () {
    pend.wait(function () {
      ee.emit('end')
    })
  })
  return ee
}

Client.prototype.copyObject = function (_s3Params) {
  let self = this
  let ee = new EventEmitter()
  let s3Params = extend({}, _s3Params)
  delete s3Params.MFA
  doWithRetry(doCopyWithPend, self.s3RetryCount, self.s3RetryDelay, function (err, data) {
    if (err) {
      ee.emit('error', err)
    } else {
      ee.emit('end', data)
    }
  })
  function doCopyWithPend (cb) {
    self.s3Pend.go(function (pendCb) {
      doTheCopy(function (err, data) {
        pendCb()
        cb(err, data)
      })
    })
  }
  function doTheCopy (cb) {
    self.s3.copyObject(s3Params, cb)
  }
  return ee
}

Client.prototype.moveObject = function (s3Params) {
  let self = this
  let ee = new EventEmitter()
  let copier = self.copyObject(s3Params)
  let copySource = s3Params.CopySource
  let mfa = s3Params.MFA
  copier.on('error', function (err) {
    ee.emit('error', err)
  })
  copier.on('end', function (data) {
    ee.emit('copySuccess', data)
    let slashIndex = copySource.indexOf('/')
    let sourceBucket = copySource.substring(0, slashIndex)
    let sourceKey = copySource.substring(slashIndex + 1)
    let deleteS3Params = {
      Bucket: sourceBucket,
      Delete: {
        Objects: [
          {
            Key: sourceKey
          }
        ],
        Quiet: true
      },
      MFA: mfa
    }
    let deleter = self.deleteObjects(deleteS3Params)
    deleter.on('error', function (err) {
      ee.emit('error', err)
    })
    let deleteData
    deleter.on('data', function (data) {
      deleteData = data
    })
    deleter.on('end', function () {
      ee.emit('end', deleteData)
    })
  })
  return ee
}

Client.prototype.downloadBuffer = function (s3Params) {
  let self = this
  let downloader = new EventEmitter()
  s3Params = extend({}, s3Params)

  downloader.progressAmount = 0

  doWithRetry(doDownloadWithPend, self.s3RetryCount, self.s3RetryDelay, function (err, buffer) {
    if (err) {
      downloader.emit('error', err)
      return
    }
    downloader.emit('end', buffer)
  })

  return downloader

  function doDownloadWithPend (cb) {
    self.s3Pend.go(function (pendCb) {
      doTheDownload(function (err, buffer) {
        pendCb()
        cb(err, buffer)
      })
    })
  }

  function doTheDownload (cb) {
    let errorOccurred = false
    let request = self.s3.getObject(s3Params)
    let hashCheckPend = new Pend()
    request.on('httpHeaders', function (statusCode, headers, resp) {
      if (statusCode >= 300) {
        handleError(new Error('http status code ' + statusCode))
        return
      }
      let contentLength = parseInt(headers['content-length'], 10)
      downloader.progressTotal = contentLength
      downloader.progressAmount = 0
      downloader.emit('progress')
      downloader.emit('httpHeaders', statusCode, headers, resp)
      let eTag = cleanETag(headers.etag)
      let eTagCount = getETagCount(eTag)

      let outStream = new StreamSink()
      let multipartETag = new MultipartETag({size: contentLength, count: eTagCount})
      let httpStream = resp.httpResponse.createUnbufferedStream()

      httpStream.on('error', handleError)
      outStream.on('error', handleError)

      hashCheckPend.go(function (cb) {
        multipartETag.on('end', function () {
          if (multipartETag.bytes !== contentLength) {
            handleError(new Error('Downloaded size does not match Content-Length'))
            return
          }
          if (eTagCount === 1 && !multipartETag.anyMatch(eTag)) {
            handleError(new Error('ETag does not match MD5 checksum'))
            return
          }
          cb()
        })
      })
      multipartETag.on('progress', function () {
        downloader.progressAmount = multipartETag.bytes
        downloader.emit('progress')
      })
      outStream.on('finish', function () {
        if (errorOccurred) return
        hashCheckPend.wait(function () {
          cb(null, outStream.toBuffer())
        })
      })

      httpStream.pipe(multipartETag)
      httpStream.pipe(outStream)
      multipartETag.resume()
    })

    request.send(handleError)

    function handleError (err) {
      if (!err) return
      if (errorOccurred) return
      errorOccurred = true
      cb(err)
    }
  }
}

Client.prototype.downloadStream = function (s3Params) {
  let self = this
  let downloadStream = new PassThrough()
  s3Params = extend({}, s3Params)

  doDownloadWithPend(function (err) {
    if (err) downloadStream.emit('error', err)
  })
  return downloadStream

  function doDownloadWithPend (cb) {
    self.s3Pend.go(function (pendCb) {
      doTheDownload(function (err) {
        pendCb()
        cb(err)
      })
    })
  }

  function doTheDownload (cb) {
    let errorOccurred = false
    let request = self.s3.getObject(s3Params)
    // let hashCheckPend = new Pend()
    request.on('httpHeaders', function (statusCode, headers, resp) {
      if (statusCode >= 300) {
        handleError(new Error('http status code ' + statusCode))
        return
      }
      downloadStream.emit('httpHeaders', statusCode, headers, resp)
      let httpStream = resp.httpResponse.createUnbufferedStream()

      httpStream.on('error', handleError)

      downloadStream.on('finish', function () {
        if (errorOccurred) return
        cb()
      })

      httpStream.pipe(downloadStream)
    })

    request.send(handleError)

    function handleError (err) {
      if (!err) return
      if (errorOccurred) return
      errorOccurred = true
      cb(err)
    }
  }
}

function syncDir (self, params, directionIsToS3) {
  let ee = new EventEmitter()
  let finditOpts = {
    fs: fs,
    followSymlinks: (params.followSymlinks == null) ? true : !!params.followSymlinks
  }
  let localDir = params.localDir
  let deleteRemoved = params.deleteRemoved === true
  let fatalError = false
  let prefix = params.s3Params.Prefix ? ensureSlash(params.s3Params.Prefix) : ''
  let bucket = params.s3Params.Bucket
  let listObjectsParams = {
    recursive: true,
    s3Params: {
      Bucket: bucket,
      Marker: null,
      MaxKeys: null,
      Prefix: prefix
    }
  }

  let getS3Params = params.getS3Params
  let baseUpDownS3Params = extend({}, params.s3Params)
  let upDownFileParams = {
    localFile: null,
    s3Params: baseUpDownS3Params,
    defaultContentType: params.defaultContentType
  }
  delete upDownFileParams.s3Params.Prefix

  ee.activeTransfers = 0
  ee.progressAmount = 0
  ee.progressTotal = 0
  ee.progressMd5Amount = 0
  ee.progressMd5Total = 0
  ee.objectsFound = 0
  ee.filesFound = 0
  ee.deleteAmount = 0
  ee.deleteTotal = 0
  ee.doneFindingFiles = false
  ee.doneFindingObjects = false
  ee.doneMd5 = false

  let allLocalFiles = []
  let allS3Objects = []
  let localFileCursor = 0
  let s3ObjectCursor = 0
  let objectsToDelete = []

  findAllS3Objects()
  startFindAllFiles()

  return ee

  function flushDeletes () {
    if (objectsToDelete.length === 0) return
    let thisObjectsToDelete = objectsToDelete
    objectsToDelete = []
    let params = {
      Bucket: bucket,
      Delete: {
        Objects: thisObjectsToDelete,
        Quiet: true
      }
    }
    let deleter = self.deleteObjects(params)
    deleter.on('error', handleError)
    deleter.on('end', function () {
      if (fatalError) return
      ee.deleteAmount += thisObjectsToDelete.length
      ee.emit('progress')
      checkDoMoreWork()
    })
  }

  function checkDoMoreWork () {
    if (fatalError) return

    let localFileStat = allLocalFiles[localFileCursor]
    let s3Object = allS3Objects[s3ObjectCursor]

    // need to wait for a file or object. checkDoMoreWork will get called
    // again when that happens.
    if (!localFileStat && !ee.doneMd5) return
    if (!s3Object && !ee.doneFindingObjects) return

    // need to wait until the md5 is done computing for the local file
    if (localFileStat && !localFileStat.multipartETag) return

    // localFileStat or s3Object could still be null - in that case we have
    // reached the real end of the list.

    // if they're both null, we've reached the true end
    if (!localFileStat && !s3Object) {
      // if we don't have any pending deletes or uploads, we're actually done
      flushDeletes()
      if (ee.deleteAmount >= ee.deleteTotal &&
          ee.progressAmount >= ee.progressTotal &&
          ee.activeTransfers === 0) {
        ee.emit('end')
        // prevent checkDoMoreWork from doing any more work
        fatalError = true
      }
      // either way, there's nothing else to do in this method
      return
    }

    // special case for directories when deleteRemoved is true and we're
    // downloading a dir from S3. We don't add directories to the list
    // unless this case is true, so we assert that fact here.
    if (localFileStat && localFileStat.isDirectory()) {
      assert.ok(!directionIsToS3)
      assert.ok(deleteRemoved)

      localFileCursor += 1
      setImmediate(checkDoMoreWork)

      if (!s3Object || s3Object.key.indexOf(localFileStat.s3Path) !== 0) {
        deleteLocalDir()
      }
      return
    }

    if (directionIsToS3) {
      if (!localFileStat) {
        deleteS3Object()
      } else if (!s3Object) {
        uploadLocalFile()
      } else if (localFileStat.s3Path < s3Object.key) {
        uploadLocalFile()
      } else if (localFileStat.s3Path > s3Object.key) {
        deleteS3Object()
      } else if (!compareMultipartETag(s3Object.ETag, localFileStat.multipartETag)) {
        // both file cursor and s3 cursor should increment
        s3ObjectCursor += 1
        uploadLocalFile()
      } else {
        skipThisOne()
      }
    } else {
      if (!localFileStat) {
        downloadS3Object()
      } else if (!s3Object) {
        deleteLocalFile()
      } else if (localFileStat.s3Path < s3Object.key) {
        deleteLocalFile()
      } else if (localFileStat.s3Path > s3Object.key) {
        downloadS3Object()
      } else if (!compareMultipartETag(s3Object.ETag, localFileStat.multipartETag)) {
        // both file cursor and s3 cursor should increment
        localFileCursor += 1
        downloadS3Object()
      } else {
        skipThisOne()
      }
    }

    function deleteLocalDir () {
      let fullPath = path.join(localDir, localFileStat.path)
      ee.deleteTotal += 1
      rimraf(fullPath, function (err) {
        if (fatalError) return
        if (err && err.code !== 'ENOENT') return handleError(err)
        ee.deleteAmount += 1
        ee.emit('progress')
        checkDoMoreWork()
      })
    }

    function deleteLocalFile () {
      localFileCursor += 1
      setImmediate(checkDoMoreWork)
      if (!deleteRemoved) return
      ee.deleteTotal += 1
      let fullPath = path.join(localDir, localFileStat.path)
      fs.unlink(fullPath, function (err) {
        if (fatalError) return
        if (err && err.code !== 'ENOENT') return handleError(err)
        ee.deleteAmount += 1
        ee.emit('progress')
        checkDoMoreWork()
      })
    }

    function downloadS3Object () {
      s3ObjectCursor += 1
      setImmediate(checkDoMoreWork)
      let fullPath = path.join(localDir, toNativeSep(s3Object.key))

      if (getS3Params) {
        getS3Params(fullPath, s3Object, haveS3Params)
      } else {
        startDownload()
      }

      function haveS3Params (err, s3Params) {
        if (fatalError) return
        if (err) return handleError(err)

        if (!s3Params) {
          // user has decided to skip this file
          return
        }

        upDownFileParams.s3Params = extend(extend({}, baseUpDownS3Params), s3Params)
        startDownload()
      }

      function startDownload () {
        ee.progressTotal += s3Object.Size
        let fullKey = s3Object.Key
        upDownFileParams.s3Params.Key = fullKey
        upDownFileParams.localFile = fullPath
        let downloader = self.downloadFile(upDownFileParams)
        let prevAmountDone = 0
        ee.activeTransfers++
        ee.emit('fileDownloadStart', fullPath, fullKey)
        downloader.on('error', handleError)
        downloader.on('progress', function () {
          if (fatalError) return
          let delta = downloader.progressAmount - prevAmountDone
          prevAmountDone = downloader.progressAmount
          ee.progressAmount += delta
          ee.emit('progress')
        })
        downloader.on('end', function () {
          ee.activeTransfers--
          ee.emit('fileDownloadEnd', fullPath, fullKey)
          ee.emit('progress')
          checkDoMoreWork()
        })
      }
    }

    function skipThisOne () {
      s3ObjectCursor += 1
      localFileCursor += 1
      setImmediate(checkDoMoreWork)
    }

    // create a revision over here
    // console.log(localFileStat.path, 'localFileStat');
    // if(localFileStat )
    //   createRevison()

    // function createRevison() {
    //   //use generatior
    //   generate().then((res)=> {
    //     fs.copyFile(path.join(__dirname, '../../build/index.html'), path.join(__dirname, '../../build/index:'+res.revisionKey+'.html'), (err) => {
    //         if (err) throw err;
    //         console.log(`${res.revisionKey}.html was copied to destination`);
    //     })
    //   })
    // }
    function uploadLocalFile () {
      localFileCursor += 1
      setImmediate(checkDoMoreWork)
      let fullPath = path.join(localDir, localFileStat.path)
      // ***

      if (getS3Params) {
        getS3Params(fullPath, localFileStat, haveS3Params)
      } else {
        upDownFileParams.s3Params = baseUpDownS3Params
        startUpload()
      }

      function haveS3Params (err, s3Params) {
        if (fatalError) return
        if (err) return handleError(err)

        if (!s3Params) {
          // user has decided to skip this file
          return
        }

        upDownFileParams.s3Params = extend(extend({}, baseUpDownS3Params), s3Params)
        startUpload()
      }

      function startUpload () {
        ee.progressTotal += localFileStat.size
        let fullKey = prefix + localFileStat.s3Path
        upDownFileParams.s3Params.Key = fullKey
        upDownFileParams.localFile = fullPath

        let uploader = self.uploadFile(upDownFileParams)
        let prevAmountDone = 0
        let prevAmountTotal = localFileStat.size
        ee.activeTransfers++
        ee.emit('fileUploadStart', fullPath, fullKey)
        uploader.on('error', handleError)
        uploader.on('progress', function () {
          if (fatalError) return
          let amountDelta = uploader.progressAmount - prevAmountDone
          prevAmountDone = uploader.progressAmount
          ee.progressAmount += amountDelta

          let totalDelta = uploader.progressTotal - prevAmountTotal
          prevAmountTotal = uploader.progressTotal
          ee.progressTotal += totalDelta

          ee.emit('progress')
        })
        uploader.on('end', function () {
          ee.activeTransfers--
          ee.emit('fileUploadEnd', fullPath, fullKey)
          ee.emit('progress')
          checkDoMoreWork()
        })
      }
    }

    function deleteS3Object () {
      s3ObjectCursor += 1
      setImmediate(checkDoMoreWork)
      if (!deleteRemoved) return
      objectsToDelete.push({Key: s3Object.Key})
      ee.deleteTotal += 1
      ee.emit('progress')
      assert.ok(objectsToDelete.length <= 1000)
      if (objectsToDelete.length === 1000) {
        flushDeletes()
      }
    }
  }

  function handleError (err) {
    if (fatalError) return
    fatalError = true
    ee.emit('error', err)
  }

  function findAllS3Objects () {
    let finder = self.listObjects(listObjectsParams)
    finder.on('error', handleError)
    finder.on('data', function (data) {
      if (fatalError) return
      ee.objectsFound += data.Contents.length
      ee.emit('progress')
      data.Contents.forEach(function (object) {
        object.key = object.Key.substring(prefix.length)
        allS3Objects.push(object)
      })
      checkDoMoreWork()
    })
    finder.on('end', function () {
      if (fatalError) return
      ee.doneFindingObjects = true
      ee.emit('progress')
      checkDoMoreWork()
    })
  }

  function startFindAllFiles () {
    findAllFiles(function (err) {
      if (fatalError) return
      if (err) return handleError(err)

      ee.doneFindingFiles = true
      ee.emit('progress')

      allLocalFiles.sort(function (a, b) {
        if (a.s3Path < b.s3Path) {
          return -1
        } else if (a.s3Path > b.s3Path) {
          return 1
        } else {
          return 0
        }
      })
      startComputingMd5Sums()
    })
  }

  function startComputingMd5Sums () {
    let index = 0
    computeOne()

    function computeOne () {
      if (fatalError) return
      let localFileStat = allLocalFiles[index]
      if (!localFileStat) {
        ee.doneMd5 = true
        ee.emit('progress')
        checkDoMoreWork()
        return
      }
      if (localFileStat.multipartETag) {
        index += 1
        setImmediate(computeOne)
        return
      }
      let fullPath = path.join(localDir, localFileStat.path)
      let inStream = fs.createReadStream(fullPath)
      let multipartETag = new MultipartETag()
      inStream.on('error', handleError)
      let prevBytes = 0
      multipartETag.on('progress', function () {
        let delta = multipartETag.bytes - prevBytes
        prevBytes = multipartETag.bytes
        ee.progressMd5Amount += delta
      })
      multipartETag.on('end', function () {
        if (fatalError) return
        localFileStat.multipartETag = multipartETag
        checkDoMoreWork()
        ee.emit('progress')
        index += 1
        computeOne()
      })
      inStream.pipe(multipartETag)
      multipartETag.resume()
    }
  }

  function findAllFiles (cb) {
    let dirWithSlash = ensureSep(localDir)
    let walker = findit(dirWithSlash, finditOpts)
    walker.on('error', function (err) {
      walker.stop()
      // when uploading, we don't want to delete based on a nonexistent source directory
      // but when downloading, the destination directory does not have to exist.
      if (!directionIsToS3 && err.path === dirWithSlash && err.code === 'ENOENT') {
        cb()
      } else {
        cb(err)
      }
    })
    walker.on('directory', function (dir, stat, stop, linkPath) {
      if (fatalError) return walker.stop()
      // we only need to save directories when deleteRemoved is true
      // and we're syncing to disk from s3
      if (!deleteRemoved || directionIsToS3) return
      let relPath = path.relative(localDir, linkPath || dir)
      if (relPath === '') return
      stat.path = relPath
      stat.s3Path = toUnixSep(relPath) + '/'
      stat.multipartETag = new MultipartETag()
      allLocalFiles.push(stat)
    })
    walker.on('file', function (file, stat, linkPath) {
      if (fatalError) return walker.stop()
      let relPath = path.relative(localDir, linkPath || file)
      stat.path = relPath
      stat.s3Path = toUnixSep(relPath)
      ee.filesFound += 1
      ee.progressMd5Total += stat.size
      ee.emit('progress')
      allLocalFiles.push(stat)
    })
    walker.on('end', function () {
      cb()
    })
  }
}

function ensureChar (str, c) {
  return (str[str.length - 1] === c) ? str : (str + c)
}

function ensureSep (dir) {
  return ensureChar(dir, path.sep)
}

function ensureSlash (dir) {
  return ensureChar(dir, '/')
}

function doWithRetry (fn, tryCount, delay, cb) {
  let tryIndex = 0

  tryOnce()

  function tryOnce () {
    fn(function (err, result) {
      if (err) {
        if (err.retryable === false) {
          cb(err)
        } else {
          tryIndex += 1
          if (tryIndex >= tryCount) {
            cb(err)
          } else {
            setTimeout(tryOnce, delay)
          }
        }
      } else {
        cb(null, result)
      }
    })
  }
}

function extend (target, source) {
  for (let propName in source) {
    target[propName] = source[propName]
  }
  return target
}

function chunkArray (array, maxLength) {
  let slices = [array]
  while (slices[slices.length - 1].length > maxLength) {
    slices.push(slices[slices.length - 1].splice(maxLength))
  }
  return slices
}

function cleanETag (eTag) {
  return eTag ? eTag.replace(/^\s*'?\s*"?\s*(.*?)\s*"?\s*'?\s*$/, '$1') : ''
}

function compareMultipartETag (eTag, multipartETag) {
  return multipartETag.anyMatch(cleanETag(eTag))
}

function getETagCount (eTag) {
  let match = (eTag || '').match(/[a-fA-F0-9]{32}-(\d+)$/)
  return match ? parseInt(match[1], 10) : 1
}

function keyOnly (item) {
  return {
    Key: item.Key,
    VersionId: item.VersionId
  }
}

function encodeSpecialCharacters (filename) {
  // Note: these characters are valid in URIs, but S3 does not like them for
  // some reason.
  return encodeURI(filename).replace(/[!'()* ]/g, function (char) {
    return '%' + char.charCodeAt(0).toString(16)
  })
}

function getPublicUrl (bucket, key, bucketLocation, endpoint) {
  let nonStandardBucketLocation = (bucketLocation && bucketLocation !== 'us-east-1')
  let hostnamePrefix = nonStandardBucketLocation ? ('s3-' + bucketLocation) : 's3'
  let parts = {
    protocol: 'https:',
    hostname: hostnamePrefix + '.' + (endpoint || 'amazonaws.com'),
    pathname: '/' + bucket + '/' + encodeSpecialCharacters(key)
  }
  return url.format(parts)
}

function getPublicUrlHttp (bucket, key, endpoint) {
  let parts = {
    protocol: 'http:',
    hostname: bucket + '.' + (endpoint || 's3.amazonaws.com'),
    pathname: '/' + encodeSpecialCharacters(key)
  }
  return url.format(parts)
}

function toUnixSep (str) {
  return str.replace(TO_UNIX_RE, '/')
}

function toNativeSep (str) {
  return str.replace(/\//g, path.sep)
}

function quotemeta (str) {
  return String(str).replace(/(\W)/g, '\\$1')
}

function smallestPartSizeFromFileSize (fileSize) {
  let partSize = Math.ceil(fileSize / MAX_MULTIPART_COUNT)
  return (partSize < MIN_MULTIPART_SIZE) ? MIN_MULTIPART_SIZE : partSize
}
