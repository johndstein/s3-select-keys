'use strict'

const fs = require('fs')
const os = require('os')
const path = require('path')
const AWS = require('aws-sdk')

// We loop over a list of S3 object keys and run an S3 select query
// against each one.

// For each key that matches we write the results to a file under
// os.tmpdir().

// We emit 'done' when we're done.

// this.files contains a list of the full path to each of the results
// files. We don't write a file for keys that did't match.

// We don't emit 'error', but you can see if anything went wrong in
// this.errors. We assume you want to keep going even if searching one
// or more keys failed.

// It would be great if we could skip writing to the file system and
// just return a single stream with all the results.

// I have never had much success with this. It's easy if you don't
// care about order of events. https://github.com/grncdr/merge-stream
// would do the trick.

// It's hard if you care about order of results.

// If you have limited disk space, this may not be the thing for you.
// AWS Lambda currently allows 512 MB storage in /tmp.
class S3SelectKeys extends require('events') {
   constructor(keys, options, s3options, sharedIniFileCredentials) {
      super()
      // keys: S3 object keys.
      //    Example: some/path/under/a/bucket
      //    Note: No forward slash (/) at the beginning.
      this.keys = keys
      // options: Options to pass to s3.selectObjectContent().
      //    https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#selectObjectContent-property
      this.options = options
      if (sharedIniFileCredentials) {
         // You could do this outside of this class and it would do the same
         // thing. Just making it easy.
         // https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/loading-node-credentials-shared.html
         AWS.config.credentials =
            new AWS.SharedIniFileCredentials(sharedIniFileCredentials)
      }
      // s3options:
      //    https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#constructor-property
      this.s3 = new AWS.S3(s3options || {})
      // files: A list of files on the local file system where we wrote results.
      //    There will be one file per key that matched the search results.
      this.files = []
      // goodKeys: Keys that did not throw an error.
      this.goodKeys = []
      // badKeys: Keys that threw an error.
      this.badKeys = []
      // errors: List of errors that we encountered along the way.
      this.errors = []
      // startTime: When we started.
      this.startTime = null
      // endTime: When we ended.
      this.endTime = null
      // elapsedSeconds: How long we took in seconds.
      this.elapsedSeconds = null
      // tmpdir: The folder we will write files to.
      //    The default is generally ok, but you may want to overrider this
      //    before calling start()
      this.tmpdir = os.tmpdir()
   }
   _buildOptionsWith(key) {
      const copy = JSON.parse(JSON.stringify(this.options))
      copy.Key = key
      return copy
   }
   _getFilePath(i) {
      return path.join(this.tmpdir, `${i.toString().padStart(7, 0)}-s3sk-${this._makeid(7)}`)
   }
   _makeid(length) {
      let result = ''
      const c = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
      const clen = c.length
      for (let i = 0; i < length; i++) {
         result += c.charAt(Math.floor(Math.random() * clen))
      }
      return result
   }
   start() {
      this.startTime = new Date()
      for (let i = 0, l = this.keys.length; i < l; i++) {
         const options = this._buildOptionsWith(this.keys[i])
         const filepath = this._getFilePath(i)
         let writestream
         const payloadfn = (Payload, event) => {
            if (event.Records) {
               if (!writestream) {
                  this.files.push(filepath)
                  writestream = fs.createWriteStream(filepath)
               }
               const keepwriting = writestream.write(event.Records.Payload)
               if (!keepwriting) {
                  // console.error('pausing', this.keys[i])
                  Payload.pause()
                  writestream.once('drain', () => {
                     Payload.resume()
                  })
               }
            }
         }
         const selectfn = (err, data) => {
            if (err) {
               this.errors.push(err)
               this.badKeys.push(this.keys[i])
            } else {
               data.Payload
                  .on('data', payloadfn.bind(this, data.Payload))
                  .on('error', (err) => {
                     // console.error('Payload error')
                     this.errors.push(err)
                     this.badKeys.push(this.keys[i])
                  })
                  // Never emits close.
                  // .on('close', () => {
                  //    console.error('Payload close')
                  // })
                  .on('end', () => {
                     // console.error('Payload end')
                     this.goodKeys.push(this.keys[i])
                  })
               // Don't need finish.
               // .on('finish', () => {
               //    console.error('Payload finish')
               // })
            }
         }
         this.s3.selectObjectContent(options, selectfn)
      }
      const interval = setInterval(() => {
         if (this.goodKeys.length + this.badKeys.length === this.keys.length) {
            clearInterval(interval)
            this.endTime = new Date()
            this.elapsedSeconds = (this.endTime - this.startTime) / 1000
            // sort is essental if you care about results ordering
            this.files.sort()
            this.emit('done', this.files)
         }
      }, 100)
   }
   // Does nothing. Just returns a command you can use to clean up the
   // tmpdir if you want.
   rmrf() {
      return `rm -rf ${path.join(this.tmpdir,'0*-s3sk-*')}`
   }
}

exports = module.exports = S3SelectKeys
