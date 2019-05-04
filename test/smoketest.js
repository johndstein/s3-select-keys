#!/usr/bin/env node

'use strict'

const S3SelectKeys = require('../')
const merge2 = require('merge2')
const fs = require('fs')

const bucket = process.env.S3_SELECT_BUCKET
const keys = fs.readFileSync(process.env.S3_SELECT_KEYS).toString().split('\n')
keys.splice(30)
const hostname = '10.160.2.12'
const options = {
	Bucket: bucket,
	Expression: `select * from s3object s where s.rawmsghostname='${hostname}'`,
	ExpressionType: 'SQL',
	InputSerialization: {
		JSON: {
			Type: 'LINES'
		},
		CompressionType: 'GZIP'
	},
	OutputSerialization: {
		JSON: {
			RecordDelimiter: '\n'
		}
	}
}
const s3s = new S3SelectKeys(keys, options)

s3s
	.on('done', (files) => {
		console.error('done', s3s)
		const stream = merge2()
		files.forEach((f) => {
			console.error(f)
			stream.add(fs.createReadStream(f))
		})
		stream.pipe(fs.createWriteStream('bigstream2'))
	})

s3s.start()