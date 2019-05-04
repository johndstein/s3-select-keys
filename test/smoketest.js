#!/usr/bin/env node

'use strict'

const S3SelectKeys = require('../')
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
		console.error('DONE!!!', s3s)
	})
s3s.start()
