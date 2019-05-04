# S3 Select Keys

I run an S3 Select query against a list of keys (files) in a single S3
bucket.

https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference.html

For each key (file) that has matching results I write the results out
to a file on the local file system.

When I am done I emit `done`. Then you can get the list of results files.

## Quick Start

The code is well commented. TLDR;

```js
const S3SelectKeys = require('s3-select-keys')
const keys = ['some/file/here', 'another/file/here']
const options = {
   Bucket: 'some-bucket',
   Expression: "select * from s3object o where o.name='harry'",
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
      console.error('Results are here.', files)
   })
s3s.start()
```

## test/smoketest.js

See `test/smoketest.js` for a working example of how to get the
results and merge them all into a single stream.

## AWS Credentials

The easy way to authenticate to AWS is to create the following two files.
Note: `aws_session_token` is not needed unless your keys are temporary.

`~/.aws/credentials`

```
[default]
aws_access_key_id = ASIA2V5YDA644PMPIEFH
aws_secret_access_key = z6VQJTSXeYEEnkkDjbDlBwNfWFsFiJYFIDVlsUEn
aws_session_token = AgoJb3JpZ2luX2VjEN7//////////wEaCXVzLXdlc3QtMiJGMEQCIEV69Cg7ooyXiDz5LGJGu/+rcYuB0Trt/jDHNehd8QO3AiBF8GJAzQL0KRF5g5KGENyxBX9m0ddDX6F+dli6wsH1rCrjAwjH//////////8BEAAaDDczNDI4ODYxMTI1NyIM8OjPKjHqqQRu9sC7KrcDui1mm7J1nEWmcJ27HFaEbm8tjIJRM2R1Lsy5diJbxoWsKnBwWPyDEzOTlW5+O9B0cHKm5mtnIHOGbJR1ZkyQ/MAq2YC3SAvsLRDUI5Dq+C2SPaywt9LIsVgT+5J4hVNE7x9aduA3TTP8IscDJwGCAFsFwHXs88C4qRsaFRsyUvSlrfireXZ6HPrcXAYX0a1TrrL5GnVctItnof6AQnqHqoB6V3kYT/pBqhRl4Z0rLClb+NQZit0OORJrTl7PIBUUXtR6sEwkEN8IGcydSR4fZu+XsewOyo2/VlTZf2Zv8jU3RAf1WqaU1hGMLbSdXiyOJSISjX3qSkQ3TAVUl8Yuf3g3HNRoyGekykZzOsSNn/tXJP9kFjzU76XgWhRbaCB/zwy+PDTcbM8Tpl9G8Cq+az6aJT82gmJcg16pEUHgBVBx6lvEqvTel1sdMcVJPe0+035nY7oliEfgteoIbZV2aAT2aFkFBUiD06p1xvnipKdX2SfrIR2gDze7AfzS5OPLHVS2mMEV6bRN6+qURweL3aibapiUulK2sBDmG1FBbFG27OmAOYPhLKqcuvcb9m91HBo7rmbEgzCs6rLmBTq1ASCOkkC3YQUc7sy4+/q4Z+49KzphjFhwFtqD0/naxGkzqHp5BNrnuh4GmsZ8dkgKQ4GDaAMglK0GbbhDOvd6uFyUeWK26/p12rHmIum6lTubuMYSo55gvOlWZ6hdHuETS40b1UaVdoAGAtXaB/Kje45QPfBF7LXg4iJqWdtFjZBY6miC4MsXcRGJ+r6uCPw/O7I5LIAMANxZCja3Hd6WeCiGxhEwbDVDFDJWlvLOmU4l0FrwC80=

[billy]
aws_access_key_id = ASIA2V5YDA644PMPIEFH
aws_secret_access_key = z6VQJTSXeYEEnkkDjbDlBwNfWFsFiJYFIDVlsUEn
aws_session_token = AgoJb3JpZ2luX2VjEN7//////////wEaCXVzLXdlc3QtMiJGMEQCIEV69Cg7ooyXiDz5LGJGu/+rcYuB0Trt/jDHNehd8QO3AiBF8GJAzQL0KRF5g5KGENyxBX9m0ddDX6F+dli6wsH1rCrjAwjH//////////8BEAAaDDczNDI4ODYxMTI1NyIM8OjPKjHqqQRu9sC7KrcDui1mm7J1nEWmcJ27HFaEbm8tjIJRM2R1Lsy5diJbxoWsKnBwWPyDEzOTlW5+O9B0cHKm5mtnIHOGbJR1ZkyQ/MAq2YC3SAvsLRDUI5Dq+C2SPaywt9LIsVgT+5J4hVNE7x9aduA3TTP8IscDJwGCAFsFwHXs88C4qRsaFRsyUvSlrfireXZ6HPrcXAYX0a1TrrL5GnVctItnof6AQnqHqoB6V3kYT/pBqhRl4Z0rLClb+NQZit0OORJrTl7PIBUUXtR6sEwkEN8IGcydSR4fZu+XsewOyo2/VlTZf2Zv8jU3RAf1WqaU1hGMLbSdXiyOJSISjX3qSkQ3TAVUl8Yuf3g3HNRoyGekykZzOsSNn/tXJP9kFjzU76XgWhRbaCB/zwy+PDTcbM8Tpl9G8Cq+az6aJT82gmJcg16pEUHgBVBx6lvEqvTel1sdMcVJPe0+035nY7oliEfgteoIbZV2aAT2aFkFBUiD06p1xvnipKdX2SfrIR2gDze7AfzS5OPLHVS2mMEV6bRN6+qURweL3aibapiUulK2sBDmG1FBbFG27OmAOYPhLKqcuvcb9m91HBo7rmbEgzCs6rLmBTq1ASCOkkC3YQUc7sy4+/q4Z+49KzphjFhwFtqD0/naxGkzqHp5BNrnuh4GmsZ8dkgKQ4GDaAMglK0GbbhDOvd6uFyUeWK26/p12rHmIum6lTubuMYSo55gvOlWZ6hdHuETS40b1UaVdoAGAtXaB/Kje45QPfBF7LXg4iJqWdtFjZBY6miC4MsXcRGJ+r6uCPw/O7I5LIAMANxZCja3Hd6WeCiGxhEwbDVDFDJWlvLOmU4l0FrwC80=

```

`~/.aws/config`

```

[default]
region = us-west-2

[billy]
region = us-east-1
```

The Node.js AWS API will use the `[default]` credentials unless you
specify otherwise. Here's how to tell it to use a profile other than
the default.

```js
const AWS = require('aws-sdk');
// If you want to use credentials other than the default profile, do this.
AWS.config.credentials = new AWS.SharedIniFileCredentials({ profile: 'billy' });
```
