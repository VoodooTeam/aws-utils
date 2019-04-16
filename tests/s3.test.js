const AWS = require('aws-sdk-mock');
const zlib = require('zlib');

let nbRetry = 0;

AWS.mock('S3', 'getObject', function (params, callback) {
    let body = Buffer.from('{"key": "value"}');

    if (params.Bucket === 'bucketNotFound') {
        body = null;
    } else if (params.Bucket === 'bucketRetry') {
        const err = new Error('Retry but fail !');
        err.retryable = true;
        return callback(err);
    } else if (params.Bucket === 'bucketRetryOnce') {
        if (nbRetry === 0) {
            nbRetry++;
            const err = new Error('Retry but fail !');
            err.retryable = true;
            return callback(err);
        }
    } else if (params.Bucket === 'bucketNoRetry') {
        const err = new Error('No Retry but fail !');
        return callback(err);
    }

	callback(null, { 'Body': body });
});

AWS.mock('S3', 'putObject', function (params, callback) {
    let body = 'ok';

    if (params.Bucket === 'bucketRetry') {
        const err = new Error('Retry but fail !');
        err.retryable = true;
        return callback(err);
    } else if (params.Bucket === 'bucketRetryOnce') {
        if (nbRetry === 0) {
            nbRetry++;
            const err = new Error('Retry but fail !');
            err.retryable = true;
            return callback(err);
        }
    } else if (params.Bucket === 'bucketNoRetry') {
        const err = new Error('No Retry but fail !');
        return callback(err);
    }

	callback(null, body);
});

describe('getObject', () => {
    let s3Tools;

    beforeEach(() => {
        nbRetry = 0;
        const awsUtils = require('../index').s3;
        const aws = require('aws-sdk');
        aws.config.update({region: 'eu-west-1'});
        const dynamoCli = new aws.S3();
        s3Tools = new awsUtils(dynamoCli);
    })

    it('Normal case', async () => {
        const res = await s3Tools.getObject('bucket', 'key');

        expect(res.key).toEqual("value");
    })

    it('Should send an error cause no file was found', async () => {
        try {
            await s3Tools.getObject('bucketNotFound', 'key');
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual("FILE_NOT_FOUND");
        }
    })

    it('Should send an error cause bad params', async () => {
        try {
            await s3Tools.getObject('bucketNotFound');
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual("S3_TOOLS_BAD_PARAM_GetObject");
        }
    })

    it('Should retry but fail', async () => {
        try {
            await s3Tools.getObject('bucketRetry', 'key');
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual("Retry but fail !");
        }
    })

    it('Should retry once then get file', async () => {
        const res = await s3Tools.getObject('bucketRetryOnce', 'key');
        expect(res.key).toEqual("value");
    })

    it('Should fail without retry', async () => {
        try {
            await s3Tools.getObject('bucketNoRetry', 'key');
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual("No Retry but fail !");
        }
    })
})

describe('putJsonObject', () => {
    let s3Tools;

    beforeEach(() => {
        nbRetry = 0;
        const awsUtils = require('../index').s3;
        const aws = require('aws-sdk');
        aws.config.update({region: 'eu-west-1'});
        const dynamoCli = new aws.S3();
        s3Tools = new awsUtils(dynamoCli);
    })

    it('Normal case', async () => {
        const res = await s3Tools.putJsonObject('bucket', 'key', {"key": "value"});
        expect(res).toEqual("ok");
    })

    it('Should send an error cause bad params', async () => {
        try {
            await s3Tools.putJsonObject('bucket');
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual("S3_TOOLS_BAD_PARAM_PutObject");
        }
    })

    it('Should retry but fail', async () => {
        try {
            await s3Tools.putJsonObject('bucketRetry', 'key', {"key": "value"});
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual("Retry but fail !");
        }
    })

    it('Should retry once then get file', async () => {
        const res = await s3Tools.putJsonObject('bucketRetryOnce', 'key', {"key": "value"});
        expect(res).toEqual("ok");
    })

    it('Should fail without retry', async () => {
        try {
            await s3Tools.putJsonObject('bucketNoRetry', 'key', {"key": "value"});
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual("No Retry but fail !");
        }
    })
})

describe('_formatGetObjectResponse', () => {
    let s3Tools;

    function zip(data) {
        return new Promise((resolve, reject) => {
            zlib.gzip(data, (err, compressed) => {
                if (err) throw err;
                resolve(compressed);
            });
        });
    }

    beforeEach(() => {
        nbRetry = 0;
        const awsUtils = require('../index').s3;
        const aws = require('aws-sdk');
        aws.config.update({region: 'eu-west-1'});
        const dynamoCli = new aws.S3();
        s3Tools = new awsUtils(dynamoCli);
    })

    it('Default case', async () => {
        const data = {"key": "value"};
        const res = await s3Tools._formatGetObjectResponse(data, 'other');
        expect(res.key).toEqual("value");
    })

    it('Normal case with object & gzip', async () => {
        const data = {"key": "value"};
        const buf = new Buffer(JSON.stringify(data), 'utf-8');
        const obj = {
            Body: await zip(buf)
        };
        const res = await s3Tools._formatGetObjectResponse(obj, 'object', true);
        expect(res.key).toEqual("value");
    })

    it('Error with object & gzip', async () => {
        const obj = {
            Body: null
        };
        try {
            await s3Tools._formatGetObjectResponse(obj, 'object', true);
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual("unexpected end of file");
        }
    })

    it('Normal case with string', async () => {
        const data = "test";
        const obj = {
            Body: data
        };
        const res = await s3Tools._formatGetObjectResponse(obj, 'string');
        expect(res).toEqual("test");
    })

    it('Normal case with string & gzip', async () => {
        const data = "test";
        const obj = {
            Body: await zip(data)
        };
        const res = await s3Tools._formatGetObjectResponse(obj, 'string', true);
        expect(res).toEqual("test");
    })

    it('Error with string & gzip', async () => {
        const obj = {
            Body: null
        };
        try {
            await s3Tools._formatGetObjectResponse(obj, 'string', true);
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual("unexpected end of file");
        }
    })

    it('Normal case with buffer', async () => {
        const data = "test";
        const obj = {
            Body: new Buffer(data)
        };
        const res = await s3Tools._formatGetObjectResponse(obj, 'buffer');
        expect(res.toString('utf8')).toEqual("test");
    })

    it('Normal case with buffer & gzip', async () => {
        const data = "test";
        const obj = {
            Body: await zip(new Buffer(data))
        };
        const res = await s3Tools._formatGetObjectResponse(obj, 'buffer', true);
        expect(res.toString('utf8')).toEqual("test");
    })

    it('Error with buffer & gzip', async () => {
        const obj = {
            Body: null
        };
        try {
            await s3Tools._formatGetObjectResponse(obj, 'buffer', true);
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual("unexpected end of file");
        }
    })
})