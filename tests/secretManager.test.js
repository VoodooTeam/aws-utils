const AWS = require('aws-sdk-mock');

let nbRetry = 0;

AWS.mock('SecretsManager', 'getSecretValue', function (params, callback) {
    if (params.SecretId === 'secretRetry') {
        const err = new Error('Retry but fail !');
        err.retryable = true;
        return callback(err);
    } else if (params.SecretId === 'secretRetryOnce') {
        if (nbRetry === 0) {
            nbRetry++;
            const err = new Error('Retry but fail !');
            err.retryable = true;
            return callback(err);
        }
    } else if (params.SecretId === 'secretNoRetry') {
        const err = new Error('No Retry but fail !');
        return callback(err);
    }

	callback(null, { "key": "value" });
});

describe('getSecretValue', () => {
    let secretManagerTools;

    beforeEach(() => {
        nbRetry = 0;
        const awsUtils = require('../index').secretManager;
        const aws = require('aws-sdk');
        aws.config.update({region: 'eu-west-1'});
        const dynamoCli = new aws.SecretsManager({});
        secretManagerTools = new awsUtils(dynamoCli);
    })

    it('Normal case', async () => {
        const res = await secretManagerTools.getSecretValue('secret');
        expect(res.key).toEqual("value");
    })

    it('Should send an error cause bad params', async () => {
        try {
            await secretManagerTools.getSecretValue();
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual("Bad params");
        }
    })

    it('Should retry but fail', async () => {
        try {
            await secretManagerTools.getSecretValue('secretRetry');
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual("Retry but fail !");
        }
    })

    it('Should retry once then get file', async () => {
        const res = await secretManagerTools.getSecretValue('secretRetryOnce');
        expect(res.key).toEqual("value");
    })

    it('Should fail without retry', async () => {
        try {
            await secretManagerTools.getSecretValue('secretNoRetry');
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual("No Retry but fail !");
        }
    })
})
