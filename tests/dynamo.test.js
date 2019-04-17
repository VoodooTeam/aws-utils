const AWS = require('aws-sdk-mock');

let nbRetry = 0;

AWS.mock('DynamoDB.DocumentClient', 'query', function (params, callback) {
    let res = [];
    let LastEvaluatedKey = null;

	if (params.TableName === 'myTable') {
		res = [{
			id: 1
		},
		{
			id: 2
		}];
	} else if (params.TableName === "myTableWithPagination") {
        if (nbRetry === 0) {
            nbRetry++;
            res = [{
                id: 1
            }];
            LastEvaluatedKey = {};
        } else {
            res = [{
                id: 2
            }];
        }
    } else if (params.TableName === 'error') {
        return callback(new Error('Error from Dynamo'));
    } else if (params.TableName === 'errorWithRetry') {
        const err = new Error('Error from Dynamo');
        err.retryable = true;
        return callback(err);
    } else if (params.TableName === 'errorWithOneRetry') {
        if (nbRetry === 0) {
            nbRetry++;
            const err = new Error('Error !');
            err.retryable = true;
            return callback(err);
        }

        res = [{
			id: 2
		}];
    } else if (params.TableName === 'noItems') {
		return callback(null, {});
	} else if (params.TableName === 'errorWithDax') {
        const err = new Error('Error from Dax');
        err.retryable = true;
        return callback(err);
    } else if (params.TableName === 'errorWithDaxButOkWithDynamo') {
        res = [{id:1}]
    }

	callback(null, { 'Items': res, 'LastEvaluatedKey': LastEvaluatedKey });
});

AWS.mock('DynamoDB.DocumentClient', 'scan', function (params, callback) {
	const res = [{
			id: 1
		},
		{
			id: 2
		}];

	callback(null, { 'Items': res });
});

AWS.mock('DynamoDB.DocumentClient', 'put', function (params, callback) {
    if (params.TableName === 'error') {
        return callback(new Error('Error from Dynamo'));
    } else if (params.TableName === 'errorWithRetry') {
        const err = new Error('Error from Dynamo');
        err.retryable = true;
        return callback(err);
    } else if (params.TableName === 'errorWithOneRetry') {
        if (nbRetry === 0) {
            nbRetry++;
            const err = new Error('Error !');
            err.retryable = true;
            return callback(err);
        }
    }

	callback(null);
});

describe('queryHashKey', () => {
    let dynamoTools;

    beforeEach(() => {
        nbRetry = 0;
        const awsUtils = require('../index').dynamo;

        const aws = require('aws-sdk');
        aws.config.update({region: 'eu-west-1'});
        const dynamoCli = new aws.DynamoDB.DocumentClient();
        dynamoTools = new awsUtils(dynamoCli);
    })

    it('Normal case', async () => {
        const res = await dynamoTools.queryHashKey('myTable', 'key', 'value');

        expect(res.length).toEqual(2);
        expect(res[0].id).toEqual(1);
    })

    it('Should return data from multiple calls (pagination)', async () => {
        const res = await dynamoTools.queryHashKey('myTableWithPagination', 'key', 'value');

        expect(res.length).toEqual(2);
        expect(res[0].id).toEqual(1);
        expect(res[1].id).toEqual(2);
    })

    it('Should throw an error', async () => {
        try {
            await dynamoTools.queryHashKey('error', 'key', 'value');
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual('Error from Dynamo');
        }
    })

    it('Should retry but throw an error', async () => {
        try {
            await dynamoTools.queryHashKey('errorWithRetry', 'key', 'value');
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual('Error from Dynamo');
        }
    }, 10000)

    it('Should retry with fallback cli but throw an error', async () => {
        try {
            const awsUtils = require('../src/DynamoTools');

            function AmazonDaxClient(){ this.query = (params, cb) => {const err = new Error('Error from Dax'); err.retryable = true; cb(err, null);}};
            const daxClient = new AmazonDaxClient();
            const logger = require('pino')();
            logger.child({
                    app_name: `App`,
                    env: `test`,
                }
            );
            dynamoTools = new awsUtils(daxClient, {
                logger: logger
            });

            await dynamoTools.queryHashKey('errorWithDax', 'key', 'value');
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual('Error from Dax');
        }
    }, 15000)

    it('Should retry with fallback cli and succeed', async () => {
        const awsUtils = require('../src/DynamoTools');

        function AmazonDaxClient(){ this.query = (params, cb) => {const err = new Error('Error from Dax'); err.retryable = true; cb(err, null);}};
        const daxClient = new AmazonDaxClient();
        dynamoTools = new awsUtils(daxClient);

        const res = await dynamoTools.queryHashKey('errorWithDaxButOkWithDynamo', 'key', 'value');
        expect(res.length).toEqual(1);
    }, 10000)

    it('Should retry, then fail one time then get data', async () => {
        const res = await dynamoTools.queryHashKey('errorWithOneRetry', 'key', 'value');
        expect(res.length).toEqual(1);
        expect(res[0].id).toEqual(2)
    })

    it('Should fail cause bad params', async () => {
        try {
            await dynamoTools.queryHashKey('errorWithOneRetry', 'key');
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual('BAD_PARAM');
        }

        try {
            await dynamoTools.queryHashKey('errorWithOneRetry', null, 'value');
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual('BAD_PARAM');
        }

        try {
            await dynamoTools.queryHashKey(null, 'key', 'value');
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual('BAD_PARAM');
        }
    })

    it('Should handle strange cases when no Items property is found', async () => {
        const res = await dynamoTools.queryHashKey('noItems', 'key', 'value');
        expect(res.length).toEqual(0);
    })
})

describe('scan', () => {
    let dynamoTools;

    beforeEach(() => {
        const awsUtils = require('../index').dynamo;

        const aws = require('aws-sdk');
        aws.config.update({region: 'eu-west-1'});
        const dynamoCli = new aws.DynamoDB.DocumentClient();
        dynamoTools = new awsUtils(dynamoCli);
    })

    it('Normal case', async () => {
        const res = await dynamoTools.scan('myTable');

        expect(res.length).toEqual(2);
        expect(res[0].id).toEqual(1)
    })
})

describe('putItem', () => {
    let dynamoTools;

    beforeEach(() => {
        nbRetry = 0;
        const awsUtils = require('../index').dynamo;

        const aws = require('aws-sdk');
        aws.config.update({region: 'eu-west-1'});
        const dynamoCli = new aws.DynamoDB.DocumentClient();
        dynamoTools = new awsUtils(dynamoCli);
    })

    it('Normal case', async () => {
        const res = await dynamoTools.putItem('myTable', {'key': 'value'});
        expect(res).toEqual(undefined);
    })

    it('Should throw an error', async () => {
        try {
            await dynamoTools.putItem('error', {'key': 'value'});
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual('Error from Dynamo');
        }
    })

    it('Should retry but throw an error', async () => {
        try {
            await dynamoTools.putItem('errorWithRetry', {'key': 'value'});
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual('Error from Dynamo');
        }
    })

    it('Should retry, then fail one time then get data', async () => {
        const res = await dynamoTools.putItem('errorWithOneRetry', {'key': 'value'});
        expect(res).toEqual(undefined);
    })

    it('Should fail cause bad params', async () => {
        try {
            await dynamoTools.putItem('errorWithOneRetry', '');
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual('BAD_PARAM');
        }

        try {
            await dynamoTools.putItem(null, {});
            throw new Error('This test should fail !');
        } catch (err) {
            expect(err.message).toEqual('BAD_PARAM');
        }
    })
})