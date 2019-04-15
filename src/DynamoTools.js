/**
 * DynamoTools - This class will handle DynamoDB calls
 */
const aws = require('aws-sdk');
const utils = require('./utils');

const CLASS_NAME = 'DynamoTools';

class DynamoTools {
    /**
     * Constructor
     * 
     * @param {Object} cli - aws cli object, instance of aws-sdk 
     * @param {Object} opt - (optional) options of dynamotools
     * @param {Number} opt.retryMax - number of retry per dynamo call
     */
    constructor(cli, opt = {}) {
        this.cli = cli;
        // fallback cli to use in case the originale (this.cli) fail
        // we use the default dynamo client
        this.cliFallback = new aws.DynamoDB.DocumentClient();
        this.retryMax = opt.retryMax || 5;
    }

    /**
     * Query a specific hashkey in dynamoDB
     *
     * @param {string} dynamoTable - The name of the dynamo table
     * @param {string} hashKeyName - The name of the hashkey
     * @param {string} hashKeyValue - The value of the hashkey
     * @param {Object} exclusiveStartKey - (optional) use for pagination
     * @param results
     */
    queryHashKey(dynamoTable, hashKeyName, hashKeyValue, exclusiveStartKey = null, results = [], cli = null) {
        return this._promiseFunction('query', 'queryHashKey', dynamoTable, hashKeyName, hashKeyValue, exclusiveStartKey = null, results = [], cli = null);
    }

    /**
     * This function will put an item to dynamoDB
     *  
     * @param {string} dynamoTable - The name of the dynamoDB table
     * @param {object} item - The item to push to dynamo DB
     */
    putItem(dynamoTable, item) {
        const errObj = {
            from: CLASS_NAME,
            params: {
                function_name: 'putItem',
                dynamo_table: dynamoTable,
                item: item
            }
        };

        return new Promise((resolve, reject) => {
            if (typeof dynamoTable !== 'string' || typeof item !== 'object' || Array.isArray(item)) {
                const myErr = new Error('BAD_PARAM');
                myErr.moreInfo = errObj;
                return reject(myErr);
            }

            const param = {
                TableName: dynamoTable,
                Item: item
            }

            this.cli.put(param, async (err) => {
                if (err) {
                    err.moreInfo = errObj;

                    if (err.retryable) {
                        try {
                            await utils.retry(this.cli.put.bind(this.cli), [param], true, this.retryMax);
                            return resolve();
                        } catch (err) {
                            err.moreInfo = errObj;
                        }
                    }

                    return reject(err);
                }

                return resolve();
            })
        })
    }

    /**
     * Scan a specific hashkey in dynamoDB
     * If no hashKeyName is provided, it scans all the talble
     *
     * @param {string} dynamoTable - The name of the dynamo table
     * @param {string} hashKeyName - (optional) The name of the hashkey
     * @param {string} hashKeyValue - (optional) The value of the hashkey
     * @param {Number} exclusiveStartKey - (optional) use for pagination
     * @param results
     */
    scan(dynamoTable, hashKeyName, hashKeyValue, exclusiveStartKey = null, results = [], cli = null) {
        return this._promiseFunction('scan', 'scan', dynamoTable, hashKeyName, hashKeyValue, exclusiveStartKey, results, cli); 
    }

    /**
     * Private method to execute a method from aws sdk as a promise, with retry system.
     * 
     * @param {Sting} AWSMethod - method from aws cli to execute 
     * @param {String} method - method from dynamotools which is currently executed
     * @param {String} dynamoTable - name of the dynamo table to use 
     * @param {String} hashKeyName - key's name to request
     * @param {String} hashKeyValue - value to request for hashKeyName
     * @param {Object} exclusiveStartKey - use for pagination
     * @param {Array} results - query's results 
     * @param {*} cli - fallback cli in case the default one fail, useful when we use DAX and we want to fallback to dynamo 
     */
    _promiseFunction (AWSMethod, method, dynamoTable, hashKeyName, hashKeyValue, exclusiveStartKey, results, cli) {
        const AWSCli = cli || this.cli;

        // error message
        const errObj = {
            from: CLASS_NAME,
            params: {
                function_name: method,
                dynamo_table: dynamoTable,
                hash_key_name: hashKeyName,
                hash_key_value: hashKeyValue,
                exclusive_start_key: exclusiveStartKey
            }
        };

        return new Promise((resolve, reject) => {
            if (method === 'queryHashKey' && (typeof dynamoTable !== "string" || typeof hashKeyName !== "string" || typeof hashKeyValue !== "string" || typeof exclusiveStartKey !== "object" || !Array.isArray(results))) {
                let myErr = new Error(`BAD_PARAM`);
                myErr.more_infos_queryHashKey = errObj;
                return reject(myErr);
            }

            const params = {
                TableName: dynamoTable
            };

            // if we request a specific key
            // we build condition and expression
            if (hashKeyName) {
                params.KeyConditionExpression = `#hashKey = :hkey`;
                params.ExpressionAttributeNames = {
                    '#hashKey': hashKeyName
                };
                params.ExpressionAttributeValues = {
                    ':hkey': hashKeyValue,
                };
            }

            // if we use pagination
            // we set it into params
            if (exclusiveStartKey !== null) {
                params.ExclusiveStartKey = exclusiveStartKey;
            }

            // call AWSCli with a specific method (query|scan)
            AWSCli[AWSMethod](params, async (err, data) => {
                if (err) {
                    let finalErr = err;

                    if (err.retryable) {
                        try {
                            // if error is retryable we will execute it again
                            await this._execute(method, AWSMethod, dynamoTable, hashKeyName, hashKeyValue, results, params, AWSCli);
                            return resolve(results);
                        } catch (err) {
                            // in case we use DAX and if we got an error
                            // use Dynamo directly
                            if(AWSCli.constructor.name === 'AmazonDaxClient') {
                                try {
                                    await this._execute(method, AWSMethod, dynamoTable, hashKeyName, hashKeyValue, results, params, this.cliFallback);
                                    return resolve(results);
                                } catch (errDynamo) {
                                    err.moreInfos = errDynamo;
                                }
                            }

                            finalErr = err;
                        }
                    }
                    finalErr.moreInfos = errObj;
                    return reject(finalErr);
                }

                // collect data into "results" before return it
                await this._collecData(method, AWSMethod, data, dynamoTable, hashKeyName, hashKeyValue, results, AWSCli);
                return resolve(results);
            })
        })
    }

    /**
     * Internal method to retry a method and collect its data.
     * 
     * @param {String} func - current method of dynamotools which is executed 
     * @param {String} cliFunc - aws cli method to execute
     * @param {String} dynamoTable - name of the dynamo table to use 
     * @param {String} hashKeyName - key's name to request
     * @param {String} hashKeyValue - value to request for hashKeyName
     * @param {Array} results - array of results
     * @param {Object} params - object to send to aws cli method
     * @param {*} cli - fallback cli 
     */
    async _execute(func, cliFunc, dynamoTable, hashKeyName, hashKeyValue, results, params, cli) {
        // retry the method with its original params
        const data = await utils.retry(cli[cliFunc], [params], true, this.retryMax);
        // collect data into results array
        await this._collecData(func, cliFunc, data, dynamoTable, hashKeyName, hashKeyValue, results, cli)
        return results;
    }

    /**
     * Internal method to collect data from aws method and put it into an array.
     * 
     * @param {String} func - current method of dynamotools which is executed 
     * @param {String} cliFunc - aws cli method to execute
     * @param {Object} data - object returns by aws sdk
     * @param {String} dynamoTable - name of the dynamo table to use 
     * @param {String} hashKeyName - key's name to request
     * @param {String} hashKeyValue - value to request for hashKeyName
     * @param {Array} results - array of results
     * @param {*} AWSCli - aws-sdk instance to use
     */
    async _collecData (func, cliFunc, data, dynamoTable, hashKeyName, hashKeyValue, results, AWSCli) {
        // collect data from "Items" property
        if (data && data.Items && data.Items.length > 0) {
            for (const obj of data.Items) {
                results.push(obj);
            }
        }
        // if we need pagination, then recall the original method of dynamoTools
        if (data.LastEvaluatedKey) return await this._promiseFunction(cliFunc, func, dynamoTable, hashKeyName, hashKeyValue, data.LastEvaluatedKey, results, AWSCli);
        return results;
    }
}

module.exports = DynamoTools;