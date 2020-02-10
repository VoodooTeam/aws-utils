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
        this.logger = opt.logger;
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
    queryHashKey(dynamoTable, hashKeyName, hashKeyValue, exclusiveStartKey = null, limit = null, params = {}, results = {Items: []}, cli = null) {
        return this._promiseFunction('query', 'queryHashKey', dynamoTable, hashKeyName, hashKeyValue, exclusiveStartKey, limit, params, results, cli);
    }

    /**
     * Make a query with conditions in dynamoDB
     *
     * @param {string} dynamoTable - The name of the dynamo table
     * @param customParams - query with the conditions
     * @param response
     * @param startKey
     * @returns results
     */
    query(dynamoTable, customParams, response = {Items: []}, startKey = null) {
        return new Promise((resolve,reject) => {
            if(typeof dynamoTable !== 'string' ||
              !Array.isArray(customParams.conditions)) {
                let myErr = new Error('BAD_PARAM');
                myErr.moreInfo = {
                    from: CLASS_NAME,
                    params: {
                        function_name: 'query',
                        dynamo_table: dynamoTable,
                        partition_key: customParams
                    }
                };
                return reject(myErr)
            }

            let params = {
                TableName: dynamoTable
            };

            if(startKey) params.ExclusiveStartKey = startKey;
            let keyCondition = '';
            let keyAttribute = {};
            let keyName = {};
            for(const [index, condition] of customParams.conditions.entries()) {
                if(keyCondition !== '') keyCondition += ' and ';
                keyCondition += `#i_${index} ${condition.operator} `;
                if(Array.isArray(condition.value)) {
                    keyCondition += ':b1 AND :b2';
                    keyAttribute[':b1'] = condition.value[0];
                    keyAttribute[':b2'] = condition.value[1]
                }
                else {
                    keyCondition += `:i_${index}`;
                    keyAttribute[`:i_${index}`] = condition.value
                }
                keyName[`#i_${index}`] = condition.key
            }

            if(keyCondition !== '') {
                params.KeyConditionExpression = keyCondition;
                params.ExpressionAttributeValues = keyAttribute;
                params.ExpressionAttributeNames = keyName
            }

            const errObj = {
                from: CLASS_NAME,
                params: {
                    function_name: 'query',
                    dynamo_table: dynamoTable,
                    params: params
                }
            };

            this.cli.query(params, async (err,data) => {
                if (err) {
                    if (err.retryable) {
                        try {
                            const data = await utils.retry(this.cli.query.bind(this.cli), [params], true, this.retryMax);

                            if(!data.Items) return resolve(response);
                            for(const item of data.Items) {
                                response.Items.push(item)
                            }
                            if(data.LastEvaluatedKey) return this.query(dynamoTable, customParams, response, data.LastEvaluatedKey);
                            return resolve(response)
                        } catch (err) {
                            err.moreInfo = errObj;
                            return reject(err)
                        }
                    }
                    err.moreInfo = errObj;
                    return reject(err)
                }

                if(!data.Items) return resolve(response);
                for(const item of data.Items) {
                    response.Items.push(item)
                }
                if(data.LastEvaluatedKey) return this.query(dynamoTable, customParams, response, data.LastEvaluatedKey);
                return resolve(response)
            })
        })
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
            };

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
     * @param {Number} limit - maximum number of items to return
     * @param {Object} params - object with the aws-sdk syntax for sending params to dynamo
     * @param results
     */
    scan(dynamoTable, hashKeyName, hashKeyValue, exclusiveStartKey = null, limit = null, params = {}, results = {Items: []}, cli = null) {
        return this._promiseFunction('scan', 'scan', dynamoTable, hashKeyName, hashKeyValue, exclusiveStartKey, limit, params, results, cli);
    }

    /**
    * This function will get an array of items from dynamoDB
    *
    * @param {string} dynamoTable - The name of the dynamoDB table
    * @param {array} partitionKeys - An array of partition keys
    */
    getItems(dynamoTable, partitionKeys) {
        const errObj = {
            from: CLASS_NAME,
            params: {
                function_name: 'getItems',
                dynamo_table: dynamoTable,
                partition_key: partitionKeys
            }
        };

        return new Promise((resolve, reject) => {
            if (typeof dynamoTable !== 'string' || typeof partitionKeys !== 'object' || !Array.isArray(partitionKeys)) {
                let myErr = new Error('BAD_PARAM');
                myErr.moreInfo = errObj;
                return reject(myErr)
            }

            const param = {
                RequestItems: {
                    [dynamoTable]: {
                        Keys: partitionKeys
                    }
                }
            };

            this.cli.batchGet(param, async (err, data) => {
                if (err) {
                    if (err.retryable) {
                        try {
                            const data = await utils.retry(this.cli.batchGet.bind(this.cli), [param], true, this.retryMax);
                            return resolve(this._formatRes(data, dynamoTable));
                        } catch (err) {
                            err.moreInfo = errObj;
                            return reject(err)
                        }
                    }
                    err.moreInfo = errObj;
                    return reject(err)
                }

                return resolve(this._formatRes(data, dynamoTable));
            })
        })
    }

    _formatRes(data, dynamoTable) {

        if (Object.prototype.hasOwnProperty.call(data, 'Responses') &&
          Object.prototype.hasOwnProperty.call(data.Responses, dynamoTable) &&
          Array.isArray(data.Responses[dynamoTable])) return data.Responses[dynamoTable];
        return [];
    }

    /**
     * This method will get a specific object from dynamo
     *
     * @param {string} dynamoTable
     * @param {Object} partitionKey
     */
    getItem(dynamoTable, partitionKey) {
        const errObj = {
            from: CLASS_NAME,
            params: {
                dynamo_table: dynamoTable,
                partition_key: partitionKey
            }
        };
        return new Promise((resolve, reject) => {
            if (typeof dynamoTable !== 'string' || typeof partitionKey !== 'object' || Array.isArray(partitionKey)) {
                let myErr = new Error('BAD_PARAM');
                myErr.moreInfo = errObj;
                return reject(myErr);
            }
            const param = {
                TableName: dynamoTable,
                Key: partitionKey
            };

            this.cli.get(param, async (err, data) => {
                if (err) {
                    if (err.retryable) {
                        try {
                            const data = await utils.retry(this.cli.get.bind(this.cli), [param], true, this.retryMax);
                            return resolve(data.Item)
                        } catch (err) {
                            err.moreInfo = errObj;
                            return reject(err);
                        }
                    }
                    err.moreInfo = errObj;
                    return reject(err);
                }

                return resolve(data.Item)
            })
        })
    }

    /**
     * Update items into DynamoDB
     *
     * @param {string} dynamoTable - The name of the dynamo table
     * @param {object} params - The entry params
     */
    updateItem(dynamoTable, params) {
        return new Promise((resolve, reject) => {
            if (typeof dynamoTable !== 'string' || typeof params !== 'object' || Array.isArray(params)) {
                let myErr = new Error('BAD_PARAM');
                myErr.more_infos_updateItem = {
                    from: CLASS_NAME,
                    params: {
                        dynamo_table: dynamoTable,
                        entry_param: params
                    }
                };
                return reject(myErr)
            }

            let param = {
                TableName: dynamoTable,
                Key: params.key,
                UpdateExpression: '',
                ExpressionAttributeValues: {}
            };

            let firstSet = true;
            let firstAdd = true;
            for (const key in params.set) {
                if (param.UpdateExpression === '') param.UpdateExpression += 'set';
                if (firstSet) {
                    param.UpdateExpression += ` ${key} = :${key}`;
                    firstSet = false
                } else {
                    param.UpdateExpression += `,${key} = :${key}`
                }
                param.ExpressionAttributeValues[`:${key}`] = params.set[key]
            }

            for (const key in params.add) {
                if (param.UpdateExpression.indexOf('add') === -1) {
                    if (param.UpdateExpression === '') param.UpdateExpression += 'add';
                    else param.UpdateExpression += ' add'
                }

                if (firstAdd) {
                    param.UpdateExpression += ` ${key} :${key}`;
                    firstAdd = false
                } else {
                    param.UpdateExpression += `,${key} :${key}`
                }
                param.ExpressionAttributeValues[`:${key}`] = params.add[key]
            }

            const errObj = {
                from: CLASS_NAME,
                params: {
                    dynamo_table: dynamoTable,
                    entry_param: params
                }
            };
            this.cli.update(param, async (err) => {
                if (err) {
                    if (err.retryable) {
                        try {
                            await utils.retry(this.cli.update.bind(this.cli), [param], true, this.retryMax);
                            return resolve()
                        } catch (err) {
                            err.more_infos_updateItem = errObj;
                            return reject(err)
                        }
                    }
                    err.more_infos_updateItem = errObj;
                    return reject(err)
                }

                return resolve()
            })
        })
    }

    /**
     * Delete a specific item by hashkey
     * @param {String} table
     * @param {String} key
     */
    async deleteItem(table, key) {
        const params = {
            TableName: table,
            Key: key
        };

        await this.cli.delete(params).promise();
    }

    /**
     * Private method to execute a method from aws sdk as a promise, with retry system.
     *
     * @param {String} AWSMethod - method from aws cli to execute
     * @param {String} method - method from dynamotools which is currently executed
     * @param {String} dynamoTable - name of the dynamo table to use
     * @param {String} hashKeyName - key's name to request
     * @param {String} hashKeyValue - value to request for hashKeyName
     * @param {Object} exclusiveStartKey - use for pagination
     * @param {Object} results - query's results
     * @param {*} cli - fallback cli in case the default one fail, useful when we use DAX and we want to fallback to dynamo
     * @param {Number} limit - in case of specific limit
     * @param {Object} params - object with the aws-sdk syntax for sending params to dynamo
     */
    _promiseFunction (AWSMethod, method, dynamoTable, hashKeyName, hashKeyValue, exclusiveStartKey, limit, params, results, cli) {
        const AWSCli = cli || this.cli;
        const AWSCliFallback = this.cliFallback;
        const logger = this.logger;

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
            if (method === 'queryHashKey' && (typeof dynamoTable !== "string" || typeof hashKeyName !== "string" || typeof hashKeyValue !== "string" || typeof exclusiveStartKey !== "object" || typeof results !== 'object')) {
                let myErr = new Error(`BAD_PARAM`);
                myErr.more_infos_queryHashKey = errObj;
                return reject(myErr);
            }

            if(!params) params = {};

            params.TableName = dynamoTable;

            if(limit) {
                params.Limit = limit
            }

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
                            await this._execute(method, AWSMethod, dynamoTable, hashKeyName, hashKeyValue, results, params, AWSCli, limit);
                            return resolve(results);
                        } catch (err) {
                            // in case we use DAX and if we got an error
                            // use Dynamo directly
                            if(AWSCli.constructor.name === 'AmazonDaxClient') {
                                if (logger) {
                                    logger.error(err);
                                }
                                try {
                                    await this._execute(method, AWSMethod, dynamoTable, hashKeyName, hashKeyValue, results, params, AWSCliFallback, limit);
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
                await this._collectData(method, AWSMethod, data, dynamoTable, hashKeyName, hashKeyValue, results, AWSCli, limit, params);
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
    async _execute(func, cliFunc, dynamoTable, hashKeyName, hashKeyValue, results, params, cli, limit) {
        // retry the method with its original params
        const data = await utils.retry(cli[cliFunc], [params], true, this.retryMax);
        // collect data into results array
        await this._collectData(func, cliFunc, data, dynamoTable, hashKeyName, hashKeyValue, results, cli, limit, params);
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
    async _collectData (func, cliFunc, data, dynamoTable, hashKeyName, hashKeyValue, results, AWSCli, limit, params) {
        // collect data from "Items" property
        if (data && data.Items && data.Items.length > 0) {

            for (const obj of data.Items) {
                results.Items.push(obj);
            }

            results.LastEvaluatedKey = data.LastEvaluatedKey;
        }
        // if we need pagination, then recall the original method of dynamoTools
        if (data.LastEvaluatedKey && (limit == null || limit > results.Items.length)) return await this._promiseFunction(cliFunc, func, dynamoTable, hashKeyName, hashKeyValue, data.LastEvaluatedKey, limit, params, results, AWSCli);
        return results;
    }
}

module.exports = DynamoTools;
