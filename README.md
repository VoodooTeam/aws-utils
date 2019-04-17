# aws-utils

<div align="center">
<b>AWS utils with built-in retry system</b><br/>
<br/><br/>

<a href="https://badge.fury.io/js/%40voodoo.io%2Faws-utils.svg">
   <img src="https://badge.fury.io/js/%40voodoo.io%2Faws-utils.svg" alt="npm version" height="18">
</a>
</div>


# Purpose

Simple wrapper around aws-sdk to make it easier to use.

# Compatibility

**/!\ This module use async/await syntax, this is why you must have node 7.6+.**

Supported and tested : >= 7.6

| Version       | Supported     | Tested         |
| ------------- |:-------------:|:--------------:|
| 10.x          | yes           | yes            |
| 9.x           | yes           | yes            |
| 8.x           | yes           | yes            |
| >= 7.6        | yes           | yes            |

# Installation

```console
$ npm install @voodoo.io/aws-utils --save
```

# Usage

## Dynamo tools

### Configure dynamo tools

```javascript
const awsUtils = require('@voodoo.io/aws-utils').dynamo;

const aws = require('aws-sdk');
aws.config.update({region: 'eu-west-1'});
const dynamoCli = new aws.DynamoDB.DocumentClient();
const dynamoTools = new awsUtils(dynamoCli)
```

### Query

```javascript
const res = await dynamoTools.queryHashKey('myTable', 'key', 'value');
```

### Scan

```javascript
const res = await dynamoTools.scan('myTable');
```

### Put item

```javascript
const res = await dynamoTools.putItem('myTable', {'key': 'value'});
```

#### queryHashKey(dynamoTable, hashKeyName, hashKeyValue, [exclusiveStartKey])

* `dynamoTable` : table's name
* `hashKeyName` : hashkey's name
* `hashKeyValue` : hashkey's value
* `exclusiveStartKey` : (optional) start search at a specific key

#### putItem(dynamoTable, item)

* `dynamoTable` : table's name
* `item` : item to insert

#### scan(dynamoTable, [hashKeyName], [hashKeyValue], [exclusiveStartKey])

* `dynamoTable` : table's name
* `hashKeyName` : (optional) hashkey's name
* `hashKeyValue` : (optional) hashkey's value
* `exclusiveStartKey` : (optional) start search at a specific key

If no hashkey is provided it returns the full table.


## Secret Manager tools

### Configure secret manager tools

```javascript
const awsUtils = require('@voodoo.io/aws-utils').secretManager;

const aws = require('aws-sdk');
aws.config.update({region: 'eu-west-1'});
const cli = new aws.SecretsManager({});
const secretManagerTools = new awsUtils(cli);
```

### getSecretValue

```javascript
const res = await secretManagerTools.getSecretValue('secret');
```

#### getSecretValue(secret)

* `secret` : secret's id


## S3 tools

### Configure S3 tools

```javascript
const awsUtils = require('@voodoo.io/aws-utils').s3;

const aws = require('aws-sdk');
aws.config.update({region: 'eu-west-1'});
const cli = new aws.S3();
const s3Tools = new awsUtils(cli);
```

### getObject

```javascript
const res = await s3Tools.getObject('bucket', 'key');
```

#### getObject(bucket, key)

* `bucket` : bucket's name
* `key` : path to the ressource (/path/file.json)

### putJsonObject

```javascript
const res = await s3Tools.putJsonObject('bucket', 'key', {"key": "value"});
```

#### putJsonObject(bucket, key, item)

* `bucket` : bucket's name
* `key` : path to the ressource (/path/file.json)
* `item` : json object to save on S3

# Test

```console
$ npm test
```

Coverage report can be found in coverage/.