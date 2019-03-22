/**
 * Utils functions
 */
'use strict';

const retry = require('async-await-retry');

/**
 * Retry system with async / await
 *
 * @param {Function} fn : function to execute
 * @param {Array} args : args to pass to fn
 * @param {Number} retriesMax : number of retries, by default 3
 * @param {Number} interval : interval (in ms) between retry, by default 200ms
 * @param {Number} exponential : use exponential retry interval, by default true
 */
exports.retry = async (fn, args, cb, retriesMax = 3, interval = 200, exponential = true) => {
    return await retry(fn, args, {
        retriesMax: retriesMax,
        interval: interval,
        exponential: exponential,
        isCb: cb
    })
};
