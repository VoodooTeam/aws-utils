'use strict';
const utils = require('./utils');

class SecretManagerTools {
    constructor(smCli) {
        this.cli = smCli;
    }

    getSecretValue (secretName) {
        const param = {
            SecretId: secretName
        };

        return new Promise((resolve, reject) => {
            if (typeof secretName !== "string") return reject(new Error(`Bad params`));

            this.cli.getSecretValue(param, async (err, data) => {
                if (err) {
                    if (err.retryable) {
                        try {
                            const data = await utils.retry(this.cli.getSecretValue.bind(this.cli), [param], true, this.retryMax);
                            return resolve(data);
                        } catch (err) {
                            return reject(err);
                        }
                    }
                    return reject(err);
                }

                return resolve(data);
            })
        })
    }
}

module.exports = SecretManagerTools;