/**
 * Module that wraps the calls to the AWS.Kinesis library. Calls are wrapped so they can be
 * retried with a custom logic instead of the one provided by the AWS-SDK. In addition to retries,
 * calls are also promisified and the call stacks are preserved even in async/await calls by using
 * the `CAPTURE_STACK_TRACE` environment variable.
 *
 * @module kinesis-client
 * @private
 */

'use strict';

const retry = require('async-retry');
const { Kinesis } = require('aws-sdk');

const { getStackObj, shouldBailRetry, transformErrorStack } = require('./utils');
const { reportError, reportRecordSent, reportResponse } = require('./stats');

const RETRIABLE_PUT_ERRORS = new Set([
  'EADDRINUSE',
  'ECONNREFUSED',
  'ECONNRESET',
  'EPIPE',
  'ESOCKETTIMEDOUT',
  'ETIMEDOUT',
  'NetworkingError',
  'ProvisionedThroughputExceededException',
  'TimeoutError'
]);

const privateData = new WeakMap();
const statsSource = 'kinesis';

/**
 * Provides access to the private data of the specified instance.
 *
 * @param {Object} instance - The private data's owner.
 * @returns {any} The private data.
 * @private
 */
function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

/**
 * Calls a method on the given instance of AWS.Kinesis. The call is promisified, the call stack
 * is preserved, and the results of the call are aggregated in the stats. Retries in this function
 * are the original ones provided by the AWS-SDK.
 *
 * @param {Object.<string, Function>} client - An instance of AWS.Kinesis.
 * @param {string} methodName - The name of the method to call.
 * @param {string} streamName - The name of the Kinesis stream for which the call relates to.
 * @param {...*} args - The arguments of the method call.
 * @fulfil {*} - The original response from the AWS.Kinesis call.
 * @reject {Error} - The error details from AWS.Kinesis with a corrected error stack.
 * @returns {Promise<any>}
 * @private
 */
async function sdkCall(client, methodName, streamName, ...args) {
  const stackObj = getStackObj(sdkCall);
  try {
    return client[methodName](...args)
      .promise()
      .then((/** @type {any} */ response) => {
        reportResponse(statsSource, streamName);
        return response;
      })
      .catch((/** @type {any} */ err) => {
        const error = transformErrorStack(err, stackObj);
        reportError(statsSource, error, streamName);
        throw error;
      });
  } catch (/** @type {any} */ err) {
    const error = transformErrorStack(err, stackObj);
    reportError(statsSource, error, streamName);
    throw error;
  }
}

/**
 * Calls a method on the given instance of AWS.Kinesis. The call is promisified, the call stack
 * is preserved, and the results of the call are aggregated in the stats. Retries in this function
 * are based on a custom logic replacing the one provided by the AWS-SDK.
 *
 * @param {Object.<string, Function>} client - An instance of AWS.Kinesis.
 * @param {string} methodName - The name of the method to call.
 * @param {string} streamName - The name of the Kinesis stream for which the call relates to.
 * @param {Object} retryOpts - The [retry options as in async-retry]{@link external:AsyncRetry}.
 * @param {...*} args - The argument of the method call.
 * @fulfil {*} - The original response from the AWS.Kinesis call.
 * @reject {Error} - The error details from AWS.Kinesis with a corrected error stack.
 * @returns {Promise<any>}
 * @private
 */
function retriableSdkCall(client, methodName, streamName, retryOpts, ...args) {
  const stackObj = getStackObj(retriableSdkCall);
  return retry((bail) => {
    try {
      return client[methodName](...args)
        .promise()
        .then((/** @type {any} */ response) => {
          reportResponse(statsSource, streamName);
          return response;
        })
        .catch((/** @type {any} */ err) => {
          const error = transformErrorStack(err, stackObj);
          reportError(statsSource, error, streamName);
          if (!shouldBailRetry(err)) throw error;
          else bail(error);
        });
    } catch (/** @type {any} */ err) {
      const error = transformErrorStack(err, stackObj);
      reportError(statsSource, error, streamName);
      bail(error);
      return undefined;
    }
  }, retryOpts);
}

/**
 * A class that wraps AWS.Kinesis.
 *
 * @alias module:kinesis-client
 */
class KinesisClient {
  /**
   * Initializes the AWS.Kinesis internal instance and prepares the retry logic.
   *
   * @param {Object} options - The initialization options.
   * @param {Object} options.awsOptions - The initialization options for AWS.Kinesis.
   * @param {Logger} options.logger - An instace of a logger.
   * @param {string} options.streamName - The name of the Kinesis stream for which calls relate to.
   * @param {boolean} options.supressThroughputWarnings - Flag indicating whether or not
   *        to supress ProvisionedThroughputExceededException warning logs.
   */
  constructor({ awsOptions, logger, streamName, supressThroughputWarnings }) {
    const client = new Kinesis(awsOptions);

    const retryOpts = {
      forever: true,
      maxTimeout: 5 * 60 * 1000,
      minTimeout: 1000,
      onRetry: (/** @type {any} */ err) => {
        const { code, message, requestId, statusCode } = err;
        const loggerMethod =
          supressThroughputWarnings && code === 'ProvisionedThroughputExceededException'
            ? 'debug'
            : 'warn';
        logger[loggerMethod](
          `Trying to recover from AWS.Kinesis error…\n${[
            `\t- Message: ${message}`,
            `\t- Request ID: ${requestId}`,
            `\t- Code: ${code} (${statusCode})`,
            `\t- Stream: ${streamName}`
          ].join('\n')}`
        );
      },
      randomize: true
    };

    Object.assign(internal(this), { client, retryOpts, streamName });
  }

  /**
   * Adds or updates tags for the specified Kinesis data stream.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise<void>}
   */
  addTagsToStream(...args) {
    const { client, streamName } = internal(this);
    return sdkCall(client, 'addTagsToStream', streamName, ...args);
  }

  /**
   * Creates a Kinesis data stream.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise<void>}
   * @throws {Error}
   */
  createStream(...args) {
    const { client, streamName } = internal(this);
    return sdkCall(client, 'createStream', streamName, ...args).catch((err) => {
      if (err.code !== 'ResourceInUseException') throw err;
    });
  }

  /**
   * To deregister a consumer, provide its ARN.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise<void>}
   */
  deregisterStreamConsumer(...args) {
    const { client, streamName } = internal(this);
    return sdkCall(client, 'deregisterStreamConsumer', streamName, ...args);
  }

  /**
   * Describes the specified Kinesis data stream.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise<Kinesis.DescribeStreamOutput>}
   */
  describeStream(...args) {
    const { client, retryOpts, streamName } = internal(this);
    return retriableSdkCall(client, 'describeStream', streamName, retryOpts, ...args);
  }

  /**
   * Summarizes the specified Kinesis data stream.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise<Kinesis.DescribeStreamSummaryOutput>}
   */
  describeStreamSummary(...args) {
    const { client, retryOpts, streamName } = internal(this);

    return sdkCall(client, 'describeStreamSummary', streamName, ...args).catch((err) => {
      if (err.code !== 'UnknownOperationException') throw err;

      return retriableSdkCall(client, 'describeStream', streamName, retryOpts, ...args).then(
        (data) => {
          const { StreamDescription } = data;
          return { StreamDescriptionSummary: StreamDescription };
        }
      );
    });
  }

  /**
   * Gets data records from a Kinesis data stream's shard.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise<Kinesis.GetRecordsOutput>}
   */
  getRecords(...args) {
    const { client, retryOpts, streamName } = internal(this);
    return retriableSdkCall(client, 'getRecords', streamName, retryOpts, ...args);
  }

  /**
   * Gets an Amazon Kinesis shard iterator.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise<Kinesis.GetShardIteratorOutput>}
   */
  getShardIterator(...args) {
    const { client, retryOpts, streamName } = internal(this);
    return retriableSdkCall(client, 'getShardIterator', streamName, retryOpts, ...args);
  }

  /**
   * Tells whether the endpoint of the client is local or not.
   *
   * @returns {boolean} `true` if the endpoints is local, `false` otherwise.
   */
  isEndpointLocal() {
    const { client } = internal(this);
    const { host } = client.endpoint;
    return host.includes('localhost') || host.includes('localstack');
  }

  /**
   * Lists the shards in a stream and provides information about each shard.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise<Kinesis.ListShardsOutput>}
   */
  listShards(...args) {
    const { client, retryOpts, streamName } = internal(this);
    return retriableSdkCall(client, 'listShards', streamName, retryOpts, ...args);
  }

  /**
   * Lists the consumers registered to receive data from a stream using enhanced fan-out, and
   * provides information about each consumer.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise<Kinesis.ListStreamConsumersOutput>}
   */
  listStreamConsumers(...args) {
    const { client, retryOpts, streamName } = internal(this);
    return retriableSdkCall(client, 'listStreamConsumers', streamName, retryOpts, ...args);
  }

  /**
   * Lists the tags for the specified Kinesis data stream.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise<Kinesis.ListTagsForStreamOutput>}
   */
  listTagsForStream(...args) {
    const { client, retryOpts, streamName } = internal(this);
    return retriableSdkCall(client, 'listTagsForStream', streamName, retryOpts, ...args);
  }

  /**
   * Writes a single data record into an Amazon Kinesis data stream.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise<Kinesis.PutRecordOutput|void>}
   */
  putRecord(...args) {
    /** @type {{client: Kinesis, retryOpts: Object, streamName: string }} */
    const { client, retryOpts, streamName } = internal(this);
    const stackObj = getStackObj(retriableSdkCall);
    return retry((bail) => {
      try {
        return client
          .putRecord(...args)
          .promise()
          .then((result) => {
            reportResponse(statsSource, streamName);
            reportRecordSent(streamName);
            return result;
          })
          .catch((err) => {
            const error = transformErrorStack(err, stackObj);
            reportError(statsSource, error, streamName);
            if (RETRIABLE_PUT_ERRORS.has(err.code)) throw error;
            else bail(error);
          });
      } catch (/** @type {any} */ err) {
        const error = transformErrorStack(err, stackObj);
        reportError(statsSource, error, streamName);
        bail(error);
        return undefined;
      }
    }, retryOpts);
  }

  /**
   * Writes multiple data records into a Kinesis data stream in a single call (also referred to as
   * a PutRecords request).
   *
   * @param {...*} args - The arguments.
   * @returns {Promise<Kinesis.PutRecordsOutput|void>}
   */
  putRecords(...args) {
    /** @type {{client: Kinesis, retryOpts: Object, streamName: string }} */
    const { client, retryOpts, streamName } = internal(this);
    const stackObj = getStackObj(retriableSdkCall);
    const [firstArg, ...restOfArgs] = args;
    let records = firstArg.Records;
    /** @type {Array<Object>} */
    const results = [];
    return retry((bail) => {
      try {
        return client
          .putRecords({ ...firstArg, Records: records }, ...restOfArgs)
          .promise()
          .then((payload) => {
            const { EncryptionType, FailedRecordCount = 0, Records } = payload;
            const failedCount = FailedRecordCount;
            const recordsCount = Records.length;
            const nextRecords = [];
            for (let i = 0; i < recordsCount; i += 1) {
              if (Records[i].ErrorCode) nextRecords.push(records[i]);
              else results.push(Records[i]);
            }
            reportResponse(statsSource, streamName);
            if (failedCount < records.length) {
              reportRecordSent(streamName);
            }
            if (failedCount === 0) {
              return { EncryptionType, Records: results };
            }
            records = nextRecords;
            /** @type {Object.<string, any>} */
            const error = new Error(`Failed to write ${failedCount} of ${recordsCount} record(s).`);
            error.code = 'ProvisionedThroughputExceededException';
            throw error;
          })
          .catch((/** @type {any} */ err) => {
            const error = transformErrorStack(err, stackObj);
            reportError(statsSource, error, streamName);
            if (RETRIABLE_PUT_ERRORS.has(err.code)) throw error;
            else bail(error);
          });
      } catch (/** @type {any} */ err) {
        const error = transformErrorStack(err, stackObj);
        reportError(statsSource, error, streamName);
        bail(error);
        return undefined;
      }
    }, retryOpts);
  }

  /**
   * Registers a consumer with a Kinesis data stream.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise<Kinesis.RegisterStreamConsumerOutput>}
   */
  registerStreamConsumer(...args) {
    const { client, streamName } = internal(this);
    return sdkCall(client, 'registerStreamConsumer', streamName, ...args);
  }

  /**
   * Enables or updates server-side encryption using an AWS KMS key for a specified stream.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise<void>}
   */
  startStreamEncryption(...args) {
    const { client, streamName } = internal(this);
    return sdkCall(client, 'startStreamEncryption', streamName, ...args).catch((err) => {
      const { code } = err;
      if (code !== 'UnknownOperationException' && code !== 'ResourceInUseException') throw err;
    });
  }

  /**
   * Waits for a given Kinesis resource.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise<Kinesis.DescribeStreamOutput>}
   */
  waitFor(...args) {
    const { client, retryOpts, streamName } = internal(this);
    return retriableSdkCall(client, 'waitFor', streamName, retryOpts, ...args);
  }
}

/**
 * @external AsyncRetry
 * @see https://github.com/zeit/async-retry#api
 */

module.exports = KinesisClient;

/**
 * @typedef KinesisError
 * @property {string} code - Error code.
 * @property {string} message - Error message.
 * @property {string} requestId - Request id from the error.
 * @property {string} statusCode - Status code of the response from the error.
 */

/**
 * @typedef Logger
 * @property {Function} debug - Prints standard debug with newline.
 * @property {Function} error - Prints standard error with newline.
 * @property {Function} warn - Prints standard warn with newline.
 */
