/**
 * Module with statics to handle stream management.
 *
 * @module stream
 * @private
 */

'use strict';

const equal = require('fast-deep-equal');
const { promisify } = require('util');

const KinesisClient = require('./kinesis-client');

const CONSUMER_STATE_CHECK_DELAY = 3000;

const wait = promisify(setTimeout);

/**
 * Checks if the given stream exists. If the stream is getting deleted or in the middle of an
 * update, it will wait for the status change completion. If the stream exists, the stream ARN and
 * created-on timestamp is returned. If the stream doesn't exist, the ARN is set to `null`.
 *
 * @param {Object} params - The parameters.
 * @param {KinesisClient} params.client - An instance of the Kinesis client.
 * @param {Logger} params.logger - An instance of a logger.
 * @param {string} params.streamName - The name of the stream to check for.
 * @fulfil {StreamExistsOutput} - An object with `streamArn` (the stream ARN) and `streamCreatedOn` (the
 *        stream creation timestamp). If the stream doesn't exist, `streamArn` is set to `null`.
 * @returns {Promise<StreamExistsOutput>}
 * @memberof module:stream
 */
async function checkIfStreamExists({ client, logger, streamName }) {
  try {
    const params = { StreamName: streamName };
    const { StreamDescriptionSummary } = await client.describeStreamSummary(params);
    const { StreamARN, StreamCreationTimestamp, StreamStatus } = StreamDescriptionSummary;

    if (StreamStatus === 'DELETING') {
      logger.debug('Waiting for the stream to complete deletion…');
      await client.waitFor('streamNotExists', params);
      logger.debug('The stream is now gone.');
      return { streamArn: null };
    }

    if (StreamStatus && StreamStatus !== 'ACTIVE') {
      logger.debug('Waiting for the stream to be active…');
      await client.waitFor('streamExists', params);
      logger.debug('The stream is now active.');
    }

    return {
      streamArn: StreamARN,
      streamCreatedOn: StreamCreationTimestamp.toISOString()
    };
  } catch (/** @type {any} */ err) {
    if (err.code === 'ResourceNotFoundException') {
      return { streamArn: null };
    }
    logger.error(err);
    throw err;
  }
}

/**
 * Checks if the given stream is tagged as specified. If the stream is currently tagged with
 * different tags, the tags are merged and the stream is tagged with them. If the stream has no
 * tags, it will get tagged with the specified ones.
 *
 * @param {Object} params - The parameters.
 * @param {KinesisClient} params.client - An instance of the Kinesis client.
 * @param {Logger} params.logger - An instance of a logger.
 * @param {string} params.streamName - The stream to check the tags for.
 * @param {Object} params.tags - The wanted tags.
 * @fulfil {undefined}
 * @returns {Promise<void>}
 * @memberof module:stream
 */
async function confirmStreamTags({ client, logger, streamName, tags }) {
  const params = { StreamName: streamName };
  const { Tags } = await client.listTagsForStream(params);
  const existingTags = Object.fromEntries(Tags.map(({ Key, Value }) => [Key, Value]));
  const mergedTags = { ...existingTags, ...tags };

  if (!equal(existingTags, mergedTags)) {
    await client.addTagsToStream({ ...params, Tags: mergedTags });
    logger.debug(`The stream tags have been updated.`);
  } else {
    logger.debug('The stream is already tagged as required.');
  }
}

/**
 * Ensures that the stream is encrypted as specified. If not encrypted, the stream will get
 * encrypted and the call won't resolve until the stream update process has completed.
 *
 * @param {Object} params - The parameters.
 * @param {KinesisClient} params.client - An instance of the Kinesis client.
 * @param {Object} params.encryption - The encryption options to enforce.
 * @param {string} params.encryption.keyId - The GUID for the customer-managed AWS KMS key
 *        to use for encryption. This value can be a globally unique identifier, a fully
 *        specified ARN to either an alias or a key, or an alias name prefixed by "alias/".
 * @param {string} params.encryption.type - The encryption type to use.
 * @param {Logger} params.logger - An instance of the logger.
 * @param {string} params.streamName - The stream to check for encryption.
 * @fulfil {undefined}
 * @returns {Promise<void>}
 * @memberof module:stream
 */
async function ensureStreamEncription(params) {
  const { client, encryption, logger, streamName: StreamName } = params;
  const { keyId: KeyId, type: EncryptionType } = encryption;

  const { StreamDescriptionSummary } = await client.describeStreamSummary({ StreamName });

  if (StreamDescriptionSummary.EncryptionType === 'NONE') {
    logger.debug('Trying to encrypt the stream…');
    await client.startStreamEncryption({ EncryptionType, KeyId, StreamName });
    logger.debug('Waiting for the stream to update…');
    await client.waitFor('streamExists', { StreamName });
    logger.debug('The stream is now encrypted.');
  } else {
    logger.debug('The stream is already encrypted.');
  }
}

/**
 * Ensures that the specified stream exists. If it doesn't exist, it process to create it and
 * wait until the new stream is activated and ready to go.
 *
 * @param {Object} params - The parameters.
 * @param {KinesisClient} params.client - An intance of the Kinesis client.
 * @param {boolean} params.createStreamIfNeeded - Whether if the Kinesis stream should
 *        be automatically created if it doesn't exist upon connection.
 * @param {Logger} params.logger - An instance of a logger.
 * @param {number} params.shardCount - The number of shards that the newly-created stream
 *        will use (if the `createStreamIfNeeded` option is set)
 * @param {string} params.streamName - The name of the stream to check/create.
 * @fulfil {Object} - An object with `streamArn` (the stream ARN) and `streamCreatedOn` (the
 *        stream creation timestamp). If the stream doesn't exist, `streamArn` is set to `null`.
 * @returns {Promise<StreamExistsOutput>}
 * @memberof module:stream
 */
async function ensureStreamExists(params) {
  const { client, createStreamIfNeeded, logger, shardCount, streamName } = params;
  logger.debug(`Verifying the "${streamName}" stream exists and it's active…`);

  const { streamArn, streamCreatedOn } = await checkIfStreamExists(params);

  if (createStreamIfNeeded && streamArn === null) {
    logger.debug('Trying to create the stream…');
    const awsParams = { StreamName: streamName };
    await client.createStream({ ...awsParams, ShardCount: shardCount });
    logger.debug('Waiting for the new stream to be active…');
    const { StreamDescription } = await client.waitFor('streamExists', awsParams);
    logger.debug('The new stream is now active.');
    const { StreamARN, StreamCreationTimestamp } = StreamDescription;
    return {
      streamArn: StreamARN,
      streamCreatedOn: StreamCreationTimestamp.toISOString()
    };
  }

  logger.debug("The stream exists and it's active.");
  return { streamArn, streamCreatedOn };
}

/**
 * Retrieves a list of the enhanced fan-out consumers registered for the stream. If any of the
 * enhanced consumers is changing status, it will wait until they all are active.
 *
 * @param {Object} params - The parameters.
 * @param {KinesisClient} params.client - An instance of the Kinesis client.
 * @param {Logger} params.logger - An instance of a logger.
 * @param {string} params.streamArn - The ARN of the stream to retrieve enhanced consumers from.
 * @fulfil {Array<Object>} - An array of objects with the properties `arn` (the ARN of the
 *        enhanced consumer), and `status`.
 * @returns {Promise<Object<string, any>>}
 * @memberof module:stream
 */
async function getEnhancedConsumers(params) {
  const { client, logger, streamArn } = params;
  const { Consumers = [] } = await client.listStreamConsumers({ StreamARN: streamArn });
  const consumers = Object.fromEntries(
    Consumers.map((consumer) => [
      consumer.ConsumerName,
      {
        arn: consumer.ConsumerARN,
        status: consumer.ConsumerStatus
      }
    ])
  );
  const shouldWaitForConsumer = Object.keys(consumers).some(
    (consumerName) => consumers[consumerName].status !== 'ACTIVE'
  );
  if (shouldWaitForConsumer) {
    logger.debug(`Waiting until all enhanced consumers are active…`);
    await wait(CONSUMER_STATE_CHECK_DELAY);
    return getEnhancedConsumers(params);
  }
  return consumers;
}

/**
 * Returns an object with the information of the stream shards. For each pair, the key corresponds
 * to the shard ID, while the value stores the details for the shard.
 *
 * @param {Object} params - The parameters.
 * @param {KinesisClient} params.client - An instance of the Kinesis client.
 * @param {Logger} params.logger - An instance of a logger.
 * @param {string} params.streamName - The name of the stream to get shards for.
 * @fulfil {Object} - The shard information as an object hashed by shard ID.
 * @returns {Promise<Object.<string, { parent: string, startingSequenceNumber: string }>>}
 * @memberof module:stream
 */
async function getStreamShards({ client, logger, streamName }) {
  logger.debug(`Retrieving shards for the "${streamName}" stream…`);

  const { Shards = [] } = await client.listShards({ StreamName: streamName });

  const shards = Object.fromEntries(
    Shards.map(({ ParentShardId, SequenceNumberRange, ShardId }) => {
      return [
        [ShardId],
        {
          parent: ParentShardId || null,
          startingSequenceNumber: SequenceNumberRange.StartingSequenceNumber
        }
      ];
    })
  );

  Object.keys(shards).forEach((id) => {
    const shard = shards[id];
    const { parent } = shard;
    if (parent && !shards[parent]) {
      shard.parent = null;
    }
  });

  return shards;
}

/**
 * Registers a new enhanced fan-out consumer for the given stream. The call won't resolve until
 * the new enhanced consumer has become active.
 *
 * @param {Object} params - The parameters.
 * @param {KinesisClient} params.client - An instance of the Kinesis client.
 * @param {string} params.consumerName - The name for the new enhanced fan-out consumer.
 * @param {Logger} params.logger - An instance of a logger.
 * @param {string} params.streamArn - The ARN of the stream to register the consumer on.
 * @fulfil {undefined}
 * @returns {Promise<void>}
 * @memberof module:stream
 */
async function registerEnhancedConsumer({ client, consumerName, logger, streamArn }) {
  logger.debug(`Registering enhanced consumer "${consumerName}"…`);
  let { ConsumerStatus } = (
    await client.registerStreamConsumer({
      ConsumerName: consumerName,
      StreamARN: streamArn
    })
  ).Consumer;
  logger.debug(`Waiting for the new enhanced consumer "${consumerName}" to be active…`);
  do {
    await wait(CONSUMER_STATE_CHECK_DELAY);
    const { Consumers = [] } = await client.listStreamConsumers({ StreamARN: streamArn });
    const consumer = Consumers.find((i) => i.ConsumerName === consumerName);
    if (consumer) ({ ConsumerStatus } = consumer);
    else ConsumerStatus = '';
  } while (ConsumerStatus !== 'ACTIVE');
  logger.debug(`The enhanced consumer "${consumerName}" is now active.`);
}

module.exports = {
  checkIfStreamExists,
  confirmStreamTags,
  ensureStreamEncription,
  ensureStreamExists,
  getEnhancedConsumers,
  getStreamShards,
  registerEnhancedConsumer
};

/**
 * @typedef StreamExistsOutput
 * @property {?string} streamArn - The stream Amazon resource name.
 * @property {string} [streamCreatedOn] - The timestamp string of when the stream was created on.
 */

/**
 * @typedef Logger
 * @property {Function} debug - Prints standard debug with newline.
 * @property {Function} error - Prints standard error with newline.
 * @property {Function} warn - Prints standard warn with newline.
 */
