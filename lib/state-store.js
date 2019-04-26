'use strict';

const projectName = require('project-name');
const { generate } = require('short-uuid');
const { hostname } = require('os');

const DynamoDbClient = require('./dynamodb-client');
const { confirmTableTags, ensureTableExists } = require('./table');
const { name } = require('../package.json');

const appName = projectName(process.cwd());
const host = hostname();
const privateData = new WeakMap();
const { pid, uptime } = process;

/**
 * Provides access to the private data of the specified instance.
 *
 * @param {Object} instance - The private data's owner.
 * @returns {Object} The private data.
 */
function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

async function getStreamState(instance) {
  const privateProps = internal(instance);
  const { client, consumerGroup, streamName } = privateProps;
  const params = { Key: { consumerGroup, streamName }, ConsistentRead: true };
  const { Item } = await client.get(params);
  return Item;
}

async function initStreamState(instance) {
  const privateProps = internal(instance);
  const { client, consumerGroup, logger, streamName, streamCreatedOn } = privateProps;

  const Key = { consumerGroup, streamName };
  const { Item } = await client.get({ Key });
  if (Item && Item.streamCreatedOn !== streamCreatedOn) {
    await client.delete({ Key });
    logger.warn('Stream state has been reset. Non-matching stream creation timestamp.');
  }

  try {
    await client.put({
      ConditionExpression: 'attribute_not_exists(streamName)',
      Item: {
        consumerGroup,
        consumers: {},
        shards: {},
        streamCreatedOn,
        streamName,
        version: generate()
      }
    });
    logger.debug('Initial state has been recorded for the stream.');
  } catch (err) {
    if (err.code !== 'ConditionalCheckFailedException') {
      logger.error(err);
      throw err;
    }
  }
}

/**
 * Class that encapsulates the DynamoDB table where the shared state for the stream is stored.
 */
class StateStore {
  /**
   * Initializes an instance of the state store.
   *
   * @param {Object} options - The initialization options.
   * @param {Object} options.dynamoDb - The initialization options passed to the Kinesis
   *        client module, specific for the DynamoDB state data table. This object can also
   *        contain any of the [`AWS.DynamoDB` options]{@link external:dynamoDbConstructor}.
   * @param {string} [options.dynamoDb.tableName=lifion-kinesis-state] - The name of the
   *        table where the shared state is stored.
   * @param {Object} [options.dynamoDb.tags={}] - If specified, the module will ensure
   *        the table has these tags during start.
   * @param {Object} options.logger - A logger instance.
   * @param {string} options.streamName - The name of the stream to keep state for.
   */
  constructor(options) {
    const {
      consumerGroup,
      consumerId,
      dynamoDb: { tableName, tags, ...awsOptions },
      logger,
      streamCreatedOn,
      streamName,
      useAutoShardAssignment
    } = options;

    const isStandalone = !useAutoShardAssignment;
    const shardsPath = isStandalone ? '#a0.#a1.#a2' : '#a';
    const shardsPathNames = isStandalone
      ? { '#a0': 'consumers', '#a1': consumerId, '#a2': 'shards' }
      : { '#a': 'shards' };

    Object.assign(internal(this), {
      awsOptions,
      consumerGroup,
      consumerId,
      isStandalone,
      logger,
      shardsPath,
      shardsPathNames,
      streamCreatedOn,
      streamName,
      tableName: tableName || `${name}-state`,
      tags
    });
  }

  /**
   * Starts the state store by initializing a DynamoDB client and a document client. Then,
   * it will ensure the table exists, that is tagged as required, and there's an entry for
   * the stream state.
   */
  async start() {
    const privateProps = internal(this);
    const { tags } = privateProps;

    const client = new DynamoDbClient(privateProps);
    privateProps.client = client;

    privateProps.tableArn = await ensureTableExists(privateProps);
    if (tags) await confirmTableTags(privateProps);
    await initStreamState(this);
  }

  async clearOldConsumers(heartbeatFailureTimeout) {
    const privateProps = internal(this);
    const { consumerGroup, client, logger, streamName } = privateProps;

    const { consumers, version } = await getStreamState(this);
    const consumerIds = Object.keys(consumers);

    const oldConsumers = consumerIds.filter(id => {
      const { heartbeat } = consumers[id];
      return Date.now() - new Date(heartbeat).getTime() > heartbeatFailureTimeout;
    });

    if (oldConsumers.length === 0) return;

    try {
      await client.update({
        Key: { consumerGroup, streamName },
        UpdateExpression: `REMOVE ${oldConsumers
          .map((id, index) => `#a.#${index}`)
          .join(', ')} SET #b = :x`,
        ConditionExpression: `#b = :y`,
        ExpressionAttributeNames: {
          '#a': 'consumers',
          '#b': 'version',
          ...oldConsumers.reduce((obj, id, index) => ({ ...obj, [`#${index}`]: id }), {})
        },
        ExpressionAttributeValues: {
          ':x': generate(),
          ':y': version
        }
      });
      logger.debug(
        `Cleared ${oldConsumers.length} old consumer(s): ${oldConsumers
          .map(i => `"${i}"`)
          .join(', ')}`
      );
    } catch (err) {
      if (err.code !== 'ConditionalCheckFailedException') {
        logger.error(err);
        throw err;
      }
      logger.debug('Old consumers were cleared somewhere else.');
    }
  }

  async registerConsumer() {
    const { client, consumerGroup, consumerId, isStandalone, logger, streamName } = internal(this);

    const heartbeat = new Date().toISOString();
    const startedOn = new Date(Date.now() - uptime() * 1000).toISOString();

    try {
      await client.update({
        Key: { consumerGroup, streamName },
        UpdateExpression: 'SET #a.#b = :x',
        ConditionExpression: 'attribute_not_exists(#a.#b)',
        ExpressionAttributeNames: {
          '#a': 'consumers',
          '#b': consumerId
        },
        ExpressionAttributeValues: {
          ':x': Object.assign(
            { appName, heartbeat, host, isStandalone, pid, startedOn },
            isStandalone && { shards: {} }
          )
        }
      });
      logger.debug(`The consumer "${consumerId}" is now registered.`);
    } catch (err) {
      if (err.code === 'ConditionalCheckFailedException') {
        await client
          .update({
            Key: { consumerGroup, streamName },
            UpdateExpression: 'SET #a.#b.#c = :x',
            ExpressionAttributeNames: {
              '#a': 'consumers',
              '#b': consumerId,
              '#c': 'heartbeat'
            },
            ExpressionAttributeValues: {
              ':x': heartbeat
            }
          })
          .catch(() => {
            logger.debug(`Missed heartbeat for "${consumerId}".`);
          });
        return;
      }
      logger.error(err);
      throw err;
    }
  }

  async ensureShardStateExists(shardId, shardData) {
    const privateProps = internal(this);
    const { client, consumerGroup, logger, shardsPath, shardsPathNames, streamName } = privateProps;
    const { parent } = shardData;

    try {
      await client.update({
        Key: { consumerGroup, streamName },
        UpdateExpression: `SET ${shardsPath}.#b = :x`,
        ConditionExpression: `attribute_not_exists(${shardsPath}.#b)`,
        ExpressionAttributeNames: { ...shardsPathNames, '#b': shardId },
        ExpressionAttributeValues: {
          ':x': {
            parent,
            checkpoint: null,
            depleted: false,
            leaseExpiration: null,
            leaseOwner: null,
            version: generate()
          }
        }
      });
    } catch (err) {
      if (err.code !== 'ConditionalCheckFailedException') {
        logger.error(err);
        throw err;
      }
    }
  }

  async getShardAndStreamState(shardId, shardData) {
    const { consumerId, isStandalone } = internal(this);

    const getState = async () => {
      const streamState = await getStreamState(this);
      const { consumers } = streamState;
      const { shards } = isStandalone ? consumers[consumerId] : streamState;
      const shardState = shards[shardId];
      return { streamState, shardState };
    };

    const states = await getState();
    if (states.shardState !== undefined) return states;
    await this.ensureShardStateExists(shardId, shardData);
    return getState();
  }

  async lockShardLease(shardId, leaseTermTimeout, version) {
    const {
      client,
      consumerGroup,
      consumerId,
      logger,
      shardsPath,
      shardsPathNames,
      streamName
    } = internal(this);

    try {
      await client.update({
        Key: { consumerGroup, streamName },
        UpdateExpression: `SET ${[
          `${shardsPath}.#b.#c = :w`,
          `${shardsPath}.#b.#d = :x`,
          `${shardsPath}.#b.#e = :y`
        ].join(', ')}`,
        ConditionExpression: `${shardsPath}.#b.#e = :z`,
        ExpressionAttributeNames: {
          ...shardsPathNames,
          '#b': shardId,
          '#c': 'leaseOwner',
          '#d': 'leaseExpiration',
          '#e': 'version'
        },
        ExpressionAttributeValues: {
          ':w': consumerId,
          ':x': new Date(Date.now() + leaseTermTimeout).toISOString(),
          ':y': generate(),
          ':z': version
        }
      });
      return true;
    } catch (err) {
      if (err.code !== 'ConditionalCheckFailedException') {
        logger.error(err);
        throw err;
      }
      return false;
    }
  }

  async releaseShardLease(shardId, version) {
    const privateProps = internal(this);
    const { client, consumerGroup, logger, shardsPath, shardsPathNames, streamName } = privateProps;
    const releasedVersion = generate();

    try {
      await client.update({
        Key: { consumerGroup, streamName },
        UpdateExpression: `SET ${[
          `${shardsPath}.#b.#c = :w`,
          `${shardsPath}.#b.#d = :x`,
          `${shardsPath}.#b.#e = :y`
        ].join(', ')}`,
        ConditionExpression: `${shardsPath}.#b.#e = :z`,
        ExpressionAttributeNames: {
          ...shardsPathNames,
          '#b': shardId,
          '#c': 'leaseOwner',
          '#d': 'leaseExpiration',
          '#e': 'version'
        },
        ExpressionAttributeValues: {
          ':w': null,
          ':x': null,
          ':y': releasedVersion,
          ':z': version
        }
      });
      return releasedVersion;
    } catch (err) {
      if (err.code !== 'ConditionalCheckFailedException') {
        logger.error(err);
        throw err;
      }
      return null;
    }
  }

  async storeShardCheckpoint(shardId, checkpoint) {
    const { client, consumerGroup, shardsPath, shardsPathNames, streamName } = internal(this);

    await client.update({
      Key: { consumerGroup, streamName },
      UpdateExpression: `SET ${shardsPath}.#b.#c = :x, ${shardsPath}.#b.#d = :y`,
      ExpressionAttributeNames: {
        ...shardsPathNames,
        '#b': shardId,
        '#c': 'checkpoint',
        '#d': 'version'
      },
      ExpressionAttributeValues: {
        ':x': checkpoint,
        ':y': generate()
      }
    });
  }

  async markShardAsDepleted(shardsData, parentShardId) {
    const { client, consumerGroup, shardsPath, shardsPathNames, streamName } = internal(this);

    const { shards } = await getStreamState(this);
    const parentShard = shards[parentShardId];

    const childrenShards = parentShard.checkpoint
      ? Object.keys(shardsData)
          .filter(shardId => shardsData[shardId].parent === parentShardId)
          .map(shardId => {
            const { startingSequenceNumber } = shardsData[shardId];
            return { shardId, startingSequenceNumber };
          })
      : [];

    await Promise.all(
      childrenShards.map(childrenShard => {
        return this.ensureShardStateExists(
          childrenShard.shardId,
          shardsData[childrenShard.shardId]
        );
      })
    );

    await client.update({
      Key: { consumerGroup, streamName },
      UpdateExpression: `SET ${[
        `${shardsPath}.#b.#c = :x`,
        `${shardsPath}.#b.#d = :y`,
        ...childrenShards.map((childShard, index) =>
          [
            `${shardsPath}.#${index}.#e = :${index * 2}`,
            `${shardsPath}.#${index}.#d = :${index * 2 + 1}`
          ].join(', ')
        )
      ].join(', ')}`,
      ExpressionAttributeNames: Object.assign(
        {
          ...shardsPathNames,
          '#b': parentShardId,
          '#c': 'depleted',
          '#d': 'version'
        },
        childrenShards.length > 0 && { '#e': 'checkpoint' },
        childrenShards.reduce(
          (obj, childShard, index) => ({ ...obj, [`#${index}`]: childShard.shardId }),
          {}
        )
      ),
      ExpressionAttributeValues: {
        ':x': true,
        ':y': generate(),
        ...childrenShards.reduce(
          (obj, childShard, index) => ({
            ...obj,
            [`:${index * 2}`]: childShard.startingSequenceNumber,
            [`:${index * 2 + 1}`]: generate()
          }),
          {}
        )
      }
    });
  }

  async getOwnedShards() {
    const { consumerId, isStandalone } = internal(this);

    const streamState = await getStreamState(this);
    const { consumers } = streamState;
    const { shards } = isStandalone ? consumers[consumerId] : streamState;

    return Object.keys(shards)
      .filter(shardId => shards[shardId].leaseOwner === consumerId)
      .reduce((obj, shardId) => {
        const { checkpoint, leaseExpiration, version } = shards[shardId];
        return { ...obj, [shardId]: { checkpoint, leaseExpiration, version } };
      }, {});
  }
}

/**
 * @external dynamoDbConstructor
 * @see https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#constructor-property
 */

module.exports = StateStore;