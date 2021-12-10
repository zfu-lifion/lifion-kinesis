/**
 * A module with statics to handle S3 buckets.
 *
 * @module bucket
 * @private
 */

'use strict';

const equal = require('fast-deep-equal');

/**
 * Checks if the specified bucket exists.
 *
 * @param {Object} params - The params.
 * @param {string} params.bucketName - The name of the bucket to check.
 * @param {import('./s3-client')} params.client - An instance of the S3 client.
 * @param {import('.').Logger} params.logger - A logger instance.
 * @returns {Promise<void>} Resolves if the bucket exists, rejects otherwise.
 * @private
 */
async function checkIfBucketExists({ bucketName, client, logger }) {
  try {
    const params = { Bucket: bucketName };
    await client.headBucket(params);
    logger.debug('Bucket exists and is accessible.');
  } catch (err) {
    logger.debug('Bucket is not accessible.');
    throw err;
  }
}

/**
 * Ensures that the bucket is tagged as expected by reading the tags then updating them if needed.
 *
 * @param {Object} params - The params.
 * @param {string} params.bucketName - The name of the bucket to check.
 * @param {import('./s3-client')} params.client - An instance of the S3 client.
 * @param {import('.').Logger} params.logger - A logger instance.
 * @param {Object} params.tags - The tags that should be present in the bucket.
 * @returns {Promise<void>} Resolves if the bucket is tagged properly, rejects otherwise.
 * @memberof module:bucket
 */
async function confirmBucketTags({ bucketName, client, logger, tags }) {
  const params = { Bucket: bucketName };
  const { TagSet } = await client.getBucketTagging(params);
  const existingTags = Object.fromEntries(TagSet.map(({ Key, Value }) => [Key, Value]));
  const mergedTags = { ...existingTags, ...tags };

  if (!equal(existingTags, mergedTags)) {
    const mergedTagSet = Object.entries(mergedTags).map(([Key, Value]) => ({ Key, Value }));

    await client.putBucketTagging({
      Bucket: bucketName,
      Tagging: { TagSet: mergedTagSet }
    });
    logger.debug('The bucket tags have been updated.');
  } else {
    logger.debug('The bucket is already tagged as required.');
  }
}

/**
 * Ensures that the bucket rules are defined properly
 *
 * @param {Object} params - The params.
 * @param {string} params.bucketName - The name of the bucket to check.
 * @param {import('./s3-client')} params.client - An instance of the S3 client.
 * @param {import('.').Logger} params.logger - A logger instance.
 * @param {Object} params.streamName - The name of the kinesis stream.
 * @returns {Promise<void>} Resolves if the bucket is ruled properly, rejects otherwise.
 * @memberof module:bucket
 */
async function confirmBucketLifecycleConfiguration({ bucketName, client, logger, streamName }) {
  const params = { Bucket: bucketName };
  const ruleId = 'lifion-kinesis-ttl-rule';
  const defaultRule = {
    AbortIncompleteMultipartUpload: {
      DaysAfterInitiation: 1
    },
    Expiration: {
      Days: 1
    },
    Filter: {
      Prefix: `${streamName}--`
    },
    ID: ruleId,
    NoncurrentVersionExpiration: {
      NoncurrentDays: 1
    },
    Status: 'Enabled'
  };

  const { Rules = [] } = await client.getBucketLifecycleConfiguration(params);
  if (!Rules.some((item) => item.ID === ruleId)) {
    await client.putBucketLifecycleConfiguration({
      ...params,
      LifecycleConfiguration: {
        Rules: [...Rules, defaultRule]
      }
    });
    logger.debug('The bucket rules have been updated.');
  } else {
    logger.debug('The bucket rules are already defined as required.');
  }
}

/**
 * Checks if a bucket exist and if not creates it.
 *
 * @param {Object} params - The params.
 * @param {import('./s3-client')} params.client - An instance of the S3 client.
 * @param {import('.').Logger} params.logger - A logger instance.
 * @param {string} params.bucketName - The name of the bucket to create.
 * @returns {Promise<void|Promise<import('aws-sdk').S3.CreateBucketOutput>>} Resolves if the bucket exists and is accessible, rejects otherwise.
 * @memberof module:bucket
 */
async function ensureBucketExists({ bucketName, client, logger }) {
  const params = { Bucket: bucketName };

  try {
    logger.debug(`Verifying the "${bucketName}" bucket exists and accessible…`);
    return await checkIfBucketExists({ bucketName, client, logger });
  } catch {
    logger.debug('Trying to create the bucket…');
    return client.createBucket(params);
  }
}

module.exports = {
  confirmBucketLifecycleConfiguration,
  confirmBucketTags,
  ensureBucketExists
};
