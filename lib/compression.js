/**
 * Module with a suite of compress and decompress functions for different compression algorithms.
 *
 * @module compression
 * @private
 */

'use strict';

const { CompressedEncoding, compressAsync, decompressAsync } = require('lzutf8');

/**
 * @type {CompressionLib}
 */
module.exports = {
  /**
   * Compress and decompress methods for the LZ-UTF8 algorithm. LZ-UTF8 is an extension to the
   * [UTF-8]{@link external:UTF8} character encoding, augmenting the UTF-8 bytestream with
   * optional compression based the [LZ77]{@link external:LZ77} algorithm.
   */
  'LZ-UTF8': {
    /**
     * Compresses the given input using the specified encoding using LZ-UTF8.
     *
     * @param {Buffer} input - The buffer to compress.
     * @param {CompressedEncoding} outputEncoding - The encoding of the result.
     * @fulfil {Buffer} - The compressed input.
     * @returns {Promise<Buffer>}
     */
    compress: (input, outputEncoding) =>
      new Promise((resolve, reject) => {
        const options = { outputEncoding, useWebWorker: false };
        compressAsync(input, options, (output, err) => {
          if (!err) resolve(output);
          else reject(err);
        });
      }),
    /**
     * Decompresses the given input using the specified encoding using LZ-UTF8.
     *
     * @param {Buffer} input - The buffer to decompress.
     * @param {CompressedEncoding} inputEncoding - The encoding of the input buffer to decompress.
     * @fulfil {string} - A decompressed UTF-8 string.
     * @returns {Promise<string>}
     */
    decompress: (input, inputEncoding) =>
      new Promise((resolve, reject) => {
        const options = { inputEncoding, useWebWorker: false };
        decompressAsync(input, options, (output, err) => {
          if (!err) resolve(output);
          else reject(err);
        });
      })
  }
};

/**
 * @external UTF8
 * @see https://en.wikipedia.org/wiki/UTF-8
 */

/**
 * @external LZ77
 * @see https://en.wikipedia.org/wiki/LZ77_and_LZ78
 */

/**
 * @typedef CompressionLib
 * @type {Object.<string, CompressionHelper>}
 */

/**
 * @typedef CompressionHelper
 * @property {Function} compress - Compression function.
 * @property {Function} decompress - Decompression function.
 */
