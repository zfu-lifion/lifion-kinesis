'use strict';

const LeaseManager = require('./lease-manager');
const { checkIfStreamExists, getStreamShards } = require('./stream');

jest.mock('./stream', () => ({
  checkIfStreamExists: jest.fn(() => Promise.resolve({ streamArn: 'stream-arn' })),
  getStreamShards: jest.fn(() => Promise.resolve({}))
}));

jest.useFakeTimers('legacy');

describe('lib/lease-manager', () => {
  const reconcile = jest.fn();
  const stop = jest.fn();
  const consumersManager = { reconcile, stop };

  const debug = jest.fn();
  const error = jest.fn();
  const logger = { debug, error };

  const getAssignedEnhancedConsumer = jest.fn();
  const getShardAndStreamState = jest.fn();
  const releaseShardLease = jest.fn();
  const lockShardLease = jest.fn(() => Promise.resolve(true));
  const stateStore = {
    getAssignedEnhancedConsumer,
    getShardAndStreamState,
    lockShardLease,
    releaseShardLease
  };

  const options = {
    consumerId: 'foo',
    consumersManager,
    logger,
    stateStore,
    useAutoShardAssignment: true
  };

  afterEach(() => {
    checkIfStreamExists.mockClear();
    clearTimeout.mockClear();
    debug.mockClear();
    error.mockClear();
    getAssignedEnhancedConsumer.mockClear();
    getShardAndStreamState.mockClear();
    getStreamShards.mockClear();
    lockShardLease.mockClear();
    reconcile.mockClear();
    releaseShardLease.mockClear();
    setTimeout.mockClear();
    stop.mockClear();
  });

  test('the module exports the expected', () => {
    expect(LeaseManager).toEqual(expect.any(Function));
    expect(LeaseManager).toThrow('Class constructor');
  });

  test("reconcile shouldn't be called on a stream with no shards", async () => {
    const manager = new LeaseManager(options);
    await expect(manager.start()).resolves.toBeUndefined();
    expect(reconcile).not.toHaveBeenCalled();
    expect(setTimeout).toHaveBeenCalledWith(expect.any(Function), expect.any(Number));
    expect(debug).toHaveBeenNthCalledWith(1, 'Attempting lease acquisition…');
    expect(debug).toHaveBeenCalledTimes(1);
    manager.stop();
  });

  test("calling start multiple times won't schedule multiple timeouts", async () => {
    const manager = new LeaseManager(options);
    await manager.start();
    await manager.start();
    expect(setTimeout).toHaveBeenCalledTimes(1);
    expect(debug).toHaveBeenNthCalledWith(1, 'Attempting lease acquisition…');
    expect(debug).toHaveBeenCalledTimes(1);
    manager.stop();
  });

  test('the manager can be stopped', async () => {
    const manager = new LeaseManager(options);
    await manager.start();
    manager.stop();
    jest.runOnlyPendingTimers();
    expect(checkIfStreamExists).toHaveBeenCalledTimes(1);
    expect(setTimeout).toHaveBeenCalledTimes(1);
    expect(clearTimeout).toHaveBeenCalled();
    expect(debug).toHaveBeenNthCalledWith(1, 'Attempting lease acquisition…');
    expect(debug).toHaveBeenNthCalledWith(2, 'The lease manager has stopped.');
    expect(debug).toHaveBeenCalledTimes(2);
  });

  test('the manager can be resumed', async () => {
    const manager = new LeaseManager(options);
    await manager.start();
    manager.stop();
    jest.runOnlyPendingTimers();
    await manager.start();
    expect(checkIfStreamExists).toHaveBeenCalledTimes(2);
    expect(setTimeout).toHaveBeenCalledTimes(2);
    expect(debug).toHaveBeenNthCalledWith(1, 'Attempting lease acquisition…');
    expect(debug).toHaveBeenNthCalledWith(2, 'The lease manager has stopped.');
    expect(debug).toHaveBeenNthCalledWith(3, 'Attempting lease acquisition…');
    expect(debug).toHaveBeenCalledTimes(3);
    manager.stop();
  });

  test('the manager should stop if the stream is gone', async () => {
    checkIfStreamExists.mockResolvedValueOnce({ streamArn: null });
    const manager = new LeaseManager(options);
    await manager.start();
    expect(debug).toHaveBeenNthCalledWith(1, 'Attempting lease acquisition…');
    expect(debug).toHaveBeenNthCalledWith(2, "Can't acquire leases as the stream is gone.");
    expect(debug).toHaveBeenNthCalledWith(3, 'The lease manager has stopped.');
    expect(debug).toHaveBeenCalledTimes(3);
    expect(stop).toHaveBeenCalled();
    manager.stop();
  });

  test("don't lease shards if fan-out is on but there are no enhanced consumers", async () => {
    const manager = new LeaseManager({ ...options, useEnhancedFanOut: true });
    await manager.start();
    expect(getStreamShards).not.toHaveBeenCalled();
    expect(debug).toHaveBeenCalledTimes(1);
    expect(debug).toHaveBeenNthCalledWith(1, 'Attempting lease acquisition…');
    manager.stop();
  });

  test("lease shards if fan-out is on and there's an assigned enhanced consumer", async () => {
    getAssignedEnhancedConsumer.mockResolvedValueOnce('consumer-arn');
    const manager = new LeaseManager({ ...options, useEnhancedFanOut: true });
    await manager.start();
    expect(getStreamShards).toHaveBeenCalled();
    expect(debug).toHaveBeenNthCalledWith(1, 'Attempting lease acquisition…');
    expect(debug).toHaveBeenCalledTimes(1);
    manager.stop();
  });

  test('shards marked as depleted cannot be leased', async () => {
    getStreamShards.mockResolvedValueOnce({ 'shardId-0000': {} });
    getShardAndStreamState.mockResolvedValueOnce({
      shardState: { depleted: true },
      streamState: { shards: {} }
    });
    const manager = new LeaseManager(options);
    await manager.start();
    expect(reconcile).not.toHaveBeenCalled();
    expect(debug).toHaveBeenNthCalledWith(1, 'Attempting lease acquisition…');
    expect(debug).toHaveBeenNthCalledWith(
      2,
      'Shard "shardId-0000" has been marked as depleted. Can\'t be leased.'
    );
    expect(debug).toHaveBeenCalledTimes(2);
    manager.stop();
  });

  test('leased shards with an active lease period should not cause changes', async () => {
    getStreamShards.mockResolvedValueOnce({ 'shardId-0000': {} });
    const fiveMinsFromNow = new Date(Date.now() + 1000 * 60 * 5);
    getShardAndStreamState.mockResolvedValueOnce({
      shardState: { leaseExpiration: fiveMinsFromNow.toISOString(), leaseOwner: 'foo' },
      streamState: { shards: {} }
    });
    const manager = new LeaseManager(options);
    await manager.start();
    expect(reconcile).not.toHaveBeenCalled();
    expect(debug).toHaveBeenNthCalledWith(1, 'Attempting lease acquisition…');
    expect(debug).toHaveBeenNthCalledWith(
      2,
      'Shard "shardId-0000" is currently owned by this consumer.'
    );
    expect(debug).toHaveBeenCalledTimes(2);
    manager.stop();
  });

  test('a shard lease failure to release should trigger reconciliation', async () => {
    getStreamShards.mockResolvedValueOnce({ 'shardId-0000': {} });
    const fiveMinsFromNow = new Date(Date.now() + 1000 * 60 * 5);
    getShardAndStreamState.mockResolvedValueOnce({
      shardState: { leaseExpiration: fiveMinsFromNow.toISOString(), leaseOwner: 'bar' },
      streamState: { consumers: {}, shards: {} }
    });
    const manager = new LeaseManager(options);
    await manager.start();
    expect(reconcile).toHaveBeenCalled();
    expect(debug).toHaveBeenNthCalledWith(1, 'Attempting lease acquisition…');
    expect(debug).toHaveBeenNthCalledWith(
      2,
      'The lease for shard "shardId-0000" couldn\'t be released.'
    );
    expect(debug).toHaveBeenCalledTimes(2);
    manager.stop();
  });

  test("an active shard lease by another active consumer shouldn't be leased", async () => {
    getStreamShards.mockResolvedValueOnce({ 'shardId-0000': {} });
    const fiveMinsFromNow = new Date(Date.now() + 1000 * 60 * 5);
    getShardAndStreamState.mockResolvedValueOnce({
      shardState: { leaseExpiration: fiveMinsFromNow.toISOString(), leaseOwner: 'bar' },
      streamState: { consumers: { bar: {} }, shards: {} }
    });
    const manager = new LeaseManager(options);
    await manager.start();
    expect(reconcile).not.toHaveBeenCalled();
    expect(debug).toHaveBeenNthCalledWith(1, 'Attempting lease acquisition…');
    expect(debug).toHaveBeenNthCalledWith(2, 'The shard "shardId-0000" is owned by "bar".');
    expect(debug).toHaveBeenCalledTimes(2);
    manager.stop();
  });

  test("shards with non-depleted parents shouldn't be leased", async () => {
    getStreamShards.mockResolvedValueOnce({ 'shardId-0001': {} });
    getShardAndStreamState.mockResolvedValueOnce({
      shardState: { parent: 'shardId-0000' },
      streamState: { shards: { 'shardId-0000': { depleted: false } } }
    });
    const manager = new LeaseManager(options);
    await manager.start();
    expect(reconcile).not.toHaveBeenCalled();
    expect(debug).toHaveBeenNthCalledWith(1, 'Attempting lease acquisition…');
    expect(debug).toHaveBeenNthCalledWith(
      2,
      'Cannot lease "shardId-0001", the parent "shardId-0000" hasn\'t been depleted.'
    );
    expect(debug).toHaveBeenCalledTimes(2);
    manager.stop();
  });

  test("the manager can't acquire leases over the maximum allowed active leases", async () => {
    getStreamShards.mockResolvedValueOnce({ 'shardId-0001': {} });
    getShardAndStreamState.mockResolvedValueOnce({
      shardState: {},
      streamState: {
        consumers: {
          bar: { isActive: true, isStandalone: false },
          foo: { isActive: true, isStandalone: false }
        },
        shards: {
          'shardId-0000': { depleted: false, leaseOwner: 'foo' },
          'shardId-0001': { depleted: false }
        }
      }
    });
    const manager = new LeaseManager(options);
    await manager.start();
    expect(reconcile).toHaveBeenCalled();
    expect(debug).toHaveBeenNthCalledWith(1, 'Attempting lease acquisition…');
    expect(debug).toHaveBeenNthCalledWith(
      2,
      'Max. of 1 active leases reached, can\'t lease "shardId-0001".'
    );
    expect(debug).toHaveBeenCalledTimes(2);
    manager.stop();
  });

  test('the shard is leased if all the required conditions are present', async () => {
    getStreamShards.mockResolvedValueOnce({ 'shardId-0000': {} });
    const fiveMinsFromNow = new Date(Date.now() + 1000 * 60 * 5);
    const streamState = { consumers: {}, shards: {} };
    getShardAndStreamState.mockResolvedValueOnce({
      shardState: { leaseExpiration: fiveMinsFromNow.toISOString(), version: 1 },
      streamState
    });
    const manager = new LeaseManager(options);
    await manager.start();
    expect(lockShardLease).toHaveBeenCalledWith('shardId-0000', 300000, 1, streamState);
    expect(reconcile).toHaveBeenCalled();
    expect(debug).toHaveBeenNthCalledWith(1, 'Attempting lease acquisition…');
    expect(debug).toHaveBeenNthCalledWith(2, 'Lease for "shardId-0000" acquired.');
    expect(debug).toHaveBeenCalledTimes(2);
    manager.stop();
  });

  test('the shard lease lock might fail even with the right required conditions', async () => {
    lockShardLease.mockResolvedValueOnce(false);
    getStreamShards.mockResolvedValueOnce({ 'shardId-0000': {} });
    const fiveMinsFromNow = new Date(Date.now() + 1000 * 60 * 5);
    getShardAndStreamState.mockResolvedValueOnce({
      shardState: { leaseExpiration: fiveMinsFromNow.toISOString() },
      streamState: { consumers: {}, shards: {} }
    });
    const manager = new LeaseManager(options);
    await manager.start();
    expect(reconcile).not.toHaveBeenCalled();
    expect(debug).toHaveBeenNthCalledWith(1, 'Attempting lease acquisition…');
    expect(debug).toHaveBeenNthCalledWith(
      2,
      'Can\'t acquire lease for "shardId-0000", someone else did it.'
    );
    expect(debug).toHaveBeenCalledTimes(2);
    manager.stop();
  });

  test('the manager can acquire leases over the maximum if in standalone mode', async () => {
    getStreamShards.mockResolvedValueOnce({ 'shardId-0001': {} });
    getShardAndStreamState.mockResolvedValueOnce({
      shardState: {},
      streamState: {
        consumers: {
          bar: { isActive: true, isStandalone: true },
          foo: { isActive: true, isStandalone: true }
        },
        shards: {
          'shardId-0000': { depleted: false },
          'shardId-0001': { depleted: false }
        }
      }
    });
    const manager = new LeaseManager({ ...options, useAutoShardAssignment: false });
    await manager.start();
    expect(reconcile).toHaveBeenCalled();
    expect(debug).toHaveBeenNthCalledWith(1, 'Attempting lease acquisition…');
    expect(debug).toHaveBeenNthCalledWith(2, 'Lease for "shardId-0001" acquired.');
    expect(debug).toHaveBeenCalledTimes(2);
    manager.stop();
  });

  test('the manager is able to renew leases about to expire', async () => {
    getStreamShards.mockResolvedValueOnce({ 'shardId-0000': {} });
    const oneMinFromNow = new Date(Date.now() + 1000 * 60);
    getShardAndStreamState.mockResolvedValueOnce({
      shardState: { leaseExpiration: oneMinFromNow.toISOString(), leaseOwner: 'foo' },
      streamState: { consumers: {}, shards: {} }
    });
    const manager = new LeaseManager(options);
    await manager.start();
    expect(reconcile).toHaveBeenCalled();
    expect(debug).toHaveBeenNthCalledWith(1, 'Attempting lease acquisition…');
    expect(debug).toHaveBeenNthCalledWith(
      2,
      'It\'s time to renew the lease of "shardId-0000" for this consumer.'
    );
    expect(debug).toHaveBeenNthCalledWith(3, 'Lease for "shardId-0000" acquired.');
    expect(debug).toHaveBeenCalledTimes(3);
    manager.stop();
  });

  test('expired leases owned by another consumer should be released and acquired', async () => {
    releaseShardLease.mockResolvedValueOnce(1);
    getStreamShards.mockResolvedValueOnce({ 'shardId-0000': {} });
    const fiveMinsAgo = new Date(Date.now() - 1000 * 60 * 5);
    getShardAndStreamState.mockResolvedValueOnce({
      shardState: { leaseExpiration: fiveMinsAgo.toISOString(), leaseOwner: 'shardId-0001' },
      streamState: { consumers: { 'shardId-0001': {} }, shards: {} }
    });
    const manager = new LeaseManager(options);
    await manager.start();
    expect(reconcile).toHaveBeenCalled();
    expect(debug).toHaveBeenNthCalledWith(1, 'Attempting lease acquisition…');
    expect(debug).toHaveBeenNthCalledWith(
      2,
      'Lease for shard "shardId-0000" released. The lease expired.'
    );
    expect(debug).toHaveBeenNthCalledWith(3, 'Lease for "shardId-0000" acquired.');
    expect(debug).toHaveBeenCalledTimes(3);
    manager.stop();
  });

  test('leases owned by unknown consumers should be released and acquired', async () => {
    releaseShardLease.mockResolvedValueOnce(1);
    getStreamShards.mockResolvedValueOnce({ 'shardId-0000': {} });
    const fiveMinFromNow = new Date(Date.now() + 1000 * 60 * 5);
    getShardAndStreamState.mockResolvedValueOnce({
      shardState: { leaseExpiration: fiveMinFromNow.toISOString(), leaseOwner: 'shardId-0000' },
      streamState: { consumers: {}, shards: {} }
    });
    const manager = new LeaseManager(options);
    await manager.start();
    expect(reconcile).toHaveBeenCalled();
    expect(debug).toHaveBeenNthCalledWith(1, 'Attempting lease acquisition…');
    expect(debug).toHaveBeenNthCalledWith(
      2,
      'Lease for shard "shardId-0000" released. The owner is gone.'
    );
    expect(debug).toHaveBeenNthCalledWith(3, 'Lease for "shardId-0000" acquired.');
    expect(debug).toHaveBeenCalledTimes(3);
    manager.stop();
  });

  test('the manager can recover from thrown errors when trying to acquire a lease', async () => {
    getStreamShards.mockResolvedValueOnce({ 'shardId-0000': {} });
    getShardAndStreamState.mockRejectedValueOnce(new Error('foo'));
    const manager = new LeaseManager(options);
    await manager.start();
    expect(error).toHaveBeenNthCalledWith(
      1,
      'Unexpected recoverable failure when trying to acquire a lease:',
      expect.objectContaining({ message: 'foo' })
    );
    expect(error).toHaveBeenCalledTimes(1);
  });

  test('the manager can recover from thrown errors when trying to acquire leases', async () => {
    checkIfStreamExists.mockRejectedValueOnce(new Error('foo'));
    const manager = new LeaseManager(options);
    await manager.start();
    expect(error).toHaveBeenNthCalledWith(
      1,
      'Unexpected recoverable failure when trying to acquire leases:',
      expect.objectContaining({ message: 'foo' })
    );
    expect(error).toHaveBeenCalledTimes(1);
  });
});
