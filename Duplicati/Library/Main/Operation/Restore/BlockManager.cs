// Copyright (C) 2025, The Duplicati Team
// https://duplicati.com, hello@duplicati.com
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CoCoL;
using Duplicati.Library.Main.Database;
using Microsoft.Extensions.Caching.Memory;

#nullable enable

namespace Duplicati.Library.Main.Operation.Restore
{

    /// <summary>
    /// Process that manages the block requests and responses to/from the
    /// `FileProcessor` process by caching the blocks.
    /// </summary>
    internal class BlockManager
    {
        /// <summary>
        /// The log tag for this class.
        /// </summary>
        private static readonly string LOGTAG = Logging.Log.LogTagFromType<BlockManager>();

        /// <summary>
        /// Manages block caching for restore operations with dual-path storage strategy.
        /// Blocks referenced by multiple volumes (above threshold) are routed to a SharedBlockStore
        /// for cross-volume reuse, while other blocks use a MemoryCache with per-volume tracking.
        /// Requesters receive a <c>Task</c> that completes when the block is available.
        /// Reference counting ensures blocks remain cached until all requests are fulfilled,
        /// and volume request channels are retired when the last reader completes.
        /// </summary>
        internal class SleepableDictionary : IDisposable
        {
            /// <summary>
            /// Lifecycle state for shared blocks accessed under m_blockcount_lock.<br/>
            /// Tracks whether blocks referenced by multiple volumes should use the SharedBlockStore or fall back to normal caching.<br/>
            /// State transitions are one-way: Pending -> (Stored | Bypassed).
            /// </summary>
            private enum SharedBlockState { 
                /// <summary>
                /// Block not yet stored. First request triggers download/store; subsequent requests wait in m_shared_waiters.
                /// </summary>
                Pending, 

                /// <summary>
                /// Block successfully stored in SharedBlockStore. All requests retrieve from shared storage. Volume count is decremented 
                /// by all remaining references at once upon store completion.
                /// </summary>
                Stored, 

                /// <summary>
                /// Block failed to store (free-space check or budget limit exceeded). Requests fall back to normal cache path with 
                /// per-reference volume count decrements.
                /// </summary>
                Bypassed 
            }

            /// <summary>
            /// Channel for submitting block requests from a volume.
            /// </summary>
            private readonly IWriteChannel<object> m_volume_request;
            /// <summary>
            /// The dictionary holding the cached blocks.
            /// </summary>
            private readonly MemoryCache m_block_cache;
            /// <summary>
            /// Secondary counter for the number of blocks in the cache. It's faster than the MemoryCache.Count property.
            /// </summary>
            private int m_block_cache_count;
            /// <summary>
            /// The maximum number of blocks to hold in the cache. If the number of blocks exceeds this value, the cache will be compacted. It is a cached value of <see cref="Options.RestoreCacheMax"/>.
            /// </summary>
            private long m_block_cache_max;
            /// <summary>
            /// Flag indicating if the cache is currently being compacted. Used to avoid triggering multiple compactions as they are expensive.
            /// Multiple triggers can occur as the eviction callback is launched in a task, so the count can be above the max for a short while.
            /// </summary>
            private int m_block_cache_compacting = 0;
            /// <summary>
            /// The dictionary holding the `Task` for each block request in flight.
            /// </summary>
            private readonly ConcurrentDictionary<long, (int Count, TaskCompletionSource<DataBlock> Task)> m_waiters = [];
            /// <summary>
            /// The number of readers accessing this dictionary. Used during shutdown / cleanup.
            /// </summary>
            private int m_readers = 0;
            /// <summary>
            /// Internal stopwatch for profiling the cache eviction.
            /// </summary>
            private readonly Stopwatch? sw_cacheevict;
            /// <summary>
            /// Internal stopwatch for profiling the `CheckCounts` method.
            /// </summary>
            private readonly Stopwatch? sw_checkcounts;
            /// <summary>
            /// Internal stopwatch for profiling setting up the waiters.
            /// </summary>
            private readonly Stopwatch? sw_get_wait;
            /// <summary>
            /// Internal stopwatch for profiling writing the block request to the volume request channel.
            /// </summary>
            private readonly Stopwatch? sw_get_write;
            /// <summary>
            /// Internal stopwatch for profiling setting a block in the cache.
            /// </summary>
            private readonly Stopwatch? sw_set_set;
            /// <summary>
            /// Internal stopwatch for profiling getting a waiter to notify that the block is available.
            /// </summary>
            private readonly Stopwatch? sw_set_wake_get;
            /// <summary>
            /// Internal stopwatch for profiling for waking up the waiting request.
            /// </summary>
            private readonly Stopwatch? sw_set_wake_set;

            /// <summary>
            /// Dictionary for keeping track of how many times each block is requested. Used to determine when a block is no longer needed.
            /// </summary>
            private readonly Dictionary<long, long> m_blockcount = new();
            /// <summary>
            /// Lock for the block count dictionary.
            /// </summary>
            private readonly object m_blockcount_lock = new();
            /// <summary>
            /// Dictionary for keeping track of how many times each volume is requested. Used to determine when a volume is no longer needed.
            /// </summary>
            private readonly Dictionary<long, long> m_volumecount = new();
            /// <summary>
            /// Snapshot of the initial block-reference count per volume, used for drain-progress logging.
            /// </summary>
            private readonly Dictionary<long, long> m_volumecount_initial = new();
            /// <summary>
            /// The options for the restore.
            /// </summary>
            private readonly Options m_options;
            /// <summary>
            /// The cache eviction options. Used for registering a callback when a block is evicted from the cache.
            /// </summary>
            private readonly MemoryCacheEntryOptions m_entry_options = new();
            /// <summary>
            /// Flag indicating if the dictionary has been retired. Used to avoid multiple retire attempts.
            /// </summary>
            private bool m_retired = false;

            /// <summary>
            /// The shared block store for cross-volume blocks.
            /// </summary>
            private readonly SharedBlockStore m_shared_block_store;
            /// <summary>
            /// Set of block IDs that are candidates for the shared-block path.
            /// </summary>
            private readonly HashSet<long> m_shared_blocks;
            /// <summary>
            /// Lifecycle state per shared block. Access under m_blockcount_lock.
            /// </summary>
            private readonly Dictionary<long, SharedBlockState> m_shared_block_state;
            /// <summary>
            /// Waiter tasks for callers of Get() while a shared block is Pending. Access under m_blockcount_lock.
            /// </summary>
            private readonly Dictionary<long, (int Count, TaskCompletionSource<DataBlock> Task)> m_shared_waiters;
            /// <summary>
            /// Temp directory path used for free-space probes in unlimited mode.
            /// </summary>
            private readonly string m_temp_dir;

            /// <summary>
            /// Last observed free space in the shared storage path. Used to trigger proactive bypass of the shared-block path when space is low.
            /// </summary>
            private long m_last_free_space_bytes = long.MaxValue;
            /// <summary>
            /// Cumulative bytes written to the SharedBlockStore since the last free space check. Used to trigger periodic re-checks.
            /// </summary>
            private long m_last_free_space_check_bytes_written = 0L;
            /// <summary>
            /// Cumulative bytes written to the SharedBlockStore. Used for monitoring and logging.
            /// </summary>
            private long m_bytes_written_to_shared = 0L;
            /// <summary>
            /// Flag to avoid spamming logs with free space probe failures.
            /// </summary>
            private bool m_free_space_probe_failed_logged = false;

            /// <summary>
            /// Initializes a new instance of the <see cref="SleepableDictionary"/> class.
            /// </summary>
            /// <param name="volume_request">Channel for submitting block requests from a volume.</param>
            /// <param name="readers">Number of readers accessing this dictionary. Used during shutdown / cleanup.</param>
            /// <param name="shared_blocks">Set of block IDs that are candidates for SharedBlockStore routing.</param>
            /// <param name="shared_block_store">The SharedBlockStore instance for routing shared blocks.</param>
            private SleepableDictionary(IWriteChannel<object> volume_request, Options options, int readers,
                HashSet<long> shared_blocks, SharedBlockStore shared_block_store)
            {
                m_options = options;
                m_volume_request = volume_request;
                var cache_options = new MemoryCacheOptions();
                m_block_cache = new MemoryCache(cache_options);
                m_block_cache_max = options.RestoreCacheMax;
                m_block_cache_count = 0;
                m_entry_options.RegisterPostEvictionCallback((key, value, reason, state) =>
                {
                    if (value is DataBlock dataBlock)
                    {
                        dataBlock.Dispose();
                    }
                    else if (value is null)
                    {
                        Logging.Log.WriteWarningMessage(LOGTAG, "CacheEvictCallback", null, "Evicted block {0} from cache, but the value was null", key);
                    }
                    else
                    {
                        Logging.Log.WriteWarningMessage(LOGTAG, "CacheEvictCallback", null, "Evicted block {0} from cache, but the value was of unexpected type {1}", key, value.GetType().FullName);
                    }
                    Interlocked.Decrement(ref m_block_cache_count);
                    Logging.Log.WriteExplicitMessage(LOGTAG, "CacheEvictCallback", "Evicted block {0} from cache", key);
                    if (reason is EvictionReason.Capacity)
                        Interlocked.Exchange(ref m_block_cache_compacting, 0);
                });
                m_readers = readers;
                sw_cacheevict = options.InternalProfiling ? new() : null;
                sw_checkcounts = options.InternalProfiling ? new() : null;
                sw_get_wait = options.InternalProfiling ? new() : null;
                sw_get_write = options.InternalProfiling ? new() : null;
                sw_set_set = options.InternalProfiling ? new() : null;
                sw_set_wake_get = options.InternalProfiling ? new() : null;
                sw_set_wake_set = options.InternalProfiling ? new() : null;

                m_shared_blocks = shared_blocks;
                m_shared_block_state = shared_blocks.ToDictionary(x => x, _ => SharedBlockState.Pending);
                m_shared_waiters = [];
                m_shared_block_store = shared_block_store;
                m_temp_dir = options.TempDir ?? System.IO.Path.GetTempPath();
            }

            /// <summary>
            /// Asynchronously creates a new instance of the <see cref="SleepableDictionary"/> class.
            /// Initializes block and volume counts from the database, identifies high-reference-count blocks
            /// for SharedBlockStore routing (based on <see cref="Options.RestoreSharedBlockCacheThreshold"/>),
            /// and configures the dual-path caching strategy with allocated budgets.
            /// </summary>
            /// <param name="db">The database holding information about how many of each block this restore requires.</param>
            /// <param name="volume_request">CoCoL channel for submitting block requests from a volume.</param>
            /// <param name="options">The restore options.</param>
            /// <param name="readers">The number of readers accessing this dictionary. Used during shutdown / cleanup.</param>
            /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
            /// <returns>A task that when awaited returns a new instance of the <see cref="SleepableDictionary"/> class.</returns>
            public static async Task<SleepableDictionary> CreateAsync(LocalRestoreDatabase db, IWriteChannel<object> volume_request, Options options, int readers, CancellationToken cancellationToken)
            {

                var blockcount = new Dictionary<long, long>();
                var volumecount = new Dictionary<long, long>();

                await foreach (var (block_id, volume_id) in db.GetBlocksAndVolumeIDs(options.SkipMetadata, cancellationToken).ConfigureAwait(false))
                {
                    blockcount[block_id] = blockcount.TryGetValue(block_id, out var c) ? c + 1 : 1;
                    volumecount[volume_id] = volumecount.TryGetValue(volume_id, out var v) ? v + 1 : 1;
                }

                int threshold = options.RestoreSharedBlockCacheThreshold;
                var shared_blocks = new HashSet<long>();
                var shared_block_store = SharedBlockStore.Empty;

                if (options.RestoreVolumeCacheHint != 0 && threshold > 0)
                {
                    foreach (var (block_id, count) in blockcount)
                        if (count > threshold)
                            shared_blocks.Add(block_id);

                    if (shared_blocks.Count > 0)
                    {
                        long store_budget = options.RestoreVolumeCacheHint > 0
                            ? RestoreCacheBudget.GetSharedBlockStoreBudget(options.RestoreVolumeCacheHint)
                            : -1L;
                        shared_block_store = new SharedBlockStore(options.TempDir ?? System.IO.Path.GetTempPath(), store_budget);
                    }

                    Logging.Log.WriteExplicitMessage(LOGTAG, "SharedBlockStore",
                        "SharedBlockStore initialized: {0} shared blocks (threshold={1}).",
                        shared_blocks.Count, threshold);
                }

                var sd = new SleepableDictionary(volume_request, options, readers, shared_blocks, shared_block_store);

                foreach (var (block_id, count) in blockcount)
                    sd.m_blockcount[block_id] = count;
                foreach (var (volume_id, count) in volumecount)
                    sd.m_volumecount[volume_id] = count;
                foreach (var kvp in sd.m_volumecount)
                    sd.m_volumecount_initial[kvp.Key] = kvp.Value;

                Logging.Log.WriteExplicitMessage(LOGTAG, "VolumeBlockCount",
                    "Volume tracking initialized: {0} volumes, {1} total block-references. Top 10 by block count: {2}",
                    sd.m_volumecount.Count,
                    sd.m_volumecount.Values.Sum(),
                    string.Join(", ", sd.m_volumecount.OrderByDescending(x => x.Value).Take(10).Select(x => $"{x.Key}:{x.Value}"))
                );

                return sd;
            }

            /// <summary>
            /// Peak bytes written to the SharedBlockStore TempFile(s).
            /// </summary>
            public long PeakSharedBlockStoreBytes => m_shared_block_store.PeakBytesWritten;

            /// <summary>
            /// Decrements block and volume reference counters after a block has been consumed.
            /// When a block's count reaches zero, it's removed from caches and SharedBlockStore references are decremented.
            /// Volume counts are only decremented for non-Stored shared blocks (Stored blocks had their volume counts
            /// pre-decremented in <see cref="Set"/>). When a volume's count reaches zero, a CacheEvict notification
            /// is sent to the VolumeDownloader.
            /// </summary>
            /// <param name="blockRequest">The block request to check.</param>
            public async Task CheckCounts(BlockRequest blockRequest)
            {
                long error_block_id = -1;
                long error_volume_id = -1;
                var emit_evict = false;
                long vol_count_after = 0;
                long vol_initial = 0;
                bool vol_milestone = false;

                Logging.Log.WriteExplicitMessage(LOGTAG, "CheckCounts", "Trying to acquire m_blockcount_lock for block {0}", blockRequest.BlockID);
                lock (m_blockcount_lock)
                {
                    sw_checkcounts?.Start();

                    var shared_state = m_shared_block_state.TryGetValue(blockRequest.BlockID, out var ss)
                        ? ss
                        : (SharedBlockState?)null;

                    if (m_blockcount.TryGetValue(blockRequest.BlockID, out var c))
                    {
                        var block_count = c - 1;

                        if (block_count > 0)
                        {
                            m_blockcount[blockRequest.BlockID] = block_count;
                        }
                        else if (block_count == 0)
                        {
                            m_blockcount.Remove(blockRequest.BlockID);
                            m_block_cache.Remove(blockRequest.BlockID);
                            if (shared_state == SharedBlockState.Stored)
                                m_shared_block_store.Decrement(blockRequest.BlockID);
                            m_shared_block_state.Remove(blockRequest.BlockID);
                        }
                        else // block_count < 0
                        {
                            error_block_id = blockRequest.BlockID;
                        }
                    }
                    else
                    {
                        error_block_id = blockRequest.BlockID;
                    }

                    // Only decrement volume count for non-Stored shared blocks.
                    // When a block is Stored, Set() already decremented the volume count
                    // by all remaining references at once.
                    if (shared_state != SharedBlockState.Stored)
                    {
                        if (m_volumecount.TryGetValue(blockRequest.VolumeID, out var vc))
                        {
                            var vol_count = vc - 1;
                            if (vol_count > 0)
                            {
                                m_volumecount[blockRequest.VolumeID] = vol_count;
                                if (m_volumecount_initial.TryGetValue(blockRequest.VolumeID, out var vi) && vi > 0)
                                {
                                    vol_initial = vi;
                                    vol_count_after = vol_count;
                                    vol_milestone = (vol_count * 10 / vi) != ((vol_count + 1) * 10 / vi);
                                }
                            }
                            else if (vol_count == 0)
                            {
                                m_volumecount_initial.TryGetValue(blockRequest.VolumeID, out vol_initial);
                                m_volumecount.Remove(blockRequest.VolumeID);
                                blockRequest.RequestType = BlockRequestType.CacheEvict;
                                emit_evict = true;
                            }
                            else // vol_count < 0
                            {
                                error_volume_id = blockRequest.VolumeID;
                            }
                        }
                        else
                        {
                            error_volume_id = blockRequest.VolumeID;
                        }
                    }

                    sw_checkcounts?.Stop();
                }
                Logging.Log.WriteExplicitMessage(LOGTAG, "CheckCounts", "Released m_blockcount_lock for block {0}", blockRequest.BlockID);

                if (vol_milestone)
                    Logging.Log.WriteExplicitMessage(LOGTAG, "VolumeBlockCount",
                        "Volume {0}: {1}/{2} block-references remaining ({3}%)",
                        blockRequest.VolumeID, vol_count_after, vol_initial, vol_count_after * 100 / vol_initial);

                // Notify the `VolumeManager` that it should evict the volume.
                if (emit_evict)
                {
                    Logging.Log.WriteExplicitMessage(LOGTAG, "VolumeBlockCount",
                        "Volume {0} fully consumed ({1} block-references). {2} volumes still tracked.",
                        blockRequest.VolumeID, vol_initial, m_volumecount.Count);
                    await m_volume_request.WriteAsync(blockRequest).ConfigureAwait(false);
                }

                if (error_block_id != -1)
                {
                    Logging.Log.WriteWarningMessage(LOGTAG, "BlockCountError", null, "Block {0} has a count below 0", blockRequest.BlockID);
                }

                if (error_volume_id != -1)
                {
                    Logging.Log.WriteWarningMessage(LOGTAG, "VolumeCountError", null, "Volume {0} has a count below 0", blockRequest.VolumeID);
                }
            }

            /// <summary>
            /// Get a block from the cache (or shared store). If the block is not available,
            /// it requests the block from the volume and returns a Task that completes when
            /// the block is available.
            /// </summary>
            /// <param name="block_request">The requested block.</param>
            /// <returns>A `Task` holding the data block.</returns>
            public async Task<DataBlock> Get(BlockRequest block_request)
            {
                Logging.Log.WriteExplicitMessage(LOGTAG, "BlockCacheGet", "Getting block {0} from cache", block_request.BlockID);

                // ---- Shared-block fast path ----
                if (m_shared_blocks.Contains(block_request.BlockID))
                {
                    SharedBlockState state;
                    TaskCompletionSource<DataBlock>? waiter = null;
                    bool issue_download = false;

                    lock (m_blockcount_lock)
                    {
                        state = m_shared_block_state.TryGetValue(block_request.BlockID, out var s) ? s : SharedBlockState.Pending;
                        if (state == SharedBlockState.Pending)
                        {
                            if (m_shared_waiters.TryGetValue(block_request.BlockID, out var existing))
                            {
                                m_shared_waiters[block_request.BlockID] = (existing.Count + 1, existing.Task);
                                waiter = existing.Task;
                            }
                            else
                            {
                                waiter = new TaskCompletionSource<DataBlock>(TaskCreationOptions.RunContinuationsAsynchronously);
                                m_shared_waiters[block_request.BlockID] = (1, waiter);
                                issue_download = true;
                            }
                        }
                    }

                    if (state == SharedBlockState.Stored)
                    {
                        long remaining_reference_count;
                        lock (m_blockcount_lock)
                            remaining_reference_count = m_blockcount.TryGetValue(block_request.BlockID, out var block_count) ? block_count : 0L;

                        var read_result = m_shared_block_store.TryGet(block_request.BlockID, remaining_reference_count, out var shared_data);
                        if (read_result == SharedBlockStoreReadResult.Hit)
                            return shared_data!;

                        if (read_result == SharedBlockStoreReadResult.ReadFailure)
                        {
                            // Roll back: mark Bypassed and restore volumecount so the normal eviction path works.
                            lock (m_blockcount_lock)
                            {
                                if (m_shared_block_state.TryGetValue(block_request.BlockID, out var current)
                                    && current == SharedBlockState.Stored)
                                {
                                    var remaining = m_blockcount.TryGetValue(block_request.BlockID, out var r) ? r : 0;
                                    if (remaining > 0)
                                        m_volumecount[block_request.VolumeID] = m_volumecount.TryGetValue(block_request.VolumeID, out var vc)
                                            ? vc + remaining
                                            : remaining;
                                    m_shared_block_state[block_request.BlockID] = SharedBlockState.Bypassed;
                                    m_shared_block_store.Decrement(block_request.BlockID);
                                }
                            }
                        }
                        // Fall through to normal path after rollback.
                    }
                    else if (state == SharedBlockState.Pending)
                    {
                        if (issue_download)
                            await m_volume_request.WriteAsync(block_request).ConfigureAwait(false);
                        return await waiter!.Task.ConfigureAwait(false);
                    }
                    // state == Bypassed: fall through to normal path below.
                }

                // ---- Normal path ----

                // Check if the block is already in the MemoryCache.
                if (m_block_cache.TryGetValue(block_request.BlockID, out DataBlock? value))
                {
                    if (value is null)
                        throw new InvalidOperationException($"Block {block_request.BlockID} was in the cache, but the value was null");

                    value.Reference();

                    // If the block was evicted in between the TryGetValue and the Reference call,
                    // we need to request it again.
                    if (value.Data is not null)
                        return value;
                }

                // If the block is not in the cache, request it from the volume.
                sw_get_wait?.Start();
                var tcs = new TaskCompletionSource<DataBlock>(TaskCreationOptions.RunContinuationsAsynchronously);
                var (_, new_tcs) = m_waiters.AddOrUpdate(block_request.BlockID, (1, tcs), (key, old) => (old.Count + 1, old.Task));
                if (tcs == new_tcs)
                {
                    sw_get_wait?.Stop();
                    Logging.Log.WriteExplicitMessage(LOGTAG, "BlockCacheGet", "Requesting block {0} from volume {1}", block_request.BlockID, block_request.VolumeID);

                    // We are the first to request this block
                    sw_get_write?.Start();
                    await m_volume_request.WriteAsync(block_request).ConfigureAwait(false);
                    sw_get_write?.Stop();

                    // Add a timeout monitor
                    var timeout = TimeSpan.FromMilliseconds(DeadlockTimer.MaxProcessingTime * 2);
                    using var tcs1 = new CancellationTokenSource();
                    var t = await Task.WhenAny(
                        Task.Delay(timeout, tcs1.Token),
                        new_tcs.Task
                    ).ConfigureAwait(false);
                    if (t != new_tcs.Task)
                        Logging.Log.WriteWarningMessage(LOGTAG, "BlockRequestTimeout", null, "Block request for block {0} has been in flight for over {1} milliseconds. This may be a deadlock.", block_request.BlockID, timeout.TotalMilliseconds);
                }
                else
                {
                    sw_get_wait?.Stop();
                    Logging.Log.WriteExplicitMessage(LOGTAG, "BlockCacheGet", "Block {0} is already being requested, waiting for it to be available", block_request.BlockID);
                }

                return await new_tcs.Task.ConfigureAwait(false);
            }

            /// <summary>
            /// Stores a block in the shared store or the MemoryCache, completing any waiters.
            /// Returns a CacheEvict BlockRequest if the volume is now fully consumed (shared path only),
            /// or null otherwise.
            /// </summary>
            /// <param name="block_request">The block request related to the value.</param>
            /// <param name="value">The data block to store.</param>
            /// <returns>A <see cref="BlockRequest"/> with type <see cref="BlockRequestType.CacheEvict"/> if the volume is now fully consumed via the shared-block path; otherwise <c>null</c>.</returns>
            public BlockRequest? Set(BlockRequest block_request, DataBlock value)
            {
                // Free-space check thresholds for unlimited mode
                const int FREE_SPACE_THRESHOLD_MULTIPLIER = 4;
                const long FREE_SPACE_CHECK_INTERVAL_BYTES = 256L * 1024 * 1024; // 256 MB

                var blockID = block_request.BlockID;
                Logging.Log.WriteExplicitMessage(LOGTAG, "BlockCacheSet", "Setting block {0} in cache", blockID);

                if (m_shared_blocks.Contains(blockID))
                {
                    bool try_store = false;

                    lock (m_blockcount_lock)
                    {
                        var current_state = m_shared_block_state.TryGetValue(blockID, out var s) ? s : SharedBlockState.Pending;

                        // Idempotency guard: already handled by a previous Set() call.
                        if (current_state != SharedBlockState.Pending)
                            return null;

                        // In unlimited mode, check free space adaptively.
                        if (m_options.RestoreVolumeCacheHint < 0)
                        {
                            bool do_check = m_last_free_space_bytes < FREE_SPACE_THRESHOLD_MULTIPLIER * m_options.RestoreVolumeCacheMinFree
                                || m_bytes_written_to_shared - m_last_free_space_check_bytes_written >= FREE_SPACE_CHECK_INTERVAL_BYTES;

                            if (do_check)
                            {
                                var fs = Library.Utility.Utility.GetFreeSpaceForPath(m_temp_dir);
                                if (fs == null)
                                {
                                    if (!m_free_space_probe_failed_logged)
                                    {
                                        m_free_space_probe_failed_logged = true;
                                        Logging.Log.WriteWarningMessage(LOGTAG, "SharedFreeSpaceProbe", null,
                                            "Unable to determine free space in '{0}'; bypassing shared-block storage for block {1}.", m_temp_dir, blockID);
                                    }
                                    m_shared_block_state[blockID] = SharedBlockState.Bypassed;
                                    current_state = SharedBlockState.Bypassed;
                                }
                                else
                                {
                                    m_last_free_space_bytes = fs.Value.FreeSpace;
                                    m_last_free_space_check_bytes_written = m_bytes_written_to_shared;

                                    if (m_last_free_space_bytes < m_options.RestoreVolumeCacheMinFree)
                                    {
                                        m_shared_block_state[blockID] = SharedBlockState.Bypassed;
                                        current_state = SharedBlockState.Bypassed;

                                        // TODO Probably want to change id to something common, but also
                                        // wondering if this should be raised to a verbose?
                                        Logging.Log.WriteExplicitMessage(LOGTAG, "SharedFreeSpaceLow",
                                            "Free space ({0} bytes) below minimum ({1} bytes); bypassing shared-block storage for block {2}.",
                                            m_last_free_space_bytes, m_options.RestoreVolumeCacheMinFree, blockID);
                                    }
                                }
                            }
                        }
                        // In explicit-size mode, the SharedBlockStore.Add budget check handles the limit.

                        if (current_state == SharedBlockState.Pending)
                            try_store = true;
                    }

                    if (try_store)
                    {
                        long reference_count;
                        lock (m_blockcount_lock)
                            reference_count = m_blockcount.TryGetValue(blockID, out var block_count) ? block_count : 0L;

                        bool stored = m_shared_block_store.Add(blockID, value.Data!.AsSpan(0, (int)block_request.BlockSize), reference_count);

                        if (stored)
                        {
                            BlockRequest? evict_request = null;

                            lock (m_blockcount_lock)
                            {
                                m_shared_block_state[blockID] = SharedBlockState.Stored;
                                var remaining = m_blockcount.TryGetValue(blockID, out var r) ? r : 0L;
                                m_bytes_written_to_shared += block_request.BlockSize;

                                // Decrement the volume count by ALL remaining block-references at once.
                                // This mirrors what CheckCounts would do one-by-one for each caller.
                                var vol_count_before = m_volumecount.TryGetValue(block_request.VolumeID, out var vc) ? vc : 0L;
                                var vol_count = vol_count_before - remaining;

                                if (vol_count > 0)
                                {
                                    m_volumecount[block_request.VolumeID] = vol_count;
                                    if (m_volumecount_initial.TryGetValue(block_request.VolumeID, out var vi) && vi > 0)
                                    {
                                        bool vol_milestone = (vol_count * 10 / vi) != ((vol_count + 1) * 10 / vi);
                                        if (vol_milestone)
                                            Logging.Log.WriteExplicitMessage(LOGTAG, "VolumeBlockCount",
                                                "Volume {0}: {1}/{2} block-references remaining ({3}%) [shared-block shortcut]",
                                                block_request.VolumeID, vol_count, vi, vol_count * 100 / vi);
                                    }
                                }
                                else
                                {
                                    m_volumecount_initial.TryGetValue(block_request.VolumeID, out var vol_initial);
                                    m_volumecount.Remove(block_request.VolumeID);
                                    Logging.Log.WriteExplicitMessage(LOGTAG, "VolumeBlockCount",
                                        "Volume {0} fully consumed via shared-block shortcut ({1} block-references). {2} volumes still tracked.",
                                        block_request.VolumeID, vol_initial, m_volumecount.Count);
                                    evict_request = new BlockRequest(
                                        block_request.BlockID,
                                        block_request.BlockOffset,
                                        block_request.BlockHash,
                                        block_request.BlockSize,
                                        block_request.VolumeID,
                                        BlockRequestType.CacheEvict);
                                }

                                // Complete all waiters that were waiting for this shared block.
                                if (m_shared_waiters.Remove(blockID, out var entry))
                                {
                                    value.Reference(entry.Count);
                                    entry.Task.SetResult(value);
                                }
                            }

                            // Dispose the producer's reference; the waiters' references keep the data alive.
                            value.Dispose();
                            return evict_request;
                        }
                        else
                        {
                            // Add failed — mark Bypassed and fall through to the normal path.
                            lock (m_blockcount_lock)
                            {
                                m_shared_block_state[blockID] = SharedBlockState.Bypassed;
                            }
                        }
                    }
                    // Either was already Bypassed or the Add failed: fall through to normal Set below.
                }

                // ---- Normal (non-shared or bypassed-shared) path ----

                sw_set_wake_get?.Start();
                var waiters_exist = m_waiters.TryRemove(blockID, out var normal_entry);
                sw_set_wake_get?.Stop();

                // Also pick up any shared waiters that were registered before the bypass was decided.
                bool shared_waiters_exist = false;
                (int Count, TaskCompletionSource<DataBlock> Task) shared_entry = default;
                if (m_shared_blocks.Contains(blockID))
                {
                    lock (m_blockcount_lock)
                    {
                        shared_waiters_exist = m_shared_waiters.Remove(blockID, out shared_entry);
                    }
                }

                int total_waiters = (waiters_exist ? normal_entry.Count : 0) + (shared_waiters_exist ? shared_entry.Count : 0);
                bool any_waiters = waiters_exist || shared_waiters_exist;

                sw_set_set?.Start();
                long blockcount_val;
                lock (m_blockcount_lock)
                {
                    blockcount_val = m_blockcount.TryGetValue(blockID, out var bc) ? bc : 0L;
                }
                bool fullfills = any_waiters && total_waiters >= blockcount_val;
                bool added_to_cache = !fullfills && m_block_cache_max > 0;
                if (added_to_cache)
                {
                    m_block_cache.Set(blockID, value, m_entry_options);
                    Interlocked.Increment(ref m_block_cache_count);
                }
                sw_set_set?.Stop();

                // Notify any waiters that the block is available.
                sw_set_wake_set?.Start();
                if (any_waiters)
                {
                    value.Reference(total_waiters);
                    if (waiters_exist)
                        normal_entry.Task.SetResult(value);
                    if (shared_waiters_exist)
                        shared_entry.Task!.SetResult(value);
                }
                sw_set_wake_set?.Stop();

                sw_cacheevict?.Start();
                if (added_to_cache)
                {
                    if (m_block_cache_count > m_block_cache_max && Interlocked.CompareExchange(ref m_block_cache_compacting, 1, 0) == 0)
                        m_block_cache.Compact(m_options.RestoreCacheEvict);
                }
                else
                    value.Dispose();
                sw_cacheevict?.Stop();

                return null;
            }

            /// <summary>
            /// Retire the dictionary. This will decrement the number of readers
            /// accessing the dictionary, and if there are no more readers, it
            /// will retire the volume request channel, effectively shutting
            /// down the restore process network.
            /// </summary>
            public void Retire()
            {
                if (!m_retired && Interlocked.Decrement(ref m_readers) <= 0)
                {
                    m_volume_request.Retire();
                    m_retired = true;
                }
            }

            /// <summary>
            /// Clean up the dictionary. This will remove the database table
            /// that was used to keep track of the block counts.
            /// </summary>
            public void Dispose()
            {
                // Verify that the tables are empty
                var blockcount = m_blockcount.Sum(x => x.Value);
                var volumecount = m_volumecount.Sum(x => x.Value);

                if (blockcount != 0)
                {
                    var blocks = m_blockcount
                        .Where(x => x.Value != 0)
                        .Take(10)
                        .Select(x => x.Key);
                    var blockids = string.Join(", ", blocks);
                    Logging.Log.WriteErrorMessage(LOGTAG, "BlockCountError", null, $"Block count in SleepableDictionarys block table is not zero: {blockcount}{Environment.NewLine}First 10 blocks: {blockids}");
                }

                if (volumecount != 0)
                {
                    var vols = m_volumecount
                        .Where(x => x.Value != 0)
                        .Take(10)
                        .Select(x => x.Key);
                    var volids = string.Join(", ", vols);
                    Logging.Log.WriteErrorMessage(LOGTAG, "VolumeCountError", null, $"Volume count in SleepableDictionarys volume table is not zero: {volumecount}{Environment.NewLine}First 10 volumes: {volids}");
                }

                if (m_block_cache.Count > 0)
                {
                    Logging.Log.WriteErrorMessage(LOGTAG, "BlockCacheMismatch", null, $"Internal Block cache is not empty: {m_block_cache.Count}");
                    Logging.Log.WriteErrorMessage(LOGTAG, "BlockCacheMismatch", null, $"First 10 block counts in cache ({m_blockcount.Count}): {string.Join(", ", m_blockcount.Take(10).Select(x => x.Value))}");
                }

                if (m_options.InternalProfiling)
                {
                    Logging.Log.WriteProfilingMessage(LOGTAG, "InternalTimings", $"Sleepable dictionary - CheckCounts: {sw_checkcounts!.ElapsedMilliseconds}ms, Get wait: {sw_get_wait!.ElapsedMilliseconds}ms, Get write: {sw_get_write!.ElapsedMilliseconds}ms, Set set: {sw_set_set!.ElapsedMilliseconds}ms, Set wake get: {sw_set_wake_get!.ElapsedMilliseconds}ms, Set wake set: {sw_set_wake_set!.ElapsedMilliseconds}ms, Cache evict: {sw_cacheevict!.ElapsedMilliseconds}ms");
                }

                if (!m_retired)
                {
                    Logging.Log.WriteErrorMessage(LOGTAG, "NotRetired", null, "SleepableDictionary was disposed without having retired channels.");
                    m_retired = true;
                    m_volume_request.Retire();
                }

                m_shared_block_store.Dispose();
            }

            /// <summary>
            /// Cancel all pending requests. This will set an exception on all
            /// pending requests, effectively cancelling them.
            /// </summary>
            public void CancelAll()
            {
                foreach (var (_, tcs) in m_waiters.Values)
                {
                    tcs.SetException(new RetiredException("Request waiter"));
                }

                // Also cancel any pending shared waiters.
                lock (m_blockcount_lock)
                {
                    foreach (var (_, entry) in m_shared_waiters)
                        entry.Task.TrySetException(new RetiredException("Shared request waiter"));
                    m_shared_waiters.Clear();
                }
            }
        }

        /// <summary>
        /// Runs the block manager process with dual-path caching strategy.
        /// Creates a <see cref="SleepableDictionary"/> that routes high-reference-count blocks to SharedBlockStore
        /// and other blocks to MemoryCache. Starts a volume consumer task (reads decompressed blocks from volumes
        /// and stores them) and multiple block handler tasks (one per FileProcessor worker, serving block requests
        /// by retrieving from cache or requesting from volumes).
        /// </summary>
        /// <param name="channels">The named channels for the restore operation.</param>
        /// <param name="db">The database holding information about how many of each block this restore requires.</param>
        /// <param name="fp_requests">The channels for reading block requests from the `FileProcessor`.</param>
        /// <param name="fp_responses">The channels for writing block responses back to the `FileProcessor`.</param>
        /// <param name="options">The restore options.</param>
        /// <param name="results">The results of the restore operation.</param>
        public static Task Run(Channels channels, LocalRestoreDatabase db, IChannel<BlockRequest>[] fp_requests, IChannel<Task<DataBlock>>[] fp_responses, Options options, RestoreResults results)
        {
            return AutomationExtensions.RunTask(
            new
            {
                Input = channels.DecompressedBlock.AsRead(),
                Ack = channels.DecompressionAck.AsWrite(),
                Output = channels.VolumeRequest.AsWrite()
            },
            async self =>
            {
                // Create a cache for the blocks,
                using SleepableDictionary cache =
                    await SleepableDictionary.CreateAsync(db, self.Output, options, fp_requests.Length, results.TaskControl.ProgressToken)
                        .ConfigureAwait(false);

                // The volume consumer will read blocks from the input channel (data blocks from the volumes) and store them in the cache.
                var volume_consumer = Task.Run(async () =>
                {
                    Stopwatch? sw_read = options.InternalProfiling ? new() : null;
                    Stopwatch? sw_set = options.InternalProfiling ? new() : null;
                    Stopwatch? sw_deadlock = options.InternalProfiling ? new() : null;
                    try
                    {
                        while (true)
                        {
                            Logging.Log.WriteExplicitMessage(LOGTAG, "VolumeConsumer", null, "Waiting for block request from volume");
                            sw_read?.Start();
                            var (block_request, data) = await self.Input.ReadAsync().ConfigureAwait(false);
                            sw_read?.Stop();

                            Logging.Log.WriteExplicitMessage(LOGTAG, "VolumeConsumer", null, "Received data for block {0} from volume {1}", block_request.BlockID, block_request.VolumeID);
                            sw_set?.Start();
                            var evict = cache.Set(block_request, data);
                            sw_set?.Stop();

                            Logging.Log.WriteExplicitMessage(LOGTAG, "VolumeConsumer", null, "Updating deadlock timer for block {0} from volume {1}", block_request.BlockID, block_request.VolumeID);
                            sw_deadlock?.Start();
                            DeadlockTimer.MaxProcessingTime = Math.Max(DeadlockTimer.MaxProcessingTime, (int)(DateTime.Now - block_request.TimestampMilliseconds).TotalMilliseconds);
                            sw_deadlock?.Stop();

                            if (evict != null)
                            {
                                Logging.Log.WriteExplicitMessage(LOGTAG, "VolumeConsumer", null, "Emitting eviction request for volume {0}", evict.VolumeID);
                                await self.Output.WriteAsync(evict).ConfigureAwait(false);
                            }
                        }
                    }
                    catch (RetiredException)
                    {
                        Logging.Log.WriteVerboseMessage(LOGTAG, "RetiredProcess", null, "BlockManager Volume consumer retired");

                        if (options.InternalProfiling)
                        {
                            Logging.Log.WriteProfilingMessage(LOGTAG, "InternalTimings", $"Volume consumer - Read: {sw_read!.ElapsedMilliseconds}ms, Set: {sw_set!.ElapsedMilliseconds}ms, Deadlock timer: {sw_deadlock!.ElapsedMilliseconds}ms");
                        }

                        // Cancel any remaining readers - although there shouldn't be any.
                        cache.CancelAll();
                    }
                    catch (Exception ex)
                    {
                        Logging.Log.WriteErrorMessage(LOGTAG, "VolumeConsumerError", ex, "Error in volume consumer");
                        self.Input.Retire();

                        // Cancel any remaining readers - although there shouldn't be any.
                        cache.CancelAll();
                        cache.Retire();
                    }
                });

                // The block handlers will read block requests from the `FileProcessor`, access the cache for the blocks, and write the resulting blocks to the `FileProcessor`.
                var block_handlers = fp_requests.Zip(fp_responses, (req, res) => Task.Run(async () =>
                {
                    Stopwatch? sw_req = options.InternalProfiling ? new() : null;
                    Stopwatch? sw_resp = options.InternalProfiling ? new() : null;
                    Stopwatch? sw_cache = options.InternalProfiling ? new() : null;
                    Stopwatch? sw_get = options.InternalProfiling ? new() : null;
                    try
                    {
                        while (true)
                        {
                            sw_req?.Start();
                            var block_request = await req.ReadAsync().ConfigureAwait(false);
                            sw_req?.Stop();
                            Logging.Log.WriteExplicitMessage(LOGTAG, "BlockHandler", null, "Received block request: {0}", block_request.RequestType);
                            switch (block_request.RequestType)
                            {
                                case BlockRequestType.Download:
                                    sw_get?.Start();
                                    var datatask = cache.Get(block_request);
                                    sw_get?.Stop();
                                    Logging.Log.WriteExplicitMessage(LOGTAG, "BlockHandler", null, "Retrieved data for block {0} and volume {1}", block_request.BlockID, block_request.VolumeID);

                                    sw_resp?.Start();
                                    await res.WriteAsync(datatask).ConfigureAwait(false);
                                    sw_resp?.Stop();
                                    Logging.Log.WriteExplicitMessage(LOGTAG, "BlockHandler", null, "Passed data for block {0} and volume {1} to FileProcessor", block_request.BlockID, block_request.VolumeID);
                                    break;
                                case BlockRequestType.CacheEvict:
                                    sw_cache?.Start();
                                    // Target file already had the block.
                                    await cache.CheckCounts(block_request).ConfigureAwait(false);
                                    sw_cache?.Stop();

                                    Logging.Log.WriteExplicitMessage(LOGTAG, "BlockHandler", null, "Decremented counts for block {0} and volume {1}", block_request.BlockID, block_request.VolumeID);
                                    break;
                                default:
                                    throw new InvalidOperationException($"Unexpected block request type: {block_request.RequestType}");
                            }
                        }
                    }
                    catch (RetiredException)
                    {
                        Logging.Log.WriteVerboseMessage(LOGTAG, "RetiredProcess", null, "BlockManager Block handler retired");

                        if (options.InternalProfiling)
                        {
                            Logging.Log.WriteProfilingMessage(LOGTAG, "InternalTimings", $"Block handler - Req: {sw_req!.ElapsedMilliseconds}ms, Resp: {sw_resp!.ElapsedMilliseconds}ms, Cache: {sw_cache!.ElapsedMilliseconds}ms, Get: {sw_get!.ElapsedMilliseconds}ms");
                        }

                        cache.Retire();
                    }
                    catch (Exception ex)
                    {
                        Logging.Log.WriteErrorMessage(LOGTAG, "BlockHandlerError", ex, "Error in block handler");
                        req.Retire();
                        res.Retire();
                        cache.Retire();
                    }
                })).ToArray();

                await Task.WhenAll([volume_consumer, .. block_handlers]).ConfigureAwait(false);

                results.PeakSharedBlockStoreBytes = cache.PeakSharedBlockStoreBytes;
            });
        }
    }
}
