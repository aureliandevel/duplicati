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
using System.Collections.Generic;
using System.IO;
using Microsoft.Win32.SafeHandles;
using Duplicati.Library.Utility;
using System.Threading;

#nullable enable

namespace Duplicati.Library.Main.Operation.Restore
{
    /// <summary>
    /// The result of a TryGet operation on a SharedBlockStore.
    /// </summary>
    internal enum SharedBlockStoreReadResult
    {
        /// <summary>
        /// Block was not found in the store.
        /// </summary>
        Miss,

        /// <summary>
        /// Block was found and read successfully.
        /// </summary>
        Hit,

        /// <summary>
        /// Block was found in the index but could not be read from disk.
        /// </summary>
        ReadFailure
    }

    /// <summary>
    /// Internal interface for test seam to abstract file I/O in SharedBlockStore.
    /// </summary>
    internal interface ISharedBlockStoreFileIO
    {
        /// <summary>
        /// Opens a new temp file for read/write use.
        /// </summary>
        (TempFile TempFile, SafeFileHandle Handle) OpenNew(string tempDir);

        /// <summary>
        /// Writes data at the specified offset. Returns false on IOException.
        /// </summary>
        bool TryWrite(SafeFileHandle handle, ReadOnlySpan<byte> data, long offset);

        /// <summary>
        /// Reads data at the specified offset into the buffer. Returns false on IOException.
        /// </summary>
        bool TryRead(SafeFileHandle handle, Span<byte> buffer, long offset);

        /// <summary>
        /// Closes the SafeFileHandle, releasing the OS file lock.
        /// </summary>
        void CloseHandle(SafeFileHandle handle);
    }

    /// <summary>
    /// Production implementation of ISharedBlockStoreFileIO using RandomAccess for thread-safe I/O.
    /// </summary>
    internal sealed class DefaultSharedBlockStoreFileIO : ISharedBlockStoreFileIO
    {
        /// <summary>
        /// Opens a new temp file in the specified directory and returns its TempFile and SafeFileHandle.   
        /// </summary>
        /// <param name="tempDir">The directory in which to create the temp file.</param>
        /// <returns>A tuple containing the TempFile and its associated SafeFileHandle.</returns>
        public (TempFile TempFile, SafeFileHandle Handle) OpenNew(string tempDir)
        {
            var tempFile = TempFile.CreateInFolder(tempDir, false);
            var handle = File.OpenHandle((string)tempFile, FileMode.Create, FileAccess.ReadWrite, FileShare.None);

            return (tempFile, handle);
        }

        /// <summary>
        /// Writes data to the specified offset in the file represented by the SafeFileHandle. Returns 
        /// false if an IOException occurs during the write operation.
        /// </summary>
        /// <param name="handle">The SafeFileHandle representing the file to write to.</param>
        /// <param name="data">The data to write.</param>
        /// <param name="offset">The offset in the file at which to start writing.</param>
        /// <returns>True if the write was successful; false if an IOException occurred.</returns>
        public bool TryWrite(SafeFileHandle handle, ReadOnlySpan<byte> data, long offset)
        {
            try
            {
                RandomAccess.Write(handle, data, offset);

                return true;
            }
            catch (IOException)
            {
                return false;
            }
        }

        /// <summary>
        /// Reads data from the specified offset in the file represented by the SafeFileHandle into the 
        /// provided buffer. Returns false if an IOException occurs during the read operation or if an 
        /// unexpected end of file is encountered (i.e., a short read). The method attempts to fill the 
        /// entire buffer, looping as necessary until all bytes are read or an error occurs.
        /// </summary>
        /// <param name="handle">The SafeFileHandle representing the file to read from.</param>
        /// <param name="buffer">The buffer to read data into.</param>
        /// <param name="offset">The offset in the file at which to start reading.</param>
        /// <returns>True if the read was successful; false if an IOException occurred or an unexpected end of file was encountered.</returns>
        public bool TryRead(SafeFileHandle handle, Span<byte> buffer, long offset)
        {
            try
            {
                // RandomAccess.Read may return fewer bytes than requested (like a short read).
                // Loop until the buffer is fully populated — equivalent to ReadExactly.
                int remaining = buffer.Length;
                long pos = offset;
                while (remaining > 0)
                {
                    int n = RandomAccess.Read(handle, buffer.Slice(buffer.Length - remaining), pos);
                    if (n == 0)
                        return false; // unexpected EOF — treat as read failure
                        
                    remaining -= n;
                    pos += n;
                }

                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// Closes the SafeFileHandle, releasing the OS file lock. Safe to call multiple
        /// times.
        /// </summary>
        /// <param name="handle">The SafeFileHandle to close.</param>
        public void CloseHandle(SafeFileHandle handle) => handle.Close();
    }

    /// <summary>
    /// A store for data blocks that are shared across multiple restore volumes.
    /// Blocks are written to temporary files and indexed for fast lookup.
    /// Thread-safe: Add/TryGet/Decrement/Dispose may be called from multiple threads.
    /// </summary>
    internal sealed class SharedBlockStore : IDisposable
    {
        /// <summary>
        /// Maximum size of a single SharedBlockStore temp file before rolling over to a new file.
        /// </summary>
        private const long MAX_TEMP_FILE_SIZE = 500L * 1024 * 1024;

        /// <summary>
        /// Log tag for this class.
        /// </summary>
        private static readonly string LOGTAG = Logging.Log.LogTagFromType<SharedBlockStore>();

        /// <summary>
        /// Null-object instance. All operations are no-ops on this instance.
        /// </summary>
        public static readonly SharedBlockStore Empty = new SharedBlockStore(isEmpty: true);

        /// <summary>
        /// Indicates whether this instance is the Empty null-object. If true, all operations are 
        /// no-ops and Add always returns false.
        /// </summary>
        private readonly bool m_isEmpty;
        /// <summary>
        /// Budget for total bytes written across all temp files. -1 means unlimited.
        /// </summary>
        private readonly long m_budget;
        /// <summary>
        /// Ceiling for bytes written to any single temp file.
        /// </summary>
        private readonly long m_per_file_ceiling;
        /// <summary>
        /// Total bytes written across all temp files. Because the temp files are append-only and
        /// individual block bytes are not reclaimed before Dispose, this is also the peak store size.
        /// </summary>
        private long m_total_written;
        /// <summary>
        /// Directory where temp files are created. Used for logging purposes.
        /// </summary>
        private readonly string m_temp_dir;        
        /// <summary>
        /// List of temp files used to store blocks. Each file is written sequentially until the per-file ceiling is 
        /// reached, then a new file is opened.
        /// </summary>
        private readonly List<TempFile> m_temp_files = [];
        /// <summary>
        /// List of SafeFileHandles corresponding to the temp files. Used for thread-safe reads and writes via RandomAccess.
        /// </summary>
        private readonly List<SafeFileHandle> m_handles = [];
        /// <summary>
        /// Index of the current temp file being written to.
        /// </summary>
        private int m_current_file_index = -1;
        /// <summary>
        /// Offset within the current temp file.
        /// </summary>
        private long m_current_offset = 0;
        /// <summary>
        /// Index mapping blockID to the location of the block in the temp files (file index, offset, length).
        /// Updated on Add and Decrement, read on TryGet.
        /// </summary>
        private readonly Dictionary<long, (int FileIndex, long Offset, int Length)> m_index = [];
        /// <summary>
        /// Lock object to protect access to mutable state (m_total_written, m_temp_files,
        /// m_handles, m_current_file_index, m_current_offset, m_index).
        /// </summary>
        private readonly Lock m_lock = new();
        /// <summary>
        /// Abstraction for file I/O operations, allowing for test seams. In production, this is an instance 
        /// of DefaultSharedBlockStoreFileIO that uses RandomAccess for thread-safe I/O.
        /// </summary>
        private readonly ISharedBlockStoreFileIO m_io;
        private bool m_disposed = false;

        /// <summary>
        /// Gets the total bytes written to the SharedBlockStore TempFile(s).
        /// Because the store is append-only, this is also the peak on-disk footprint.
        /// </summary>
        public long PeakBytesWritten { get { lock (m_lock) return m_total_written; } }

        /// <summary>
        /// Private constructor for the Empty null-object instance.
        /// </summary>
        private SharedBlockStore(bool isEmpty)
        {
            m_isEmpty = isEmpty;
            m_budget = -1;
            m_per_file_ceiling = MAX_TEMP_FILE_SIZE;
            m_io = null!;
            m_temp_dir = string.Empty;
        }

        /// <summary>
        /// Creates a new SharedBlockStore that writes blocks to temporary files in the given directory.
        /// </summary>
        /// <param name="tempDir">Directory where temp files are created.</param>
        /// <param name="budget">-1 = unlimited, &gt;0 = max total bytes that may be written.</param>
        /// <param name="io">Optional test seam for file I/O; uses DefaultSharedBlockStoreFileIO when null.</param>
        public SharedBlockStore(string tempDir, long budget = -1, ISharedBlockStoreFileIO? io = null)
        {
            m_isEmpty = false;
            m_budget = budget;
            m_per_file_ceiling = budget > 0 ? Math.Min(MAX_TEMP_FILE_SIZE, budget) : MAX_TEMP_FILE_SIZE;
            m_io = io ?? new DefaultSharedBlockStoreFileIO();
            m_temp_dir = tempDir;
        }

        /// <summary>
        /// Adds a block to the store.
        /// </summary>
        /// <param name="blockID">The block's database ID (used as the key).</param>
        /// <param name="data">The block data to persist.</param>
        /// <returns>
        /// <c>true</c> on success; <c>false</c> if the store is full, the budget is exhausted,
        /// an I/O error occurred, or the instance is disposed.
        /// </returns>
        public bool Add(long blockID, ReadOnlySpan<byte> data)
        {
            if (m_isEmpty || m_disposed)
                return false;

            int length = data.Length;
            int fileIndex;
            long offset;
            SafeFileHandle handle;

            lock (m_lock)
            {
                // Check budget
                if (m_budget > 0 && m_total_written + length > m_budget)
                {
                    Logging.Log.WriteExplicitMessage(LOGTAG, "SharedBlockStore",
                        "SharedBlockStore budget exceeded: blockID={0}, budget={1}, written={2}, length={3}",
                        blockID, m_budget, m_total_written, length);
                    
                    return false;
                }

                // Check if we need to open a new file
                if (m_current_file_index < 0 || m_current_offset + length > m_per_file_ceiling)
                {
                    TempFile tempFile;
                    SafeFileHandle newHandle;
                    try
                    {
                        (tempFile, newHandle) = m_io.OpenNew(m_temp_dir);
                    }
                    catch (IOException ex)
                    {
                        Logging.Log.WriteWarningMessage(LOGTAG, "AddOpenNewFailed", null,
                            "SharedBlockStore failed to open new temp file for block {0}: {1}", blockID, ex.Message);
                    
                        return false;
                    }

                    m_temp_files.Add(tempFile);
                    m_handles.Add(newHandle);
                    m_current_file_index = m_handles.Count - 1;
                    m_current_offset = 0;
                }

                fileIndex = m_current_file_index;
                offset = m_current_offset;
                handle = m_handles[fileIndex];

                // Advance counters under the lock. Even if the write fails we leave
                // the counters advanced so the file position is correct.
                m_current_offset += length;
                m_total_written += length;
            }

            // Write outside the lock so reads and writes can proceed concurrently.
            bool success = m_io.TryWrite(handle, data, offset);

            if (success)
            {
                lock (m_lock)
                {
                    m_index[blockID] = (fileIndex, offset, length);
                }

                Logging.Log.WriteExplicitMessage(LOGTAG, "SharedBlockStore",
                    "SharedBlockStore: block {0} stored at file={1}, offset={2}, length={3}",
                    blockID, fileIndex, offset, length);
            }
            else
            {
                Logging.Log.WriteWarningMessage(LOGTAG, "AddWriteFailed", null,
                    "SharedBlockStore: write failed for block {0} at file={1}, offset={2}, length={3}",
                    blockID, fileIndex, offset, length);
            }

            return success;
        }

        /// <summary>
        /// Tries to read a block from the store.
        /// </summary>
        /// <param name="blockID">The block's database ID.</param>
        /// <param name="data">
        /// On <see cref="SharedBlockStoreReadResult.Hit"/>, receives a new <see cref="DataBlock"/>
        /// backed by an ArrayPool buffer.  The caller is responsible for calling Dispose().
        /// </param>
        /// <returns>
        /// <see cref="SharedBlockStoreReadResult.Miss"/> if the block is not in the index,
        /// <see cref="SharedBlockStoreReadResult.Hit"/> if the block was read successfully,
        /// <see cref="SharedBlockStoreReadResult.ReadFailure"/> if the block is indexed but could not be read.
        /// </returns>
        public SharedBlockStoreReadResult TryGet(long blockID, out DataBlock? data)
        {
            data = null;

            if (m_isEmpty)
                return SharedBlockStoreReadResult.Miss;

            int fileIndex;
            long offset;
            int length;
            SafeFileHandle handle;

            lock (m_lock)
            {
                if (!m_index.TryGetValue(blockID, out var entry))
                {
                    Logging.Log.WriteExplicitMessage(LOGTAG, "SharedBlockStore",
                        "SharedBlockStore: miss for block {0}", blockID);
                
                    return SharedBlockStoreReadResult.Miss;
                }

                fileIndex = entry.FileIndex;
                offset = entry.Offset;
                length = entry.Length;
                handle = m_handles[fileIndex];
            }

            var buffer = ArrayPool<byte>.Shared.Rent(length);
            bool success = m_io.TryRead(handle, buffer.AsSpan(0, length), offset);

            if (success)
            {
                data = new DataBlock(buffer);
                Logging.Log.WriteExplicitMessage(LOGTAG, "SharedBlockStore",
                    "SharedBlockStore: hit for block {0} at file={1}, offset={2}, length={3}",
                    blockID, fileIndex, offset, length);
                
                return SharedBlockStoreReadResult.Hit;
            }
            else
            {
                ArrayPool<byte>.Shared.Return(buffer);
                Logging.Log.WriteWarningMessage(LOGTAG, "BlockReadFailure", null,
                    "SharedBlockStore: read failure for block {0} at file={1}, offset={2}, length={3}",
                    blockID, fileIndex, offset, length);
                
                return SharedBlockStoreReadResult.ReadFailure;
            }
        }

        /// <summary>
        /// Removes a block from the store index.
        /// After this call, future <see cref="TryGet"/> calls for <paramref name="blockID"/> return
        /// <see cref="SharedBlockStoreReadResult.Miss"/> (the on-disk bytes are not reclaimed until Dispose).
        /// </summary>
        public void Decrement(long blockID)
        {
            if (m_isEmpty)
                return;

            lock (m_lock)
            {
                m_index.Remove(blockID);
            }

            Logging.Log.WriteExplicitMessage(LOGTAG, "SharedBlockStore",
                "SharedBlockStore: block {0} removed from index", blockID);
        }

        /// <summary>
        /// Disposes the SharedBlockStore, closing all file handles and deleting all temp files.
        /// Safe to call multiple times.
        /// </summary>
        public void Dispose()
        {
            if (m_isEmpty || m_disposed)
                return;

            m_disposed = true;

            int remaining;
            lock (m_lock)
            {
                remaining = m_index.Count;
                m_index.Clear();
            }

            if (remaining > 0)
                Logging.Log.WriteWarningMessage(LOGTAG, "DisposeWithRemainder", null,
                    "SharedBlockStore disposed with {0} blocks still in index (ref-count imbalance)", remaining);

            Logging.Log.WriteExplicitMessage(LOGTAG, "SharedBlockStore",
                "SharedBlockStore disposing: {0} temp file(s), {1} remaining index entries at dispose time",
                m_temp_files.Count, remaining);

            // Close all handles FIRST, then dispose temp files so the OS can delete them.
            foreach (var h in m_handles)
                m_io.CloseHandle(h);

            foreach (var f in m_temp_files)
                f.Dispose();
        }
    }
}
