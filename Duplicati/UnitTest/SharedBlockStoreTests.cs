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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Duplicati.Library.Main.Operation.Restore;
using Duplicati.Library.Utility;
using Microsoft.Win32.SafeHandles;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

#nullable enable

namespace Duplicati.UnitTest
{
    [TestFixture]
    public class SharedBlockStoreTests
    {
        // ----------------------------------------------------------------
        // Mock I/O implementation backed by in-memory buffers
        // ----------------------------------------------------------------

        private class MockSharedBlockStoreFileIO : ISharedBlockStoreFileIO
        {
            // Maps a sequential file index to a growable byte buffer.
            private readonly Dictionary<int, byte[]> m_buffers = [];
            private readonly List<SafeFileHandle> m_handles = [];
            private int m_next_index = 0;
            private readonly string m_tempDir;

            public bool FailNextWrite { get; set; }
            public bool FailNextRead { get; set; }

            public MockSharedBlockStoreFileIO(string tempDir)
            {
                m_tempDir = tempDir;
            }

            public (TempFile TempFile, SafeFileHandle Handle) OpenNew(string tempDir)
            {
                var tf = TempFile.CreateInFolder(m_tempDir, false);
                var handle = File.OpenHandle((string)tf, FileMode.Create, FileAccess.ReadWrite, FileShare.None);
                // 64 MB backing buffer per file — sufficient for all unit-test payloads.
                m_buffers[m_next_index++] = new byte[64 * 1024 * 1024];
                m_handles.Add(handle);
                return (tf, handle);
            }

            public bool TryWrite(SafeFileHandle handle, ReadOnlySpan<byte> data, long offset)
            {
                if (FailNextWrite) { FailNextWrite = false; return false; }
                // Find the buffer that belongs to this handle (last opened).
                var buf = m_buffers[m_buffers.Count - 1];
                data.CopyTo(buf.AsSpan((int)offset));
                return true;
            }

            public bool TryRead(SafeFileHandle handle, Span<byte> buffer, long offset)
            {
                if (FailNextRead) { FailNextRead = false; return false; }
                // Iterate buffers to find one large enough to contain the requested offset.
                foreach (var buf in m_buffers.Values)
                {
                    if (buf.Length >= offset + buffer.Length)
                    {
                        buf.AsSpan((int)offset, buffer.Length).CopyTo(buffer);
                        return true;
                    }
                }
                return false;
            }

            public void CloseHandle(SafeFileHandle handle) => handle.Close();
        }

        // ----------------------------------------------------------------
        // Test helpers
        // ----------------------------------------------------------------

        private string m_testDir = "";

        [SetUp]
        public void SetUp()
        {
            m_testDir = Path.Combine(Path.GetTempPath(), $"SharedBlockStoreTests_{Guid.NewGuid():N}");
            Directory.CreateDirectory(m_testDir);
        }

        [TearDown]
        public void TearDown()
        {
            try { Directory.Delete(m_testDir, true); } catch { }
        }

        private static byte[] MakeData(int length, byte fill = 0xAB)
        {
            var buf = new byte[length];
            Array.Fill(buf, fill);
            return buf;
        }

        // ----------------------------------------------------------------
        // Test 1: Basic Add → TryGet → Decrement round-trip
        // ----------------------------------------------------------------
        [Test]
        [Category("SharedBlockStore")]
        public void AddThenTryGetThenDecrement_BasicRoundTrip()
        {
            var mock = new MockSharedBlockStoreFileIO(m_testDir);
            using var store = new SharedBlockStore(m_testDir, -1, mock);

            var data = MakeData(256);
            bool added = store.Add(42L, data.AsSpan());
            Assert.IsTrue(added, "Add should succeed");

            var result = store.TryGet(42L, out var block);
            Assert.AreEqual(SharedBlockStoreReadResult.Hit, result);
            Assert.IsNotNull(block);
            Assert.AreEqual(data.Length, block!.Data!.Length);
            block.Dispose();

            store.Decrement(42L);
            var result2 = store.TryGet(42L, out var block2);
            Assert.AreEqual(SharedBlockStoreReadResult.Miss, result2);
            Assert.IsNull(block2);
        }

        // ----------------------------------------------------------------
        // Test 2: TryGet before Add returns Miss
        // ----------------------------------------------------------------
        [Test]
        [Category("SharedBlockStore")]
        public void TryGetBeforeAdd_ReturnsMiss()
        {
            var mock = new MockSharedBlockStoreFileIO(m_testDir);
            using var store = new SharedBlockStore(m_testDir, -1, mock);

            var result = store.TryGet(99L, out var block);
            Assert.AreEqual(SharedBlockStoreReadResult.Miss, result);
            Assert.IsNull(block);
        }

        // ----------------------------------------------------------------
        // Test 3: Multiple TryGet calls then Decrement
        // ----------------------------------------------------------------
        [Test]
        [Category("SharedBlockStore")]
        public void MultipleTryGetThenDecrement()
        {
            var mock = new MockSharedBlockStoreFileIO(m_testDir);
            using var store = new SharedBlockStore(m_testDir, -1, mock);

            var data = MakeData(128, 0x55);
            store.Add(10L, data.AsSpan());

            // Three TryGet calls should all return Hit before Decrement.
            for (int i = 0; i < 3; i++)
            {
                var result = store.TryGet(10L, out var block);
                Assert.AreEqual(SharedBlockStoreReadResult.Hit, result, $"Attempt {i}");
                block!.Dispose();
            }

            // After Decrement, the block should be a miss.
            store.Decrement(10L);
            var finalResult = store.TryGet(10L, out var finalBlock);
            Assert.AreEqual(SharedBlockStoreReadResult.Miss, finalResult);
            Assert.IsNull(finalBlock);
        }

        // ----------------------------------------------------------------
        // Test 4: Data integrity for 1-byte and 1024-byte blocks
        // ----------------------------------------------------------------
        [Test]
        [Category("SharedBlockStore")]
        public void DataIntegrity_1ByteAnd1024Byte()
        {
            var mock = new MockSharedBlockStoreFileIO(m_testDir);
            using var store = new SharedBlockStore(m_testDir, -1, mock);

            // 1-byte block
            var one = new byte[] { 0xDE };
            store.Add(1L, one.AsSpan());
            var r1 = store.TryGet(1L, out var b1);
            Assert.AreEqual(SharedBlockStoreReadResult.Hit, r1);
            Assert.AreEqual(0xDE, b1!.Data![0]);
            b1.Dispose();

            // 1024-byte block
            var large = MakeData(1024, 0xBE);
            store.Add(2L, large.AsSpan());
            var r2 = store.TryGet(2L, out var b2);
            Assert.AreEqual(SharedBlockStoreReadResult.Hit, r2);
            CollectionAssert.AreEqual(large, b2!.Data!.Take(1024).ToArray());
            b2.Dispose();
        }

        // ----------------------------------------------------------------
        // Test 5: Concurrent TryGet after Add — all return Hit
        // ----------------------------------------------------------------
        [Test]
        [Category("SharedBlockStore")]
        public void ConcurrentTryGetAfterAdd_AllReturnHit()
        {
            var mock = new MockSharedBlockStoreFileIO(m_testDir);
            using var store = new SharedBlockStore(m_testDir, -1, mock);

            var data = MakeData(512, 0xCC);
            store.Add(100L, data.AsSpan());

            int threadCount = 10;
            var results = new SharedBlockStoreReadResult[threadCount];
            var tasks = Enumerable.Range(0, threadCount).Select(i => Task.Run(() =>
            {
                var r = store.TryGet(100L, out var block);
                results[i] = r;
                block?.Dispose();
            })).ToArray();

            Task.WaitAll(tasks);

            foreach (var r in results)
                Assert.AreEqual(SharedBlockStoreReadResult.Hit, r);
        }

        // ----------------------------------------------------------------
        // Test 6: Concurrent TryGet and Decrement — no crash
        // ----------------------------------------------------------------
        [Test]
        [Category("SharedBlockStore")]
        public void ConcurrentTryGetAndDecrement_NoCrash()
        {
            var mock = new MockSharedBlockStoreFileIO(m_testDir);
            using var store = new SharedBlockStore(m_testDir, -1, mock);

            var data = MakeData(256, 0xAA);
            store.Add(200L, data.AsSpan());

            var tasks = new List<Task>();
            // Several readers
            for (int i = 0; i < 8; i++)
            {
                tasks.Add(Task.Run(() =>
                {
                    var r = store.TryGet(200L, out var block);
                    block?.Dispose();
                }));
            }
            // One decrementer (runs concurrently with readers)
            tasks.Add(Task.Run(() => store.Decrement(200L)));

            Assert.DoesNotThrow(() => Task.WaitAll([.. tasks]));
        }

        // ----------------------------------------------------------------
        // Test 7: IO error in Add — Add returns false, TryGet returns Miss
        // ----------------------------------------------------------------
        [Test]
        [Category("SharedBlockStore")]
        public void IoErrorInAdd_AddReturnsFalse_TryGetReturnsMiss()
        {
            var mock = new MockSharedBlockStoreFileIO(m_testDir);
            using var store = new SharedBlockStore(m_testDir, -1, mock);

            mock.FailNextWrite = true;
            var data = MakeData(64, 0x01);
            bool added = store.Add(300L, data.AsSpan());
            Assert.IsFalse(added, "Add should return false when write fails");

            var result = store.TryGet(300L, out var block);
            Assert.AreEqual(SharedBlockStoreReadResult.Miss, result);
            Assert.IsNull(block);
        }

        // ----------------------------------------------------------------
        // Test 8: Dispose with remainder — logs warning but does not throw
        // ----------------------------------------------------------------
        [Test]
        [Category("SharedBlockStore")]
        public void DisposeWithRemainder_DoesNotThrow()
        {
            var mock = new MockSharedBlockStoreFileIO(m_testDir);
            var store = new SharedBlockStore(m_testDir, -1, mock);

            var data = MakeData(32, 0x77);
            store.Add(400L, data.AsSpan());
            // Intentionally not calling Decrement before Dispose.

            Assert.DoesNotThrow(() => store.Dispose());
        }

        // ----------------------------------------------------------------
        // Test 9: Read failure recovery — TryGet returns ReadFailure
        // ----------------------------------------------------------------
        [Test]
        [Category("SharedBlockStore")]
        public void ReadFailureRecovery_TryGetReturnsReadFailure()
        {
            var mock = new MockSharedBlockStoreFileIO(m_testDir);
            using var store = new SharedBlockStore(m_testDir, -1, mock);

            var data = MakeData(128, 0x42);
            bool added = store.Add(500L, data.AsSpan());
            Assert.IsTrue(added);

            mock.FailNextRead = true;
            var result = store.TryGet(500L, out var block);
            Assert.AreEqual(SharedBlockStoreReadResult.ReadFailure, result);
            Assert.IsNull(block);
        }

        // ----------------------------------------------------------------
        // Test 10: Empty instance no-ops — Add returns false, TryGet returns Miss,
        //          Decrement and Dispose do not throw.
        // ----------------------------------------------------------------
        [Test]
        [Category("SharedBlockStore")]
        public void EmptyInstance_AllNoOps()
        {
            var empty = SharedBlockStore.Empty;

            var data = MakeData(64, 0xFF);
            bool added = empty.Add(999L, data.AsSpan());
            Assert.IsFalse(added, "Empty.Add should return false");

            var result = empty.TryGet(999L, out var block);
            Assert.AreEqual(SharedBlockStoreReadResult.Miss, result);
            Assert.IsNull(block);

            Assert.DoesNotThrow(() => empty.Decrement(999L));
            Assert.DoesNotThrow(() => empty.Dispose());
        }

        // ----------------------------------------------------------------
        // Test 11: Budget enforcement — Add beyond budget returns false
        // ----------------------------------------------------------------
        [Test]
        [Category("SharedBlockStore")]
        public void BudgetEnforcement_AddBeyondBudgetReturnsFalse()
        {
            var mock = new MockSharedBlockStoreFileIO(m_testDir);
            // Budget of 100 bytes
            using var store = new SharedBlockStore(m_testDir, 100L, mock);

            var data80 = MakeData(80, 0x01);
            bool added1 = store.Add(1L, data80.AsSpan());
            Assert.IsTrue(added1);

            // Second add would push total to 160, exceeding budget of 100.
            var data80b = MakeData(80, 0x02);
            bool added2 = store.Add(2L, data80b.AsSpan());
            Assert.IsFalse(added2, "Should fail: would exceed budget");
        }

        // ----------------------------------------------------------------
        // Test 12: PeakBytesWritten reflects the append-only on-disk footprint
        // ----------------------------------------------------------------
        [Test]
        [Category("SharedBlockStore")]
        public void PeakBytesWritten_TrackedCorrectly()
        {
            var mock = new MockSharedBlockStoreFileIO(m_testDir);
            using var store = new SharedBlockStore(m_testDir, -1, mock);

            Assert.AreEqual(0L, store.PeakBytesWritten);

            var data = MakeData(100, 0x01);
            store.Add(1L, data.AsSpan());
            Assert.AreEqual(100L, store.PeakBytesWritten);

            store.Add(2L, data.AsSpan());
            Assert.AreEqual(200L, store.PeakBytesWritten);

            store.Decrement(1L);
            store.Decrement(2L);
            // The store is append-only, so the reported value should not decrease after Decrement.
            Assert.AreEqual(200L, store.PeakBytesWritten);
        }

        // ----------------------------------------------------------------
        // Test 13: Dispose is idempotent
        // ----------------------------------------------------------------
        [Test]
        [Category("SharedBlockStore")]
        public void DoubleDispose_IsIdempotent()
        {
            var mock = new MockSharedBlockStoreFileIO(m_testDir);
            var store = new SharedBlockStore(m_testDir, -1, mock);
            var data = MakeData(32, 0x12);
            store.Add(10L, data.AsSpan());
            store.Decrement(10L);

            Assert.DoesNotThrow(() => store.Dispose());
            Assert.DoesNotThrow(() => store.Dispose());
        }
    }
}
