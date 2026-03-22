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
using System.Threading;
using System.Threading.Tasks;
using Duplicati.Library.Common.IO;
using Duplicati.Library.Main.Database;
using Duplicati.Library.SQLiteHelper;
using Duplicati.Library.Utility;
using NUnit.Framework;

namespace Duplicati.UnitTest
{
    public class LocalRestoreDatabaseOrderingTests : BasicSetupHelper
    {
        [Test]
        [Category("RestoreHandler")]
        public async Task GetFilesAndSymlinksToRestore_PrefersSharedRefScoreOverDensity_WhenEnabled()
        {
            using var tempFile = new TempFile();
            await using var db = await LocalRestoreDatabase.CreateAsync(tempFile, null, CancellationToken.None)
                .ConfigureAwait(false);

            SeedRestoreOrderingScenario(db, Util.AppendDirSeparator(DATAFOLDER));

            await db.PrepareRestoreFilelist(DateTime.UtcNow, [], new FilterExpression(), CancellationToken.None)
                .ConfigureAwait(false);
            await db.SetTargetPaths(Util.AppendDirSeparator(DATAFOLDER), RESTOREFOLDER, CancellationToken.None)
                .ConfigureAwait(false);

            var legacyOrder = await CollectAsync(db.GetFilesAndSymlinksToRestore(0, true, CancellationToken.None))
                .ConfigureAwait(false);
            var sharedAwareOrder = await CollectAsync(db.GetFilesAndSymlinksToRestore(1, true, CancellationToken.None))
                .ConfigureAwait(false);

            var denseFiles = new[] { "dense-a-1.bin", "dense-a-2.bin", "dense-a-3.bin" };
            var sharedFiles = new[] { "shared-b-1.bin", "shared-b-2.bin" };

            Assert.That(legacyOrder.Take(3).Select(x => Path.GetFileName(x.TargetPath)), Is.EquivalentTo(denseFiles),
                "Threshold 0 should keep the legacy density-first Phase-2 ordering.");
            Assert.That(sharedAwareOrder.Take(2).Select(x => Path.GetFileName(x.TargetPath)), Is.EquivalentTo(sharedFiles),
                "Shared-aware ordering should promote the lower-density volume with the higher shared-reference drain score.");
            Assert.That(sharedAwareOrder.Skip(2).Take(3).Select(x => Path.GetFileName(x.TargetPath)), Is.EquivalentTo(denseFiles),
                "The dense single-volume files should fall back behind the shared-heavy volume once the new primary sort key is active.");
        }

        private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> source)
        {
            var results = new List<T>();
            await foreach (var item in source.ConfigureAwait(false))
                results.Add(item);

            return results;
        }

        private static void SeedRestoreOrderingScenario(LocalRestoreDatabase db, string sourcePrefix)
        {
            using var cmd = db.Connection.CreateCommand();

            cmd.SetCommandAndParameters(@"
                INSERT OR IGNORE INTO Operation (ID, Description, Timestamp)
                VALUES (1, 'RestoreOrderingTest', 0);")
                .ExecuteNonQuery();

            InsertRemoteVolume(cmd, 10, "dense-blocks.dblock.zip.aes", "Blocks");
            InsertRemoteVolume(cmd, 20, "shared-blocks.dblock.zip.aes", "Blocks");
            InsertRemoteVolume(cmd, 100, "fileset.dlist.zip.aes", "Files");

            cmd.SetCommandAndParameters(@"
                INSERT INTO Fileset (ID, OperationID, VolumeID, IsFullBackup, Timestamp)
                VALUES (1, 1, 100, 1, 1);")
                .ExecuteNonQuery();

            cmd.SetCommandAndParameters(@"
                INSERT INTO PathPrefix (ID, Prefix)
                VALUES (1, @Prefix);")
                .SetParameterValue("@Prefix", sourcePrefix)
                .ExecuteNonQuery();

            for (var fileIndex = 1; fileIndex <= 3; fileIndex++)
            {
                var dataBlocksetId = 100 + fileIndex;
                var metadataBlocksetId = 200 + fileIndex;
                var metadataId = 300 + fileIndex;
                var dataBlockId = 400 + fileIndex;
                var metadataBlockId = 500 + fileIndex;

                InsertBlockset(cmd, dataBlocksetId, 100, $"dense-data-{fileIndex}");
                InsertBlock(cmd, dataBlockId, $"dense-block-{fileIndex}", 100, 10);
                InsertBlocksetEntry(cmd, dataBlocksetId, 0, dataBlockId);

                InsertBlockset(cmd, metadataBlocksetId, 1, $"dense-meta-{fileIndex}");
                InsertBlock(cmd, metadataBlockId, $"dense-meta-block-{fileIndex}", 1, 10);
                InsertBlocksetEntry(cmd, metadataBlocksetId, 0, metadataBlockId);
                InsertMetadata(cmd, metadataId, metadataBlocksetId);

                InsertFile(cmd, fileIndex, $"dense-a-{fileIndex}.bin", dataBlocksetId, metadataId);
            }

            for (var sharedIndex = 0; sharedIndex < 4; sharedIndex++)
            {
                var blockId = 700 + sharedIndex;
                InsertBlock(cmd, blockId, $"shared-block-{sharedIndex}", 50, 20);
            }

            for (var fileIndex = 1; fileIndex <= 2; fileIndex++)
            {
                var fileId = 10 + fileIndex;
                var dataBlocksetId = 800 + fileIndex;
                var metadataBlocksetId = 900 + fileIndex;
                var metadataId = 1000 + fileIndex;
                var metadataBlockId = 1100 + fileIndex;

                InsertBlockset(cmd, dataBlocksetId, 200, $"shared-data-{fileIndex}");
                for (var sharedIndex = 0; sharedIndex < 4; sharedIndex++)
                    InsertBlocksetEntry(cmd, dataBlocksetId, sharedIndex, 700 + sharedIndex);

                InsertBlockset(cmd, metadataBlocksetId, 1, $"shared-meta-{fileIndex}");
                InsertBlock(cmd, metadataBlockId, $"shared-meta-block-{fileIndex}", 1, 20);
                InsertBlocksetEntry(cmd, metadataBlocksetId, 0, metadataBlockId);
                InsertMetadata(cmd, metadataId, metadataBlocksetId);

                InsertFile(cmd, fileId, $"shared-b-{fileIndex}.bin", dataBlocksetId, metadataId);
            }
        }

        private static void InsertRemoteVolume(System.Data.Common.DbCommand cmd, long id, string name, string type)
        {
            cmd.SetCommandAndParameters(@"
                INSERT INTO RemoteVolume (ID, OperationID, Name, Type, State, Size, VerificationCount, DeleteGraceTime, ArchiveTime, LockExpirationTime)
                VALUES (@Id, 1, @Name, @Type, 'Verified', 1024, 0, 0, 0, 0);")
                .SetParameterValue("@Id", id)
                .SetParameterValue("@Name", name)
                .SetParameterValue("@Type", type)
                .ExecuteNonQuery();
        }

        private static void InsertBlockset(System.Data.Common.DbCommand cmd, long blocksetId, long length, string fullHash)
        {
            cmd.SetCommandAndParameters(@"
                INSERT INTO Blockset (ID, Length, FullHash)
                VALUES (@BlocksetId, @Length, @FullHash);")
                .SetParameterValue("@BlocksetId", blocksetId)
                .SetParameterValue("@Length", length)
                .SetParameterValue("@FullHash", fullHash)
                .ExecuteNonQuery();
        }

        private static void InsertBlock(System.Data.Common.DbCommand cmd, long blockId, string hash, long size, long volumeId)
        {
            cmd.SetCommandAndParameters(@"
                INSERT INTO Block (ID, Hash, Size, VolumeID)
                VALUES (@BlockId, @Hash, @Size, @VolumeId);")
                .SetParameterValue("@BlockId", blockId)
                .SetParameterValue("@Hash", hash)
                .SetParameterValue("@Size", size)
                .SetParameterValue("@VolumeId", volumeId)
                .ExecuteNonQuery();
        }

        private static void InsertBlocksetEntry(System.Data.Common.DbCommand cmd, long blocksetId, long index, long blockId)
        {
            cmd.SetCommandAndParameters(@"
                INSERT INTO BlocksetEntry (BlocksetID, ""Index"", BlockID)
                VALUES (@BlocksetId, @Index, @BlockId);")
                .SetParameterValue("@BlocksetId", blocksetId)
                .SetParameterValue("@Index", index)
                .SetParameterValue("@BlockId", blockId)
                .ExecuteNonQuery();
        }

        private static void InsertMetadata(System.Data.Common.DbCommand cmd, long metadataId, long blocksetId)
        {
            cmd.SetCommandAndParameters(@"
                INSERT INTO Metadataset (ID, BlocksetID)
                VALUES (@MetadataId, @BlocksetId);")
                .SetParameterValue("@MetadataId", metadataId)
                .SetParameterValue("@BlocksetId", blocksetId)
                .ExecuteNonQuery();
        }

        private static void InsertFile(System.Data.Common.DbCommand cmd, long fileId, string name, long blocksetId, long metadataId)
        {
            cmd.SetCommandAndParameters(@"
                INSERT INTO FileLookup (ID, PrefixID, Path, BlocksetID, MetadataID)
                VALUES (@FileId, 1, @Path, @BlocksetId, @MetadataId);")
                .SetParameterValue("@FileId", fileId)
                .SetParameterValue("@Path", name)
                .SetParameterValue("@BlocksetId", blocksetId)
                .SetParameterValue("@MetadataId", metadataId)
                .ExecuteNonQuery();

            cmd.SetCommandAndParameters(@"
                INSERT INTO FilesetEntry (FilesetID, FileID, Lastmodified)
                VALUES (1, @FileId, 0);")
                .SetParameterValue("@FileId", fileId)
                .ExecuteNonQuery();
        }
    }
}