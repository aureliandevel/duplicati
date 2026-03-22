using System.CommandLine;
using System.CommandLine.NamingConventionBinder;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Duplicati.Library.Interface;
using Duplicati.Library.Main.Database;
using Duplicati.Library.Main.Operation.Restore;
using Duplicati.Library.Utility;

namespace Duplicati.CommandLine.DatabaseTool.Commands;

/// <summary>
/// Analyzes restore-set volume spread and predicts single-processor cache residency.
/// </summary>
public static class AnalyzeRestoreCache
{
    private const int DEFAULT_PHASE1_WINDOW_SIZE = 5000;
    private const int DEFAULT_PHASE1_TOP_WINDOWS = 5;
    private const int MAX_DEFRAG_CANDIDATE_SIMULATIONS = 5;

    private enum Phase2Heuristic
    {
        CurrentSql,
        ExistingVolumeFirst,
        DrainActiveVolumeFirst,
        MinResidualTail,
    }

    private enum VolumeTouchKind
    {
        FileData,
        FileMetadata,
        FolderMetadata,
    }

    private enum RestoreItemKind
    {
        FileData,
        FolderMetadata,
    }

    private enum SqlRestorePhase
    {
        Phase1 = 1,
        Phase2 = 2,
        Phase3 = 3,
        FolderMetadata = 4,
    }

    private sealed record AnalysisOptions(
        DateTime RestoreTime,
        long[] Versions,
        string[] Paths,
        bool SkipMetadata,
        int SharedBlockThreshold,
        long RestoreVolumeCacheHint,
        int TopVolumes,
        int Phase1WindowSize,
        int Phase1TopWindows,
        bool RedactPaths,
        bool ComparePhase2Heuristics,
        string HtmlOutputPath
    );

    private sealed record RestoreItem(FileRequest File, RestoreItemKind Kind, int SequenceIndex, SqlRestorePhase SqlPhase);

    private sealed record SimulatedBlock(long BlockId, long BlockSize, long VolumeId, bool IsMetadata);

    private sealed record RestoreItemProfile(
        RestoreItem Item,
        IReadOnlyList<SimulatedBlock> Blocks,
        IReadOnlyDictionary<long, long> VolumeReferenceUsage,
        IReadOnlyCollection<long> DistinctVolumes,
        int OriginalOrder)
    {
        public bool IsPhase1File => Item.Kind == RestoreItemKind.FileData && Item.SqlPhase == SqlRestorePhase.Phase1;
        public bool IsPhase2Candidate => Item.Kind == RestoreItemKind.FileData && Item.SqlPhase == SqlRestorePhase.Phase2;
        public bool IsPhase3File => Item.Kind == RestoreItemKind.FileData && Item.SqlPhase == SqlRestorePhase.Phase3;
    }

    private sealed class VolumeObservation(long volumeId, long initialReferenceCount, long sharedRefScore)
    {
        public long VolumeId { get; } = volumeId;
        public long InitialReferenceCount { get; } = initialReferenceCount;
        public long SharedRefScore { get; } = sharedRefScore;
        public string Name { get; set; } = string.Empty;
        public long Size { get; set; }
        public int? FirstTouchItemIndex { get; set; }
        public string? FirstTouchPath { get; set; }
        public VolumeTouchKind? FirstTouchKind { get; set; }
        public int? ReleaseItemIndex { get; set; }
        public string? ReleasePath { get; set; }
        public VolumeTouchKind? ReleaseKind { get; set; }
        public int DistinctTouchedFiles { get; set; }
        public long LastTouchedFileId { get; set; } = long.MinValue;
        public long OperationId { get; set; } = -1;
        public DateTime? CreatedAtUtc { get; set; }
        public DateTime? ArchiveTimeUtc { get; set; }
        public string State { get; set; } = string.Empty;
    }

    private sealed record PeakPoint(
        int ActiveVolumeCount,
        int CachedVolumeCount,
        int EventIndex,
        int ItemIndex,
        string Path,
        long BlockId,
        long VolumeId,
        bool IsMetadata,
        IReadOnlyList<long> ActiveVolumeIds,
        IReadOnlyDictionary<long, long> RemainingReferencesAtPeak
    );

    private sealed record PhaseSnapshot(
        string Name,
        int ItemIndex,
        int ActiveVolumeCount,
        int CachedVolumeCount
    );

    private sealed record Phase1WindowSummary(
        int StartItemIndex,
        int EndItemIndex,
        int ItemCount,
        int StartActiveVolumeCount,
        int EndActiveVolumeCount,
        int NetActiveVolumeDelta,
        int PeakActiveVolumeCount,
        int PeakItemIndex,
        string StartPath,
        string EndPath,
        string PeakPath
    );

    private sealed record Phase1WindowXray(
        Phase1WindowSummary Window,
        int IntroducedVolumeCount,
        int PeakSurvivingVolumeCount,
        IReadOnlyList<VolumePeakDetail> TopIntroducedVolumes
    );

    private sealed record Phase1WindowState(
        Phase1WindowSummary Summary,
        IReadOnlyList<long> IntroducedVolumeIds
    );

    private sealed record VolumePeakDetail(
        long VolumeId,
        string Name,
        long Size,
        long RemainingReferencesAtPeak,
        long InitialReferences,
        long SharedRefScore,
        int DistinctTouchedFiles,
        long OperationId,
        DateTime? CreatedAtUtc,
        double? AgeAtRestoreDays,
        DateTime? ArchiveTimeUtc,
        string State,
        int? FirstTouchItemIndex,
        string? FirstTouchPath,
        string? FirstTouchKind,
        int? ReleaseItemIndex,
        string? ReleasePath,
        string? ReleaseKind
    );

    private sealed record FragmentationItemDetail(
        int ItemIndex,
        string Path,
        string ItemKind,
        string SqlPhase,
        int BlockReferences,
        int DataBlockReferences,
        int MetadataBlockReferences,
        int DistinctVolumes,
        double FragmentationScore,
        double EffectiveVolumeCount,
        double LargestVolumeShare,
        long LargestVolumeReferences
    );

    private sealed record FragmentationViewSummary(
        string Name,
        double BlockWeightedScore,
        double ItemWeightedScore,
        int ItemsWithBlocks,
        int MultiVolumeItems,
        double MultiVolumeItemPercentage,
        double AverageDistinctVolumes,
        int MedianDistinctVolumes,
        int P95DistinctVolumes,
        double AverageFragmentationScore,
        double MedianFragmentationScore,
        double P95FragmentationScore,
        double AverageEffectiveVolumeCount,
        double P95EffectiveVolumeCount,
        IReadOnlyList<FragmentationItemDetail> TopFragmentedItems
    );

    private sealed record FragmentationSummary(
        FragmentationViewSummary Overall,
        FragmentationViewSummary SharedVolumeOnly
    );

    private sealed record PeakPressureItem(
        int ItemIndex,
        string Path,
        string SqlPhase,
        long SharedDataBytes,
        int SharedDistinctVolumes,
        int PeakActiveVolumeOverlap,
        int PeakSharedVolumeOverlap,
        long PeakRemainingReferencesSum,
        long PeakSharedRemainingReferencesSum,
        double PeakPressureScore
    );

    private sealed record PeakPressureSummary(
        IReadOnlyList<PeakPressureItem> TopItems
    );

    private sealed record OrderingBaseline(
        string Name,
        int PredictedPeakActiveVolumes,
        int DeltaFromCurrent
    );

    private sealed record OrderingWindowDynamics(
        int StartActiveVolumes,
        int EndActiveVolumes,
        int NetActiveVolumeDelta,
        int IntroducedVolumes,
        int DrainedVolumes,
        long TotalDataBytes,
        long AverageDataBytes,
        int Phase1Items,
        int Phase2Items,
        int Phase3Items,
        int FolderMetadataItems
    );

    private sealed record OrderingComparisonWindow(
        int StartPosition,
        int EndPosition,
        int CurrentPeakActiveVolumes,
        int CurrentPeakPosition,
        string CurrentPeakPath,
        int PlainPeakActiveVolumes,
        int PlainPeakPosition,
        string PlainPeakPath,
        int PlainMinusCurrentDelta,
        OrderingWindowDynamics CurrentDynamics,
        OrderingWindowDynamics PlainDynamics
    );

    private sealed record OrderingComparisonSummary(
        int WindowSize,
        IReadOnlyList<OrderingBaseline> Baselines,
        IReadOnlyList<OrderingComparisonWindow> TopWindowsWherePlainBetter,
        IReadOnlyList<OrderingComparisonWindow> TopWindowsWherePhasedBetter
    );

    private sealed record BoundaryRelaxationScenario(
        string Name,
        int PreservedPhase1Items,
        double PreservedPhase1Percentage,
        int PredictedPeakActiveVolumes,
        int DeltaFromCurrent
    );

    private sealed record BoundaryRelaxationSummary(
        IReadOnlyList<BoundaryRelaxationScenario> Scenarios,
        BoundaryRelaxationScenario? FirstImprovement
    );

    private sealed record ConstrainedPrefixScenario(
        string Name,
        int PrefixFileItems,
        double PrefixPercentage,
        int PredictedPeakActiveVolumes,
        int DeltaFromCurrent
    );

    private sealed record ConstrainedPrefixSummary(
        IReadOnlyList<ConstrainedPrefixScenario> Scenarios
    );

    private sealed record AdaptiveInterleavingScenario(
        string Name,
        string Trigger,
        int PreservedPhase1Items,
        double PreservedPhase1Percentage,
        int PredictedPeakActiveVolumes,
        int DeltaFromCurrent
    );

    private sealed record AdaptiveInterleavingSummary(
        IReadOnlyList<AdaptiveInterleavingScenario> Scenarios,
        AdaptiveInterleavingScenario? BestScenario
    );

    private sealed record Phase1SizeThresholdScenario(
        string Name,
        long ThresholdBytes,
        int PreservedPhase1Items,
        double PreservedPhase1Percentage,
        int PredictedPeakActiveVolumes,
        int DeltaFromCurrent
    );

    private sealed record Phase1SizeThresholdSweepSummary(
        IReadOnlyList<Phase1SizeThresholdScenario> Scenarios,
        Phase1SizeThresholdScenario? FirstImprovement,
        Phase1SizeThresholdScenario? BestScenario
    );

    private sealed record InteractiveCatalogItem(
        int ItemIndex,
        string Path,
        string SqlPhase,
        string ItemKind,
        int BlockReferences,
        int DataBlockReferences,
        int MetadataBlockReferences,
        long DataBytes,
        long MetadataBytes,
        int DistinctVolumes,
        int SharedDistinctVolumes,
        int SharedBlockReferences,
        int SharedDataBlockReferences,
        int SharedMetadataBlockReferences,
        long SharedDataBytes,
        long SharedMetadataBytes
    );

    private sealed record InteractiveVolumeItem(
        long VolumeId,
        string Name,
        long Size,
        long InitialReferenceCount,
        long SharedRefScore,
        long OperationId,
        string State,
        IReadOnlyList<InteractiveVolumeUsage> Items
    );

    private sealed record InteractiveVolumeUsage(
        int ItemIndex,
        string Path,
        string SqlPhase,
        string ItemKind,
        int BlockReferences,
        int DataBlockReferences,
        int MetadataBlockReferences,
        long DataBytes,
        long MetadataBytes,
        int SharedBlockReferences,
        int SharedDataBlockReferences,
        int SharedMetadataBlockReferences,
        long SharedDataBytes,
        long SharedMetadataBytes
    );

    private sealed record InteractiveStrategyTrace(
        string Key,
        string Name,
        int PredictedPeakActiveVolumes,
        int PredictedPeakCachedVolumes,
        int DeltaFromCurrent,
        int PeakItemIndex,
        IReadOnlyList<int> Order,
        IReadOnlyList<int> ActiveAfter,
        IReadOnlyList<int> CachedAfter,
        IReadOnlyList<int> Introduced,
        IReadOnlyList<int> Drained,
        IReadOnlyList<int> RetiredTotal,
        IReadOnlyList<IReadOnlyList<long>> IntroducedVolumeIds,
        IReadOnlyList<IReadOnlyList<long>> DrainedVolumeIds,
        IReadOnlyDictionary<long, int> VolumeIntroducedAtItem,
        IReadOnlyDictionary<long, int> VolumeDrainedAtItem
    );

    private sealed record InteractiveRestoreMap(
        string Database,
        DateTime RestoreTimeUtc,
        int TotalItems,
        int DistinctVolumes,
        int SharedBlockThreshold,
        int CurrentPeakActiveVolumes,
        IReadOnlyList<InteractiveCatalogItem> Items,
        IReadOnlyList<InteractiveVolumeItem> Volumes,
        IReadOnlyList<InteractiveStrategyTrace> Strategies
    );

    private sealed record RuntimeCutoverRuleScenario(
        string Name,
        string Trigger,
        int PreservedPhase1Items,
        double PreservedPhase1Percentage,
        int PredictedPeakActiveVolumes,
        int DeltaFromCurrent
    );

    private sealed record RuntimeCutoverRuleSummary(
        IReadOnlyList<RuntimeCutoverRuleScenario> Scenarios,
        RuntimeCutoverRuleScenario? FirstImprovement,
        RuntimeCutoverRuleScenario? BestScenario
    );

    private sealed record LatePhase1TailCandidate(
        int ItemIndex,
        string Path,
        int TailOffset,
        long DataBytes,
        int DistinctVolumes,
        int IntroducedVolumes,
        int DrainedVolumes,
        int NetActiveVolumeDelta,
        int ActiveVolumesBefore,
        int ActiveVolumesAfter
    );

    private sealed record LatePhase1TailSummary(
        int TailSampleSize,
        IReadOnlyList<LatePhase1TailCandidate> Candidates
    );

    private sealed record LatePhase1TailCompositionSummary(
        int TailSampleSize,
        int ZeroDataItems,
        int AtMost4KiBItems,
        int AtMost64KiBItems,
        int AtMost256KiBItems,
        int MetadataDominantItems,
        int IntroducesNoVolumesItems,
        int DrainsNoVolumesItems,
        int NoVolumeEffectItems
    );

    private sealed record BoundaryRegionSegment(
        int StartPosition,
        int EndPosition,
        OrderingWindowDynamics CurrentDynamics,
        OrderingWindowDynamics PlainDynamics,
        int CurrentPeakActiveVolumes,
        int PlainPeakActiveVolumes,
        int PlainMinusCurrentDelta,
        string CurrentPeakPath,
        string PlainPeakPath
    );

    private sealed record BoundaryRegionSummary(
        int BoundaryPosition,
        int WindowSize,
        IReadOnlyList<BoundaryRegionSegment> Segments
    );

    private sealed record DefragWhatIfCandidate(
        int ItemIndex,
        string Path,
        string SqlPhase,
        int SharedBlockReferences,
        int SharedDataBlockReferences,
        long SharedDataBytes,
        int SharedDistinctVolumes,
        double SharedFragmentationScore,
        double SharedEffectiveVolumeCount,
        int PeakSharedVolumeOverlap,
        long PeakSharedRemainingReferencesSum,
        double PeakPressureScore,
        int EstimatedPrivateVolumesAfterDefrag,
        int PredictedPeakActiveVolumesAfterDefrag,
        int PredictedPeakActiveVolumeDelta,
        int PredictedPhase1TailSizePeakAfterDefrag,
        int PredictedPhase1TailSizePeakDelta,
        int PredictedPlainSizePeakAfterDefrag,
        int PredictedPlainSizePeakDelta
    );

    private sealed record DefragWhatIfScenario(
        string Name,
        int FilesDefragmented,
        int PredictedPeakActiveVolumesAfterDefrag,
        int PredictedPeakActiveVolumeDelta,
        int PredictedPhase1TailSizePeakAfterDefrag,
        int PredictedPhase1TailSizePeakDelta,
        int PredictedPlainSizePeakAfterDefrag,
        int PredictedPlainSizePeakDelta
    );

    private sealed record DefragWhatIfSummary(
        int CurrentPhasedPeakActiveVolumes,
        int CurrentPhase1TailSizePeakActiveVolumes,
        int CurrentPhase1TailSizePeakDelta,
        int CurrentPlainSizePeakActiveVolumes,
        int CurrentPlainSizePeakDelta,
        int PeakEligibleCandidateCount,
        int CandidateCountSimulated,
        IReadOnlyList<DefragWhatIfCandidate> TopCandidates,
        IReadOnlyList<DefragWhatIfScenario> IncrementalScenarios
    );

    private sealed record AnalysisReport(
        string Database,
        DateTime RestoreTimeUtc,
        IReadOnlyList<long> Versions,
        IReadOnlyList<string> Paths,
        bool SkipMetadata,
        long RestoreVolumeCacheHint,
        long EffectiveVolumeCacheBudget,
        int RequestedSharedBlockThreshold,
        int EffectiveSharedBlockThreshold,
        int RestoreItems,
        int FileItems,
        int FolderMetadataItems,
        long BlockRequests,
        int DistinctBlocks,
        int DistinctVolumes,
        int SharedBlockCandidates,
        int SharedBlocksStored,
        long SharedBlockStoreBytesWritten,
        int PredictedPeakActiveVolumes,
        int PredictedPeakCachedVolumes,
        PeakPoint? Peak,
        IReadOnlyList<PhaseSnapshot> PhaseSnapshots,
        IReadOnlyList<Phase1WindowSummary> Phase1TopGrowthWindows,
        IReadOnlyList<Phase1WindowXray> Phase1WindowXray,
        IReadOnlyDictionary<string, int> PeakActiveVolumesByFirstTouchKind,
        IReadOnlyDictionary<string, int> PeakActiveVolumesByReleaseKind,
        FragmentationSummary Fragmentation,
        PeakPressureSummary PeakPressure,
        OrderingComparisonSummary OrderingComparison,
        BoundaryRelaxationSummary BoundaryRelaxation,
        ConstrainedPrefixSummary ConstrainedPrefixes,
        AdaptiveInterleavingSummary AdaptiveInterleaving,
        Phase1SizeThresholdSweepSummary Phase1SizeThresholdSweep,
        RuntimeCutoverRuleSummary RuntimeCutoverRules,
        InteractiveRestoreMap? InteractiveMap,
        LatePhase1TailSummary LatePhase1Tail,
        LatePhase1TailCompositionSummary LatePhase1TailComposition,
        BoundaryRegionSummary BoundaryRegion,
        DefragWhatIfSummary DefragWhatIf,
        int Phase2CandidateItems,
        IReadOnlyList<HeuristicComparison> Phase2HeuristicComparisons,
        string? Phase2HeuristicComparisonStatus,
        int PeakVolumesIntroducedBeforePhase2,
        IReadOnlyList<VolumePeakDetail> TopPrePhase2PinningVolumes,
        IReadOnlyList<VolumePeakDetail> TopPinningVolumes,
        IReadOnlyList<string> Assumptions
    );

    private sealed record HeuristicComparison(
        string Name,
        int PredictedPeakActiveVolumes,
        int PredictedPeakCachedVolumes,
        int ActiveVolumeDeltaFromCurrent,
        int CachedVolumeDeltaFromCurrent
    );

    private sealed record ItemTransition(
        int ItemIndex,
        int ActiveVolumesBefore,
        int ActiveVolumesAfter,
        int IntroducedVolumes,
        int DrainedVolumes,
        int NetActiveVolumeDelta
    );

    private sealed record SimulationModel(
        IReadOnlyList<RestoreItemProfile> Profiles,
        IReadOnlyDictionary<long, long> BlockReferenceCounts,
        IReadOnlyDictionary<long, long> VolumeReferenceCounts,
        IReadOnlyDictionary<long, long> SharedRefScores,
        IReadOnlyDictionary<long, string> VolumeNames,
        IReadOnlyDictionary<long, long> VolumeSizes,
        IReadOnlyDictionary<long, long> VolumeOperationIds,
        IReadOnlyDictionary<long, DateTime?> VolumeCreatedAtUtc,
        IReadOnlyDictionary<long, DateTime?> VolumeArchiveTimeUtc,
        IReadOnlyDictionary<long, string> VolumeStates
    );

    private sealed record SimulationResult(
        long BlockRequests,
        int DistinctBlocks,
        int DistinctVolumes,
        int SharedBlockCandidates,
        int SharedBlocksStored,
        long SharedBlockStoreBytesWritten,
        int PredictedPeakActiveVolumes,
        int PredictedPeakCachedVolumes,
        PeakPoint? Peak,
        IReadOnlyList<PhaseSnapshot> PhaseSnapshots,
        IReadOnlyList<Phase1WindowSummary> Phase1TopGrowthWindows,
        IReadOnlyList<Phase1WindowXray> Phase1WindowXray,
        IReadOnlyDictionary<string, int> PeakActiveVolumesByFirstTouchKind,
        IReadOnlyDictionary<string, int> PeakActiveVolumesByReleaseKind,
        IReadOnlyList<int>? ActiveVolumesAfterEachItem,
        IReadOnlyList<int>? OrderedItemIndexes,
        IReadOnlyList<int>? CachedVolumesAfterEachItem,
        IReadOnlyList<int>? IntroducedVolumesByItem,
        IReadOnlyList<int>? DrainedVolumesByItem,
        IReadOnlyList<int>? RetiredVolumesTotalAfterEachItem,
        IReadOnlyList<IReadOnlyList<long>>? IntroducedVolumeIdsByItem,
        IReadOnlyList<IReadOnlyList<long>>? DrainedVolumeIdsByItem,
        IReadOnlyDictionary<long, int>? VolumeIntroducedAtItem,
        IReadOnlyDictionary<long, int>? VolumeDrainedAtItem,
        int PeakVolumesIntroducedBeforePhase2,
        IReadOnlyList<VolumePeakDetail> TopPrePhase2PinningVolumes,
        IReadOnlyList<VolumePeakDetail> TopPinningVolumes
    );

    private sealed record DefragCandidateSeed(
        RestoreItemProfile Profile,
        FragmentationItemDetail SharedItem,
        PeakPressureItem? PeakPressureItem,
        int SharedBlockReferences,
        int SharedDataBlockReferences,
        long SharedDataBytes,
        int SharedDistinctVolumes,
        double PriorityScore
    );

    private sealed record DefragLayout(
        IReadOnlyList<RestoreItemProfile> Profiles,
        IReadOnlyDictionary<long, long> SyntheticVolumeSizes,
        IReadOnlyDictionary<long, int> EstimatedPrivateVolumeCountsByFileId
    );

    /// <summary>
    /// Creates the command.
    /// </summary>
    public static Command Create() =>
        new Command("analyze-restore-cache", "Analyzes restore-set volume spread and predicts single-processor restore-cache residency")
        {
            new Argument<string>("database", "The local backup database to analyze"),
            new Argument<string[]>("paths")
            {
                Arity = ArgumentArity.ZeroOrMore,
                Description = "Optional restore paths or filter expressions. If omitted, the full restore set is analyzed."
            },
            new Option<string>("--time", description: "Restore time to analyze. Defaults to now (latest available fileset).", getDefaultValue: () => string.Empty),
            new Option<long[]>("--version", description: "Specific restore version(s) to analyze. If omitted, restore-time selection is used.")
            {
                Arity = ArgumentArity.ZeroOrMore,
            },
            new Option<bool>("--skip-metadata", description: "Exclude metadata blocks from the analysis", getDefaultValue: () => false),
            new Option<int>("--shared-block-threshold", description: "Shared-block threshold used for routing and Phase-2 ordering", getDefaultValue: () => Duplicati.Library.Main.Options.DEFAULT_RESTORE_SHARED_BLOCK_CACHE_THRESHOLD),
            new Option<long>("--restore-volume-cache-hint", description: "Restore volume cache hint in bytes. Use -1 for unlimited, 0 to disable caching.", getDefaultValue: () => -1L),
            new Option<int>("--top", description: "Number of top pinning volumes to display", getDefaultValue: () => 20),
            new Option<int>("--phase1-window-size", description: "Number of SQL Phase-1 items per reporting window.", getDefaultValue: () => DEFAULT_PHASE1_WINDOW_SIZE),
            new Option<int>("--phase1-top-windows", description: "How many Phase-1 growth windows to display.", getDefaultValue: () => DEFAULT_PHASE1_TOP_WINDOWS),
            new Option<bool>("--redact-paths", description: "Redact file and folder paths in output using stable anonymous tokens.", getDefaultValue: () => false),
            new Option<bool>("--compare-phase2-heuristics", description: "Run expensive offline Phase-2 reordering comparisons. Disabled by default because large restore sets can be CPU-heavy.", getDefaultValue: () => false),
            new Option<string>("--output-html", description: "Write a self-contained interactive HTML restore map to this path.", getDefaultValue: () => string.Empty),
            new Option<bool>("--output-json", description: "Output the analysis as JSON", getDefaultValue: () => false),
        }
        .WithHandler(CommandHandler.Create<string, string[], string, long[], bool, int, long, int, int, int, bool, bool, string, bool>(RunAsync));

    private static async Task RunAsync(string database, string[] paths, string time, long[] version, bool skipmetadata, int sharedblockthreshold, long restorevolumecachehint, int top, int phase1windowsize, int phase1topwindows, bool redactpaths, bool comparephase2heuristics, string outputhtml, bool outputjson)
    {
        if (!File.Exists(database))
            throw new UserInformationException($"Database {database} does not exist", "DatabaseNotFound");

        if (sharedblockthreshold < 0)
            throw new UserInformationException("--shared-block-threshold must be zero or greater", "InvalidSharedBlockThreshold");

        if (top <= 0)
            throw new UserInformationException("--top must be greater than zero", "InvalidTopCount");

        if (phase1windowsize <= 0)
            throw new UserInformationException("--phase1-window-size must be greater than zero", "InvalidPhase1WindowSize");

        if (phase1topwindows <= 0)
            throw new UserInformationException("--phase1-top-windows must be greater than zero", "InvalidPhase1TopWindows");

        var options = new AnalysisOptions(
            ParseRestoreTime(time),
            version ?? [],
            paths ?? [],
            skipmetadata,
            sharedblockthreshold,
            restorevolumecachehint,
            top,
            phase1windowsize,
            phase1topwindows,
            redactpaths,
            comparephase2heuristics,
            outputhtml ?? string.Empty);

        var report = await AnalyzeAsync(database, options, CancellationToken.None).ConfigureAwait(false);
        if (options.RedactPaths)
            report = RedactReportPaths(report);

        if (!string.IsNullOrWhiteSpace(options.HtmlOutputPath) && report.InteractiveMap is not null)
            WriteInteractiveRestoreMapHtml(report.InteractiveMap, options.HtmlOutputPath);

        if (outputjson)
        {
            Console.WriteLine(JsonSerializer.Serialize(report, new JsonSerializerOptions { WriteIndented = true }));
            return;
        }

        PrintReport(report);
        if (!string.IsNullOrWhiteSpace(options.HtmlOutputPath))
        {
            Console.WriteLine();
            Console.WriteLine($"Interactive restore map written to: {options.HtmlOutputPath}");
        }
    }

    private static async Task<AnalysisReport> AnalyzeAsync(string database, AnalysisOptions options, CancellationToken token)
    {
        await using var db = await LocalRestoreDatabase.CreateAsync(database, null, token).ConfigureAwait(false);

        IFilter filter = options.Paths.Length == 0
            ? new FilterExpression()
            : new FilterExpression(options.Paths);

        await db.PrepareRestoreFilelist(options.RestoreTime, options.Versions, filter, token).ConfigureAwait(false);
        await db.SetTargetPaths(string.Empty, string.Empty, token).ConfigureAwait(false);

        int effectiveSharedThreshold = options.RestoreVolumeCacheHint != 0 && options.SharedBlockThreshold > 0
            ? options.SharedBlockThreshold
            : 0;
        long effectiveVolumeCacheBudget = options.RestoreVolumeCacheHint > 0
            ? RestoreCacheBudget.GetVolumeCacheBudget(options.RestoreVolumeCacheHint)
            : options.RestoreVolumeCacheHint;
        long sharedBlockStoreBudget = options.RestoreVolumeCacheHint > 0
            ? RestoreCacheBudget.GetSharedBlockStoreBudget(options.RestoreVolumeCacheHint)
            : -1L;
        var profiles = await CollectRestoreItemProfilesAsync(db, effectiveSharedThreshold, options.SkipMetadata, token).ConfigureAwait(false);
        var simulationModel = await BuildSimulationModelAsync(db, profiles, effectiveSharedThreshold, token).ConfigureAwait(false);
        var volumeItemCounts = BuildVolumeItemCounts(simulationModel.Profiles);
        var fragmentation = BuildFragmentationSummary(simulationModel.Profiles, volumeItemCounts, options.TopVolumes);
        var phase2CandidateItems = simulationModel.Profiles.Count(x => x.IsPhase2Candidate);
        var phase1TailSizeOrderedProfiles = ReorderProfilesByPhase1ThenSizeDescendingTail(simulationModel.Profiles);
        var plainOrderedProfiles = ReorderProfilesByPlainSizeDescending(simulationModel.Profiles);
        int phaseBoundaryPosition = simulationModel.Profiles.FirstOrDefault(profile => profile.IsPhase2Candidate)?.Item.SequenceIndex ?? simulationModel.Profiles.Count + 1;
        var currentSimulation = Simulate(
            simulationModel.Profiles,
            simulationModel,
            effectiveSharedThreshold,
            effectiveVolumeCacheBudget,
            sharedBlockStoreBudget,
            options.RestoreVolumeCacheHint,
            options.TopVolumes,
            options.Phase1WindowSize,
            options.Phase1TopWindows,
            options.RestoreTime.ToUniversalTime(),
            captureDetails: true,
            captureItemTrace: true,
            captureTimeline: !string.IsNullOrWhiteSpace(options.HtmlOutputPath));
        var phase1TailSizeBaseline = Simulate(
            phase1TailSizeOrderedProfiles,
            simulationModel,
            effectiveSharedThreshold,
            effectiveVolumeCacheBudget,
            sharedBlockStoreBudget,
            options.RestoreVolumeCacheHint,
            topVolumes: 0,
            phase1WindowSize: 1,
            phase1TopWindows: 0,
            options.RestoreTime.ToUniversalTime(),
            captureDetails: false,
            captureItemTrace: false,
            captureTimeline: !string.IsNullOrWhiteSpace(options.HtmlOutputPath));
        var plainSizeBaseline = Simulate(
            plainOrderedProfiles,
            simulationModel,
            effectiveSharedThreshold,
            effectiveVolumeCacheBudget,
            sharedBlockStoreBudget,
            options.RestoreVolumeCacheHint,
            topVolumes: 0,
            phase1WindowSize: 1,
            phase1TopWindows: 0,
            options.RestoreTime.ToUniversalTime(),
            captureDetails: false,
            captureItemTrace: true,
            captureTimeline: !string.IsNullOrWhiteSpace(options.HtmlOutputPath));
        var peakPressure = BuildPeakPressureSummary(simulationModel.Profiles, volumeItemCounts, currentSimulation.Peak, options.TopVolumes);
        var orderingComparison = BuildOrderingComparisonSummary(
            simulationModel.Profiles,
            currentSimulation,
            phase1TailSizeBaseline,
            plainOrderedProfiles,
            plainSizeBaseline,
            options.Phase1WindowSize,
            Math.Min(options.TopVolumes, 5));
        var boundaryRelaxation = BuildBoundaryRelaxationSummary(
            simulationModel,
            effectiveSharedThreshold,
            effectiveVolumeCacheBudget,
            sharedBlockStoreBudget,
            options.RestoreVolumeCacheHint,
            options.RestoreTime.ToUniversalTime(),
            currentSimulation.PredictedPeakActiveVolumes);
        var constrainedPrefixes = BuildConstrainedPrefixSummary(
            simulationModel,
            effectiveSharedThreshold,
            effectiveVolumeCacheBudget,
            sharedBlockStoreBudget,
            options.RestoreVolumeCacheHint,
            options.RestoreTime.ToUniversalTime(),
            currentSimulation.PredictedPeakActiveVolumes);
        var adaptiveInterleaving = BuildAdaptiveInterleavingSummary(
            simulationModel,
            effectiveSharedThreshold,
            effectiveVolumeCacheBudget,
            sharedBlockStoreBudget,
            options.RestoreVolumeCacheHint,
            options.RestoreTime.ToUniversalTime(),
            currentSimulation.PredictedPeakActiveVolumes);
        var phase1SizeThresholdSweep = BuildPhase1SizeThresholdSweepSummary(
            simulationModel,
            effectiveSharedThreshold,
            effectiveVolumeCacheBudget,
            sharedBlockStoreBudget,
            options.RestoreVolumeCacheHint,
            options.RestoreTime.ToUniversalTime(),
            currentSimulation.PredictedPeakActiveVolumes);
        var runtimeCutoverRules = BuildRuntimeCutoverRuleSummary(
            simulationModel,
            effectiveSharedThreshold,
            effectiveVolumeCacheBudget,
            sharedBlockStoreBudget,
            options.RestoreVolumeCacheHint,
            options.RestoreTime.ToUniversalTime(),
            currentSimulation.PredictedPeakActiveVolumes);
        var interactiveMap = BuildInteractiveRestoreMap(
            database,
            options,
            simulationModel,
            currentSimulation,
            phase1TailSizeOrderedProfiles,
            phase1TailSizeBaseline,
            plainOrderedProfiles,
            plainSizeBaseline,
            boundaryRelaxation,
            constrainedPrefixes,
            adaptiveInterleaving,
            phase1SizeThresholdSweep,
            runtimeCutoverRules,
            effectiveSharedThreshold,
            effectiveVolumeCacheBudget,
            sharedBlockStoreBudget,
            currentSimulation.PredictedPeakActiveVolumes);
        var latePhase1Tail = BuildLatePhase1TailSummary(
            simulationModel.Profiles,
            simulationModel,
            options.TopVolumes,
            options.Phase1WindowSize * 2);
        var latePhase1TailComposition = BuildLatePhase1TailCompositionSummary(
            simulationModel.Profiles,
            simulationModel,
            options.Phase1WindowSize * 2);
        var boundaryRegion = BuildBoundaryRegionSummary(
            simulationModel.Profiles,
            currentSimulation,
            plainOrderedProfiles,
            plainSizeBaseline,
            phaseBoundaryPosition,
            options.Phase1WindowSize);
        var defragWhatIf = BuildDefragWhatIfSummary(
            simulationModel,
            volumeItemCounts,
            peakPressure,
            currentSimulation,
            phase1TailSizeBaseline,
            plainSizeBaseline,
            effectiveSharedThreshold,
            effectiveVolumeCacheBudget,
            sharedBlockStoreBudget,
            options.RestoreVolumeCacheHint,
            options.TopVolumes,
            options.RestoreTime.ToUniversalTime());
        var heuristicComparisons = options.ComparePhase2Heuristics
            ? ComparePhase2Heuristics(
                simulationModel,
                effectiveSharedThreshold,
                effectiveVolumeCacheBudget,
                sharedBlockStoreBudget,
                options.RestoreVolumeCacheHint,
                options.RestoreTime.ToUniversalTime(),
                currentSimulation)
            : [];
        var heuristicComparisonStatus = options.ComparePhase2Heuristics
            ? null
            : $"skipped for this run; pass --compare-phase2-heuristics to enable (Phase 2 candidates: {phase2CandidateItems})";

        var assumptions = new[]
        {
            "Assumes a single FileProcessor and restore order identical to LocalRestoreDatabase.GetFilesAndSymlinksToRestore().",
            "Assumes no local-target or local-source block reuse shortcuts; blocks are treated as needing restore work.",
            "Models intrinsic volume pinning exactly from restore references, but ignores block-memory pressure and unlimited-mode disk-pressure evictions.",
            "Hard-cap cache simulation follows VolumeManager LRU insertion and explicit CacheEvict handling, but does not model free-space probes in unlimited mode.",
            "Phase 2 heuristic comparisons use greedy volume-frontier scoring; the reported peaks are exact for each resulting order, but the planner is heuristic rather than globally optimal.",
        };

        return new AnalysisReport(
            database,
            options.RestoreTime.ToUniversalTime(),
            options.Versions,
            options.Paths,
            options.SkipMetadata,
            options.RestoreVolumeCacheHint,
            effectiveVolumeCacheBudget,
            options.SharedBlockThreshold,
            effectiveSharedThreshold,
            simulationModel.Profiles.Count,
            simulationModel.Profiles.Count(x => x.Item.Kind == RestoreItemKind.FileData),
            simulationModel.Profiles.Count(x => x.Item.Kind == RestoreItemKind.FolderMetadata),
            currentSimulation.BlockRequests,
            currentSimulation.DistinctBlocks,
            currentSimulation.DistinctVolumes,
            currentSimulation.SharedBlockCandidates,
            currentSimulation.SharedBlocksStored,
            currentSimulation.SharedBlockStoreBytesWritten,
            currentSimulation.PredictedPeakActiveVolumes,
            currentSimulation.PredictedPeakCachedVolumes,
            currentSimulation.Peak,
            currentSimulation.PhaseSnapshots,
            currentSimulation.Phase1TopGrowthWindows,
            currentSimulation.Phase1WindowXray,
            currentSimulation.PeakActiveVolumesByFirstTouchKind,
            currentSimulation.PeakActiveVolumesByReleaseKind,
            fragmentation,
            peakPressure,
            orderingComparison,
            boundaryRelaxation,
            constrainedPrefixes,
            adaptiveInterleaving,
            phase1SizeThresholdSweep,
            runtimeCutoverRules,
            interactiveMap,
            latePhase1Tail,
            latePhase1TailComposition,
            boundaryRegion,
            defragWhatIf,
            phase2CandidateItems,
            heuristicComparisons,
            heuristicComparisonStatus,
            currentSimulation.PeakVolumesIntroducedBeforePhase2,
            currentSimulation.TopPrePhase2PinningVolumes,
            currentSimulation.TopPinningVolumes,
            assumptions);
    }

    private static async Task<List<RestoreItemProfile>> CollectRestoreItemProfilesAsync(LocalRestoreDatabase db, int effectiveSharedThreshold, bool skipMetadata, CancellationToken token)
    {
        var items = new List<RestoreItem>();

        await foreach (var (file, restorePhase) in db.GetFilesAndSymlinksToRestoreWithPhases(effectiveSharedThreshold, skipMetadata, token).ConfigureAwait(false))
            items.Add(new RestoreItem(
                file,
                RestoreItemKind.FileData,
                items.Count + 1,
                Enum.IsDefined(typeof(SqlRestorePhase), restorePhase)
                    ? (SqlRestorePhase)restorePhase
                    : SqlRestorePhase.Phase3));

        if (!skipMetadata)
        {
            await foreach (var folder in db.GetFolderMetadataToRestore(token).ConfigureAwait(false))
                items.Add(new RestoreItem(folder, RestoreItemKind.FolderMetadata, items.Count + 1, SqlRestorePhase.FolderMetadata));
        }

        var profiles = new List<RestoreItemProfile>(items.Count);
        foreach (var item in items)
        {
            var blocks = new List<SimulatedBlock>();

            if (item.Kind == RestoreItemKind.FileData)
            {
                await foreach (var block in db.GetBlocksFromFile(item.File.BlocksetID, token).ConfigureAwait(false))
                    blocks.Add(new SimulatedBlock(block.BlockID, block.BlockSize, block.VolumeID, false));
            }

            if (!skipMetadata)
            {
                await foreach (var block in db.GetMetadataBlocksFromFile(item.File.ID, token).ConfigureAwait(false))
                    blocks.Add(new SimulatedBlock(block.BlockID, block.BlockSize, block.VolumeID, true));
            }

            var volumeReferenceUsage = blocks
                .GroupBy(block => block.VolumeId)
                .ToDictionary(group => group.Key, group => (long)group.Count());
            var distinctVolumes = blocks
                .Where(block => block.BlockSize > 0)
                .Select(block => block.VolumeId)
                .Distinct()
                .ToArray();

            profiles.Add(new RestoreItemProfile(item, blocks, volumeReferenceUsage, distinctVolumes, item.SequenceIndex));
        }

        return profiles;
    }

    private static async Task<SimulationModel> BuildSimulationModelAsync(LocalRestoreDatabase db, IReadOnlyList<RestoreItemProfile> profiles, int effectiveSharedThreshold, CancellationToken token)
    {
        var blockReferenceCounts = new Dictionary<long, long>();
        var volumeReferenceCounts = new Dictionary<long, long>();
        var blockVolumes = new Dictionary<long, long>();

        foreach (var profile in profiles)
        {
            foreach (var block in profile.Blocks)
            {
                blockReferenceCounts[block.BlockId] = blockReferenceCounts.TryGetValue(block.BlockId, out var blockCount) ? blockCount + 1 : 1;
                volumeReferenceCounts[block.VolumeId] = volumeReferenceCounts.TryGetValue(block.VolumeId, out var volumeCount) ? volumeCount + 1 : 1;
                blockVolumes.TryAdd(block.BlockId, block.VolumeId);
            }
        }

        var sharedRefScores = new Dictionary<long, long>();
        if (effectiveSharedThreshold > 0)
        {
            foreach (var (blockId, referenceCount) in blockReferenceCounts)
            {
                if (referenceCount <= effectiveSharedThreshold)
                    continue;

                var volumeId = blockVolumes[blockId];
                sharedRefScores[volumeId] = sharedRefScores.TryGetValue(volumeId, out var sharedRefScore)
                    ? sharedRefScore + referenceCount
                    : referenceCount;
            }
        }

        var volumeNames = new Dictionary<long, string>();
        var volumeSizes = new Dictionary<long, long>();
        var volumeOperationIds = new Dictionary<long, long>();
        var volumeCreatedAtUtc = new Dictionary<long, DateTime?>();
        var volumeArchiveTimeUtc = new Dictionary<long, DateTime?>();
        var volumeStates = new Dictionary<long, string>();
        foreach (var volumeId in volumeReferenceCounts.Keys)
        {
            var volumeInfo = await db.GetVolumeExtendedInfo(volumeId, token).ToListAsync(token).ConfigureAwait(false);
            if (volumeInfo.Count == 0)
            {
                volumeNames[volumeId] = $"<missing:{volumeId}>";
                volumeSizes[volumeId] = 0;
                volumeOperationIds[volumeId] = -1;
                volumeCreatedAtUtc[volumeId] = null;
                volumeArchiveTimeUtc[volumeId] = null;
                volumeStates[volumeId] = string.Empty;
                continue;
            }

            var (name, size, _, operationId, operationTimestamp, archiveTime, state) = volumeInfo[0];
            volumeNames[volumeId] = name;
            volumeSizes[volumeId] = size;
            volumeOperationIds[volumeId] = operationId;
            volumeCreatedAtUtc[volumeId] = operationTimestamp > 0 ? DateTimeOffset.FromUnixTimeSeconds(operationTimestamp).UtcDateTime : null;
            volumeArchiveTimeUtc[volumeId] = archiveTime > 0 ? DateTimeOffset.FromUnixTimeSeconds(archiveTime).UtcDateTime : null;
            volumeStates[volumeId] = state;
        }

        return new SimulationModel(profiles, blockReferenceCounts, volumeReferenceCounts, sharedRefScores, volumeNames, volumeSizes, volumeOperationIds, volumeCreatedAtUtc, volumeArchiveTimeUtc, volumeStates);
    }

    private static IReadOnlyDictionary<long, int> BuildVolumeItemCounts(IReadOnlyList<RestoreItemProfile> profiles)
    {
        return profiles
            .SelectMany(profile => profile.DistinctVolumes.Select(volumeId => (volumeId, profile.Item.File.ID)))
            .GroupBy(x => x.volumeId)
            .ToDictionary(group => group.Key, group => group.Select(x => x.ID).Distinct().Count());
    }

    private static FragmentationSummary BuildFragmentationSummary(IReadOnlyList<RestoreItemProfile> profiles, IReadOnlyDictionary<long, int> volumeItemCounts, int topItems)
    {
        var overallItems = BuildFragmentationItems(profiles, volumeItemCounts, includeExclusiveVolumes: true);
        var sharedVolumeOnlyItems = BuildFragmentationItems(profiles, volumeItemCounts, includeExclusiveVolumes: false);

        return new FragmentationSummary(
            BuildFragmentationViewSummary("overall", overallItems, topItems),
            BuildFragmentationViewSummary("shared-volume-only", sharedVolumeOnlyItems, topItems));
    }

    private static List<FragmentationItemDetail> BuildFragmentationItems(
        IReadOnlyList<RestoreItemProfile> profiles,
        IReadOnlyDictionary<long, int> volumeItemCounts,
        bool includeExclusiveVolumes)
    {
        return profiles
            .Select(profile =>
            {
                var blocks = includeExclusiveVolumes
                    ? profile.Blocks
                    : profile.Blocks.Where(block => volumeItemCounts.GetValueOrDefault(block.VolumeId) > 1).ToList();
                var volumeReferenceUsage = blocks
                    .GroupBy(block => block.VolumeId)
                    .ToDictionary(group => group.Key, group => (long)group.Count());

                return BuildFragmentationItemDetail(profile, blocks, volumeReferenceUsage);
            })
            .Where(item => item is not null)
            .Cast<FragmentationItemDetail>()
            .ToList();
    }

    private static FragmentationItemDetail? BuildFragmentationItemDetail(
        RestoreItemProfile profile,
        IReadOnlyList<SimulatedBlock> blocks,
        IReadOnlyDictionary<long, long> volumeReferenceUsage)
    {
        int blockReferences = blocks.Count;
        if (blockReferences == 0)
            return null;

        int dataBlockReferences = blocks.Count(block => !block.IsMetadata);
        int metadataBlockReferences = blockReferences - dataBlockReferences;
        int distinctVolumes = volumeReferenceUsage.Count;
        long largestVolumeReferences = volumeReferenceUsage.Count == 0
            ? 0
            : volumeReferenceUsage.Max(entry => entry.Value);
        double largestVolumeShare = blockReferences > 0
            ? (double)largestVolumeReferences / blockReferences
            : 0;
        double effectiveVolumeCount = ComputeEffectiveVolumeCount(volumeReferenceUsage, blockReferences);
        double fragmentationScore = ComputeFragmentationScore(volumeReferenceUsage, blockReferences);

        return new FragmentationItemDetail(
            profile.Item.SequenceIndex,
            profile.Item.File.TargetPath,
            profile.Item.Kind.ToString(),
            profile.Item.SqlPhase.ToString(),
            blockReferences,
            dataBlockReferences,
            metadataBlockReferences,
            distinctVolumes,
            fragmentationScore,
            effectiveVolumeCount,
            largestVolumeShare,
            largestVolumeReferences);
    }

    private static FragmentationViewSummary BuildFragmentationViewSummary(string name, IReadOnlyList<FragmentationItemDetail> itemsWithBlocks, int topItems)
    {
        if (itemsWithBlocks.Count == 0)
        {
            return new FragmentationViewSummary(
                name,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                []);
        }

        int multiVolumeItems = itemsWithBlocks.Count(item => item.DistinctVolumes > 1);
        long totalBlockReferences = itemsWithBlocks.Sum(item => (long)item.BlockReferences);
        double blockWeightedScore = totalBlockReferences > 0
            ? itemsWithBlocks.Sum(item => item.FragmentationScore * item.BlockReferences) / totalBlockReferences
            : 0;
        double itemWeightedScore = itemsWithBlocks.Average(item => item.FragmentationScore);
        double averageDistinctVolumes = itemsWithBlocks.Average(item => item.DistinctVolumes);
        double averageEffectiveVolumeCount = itemsWithBlocks.Average(item => item.EffectiveVolumeCount);
        double averageFragmentationScore = itemsWithBlocks.Average(item => item.FragmentationScore);

        var topFragmentedItems = itemsWithBlocks
            .Where(item => item.DistinctVolumes > 1)
            .OrderByDescending(item => item.FragmentationScore)
            .ThenByDescending(item => item.DistinctVolumes)
            .ThenByDescending(item => item.EffectiveVolumeCount)
            .ThenByDescending(item => item.BlockReferences)
            .ThenBy(item => item.ItemIndex)
            .Take(topItems)
            .ToList();

        return new FragmentationViewSummary(
            name,
            blockWeightedScore,
            itemWeightedScore,
            itemsWithBlocks.Count,
            multiVolumeItems,
            100.0 * multiVolumeItems / itemsWithBlocks.Count,
            averageDistinctVolumes,
            Percentile(itemsWithBlocks.Select(item => (double)item.DistinctVolumes), 0.50),
            Percentile(itemsWithBlocks.Select(item => (double)item.DistinctVolumes), 0.95),
            averageFragmentationScore,
            PercentileDouble(itemsWithBlocks.Select(item => item.FragmentationScore), 0.50),
            PercentileDouble(itemsWithBlocks.Select(item => item.FragmentationScore), 0.95),
            averageEffectiveVolumeCount,
            PercentileDouble(itemsWithBlocks.Select(item => item.EffectiveVolumeCount), 0.95),
            topFragmentedItems);
    }

    private static PeakPressureSummary BuildPeakPressureSummary(
        IReadOnlyList<RestoreItemProfile> profiles,
        IReadOnlyDictionary<long, int> volumeItemCounts,
        PeakPoint? peak,
        int topItems)
    {
        if (peak == null)
            return new PeakPressureSummary([]);

        var peakActiveVolumeIds = peak.ActiveVolumeIds.ToHashSet();
        var topItemsList = profiles
            .Where(profile => profile.Item.Kind == RestoreItemKind.FileData && profile.Item.SequenceIndex <= peak.ItemIndex)
            .Select(profile =>
            {
                var touchedPeakVolumes = profile.DistinctVolumes.Where(peakActiveVolumeIds.Contains).ToArray();
                if (touchedPeakVolumes.Length == 0)
                    return null;

                var sharedTouchedPeakVolumes = touchedPeakVolumes.Where(volumeId => volumeItemCounts.GetValueOrDefault(volumeId) > 1).ToArray();
                long peakRemainingReferencesSum = touchedPeakVolumes.Sum(volumeId => peak.RemainingReferencesAtPeak.GetValueOrDefault(volumeId));
                long peakSharedRemainingReferencesSum = sharedTouchedPeakVolumes.Sum(volumeId => peak.RemainingReferencesAtPeak.GetValueOrDefault(volumeId));
                long sharedDataBytes = profile.Blocks
                    .Where(block => !block.IsMetadata && sharedTouchedPeakVolumes.Contains(block.VolumeId))
                    .Sum(block => block.BlockSize);
                double peakPressureScore = sharedTouchedPeakVolumes.Length == 0 || peakSharedRemainingReferencesSum <= 0
                    ? 0
                    : Math.Log10(sharedDataBytes + 1) * sharedTouchedPeakVolumes.Length * Math.Log10(peakSharedRemainingReferencesSum + 1);

                return new PeakPressureItem(
                    profile.Item.SequenceIndex,
                    profile.Item.File.TargetPath,
                    profile.Item.SqlPhase.ToString(),
                    sharedDataBytes,
                    sharedTouchedPeakVolumes.Length,
                    touchedPeakVolumes.Length,
                    sharedTouchedPeakVolumes.Length,
                    peakRemainingReferencesSum,
                    peakSharedRemainingReferencesSum,
                    peakPressureScore);
            })
            .Where(item => item is not null)
            .Cast<PeakPressureItem>()
            .OrderByDescending(item => item.PeakPressureScore)
            .ThenByDescending(item => item.PeakSharedRemainingReferencesSum)
            .ThenByDescending(item => item.PeakSharedVolumeOverlap)
            .ThenByDescending(item => item.SharedDataBytes)
            .ThenBy(item => item.ItemIndex)
            .Take(topItems)
            .ToList();

        return new PeakPressureSummary(topItemsList);
    }

    private static OrderingComparisonSummary BuildOrderingComparisonSummary(
        IReadOnlyList<RestoreItemProfile> currentOrderedProfiles,
        SimulationResult currentSimulation,
        SimulationResult phase1TailSizeBaseline,
        IReadOnlyList<RestoreItemProfile> plainOrderedProfiles,
        SimulationResult plainSimulation,
        int windowSize,
        int topWindows)
    {
        var currentTrace = currentSimulation.ActiveVolumesAfterEachItem;
        var plainTrace = plainSimulation.ActiveVolumesAfterEachItem;
        if (currentTrace == null || plainTrace == null || currentTrace.Count == 0 || plainTrace.Count == 0)
            return new OrderingComparisonSummary(
                windowSize,
                BuildOrderingBaselines(currentSimulation.PredictedPeakActiveVolumes, phase1TailSizeBaseline, plainSimulation),
                [],
                []);

        var currentVolumeTouchPositions = BuildVolumeTouchPositions(currentOrderedProfiles);
        var plainVolumeTouchPositions = BuildVolumeTouchPositions(plainOrderedProfiles);

        var windows = new List<OrderingComparisonWindow>();
        int count = Math.Min(currentTrace.Count, plainTrace.Count);
        for (int start = 0; start < count; start += windowSize)
        {
            int endExclusive = Math.Min(start + windowSize, count);
            var currentWindow = currentTrace.Skip(start).Take(endExclusive - start).ToArray();
            var plainWindow = plainTrace.Skip(start).Take(endExclusive - start).ToArray();
            int currentPeak = currentWindow.Max();
            int plainPeak = plainWindow.Max();
            int currentPeakOffset = Array.IndexOf(currentWindow, currentPeak);
            int plainPeakOffset = Array.IndexOf(plainWindow, plainPeak);
            int currentPeakPosition = start + currentPeakOffset + 1;
            int plainPeakPosition = start + plainPeakOffset + 1;
            var currentDynamics = BuildOrderingWindowDynamics(currentOrderedProfiles, currentTrace, currentVolumeTouchPositions, start, endExclusive);
            var plainDynamics = BuildOrderingWindowDynamics(plainOrderedProfiles, plainTrace, plainVolumeTouchPositions, start, endExclusive);

            windows.Add(new OrderingComparisonWindow(
                start + 1,
                endExclusive,
                currentPeak,
                currentPeakPosition,
                currentOrderedProfiles[currentPeakPosition - 1].Item.File.TargetPath,
                plainPeak,
                plainPeakPosition,
                plainOrderedProfiles[plainPeakPosition - 1].Item.File.TargetPath,
                plainPeak - currentPeak,
                currentDynamics,
                plainDynamics));
        }

        return new OrderingComparisonSummary(
            windowSize,
            BuildOrderingBaselines(currentSimulation.PredictedPeakActiveVolumes, phase1TailSizeBaseline, plainSimulation),
            windows
                .Where(window => window.PlainMinusCurrentDelta < 0)
                .OrderBy(window => window.PlainMinusCurrentDelta)
                .ThenBy(window => window.StartPosition)
                .Take(topWindows)
                .ToList(),
            windows
                .Where(window => window.PlainMinusCurrentDelta > 0)
                .OrderByDescending(window => window.PlainMinusCurrentDelta)
                .ThenBy(window => window.StartPosition)
                .Take(topWindows)
                .ToList());
    }

    private static IReadOnlyList<OrderingBaseline> BuildOrderingBaselines(int currentPeak, SimulationResult phase1TailSizeBaseline, SimulationResult plainSimulation)
        =>
        [
            new OrderingBaseline(
                "Phase1-preserved tail size-desc",
                phase1TailSizeBaseline.PredictedPeakActiveVolumes,
                phase1TailSizeBaseline.PredictedPeakActiveVolumes - currentPeak),
            new OrderingBaseline(
                "Full plain size-desc",
                plainSimulation.PredictedPeakActiveVolumes,
                plainSimulation.PredictedPeakActiveVolumes - currentPeak),
        ];

    private static (IReadOnlyDictionary<long, int> FirstTouchPositions, IReadOnlyDictionary<long, int> LastTouchPositions) BuildVolumeTouchPositions(IReadOnlyList<RestoreItemProfile> orderedProfiles)
    {
        var firstTouchPositions = new Dictionary<long, int>();
        var lastTouchPositions = new Dictionary<long, int>();

        for (int index = 0; index < orderedProfiles.Count; index++)
        {
            int position = index + 1;
            foreach (var volumeId in orderedProfiles[index].DistinctVolumes)
            {
                firstTouchPositions.TryAdd(volumeId, position);
                lastTouchPositions[volumeId] = position;
            }
        }

        return (firstTouchPositions, lastTouchPositions);
    }

    private static OrderingWindowDynamics BuildOrderingWindowDynamics(
        IReadOnlyList<RestoreItemProfile> orderedProfiles,
        IReadOnlyList<int> activeVolumesAfterEachItem,
        (IReadOnlyDictionary<long, int> FirstTouchPositions, IReadOnlyDictionary<long, int> LastTouchPositions) volumeTouchPositions,
        int startIndex,
        int endExclusive)
    {
        int startPosition = startIndex + 1;
        int endPosition = endExclusive;
        int startActive = startIndex == 0 ? 0 : activeVolumesAfterEachItem[startIndex - 1];
        int endActive = activeVolumesAfterEachItem[endExclusive - 1];
        int introducedVolumes = volumeTouchPositions.FirstTouchPositions.Values.Count(position => position >= startPosition && position <= endPosition);
        int drainedVolumes = volumeTouchPositions.LastTouchPositions.Values.Count(position => position >= startPosition && position <= endPosition);
        var windowProfiles = orderedProfiles.Skip(startIndex).Take(endExclusive - startIndex).ToArray();
        long totalDataBytes = windowProfiles.Sum(GetDataBlockBytes);
        long averageDataBytes = windowProfiles.Length == 0 ? 0 : totalDataBytes / windowProfiles.Length;

        return new OrderingWindowDynamics(
            startActive,
            endActive,
            endActive - startActive,
            introducedVolumes,
            drainedVolumes,
            totalDataBytes,
            averageDataBytes,
            windowProfiles.Count(profile => profile.IsPhase1File),
            windowProfiles.Count(profile => profile.IsPhase2Candidate),
            windowProfiles.Count(profile => profile.IsPhase3File),
            windowProfiles.Count(profile => profile.Item.Kind == RestoreItemKind.FolderMetadata));
    }

    private static BoundaryRelaxationSummary BuildBoundaryRelaxationSummary(
        SimulationModel simulationModel,
        int effectiveSharedThreshold,
        long effectiveVolumeCacheBudget,
        long sharedBlockStoreBudget,
        long restoreVolumeCacheHint,
        DateTime restoreTimeUtc,
        int currentPeak)
    {
        var phase1 = simulationModel.Profiles.Where(profile => profile.IsPhase1File).ToList();
        int phase1Count = phase1.Count;
        if (phase1Count == 0)
            return new BoundaryRelaxationSummary([], null);

        int[] preservePercentages = [100, 99, 95, 90, 75, 50, 25, 0];
        var scenarios = new List<BoundaryRelaxationScenario>(preservePercentages.Length);
        foreach (var preservePercentage in preservePercentages)
        {
            int preservedPhase1Items = (int)Math.Round(phase1Count * preservePercentage / 100.0, MidpointRounding.AwayFromZero);
            preservedPhase1Items = Math.Clamp(preservedPhase1Items, 0, phase1Count);
            var reordered = ReorderProfilesByBoundaryRelaxation(simulationModel.Profiles, preservedPhase1Items);
            var simulation = Simulate(
                reordered,
                simulationModel,
                effectiveSharedThreshold,
                effectiveVolumeCacheBudget,
                sharedBlockStoreBudget,
                restoreVolumeCacheHint,
                topVolumes: 0,
                phase1WindowSize: 1,
                phase1TopWindows: 0,
                restoreTimeUtc,
                captureDetails: false);

            scenarios.Add(new BoundaryRelaxationScenario(
                preservePercentage == 100
                    ? "Keep full Phase 1 prefix"
                    : preservePercentage == 0
                        ? "Drop boundary entirely"
                        : $"Keep first {preservePercentage}% of Phase 1",
                preservedPhase1Items,
                phase1Count == 0 ? 0 : (100.0 * preservedPhase1Items / phase1Count),
                simulation.PredictedPeakActiveVolumes,
                simulation.PredictedPeakActiveVolumes - currentPeak));
        }

        var dedupedScenarios = scenarios
            .GroupBy(scenario => scenario.PreservedPhase1Items)
            .Select(group => group.First())
            .OrderByDescending(scenario => scenario.PreservedPhase1Items)
            .ToList();

        return new BoundaryRelaxationSummary(
            dedupedScenarios,
            dedupedScenarios.FirstOrDefault(scenario => scenario.DeltaFromCurrent < 0));
    }

    private static ConstrainedPrefixSummary BuildConstrainedPrefixSummary(
        SimulationModel simulationModel,
        int effectiveSharedThreshold,
        long effectiveVolumeCacheBudget,
        long sharedBlockStoreBudget,
        long restoreVolumeCacheHint,
        DateTime restoreTimeUtc,
        int currentPeak)
    {
        var fileItems = simulationModel.Profiles.Where(profile => profile.Item.Kind == RestoreItemKind.FileData).ToList();
        if (fileItems.Count == 0)
            return new ConstrainedPrefixSummary([]);

        int[] prefixPercentages = [1, 2, 5, 10, 20, 30];
        var scenarios = new List<ConstrainedPrefixScenario>(prefixPercentages.Length);
        foreach (var prefixPercentage in prefixPercentages)
        {
            int prefixFileItems = Math.Clamp((int)Math.Round(fileItems.Count * prefixPercentage / 100.0, MidpointRounding.AwayFromZero), 1, fileItems.Count);
            var reordered = ReorderProfilesByLargestPrefixThenCurrentOrder(simulationModel.Profiles, prefixFileItems);
            var simulation = Simulate(
                reordered,
                simulationModel,
                effectiveSharedThreshold,
                effectiveVolumeCacheBudget,
                sharedBlockStoreBudget,
                restoreVolumeCacheHint,
                topVolumes: 0,
                phase1WindowSize: 1,
                phase1TopWindows: 0,
                restoreTimeUtc,
                captureDetails: false);

            scenarios.Add(new ConstrainedPrefixScenario(
                $"Largest {prefixPercentage}% prefix, then current order",
                prefixFileItems,
                100.0 * prefixFileItems / fileItems.Count,
                simulation.PredictedPeakActiveVolumes,
                simulation.PredictedPeakActiveVolumes - currentPeak));
        }

        return new ConstrainedPrefixSummary(scenarios);
    }

    private static AdaptiveInterleavingSummary BuildAdaptiveInterleavingSummary(
        SimulationModel simulationModel,
        int effectiveSharedThreshold,
        long effectiveVolumeCacheBudget,
        long sharedBlockStoreBudget,
        long restoreVolumeCacheHint,
        DateTime restoreTimeUtc,
        int currentPeak)
    {
        var phase1 = simulationModel.Profiles.Where(profile => profile.IsPhase1File).OrderBy(profile => profile.OriginalOrder).ToList();
        var phase2Sizes = simulationModel.Profiles
            .Where(profile => profile.IsPhase2Candidate)
            .Select(GetDataBlockBytes)
            .Where(size => size > 0)
            .Select(size => (double)size)
            .ToArray();

        if (phase1.Count == 0 || phase2Sizes.Length == 0)
            return new AdaptiveInterleavingSummary([], null);

        long phase2P95 = (long)Math.Round(PercentileDouble(phase2Sizes, 0.95), MidpointRounding.AwayFromZero);
        long phase2P99 = (long)Math.Round(PercentileDouble(phase2Sizes, 0.99), MidpointRounding.AwayFromZero);
        int prefixByP95 = DeterminePhase1PrefixByMinBytes(phase1, phase2P95);
        int prefixByP99 = DeterminePhase1PrefixByMinBytes(phase1, phase2P99);
        int prefixByRollingP95 = DeterminePhase1PrefixByRollingAverage(phase1, lookaheadCount: 1000, minimumAverageBytes: phase2P95);

        var scenarioInputs = new[]
        {
            (Name: "Phase1 prefix while item >= Phase2 p95", Trigger: $"threshold={FormatBytes(phase2P95)}", Preserved: prefixByP95),
            (Name: "Phase1 prefix while item >= Phase2 p99", Trigger: $"threshold={FormatBytes(phase2P99)}", Preserved: prefixByP99),
            (Name: "Phase1 prefix while next 1000 avg >= Phase2 p95", Trigger: $"rollingAvgThreshold={FormatBytes(phase2P95)}", Preserved: prefixByRollingP95),
        };

        var scenarios = scenarioInputs
            .DistinctBy(input => (input.Name, input.Preserved))
            .Select(input =>
            {
                var reordered = ReorderProfilesByBoundaryRelaxation(simulationModel.Profiles, input.Preserved);
                var simulation = Simulate(
                    reordered,
                    simulationModel,
                    effectiveSharedThreshold,
                    effectiveVolumeCacheBudget,
                    sharedBlockStoreBudget,
                    restoreVolumeCacheHint,
                    topVolumes: 0,
                    phase1WindowSize: 1,
                    phase1TopWindows: 0,
                    restoreTimeUtc,
                    captureDetails: false);

                return new AdaptiveInterleavingScenario(
                    input.Name,
                    input.Trigger,
                    input.Preserved,
                    100.0 * input.Preserved / phase1.Count,
                    simulation.PredictedPeakActiveVolumes,
                    simulation.PredictedPeakActiveVolumes - currentPeak);
            })
            .OrderBy(scenario => scenario.PredictedPeakActiveVolumes)
            .ThenBy(scenario => scenario.PreservedPhase1Items)
            .ThenBy(scenario => scenario.Name)
            .ToList();

        return new AdaptiveInterleavingSummary(
            scenarios,
            scenarios.FirstOrDefault(scenario => scenario.DeltaFromCurrent < 0));
    }

    private static Phase1SizeThresholdSweepSummary BuildPhase1SizeThresholdSweepSummary(
        SimulationModel simulationModel,
        int effectiveSharedThreshold,
        long effectiveVolumeCacheBudget,
        long sharedBlockStoreBudget,
        long restoreVolumeCacheHint,
        DateTime restoreTimeUtc,
        int currentPeak)
    {
        var phase1 = simulationModel.Profiles.Where(profile => profile.IsPhase1File).OrderBy(profile => profile.OriginalOrder).ToList();
        var phase2Sizes = simulationModel.Profiles
            .Where(profile => profile.IsPhase2Candidate)
            .Select(GetDataBlockBytes)
            .Where(size => size > 0)
            .Select(size => (double)size)
            .ToArray();
        if (phase1.Count == 0)
            return new Phase1SizeThresholdSweepSummary([], null, null);

        long phase2P95 = phase2Sizes.Length == 0 ? 0 : (long)Math.Round(PercentileDouble(phase2Sizes, 0.95), MidpointRounding.AwayFromZero);
        long phase2P99 = phase2Sizes.Length == 0 ? 0 : (long)Math.Round(PercentileDouble(phase2Sizes, 0.99), MidpointRounding.AwayFromZero);
        var thresholdInputs = new (string Name, long ThresholdBytes)[]
        {
            ("cutover<4 KiB", 4L * 1024),
            ("cutover<16 KiB", 16L * 1024),
            ("cutover<64 KiB", 64L * 1024),
            ("cutover<256 KiB", 256L * 1024),
            ("cutover<1 MiB", 1024L * 1024),
            ("cutover<4 MiB", 4L * 1024 * 1024),
            ("cutover<Phase2 p95", phase2P95),
            ("cutover<Phase2 p99", phase2P99),
        };

        var scenarios = thresholdInputs
            .Where(input => input.ThresholdBytes > 0)
            .DistinctBy(input => input.ThresholdBytes)
            .Select(input =>
            {
                int preservedPhase1Items = DeterminePhase1PrefixByMinBytes(phase1, input.ThresholdBytes);
                var reordered = ReorderProfilesByBoundaryRelaxation(simulationModel.Profiles, preservedPhase1Items);
                var simulation = Simulate(
                    reordered,
                    simulationModel,
                    effectiveSharedThreshold,
                    effectiveVolumeCacheBudget,
                    sharedBlockStoreBudget,
                    restoreVolumeCacheHint,
                    topVolumes: 0,
                    phase1WindowSize: 1,
                    phase1TopWindows: 0,
                    restoreTimeUtc,
                    captureDetails: false);

                return new Phase1SizeThresholdScenario(
                    input.Name,
                    input.ThresholdBytes,
                    preservedPhase1Items,
                    100.0 * preservedPhase1Items / phase1.Count,
                    simulation.PredictedPeakActiveVolumes,
                    simulation.PredictedPeakActiveVolumes - currentPeak);
            })
            .OrderBy(scenario => scenario.ThresholdBytes)
            .ToList();

        return new Phase1SizeThresholdSweepSummary(
            scenarios,
            scenarios.FirstOrDefault(scenario => scenario.DeltaFromCurrent < 0),
            scenarios.OrderBy(scenario => scenario.PredictedPeakActiveVolumes).ThenBy(scenario => scenario.ThresholdBytes).FirstOrDefault());
    }

    private static RuntimeCutoverRuleSummary BuildRuntimeCutoverRuleSummary(
        SimulationModel simulationModel,
        int effectiveSharedThreshold,
        long effectiveVolumeCacheBudget,
        long sharedBlockStoreBudget,
        long restoreVolumeCacheHint,
        DateTime restoreTimeUtc,
        int currentPeak)
    {
        var phase1 = simulationModel.Profiles.Where(profile => profile.IsPhase1File).OrderBy(profile => profile.OriginalOrder).ToList();
        var phase2Sizes = simulationModel.Profiles
            .Where(profile => profile.IsPhase2Candidate)
            .Select(GetDataBlockBytes)
            .Where(size => size > 0)
            .Select(size => (double)size)
            .ToArray();
        if (phase1.Count == 0 || phase2Sizes.Length == 0)
            return new RuntimeCutoverRuleSummary([], null, null);

        long phase2P95 = (long)Math.Round(PercentileDouble(phase2Sizes, 0.95), MidpointRounding.AwayFromZero);
        long phase2P99 = (long)Math.Round(PercentileDouble(phase2Sizes, 0.99), MidpointRounding.AwayFromZero);
        var scenarioInputs = new[]
        {
            (Name: "Planner cut before first item < Phase2 p95", ThresholdName: "Phase2 p95", ThresholdBytes: phase2P95, ConsecutiveCount: 1, CutAfterTriggerRun: false),
            (Name: "Planner cut before first 32-item run < Phase2 p95", ThresholdName: "Phase2 p95", ThresholdBytes: phase2P95, ConsecutiveCount: 32, CutAfterTriggerRun: false),
            (Name: "Online cut after first item < Phase2 p95", ThresholdName: "Phase2 p95", ThresholdBytes: phase2P95, ConsecutiveCount: 1, CutAfterTriggerRun: true),
            (Name: "Online cut after first 32-item run < Phase2 p95", ThresholdName: "Phase2 p95", ThresholdBytes: phase2P95, ConsecutiveCount: 32, CutAfterTriggerRun: true),
            (Name: "Planner cut before first item < Phase2 p99", ThresholdName: "Phase2 p99", ThresholdBytes: phase2P99, ConsecutiveCount: 1, CutAfterTriggerRun: false),
            (Name: "Planner cut before first 32-item run < Phase2 p99", ThresholdName: "Phase2 p99", ThresholdBytes: phase2P99, ConsecutiveCount: 32, CutAfterTriggerRun: false),
            (Name: "Online cut after first item < Phase2 p99", ThresholdName: "Phase2 p99", ThresholdBytes: phase2P99, ConsecutiveCount: 1, CutAfterTriggerRun: true),
            (Name: "Online cut after first 32-item run < Phase2 p99", ThresholdName: "Phase2 p99", ThresholdBytes: phase2P99, ConsecutiveCount: 32, CutAfterTriggerRun: true),
        };

        var scenarios = scenarioInputs
            .Where(input => input.ThresholdBytes > 0)
            .DistinctBy(input => (input.ThresholdBytes, input.ConsecutiveCount, input.CutAfterTriggerRun))
            .Select(input =>
            {
                int preservedPhase1Items = DeterminePhase1PrefixByConsecutiveThreshold(
                    phase1,
                    input.ThresholdBytes,
                    input.ConsecutiveCount,
                    input.CutAfterTriggerRun);
                string trigger = BuildRuntimeCutoverTriggerDescription(
                    phase1,
                    input.ThresholdName,
                    input.ThresholdBytes,
                    input.ConsecutiveCount,
                    input.CutAfterTriggerRun,
                    preservedPhase1Items);
                var reordered = ReorderProfilesByBoundaryRelaxation(simulationModel.Profiles, preservedPhase1Items);
                var simulation = Simulate(
                    reordered,
                    simulationModel,
                    effectiveSharedThreshold,
                    effectiveVolumeCacheBudget,
                    sharedBlockStoreBudget,
                    restoreVolumeCacheHint,
                    topVolumes: 0,
                    phase1WindowSize: 1,
                    phase1TopWindows: 0,
                    restoreTimeUtc,
                    captureDetails: false);

                return new RuntimeCutoverRuleScenario(
                    input.Name,
                    trigger,
                    preservedPhase1Items,
                    100.0 * preservedPhase1Items / phase1.Count,
                    simulation.PredictedPeakActiveVolumes,
                    simulation.PredictedPeakActiveVolumes - currentPeak);
            })
            .OrderBy(scenario => scenario.PredictedPeakActiveVolumes)
            .ThenBy(scenario => scenario.PreservedPhase1Items)
            .ThenBy(scenario => scenario.Name)
            .ToList();

        return new RuntimeCutoverRuleSummary(
            scenarios,
            scenarios.FirstOrDefault(scenario => scenario.DeltaFromCurrent < 0),
            scenarios.FirstOrDefault());
    }

    private static InteractiveRestoreMap? BuildInteractiveRestoreMap(
        string database,
        AnalysisOptions options,
        SimulationModel simulationModel,
        SimulationResult currentSimulation,
        IReadOnlyList<RestoreItemProfile> phase1TailOrderedProfiles,
        SimulationResult phase1TailSimulation,
        IReadOnlyList<RestoreItemProfile> plainOrderedProfiles,
        SimulationResult plainSimulation,
        BoundaryRelaxationSummary boundaryRelaxation,
        ConstrainedPrefixSummary constrainedPrefixes,
        AdaptiveInterleavingSummary adaptiveInterleaving,
        Phase1SizeThresholdSweepSummary phase1SizeThresholdSweep,
        RuntimeCutoverRuleSummary runtimeCutoverRules,
        int effectiveSharedThreshold,
        long effectiveVolumeCacheBudget,
        long sharedBlockStoreBudget,
        int currentPeak)
    {
        if (string.IsNullOrWhiteSpace(options.HtmlOutputPath))
            return null;

        var catalog = BuildInteractiveCatalog(simulationModel, effectiveSharedThreshold);
        var volumes = BuildInteractiveVolumeCatalog(simulationModel, effectiveSharedThreshold);
        var strategies = new List<InteractiveStrategyTrace>
        {
            BuildInteractiveStrategyTrace("current-sql", "Current SQL order", currentSimulation, 0),
            BuildInteractiveStrategyTrace(
                "phase1-tail-size",
                "Phase1-preserved tail size-desc",
                phase1TailSimulation,
                phase1TailSimulation.PredictedPeakActiveVolumes - currentPeak),
            BuildInteractiveStrategyTrace(
                "plain-size",
                "Full plain size-desc",
                plainSimulation,
                plainSimulation.PredictedPeakActiveVolumes - currentPeak),
        };

        int phase1Count = simulationModel.Profiles.Count(profile => profile.IsPhase1File);
        TryAddInteractiveStrategy(
            strategies,
            simulationModel,
            effectiveSharedThreshold,
            effectiveVolumeCacheBudget,
            sharedBlockStoreBudget,
            options,
            currentPeak,
            "boundary-50",
            "Boundary relaxation: keep first 50% of Phase 1",
            ReorderProfilesByBoundaryRelaxation(simulationModel.Profiles, phase1Count / 2));
        TryAddInteractiveStrategy(
            strategies,
            simulationModel,
            effectiveSharedThreshold,
            effectiveVolumeCacheBudget,
            sharedBlockStoreBudget,
            options,
            currentPeak,
            "boundary-25",
            "Boundary relaxation: keep first 25% of Phase 1",
            ReorderProfilesByBoundaryRelaxation(simulationModel.Profiles, phase1Count / 4));

        if (adaptiveInterleaving.BestScenario is not null)
            TryAddInteractiveStrategy(
                strategies,
                simulationModel,
                effectiveSharedThreshold,
                effectiveVolumeCacheBudget,
                sharedBlockStoreBudget,
                options,
                currentPeak,
                "adaptive-best",
                $"Best adaptive: {adaptiveInterleaving.BestScenario.Name}",
                ReorderProfilesByBoundaryRelaxation(simulationModel.Profiles, adaptiveInterleaving.BestScenario.PreservedPhase1Items));

        if (phase1SizeThresholdSweep.BestScenario is not null)
            TryAddInteractiveStrategy(
                strategies,
                simulationModel,
                effectiveSharedThreshold,
                effectiveVolumeCacheBudget,
                sharedBlockStoreBudget,
                options,
                currentPeak,
                "threshold-best",
                $"Best threshold: {phase1SizeThresholdSweep.BestScenario.Name}",
                ReorderProfilesByBoundaryRelaxation(simulationModel.Profiles, phase1SizeThresholdSweep.BestScenario.PreservedPhase1Items));

        var bestConstrained = constrainedPrefixes.Scenarios
            .OrderBy(scenario => scenario.PredictedPeakActiveVolumes)
            .ThenBy(scenario => scenario.PrefixFileItems)
            .FirstOrDefault();
        if (bestConstrained is not null)
            TryAddInteractiveStrategy(
                strategies,
                simulationModel,
                effectiveSharedThreshold,
                effectiveVolumeCacheBudget,
                sharedBlockStoreBudget,
                options,
                currentPeak,
                "constrained-best",
                $"Best constrained prefix: {bestConstrained.Name}",
                ReorderProfilesByLargestPrefixThenCurrentOrder(simulationModel.Profiles, bestConstrained.PrefixFileItems));

        if (runtimeCutoverRules.BestScenario is not null)
        {
            var bestScenario = runtimeCutoverRules.BestScenario;
            var exactOrderedProfiles = ReorderProfilesByBoundaryRelaxation(simulationModel.Profiles, bestScenario.PreservedPhase1Items);
            var exactSimulation = Simulate(
                exactOrderedProfiles,
                simulationModel,
                effectiveSharedThreshold,
                effectiveVolumeCacheBudget,
                sharedBlockStoreBudget,
                options.RestoreVolumeCacheHint,
                topVolumes: 0,
                phase1WindowSize: 1,
                phase1TopWindows: 0,
                options.RestoreTime.ToUniversalTime(),
                captureDetails: false,
                captureItemTrace: false,
                captureTimeline: true);

            strategies.Add(BuildInteractiveStrategyTrace(
                "exact-best",
                $"Best exact rule: {bestScenario.Name}",
                exactSimulation,
                bestScenario.DeltaFromCurrent));
        }

        return new InteractiveRestoreMap(
            database,
            options.RestoreTime.ToUniversalTime(),
            catalog.Count,
            simulationModel.VolumeReferenceCounts.Count,
            effectiveSharedThreshold,
            currentPeak,
            catalog,
            volumes,
            strategies);
    }

    private static void TryAddInteractiveStrategy(
        ICollection<InteractiveStrategyTrace> strategies,
        SimulationModel simulationModel,
        int effectiveSharedThreshold,
        long effectiveVolumeCacheBudget,
        long sharedBlockStoreBudget,
        AnalysisOptions options,
        int currentPeak,
        string key,
        string name,
        IReadOnlyList<RestoreItemProfile> orderedProfiles)
    {
        if (strategies.Any(strategy => strategy.Key == key))
            return;

        var simulation = Simulate(
            orderedProfiles,
            simulationModel,
            effectiveSharedThreshold,
            effectiveVolumeCacheBudget,
            sharedBlockStoreBudget,
            options.RestoreVolumeCacheHint,
            topVolumes: 0,
            phase1WindowSize: 1,
            phase1TopWindows: 0,
            options.RestoreTime.ToUniversalTime(),
            captureDetails: false,
            captureItemTrace: false,
            captureTimeline: true);

        strategies.Add(BuildInteractiveStrategyTrace(
            key,
            name,
            simulation,
            simulation.PredictedPeakActiveVolumes - currentPeak));
    }

    private static IReadOnlyList<InteractiveCatalogItem> BuildInteractiveCatalog(SimulationModel simulationModel, int effectiveSharedThreshold)
        => simulationModel.Profiles
            .OrderBy(profile => profile.Item.SequenceIndex)
            .Select(profile =>
            {
                int blockReferences = 0;
                int dataBlockReferences = 0;
                int metadataBlockReferences = 0;
                long dataBytes = 0;
                long metadataBytes = 0;
                var sharedVolumes = new HashSet<long>();
                int sharedBlockReferences = 0;
                int sharedDataBlockReferences = 0;
                int sharedMetadataBlockReferences = 0;
                long sharedDataBytes = 0;
                long sharedMetadataBytes = 0;

                foreach (var block in profile.Blocks)
                {
                    blockReferences++;
                    if (block.IsMetadata)
                    {
                        metadataBytes += block.BlockSize;
                        metadataBlockReferences++;
                    }
                    else
                    {
                        dataBytes += block.BlockSize;
                        dataBlockReferences++;
                    }

                    bool isShared = effectiveSharedThreshold > 0
                        && simulationModel.BlockReferenceCounts.TryGetValue(block.BlockId, out var refs)
                        && refs > effectiveSharedThreshold;
                    if (!isShared)
                        continue;

                    sharedBlockReferences++;
                    sharedVolumes.Add(block.VolumeId);
                    if (block.IsMetadata)
                    {
                        sharedMetadataBlockReferences++;
                        sharedMetadataBytes += block.BlockSize;
                    }
                    else
                    {
                        sharedDataBlockReferences++;
                        sharedDataBytes += block.BlockSize;
                    }
                }

                return new InteractiveCatalogItem(
                    profile.Item.SequenceIndex,
                    profile.Item.File.TargetPath,
                    profile.Item.SqlPhase.ToString(),
                    profile.Item.Kind.ToString(),
                    blockReferences,
                    dataBlockReferences,
                    metadataBlockReferences,
                    dataBytes,
                    metadataBytes,
                    profile.DistinctVolumes.Count,
                    sharedVolumes.Count,
                    sharedBlockReferences,
                    sharedDataBlockReferences,
                    sharedMetadataBlockReferences,
                    sharedDataBytes,
                    sharedMetadataBytes);
            })
            .ToArray();

    private static IReadOnlyList<InteractiveVolumeItem> BuildInteractiveVolumeCatalog(SimulationModel simulationModel, int effectiveSharedThreshold)
    {
        var usagesByVolume = new Dictionary<long, List<InteractiveVolumeUsage>>();

        foreach (var profile in simulationModel.Profiles.OrderBy(profile => profile.Item.SequenceIndex))
        {
            foreach (var volumeGroup in profile.Blocks.GroupBy(block => block.VolumeId))
            {
                int blockReferences = 0;
                int dataBlockReferences = 0;
                int metadataBlockReferences = 0;
                long dataBytes = 0;
                long metadataBytes = 0;
                int sharedBlockReferences = 0;
                int sharedDataBlockReferences = 0;
                int sharedMetadataBlockReferences = 0;
                long sharedDataBytes = 0;
                long sharedMetadataBytes = 0;

                foreach (var block in volumeGroup)
                {
                    blockReferences++;
                    bool isShared = effectiveSharedThreshold > 0
                        && simulationModel.BlockReferenceCounts.TryGetValue(block.BlockId, out var refs)
                        && refs > effectiveSharedThreshold;

                    if (block.IsMetadata)
                    {
                        metadataBlockReferences++;
                        metadataBytes += block.BlockSize;
                        if (isShared)
                        {
                            sharedBlockReferences++;
                            sharedMetadataBlockReferences++;
                            sharedMetadataBytes += block.BlockSize;
                        }
                    }
                    else
                    {
                        dataBlockReferences++;
                        dataBytes += block.BlockSize;
                        if (isShared)
                        {
                            sharedBlockReferences++;
                            sharedDataBlockReferences++;
                            sharedDataBytes += block.BlockSize;
                        }
                    }
                }

                if (!usagesByVolume.TryGetValue(volumeGroup.Key, out var volumeUsages))
                {
                    volumeUsages = [];
                    usagesByVolume[volumeGroup.Key] = volumeUsages;
                }

                volumeUsages.Add(new InteractiveVolumeUsage(
                    profile.Item.SequenceIndex,
                    profile.Item.File.TargetPath,
                    profile.Item.SqlPhase.ToString(),
                    profile.Item.Kind.ToString(),
                    blockReferences,
                    dataBlockReferences,
                    metadataBlockReferences,
                    dataBytes,
                    metadataBytes,
                    sharedBlockReferences,
                    sharedDataBlockReferences,
                    sharedMetadataBlockReferences,
                    sharedDataBytes,
                    sharedMetadataBytes));
            }
        }

        return simulationModel.VolumeReferenceCounts
            .OrderBy(entry => entry.Key)
            .Select(entry => new InteractiveVolumeItem(
                entry.Key,
                simulationModel.VolumeNames.GetValueOrDefault(entry.Key, string.Empty),
                simulationModel.VolumeSizes.GetValueOrDefault(entry.Key),
                entry.Value,
                simulationModel.SharedRefScores.GetValueOrDefault(entry.Key),
                simulationModel.VolumeOperationIds.GetValueOrDefault(entry.Key, -1),
                simulationModel.VolumeStates.GetValueOrDefault(entry.Key, string.Empty),
                usagesByVolume.GetValueOrDefault(entry.Key, []).OrderBy(usage => usage.ItemIndex).ToArray()))
            .ToArray();
    }

    private static InteractiveStrategyTrace BuildInteractiveStrategyTrace(
        string key,
        string name,
        SimulationResult simulation,
        int deltaFromCurrent)
    {
        if (simulation.OrderedItemIndexes is null
            || simulation.ActiveVolumesAfterEachItem is null
            || simulation.CachedVolumesAfterEachItem is null
            || simulation.IntroducedVolumesByItem is null
            || simulation.DrainedVolumesByItem is null
            || simulation.RetiredVolumesTotalAfterEachItem is null
            || simulation.IntroducedVolumeIdsByItem is null
            || simulation.DrainedVolumeIdsByItem is null
            || simulation.VolumeIntroducedAtItem is null
            || simulation.VolumeDrainedAtItem is null)
            throw new InvalidOperationException($"Timeline capture was not enabled for strategy '{name}'.");

        return new InteractiveStrategyTrace(
            key,
            name,
            simulation.PredictedPeakActiveVolumes,
            simulation.PredictedPeakCachedVolumes,
            deltaFromCurrent,
            simulation.Peak?.ItemIndex ?? 0,
            simulation.OrderedItemIndexes,
            simulation.ActiveVolumesAfterEachItem,
            simulation.CachedVolumesAfterEachItem,
            simulation.IntroducedVolumesByItem,
            simulation.DrainedVolumesByItem,
                simulation.RetiredVolumesTotalAfterEachItem,
                simulation.IntroducedVolumeIdsByItem,
                simulation.DrainedVolumeIdsByItem,
                simulation.VolumeIntroducedAtItem,
                simulation.VolumeDrainedAtItem);
    }

    private static LatePhase1TailSummary BuildLatePhase1TailSummary(
        IReadOnlyList<RestoreItemProfile> profiles,
        SimulationModel simulationModel,
        int topItems,
        int tailSampleSize)
    {
        var phase1 = profiles.Where(profile => profile.IsPhase1File).OrderBy(profile => profile.OriginalOrder).ToList();
        if (phase1.Count == 0)
            return new LatePhase1TailSummary(0, []);

        int actualTailSampleSize = Math.Min(Math.Max(topItems, tailSampleSize), phase1.Count);
        var phase1Tail = phase1.Skip(phase1.Count - actualTailSampleSize).ToList();
        var transitions = BuildItemTransitions(profiles, simulationModel);
        var phase1Positions = phase1
            .Select((profile, index) => (profile.Item.SequenceIndex, Position: index))
            .ToDictionary(x => x.SequenceIndex, x => x.Position);

        var candidates = phase1Tail
            .Select(profile =>
            {
                var transition = transitions[profile.Item.SequenceIndex];
                int phase1Position = phase1Positions[profile.Item.SequenceIndex];
                int tailOffset = (phase1.Count - 1) - phase1Position;

                return new LatePhase1TailCandidate(
                    profile.Item.SequenceIndex,
                    profile.Item.File.TargetPath,
                    tailOffset,
                    GetDataBlockBytes(profile),
                    profile.DistinctVolumes.Count,
                    transition.IntroducedVolumes,
                    transition.DrainedVolumes,
                    transition.NetActiveVolumeDelta,
                    transition.ActiveVolumesBefore,
                    transition.ActiveVolumesAfter);
            })
            .OrderBy(candidate => candidate.DataBytes)
            .ThenBy(candidate => candidate.DrainedVolumes)
            .ThenBy(candidate => candidate.IntroducedVolumes)
            .ThenBy(candidate => candidate.DistinctVolumes)
            .ThenBy(candidate => candidate.TailOffset)
            .Take(topItems)
            .ToList();

        return new LatePhase1TailSummary(actualTailSampleSize, candidates);
    }

    private static LatePhase1TailCompositionSummary BuildLatePhase1TailCompositionSummary(
        IReadOnlyList<RestoreItemProfile> profiles,
        SimulationModel simulationModel,
        int tailSampleSize)
    {
        var phase1 = profiles.Where(profile => profile.IsPhase1File).OrderBy(profile => profile.OriginalOrder).ToList();
        if (phase1.Count == 0)
            return new LatePhase1TailCompositionSummary(0, 0, 0, 0, 0, 0, 0, 0, 0);

        int actualTailSampleSize = Math.Min(tailSampleSize, phase1.Count);
        var phase1Tail = phase1.Skip(phase1.Count - actualTailSampleSize).ToList();
        var transitions = BuildItemTransitions(profiles, simulationModel);

        int zeroDataItems = 0;
        int atMost4KiBItems = 0;
        int atMost64KiBItems = 0;
        int atMost256KiBItems = 0;
        int metadataDominantItems = 0;
        int introducesNoVolumesItems = 0;
        int drainsNoVolumesItems = 0;
        int noVolumeEffectItems = 0;

        foreach (var profile in phase1Tail)
        {
            long dataBytes = GetDataBlockBytes(profile);
            long metadataBytes = profile.Blocks.Where(block => block.IsMetadata).Sum(block => block.BlockSize);
            var transition = transitions[profile.Item.SequenceIndex];

            if (dataBytes == 0)
                zeroDataItems++;
            if (dataBytes <= 4L * 1024)
                atMost4KiBItems++;
            if (dataBytes <= 64L * 1024)
                atMost64KiBItems++;
            if (dataBytes <= 256L * 1024)
                atMost256KiBItems++;
            if (metadataBytes > 0 && metadataBytes >= dataBytes)
                metadataDominantItems++;
            if (transition.IntroducedVolumes == 0)
                introducesNoVolumesItems++;
            if (transition.DrainedVolumes == 0)
                drainsNoVolumesItems++;
            if (transition.IntroducedVolumes == 0 && transition.DrainedVolumes == 0)
                noVolumeEffectItems++;
        }

        return new LatePhase1TailCompositionSummary(
            actualTailSampleSize,
            zeroDataItems,
            atMost4KiBItems,
            atMost64KiBItems,
            atMost256KiBItems,
            metadataDominantItems,
            introducesNoVolumesItems,
            drainsNoVolumesItems,
            noVolumeEffectItems);
    }

    private static IReadOnlyDictionary<int, ItemTransition> BuildItemTransitions(
        IReadOnlyList<RestoreItemProfile> orderedProfiles,
        SimulationModel simulationModel)
    {
        var remainingVolumeReferences = new Dictionary<long, long>(simulationModel.VolumeReferenceCounts);
        var activeVolumes = new HashSet<long>();
        var transitions = new Dictionary<int, ItemTransition>(orderedProfiles.Count);

        foreach (var profile in orderedProfiles)
        {
            int activeBefore = activeVolumes.Count;
            int introducedVolumes = profile.DistinctVolumes.Count(volumeId => !activeVolumes.Contains(volumeId));

            foreach (var volumeId in profile.DistinctVolumes)
                activeVolumes.Add(volumeId);

            int drainedVolumes = 0;
            foreach (var (volumeId, usage) in profile.VolumeReferenceUsage)
            {
                if (!remainingVolumeReferences.TryGetValue(volumeId, out var remaining))
                    continue;

                var updated = Math.Max(0, remaining - usage);
                if (updated == 0)
                {
                    drainedVolumes++;
                    remainingVolumeReferences.Remove(volumeId);
                    activeVolumes.Remove(volumeId);
                }
                else
                {
                    remainingVolumeReferences[volumeId] = updated;
                }
            }

            int activeAfter = activeVolumes.Count;
            transitions[profile.Item.SequenceIndex] = new ItemTransition(
                profile.Item.SequenceIndex,
                activeBefore,
                activeAfter,
                introducedVolumes,
                drainedVolumes,
                activeAfter - activeBefore);
        }

        return transitions;
    }

    private static int DeterminePhase1PrefixByMinBytes(IReadOnlyList<RestoreItemProfile> phase1Profiles, long minimumDataBytes)
    {
        int prefixCount = 0;
        foreach (var profile in phase1Profiles)
        {
            if (GetDataBlockBytes(profile) < minimumDataBytes)
                break;

            prefixCount++;
        }

        return prefixCount;
    }

    private static int DeterminePhase1PrefixByConsecutiveThreshold(
        IReadOnlyList<RestoreItemProfile> phase1Profiles,
        long thresholdBytes,
        int consecutiveCount,
        bool cutAfterTriggerRun)
    {
        if (phase1Profiles.Count == 0)
            return 0;

        consecutiveCount = Math.Max(1, consecutiveCount);
        int streak = 0;
        int streakStart = -1;

        for (int index = 0; index < phase1Profiles.Count; index++)
        {
            if (GetDataBlockBytes(phase1Profiles[index]) < thresholdBytes)
            {
                if (streak == 0)
                    streakStart = index;

                streak++;
                if (streak >= consecutiveCount)
                    return cutAfterTriggerRun ? index + 1 : streakStart;
            }
            else
            {
                streak = 0;
                streakStart = -1;
            }
        }

        return phase1Profiles.Count;
    }

    private static string BuildRuntimeCutoverTriggerDescription(
        IReadOnlyList<RestoreItemProfile> phase1Profiles,
        string thresholdName,
        long thresholdBytes,
        int consecutiveCount,
        bool cutAfterTriggerRun,
        int preservedPhase1Items)
    {
        string timing = cutAfterTriggerRun ? "onlineAfterRun" : "plannerBeforeRun";
        if (preservedPhase1Items >= phase1Profiles.Count)
            return $"{timing}, threshold={thresholdName} ({FormatBytes(thresholdBytes)}), streak={consecutiveCount}, trigger=never";

        int cutoverItemIndex = phase1Profiles[preservedPhase1Items].Item.SequenceIndex;
        return $"{timing}, threshold={thresholdName} ({FormatBytes(thresholdBytes)}), streak={consecutiveCount}, cutoverAtItem={cutoverItemIndex}";
    }

    private static int DeterminePhase1PrefixByRollingAverage(IReadOnlyList<RestoreItemProfile> phase1Profiles, int lookaheadCount, long minimumAverageBytes)
    {
        if (phase1Profiles.Count == 0)
            return 0;

        var dataBytes = phase1Profiles.Select(GetDataBlockBytes).ToArray();
        var prefixSums = new long[dataBytes.Length + 1];
        for (int i = 0; i < dataBytes.Length; i++)
            prefixSums[i + 1] = prefixSums[i] + dataBytes[i];

        for (int index = 0; index < dataBytes.Length; index++)
        {
            int endExclusive = Math.Min(dataBytes.Length, index + lookaheadCount);
            long windowBytes = prefixSums[endExclusive] - prefixSums[index];
            double averageBytes = (double)windowBytes / Math.Max(1, endExclusive - index);
            if (averageBytes < minimumAverageBytes)
                return index;
        }

        return dataBytes.Length;
    }

    private static BoundaryRegionSummary BuildBoundaryRegionSummary(
        IReadOnlyList<RestoreItemProfile> currentOrderedProfiles,
        SimulationResult currentSimulation,
        IReadOnlyList<RestoreItemProfile> plainOrderedProfiles,
        SimulationResult plainSimulation,
        int boundaryPosition,
        int windowSize)
    {
        var currentTrace = currentSimulation.ActiveVolumesAfterEachItem;
        var plainTrace = plainSimulation.ActiveVolumesAfterEachItem;
        if (currentTrace == null || plainTrace == null || currentTrace.Count == 0 || plainTrace.Count == 0)
            return new BoundaryRegionSummary(boundaryPosition, windowSize, []);

        var currentVolumeTouchPositions = BuildVolumeTouchPositions(currentOrderedProfiles);
        var plainVolumeTouchPositions = BuildVolumeTouchPositions(plainOrderedProfiles);
        int boundaryStart = Math.Max(1, boundaryPosition - (2 * windowSize));
        int boundaryEnd = Math.Min(currentOrderedProfiles.Count, boundaryPosition + (2 * windowSize) - 1);
        var segments = new List<BoundaryRegionSegment>();

        for (int startPosition = boundaryStart; startPosition <= boundaryEnd; startPosition += windowSize)
        {
            int endPosition = Math.Min(startPosition + windowSize - 1, boundaryEnd);
            int startIndex = startPosition - 1;
            int endExclusive = endPosition;
            var currentWindow = currentTrace.Skip(startIndex).Take(endExclusive - startIndex).ToArray();
            var plainWindow = plainTrace.Skip(startIndex).Take(endExclusive - startIndex).ToArray();
            int currentPeak = currentWindow.Max();
            int plainPeak = plainWindow.Max();
            int currentPeakPosition = startPosition + Array.IndexOf(currentWindow, currentPeak);
            int plainPeakPosition = startPosition + Array.IndexOf(plainWindow, plainPeak);

            segments.Add(new BoundaryRegionSegment(
                startPosition,
                endPosition,
                BuildOrderingWindowDynamics(currentOrderedProfiles, currentTrace, currentVolumeTouchPositions, startIndex, endExclusive),
                BuildOrderingWindowDynamics(plainOrderedProfiles, plainTrace, plainVolumeTouchPositions, startIndex, endExclusive),
                currentPeak,
                plainPeak,
                plainPeak - currentPeak,
                currentOrderedProfiles[currentPeakPosition - 1].Item.File.TargetPath,
                plainOrderedProfiles[plainPeakPosition - 1].Item.File.TargetPath));
        }

        return new BoundaryRegionSummary(boundaryPosition, windowSize, segments);
    }

    private static DefragWhatIfSummary BuildDefragWhatIfSummary(
        SimulationModel simulationModel,
        IReadOnlyDictionary<long, int> volumeItemCounts,
        PeakPressureSummary peakPressure,
        SimulationResult currentSimulation,
        SimulationResult phase1TailSizeBaseline,
        SimulationResult plainSizeBaseline,
        int effectiveSharedThreshold,
        long effectiveVolumeCacheBudget,
        long sharedBlockStoreBudget,
        long restoreVolumeCacheHint,
        int topItems,
        DateTime restoreTimeUtc)
    {
        var peakPressureItems = peakPressure.TopItems.ToDictionary(item => item.ItemIndex);
        var sharedFragmentationItems = BuildFragmentationItems(simulationModel.Profiles, volumeItemCounts, includeExclusiveVolumes: false)
            .ToDictionary(item => item.ItemIndex);
        var candidateSeeds = simulationModel.Profiles
            .Where(profile => profile.Item.Kind == RestoreItemKind.FileData)
            .Select(profile => BuildDefragCandidateSeed(profile, volumeItemCounts, sharedFragmentationItems, peakPressureItems))
            .Where(seed => seed is not null)
            .Cast<DefragCandidateSeed>()
            .OrderByDescending(seed => seed.PriorityScore)
            .ThenByDescending(seed => seed.PeakPressureItem?.PeakPressureScore ?? 0)
            .ThenByDescending(seed => seed.SharedDataBytes)
            .ThenByDescending(seed => seed.SharedDistinctVolumes)
            .ThenBy(seed => seed.Profile.Item.SequenceIndex)
            .ToList();
        int peakEligibleCandidateCount = candidateSeeds.Count;
        candidateSeeds = candidateSeeds
            .Take(Math.Min(topItems, MAX_DEFRAG_CANDIDATE_SIMULATIONS))
            .ToList();

        long targetVolumeSize = simulationModel.VolumeSizes.Values.Where(size => size > 0).DefaultIfEmpty(50L * 1024 * 1024).Max();
        var candidateResults = new List<DefragWhatIfCandidate>(candidateSeeds.Count);

        foreach (var seed in candidateSeeds)
        {
            var layout = BuildDefraggedProfiles(simulationModel.Profiles, [seed.Profile.Item.File.ID], volumeItemCounts, targetVolumeSize);
            var defraggedModel = BuildSimulationModelFromProfiles(layout.Profiles, effectiveSharedThreshold, simulationModel, layout.SyntheticVolumeSizes);
            var phasedSimulation = Simulate(
                defraggedModel.Profiles,
                defraggedModel,
                effectiveSharedThreshold,
                effectiveVolumeCacheBudget,
                sharedBlockStoreBudget,
                restoreVolumeCacheHint,
                topVolumes: 0,
                phase1WindowSize: 1,
                phase1TopWindows: 0,
                restoreTimeUtc,
                captureDetails: false);
            var phase1TailSimulation = Simulate(
                ReorderProfilesByPhase1ThenSizeDescendingTail(defraggedModel.Profiles),
                defraggedModel,
                effectiveSharedThreshold,
                effectiveVolumeCacheBudget,
                sharedBlockStoreBudget,
                restoreVolumeCacheHint,
                topVolumes: 0,
                phase1WindowSize: 1,
                phase1TopWindows: 0,
                restoreTimeUtc,
                captureDetails: false);
            var plainSimulation = Simulate(
                ReorderProfilesByPlainSizeDescending(defraggedModel.Profiles),
                defraggedModel,
                effectiveSharedThreshold,
                effectiveVolumeCacheBudget,
                sharedBlockStoreBudget,
                restoreVolumeCacheHint,
                topVolumes: 0,
                phase1WindowSize: 1,
                phase1TopWindows: 0,
                restoreTimeUtc,
                captureDetails: false);

            candidateResults.Add(new DefragWhatIfCandidate(
                seed.Profile.Item.SequenceIndex,
                seed.Profile.Item.File.TargetPath,
                seed.Profile.Item.SqlPhase.ToString(),
                seed.SharedBlockReferences,
                seed.SharedDataBlockReferences,
                seed.SharedDataBytes,
                seed.SharedDistinctVolumes,
                seed.SharedItem.FragmentationScore,
                seed.SharedItem.EffectiveVolumeCount,
                seed.PeakPressureItem?.PeakSharedVolumeOverlap ?? 0,
                seed.PeakPressureItem?.PeakSharedRemainingReferencesSum ?? 0,
                seed.PeakPressureItem?.PeakPressureScore ?? 0,
                layout.EstimatedPrivateVolumeCountsByFileId.GetValueOrDefault(seed.Profile.Item.File.ID),
                phasedSimulation.PredictedPeakActiveVolumes,
                phasedSimulation.PredictedPeakActiveVolumes - currentSimulation.PredictedPeakActiveVolumes,
                phase1TailSimulation.PredictedPeakActiveVolumes,
                phase1TailSimulation.PredictedPeakActiveVolumes - phase1TailSizeBaseline.PredictedPeakActiveVolumes,
                plainSimulation.PredictedPeakActiveVolumes,
                plainSimulation.PredictedPeakActiveVolumes - plainSizeBaseline.PredictedPeakActiveVolumes));
        }

        candidateResults = candidateResults
            .OrderBy(candidate => Math.Min(candidate.PredictedPeakActiveVolumeDelta, Math.Min(candidate.PredictedPhase1TailSizePeakDelta, candidate.PredictedPlainSizePeakDelta)))
            .ThenBy(candidate => candidate.PredictedPhase1TailSizePeakDelta)
            .ThenBy(candidate => candidate.PredictedPlainSizePeakDelta)
            .ThenByDescending(candidate => candidate.PeakPressureScore)
            .ThenByDescending(candidate => candidate.SharedDataBytes)
            .ThenBy(candidate => candidate.ItemIndex)
            .ToList();

        var incrementalScenarios = new List<DefragWhatIfScenario>();
        for (int count = 1; count <= Math.Min(2, candidateResults.Count); count++)
        {
            var selectedFileIds = candidateResults.Take(count)
                .Join(simulationModel.Profiles, candidate => candidate.ItemIndex, profile => profile.Item.SequenceIndex, (candidate, profile) => profile.Item.File.ID)
                .Distinct()
                .ToArray();
            var layout = BuildDefraggedProfiles(simulationModel.Profiles, selectedFileIds, volumeItemCounts, targetVolumeSize);
            var defraggedModel = BuildSimulationModelFromProfiles(layout.Profiles, effectiveSharedThreshold, simulationModel, layout.SyntheticVolumeSizes);
            var phasedSimulation = Simulate(
                defraggedModel.Profiles,
                defraggedModel,
                effectiveSharedThreshold,
                effectiveVolumeCacheBudget,
                sharedBlockStoreBudget,
                restoreVolumeCacheHint,
                topVolumes: 0,
                phase1WindowSize: 1,
                phase1TopWindows: 0,
                restoreTimeUtc,
                captureDetails: false);
            var phase1TailSimulation = Simulate(
                ReorderProfilesByPhase1ThenSizeDescendingTail(defraggedModel.Profiles),
                defraggedModel,
                effectiveSharedThreshold,
                effectiveVolumeCacheBudget,
                sharedBlockStoreBudget,
                restoreVolumeCacheHint,
                topVolumes: 0,
                phase1WindowSize: 1,
                phase1TopWindows: 0,
                restoreTimeUtc,
                captureDetails: false);
            var plainSimulation = Simulate(
                ReorderProfilesByPlainSizeDescending(defraggedModel.Profiles),
                defraggedModel,
                effectiveSharedThreshold,
                effectiveVolumeCacheBudget,
                sharedBlockStoreBudget,
                restoreVolumeCacheHint,
                topVolumes: 0,
                phase1WindowSize: 1,
                phase1TopWindows: 0,
                restoreTimeUtc,
                captureDetails: false);

            incrementalScenarios.Add(new DefragWhatIfScenario(
                $"Top {count} candidate{(count == 1 ? string.Empty : "s")}",
                count,
                phasedSimulation.PredictedPeakActiveVolumes,
                phasedSimulation.PredictedPeakActiveVolumes - currentSimulation.PredictedPeakActiveVolumes,
                phase1TailSimulation.PredictedPeakActiveVolumes,
                phase1TailSimulation.PredictedPeakActiveVolumes - phase1TailSizeBaseline.PredictedPeakActiveVolumes,
                plainSimulation.PredictedPeakActiveVolumes,
                plainSimulation.PredictedPeakActiveVolumes - plainSizeBaseline.PredictedPeakActiveVolumes));
        }

        return new DefragWhatIfSummary(
            currentSimulation.PredictedPeakActiveVolumes,
            phase1TailSizeBaseline.PredictedPeakActiveVolumes,
            phase1TailSizeBaseline.PredictedPeakActiveVolumes - currentSimulation.PredictedPeakActiveVolumes,
            plainSizeBaseline.PredictedPeakActiveVolumes,
            plainSizeBaseline.PredictedPeakActiveVolumes - currentSimulation.PredictedPeakActiveVolumes,
            peakEligibleCandidateCount,
            candidateResults.Count,
            candidateResults,
            incrementalScenarios);
    }

    private static DefragCandidateSeed? BuildDefragCandidateSeed(
        RestoreItemProfile profile,
        IReadOnlyDictionary<long, int> volumeItemCounts,
        IReadOnlyDictionary<int, FragmentationItemDetail> sharedFragmentationItems,
        IReadOnlyDictionary<int, PeakPressureItem> peakPressureItems)
    {
        if (!sharedFragmentationItems.TryGetValue(profile.Item.SequenceIndex, out var sharedItem) || sharedItem.DistinctVolumes <= 1)
            return null;

        if (!peakPressureItems.TryGetValue(profile.Item.SequenceIndex, out var peakPressureItem)
            || peakPressureItem.PeakSharedVolumeOverlap <= 0
            || peakPressureItem.PeakSharedRemainingReferencesSum <= 0)
            return null;

        var sharedBlocks = profile.Blocks
            .Where(block => volumeItemCounts.GetValueOrDefault(block.VolumeId) > 1)
            .ToList();
        int sharedDataBlockReferences = sharedBlocks.Count(block => !block.IsMetadata);
        long sharedDataBytes = sharedBlocks.Where(block => !block.IsMetadata).Sum(block => block.BlockSize);
        if (sharedDataBlockReferences <= 0 || sharedDataBytes <= 0)
            return null;

        double peakPressureScore = peakPressureItem.PeakPressureScore;
        double priorityScore = (peakPressureScore + 1)
            * Math.Log10(sharedDataBytes + 1)
            * Math.Max(1, peakPressureItem.PeakSharedVolumeOverlap)
            * Math.Max(1, sharedItem.EffectiveVolumeCount - 1);

        return new DefragCandidateSeed(
            profile,
            sharedItem,
            peakPressureItem,
            sharedBlocks.Count,
            sharedDataBlockReferences,
            sharedDataBytes,
            sharedItem.DistinctVolumes,
            priorityScore);
    }

    private static DefragLayout BuildDefraggedProfiles(
        IReadOnlyList<RestoreItemProfile> profiles,
        IReadOnlyCollection<long> fileIdsToDefrag,
        IReadOnlyDictionary<long, int> volumeItemCounts,
        long targetVolumeSize)
    {
        var fileIdSet = fileIdsToDefrag.ToHashSet();
        long nextBlockId = profiles.SelectMany(profile => profile.Blocks).Select(block => block.BlockId).DefaultIfEmpty(0).Max() + 1;
        long nextVolumeId = profiles.SelectMany(profile => profile.DistinctVolumes).DefaultIfEmpty(0).Max() + 1;
        var syntheticVolumeSizes = new Dictionary<long, long>();
        var estimatedPrivateVolumeCountsByFileId = new Dictionary<long, int>();
        var updatedProfiles = new List<RestoreItemProfile>(profiles.Count);

        foreach (var profile in profiles)
        {
            if (!fileIdSet.Contains(profile.Item.File.ID) || profile.Item.Kind != RestoreItemKind.FileData)
            {
                updatedProfiles.Add(profile);
                continue;
            }

            var rewrittenBlocks = new List<SimulatedBlock>(profile.Blocks.Count);
            long currentSyntheticVolumeId = -1;
            long currentSyntheticVolumeBytes = 0;
            int syntheticVolumeCount = 0;

            foreach (var block in profile.Blocks)
            {
                bool shouldRewrite = block.BlockSize > 0 && volumeItemCounts.GetValueOrDefault(block.VolumeId) > 1;
                if (!shouldRewrite)
                {
                    rewrittenBlocks.Add(block);
                    continue;
                }

                if (currentSyntheticVolumeId < 0 || (targetVolumeSize > 0 && currentSyntheticVolumeBytes > 0 && currentSyntheticVolumeBytes + block.BlockSize > targetVolumeSize))
                {
                    currentSyntheticVolumeId = nextVolumeId++;
                    currentSyntheticVolumeBytes = 0;
                    syntheticVolumeSizes[currentSyntheticVolumeId] = 0;
                    syntheticVolumeCount++;
                }

                rewrittenBlocks.Add(new SimulatedBlock(nextBlockId++, block.BlockSize, currentSyntheticVolumeId, block.IsMetadata));
                currentSyntheticVolumeBytes += block.BlockSize;
                syntheticVolumeSizes[currentSyntheticVolumeId] += block.BlockSize;
            }

            var volumeReferenceUsage = rewrittenBlocks
                .GroupBy(block => block.VolumeId)
                .ToDictionary(group => group.Key, group => (long)group.Count());
            var distinctVolumes = rewrittenBlocks
                .Where(block => block.BlockSize > 0)
                .Select(block => block.VolumeId)
                .Distinct()
                .ToArray();

            estimatedPrivateVolumeCountsByFileId[profile.Item.File.ID] = syntheticVolumeCount;
            updatedProfiles.Add(new RestoreItemProfile(profile.Item, rewrittenBlocks, volumeReferenceUsage, distinctVolumes, profile.OriginalOrder));
        }

        return new DefragLayout(updatedProfiles, syntheticVolumeSizes, estimatedPrivateVolumeCountsByFileId);
    }

    private static SimulationModel BuildSimulationModelFromProfiles(
        IReadOnlyList<RestoreItemProfile> profiles,
        int effectiveSharedThreshold,
        SimulationModel baselineModel,
        IReadOnlyDictionary<long, long> syntheticVolumeSizes)
    {
        var blockReferenceCounts = new Dictionary<long, long>();
        var volumeReferenceCounts = new Dictionary<long, long>();
        var blockVolumes = new Dictionary<long, long>();

        foreach (var profile in profiles)
        {
            foreach (var block in profile.Blocks)
            {
                blockReferenceCounts[block.BlockId] = blockReferenceCounts.TryGetValue(block.BlockId, out var blockCount) ? blockCount + 1 : 1;
                volumeReferenceCounts[block.VolumeId] = volumeReferenceCounts.TryGetValue(block.VolumeId, out var volumeCount) ? volumeCount + 1 : 1;
                blockVolumes.TryAdd(block.BlockId, block.VolumeId);
            }
        }

        var sharedRefScores = new Dictionary<long, long>();
        if (effectiveSharedThreshold > 0)
        {
            foreach (var (blockId, referenceCount) in blockReferenceCounts)
            {
                if (referenceCount <= effectiveSharedThreshold)
                    continue;

                var volumeId = blockVolumes[blockId];
                sharedRefScores[volumeId] = sharedRefScores.TryGetValue(volumeId, out var score)
                    ? score + referenceCount
                    : referenceCount;
            }
        }

        var volumeNames = new Dictionary<long, string>();
        var volumeSizes = new Dictionary<long, long>();
        var volumeOperationIds = new Dictionary<long, long>();
        var volumeCreatedAtUtc = new Dictionary<long, DateTime?>();
        var volumeArchiveTimeUtc = new Dictionary<long, DateTime?>();
        var volumeStates = new Dictionary<long, string>();

        foreach (var volumeId in volumeReferenceCounts.Keys)
        {
            if (baselineModel.VolumeNames.ContainsKey(volumeId))
            {
                volumeNames[volumeId] = baselineModel.VolumeNames.GetValueOrDefault(volumeId, string.Empty);
                volumeSizes[volumeId] = baselineModel.VolumeSizes.GetValueOrDefault(volumeId);
                volumeOperationIds[volumeId] = baselineModel.VolumeOperationIds.GetValueOrDefault(volumeId, -1);
                volumeCreatedAtUtc[volumeId] = baselineModel.VolumeCreatedAtUtc.GetValueOrDefault(volumeId);
                volumeArchiveTimeUtc[volumeId] = baselineModel.VolumeArchiveTimeUtc.GetValueOrDefault(volumeId);
                volumeStates[volumeId] = baselineModel.VolumeStates.GetValueOrDefault(volumeId, string.Empty);
                continue;
            }

            volumeNames[volumeId] = $"<synthetic-defrag:{volumeId}>";
            volumeSizes[volumeId] = syntheticVolumeSizes.GetValueOrDefault(volumeId);
            volumeOperationIds[volumeId] = -1;
            volumeCreatedAtUtc[volumeId] = null;
            volumeArchiveTimeUtc[volumeId] = null;
            volumeStates[volumeId] = "Synthetic";
        }

        return new SimulationModel(profiles, blockReferenceCounts, volumeReferenceCounts, sharedRefScores, volumeNames, volumeSizes, volumeOperationIds, volumeCreatedAtUtc, volumeArchiveTimeUtc, volumeStates);
    }

    private static SimulationResult Simulate(
        IReadOnlyList<RestoreItemProfile> orderedProfiles,
        SimulationModel simulationModel,
        int effectiveSharedThreshold,
        long effectiveVolumeCacheBudget,
        long sharedBlockStoreBudget,
        long restoreVolumeCacheHint,
        int topVolumes,
        int phase1WindowSize,
        int phase1TopWindows,
        DateTime restoreTimeUtc,
        bool captureDetails,
        bool captureItemTrace = false,
        bool captureTimeline = false)
    {
        var remainingBlockReferences = new Dictionary<long, long>(simulationModel.BlockReferenceCounts);
        var remainingVolumeReferences = new Dictionary<long, long>(simulationModel.VolumeReferenceCounts);
        var sharedStates = new Dictionary<long, bool>();
        var seenBlocks = new HashSet<long>();
        var seenVolumes = new HashSet<long>();
        var activeVolumes = new HashSet<long>();
        var cachedVolumes = new Dictionary<long, long>();
        var cacheLru = new List<long>();
        var volumeObservations = simulationModel.VolumeReferenceCounts.ToDictionary(
            x => x.Key,
            x => new VolumeObservation(x.Key, x.Value, simulationModel.SharedRefScores.GetValueOrDefault(x.Key))
            {
                Name = simulationModel.VolumeNames.GetValueOrDefault(x.Key, string.Empty),
                Size = simulationModel.VolumeSizes.GetValueOrDefault(x.Key),
                OperationId = simulationModel.VolumeOperationIds.GetValueOrDefault(x.Key, -1),
                CreatedAtUtc = simulationModel.VolumeCreatedAtUtc.GetValueOrDefault(x.Key),
                ArchiveTimeUtc = simulationModel.VolumeArchiveTimeUtc.GetValueOrDefault(x.Key),
                State = simulationModel.VolumeStates.GetValueOrDefault(x.Key, string.Empty)
            });

        PeakPoint? peak = null;
        var phaseSnapshots = new List<PhaseSnapshot>();
        long cacheBytes = 0;
        int predictedPeakCachedVolumes = 0;
        int sharedBlocksStored = 0;
        long sharedBlockStoreBytesWritten = 0;
        long blockRequests = 0;
        int eventIndex = 0;
        int firstPhase2ItemIndex = orderedProfiles.FirstOrDefault(x => x.IsPhase2Candidate)?.Item.SequenceIndex ?? int.MaxValue;
        int lastPhase1Position = orderedProfiles
            .Select((profile, index) => (profile, index))
            .Where(x => x.profile.IsPhase1File)
            .Select(x => x.index)
            .DefaultIfEmpty(-1)
            .Max();
        int lastPhase2Position = orderedProfiles
            .Select((profile, index) => (profile, index))
            .Where(x => x.profile.IsPhase2Candidate)
            .Select(x => x.index)
            .DefaultIfEmpty(-1)
            .Max();
        var phase1Windows = new List<Phase1WindowSummary>();
        int phase1WindowItemCount = 0;
        int phase1WindowStartItemIndex = 0;
        string phase1WindowStartPath = string.Empty;
        int phase1WindowStartActive = 0;
        int phase1WindowPeakActive = 0;
        int phase1WindowPeakItemIndex = 0;
        string phase1WindowPeakPath = string.Empty;
        int phase1WindowEndItemIndex = 0;
        string phase1WindowEndPath = string.Empty;
        HashSet<long>? phase1WindowIntroducedVolumes = null;
        bool phase1SnapshotRecorded = false;
        bool phase2StartRecorded = false;
        bool phase2EndRecorded = false;
        var phase1WindowStates = new List<Phase1WindowState>();
        var activeVolumesAfterEachItem = (captureItemTrace || captureTimeline) ? new List<int>(orderedProfiles.Count) : null;
        var orderedItemIndexes = captureTimeline ? new List<int>(orderedProfiles.Count) : null;
        var cachedVolumesAfterEachItem = captureTimeline ? new List<int>(orderedProfiles.Count) : null;
        var introducedVolumesByItem = captureTimeline ? new List<int>(orderedProfiles.Count) : null;
        var drainedVolumesByItem = captureTimeline ? new List<int>(orderedProfiles.Count) : null;
        var retiredVolumesTotalAfterEachItem = captureTimeline ? new List<int>(orderedProfiles.Count) : null;
        var introducedVolumeIdsByItem = captureTimeline ? new List<IReadOnlyList<long>>(orderedProfiles.Count) : null;
        var drainedVolumeIdsByItem = captureTimeline ? new List<IReadOnlyList<long>>(orderedProfiles.Count) : null;
        var volumeIntroducedAtItem = captureTimeline ? new Dictionary<long, int>() : null;
        var volumeDrainedAtItem = captureTimeline ? new Dictionary<long, int>() : null;
        int retiredVolumesTotal = 0;

        foreach (var (profile, profileIndex) in orderedProfiles.Select((profile, index) => (profile, index)))
        {
            int introducedVolumesThisItem = 0;
            int drainedVolumesThisItem = 0;
            var introducedVolumeIdsThisItem = captureTimeline ? new List<long>() : null;
            var drainedVolumeIdsThisItem = captureTimeline ? new List<long>() : null;

            if (captureDetails && profile.IsPhase1File && phase1WindowItemCount == 0)
            {
                phase1WindowStartItemIndex = profile.Item.SequenceIndex;
                phase1WindowStartPath = profile.Item.File.TargetPath;
                phase1WindowStartActive = activeVolumes.Count;
                phase1WindowPeakActive = activeVolumes.Count;
                phase1WindowPeakItemIndex = profile.Item.SequenceIndex;
                phase1WindowPeakPath = profile.Item.File.TargetPath;
                phase1WindowIntroducedVolumes = [];
            }

            if (!phase2StartRecorded && profile.IsPhase2Candidate)
            {
                phaseSnapshots.Add(new PhaseSnapshot("Phase2Start", profile.Item.SequenceIndex, activeVolumes.Count, cachedVolumes.Count));
                phase2StartRecorded = true;
            }

            foreach (var block in profile.Blocks)
            {
                blockRequests++;
                eventIndex++;

                var touchKind = GetTouchKind(profile.Item, block.IsMetadata);
                bool isSharedCandidate = effectiveSharedThreshold > 0
                    && simulationModel.BlockReferenceCounts.TryGetValue(block.BlockId, out var initialBlockRefs)
                    && initialBlockRefs > effectiveSharedThreshold;
                bool firstBlockOccurrence = seenBlocks.Add(block.BlockId);
                bool storedSharedBlock = false;

                if (isSharedCandidate && firstBlockOccurrence)
                {
                    if (sharedBlockStoreBudget < 0 || sharedBlockStoreBytesWritten + block.BlockSize <= sharedBlockStoreBudget)
                    {
                        sharedStates[block.BlockId] = true;
                        storedSharedBlock = true;
                        sharedBlocksStored++;
                        sharedBlockStoreBytesWritten += block.BlockSize;
                    }
                    else
                    {
                        sharedStates[block.BlockId] = false;
                    }
                }
                else if (isSharedCandidate)
                {
                    storedSharedBlock = sharedStates.GetValueOrDefault(block.BlockId);
                }

                bool touchesVolume = firstBlockOccurrence && block.BlockSize > 0;
                if (touchesVolume)
                {
                    if (activeVolumes.Add(block.VolumeId))
                    {
                        introducedVolumesThisItem++;
                        introducedVolumeIdsThisItem?.Add(block.VolumeId);
                        volumeIntroducedAtItem?.TryAdd(block.VolumeId, profile.Item.SequenceIndex);
                    }
                    if (captureDetails)
                        TouchObservation(volumeObservations[block.VolumeId], profile.Item, touchKind);

                    if (captureDetails && profile.IsPhase1File && seenVolumes.Add(block.VolumeId))
                        phase1WindowIntroducedVolumes?.Add(block.VolumeId);

                    if (restoreVolumeCacheHint != 0)
                    {
                        SimulateCacheTouch(block.VolumeId);
                    }

                    if (activeVolumes.Count > (peak?.ActiveVolumeCount ?? 0))
                    {
                        peak = new PeakPoint(
                            activeVolumes.Count,
                            cachedVolumes.Count,
                            eventIndex,
                            profile.Item.SequenceIndex,
                            profile.Item.File.TargetPath,
                            block.BlockId,
                            block.VolumeId,
                            block.IsMetadata,
                            [.. activeVolumes.OrderBy(x => x)],
                            activeVolumes.ToDictionary(x => x, x => remainingVolumeReferences.GetValueOrDefault(x)));
                    }

                    if (captureDetails && profile.IsPhase1File && activeVolumes.Count >= phase1WindowPeakActive)
                    {
                        phase1WindowPeakActive = activeVolumes.Count;
                        phase1WindowPeakItemIndex = profile.Item.SequenceIndex;
                        phase1WindowPeakPath = profile.Item.File.TargetPath;
                    }
                }

                if (cachedVolumes.Count > predictedPeakCachedVolumes)
                    predictedPeakCachedVolumes = cachedVolumes.Count;

                if (!remainingBlockReferences.TryGetValue(block.BlockId, out var remainingBlockCountBefore) || remainingBlockCountBefore <= 0)
                    continue;

                if (storedSharedBlock && firstBlockOccurrence)
                {
                    remainingVolumeReferences[block.VolumeId] = Math.Max(0, remainingVolumeReferences.GetValueOrDefault(block.VolumeId) - remainingBlockCountBefore);
                    remainingBlockReferences[block.BlockId] = remainingBlockCountBefore - 1;
                }
                else if (storedSharedBlock)
                {
                    remainingBlockReferences[block.BlockId] = remainingBlockCountBefore - 1;
                }
                else
                {
                    remainingBlockReferences[block.BlockId] = remainingBlockCountBefore - 1;
                    remainingVolumeReferences[block.VolumeId] = Math.Max(0, remainingVolumeReferences.GetValueOrDefault(block.VolumeId) - 1);
                }

                if (remainingBlockReferences[block.BlockId] == 0)
                    remainingBlockReferences.Remove(block.BlockId);

                if (remainingVolumeReferences.TryGetValue(block.VolumeId, out var remainingVolumeCount) && remainingVolumeCount == 0)
                {
                    remainingVolumeReferences.Remove(block.VolumeId);
                    activeVolumes.Remove(block.VolumeId);
                    cachedVolumes.Remove(block.VolumeId, out var removedSize);
                    cacheBytes -= removedSize;
                    cacheLru.Remove(block.VolumeId);
                    drainedVolumesThisItem++;
                    drainedVolumeIdsThisItem?.Add(block.VolumeId);
                    volumeDrainedAtItem?[block.VolumeId] = profile.Item.SequenceIndex;
                    retiredVolumesTotal++;

                    if (captureDetails)
                    {
                        var observation = volumeObservations[block.VolumeId];
                        observation.ReleaseItemIndex ??= profile.Item.SequenceIndex;
                        observation.ReleasePath ??= profile.Item.File.TargetPath;
                        observation.ReleaseKind ??= touchKind;
                    }
                }
            }

            if (captureDetails && profile.IsPhase1File)
            {
                phase1WindowItemCount++;
                phase1WindowEndItemIndex = profile.Item.SequenceIndex;
                phase1WindowEndPath = profile.Item.File.TargetPath;

                bool closeWindow = phase1WindowItemCount >= phase1WindowSize || profileIndex == lastPhase1Position;
                if (closeWindow)
                {
                    var windowSummary = new Phase1WindowSummary(
                        phase1WindowStartItemIndex,
                        phase1WindowEndItemIndex,
                        phase1WindowItemCount,
                        phase1WindowStartActive,
                        activeVolumes.Count,
                        activeVolumes.Count - phase1WindowStartActive,
                        phase1WindowPeakActive,
                        phase1WindowPeakItemIndex,
                        phase1WindowStartPath,
                        phase1WindowEndPath,
                        phase1WindowPeakPath);

                    phase1Windows.Add(windowSummary);
                    phase1WindowStates.Add(new Phase1WindowState(
                        windowSummary,
                        phase1WindowIntroducedVolumes?.OrderBy(x => x).ToArray() ?? []));

                    phase1WindowItemCount = 0;
                    phase1WindowStartItemIndex = 0;
                    phase1WindowStartPath = string.Empty;
                    phase1WindowStartActive = 0;
                    phase1WindowPeakActive = 0;
                    phase1WindowPeakItemIndex = 0;
                    phase1WindowPeakPath = string.Empty;
                    phase1WindowEndItemIndex = 0;
                    phase1WindowEndPath = string.Empty;
                    phase1WindowIntroducedVolumes = null;
                }
            }

            if (!phase1SnapshotRecorded && profileIndex == lastPhase1Position)
            {
                phaseSnapshots.Add(new PhaseSnapshot("AfterPhase1", profile.Item.SequenceIndex, activeVolumes.Count, cachedVolumes.Count));
                phase1SnapshotRecorded = true;
            }

            if (!phase2EndRecorded && profileIndex == lastPhase2Position)
            {
                phaseSnapshots.Add(new PhaseSnapshot("AfterPhase2", profile.Item.SequenceIndex, activeVolumes.Count, cachedVolumes.Count));
                phase2EndRecorded = true;
            }

            activeVolumesAfterEachItem?.Add(activeVolumes.Count);
            orderedItemIndexes?.Add(profile.Item.SequenceIndex);
            cachedVolumesAfterEachItem?.Add(cachedVolumes.Count);
            introducedVolumesByItem?.Add(introducedVolumesThisItem);
            drainedVolumesByItem?.Add(drainedVolumesThisItem);
            retiredVolumesTotalAfterEachItem?.Add(retiredVolumesTotal);
            introducedVolumeIdsByItem?.Add(introducedVolumeIdsThisItem?.ToArray() ?? []);
            drainedVolumeIdsByItem?.Add(drainedVolumeIdsThisItem?.ToArray() ?? []);
        }

        if (!phase1SnapshotRecorded)
            phaseSnapshots.Add(new PhaseSnapshot("AfterPhase1", 0, activeVolumes.Count, cachedVolumes.Count));
        if (!phase2StartRecorded)
            phaseSnapshots.Add(new PhaseSnapshot("Phase2Start", 0, activeVolumes.Count, cachedVolumes.Count));
        if (!phase2EndRecorded)
            phaseSnapshots.Add(new PhaseSnapshot("AfterPhase2", 0, activeVolumes.Count, cachedVolumes.Count));

        var topPinningVolumes = !captureDetails || peak == null
            ? []
            : peak.ActiveVolumeIds
                .Select(volumeId =>
                {
                    var observation = volumeObservations[volumeId];
                    return new VolumePeakDetail(
                        volumeId,
                        observation.Name,
                        observation.Size,
                        peak.RemainingReferencesAtPeak.GetValueOrDefault(volumeId),
                        observation.InitialReferenceCount,
                        observation.SharedRefScore,
                        observation.DistinctTouchedFiles,
                        observation.OperationId,
                        observation.CreatedAtUtc,
                        observation.CreatedAtUtc.HasValue ? (restoreTimeUtc - observation.CreatedAtUtc.Value).TotalDays : null,
                        observation.ArchiveTimeUtc,
                        observation.State,
                        observation.FirstTouchItemIndex,
                        observation.FirstTouchPath,
                        observation.FirstTouchKind?.ToString(),
                        observation.ReleaseItemIndex,
                        observation.ReleasePath,
                        observation.ReleaseKind?.ToString());
                })
                .OrderByDescending(x => x.RemainingReferencesAtPeak)
                .ThenByDescending(x => x.SharedRefScore)
                .ThenBy(x => x.VolumeId)
                .Take(topVolumes)
                .ToList();
        var allPrePhase2PeakVolumes = !captureDetails || peak == null
            ? []
            : peak.ActiveVolumeIds
                .Where(volumeId => (volumeObservations[volumeId].FirstTouchItemIndex ?? int.MaxValue) < firstPhase2ItemIndex)
                .Select(volumeId =>
                {
                    var observation = volumeObservations[volumeId];
                    return new VolumePeakDetail(
                        volumeId,
                        observation.Name,
                        observation.Size,
                        peak.RemainingReferencesAtPeak.GetValueOrDefault(volumeId),
                        observation.InitialReferenceCount,
                        observation.SharedRefScore,
                        observation.DistinctTouchedFiles,
                        observation.OperationId,
                        observation.CreatedAtUtc,
                        observation.CreatedAtUtc.HasValue ? (restoreTimeUtc - observation.CreatedAtUtc.Value).TotalDays : null,
                        observation.ArchiveTimeUtc,
                        observation.State,
                        observation.FirstTouchItemIndex,
                        observation.FirstTouchPath,
                        observation.FirstTouchKind?.ToString(),
                        observation.ReleaseItemIndex,
                        observation.ReleasePath,
                        observation.ReleaseKind?.ToString());
                })
                .OrderByDescending(x => x.RemainingReferencesAtPeak)
                .ThenByDescending(x => x.SharedRefScore)
                .ThenBy(x => x.VolumeId)
                .ToList();
        var prePhase2PeakVolumes = allPrePhase2PeakVolumes.Take(topVolumes).ToList();

        var peakActiveVolumesByFirstTouchKind = !captureDetails || peak == null
            ? new Dictionary<string, int>()
            : CountKinds(peak.ActiveVolumeIds.Select(volumeId => volumeObservations[volumeId].FirstTouchKind?.ToString()));
        var peakActiveVolumesByReleaseKind = !captureDetails || peak == null
            ? new Dictionary<string, int>()
            : CountKinds(peak.ActiveVolumeIds.Select(volumeId => volumeObservations[volumeId].ReleaseKind?.ToString()));
        var phase1TopGrowthWindows = !captureDetails
            ? []
            : phase1Windows
                .OrderByDescending(x => x.NetActiveVolumeDelta)
                .ThenByDescending(x => x.PeakActiveVolumeCount)
                .ThenBy(x => x.StartItemIndex)
                .Take(phase1TopWindows)
                .ToList();
        var phase1WindowXray = !captureDetails
            ? []
            : phase1WindowStates
                .Where(window => phase1TopGrowthWindows.Any(summary => summary == window.Summary))
                .OrderByDescending(window => window.Summary.NetActiveVolumeDelta)
                .ThenByDescending(window => window.Summary.PeakActiveVolumeCount)
                .ThenBy(window => window.Summary.StartItemIndex)
                .Select(window => BuildPhase1WindowXray(window, peak, volumeObservations, topVolumes, restoreTimeUtc))
                .ToList();

        return new SimulationResult(
            blockRequests,
            simulationModel.BlockReferenceCounts.Count,
            simulationModel.VolumeReferenceCounts.Count,
            simulationModel.BlockReferenceCounts.Count(x => effectiveSharedThreshold > 0 && x.Value > effectiveSharedThreshold),
            sharedBlocksStored,
            sharedBlockStoreBytesWritten,
            peak?.ActiveVolumeCount ?? 0,
            predictedPeakCachedVolumes,
            peak,
            phaseSnapshots.OrderBy(x => x.ItemIndex).ToList(),
            phase1TopGrowthWindows,
            phase1WindowXray,
            peakActiveVolumesByFirstTouchKind,
            peakActiveVolumesByReleaseKind,
            activeVolumesAfterEachItem,
            orderedItemIndexes,
            cachedVolumesAfterEachItem,
            introducedVolumesByItem,
            drainedVolumesByItem,
            retiredVolumesTotalAfterEachItem,
            introducedVolumeIdsByItem,
            drainedVolumeIdsByItem,
            volumeIntroducedAtItem,
            volumeDrainedAtItem,
            allPrePhase2PeakVolumes.Count,
            prePhase2PeakVolumes,
            topPinningVolumes);

        void TouchObservation(VolumeObservation observation, RestoreItem item, VolumeTouchKind touchKind)
        {
            observation.FirstTouchItemIndex ??= item.SequenceIndex;
            observation.FirstTouchPath ??= item.File.TargetPath;
            observation.FirstTouchKind ??= touchKind;
            if (observation.LastTouchedFileId != item.File.ID)
            {
                observation.LastTouchedFileId = item.File.ID;
                observation.DistinctTouchedFiles++;
            }
        }

        void SimulateCacheTouch(long volumeId)
        {
            if (cachedVolumes.ContainsKey(volumeId))
            {
                cacheLru.Remove(volumeId);
                cacheLru.Add(volumeId);
                return;
            }

            var volumeSize = simulationModel.VolumeSizes.GetValueOrDefault(volumeId);
            if (restoreVolumeCacheHint > 0)
            {
                if (volumeSize > effectiveVolumeCacheBudget)
                    return;

                while (cacheBytes > 0 && cacheBytes + volumeSize > effectiveVolumeCacheBudget && cacheLru.Count > 0)
                {
                    var lruVolumeId = cacheLru[0];
                    cacheLru.RemoveAt(0);
                    if (cachedVolumes.Remove(lruVolumeId, out var lruSize))
                        cacheBytes -= lruSize;
                }
            }

            cachedVolumes[volumeId] = volumeSize;
            cacheBytes += volumeSize;
            cacheLru.Add(volumeId);
        }

        static Phase1WindowXray BuildPhase1WindowXray(
            Phase1WindowState window,
            PeakPoint? peak,
            IReadOnlyDictionary<long, VolumeObservation> volumeObservations,
            int topVolumes,
            DateTime restoreTimeUtc)
        {
            var peakActiveVolumeIds = peak?.ActiveVolumeIds?.ToHashSet() ?? [];
            var peakRemainingReferences = peak?.RemainingReferencesAtPeak ?? new Dictionary<long, long>();
            var topIntroducedVolumes = window.IntroducedVolumeIds
                .Select(volumeId =>
                {
                    var observation = volumeObservations[volumeId];
                    return new VolumePeakDetail(
                        volumeId,
                        observation.Name,
                        observation.Size,
                        peakRemainingReferences.GetValueOrDefault(volumeId),
                        observation.InitialReferenceCount,
                        observation.SharedRefScore,
                        observation.DistinctTouchedFiles,
                        observation.OperationId,
                        observation.CreatedAtUtc,
                        observation.CreatedAtUtc.HasValue ? (restoreTimeUtc - observation.CreatedAtUtc.Value).TotalDays : null,
                        observation.ArchiveTimeUtc,
                        observation.State,
                        observation.FirstTouchItemIndex,
                        observation.FirstTouchPath,
                        observation.FirstTouchKind?.ToString(),
                        observation.ReleaseItemIndex,
                        observation.ReleasePath,
                        observation.ReleaseKind?.ToString());
                })
                .OrderByDescending(x => x.RemainingReferencesAtPeak)
                .ThenByDescending(x => x.SharedRefScore)
                .ThenByDescending(x => x.ReleaseItemIndex ?? int.MinValue)
                .ThenBy(x => x.VolumeId)
                .Take(topVolumes)
                .ToList();

            return new Phase1WindowXray(
                window.Summary,
                window.IntroducedVolumeIds.Count,
                window.IntroducedVolumeIds.Count(peakActiveVolumeIds.Contains),
                topIntroducedVolumes);
        }
    }

    private static List<HeuristicComparison> ComparePhase2Heuristics(
        SimulationModel simulationModel,
        int effectiveSharedThreshold,
        long effectiveVolumeCacheBudget,
        long sharedBlockStoreBudget,
        long restoreVolumeCacheHint,
        DateTime restoreTimeUtc,
        SimulationResult currentSimulation)
    {
        var comparisons = new List<HeuristicComparison>();

        foreach (var heuristic in Enum.GetValues<Phase2Heuristic>())
        {
            if (heuristic == Phase2Heuristic.CurrentSql)
                continue;

            var reorderedProfiles = ReorderPhase2Profiles(simulationModel, heuristic);
            var simulation = Simulate(
                reorderedProfiles,
                simulationModel,
                effectiveSharedThreshold,
                effectiveVolumeCacheBudget,
                sharedBlockStoreBudget,
                restoreVolumeCacheHint,
                topVolumes: 0,
                phase1WindowSize: 1,
                phase1TopWindows: 0,
                restoreTimeUtc,
                captureDetails: false);

            comparisons.Add(new HeuristicComparison(
                GetHeuristicName(heuristic),
                simulation.PredictedPeakActiveVolumes,
                simulation.PredictedPeakCachedVolumes,
                simulation.PredictedPeakActiveVolumes - currentSimulation.PredictedPeakActiveVolumes,
                simulation.PredictedPeakCachedVolumes - currentSimulation.PredictedPeakCachedVolumes));
        }

        return comparisons
            .OrderBy(x => x.PredictedPeakActiveVolumes)
            .ThenBy(x => x.PredictedPeakCachedVolumes)
            .ThenBy(x => x.Name)
            .ToList();
    }

    private static IReadOnlyList<RestoreItemProfile> ReorderPhase2Profiles(SimulationModel simulationModel, Phase2Heuristic heuristic)
    {
        var phase1 = simulationModel.Profiles.Where(x => x.IsPhase1File).ToList();
        var phase2 = simulationModel.Profiles.Where(x => x.IsPhase2Candidate).ToList();
        var phase3 = simulationModel.Profiles.Where(x => x.IsPhase3File).ToList();
        var folderMetadata = simulationModel.Profiles.Where(x => x.Item.Kind == RestoreItemKind.FolderMetadata).ToList();

        if (phase2.Count <= 1)
            return [.. phase1, .. phase2, .. phase3, .. folderMetadata];

        var activeVolumes = new HashSet<long>();
        var remainingVolumeReferences = new Dictionary<long, long>(simulationModel.VolumeReferenceCounts);
        foreach (var profile in phase1)
            ApplyPlannerProfile(profile, activeVolumes, remainingVolumeReferences);

        var pending = new List<RestoreItemProfile>(phase2);
        var orderedPhase2 = new List<RestoreItemProfile>(phase2.Count);
        while (pending.Count > 0)
        {
            var next = pending
                .OrderBy(profile => GetPhase2SortKey(profile, heuristic, activeVolumes, remainingVolumeReferences, simulationModel.SharedRefScores))
                .First();
            orderedPhase2.Add(next);
            pending.Remove(next);
            ApplyPlannerProfile(next, activeVolumes, remainingVolumeReferences);
        }

        return [.. phase1, .. orderedPhase2, .. phase3, .. folderMetadata];
    }

    private static IReadOnlyList<RestoreItemProfile> ReorderProfilesByPlainSizeDescending(IReadOnlyList<RestoreItemProfile> profiles)
    {
        var fileItems = profiles
            .Where(profile => profile.Item.Kind == RestoreItemKind.FileData)
            .OrderByDescending(GetDataBlockBytes)
            .ThenBy(profile => profile.OriginalOrder);
        var folderMetadata = profiles
            .Where(profile => profile.Item.Kind == RestoreItemKind.FolderMetadata)
            .OrderBy(profile => profile.OriginalOrder);

        return [.. fileItems, .. folderMetadata];
    }

    private static IReadOnlyList<RestoreItemProfile> ReorderProfilesByPhase1ThenSizeDescendingTail(IReadOnlyList<RestoreItemProfile> profiles)
    {
        var phase1 = profiles
            .Where(profile => profile.IsPhase1File)
            .OrderBy(profile => profile.OriginalOrder);
        var tailFileItems = profiles
            .Where(profile => profile.Item.Kind == RestoreItemKind.FileData && !profile.IsPhase1File)
            .OrderByDescending(GetDataBlockBytes)
            .ThenBy(profile => profile.OriginalOrder);
        var folderMetadata = profiles
            .Where(profile => profile.Item.Kind == RestoreItemKind.FolderMetadata)
            .OrderBy(profile => profile.OriginalOrder);

        return [.. phase1, .. tailFileItems, .. folderMetadata];
    }

    private static IReadOnlyList<RestoreItemProfile> ReorderProfilesByBoundaryRelaxation(IReadOnlyList<RestoreItemProfile> profiles, int preservedPhase1Items)
    {
        var phase1 = profiles.Where(profile => profile.IsPhase1File).OrderBy(profile => profile.OriginalOrder).ToList();
        preservedPhase1Items = Math.Clamp(preservedPhase1Items, 0, phase1.Count);
        var preservedPrefix = phase1.Take(preservedPhase1Items).ToHashSet();
        var prefix = phase1.Take(preservedPhase1Items).OrderBy(profile => profile.OriginalOrder);
        var remainingFileItems = profiles
            .Where(profile => profile.Item.Kind == RestoreItemKind.FileData && !preservedPrefix.Contains(profile))
            .OrderByDescending(GetDataBlockBytes)
            .ThenBy(profile => profile.OriginalOrder);
        var folderMetadata = profiles
            .Where(profile => profile.Item.Kind == RestoreItemKind.FolderMetadata)
            .OrderBy(profile => profile.OriginalOrder);

        return [.. prefix, .. remainingFileItems, .. folderMetadata];
    }

    private static IReadOnlyList<RestoreItemProfile> ReorderProfilesByLargestPrefixThenCurrentOrder(IReadOnlyList<RestoreItemProfile> profiles, int prefixFileItems)
    {
        var sortedFileItems = profiles
            .Where(profile => profile.Item.Kind == RestoreItemKind.FileData)
            .OrderByDescending(GetDataBlockBytes)
            .ThenBy(profile => profile.OriginalOrder)
            .ToList();
        prefixFileItems = Math.Clamp(prefixFileItems, 0, sortedFileItems.Count);
        var prefixSet = sortedFileItems.Take(prefixFileItems).ToHashSet();
        var prefix = sortedFileItems.Take(prefixFileItems);
        var remainder = profiles
            .Where(profile => profile.Item.Kind == RestoreItemKind.FileData && !prefixSet.Contains(profile))
            .OrderBy(profile => profile.OriginalOrder);
        var folderMetadata = profiles
            .Where(profile => profile.Item.Kind == RestoreItemKind.FolderMetadata)
            .OrderBy(profile => profile.OriginalOrder);

        return [.. prefix, .. remainder, .. folderMetadata];
    }

    private static long GetDataBlockBytes(RestoreItemProfile profile)
        => profile.Blocks.Where(block => !block.IsMetadata).Sum(block => block.BlockSize);

    private static (long A, long B, long C, long D, long E, long F) GetPhase2SortKey(
        RestoreItemProfile profile,
        Phase2Heuristic heuristic,
        HashSet<long> activeVolumes,
        Dictionary<long, long> remainingVolumeReferences,
        IReadOnlyDictionary<long, long> sharedRefScores)
    {
        long newVolumes = profile.DistinctVolumes.Count(volumeId => !activeVolumes.Contains(volumeId));
        long activeTouches = profile.DistinctVolumes.Count(activeVolumes.Contains);
        long activeDrains = profile.VolumeReferenceUsage.Count(entry => activeVolumes.Contains(entry.Key) && remainingVolumeReferences.GetValueOrDefault(entry.Key) <= entry.Value);
        long residualTail = profile.VolumeReferenceUsage.Sum(entry => Math.Max(0, remainingVolumeReferences.GetValueOrDefault(entry.Key) - entry.Value));
        long sharedScore = profile.DistinctVolumes.Sum(volumeId => sharedRefScores.GetValueOrDefault(volumeId));
        long originalOrder = profile.OriginalOrder;

        return heuristic switch
        {
            Phase2Heuristic.ExistingVolumeFirst => (newVolumes, residualTail, -activeDrains, -sharedScore, originalOrder, 0),
            Phase2Heuristic.DrainActiveVolumeFirst => (-activeDrains, newVolumes, residualTail, -activeTouches, -sharedScore, originalOrder),
            Phase2Heuristic.MinResidualTail => (residualTail, newVolumes, -activeDrains, -activeTouches, -sharedScore, originalOrder),
            _ => (originalOrder, 0, 0, 0, 0, 0),
        };
    }

    private static void ApplyPlannerProfile(RestoreItemProfile profile, HashSet<long> activeVolumes, Dictionary<long, long> remainingVolumeReferences)
    {
        foreach (var volumeId in profile.DistinctVolumes)
            activeVolumes.Add(volumeId);

        foreach (var (volumeId, usage) in profile.VolumeReferenceUsage)
        {
            if (!remainingVolumeReferences.TryGetValue(volumeId, out var remaining))
                continue;

            var updated = Math.Max(0, remaining - usage);
            if (updated == 0)
            {
                remainingVolumeReferences.Remove(volumeId);
                activeVolumes.Remove(volumeId);
            }
            else
            {
                remainingVolumeReferences[volumeId] = updated;
            }
        }
    }

    private static string GetHeuristicName(Phase2Heuristic heuristic)
        => heuristic switch
        {
            Phase2Heuristic.ExistingVolumeFirst => "ExistingVolumeFirst",
            Phase2Heuristic.DrainActiveVolumeFirst => "DrainActiveVolumeFirst",
            Phase2Heuristic.MinResidualTail => "MinResidualTail",
            _ => "CurrentSql",
        };

    private static VolumeTouchKind GetTouchKind(RestoreItem item, bool isMetadata)
        => item.Kind switch
        {
            RestoreItemKind.FolderMetadata => VolumeTouchKind.FolderMetadata,
            RestoreItemKind.FileData when isMetadata => VolumeTouchKind.FileMetadata,
            _ => VolumeTouchKind.FileData,
        };

    private static DateTime ParseRestoreTime(string time)
    {
        if (string.IsNullOrWhiteSpace(time))
            return DateTime.UtcNow;

        if (!DateTime.TryParse(time, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.RoundtripKind, out var parsed))
            throw new UserInformationException($"Unable to parse restore time '{time}'", "InvalidRestoreTime");

        return parsed;
    }

    private static Dictionary<string, int> CountKinds(IEnumerable<string?> kinds)
        => kinds
            .Select(kind => kind ?? "Unknown")
            .GroupBy(kind => kind)
            .OrderByDescending(group => group.Count())
            .ThenBy(group => group.Key)
            .ToDictionary(group => group.Key, group => group.Count());

    private static double ComputeFragmentationScore(IReadOnlyDictionary<long, long> volumeReferenceUsage, int totalReferences)
    {
        if (totalReferences <= 1 || volumeReferenceUsage.Count <= 1)
            return 0;

        double entropy = 0;
        foreach (var references in volumeReferenceUsage.Values)
        {
            double share = (double)references / totalReferences;
            entropy -= share * Math.Log(share);
        }

        double normalizedEntropy = entropy / Math.Log(volumeReferenceUsage.Count);
        double spreadFactor = (double)(volumeReferenceUsage.Count - 1) / volumeReferenceUsage.Count;
        return 100.0 * normalizedEntropy * spreadFactor;
    }

    private static double ComputeEffectiveVolumeCount(IReadOnlyDictionary<long, long> volumeReferenceUsage, int totalReferences)
    {
        if (totalReferences <= 0 || volumeReferenceUsage.Count == 0)
            return 0;

        double concentration = 0;
        foreach (var references in volumeReferenceUsage.Values)
        {
            double share = (double)references / totalReferences;
            concentration += share * share;
        }

        return concentration > 0 ? 1.0 / concentration : 0;
    }

    private static int Percentile(IEnumerable<double> values, double percentile)
        => (int)Math.Round(PercentileDouble(values, percentile), MidpointRounding.AwayFromZero);

    private static double PercentileDouble(IEnumerable<double> values, double percentile)
    {
        var ordered = values.OrderBy(value => value).ToArray();
        if (ordered.Length == 0)
            return 0;

        if (ordered.Length == 1)
            return ordered[0];

        double position = percentile * (ordered.Length - 1);
        int lowerIndex = (int)Math.Floor(position);
        int upperIndex = (int)Math.Ceiling(position);
        if (lowerIndex == upperIndex)
            return ordered[lowerIndex];

        double weight = position - lowerIndex;
        return ordered[lowerIndex] + ((ordered[upperIndex] - ordered[lowerIndex]) * weight);
    }

    private static AnalysisReport RedactReportPaths(AnalysisReport report)
        => report with
        {
            Paths = report.Paths.Select(RedactRequiredPath).ToArray(),
            Peak = report.Peak is null ? null : report.Peak with { Path = RedactRequiredPath(report.Peak.Path) },
            Phase1TopGrowthWindows = report.Phase1TopGrowthWindows.Select(window => window with
            {
                StartPath = RedactRequiredPath(window.StartPath),
                EndPath = RedactRequiredPath(window.EndPath),
                PeakPath = RedactRequiredPath(window.PeakPath),
            }).ToArray(),
            Phase1WindowXray = report.Phase1WindowXray.Select(window => window with
            {
                Window = window.Window with
                {
                    StartPath = RedactRequiredPath(window.Window.StartPath),
                    EndPath = RedactRequiredPath(window.Window.EndPath),
                    PeakPath = RedactRequiredPath(window.Window.PeakPath),
                },
                TopIntroducedVolumes = window.TopIntroducedVolumes.Select(RedactVolumePeakDetail).ToArray(),
            }).ToArray(),
            TopPrePhase2PinningVolumes = report.TopPrePhase2PinningVolumes.Select(RedactVolumePeakDetail).ToArray(),
            TopPinningVolumes = report.TopPinningVolumes.Select(RedactVolumePeakDetail).ToArray(),
            Fragmentation = report.Fragmentation with
            {
                Overall = report.Fragmentation.Overall with
                {
                    TopFragmentedItems = report.Fragmentation.Overall.TopFragmentedItems.Select(item => item with
                    {
                        Path = RedactRequiredPath(item.Path),
                    }).ToArray(),
                },
                SharedVolumeOnly = report.Fragmentation.SharedVolumeOnly with
                {
                    TopFragmentedItems = report.Fragmentation.SharedVolumeOnly.TopFragmentedItems.Select(item => item with
                    {
                        Path = RedactRequiredPath(item.Path),
                    }).ToArray(),
                },
            },
            DefragWhatIf = report.DefragWhatIf with
            {
                TopCandidates = report.DefragWhatIf.TopCandidates.Select(candidate => candidate with
                {
                    Path = RedactRequiredPath(candidate.Path),
                }).ToArray(),
            },
            PeakPressure = report.PeakPressure with
            {
                TopItems = report.PeakPressure.TopItems.Select(item => item with
                {
                    Path = RedactRequiredPath(item.Path),
                }).ToArray(),
            },
            OrderingComparison = report.OrderingComparison with
            {
                TopWindowsWherePlainBetter = report.OrderingComparison.TopWindowsWherePlainBetter.Select(window => window with
                {
                    CurrentPeakPath = RedactRequiredPath(window.CurrentPeakPath),
                    PlainPeakPath = RedactRequiredPath(window.PlainPeakPath),
                }).ToArray(),
                TopWindowsWherePhasedBetter = report.OrderingComparison.TopWindowsWherePhasedBetter.Select(window => window with
                {
                    CurrentPeakPath = RedactRequiredPath(window.CurrentPeakPath),
                    PlainPeakPath = RedactRequiredPath(window.PlainPeakPath),
                }).ToArray(),
            },
            BoundaryRegion = report.BoundaryRegion with
            {
                Segments = report.BoundaryRegion.Segments.Select(segment => segment with
                {
                    CurrentPeakPath = RedactRequiredPath(segment.CurrentPeakPath),
                    PlainPeakPath = RedactRequiredPath(segment.PlainPeakPath),
                }).ToArray(),
            },
            LatePhase1Tail = report.LatePhase1Tail with
            {
                Candidates = report.LatePhase1Tail.Candidates.Select(candidate => candidate with
                {
                    Path = RedactRequiredPath(candidate.Path),
                }).ToArray(),
            },
            InteractiveMap = report.InteractiveMap is null ? null : report.InteractiveMap with
            {
                Items = report.InteractiveMap.Items.Select(item => item with
                {
                    Path = RedactRequiredPath(item.Path),
                }).ToArray(),
                Volumes = report.InteractiveMap.Volumes.Select(volume => volume with
                {
                    Items = volume.Items.Select(item => item with
                    {
                        Path = RedactRequiredPath(item.Path),
                    }).ToArray(),
                }).ToArray(),
            },
        };

    private static void WriteInteractiveRestoreMapHtml(InteractiveRestoreMap map, string outputPath)
    {
        string json = JsonSerializer.Serialize(map, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        }).Replace("</", "<\\/");
        string? directory = Path.GetDirectoryName(outputPath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        var html = new StringBuilder();
        html.AppendLine("<!DOCTYPE html>");
        html.AppendLine("<html lang=\"en\">");
        html.AppendLine("<head>");
        html.AppendLine("<meta charset=\"utf-8\">");
        html.AppendLine("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">");
        html.AppendLine("<title>Restore Cache Map</title>");
        html.AppendLine("<style>");
        html.AppendLine("  :root {");
        html.AppendLine("    --bg: #f3efe6;");
        html.AppendLine("    --panel: rgba(255, 252, 246, 0.92);");
        html.AppendLine("    --ink: #1f2933;");
        html.AppendLine("    --muted: #52606d;");
        html.AppendLine("    --line: rgba(31, 41, 51, 0.15);");
        html.AppendLine("    --accent: #0f766e;");
        html.AppendLine("    --accent-2: #c2410c;");
        html.AppendLine("    --p1: #0f766e;");
        html.AppendLine("    --p2: #b45309;");
        html.AppendLine("    --p3: #475569;");
        html.AppendLine("    --meta: #7c3aed;");
        html.AppendLine("  }");
        html.AppendLine("  * { box-sizing: border-box; }");
        html.AppendLine("  body { margin: 0; font-family: \"Segoe UI\", \"Helvetica Neue\", sans-serif; color: var(--ink); background: radial-gradient(circle at top, #fffaf1 0, var(--bg) 55%, #e7e0d2 100%); }");
        html.AppendLine("  .shell { display: grid; grid-template-rows: auto auto 1fr; min-height: 100vh; }");
        html.AppendLine("  .topbar { position: sticky; top: 0; z-index: 10; backdrop-filter: blur(18px); background: rgba(243, 239, 230, 0.88); border-bottom: 1px solid var(--line); padding: 16px 22px; }");
        html.AppendLine("  .headline { display: flex; justify-content: space-between; gap: 16px; align-items: end; flex-wrap: wrap; }");
        html.AppendLine("  .headline h1 { margin: 0; font-size: 1.3rem; font-weight: 700; letter-spacing: 0.01em; }");
        html.AppendLine("  .headline .meta { color: var(--muted); font-size: 0.9rem; }");
        html.AppendLine("  .controls { display: flex; gap: 12px; flex-wrap: wrap; margin-top: 14px; align-items: center; }");
        html.AppendLine("  select, input, button { border: 1px solid var(--line); background: #fffdfa; color: var(--ink); border-radius: 10px; padding: 10px 12px; font: inherit; }");
        html.AppendLine("  button { cursor: pointer; }");
        html.AppendLine("  .summary { display: grid; grid-template-columns: repeat(4, minmax(150px, 1fr)); gap: 12px; padding: 16px 22px 8px; }");
        html.AppendLine("  .card { background: var(--panel); border: 1px solid var(--line); border-radius: 16px; padding: 14px 16px; box-shadow: 0 10px 30px rgba(15, 23, 42, 0.05); }");
        html.AppendLine("  .card .label { color: var(--muted); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.08em; }");
        html.AppendLine("  .card .value { margin-top: 6px; font-size: 1.4rem; font-weight: 700; }");
        html.AppendLine("  .main { display: grid; grid-template-columns: minmax(320px, 380px) 1fr; gap: 16px; padding: 8px 22px 22px; align-items: start; }");
        html.AppendLine("  .panel { background: var(--panel); border: 1px solid var(--line); border-radius: 18px; overflow: hidden; box-shadow: 0 18px 38px rgba(15, 23, 42, 0.06); }");
        html.AppendLine("  .detail { position: sticky; top: 150px; align-self: start; max-height: calc(100vh - 172px); display: grid; grid-template-rows: auto minmax(0, 1fr); }");
        html.AppendLine("  .panel-header { padding: 16px 18px 10px; border-bottom: 1px solid var(--line); }");
        html.AppendLine("  .panel-header h2 { margin: 0; font-size: 0.98rem; }");
        html.AppendLine("  .panel-body { padding: 16px 18px 18px; }");
        html.AppendLine("  .detail .panel-body { overflow: auto; min-height: 0; }");
        html.AppendLine("  #chart { width: 100%; height: 280px; display: block; background: linear-gradient(180deg, rgba(255,255,255,0.35), rgba(255,255,255,0.05)); }");
        html.AppendLine("  .legend { display: flex; gap: 14px; flex-wrap: wrap; color: var(--muted); font-size: 0.84rem; margin-top: 10px; }");
        html.AppendLine("  .legend span::before { content: \"\"; display: inline-block; width: 10px; height: 10px; border-radius: 999px; margin-right: 6px; vertical-align: middle; }");
        html.AppendLine("  .legend .active::before { background: var(--accent); }");
        html.AppendLine("  .legend .cached::before { background: var(--accent-2); }");
        html.AppendLine("  .legend .p1::before { background: var(--p1); }");
        html.AppendLine("  .legend .p2::before { background: var(--p2); }");
        html.AppendLine("  .legend .p3::before { background: var(--p3); }");
        html.AppendLine("  .legend .meta::before { background: var(--meta); }");
        html.AppendLine("  .detail-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px 14px; }");
        html.AppendLine("  .detail-grid .wide { grid-column: 1 / -1; }");
        html.AppendLine("  .kv .k { color: var(--muted); font-size: 0.76rem; text-transform: uppercase; letter-spacing: 0.07em; }");
        html.AppendLine("  .kv .v { margin-top: 4px; font-weight: 600; word-break: break-word; }");
        html.AppendLine("  .subsection { margin-top: 16px; padding-top: 14px; border-top: 1px solid var(--line); }");
        html.AppendLine("  .subsection h3 { margin: 0 0 10px; font-size: 0.86rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); }");
        html.AppendLine("  .pill-list { display: flex; flex-wrap: wrap; gap: 8px; max-height: 132px; overflow: auto; align-content: flex-start; padding-right: 4px; }");
        html.AppendLine("  .pill { border: 1px solid var(--line); background: #fffdfa; border-radius: 999px; padding: 6px 10px; font: inherit; cursor: pointer; }");
        html.AppendLine("  .pill small { color: var(--muted); margin-left: 6px; }");
        html.AppendLine("  .empty-note { color: var(--muted); font-size: 0.85rem; }");
        html.AppendLine("  .volume-inspector { display: grid; gap: 10px; max-height: 220px; overflow: auto; padding-right: 4px; }");
        html.AppendLine("  .inspector-actions { display: flex; gap: 8px; flex-wrap: wrap; }");
        html.AppendLine("  .usage-list { display: grid; gap: 8px; max-height: 260px; overflow: auto; padding-right: 4px; }");
        html.AppendLine("  .usage-item { border: 1px solid var(--line); border-radius: 12px; padding: 10px 12px; background: rgba(255,255,255,0.6); }");
        html.AppendLine("  .usage-head { display: flex; justify-content: space-between; gap: 10px; align-items: center; margin-bottom: 6px; }");
        html.AppendLine("  .usage-stats { color: var(--muted); font-size: 0.8rem; display: flex; gap: 10px; flex-wrap: wrap; }");
        html.AppendLine("  .list-shell { display: grid; grid-template-rows: auto 1fr; min-height: 70vh; }");
        html.AppendLine("  .list-toolbar { display: flex; justify-content: space-between; gap: 12px; align-items: center; padding: 14px 18px; border-bottom: 1px solid var(--line); }");
        html.AppendLine("  .list-toolbar .status { color: var(--muted); font-size: 0.88rem; }");
        html.AppendLine("  .viewport { position: relative; height: 72vh; overflow: auto; background: linear-gradient(180deg, rgba(255,255,255,0.32), rgba(255,255,255,0.1)); }");
        html.AppendLine("  .row-layer { position: relative; }");
        html.AppendLine("  .row { position: absolute; left: 0; right: 0; display: grid; grid-template-columns: 148px 76px minmax(320px, 1fr) 92px 92px 116px 132px; gap: 10px; align-items: center; height: 26px; padding: 0 14px; border-bottom: 1px solid rgba(31, 41, 51, 0.05); font-size: 0.82rem; }");
        html.AppendLine("  .row.current { background: rgba(15, 118, 110, 0.12); }");
        html.AppendLine("  .row:hover { background: rgba(15, 118, 110, 0.08); }");
        html.AppendLine("  .phase-tag { display: inline-flex; align-items: center; justify-content: center; border-radius: 999px; padding: 2px 8px; font-size: 0.72rem; font-weight: 700; color: white; }");
        html.AppendLine("  .phase-tag.Phase1 { background: var(--p1); }");
        html.AppendLine("  .phase-tag.Phase2 { background: var(--p2); }");
        html.AppendLine("  .phase-tag.Phase3 { background: var(--p3); }");
        html.AppendLine("  .phase-tag.FolderMetadata { background: var(--meta); }");
        html.AppendLine("  .mono { font-variant-numeric: tabular-nums; font-feature-settings: \"tnum\" 1; }");
        html.AppendLine("  .path { overflow: hidden; white-space: nowrap; text-overflow: ellipsis; }");
        html.AppendLine("  @media (max-width: 1100px) { .summary { grid-template-columns: repeat(2, minmax(150px, 1fr)); } .main { grid-template-columns: 1fr; } .detail { position: static; } }");
        html.AppendLine("  @media (max-width: 720px) { .summary { grid-template-columns: 1fr; } .row { grid-template-columns: 116px 66px minmax(160px, 1fr) 74px 74px 86px 96px; font-size: 0.75rem; } }");
        html.AppendLine("</style>");
        html.AppendLine("</head>");
        html.AppendLine("<body>");
        html.AppendLine("<div class=\"shell\">");
        html.AppendLine("  <div class=\"topbar\">");
        html.AppendLine("    <div class=\"headline\">");
        html.AppendLine("      <div>");
        html.AppendLine("        <h1>Restore Cache Activity Map</h1>");
        html.AppendLine("        <div class=\"meta\" id=\"subtitle\"></div>");
        html.AppendLine("      </div>");
        html.AppendLine("      <div class=\"meta\" id=\"strategySummary\"></div>");
        html.AppendLine("    </div>");
        html.AppendLine("    <div class=\"controls\">");
        html.AppendLine("      <label>Strategy <select id=\"strategySelect\"></select></label>");
        html.AppendLine("      <label>Jump to item <input id=\"jumpInput\" type=\"number\" min=\"1\" step=\"1\"></label>");
        html.AppendLine("      <button id=\"jumpButton\" type=\"button\">Go</button>");
        html.AppendLine("      <button id=\"peakButton\" type=\"button\">Go to peak</button>");
        html.AppendLine("      <label>Volume <input id=\"volumeInput\" type=\"number\" min=\"1\" step=\"1\"></label>");
        html.AppendLine("      <button id=\"volumeButton\" type=\"button\">Inspect volume</button>");
        html.AppendLine("    </div>");
        html.AppendLine("  </div>");
        html.AppendLine("  <div class=\"summary\">");
        html.AppendLine("    <div class=\"card\"><div class=\"label\">Restore Items</div><div class=\"value mono\" id=\"totalItems\"></div></div>");
        html.AppendLine("    <div class=\"card\"><div class=\"label\">Current Peak Active</div><div class=\"value mono\" id=\"currentPeak\"></div></div>");
        html.AppendLine("    <div class=\"card\"><div class=\"label\">Strategy Peak</div><div class=\"value mono\" id=\"strategyPeak\"></div></div>");
        html.AppendLine("    <div class=\"card\"><div class=\"label\">Shared Threshold</div><div class=\"value mono\" id=\"sharedThreshold\"></div></div>");
        html.AppendLine("  </div>");
        html.AppendLine("  <div class=\"main\">");
        html.AppendLine("    <section class=\"panel detail\">");
        html.AppendLine("      <div class=\"panel-header\"><h2>Current Marker</h2></div>");
        html.AppendLine("      <div class=\"panel-body\">");
        html.AppendLine("        <div class=\"detail-grid\" id=\"detailGrid\"></div>");
        html.AppendLine("        <div class=\"subsection\"><h3>Active Volumes</h3><div id=\"activeVolumes\" class=\"pill-list\"></div></div>");
        html.AppendLine("        <div class=\"subsection\"><h3>Introduced Volumes</h3><div id=\"introducedVolumes\" class=\"pill-list\"></div></div>");
        html.AppendLine("        <div class=\"subsection\"><h3>Drained Volumes</h3><div id=\"drainedVolumes\" class=\"pill-list\"></div></div>");
        html.AppendLine("        <div class=\"subsection\"><h3>Volume Inspector</h3><div id=\"volumeInspector\" class=\"volume-inspector\"></div></div>");
        html.AppendLine("        <div class=\"subsection\"><h3>Volume-Associated Files</h3><div id=\"volumeUsageList\" class=\"usage-list\"></div></div>");
        html.AppendLine("      </div>");
        html.AppendLine("    </section>");
        html.AppendLine("    <section class=\"panel list-shell\">");
        html.AppendLine("      <div>");
        html.AppendLine("        <canvas id=\"chart\"></canvas>");
        html.AppendLine("        <div class=\"panel-body\">");
        html.AppendLine("          <div class=\"legend\">");
        html.AppendLine("            <span class=\"active\">Active volumes</span>");
        html.AppendLine("            <span class=\"cached\">Cached volumes</span>");
        html.AppendLine("            <span class=\"p1\">Phase 1</span>");
        html.AppendLine("            <span class=\"p2\">Phase 2</span>");
        html.AppendLine("            <span class=\"p3\">Phase 3</span>");
        html.AppendLine("            <span class=\"meta\">Folder metadata</span>");
        html.AppendLine("          </div>");
        html.AppendLine("        </div>");
        html.AppendLine("      </div>");
        html.AppendLine("      <div class=\"list-toolbar\"><div class=\"status\" id=\"positionStatus\"></div><div class=\"status\">Scroll the run, or click the chart to jump.</div></div>");
        html.AppendLine("      <div class=\"viewport\" id=\"viewport\"><div class=\"row-layer\" id=\"rowLayer\"></div></div>");
        html.AppendLine("    </section>");
        html.AppendLine("  </div>");
        html.AppendLine("</div>");
        html.AppendLine("<script id=\"restore-map-data\" type=\"application/json\">");
        html.AppendLine(json);
        html.AppendLine("</script>");
        html.AppendLine("<script>");
        html.AppendLine("const data = JSON.parse(document.getElementById('restore-map-data').textContent);");
        html.AppendLine("const rowHeight = 26;");
        html.AppendLine("const overscan = 24;");
        html.AppendLine("const viewport = document.getElementById('viewport');");
        html.AppendLine("const rowLayer = document.getElementById('rowLayer');");
        html.AppendLine("const chart = document.getElementById('chart');");
        html.AppendLine("const strategySelect = document.getElementById('strategySelect');");
        html.AppendLine("const jumpInput = document.getElementById('jumpInput');");
        html.AppendLine("const volumeInput = document.getElementById('volumeInput');");
        html.AppendLine("const detailGrid = document.getElementById('detailGrid');");
        html.AppendLine("const activeVolumes = document.getElementById('activeVolumes');");
        html.AppendLine("const introducedVolumes = document.getElementById('introducedVolumes');");
        html.AppendLine("const drainedVolumes = document.getElementById('drainedVolumes');");
        html.AppendLine("const volumeInspector = document.getElementById('volumeInspector');");
        html.AppendLine("const volumeUsageList = document.getElementById('volumeUsageList');");
        html.AppendLine("const volumesById = new Map(data.volumes.map(volume => [volume.volumeId, volume]));");
        html.AppendLine("let strategyIndex = 0;");
        html.AppendLine("let currentPosition = 0;");
        html.AppendLine("let selectedVolumeId = null;");
        html.AppendLine("let strategyPositions = new Map();");
        html.AppendLine("for (const strategy of data.strategies) { const option = document.createElement('option'); option.value = strategy.key; option.textContent = strategy.name; strategySelect.appendChild(option); }");
        html.AppendLine("function getStrategy() { return data.strategies[strategyIndex]; }");
        html.AppendLine("function refreshStrategyPositions() { strategyPositions = new Map(getStrategy().order.map((itemIndex, position) => [itemIndex, position])); }");
        html.AppendLine("function positionForItemIndex(itemIndex) { return strategyPositions.get(itemIndex) ?? -1; }");
        html.AppendLine("function itemFor(position) { const strategy = getStrategy(); const itemIndex = strategy.order[position] - 1; return data.items[itemIndex]; }");
        html.AppendLine("function phaseColor(phase) { if (phase === 'Phase1') return getComputedStyle(document.documentElement).getPropertyValue('--p1').trim(); if (phase === 'Phase2') return getComputedStyle(document.documentElement).getPropertyValue('--p2').trim(); if (phase === 'Phase3') return getComputedStyle(document.documentElement).getPropertyValue('--p3').trim(); return getComputedStyle(document.documentElement).getPropertyValue('--meta').trim(); }");
        html.AppendLine("function formatBytes(value) { const units = ['B','KiB','MiB','GiB','TiB']; let size = Number(value); let idx = 0; while (size >= 1024 && idx < units.length - 1) { size /= 1024; idx += 1; } return (idx === 0 ? size.toFixed(0) : size.toFixed(size >= 10 ? 1 : 2)) + ' ' + units[idx]; }");
        html.AppendLine("function formatDelta(value) { return value > 0 ? '+' + value : String(value); }");
        html.AppendLine("function escapeHtml(value) { return String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/\"/g, '&quot;'); }");
        html.AppendLine("function currentItemIndex() { return getStrategy().order[currentPosition]; }");
        html.AppendLine("function activeVolumeIdsForCurrentPosition() { const strategy = getStrategy(); const itemIndex = currentItemIndex(); const result = []; for (const volume of data.volumes) { const introducedAt = strategy.volumeIntroducedAtItem[String(volume.volumeId)] ?? strategy.volumeIntroducedAtItem[volume.volumeId]; const drainedAt = strategy.volumeDrainedAtItem[String(volume.volumeId)] ?? strategy.volumeDrainedAtItem[volume.volumeId]; if (introducedAt != null && introducedAt <= itemIndex && (drainedAt == null || drainedAt > itemIndex)) result.push(volume.volumeId); } return result; }");
        html.AppendLine("function renderSummary() { const strategy = getStrategy(); document.getElementById('subtitle').textContent = data.database + ' | restore time ' + data.restoreTimeUtc; document.getElementById('strategySummary').textContent = strategy.name + ' | peak ' + strategy.predictedPeakActiveVolumes + ' (' + formatDelta(strategy.deltaFromCurrent) + ' vs current)'; document.getElementById('totalItems').textContent = data.totalItems.toLocaleString(); document.getElementById('currentPeak').textContent = data.currentPeakActiveVolumes.toLocaleString(); document.getElementById('strategyPeak').textContent = strategy.predictedPeakActiveVolumes.toLocaleString() + ' (' + formatDelta(strategy.deltaFromCurrent) + ')'; document.getElementById('sharedThreshold').textContent = String(data.sharedBlockThreshold); jumpInput.max = strategy.order.length; }");
        html.AppendLine("function renderVolumePills(container, volumeIds) { const strategy = getStrategy(); container.innerHTML = ''; if (!volumeIds || volumeIds.length === 0) { container.innerHTML = '<div class=\"empty-note\">None for this item.</div>'; return; } for (const volumeId of volumeIds) { const volume = volumesById.get(volumeId); const button = document.createElement('button'); button.type = 'button'; button.className = 'pill mono'; const drainedAt = strategy.volumeDrainedAtItem[String(volumeId)] ?? strategy.volumeDrainedAtItem[volumeId]; button.innerHTML = volume ? '#' + volumeId + ' <small>' + escapeHtml(volume.name || 'volume') + ' | release ' + (drainedAt ?? '-') + '</small>' : '#' + volumeId; button.addEventListener('click', () => { selectedVolumeId = volumeId; volumeInput.value = volumeId; renderVolumeInspector(); }); container.appendChild(button); } }");
        html.AppendLine("function renderVolumeInspector() { const strategy = getStrategy(); if (selectedVolumeId == null) { volumeInspector.innerHTML = '<div class=\"empty-note\">Choose a volume from the current item or type a volume id above.</div>'; volumeUsageList.innerHTML = '<div class=\"empty-note\">Choose a volume to see associated files.</div>'; return; } const volume = volumesById.get(selectedVolumeId); if (!volume) { volumeInspector.innerHTML = '<div class=\"empty-note\">Volume ' + selectedVolumeId + ' is not present in this dataset.</div>'; volumeUsageList.innerHTML = '<div class=\"empty-note\">No associated-file data found.</div>'; return; } const introducedAt = strategy.volumeIntroducedAtItem[String(selectedVolumeId)] ?? strategy.volumeIntroducedAtItem[selectedVolumeId]; const drainedAt = strategy.volumeDrainedAtItem[String(selectedVolumeId)] ?? strategy.volumeDrainedAtItem[selectedVolumeId]; const itemIndex = currentItemIndex(); const isActiveNow = introducedAt != null && introducedAt <= itemIndex && (drainedAt == null || drainedAt > itemIndex); volumeInspector.innerHTML = '<div class=\"detail-grid\">' + '<div class=\"kv\"><div class=\"k\">Volume id</div><div class=\"v mono\">' + volume.volumeId + '</div></div>' + '<div class=\"kv\"><div class=\"k\">Operation id</div><div class=\"v mono\">' + volume.operationId + '</div></div>' + '<div class=\"kv wide\"><div class=\"k\">Name</div><div class=\"v\">' + escapeHtml(volume.name || '(unnamed)') + '</div></div>' + '<div class=\"kv\"><div class=\"k\">Size</div><div class=\"v mono\">' + formatBytes(volume.size) + '</div></div>' + '<div class=\"kv\"><div class=\"k\">Initial refs</div><div class=\"v mono\">' + Number(volume.initialReferenceCount).toLocaleString() + '</div></div>' + '<div class=\"kv\"><div class=\"k\">Shared ref score</div><div class=\"v mono\">' + Number(volume.sharedRefScore).toLocaleString() + '</div></div>' + '<div class=\"kv\"><div class=\"k\">State</div><div class=\"v\">' + escapeHtml(volume.state || '') + '</div></div>' + '<div class=\"kv\"><div class=\"k\">Introduced at item</div><div class=\"v mono\">' + (introducedAt ?? '-') + '</div></div>' + '<div class=\"kv\"><div class=\"k\">Drained at item</div><div class=\"v mono\">' + (drainedAt ?? '-') + '</div></div>' + '<div class=\"kv\"><div class=\"k\">Active now</div><div class=\"v\">' + (isActiveNow ? 'Yes' : 'No') + '</div></div>' + '<div class=\"kv\"><div class=\"k\">Associated items</div><div class=\"v mono\">' + volume.items.length.toLocaleString() + '</div></div>' + '</div>' + '<div class=\"inspector-actions\">' + (introducedAt ? '<button type=\"button\" id=\"jumpIntro\">Jump to intro item</button>' : '') + (drainedAt ? '<button type=\"button\" id=\"jumpDrain\">Jump to drain item</button>' : '') + '</div>'; const introButton = document.getElementById('jumpIntro'); if (introButton) introButton.addEventListener('click', () => { const position = positionForItemIndex(introducedAt); if (position >= 0) setCurrentPosition(position, true); }); const drainButton = document.getElementById('jumpDrain'); if (drainButton) drainButton.addEventListener('click', () => { const position = positionForItemIndex(drainedAt); if (position >= 0) setCurrentPosition(position, true); }); volumeUsageList.innerHTML = ''; for (const usage of volume.items) { const position = positionForItemIndex(usage.itemIndex); const button = document.createElement('button'); button.type = 'button'; button.className = 'usage-item'; button.innerHTML = '<div class=\"usage-head\"><span class=\"mono\">item ' + usage.itemIndex + (position >= 0 ? ' | pos ' + (position + 1).toLocaleString() : '') + '</span><span class=\"phase-tag ' + usage.sqlPhase + '\">' + usage.sqlPhase.replace('FolderMetadata', 'Meta') + '</span></div>' + '<div class=\"path\">' + escapeHtml(usage.path) + '</div>' + '<div class=\"usage-stats\"><span>refs ' + usage.blockReferences + ' (' + usage.dataBlockReferences + ' data / ' + usage.metadataBlockReferences + ' meta)</span><span>bytes ' + formatBytes(Number(usage.dataBytes) + Number(usage.metadataBytes)) + '</span><span>shared refs ' + usage.sharedBlockReferences + '</span><span>shared bytes ' + formatBytes(Number(usage.sharedDataBytes) + Number(usage.sharedMetadataBytes)) + '</span></div>'; button.addEventListener('click', () => { if (position >= 0) setCurrentPosition(position, true); }); volumeUsageList.appendChild(button); } if (volume.items.length === 0) volumeUsageList.innerHTML = '<div class=\"empty-note\">No associated-file data found.</div>'; }");
        html.AppendLine("function renderDetails() { const strategy = getStrategy(); const item = itemFor(currentPosition); const activeAfter = strategy.activeAfter[currentPosition]; const activeBefore = currentPosition === 0 ? 0 : strategy.activeAfter[currentPosition - 1]; const cachedAfter = strategy.cachedAfter[currentPosition]; const cachedBefore = currentPosition === 0 ? 0 : strategy.cachedAfter[currentPosition - 1]; const introduced = strategy.introduced[currentPosition]; const drained = strategy.drained[currentPosition]; const retiredTotal = strategy.retiredTotal[currentPosition]; const introducedVolumeIds = strategy.introducedVolumeIds[currentPosition] || []; const drainedVolumeIds = strategy.drainedVolumeIds[currentPosition] || []; const activeVolumeIds = activeVolumeIdsForCurrentPosition(); const totalBytes = Number(item.dataBytes) + Number(item.metadataBytes); document.getElementById('positionStatus').textContent = 'Position ' + (currentPosition + 1).toLocaleString() + ' of ' + strategy.order.length.toLocaleString() + ' | item ' + item.itemIndex.toLocaleString(); const entries = [ ['Position', (currentPosition + 1).toLocaleString()], ['Item index', item.itemIndex.toLocaleString()], ['Phase', item.sqlPhase], ['Item kind', item.itemKind], ['Path', item.path, true], ['Total bytes', formatBytes(totalBytes)], ['Data bytes', formatBytes(item.dataBytes)], ['Metadata bytes', formatBytes(item.metadataBytes)], ['Block refs', Number(item.blockReferences).toLocaleString()], ['Data block refs', Number(item.dataBlockReferences).toLocaleString()], ['Metadata block refs', Number(item.metadataBlockReferences).toLocaleString()], ['Distinct volumes', item.distinctVolumes.toLocaleString()], ['Shared distinct volumes', Number(item.sharedDistinctVolumes).toLocaleString()], ['Shared data bytes', formatBytes(item.sharedDataBytes)], ['Shared metadata bytes', formatBytes(item.sharedMetadataBytes)], ['Shared block refs', item.sharedBlockReferences.toLocaleString()], ['Shared data refs', item.sharedDataBlockReferences.toLocaleString()], ['Shared metadata refs', item.sharedMetadataBlockReferences.toLocaleString()], ['Active volumes', activeBefore.toLocaleString() + ' -> ' + activeAfter.toLocaleString() + ' [' + activeVolumeIds.length + ' active]'], ['Cached volumes', cachedBefore.toLocaleString() + ' -> ' + cachedAfter.toLocaleString()], ['Introduced volumes', introduced.toLocaleString() + ' [' + introducedVolumeIds.join(', ') + ']'], ['Drained volumes', drained.toLocaleString() + ' [' + drainedVolumeIds.join(', ') + ']'], ['Retired volumes total', retiredTotal.toLocaleString()] ]; detailGrid.innerHTML = ''; for (const entry of entries) { const wrapper = document.createElement('div'); wrapper.className = 'kv' + (entry[2] ? ' wide' : ''); const k = document.createElement('div'); k.className = 'k'; k.textContent = entry[0]; const v = document.createElement('div'); v.className = 'v mono'; if (entry[0] === 'Phase') { const tag = document.createElement('span'); tag.className = 'phase-tag ' + item.sqlPhase; tag.textContent = item.sqlPhase; v.textContent = ''; v.appendChild(tag); } else { v.textContent = entry[1]; if (entry[2]) v.classList.remove('mono'); } wrapper.appendChild(k); wrapper.appendChild(v); detailGrid.appendChild(wrapper); } renderVolumePills(activeVolumes, activeVolumeIds); renderVolumePills(introducedVolumes, introducedVolumeIds); renderVolumePills(drainedVolumes, drainedVolumeIds); if (selectedVolumeId == null && activeVolumeIds.length > 0) { selectedVolumeId = activeVolumeIds[0]; volumeInput.value = selectedVolumeId; } renderVolumeInspector(); }");
        html.AppendLine("function renderRows() { const strategy = getStrategy(); const total = strategy.order.length; rowLayer.style.height = (total * rowHeight) + 'px'; const scrollTop = viewport.scrollTop; const start = Math.max(0, Math.floor(scrollTop / rowHeight) - overscan); const visible = Math.ceil(viewport.clientHeight / rowHeight) + (overscan * 2); const end = Math.min(total, start + visible); rowLayer.innerHTML = ''; for (let position = start; position < end; position += 1) { const item = itemFor(position); const row = document.createElement('button'); row.type = 'button'; row.className = 'row' + (position === currentPosition ? ' current' : ''); row.style.top = (position * rowHeight) + 'px'; const activeAfter = strategy.activeAfter[position]; const activeBefore = position === 0 ? 0 : strategy.activeAfter[position - 1]; const cachedAfter = strategy.cachedAfter[position]; const introduced = strategy.introduced[position]; const drained = strategy.drained[position]; row.innerHTML = '<div class=\"mono\">pos ' + (position + 1).toLocaleString() + ' | #' + item.itemIndex.toLocaleString() + '</div>' + '<div><span class=\"phase-tag ' + item.sqlPhase + '\">' + item.sqlPhase.replace('FolderMetadata', 'Meta') + '</span></div>' + '<div class=\"path\" title=\"' + escapeHtml(item.path) + '\">' + escapeHtml(item.path) + '</div>' + '<div class=\"mono\">' + formatBytes(item.dataBytes) + '</div>' + '<div class=\"mono\">' + activeAfter.toLocaleString() + '</div>' + '<div class=\"mono\">' + formatDelta(activeAfter - activeBefore) + ' / ' + cachedAfter.toLocaleString() + '</div>' + '<div class=\"mono\">+' + introduced.toLocaleString() + ' / -' + drained.toLocaleString() + '</div>'; row.addEventListener('click', () => setCurrentPosition(position, true)); rowLayer.appendChild(row); } }");
        html.AppendLine("function drawChart() { const strategy = getStrategy(); const rect = chart.getBoundingClientRect(); const dpr = window.devicePixelRatio || 1; chart.width = Math.max(1, Math.floor(rect.width * dpr)); chart.height = Math.max(1, Math.floor(rect.height * dpr)); const ctx = chart.getContext('2d'); ctx.setTransform(1, 0, 0, 1, 0, 0); ctx.scale(dpr, dpr); const width = rect.width; const height = rect.height; ctx.clearRect(0, 0, width, height); ctx.fillStyle = 'rgba(255,255,255,0.45)'; ctx.fillRect(0, 0, width, height); const peak = Math.max(strategy.predictedPeakActiveVolumes, strategy.predictedPeakCachedVolumes, 1); const stripHeight = 18; const graphTop = 12; const graphBottom = height - stripHeight - 14; const graphHeight = graphBottom - graphTop; const length = strategy.order.length; const step = length / Math.max(1, width); for (let x = 0; x < width; x += 1) { const start = Math.floor(x * step); const end = Math.min(length, Math.max(start + 1, Math.floor((x + 1) * step))); let activeMax = 0; let cachedMax = 0; const sampleItem = itemFor(Math.min(length - 1, start)); for (let i = start; i < end; i += 1) { activeMax = Math.max(activeMax, strategy.activeAfter[i]); cachedMax = Math.max(cachedMax, strategy.cachedAfter[i]); } const activeHeight = (activeMax / peak) * graphHeight; const cachedHeight = (cachedMax / peak) * graphHeight; ctx.fillStyle = 'rgba(15, 118, 110, 0.78)'; ctx.fillRect(x, graphBottom - activeHeight, 1, activeHeight); ctx.fillStyle = 'rgba(194, 65, 12, 0.42)'; ctx.fillRect(x, graphBottom - cachedHeight, 1, 2); ctx.fillStyle = phaseColor(sampleItem.sqlPhase); ctx.fillRect(x, height - stripHeight, 1, stripHeight - 4); } ctx.strokeStyle = 'rgba(31, 41, 51, 0.14)'; ctx.beginPath(); for (let y = 0; y <= 4; y += 1) { const py = graphTop + ((graphHeight / 4) * y); ctx.moveTo(0, py); ctx.lineTo(width, py); } ctx.stroke(); const markerX = length <= 1 ? 0 : (currentPosition / (length - 1)) * width; ctx.strokeStyle = '#111827'; ctx.lineWidth = 2; ctx.beginPath(); ctx.moveTo(markerX, 0); ctx.lineTo(markerX, height); ctx.stroke(); }");
        html.AppendLine("function syncScroll() { viewport.scrollTop = currentPosition * rowHeight; renderRows(); drawChart(); renderDetails(); }");
        html.AppendLine("function setCurrentPosition(position, updateScroll) { const strategy = getStrategy(); currentPosition = Math.max(0, Math.min(strategy.order.length - 1, position)); if (updateScroll) { viewport.scrollTop = currentPosition * rowHeight; } renderRows(); drawChart(); renderDetails(); jumpInput.value = currentPosition + 1; }");
        html.AppendLine("strategySelect.addEventListener('change', () => { strategyIndex = strategySelect.selectedIndex; refreshStrategyPositions(); const strategy = getStrategy(); currentPosition = Math.min(currentPosition, strategy.order.length - 1); renderSummary(); syncScroll(); });");
        html.AppendLine("viewport.addEventListener('scroll', () => { const strategy = getStrategy(); const nextPosition = Math.max(0, Math.min(strategy.order.length - 1, Math.floor(viewport.scrollTop / rowHeight))); if (nextPosition !== currentPosition) { currentPosition = nextPosition; drawChart(); renderDetails(); } renderRows(); });");
        html.AppendLine("chart.addEventListener('click', (event) => { const rect = chart.getBoundingClientRect(); const ratio = Math.max(0, Math.min(1, (event.clientX - rect.left) / Math.max(1, rect.width))); const strategy = getStrategy(); setCurrentPosition(Math.round(ratio * (strategy.order.length - 1)), true); });");
        html.AppendLine("document.getElementById('jumpButton').addEventListener('click', () => { const value = Number(jumpInput.value); if (Number.isFinite(value)) setCurrentPosition(value - 1, true); });");
        html.AppendLine("document.getElementById('peakButton').addEventListener('click', () => { const strategy = getStrategy(); const peakItemIndex = strategy.peakItemIndex; const position = positionForItemIndex(peakItemIndex); if (position >= 0) setCurrentPosition(position, true); });");
        html.AppendLine("document.getElementById('volumeButton').addEventListener('click', () => { const value = Number(volumeInput.value); if (Number.isFinite(value)) { selectedVolumeId = value; renderVolumeInspector(); } });");
        html.AppendLine("window.addEventListener('resize', () => { drawChart(); renderRows(); });");
        html.AppendLine("refreshStrategyPositions(); renderSummary(); setCurrentPosition(0, true);");
        html.AppendLine("</script>");
        html.AppendLine("</body>");
        html.AppendLine("</html>");

        File.WriteAllText(outputPath, html.ToString(), Encoding.UTF8);
    }

    private static VolumePeakDetail RedactVolumePeakDetail(VolumePeakDetail detail)
        => detail with
        {
            FirstTouchPath = RedactPath(detail.FirstTouchPath),
            ReleasePath = RedactPath(detail.ReleasePath),
        };

    private static string RedactRequiredPath(string path)
        => RedactPath(path) ?? string.Empty;

    private static string? RedactPath(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return path;

        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(path))).Substring(0, 12);
        var leaf = Path.GetFileName(path.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
        return string.IsNullOrWhiteSpace(leaf)
            ? $"<redacted:{hash}>"
            : $"<redacted:{hash}:{leaf}>";
    }

    private static void PrintReport(AnalysisReport report)
    {
        Console.WriteLine($"Database: {report.Database}");
        Console.WriteLine($"Restore time (UTC): {report.RestoreTimeUtc:O}");
        Console.WriteLine($"Versions: {(report.Versions.Count == 0 ? "<restore-time selection>" : string.Join(", ", report.Versions))}");
        Console.WriteLine($"Paths: {(report.Paths.Count == 0 ? "<entire restore set>" : string.Join(", ", report.Paths))}");
        Console.WriteLine($"Skip metadata: {report.SkipMetadata}");
        Console.WriteLine($"Restore cache hint: {report.RestoreVolumeCacheHint}");
        Console.WriteLine($"Effective volume cache budget: {report.EffectiveVolumeCacheBudget}");
        Console.WriteLine($"Shared-block threshold: requested={report.RequestedSharedBlockThreshold}, effective={report.EffectiveSharedBlockThreshold}");
        Console.WriteLine();

        Console.WriteLine("Summary:");
        Console.WriteLine($"  Restore items: {report.RestoreItems} ({report.FileItems} files/symlinks, {report.FolderMetadataItems} folder-metadata items)");
        Console.WriteLine($"  Block requests: {report.BlockRequests}");
        Console.WriteLine($"  Distinct blocks: {report.DistinctBlocks}");
        Console.WriteLine($"  Distinct volumes: {report.DistinctVolumes}");
        Console.WriteLine($"  Shared-block candidates: {report.SharedBlockCandidates}");
        Console.WriteLine($"  Shared blocks stored in simulation: {report.SharedBlocksStored}");
        Console.WriteLine($"  SharedBlockStore bytes written: {report.SharedBlockStoreBytesWritten}");
        Console.WriteLine($"  Predicted peak active volumes: {report.PredictedPeakActiveVolumes}");
        Console.WriteLine($"  Predicted peak cached volumes: {report.PredictedPeakCachedVolumes}");
        Console.WriteLine($"  Fragmentation score, overall (block-weighted): {FormatScore(report.Fragmentation.Overall.BlockWeightedScore)}");
        Console.WriteLine($"  Fragmentation score, shared-volume-only (block-weighted): {FormatScore(report.Fragmentation.SharedVolumeOnly.BlockWeightedScore)}");
        Console.WriteLine($"  Phase1-preserved tail size-desc peak: {report.DefragWhatIf.CurrentPhase1TailSizePeakActiveVolumes} ({FormatDelta(report.DefragWhatIf.CurrentPhase1TailSizePeakDelta)})");
        Console.WriteLine($"  Plain size-descending baseline peak: {report.DefragWhatIf.CurrentPlainSizePeakActiveVolumes} ({FormatDelta(report.DefragWhatIf.CurrentPlainSizePeakDelta)})");

        PrintFragmentationViewSummary(report.Fragmentation.Overall);
        PrintFragmentationViewSummary(report.Fragmentation.SharedVolumeOnly);
        PrintPeakPressureSummary(report.PeakPressure);
        PrintOrderingComparisonSummary(report.OrderingComparison);
        PrintBoundaryRelaxationSummary(report.BoundaryRelaxation);
        PrintConstrainedPrefixSummary(report.ConstrainedPrefixes);
        PrintAdaptiveInterleavingSummary(report.AdaptiveInterleaving);
        PrintPhase1SizeThresholdSweepSummary(report.Phase1SizeThresholdSweep);
        PrintRuntimeCutoverRuleSummary(report.RuntimeCutoverRules);
        PrintLatePhase1TailCompositionSummary(report.LatePhase1TailComposition);
        PrintLatePhase1TailSummary(report.LatePhase1Tail);
        PrintBoundaryRegionSummary(report.BoundaryRegion);
        PrintDefragWhatIfSummary(report.DefragWhatIf);

        if (report.Peak != null)
        {
            Console.WriteLine();
            Console.WriteLine("Peak:");
            Console.WriteLine($"  Item index: {report.Peak.ItemIndex}");
            Console.WriteLine($"  Path: {report.Peak.Path}");
            Console.WriteLine($"  Block: {report.Peak.BlockId} from volume {report.Peak.VolumeId} {(report.Peak.IsMetadata ? "(metadata)" : "(data)")}");
            Console.WriteLine($"  Event index: {report.Peak.EventIndex}");
            Console.WriteLine($"  Active volumes: {report.Peak.ActiveVolumeCount}");
            Console.WriteLine($"  Cached volumes: {report.Peak.CachedVolumeCount}");
            if (report.PeakActiveVolumesByFirstTouchKind.Count > 0)
                Console.WriteLine($"  First-touch kinds at peak: {string.Join(", ", report.PeakActiveVolumesByFirstTouchKind.Select(x => $"{x.Key}={x.Value}"))}");
            if (report.PeakActiveVolumesByReleaseKind.Count > 0)
                Console.WriteLine($"  Release kinds for peak volumes: {string.Join(", ", report.PeakActiveVolumesByReleaseKind.Select(x => $"{x.Key}={x.Value}"))}");
        }

        if (report.PhaseSnapshots.Count > 0)
        {
            Console.WriteLine();
            Console.WriteLine("Phase snapshots:");
            foreach (var snapshot in report.PhaseSnapshots)
                Console.WriteLine($"  {snapshot.Name}: item={snapshot.ItemIndex}, active={snapshot.ActiveVolumeCount}, cached={snapshot.CachedVolumeCount}");
        }

        if (report.Phase1TopGrowthWindows.Count > 0)
        {
            Console.WriteLine();
            Console.WriteLine("Top Phase 1 growth windows:");
            foreach (var window in report.Phase1TopGrowthWindows)
            {
                Console.WriteLine($"  items {window.StartItemIndex}-{window.EndItemIndex} ({window.ItemCount} items): active {window.StartActiveVolumeCount}->{window.EndActiveVolumeCount} ({FormatDelta(window.NetActiveVolumeDelta)}), peak={window.PeakActiveVolumeCount} at item {window.PeakItemIndex}");
                Console.WriteLine($"    start={window.StartPath}");
                Console.WriteLine($"    peak={window.PeakPath}");
                Console.WriteLine($"    end={window.EndPath}");
            }
        }

        if (report.Phase1WindowXray.Count > 0)
        {
            Console.WriteLine();
            Console.WriteLine("Phase 1 window xray:");
            foreach (var window in report.Phase1WindowXray)
            {
                Console.WriteLine($"  items {window.Window.StartItemIndex}-{window.Window.EndItemIndex}: introduced={window.IntroducedVolumeCount}, survivingAtPeak={window.PeakSurvivingVolumeCount}");
                foreach (var volume in window.TopIntroducedVolumes)
                {
                    Console.WriteLine($"    {volume.VolumeId}: {volume.Name}");
                    Console.WriteLine($"      peakRemaining={volume.RemainingReferencesAtPeak}, initial={volume.InitialReferences}, sharedRefScore={volume.SharedRefScore}, touchedFiles={volume.DistinctTouchedFiles}");
                    Console.WriteLine($"      createdOp={volume.OperationId}, createdAtUtc={FormatDateTime(volume.CreatedAtUtc)}, ageAtRestoreDays={FormatDays(volume.AgeAtRestoreDays)}, archivedAtUtc={FormatDateTime(volume.ArchiveTimeUtc)}, state={volume.State}");
                    Console.WriteLine($"      first-touch=item {volume.FirstTouchItemIndex} kind={volume.FirstTouchKind} path={volume.FirstTouchPath}");
                    Console.WriteLine($"      release=item {volume.ReleaseItemIndex} kind={volume.ReleaseKind} path={volume.ReleasePath}");
                }
            }
        }

        if (report.TopPinningVolumes.Count > 0)
        {
            Console.WriteLine();
            Console.WriteLine("Top pinning volumes at peak:");
            foreach (var volume in report.TopPinningVolumes)
            {
                Console.WriteLine($"  {volume.VolumeId}: {volume.Name}");
                Console.WriteLine($"    remaining={volume.RemainingReferencesAtPeak}, initial={volume.InitialReferences}, sharedRefScore={volume.SharedRefScore}, touchedFiles={volume.DistinctTouchedFiles}, size={volume.Size}");
                Console.WriteLine($"    createdOp={volume.OperationId}, createdAtUtc={FormatDateTime(volume.CreatedAtUtc)}, ageAtRestoreDays={FormatDays(volume.AgeAtRestoreDays)}, archivedAtUtc={FormatDateTime(volume.ArchiveTimeUtc)}, state={volume.State}");
                Console.WriteLine($"    first-touch=item {volume.FirstTouchItemIndex} kind={volume.FirstTouchKind} path={volume.FirstTouchPath}");
                Console.WriteLine($"    release=item {volume.ReleaseItemIndex} kind={volume.ReleaseKind} path={volume.ReleasePath}");
            }
        }

        PrintTopFragmentedItems(report.Fragmentation.Overall, "Top fragmented restore items (overall):");
        PrintTopFragmentedItems(report.Fragmentation.SharedVolumeOnly, "Top fragmented restore items (shared-volume-only):");

        if (report.PeakVolumesIntroducedBeforePhase2 > 0)
        {
            Console.WriteLine();
            Console.WriteLine($"Peak volumes introduced before Phase 2: {report.PeakVolumesIntroducedBeforePhase2}/{report.Peak?.ActiveVolumeCount ?? 0}");
            foreach (var volume in report.TopPrePhase2PinningVolumes)
            {
                Console.WriteLine($"  {volume.VolumeId}: {volume.Name}");
                Console.WriteLine($"    remaining={volume.RemainingReferencesAtPeak}, initial={volume.InitialReferences}, sharedRefScore={volume.SharedRefScore}, touchedFiles={volume.DistinctTouchedFiles}, size={volume.Size}");
                Console.WriteLine($"    createdOp={volume.OperationId}, createdAtUtc={FormatDateTime(volume.CreatedAtUtc)}, ageAtRestoreDays={FormatDays(volume.AgeAtRestoreDays)}, archivedAtUtc={FormatDateTime(volume.ArchiveTimeUtc)}, state={volume.State}");
                Console.WriteLine($"    first-touch=item {volume.FirstTouchItemIndex} kind={volume.FirstTouchKind} path={volume.FirstTouchPath}");
                Console.WriteLine($"    release=item {volume.ReleaseItemIndex} kind={volume.ReleaseKind} path={volume.ReleasePath}");
            }
        }

        if (report.Phase2HeuristicComparisons.Count > 0)
        {
            Console.WriteLine();
            Console.WriteLine("Phase 2 heuristic comparisons:");
            foreach (var comparison in report.Phase2HeuristicComparisons)
            {
                Console.WriteLine($"  {comparison.Name}: active={comparison.PredictedPeakActiveVolumes} ({FormatDelta(comparison.ActiveVolumeDeltaFromCurrent)}), cached={comparison.PredictedPeakCachedVolumes} ({FormatDelta(comparison.CachedVolumeDeltaFromCurrent)})");
            }
        }
        else if (!string.IsNullOrWhiteSpace(report.Phase2HeuristicComparisonStatus))
        {
            Console.WriteLine();
            Console.WriteLine($"Phase 2 heuristic comparisons: {report.Phase2HeuristicComparisonStatus}");
        }

        Console.WriteLine();
        Console.WriteLine("Assumptions:");
        foreach (var assumption in report.Assumptions)
            Console.WriteLine($"  - {assumption}");
    }

    private static string FormatDelta(int delta)
        => delta > 0 ? $"+{delta}" : delta.ToString(CultureInfo.InvariantCulture);

    private static string FormatScore(double score)
        => score.ToString("0.0", CultureInfo.InvariantCulture) + "/100";

    private static void PrintFragmentationViewSummary(FragmentationViewSummary summary)
    {
        if (summary.ItemsWithBlocks <= 0)
            return;

        Console.WriteLine();
        Console.WriteLine($"Fragmentation summary ({summary.Name}):");
        Console.WriteLine($"  Items with blocks: {summary.ItemsWithBlocks}");
        Console.WriteLine($"  Multi-volume items: {summary.MultiVolumeItems} ({summary.MultiVolumeItemPercentage.ToString("0.0", CultureInfo.InvariantCulture)}%)");
        Console.WriteLine($"  Block-weighted score: {FormatScore(summary.BlockWeightedScore)}");
        Console.WriteLine($"  Item-weighted score: {FormatScore(summary.ItemWeightedScore)}");
        Console.WriteLine($"  Distinct volumes per item: avg={summary.AverageDistinctVolumes.ToString("0.00", CultureInfo.InvariantCulture)}, p50={summary.MedianDistinctVolumes}, p95={summary.P95DistinctVolumes}");
        Console.WriteLine($"  Fragmentation score per item: avg={FormatScore(summary.AverageFragmentationScore)}, p50={FormatScore(summary.MedianFragmentationScore)}, p95={FormatScore(summary.P95FragmentationScore)}");
        Console.WriteLine($"  Effective volumes per item: avg={summary.AverageEffectiveVolumeCount.ToString("0.00", CultureInfo.InvariantCulture)}, p95={summary.P95EffectiveVolumeCount.ToString("0.00", CultureInfo.InvariantCulture)}");
    }

    private static void PrintTopFragmentedItems(FragmentationViewSummary summary, string title)
    {
        if (summary.TopFragmentedItems.Count <= 0)
            return;

        Console.WriteLine();
        Console.WriteLine(title);
        foreach (var item in summary.TopFragmentedItems)
        {
            Console.WriteLine($"  item {item.ItemIndex}: {item.Path}");
            Console.WriteLine($"    kind={item.ItemKind}, phase={item.SqlPhase}, score={FormatScore(item.FragmentationScore)}, effectiveVolumes={item.EffectiveVolumeCount.ToString("0.00", CultureInfo.InvariantCulture)}");
            Console.WriteLine($"    blockRefs={item.BlockReferences} (data={item.DataBlockReferences}, metadata={item.MetadataBlockReferences}), distinctVolumes={item.DistinctVolumes}");
            Console.WriteLine($"    largestVolumeShare={item.LargestVolumeShare.ToString("0.000", CultureInfo.InvariantCulture)}, largestVolumeRefs={item.LargestVolumeReferences}");
        }
    }

    private static void PrintDefragWhatIfSummary(DefragWhatIfSummary summary)
    {
        if (summary.CandidateCountSimulated <= 0)
            return;

        Console.WriteLine();
        Console.WriteLine("Defrag what-if:");
        Console.WriteLine($"  Current phased peak: {summary.CurrentPhasedPeakActiveVolumes}");
        Console.WriteLine($"  Current Phase1-preserved tail size-desc peak: {summary.CurrentPhase1TailSizePeakActiveVolumes} ({FormatDelta(summary.CurrentPhase1TailSizePeakDelta)} vs phased)");
        Console.WriteLine($"  Current plain size-desc peak: {summary.CurrentPlainSizePeakActiveVolumes} ({FormatDelta(summary.CurrentPlainSizePeakDelta)} vs phased)");
        Console.WriteLine($"  Candidates simulated: {summary.CandidateCountSimulated} of {summary.PeakEligibleCandidateCount} peak-overlapping file items (capped at {MAX_DEFRAG_CANDIDATE_SIMULATIONS})");

        if (summary.TopCandidates.Count > 0)
        {
            Console.WriteLine();
            Console.WriteLine("Top defrag candidates by simulated peak reduction:");
            foreach (var candidate in summary.TopCandidates)
            {
                Console.WriteLine($"  item {candidate.ItemIndex}: {candidate.Path}");
                Console.WriteLine($"    phase={candidate.SqlPhase}, sharedScore={FormatScore(candidate.SharedFragmentationScore)}, sharedEffectiveVolumes={candidate.SharedEffectiveVolumeCount.ToString("0.00", CultureInfo.InvariantCulture)}");
                Console.WriteLine($"    sharedRefs={candidate.SharedBlockReferences} (data={candidate.SharedDataBlockReferences}), sharedBytes={FormatBytes(candidate.SharedDataBytes)}, sharedVolumes={candidate.SharedDistinctVolumes}, estPrivateVolumes={candidate.EstimatedPrivateVolumesAfterDefrag}");
                Console.WriteLine($"    peakSharedOverlap={candidate.PeakSharedVolumeOverlap}, peakSharedRemainingRefs={candidate.PeakSharedRemainingReferencesSum}, peakPressure={candidate.PeakPressureScore.ToString("0.0", CultureInfo.InvariantCulture)}");
                Console.WriteLine($"    phasedPeakAfter={candidate.PredictedPeakActiveVolumesAfterDefrag} ({FormatDelta(candidate.PredictedPeakActiveVolumeDelta)}), phase1TailSizePeakAfter={candidate.PredictedPhase1TailSizePeakAfterDefrag} ({FormatDelta(candidate.PredictedPhase1TailSizePeakDelta)}), plainSizePeakAfter={candidate.PredictedPlainSizePeakAfterDefrag} ({FormatDelta(candidate.PredictedPlainSizePeakDelta)})");
            }
        }

        if (summary.IncrementalScenarios.Count > 0)
        {
            Console.WriteLine();
            Console.WriteLine("Incremental defrag scenarios:");
            foreach (var scenario in summary.IncrementalScenarios)
            {
                Console.WriteLine($"  {scenario.Name}: phasedPeakAfter={scenario.PredictedPeakActiveVolumesAfterDefrag} ({FormatDelta(scenario.PredictedPeakActiveVolumeDelta)}), phase1TailSizePeakAfter={scenario.PredictedPhase1TailSizePeakAfterDefrag} ({FormatDelta(scenario.PredictedPhase1TailSizePeakDelta)}), plainSizePeakAfter={scenario.PredictedPlainSizePeakAfterDefrag} ({FormatDelta(scenario.PredictedPlainSizePeakDelta)})");
            }
        }
    }

    private static void PrintPeakPressureSummary(PeakPressureSummary summary)
    {
        if (summary.TopItems.Count <= 0)
            return;

        Console.WriteLine();
        Console.WriteLine("Top peak-pressure files:");
        foreach (var item in summary.TopItems)
        {
            Console.WriteLine($"  item {item.ItemIndex}: {item.Path}");
            Console.WriteLine($"    phase={item.SqlPhase}, sharedBytes={FormatBytes(item.SharedDataBytes)}, sharedPeakVolumes={item.PeakSharedVolumeOverlap}/{item.PeakActiveVolumeOverlap}");
            Console.WriteLine($"    peakRemainingRefs={item.PeakRemainingReferencesSum}, peakSharedRemainingRefs={item.PeakSharedRemainingReferencesSum}, peakPressure={item.PeakPressureScore.ToString("0.0", CultureInfo.InvariantCulture)}");
        }
    }

    private static void PrintOrderingComparisonSummary(OrderingComparisonSummary summary)
    {
        if (summary.TopWindowsWherePlainBetter.Count <= 0 && summary.TopWindowsWherePhasedBetter.Count <= 0)
            return;

        Console.WriteLine();
        Console.WriteLine($"Ordering comparison (window={summary.WindowSize} items):");

        if (summary.Baselines.Count > 0)
        {
            Console.WriteLine("  Ordering baselines:");
            foreach (var baseline in summary.Baselines)
                Console.WriteLine($"    {baseline.Name}: peak={baseline.PredictedPeakActiveVolumes} ({FormatDelta(baseline.DeltaFromCurrent)})");
        }

        if (summary.TopWindowsWherePlainBetter.Count > 0)
        {
            Console.WriteLine("  Windows where plain size-desc is better:");
            foreach (var window in summary.TopWindowsWherePlainBetter)
            {
                Console.WriteLine($"    items {window.StartPosition}-{window.EndPosition}: plain-current={window.PlainMinusCurrentDelta}");
                Console.WriteLine($"      phasedPeak={window.CurrentPeakActiveVolumes} at position {window.CurrentPeakPosition} path={window.CurrentPeakPath}");
                Console.WriteLine($"      plainPeak={window.PlainPeakActiveVolumes} at position {window.PlainPeakPosition} path={window.PlainPeakPath}");
                Console.WriteLine($"      phasedWindow: active {window.CurrentDynamics.StartActiveVolumes}->{window.CurrentDynamics.EndActiveVolumes} ({FormatDelta(window.CurrentDynamics.NetActiveVolumeDelta)}), introduced={window.CurrentDynamics.IntroducedVolumes}, drained={window.CurrentDynamics.DrainedVolumes}, avgDataBytes={FormatBytes(window.CurrentDynamics.AverageDataBytes)}");
                Console.WriteLine($"      phasedMix: {FormatPhaseMix(window.CurrentDynamics)}");
                Console.WriteLine($"      plainWindow: active {window.PlainDynamics.StartActiveVolumes}->{window.PlainDynamics.EndActiveVolumes} ({FormatDelta(window.PlainDynamics.NetActiveVolumeDelta)}), introduced={window.PlainDynamics.IntroducedVolumes}, drained={window.PlainDynamics.DrainedVolumes}, avgDataBytes={FormatBytes(window.PlainDynamics.AverageDataBytes)}");
                Console.WriteLine($"      plainMix: {FormatPhaseMix(window.PlainDynamics)}");
            }
        }

        if (summary.TopWindowsWherePhasedBetter.Count > 0)
        {
            Console.WriteLine("  Windows where phased order is better:");
            foreach (var window in summary.TopWindowsWherePhasedBetter)
            {
                Console.WriteLine($"    items {window.StartPosition}-{window.EndPosition}: plain-current={window.PlainMinusCurrentDelta}");
                Console.WriteLine($"      phasedPeak={window.CurrentPeakActiveVolumes} at position {window.CurrentPeakPosition} path={window.CurrentPeakPath}");
                Console.WriteLine($"      plainPeak={window.PlainPeakActiveVolumes} at position {window.PlainPeakPosition} path={window.PlainPeakPath}");
                Console.WriteLine($"      phasedWindow: active {window.CurrentDynamics.StartActiveVolumes}->{window.CurrentDynamics.EndActiveVolumes} ({FormatDelta(window.CurrentDynamics.NetActiveVolumeDelta)}), introduced={window.CurrentDynamics.IntroducedVolumes}, drained={window.CurrentDynamics.DrainedVolumes}, avgDataBytes={FormatBytes(window.CurrentDynamics.AverageDataBytes)}");
                Console.WriteLine($"      phasedMix: {FormatPhaseMix(window.CurrentDynamics)}");
                Console.WriteLine($"      plainWindow: active {window.PlainDynamics.StartActiveVolumes}->{window.PlainDynamics.EndActiveVolumes} ({FormatDelta(window.PlainDynamics.NetActiveVolumeDelta)}), introduced={window.PlainDynamics.IntroducedVolumes}, drained={window.PlainDynamics.DrainedVolumes}, avgDataBytes={FormatBytes(window.PlainDynamics.AverageDataBytes)}");
                Console.WriteLine($"      plainMix: {FormatPhaseMix(window.PlainDynamics)}");
            }
        }
    }

    private static void PrintBoundaryRelaxationSummary(BoundaryRelaxationSummary summary)
    {
        if (summary.Scenarios.Count <= 0)
            return;

        Console.WriteLine();
        Console.WriteLine("Boundary relaxation scenarios:");
        foreach (var scenario in summary.Scenarios)
            Console.WriteLine($"  {scenario.Name}: preserve={scenario.PreservedPhase1Items} ({scenario.PreservedPhase1Percentage.ToString("0.0", CultureInfo.InvariantCulture)}%), peak={scenario.PredictedPeakActiveVolumes} ({FormatDelta(scenario.DeltaFromCurrent)})");

        if (summary.FirstImprovement is not null)
            Console.WriteLine($"  First improvement: {summary.FirstImprovement.Name} -> peak={summary.FirstImprovement.PredictedPeakActiveVolumes} ({FormatDelta(summary.FirstImprovement.DeltaFromCurrent)})");
    }

    private static void PrintConstrainedPrefixSummary(ConstrainedPrefixSummary summary)
    {
        if (summary.Scenarios.Count <= 0)
            return;

        Console.WriteLine();
        Console.WriteLine("Constrained prefix heuristics:");
        foreach (var scenario in summary.Scenarios)
            Console.WriteLine($"  {scenario.Name}: prefixFiles={scenario.PrefixFileItems} ({scenario.PrefixPercentage.ToString("0.0", CultureInfo.InvariantCulture)}%), peak={scenario.PredictedPeakActiveVolumes} ({FormatDelta(scenario.DeltaFromCurrent)})");
    }

    private static void PrintAdaptiveInterleavingSummary(AdaptiveInterleavingSummary summary)
    {
        if (summary.Scenarios.Count <= 0)
            return;

        Console.WriteLine();
        Console.WriteLine("Adaptive interleaving heuristics:");
        foreach (var scenario in summary.Scenarios)
            Console.WriteLine($"  {scenario.Name}: {scenario.Trigger}, preserve={scenario.PreservedPhase1Items} ({scenario.PreservedPhase1Percentage.ToString("0.0", CultureInfo.InvariantCulture)}%), peak={scenario.PredictedPeakActiveVolumes} ({FormatDelta(scenario.DeltaFromCurrent)})");

        if (summary.BestScenario is not null)
            Console.WriteLine($"  Best adaptive scenario: {summary.BestScenario.Name} -> peak={summary.BestScenario.PredictedPeakActiveVolumes} ({FormatDelta(summary.BestScenario.DeltaFromCurrent)})");
    }

    private static void PrintPhase1SizeThresholdSweepSummary(Phase1SizeThresholdSweepSummary summary)
    {
        if (summary.Scenarios.Count <= 0)
            return;

        Console.WriteLine();
        Console.WriteLine("Phase 1 size-threshold sweep:");
        foreach (var scenario in summary.Scenarios)
            Console.WriteLine($"  {scenario.Name}: threshold={FormatBytes(scenario.ThresholdBytes)}, preserve={scenario.PreservedPhase1Items} ({scenario.PreservedPhase1Percentage.ToString("0.0", CultureInfo.InvariantCulture)}%), peak={scenario.PredictedPeakActiveVolumes} ({FormatDelta(scenario.DeltaFromCurrent)})");

        if (summary.FirstImprovement is not null)
            Console.WriteLine($"  First threshold improvement: {summary.FirstImprovement.Name} -> peak={summary.FirstImprovement.PredictedPeakActiveVolumes} ({FormatDelta(summary.FirstImprovement.DeltaFromCurrent)})");
        if (summary.BestScenario is not null)
            Console.WriteLine($"  Best threshold scenario: {summary.BestScenario.Name} -> peak={summary.BestScenario.PredictedPeakActiveVolumes} ({FormatDelta(summary.BestScenario.DeltaFromCurrent)})");
    }

    private static void PrintRuntimeCutoverRuleSummary(RuntimeCutoverRuleSummary summary)
    {
        if (summary.Scenarios.Count <= 0)
            return;

        Console.WriteLine();
        Console.WriteLine("Exact runtime-style cutover rules:");
        foreach (var scenario in summary.Scenarios)
            Console.WriteLine($"  {scenario.Name}: {scenario.Trigger}, preserve={scenario.PreservedPhase1Items} ({scenario.PreservedPhase1Percentage.ToString("0.0", CultureInfo.InvariantCulture)}%), peak={scenario.PredictedPeakActiveVolumes} ({FormatDelta(scenario.DeltaFromCurrent)})");

        if (summary.FirstImprovement is not null)
            Console.WriteLine($"  First exact-rule improvement: {summary.FirstImprovement.Name} -> peak={summary.FirstImprovement.PredictedPeakActiveVolumes} ({FormatDelta(summary.FirstImprovement.DeltaFromCurrent)})");
        if (summary.BestScenario is not null)
            Console.WriteLine($"  Best exact-rule scenario: {summary.BestScenario.Name} -> peak={summary.BestScenario.PredictedPeakActiveVolumes} ({FormatDelta(summary.BestScenario.DeltaFromCurrent)})");
    }

    private static void PrintLatePhase1TailCompositionSummary(LatePhase1TailCompositionSummary summary)
    {
        if (summary.TailSampleSize <= 0)
            return;

        Console.WriteLine();
        Console.WriteLine($"Late Phase 1 tail composition (last {summary.TailSampleSize} Phase 1 items):");
        Console.WriteLine($"  zeroData={FormatCountShare(summary.ZeroDataItems, summary.TailSampleSize)}");
        Console.WriteLine($"  <=4KiB={FormatCountShare(summary.AtMost4KiBItems, summary.TailSampleSize)}");
        Console.WriteLine($"  <=64KiB={FormatCountShare(summary.AtMost64KiBItems, summary.TailSampleSize)}");
        Console.WriteLine($"  <=256KiB={FormatCountShare(summary.AtMost256KiBItems, summary.TailSampleSize)}");
        Console.WriteLine($"  metadataDominant={FormatCountShare(summary.MetadataDominantItems, summary.TailSampleSize)}");
        Console.WriteLine($"  introducesNoVolumes={FormatCountShare(summary.IntroducesNoVolumesItems, summary.TailSampleSize)}");
        Console.WriteLine($"  drainsNoVolumes={FormatCountShare(summary.DrainsNoVolumesItems, summary.TailSampleSize)}");
        Console.WriteLine($"  noVolumeEffect={FormatCountShare(summary.NoVolumeEffectItems, summary.TailSampleSize)}");
    }

    private static void PrintLatePhase1TailSummary(LatePhase1TailSummary summary)
    {
        if (summary.Candidates.Count <= 0)
            return;

        Console.WriteLine();
        Console.WriteLine($"Late Phase 1 tail candidates (sampled from last {summary.TailSampleSize} Phase 1 items):");
        foreach (var candidate in summary.Candidates)
        {
            Console.WriteLine($"  item {candidate.ItemIndex}: {candidate.Path}");
            Console.WriteLine($"    tailOffset={candidate.TailOffset}, dataBytes={FormatBytes(candidate.DataBytes)}, distinctVolumes={candidate.DistinctVolumes}");
            Console.WriteLine($"    active={candidate.ActiveVolumesBefore}->{candidate.ActiveVolumesAfter} ({FormatDelta(candidate.NetActiveVolumeDelta)}), introduced={candidate.IntroducedVolumes}, drained={candidate.DrainedVolumes}");
        }
    }

    private static void PrintBoundaryRegionSummary(BoundaryRegionSummary summary)
    {
        if (summary.Segments.Count <= 0)
            return;

        Console.WriteLine();
        Console.WriteLine($"Boundary region around Phase2 start (boundary item={summary.BoundaryPosition}, window={summary.WindowSize}):");
        foreach (var segment in summary.Segments)
        {
            Console.WriteLine($"  items {segment.StartPosition}-{segment.EndPosition}: plain-current={segment.PlainMinusCurrentDelta}");
            Console.WriteLine($"    phasedPeak={segment.CurrentPeakActiveVolumes} path={segment.CurrentPeakPath}");
            Console.WriteLine($"    plainPeak={segment.PlainPeakActiveVolumes} path={segment.PlainPeakPath}");
            Console.WriteLine($"    phasedWindow: active {segment.CurrentDynamics.StartActiveVolumes}->{segment.CurrentDynamics.EndActiveVolumes} ({FormatDelta(segment.CurrentDynamics.NetActiveVolumeDelta)}), introduced={segment.CurrentDynamics.IntroducedVolumes}, drained={segment.CurrentDynamics.DrainedVolumes}, avgDataBytes={FormatBytes(segment.CurrentDynamics.AverageDataBytes)}");
            Console.WriteLine($"    phasedMix: {FormatPhaseMix(segment.CurrentDynamics)}");
            Console.WriteLine($"    plainWindow: active {segment.PlainDynamics.StartActiveVolumes}->{segment.PlainDynamics.EndActiveVolumes} ({FormatDelta(segment.PlainDynamics.NetActiveVolumeDelta)}), introduced={segment.PlainDynamics.IntroducedVolumes}, drained={segment.PlainDynamics.DrainedVolumes}, avgDataBytes={FormatBytes(segment.PlainDynamics.AverageDataBytes)}");
            Console.WriteLine($"    plainMix: {FormatPhaseMix(segment.PlainDynamics)}");
        }
    }

    private static string FormatPhaseMix(OrderingWindowDynamics dynamics)
    {
        var parts = new List<string>(4);
        if (dynamics.Phase1Items > 0)
            parts.Add($"P1={dynamics.Phase1Items}");
        if (dynamics.Phase2Items > 0)
            parts.Add($"P2={dynamics.Phase2Items}");
        if (dynamics.Phase3Items > 0)
            parts.Add($"P3={dynamics.Phase3Items}");
        if (dynamics.FolderMetadataItems > 0)
            parts.Add($"FolderMeta={dynamics.FolderMetadataItems}");

        return parts.Count == 0 ? "<none>" : string.Join(", ", parts);
    }

    private static string FormatCountShare(int count, int total)
        => $"{count}/{total} ({(total <= 0 ? 0 : 100.0 * count / total).ToString("0.0", CultureInfo.InvariantCulture)}%)";

    private static string FormatBytes(long value)
    {
        string[] units = ["B", "KiB", "MiB", "GiB", "TiB"];
        double size = value;
        int unitIndex = 0;
        while (size >= 1024 && unitIndex < units.Length - 1)
        {
            size /= 1024;
            unitIndex++;
        }

        return size.ToString(size >= 10 || unitIndex == 0 ? "0" : "0.0", CultureInfo.InvariantCulture) + " " + units[unitIndex];
    }

    private static string FormatDateTime(DateTime? value)
        => value?.ToString("O", CultureInfo.InvariantCulture) ?? "<unknown>";

    private static string FormatDays(double? value)
        => value.HasValue ? value.Value.ToString("0.0", CultureInfo.InvariantCulture) : "<unknown>";
}