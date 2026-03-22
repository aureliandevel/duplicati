# Restore Cache Investigation

## Purpose

This branch contains the original Duplicati-coupled restore-cache investigation work.

It is not intended to be treated as a direct upstream-ready change set. It is primarily a reference branch that captures:

- runtime and database changes made during the investigation
- the original in-repo `analyze-restore-cache` command
- conclusions reached during the reorder-focused phase of the work
- the transition point to the external analyser project

## Why The In-Repo Analyser Still Exists

The in-repo analyser in `Duplicati/CommandLine/DatabaseTool/Commands/AnalyzeRestoreCache.cs` is retained here deliberately.

It remains useful as:

- the historical snapshot of the original Duplicati-integrated investigation tool
- the exact tool that accompanied the restore-ordering and restore-database changes made in this branch
- a provenance-preserving reference for anyone trying to reproduce or understand the early investigation phase

The later external analyser project does not make this file obsolete in a historical sense. The external project became the main ongoing workbench once the investigation expanded beyond a product-adjacent diagnostic and into broader simulation and reporting work.

## Relationship To The External Analyser Repo

The continuation of this work lives in the sibling repository:

- `https://github.com/aureliandevel/duplicati-restore-cache-analyser`

That external repository exists because the investigation evolved into a broader research tool with:

- many experimental heuristics
- HTML reporting and forensic inspection workflows
- repack and consolidation simulation
- work that should not necessarily ship inside Duplicati's `DatabaseTool`

The split should be understood like this:

- this repo contains the original Duplicati-integrated investigation phase
- the external repo contains the continuing research workbench

## Friend Assembly Note

The `InternalsVisibleTo` entry in `Duplicati/Library/Main/Database/LocalDatabase.cs` should remain in place for this branch.

The external implementation project intentionally keeps the assembly identity:

- `Duplicati.CommandLine.DatabaseTool.Implementation`

This is deliberate. It preserves access to internal Duplicati APIs without adding a new friend assembly name just for the external analyser.

The user-facing external executable has a different assembly name, but the implementation library keeps the historical friend-assembly identity on purpose.

## Investigation Status

Current practical conclusion:

- restore volume reordering is considered substantially exhausted as the main hoped-for fix
- phased restore and shared-block caching may still contribute to a final solution
- the evidence now points more strongly toward investigating how restore-hostile volume packing develops over time

The next major line of work is therefore not just "what should restore next?" but also:

- how backup and compact operations pack changed data into volumes
- how that packing drifts over retained history
- whether repacking or consolidation strategies can materially reduce restore-cache pressure

## What Held Up From The Reorder Phase

The reorder-focused phase produced useful findings even though it did not produce a practical fix.

What held up:

- reorder-only strategies produced real but modest gains
- multiple reorder families repeatedly plateaued near the same small improvement band
- stricter runtime-like cutover rules did not unlock a materially better outcome
- some long-lived pinned volumes were caused by sparse early-to-late bridge structure rather than shared-block-store effects

Practical reading:

- restore ordering matters
- but ordering alone does not appear capable of bringing the analysed restore sets back toward the intended cache envelope

## Important Interpretation Notes

### Blocksize History Matters

Older backups may use the historical default blocksize of `100kb` while newer backups may use the later `1mb` default.

That history matters when comparing restore behavior across databases.

Code history for the default change:

- authored on `2023-04-22`
- merged to main on `2024-04-21`
- later called out in `changelog.txt` on `2024-06-27`

### The Analyser Does Not Assume The Current Default Blocksize

The in-repo analyser uses block sizes recorded in the restore database, not the current product default.

That means analysis should be interpreted against the actual backup being analysed, not against whatever Duplicati happens to default to today.

### Blocksize And Dblock Size Are Different

Do not conflate:

- `blocksize`: per-file deduplication chunk size
- `dblock-size`: size limit of the remote block volume files

This distinction matters when reasoning about how many blocks can appear in a volume and when interpreting historical backups.

## Runtime Changes Versus Investigation Scaffolding

This branch mixes two kinds of work:

- runtime and database behavior changes relevant to restore investigation
- analyser and reporting scaffolding used to understand that behavior

That split is intentional in this branch because the goal is preservation of the investigation context, not just a minimal runtime patch.

## Suggested Documentation And Artifact Policy

If work from this branch is committed for long-term reference, prefer:

- concise anonymized markdown documents
- redacted text outputs
- summaries of findings and dead ends

Avoid committing by default:

- raw local paths
- large uncurated HTML outputs
- bulky transient scratch files that do not add new information

In practice, the most useful durable artifacts are:

- investigation history notes
- project decision notes
- redacted analyser outputs that support a conclusion
- small focused examples that demonstrate a structural pattern such as bridge volumes

## Recommended Reading Order

For someone picking this work up later:

1. Read this note for the repo split and current status.
2. Read the external analyser repo documentation for the continuing workbench direction.
3. Use the in-repo analyser only when reproducing or understanding the original Duplicati-integrated investigation phase.
4. Use the external analyser repo for ongoing simulation, reporting, and repack-oriented investigation.