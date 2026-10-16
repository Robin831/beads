# Upgrading our fleet from bd 1.1.2 (fork) to 1.3.x

Groundwork for bead **DevInfra-o8m**. This is a read-only analysis. It changes no code.

| | |
|---|---|
| Fork | [Robin831/beads](https://github.com/Robin831/beads) `main` = [`9387fdb37`](https://github.com/Robin831/beads/commit/9387fdb374822ecb2ad38a8d169cfa5370a778de) (bd 1.1.2 + fork commits). The devbox runs [`da76d3d5c`](https://github.com/Robin831/beads/commit/da76d3d5c), which predates fixes #2–#4 |
| Upstream | [gastownhall/beads](https://github.com/gastownhall/beads) `main` = `d2748411c` (2026-10-16); [v1.3.1](https://github.com/gastownhall/beads/tree/v1.3.1) = `c1c4b642a`; [v1.3.2-rc.2](https://github.com/gastownhall/beads/tree/v1.3.2-rc.2) = `53637a74a` |
| Merge-base | `8e4e59d39` (= v1.1.0). v1.1.2, v1.3.1 and v1.3.2-rc.2 all sit on release branches: v1.3.x branches off main at `f45b249ce` (v1.3.0), with 41 (v1.3.1) and 59 (rc.2) tag-only commits |
| Shared server | `dolt-beads`, databases `munin`, `heimdall`, `devinfra`, `beads` |
| Method | Reading the code at each ref (`git show`, `git grep`, `git log -S`). In addition, upstream's upgrade-smoke script was **run** against binaries built from v1.3.1 and from the fork (§5). Nothing was run against a `dolt sql-server`. Anything not settled by the code or a run is marked **UNVERIFIED**. |

Line references are to the named tag. Upstream file links use `https://github.com/gastownhall/beads/blob/v1.3.1/<path>`.

---

## TL;DR

1. **The whole fleet switches together, per database.** Once 1.3.x migrates a database, the 1.1.2 binary refuses to open it: `schema version mismatch: database is at v66, binary knows up to v53`. 1.3.x, for its part, will not migrate a shared server on its own. It refuses to write until an operator runs `bd migrate schema`, and its reads against the old schema probably fail too (UNVERIFIED, §3.2). Mixed-version operation does not work.
2. **The migration is large and partly one-way.** It covers main migrations 0054–0066 and clone-local ones ignored/0012–0026. It includes:
   - moving leases into a new clone-local table;
   - a full `is_blocked` recompute;
   - a **primary-key rewrite** of comments, snapshots and events;
   - a dependency re-key that can **delete duplicate edges**;
   - `events` taken out of Dolt versioning (`DROP TABLE events` is committed).

   bd never executes the `.down.sql` files. **Rollback means restoring a backup or resetting to a recorded commit.** v1.3.2-rc.2 adds no schema changes on top of v1.3.1.
3. **varde-writer needs `DELETE` on 1.3.x.** Without it, `bd close`, status/assignee updates, reopen, and parent-child dependency adds fail. These now issue `DELETE FROM leases` and/or `REPLACE INTO local_metadata`, and REPLACE needs INSERT+DELETE. The migration itself needs a full admin user.
4. **1.3.x brings back both of our recent bugs.** Upstream has no equivalent of fork #2 (open without CREATE) or #3/#4 (close with wisp tables absent), so they must be carried over. The clone-local set is now larger: it includes `leases` and `events`. A wipe therefore breaks reads as well as writes on 1.3.x.
5. **Upstream's upgrade-smoke passes on v1.3.1, and our two smoke failures are not real bugs.** The script's argless default picked `v1.3.2-rc.1` as the "previous" version, so the run was a 1.3 → 1.1 *downgrade*. The fork passes the script when given `v1.1.2` explicitly.
6. **Fork commits:** carry over 9 of the 23 non-merge commits, squashed into 7 changes, plus an optional eighth from #3. Drop the other 14 (13 if #3 is kept).

---

## 1. Our commits

`git log upstream/main..origin/main` lists **27 commits: 23 non-merge + 4 merges**:
- merges: [`069860bfa`](https://github.com/Robin831/beads/commit/069860bfa), [`792c85dcd`](https://github.com/Robin831/beads/commit/792c85dcd), [`883611e4c`](https://github.com/Robin831/beads/commit/883611e4c), [`a9c0a2f87`](https://github.com/Robin831/beads/commit/a9c0a2f87) "Merge tag 'v1.1.2'";
- 18 of the non-merge commits are ours;
- 5 are upstream's: four came in with the v1.1.2 tag merge, and `da76d3d5c` is a cherry-pick.

The "25" in the bead does not match any split of this clone, so **UNVERIFIED**. It may be a GitHub compare against an older upstream head.

Commit links: fork `https://github.com/Robin831/beads/commit/<sha>`, upstream `https://github.com/gastownhall/beads/commit/<sha>`.

### 1.1 Summary

| # | sha | title | upstream has the behaviour? | verdict |
|---|---|---|---|---|
| 1 | [`97ee73664`](https://github.com/Robin831/beads/commit/97ee73664) | port: GitHub auto-sync (Stage A) | **No** (no auto-sync, `--close-only`, `--push-open-only`, `GH_TOKEN` fallback, title idempotency, bead-ID body marker) | **carry over with rework** |
| 2 | [`9506cd85d`](https://github.com/Robin831/beads/commit/9506cd85d) | port: Dolt auto-pull hook + escaped-write flush (Stage B) | Hook: **no**. Flush: **yes** (`CommitPending` before every pull, embedded and server) | **carry over with rework** (hook only) |
| 3 | [`d2562300e`](https://github.com/Robin831/beads/commit/d2562300e) | port: `bd dolt resolve` suite (Stage C) | **Mostly**: `bd conflicts resolve` ([`144876baa`](https://github.com/gastownhall/beads/commit/144876baa)), `bd dolt pull --strategy` ([`3830f0a34`](https://github.com/gastownhall/beads/commit/3830f0a34), [#5119](https://github.com/gastownhall/beads/pull/5119)); no row-loss guard | **carry over with rework** (row-loss guard only) — or drop |
| 4 | [`7cae042d9`](https://github.com/Robin831/beads/commit/7cae042d9) | embedded pull-time auto-resolve (Stage D′, default off) | **Yes, on by default**: `versioncontrolops.Pull` → `MergeAndSettle` → `TryAutoResolveMergeConflicts`; field-level 3-way merge on issues ([`34695bdb6`](https://github.com/gastownhall/beads/commit/34695bdb6), [`db4b11d8f`](https://github.com/gastownhall/beads/commit/db4b11d8f) / [#4699](https://github.com/gastownhall/beads/pull/4699)) | **drop** |
| 5 | [`161450b94`](https://github.com/Robin831/beads/commit/161450b94) | resolve `--auto`: whole-table resolve | fixup of our `--auto`, which upstream lacks | **drop** |
| 6 | [`4dcd92362`](https://github.com/Robin831/beads/commit/4dcd92362) | github: never push `state:open` | **No**: [`internal/github/mapping.go:191-195`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/github/mapping.go#L191-L195) still sends `state:"open"` | **carry over with rework** |
| 7 | [`ccfea6ff5`](https://github.com/Robin831/beads/commit/ccfea6ff5) | `--auto`: open/in_progress claim race | covered by upstream's field-level merge (one-sided cell change) | **drop** |
| 8 | [`19f432b5b`](https://github.com/Robin831/beads/commit/19f432b5b) | `--auto`: uniform-side picks directly | fixup of `--auto` | **drop** |
| 9 | [`564049bb8`](https://github.com/Robin831/beads/commit/564049bb8) | schema: dolt#11131 drift in aux re-key (#4380) | **Yes**, as a superset: [`47a90d505`](https://github.com/gastownhall/beads/commit/47a90d505) ([#5064](https://github.com/gastownhall/beads/pull/5064)) | **drop** |
| 10 | [`470a69484`](https://github.com/Robin831/beads/commit/470a69484) | release: bump 1.1.1 | release chore | **drop** |
| 11 | [`e331e1c72`](https://github.com/Robin831/beads/commit/e331e1c72) | release: MCP lock 1.1.1 | release chore | **drop** |
| 12 | [`20e493e56`](https://github.com/Robin831/beads/commit/20e493e56) | release: bump 1.1.2 | release chore | **drop** |
| 13 | [`f82a2531c`](https://github.com/Robin831/beads/commit/f82a2531c) | drop duplicate `bd dolt resolve` registration | merge artefact of #3 | **drop** (fold into #3 if kept) |
| 14 | [`ad9ce6d8d`](https://github.com/Robin831/beads/commit/ad9ce6d8d) | pin server connections to the sync branch | **No**: [`dolt/store.go:2538-2539`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/dolt/store.go#L2538-L2539) uses the bare database name; `:2024` hard-codes `branch: "main"` | **carry over with rework** |
| 15 | [`1408c02fd`](https://github.com/Robin831/beads/commit/1408c02fd) | embedded store targets the sync branch | **No**: [`cmd/bd/store_factory.go`](https://github.com/gastownhall/beads/blob/v1.3.1/cmd/bd/store_factory.go) lines 94, 99, 108, 120, 188, 294 pass `"main"` | **carry over** (6 call sites; we patched 5) |
| 16 | [`67391c53c`](https://github.com/Robin831/beads/commit/67391c53c) | `DOLT_CLONE --user` | **Yes**: [`374d4cae6`](https://github.com/gastownhall/beads/commit/374d4cae6) ([#4484](https://github.com/gastownhall/beads/pull/4484)), in v1.2.0 | **drop** |
| 17 | [`3b273d5cc`](https://github.com/Robin831/beads/commit/3b273d5cc) | restore issues auto-resolve on server path | **Yes**: [`versioncontrolops/mergesettle.go:396-429`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/versioncontrolops/mergesettle.go#L396-L429) | **drop** |
| 18 | [`df2fdf26b`](https://github.com/Robin831/beads/commit/df2fdf26b) | test: un-skip embedded auto-resolve test | test for #4 | **drop** |
| 19 | [`78f74f6c2`](https://github.com/Robin831/beads/commit/78f74f6c2) | github close-only passed non-existent `completed_at` | fixes our own #1; upstream has no close-only path | **carry over** (squash into #1) |
| 20 | [`da76d3d5c`](https://github.com/Robin831/beads/commit/da76d3d5c) | schema: stop believing a contradicted ignored-migration cursor | **Yes**: [`7e10378b9`](https://github.com/gastownhall/beads/commit/7e10378b9) ([#5190](https://github.com/gastownhall/beads/pull/5190), issue [#5033](https://github.com/gastownhall/beads/issues/5033)), in v1.2.0 | **drop** |
| 21 | [`eed8c0517`](https://github.com/Robin831/beads/commit/eed8c0517) | schema: open without CREATE when clone-local tables are emptied ([beads#2](https://github.com/Robin831/beads/pull/2)) | **No** (§1.3) | **carry over with rework** |
| 22 | [`7385ba71f`](https://github.com/Robin831/beads/commit/7385ba71f) | close + blocked recompute with wisp tables absent ([beads#3](https://github.com/Robin831/beads/pull/3)) | **No** (§1.3) | **carry over** onto v1.3.x (rework for upstream main) |
| 23 | [`9387fdb37`](https://github.com/Robin831/beads/commit/9387fdb374822ecb2ad38a8d169cfa5370a778de) | absent-wisp retry matches any whitespace/case ([beads#4](https://github.com/Robin831/beads/pull/4)) | **No** | **carry over** (squash into #22) |

Carried over, after squashing: 9 commits become 7 changes.
1. GitHub sync (#1 + #19)
2. never push `state:open` (#6)
3. auto-pull hook (#2)
4. sync-branch pinning, server (#14)
5. sync-branch pinning, embedded (#15)
6. the no-CREATE open (#21)
7. the absent-wisp close (#22 + #23)

Optionally, an eighth: the row-loss guard from #3. Everything else, 14 commits, is dropped.

### 1.2 Rework notes for what we carry over

- **GitHub sync (#1, #19).** Rework three things against upstream's newer code:
  - **Push dedup.** Upstream added it in [`9827c6631`](https://github.com/gastownhall/beads/commit/9827c6631) ([#4329](https://github.com/gastownhall/beads/pull/4329)). `PushFieldsEqual` and `PushContentHash` compare the description against the issue body. Our `<!-- Bead: <id> -->` marker makes those always differ, so every sync would re-PATCH every issue. Apply the marker inside both functions, or strip it before comparing.
  - **Close policy.** Tracker writes into `closed` now enforce close policy ([#5191](https://github.com/gastownhall/beads/pull/5191), [#5206](https://github.com/gastownhall/beads/pull/5206)). `doCloseOnly` must pass `issueops.OpForceClosePolicy`, or beads with open children are refused.
  - **last-touched.** It changed ([#4839](https://github.com/gastownhall/beads/pull/4839), [#5786](https://github.com/gastownhall/beads/pull/5786)). `show.go:285,291` need `RecordViewedID`, and `cmd/bd/main.go`'s `PersistentPostRun` was heavily restructured.
- **Never push `state:open` (#6).** `PushFieldsEqual` and `PushContentHash` both assume `desiredState := "open"`, so they need the same closed-is-sticky rule. Otherwise every open bead whose GitHub issue is closed is re-sent on every run.
- **Auto-pull hook (#2).** Keep the `maybeAutoPull` hook; upstream only has `maybeAutoPush`. Drop `flushEscapedWritesBeforePull`, which upstream does unconditionally:
  - [`embeddeddolt/version_control.go:543-587`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/embeddeddolt/version_control.go#L543-L587);
  - server `commitBeforePull`, [`dolt/store.go:4309-4319`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/dolt/store.go#L4309-L4319).

  Also respect `isDoltLocalOnly()`.
- **Row-loss guard (#3).** Upstream concludes merges with `commitAll(-Am)`, which stages the auto-merged rows; leaving them unstaged was the cause of our 2026-05-26 row loss. We can rework the guard as a post-condition inside `bd conflicts resolve` / `--conclude`, or drop it. **UNVERIFIED** whether upstream can still lose a one-sided row by another route.
- **Sync-branch pinning, server (#14).** `openServerConnection` now returns `serverConnFacts`, and `pinStoreBranch` ([#5848](https://github.com/gastownhall/beads/pull/5848)) reads `active_branch()`, so it is compatible with a `db/branch` DSN. **There is a gap in our fork as well:** `s.branch` stays `"main"`, and server push/pull use `s.branch` ([`store.go:4136-4259`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/dolt/store.go#L4136-L4259)). The port should set it to the resolved sync branch.

### 1.3 The three recent fixes

**The failure, as the fork commits describe it:**
1. A push into a branch served by `dolt sql-server` empties that branch's dolt-ignored, clone-local tables (`wisps`, `wisp_*`, `local_metadata`, `ignored_schema_migrations`).
2. On the next open, the ignored series looks behind. The first statement of `migrate()` is `CREATE TABLE IF NOT EXISTS …`.
3. varde-writer has no CREATE, so it is refused that statement even when the table exists, and every command fails at open.
4. Once the open was tolerated, `bd close` still failed, because the `is_blocked` mark/unmark templates `JOIN wisps` and read `wisp_dependencies`.

**UNVERIFIED** (not reproduced here):
- the emptying-on-push premise itself;
- how it interacts with migration 0040's `dolt_nonlocal_tables` entry (`target_ref 'main'`).

**[`eed8c0517`](https://github.com/Robin831/beads/commit/eed8c0517) (beads#2): upstream fails the same way on v1.3.1, rc.2 and main.**
- [`schema.go:1770-1772`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/schema.go#L1770-L1772) still runs the cursor bootstrap `CREATE` unconditionally in every `migrate()` (main: `schema.go:2026`).
- `migrationWorkNeeded` (`:915-918`) returns true whenever the ignored series is behind.
- The lock-free fast path (`converged.go:56-61`) defers to it.
- `git grep skipLocalSeries v1.3.1` finds nothing, and no privilege-denied handling exists in `internal/storage/schema`.

To rework the fix:
- `MigrateUp` was split, so `skipLocalSeries` goes in `migrateUpAfterReconcile`, after the `if !needed {…}` block and after the dolt_ignore seed has committed.
- `mainWorkNeeded` must mirror v1.3.1's `migrationWorkNeeded` (`:915-939`) without the ignored-series checks.
- The `sentinelTableExists` guard in `migrate()` applies as it is.
- **The fix is now bigger than before.** The clone-local set on 1.3.x also holds `leases`, `events` (0062) and `bd_events_journal`/`bd_events_seq`. Every full-issue query does `LEFT JOIN leases` ([`sqlbuild/sqlbuild.go:96-101`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/sqlbuild/sqlbuild.go#L96-L101)). Skipping the series alone would leave varde unable to *read* after a wipe, so the port also needs an absent-table fallback for `leases`, or someone with CREATE must reopen promptly.

**[`7385ba71f`](https://github.com/Robin831/beads/commit/7385ba71f) + [`9387fdb37`](https://github.com/Robin831/beads/commit/9387fdb374822ecb2ad38a8d169cfa5370a778de) (beads#3, #4): unguarded on v1.3.1 and rc.2, where the code is identical.**
- The mark/unmark `ExecContext` calls at [`issueops/blocked_state.go:412, 422, 445`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/issueops/blocked_state.go#L403-L445) are unguarded.
- Their templates contain `JOIN wisps` (`:222, 236, 267, 281`) and `FROM wisp_dependencies` (`:33, 54`).
- The three reads the fork guards are unguarded too: `:570-572`, `:715-717`, `:749-755`.
- Only `close.go:225,279,309` guards its own direct wisp reads.
- The template columns all fall inside the fork's derived-table column lists, so the squashed patch should apply almost verbatim to v1.3.x.

**On upstream main this becomes a re-implementation**, because the code was restructured ([#6291](https://github.com/gastownhall/beads/pull/6291), [#6602](https://github.com/gastownhall/beads/pull/6602), [#7224](https://github.com/gastownhall/beads/pull/7224)):
- `blockedTableSpec` and an exogeneity read before each mark;
- a new `FROM wisps sp` leg (`:124`) that our `\bJOIN\s+wisps\b` regex does not match.

---

## 2. Schema migrations: fork → v1.3.1 (and rc.2)

| | fork | v1.3.0 | v1.3.1 | v1.3.2-rc.2 | upstream main |
|---|---|---|---|---|---|
| main (versioned) series | 0053 | 0066 | 0066 | 0066 | 0069 |
| ignored (clone-local) series | 0011 | 0026 | 0026 | 0026 | 0027 |

- `git diff --name-status origin/main v1.3.1 -- internal/storage/schema/migrations/` shows **only additions**. 0001–0053 and ignored/0001–0011 are byte-identical, so the recorded `content_hash` values match and no content skew is expected.
- **`git diff v1.3.1 v1.3.2-rc.2 -- internal/storage/schema/` is empty.** rc.2 changes nothing in the schema.
- **`.down.sql` files are never embedded or run by bd.** Only `*.up.sql` is `//go:embed`ed ([`schema.go:218-222`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/schema.go#L218-L222)), and `scripts/check-migration-hygiene.sh` (check E) says so. The ignored series has no down files at all. "Reversible" below therefore means "a hand-run down file would restore the shape without losing data".
- The schema version is a table, not a metadata key: `MAX(version)` of `schema_migrations` (versioned) and of `ignored_schema_migrations` (dolt-ignored, per clone), with a sha256 per row.

### 2.1 Main series 0054–0066

| # | file (links at v1.3.1) | intro | what it changes | data rewrite / backfill | down |
|---|---|---|---|---|---|
| 0054 | [`add_lease_columns`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/migrations/0054_add_lease_columns.up.sql) | [`e97839a2e`](https://github.com/gastownhall/beads/commit/e97839a2e) | `issues`/`wisps` + `lease_expires_at`, `heartbeat_at`, `row_lock BIGINT NOT NULL DEFAULT 0`; `idx_issues_lease` | none | yes, lossless |
| 0055 | [`move_leases_to_table`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/migrations/0055_move_leases_to_table.up.sql) | [`59f8d6ae9`](https://github.com/gastownhall/beads/commit/59f8d6ae9) | creates **clone-local** `leases`; drops `lease_expires_at`/`heartbeat_at` and the index from `issues` and `wisps` | **row move**: live leases go `issues` → `leases`. From the fork this copy is empty (0054 added the columns one step earlier) | yes, **lossy** |
| 0056 | [`add_comments_keyset_index`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/migrations/0056_add_comments_keyset_index.up.sql) | [`873e3040b`](https://github.com/gastownhall/beads/commit/873e3040b) | index `comments(issue_id, created_at, id)` (+ `wisp_comments`) | none | yes, lossless |
| 0057 | [`events_value_columns_idempotent_longtext`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/migrations/0057_events_value_columns_idempotent_longtext.up.sql) | [`3f5e1ba3e`](https://github.com/gastownhall/beads/commit/3f5e1ba3e) | guarded `events.old/new_value` → LONGTEXT; a no-op on ours | none | no-op by design |
| 0058 | [`heal_wisp_dependencies_split_constraints`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/migrations/0058_heal_wisp_dependencies_split_constraints.up.sql) | [`2f9367d6a`](https://github.com/gastownhall/beads/commit/2f9367d6a) | FKs + CHECK on clone-local `wisp_dependencies` | **DELETEs** orphan and zero-target rows, collapses multi-target rows. The Go pre-repair `repairWispDependenciesForwardShape` ([`634fd2825`](https://github.com/gastownhall/beads/commit/634fd2825)) may rebuild and dedupe the table first | no-op |
| 0059 | [`recompute_null_gate_is_blocked`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/migrations/0059_recompute_null_gate_is_blocked.up.sql) | [`b445e1a2d`](https://github.com/gastownhall/beads/commit/b445e1a2d) | — | **recomputes `issues.is_blocked` for every row** (versioned; `updated_at` preserved) | none |
| 0060 | [`add_storage_class`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/migrations/0060_add_storage_class.up.sql) | [`b0c2286bf`](https://github.com/gastownhall/beads/commit/b0c2286bf) | nullable `storage_class` on `issues`/`wisps` | none | yes |
| 0061 | [`content_derived_aux_ids`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/migrations/0061_content_derived_aux_ids.up.sql) | [`423afdcb2`](https://github.com/gastownhall/beads/commit/423afdcb2) | `SELECT 1`, a version anchor | **indirect:** triggers the Go re-key that **rewrites primary keys** of `comments`, `issue_snapshots`, `compaction_snapshots` (versioned) and `events` to content-derived v5 UUIDs. It will run on our databases (cursor 53 < 61) | none |
| 0062 | [`events_dolt_ignore`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/migrations/0062_events_dolt_ignore.up.sql) | [`0e069115a`](https://github.com/gastownhall/beads/commit/0e069115a) | moves `events` into dolt_ignore | copies rows to a temp table, **`DROP TABLE events` + its own `DOLT_COMMIT`** ("schema: drop versioned events table for ignored-plane flip"), `REPLACE INTO dolt_ignore ('events')`, recreates the table and restores the rows **into the working set only** | none |
| 0063 | [`create_provenance_events`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/migrations/0063_create_provenance_events.up.sql) | [`c0f18bc03`](https://github.com/gastownhall/beads/commit/c0f18bc03) | new **versioned** `provenance_events` | none | yes; drops the rows |
| 0064 | [`create_events_journal`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/migrations/0064_create_events_journal.up.sql) | [`fffddc67c`](https://github.com/gastownhall/beads/commit/fffddc67c) | clone-local `bd_events_journal`, `bd_events_seq` | seeds the counter | yes; destructive (seq restarts) |
| 0065 | [`widen_wisp_comments_text`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/migrations/0065_widen_wisp_comments_text.up.sql) | [`cc3d0bcf3`](https://github.com/gastownhall/beads/commit/cc3d0bcf3) | `wisp_comments.text` → LONGTEXT | type change only | no-op |
| 0066 | [`add_events_journal_actor`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/migrations/0066_add_events_journal_actor.up.sql) | [`67556264c`](https://github.com/gastownhall/beads/commit/67556264c) | `bd_events_journal.actor` | none | yes |

### 2.2 Ignored (clone-local) series 0012–0026

These run per clone against dolt-ignored tables. Their cursor is in the ignored `ignored_schema_migrations`, and they are **never committed**. There are no down files. Most are fresh-clone twins of a main migration:

| ignored | twin of main |
|---|---|
| 0012 | 0055 |
| 0013 | `wisps.row_lock` |
| 0014 | 0056 |
| 0015 | 0059 |
| 0019 | 0062 |
| 0020 | 0060 |
| 0022 | 0064 |
| 0024 | 0065 |
| 0025 | 0066 |

Others:
- **0016:** `leases.granted_node`.
- **0017:** `wisps.defer_until` index.
- **0021:** widens wisp text columns.
- **0023:** guards against *bd-enterprise's* fork, not ours; here it is a set of no-op probes.

The ones that rewrite data:
- **0015:** recomputes `wisps.is_blocked`.
- **0018** ([marker](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/migrations/ignored/0018_aux_row_id_rekey2_marker.up.sql)): while pending, the aux primary-key re-key described under 0061 runs.
- **0026** ([marker](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/migrations/ignored/0026_dep_rekey_dedup_marker.up.sql), [`680476c24`](https://github.com/gastownhall/beads/commit/680476c24), [#5268](https://github.com/gastownhall/beads/pull/5268)): forces one more pass of [`dep_id_backfill.go`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/dep_id_backfill.go). That pass re-keys `dependencies`/`wisp_dependencies` ids and **DELETEs duplicate logical edges in the versioned `dependencies` table**. Survivor priority: issue, then wisp, then external.

**How the ignored cursor behaves** ([`schema.go:1384-1496`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/schema.go#L1384-L1496)). A cursor is believed only as far as the live tables bear it out:

| missing | effective cursor clamped to |
|---|---|
| `wisps` or `wisp_dependencies` | 0 |
| `events` | 18 |
| journal tables | 21 |
| `leases.granted_node` | 11 |

On our databases the cursor reads 11, so 0012–0026 apply and 0001–0011 do not replay.

### 2.3 Dolt-specific behaviour

- **Commits.**
  - Each main migration commits separately: `schema: apply migration <file>`.
  - 0062 adds its own drop commit.
  - Re-key rewrites land in a final `schema: apply migrations` commit.
  - A `schema: seed dolt_ignore patterns` commit runs whenever patterns are missing. The patterns are `bd_events_journal`, `bd_events_seq`, `ignored_schema_migrations`, `leases`, `local_metadata`, `repo_mtimes`, `wisp_%`, `wisps`, plus `events` from v62 on ([`schema.go:493-547`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/schema.go#L493-L547)).
- **Branches.** All commits go to the session's **active branch**; no branch is ever named. There is no sync-branch concept in the schema code. Make sure the migrating session is on the branch the server serves to clients. This matters for us because of fork #14/#15.
- **Push.** Nothing in `internal/storage/schema` pushes. **UNVERIFIED:** whether a higher layer, such as auto-push, pushes after a migrating open.
- **Events after 0062.** After 0062, HEAD has no versioned `events`.
  - Older commits still contain the pre-flip rows.
  - New events no longer replicate; each clone keeps its own.
  - **UNVERIFIED inference:** a clone that adopts an already-flipped remote gets an empty `events` table (created by ignored/0019).
- **Legacy-lineage heal.** `ignored_cursor_untrack.go` runs on every writable open ([file](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/ignored_cursor_untrack.go)). If `ignored_schema_migrations` was ever *committed*, it drops the table, commits, and restores the rows. **UNVERIFIED** whether any of our four lineages has that table tracked at HEAD; check in the dry run.
- **Migration lock.** On a server the pass is serialised with `GET_LOCK('bd_schema_init:<db>')` ([`lock.go`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/lock.go)).
- **Dirty tables.** A pending migration that touches a table with uncommitted changes refuses (`DirtyTablesError`). Commit everything before migrating.
- **Remote gate** ([`remote_migrate_gate.go`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/remote_migrate_gate.go), [`smart_remote_migrate_gate.go`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/smart_remote_migrate_gate.go)):
  - **Shared server with no remote:** refused until `bd migrate schema`.
  - **With a Dolt remote:** needs `bd migrate --force` or `BD_ALLOW_REMOTE_MIGRATE=1` from exactly one machine, then `bd dolt push`. The smart gate stops on `adopt`, `fork-skew` and `data-behind` (#6575: pull first, because of 0062).

### 2.4 Coming later (upstream main only, not in any 1.3 tag)

- 0067 `add_versioned_beads_schema` ([`2bb1e20de`](https://github.com/gastownhall/beads/commit/2bb1e20de)): `issue_versions`, `store_epoch`, `current_revision`.
- 0068 `add_attribution_status` ([`da6263374`](https://github.com/gastownhall/beads/commit/da6263374)).
- 0069 `widen_issue_versions_datetime_precision` ([`73702cc73`](https://github.com/gastownhall/beads/commit/73702cc73)).
- ignored/0027 `add_wisps_current_revision`, renumbered from 0026 in [`b3d17caa6`](https://github.com/gastownhall/beads/commit/b3d17caa6).

All are additive, but expect a second coordinated cut-over for 1.4.

---

## 3. Compatibility during a rollout

### 3.1 A 1.1.2 client on a database 1.3.x has migrated: refuses

**Every server-mode open, read-only or writable, runs `CheckForwardDrift` first** ([fork `dolt/store.go:1179`](https://github.com/Robin831/beads/blob/9387fdb37/internal/storage/dolt/store.go#L1179), [fork `schema.go:80`](https://github.com/Robin831/beads/blob/9387fdb37/internal/storage/schema/schema.go#L80)). It fails with "database is at v66, binary knows up to v53".
- This was reproduced in the §5 run.
- **Every command that opens the store breaks:** create, update, close, dep, comments, ready, blocked, list, show, history, `dolt push/pull`, claim.

**`BD_IGNORE_SCHEMA_SKEW=1` lets 1.1.2 through.** In the §5 run, `close` then worked. Its inserts still succeed (`row_lock` has a default and `storage_class` is nullable), but it silently breaks 1.3.x's invariants:
1. **Leases.** It never touches `leases`, so stale lease rows survive close/reassign ([`lease.go:164-170`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/issueops/lease.go#L164-L170)).
2. **`row_lock`.** It never bumps `row_lock`, so 1.3.x's protection against lost concurrent closes is defeated ([`close.go:335-343`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/issueops/close.go#L335-L343)).
3. **Aux ids.** It mints random aux ids where 1.3.x derives them from content, and those never re-key (marker already recorded).
4. **Event journal.** It writes no journal rows; this only matters if the journal is enabled, which it is not by default.

**Do not use the escape hatch fleet-wide.**

### 3.2 A 1.3.x client on a v53 (1.1.2) database: refuses to write; reads probably fail

- **Writes.** On a shared server, writable opens hit `CheckSharedStoreMigrateGate`. Our setup counts as shared: [`dolt/store.go:2923-2957`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/dolt/store.go#L2923-L2957), [`:3014-3026`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/dolt/store.go#L3014-L3026). The open then fails with "refusing to auto-apply N pending schema migrations to a shared server database" ([`remote_migrate_gate.go:824-856`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/remote_migrate_gate.go#L824-L856)). No 1.3.x client migrates by accident.
- **Reads.** Read-only commands skip migration ([`cmd/bd/main.go:173-190`](https://github.com/gastownhall/beads/blob/v1.3.1/cmd/bd/main.go#L173-L190)). Upstream's [`upgrading.md`](https://github.com/gastownhall/beads/blob/v1.3.1/docs/getting-started/upgrading.md#L327-L360) says "Reads keep working throughout". However, full-issue hydration selects `row_lock` and `storage_class` and does `LEFT JOIN leases` ([`sqlbuild.go:46-101`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/sqlbuild/sqlbuild.go#L46-L101)), all of which are absent at v53, and no fallback was found. **UNVERIFIED:** expect `bd list`/`show`/`ready` on 1.3.x against an unmigrated database to fail. Test it in the dry run.

### 3.3 What upstream says

- **The cross-version smoke checks one direction only.** [`scripts/cross-version-smoke-test.sh`](https://github.com/gastownhall/beads/blob/v1.3.1/scripts/cross-version-smoke-test.sh#L8-L14) covers old data read by a new binary, in a private workspace. It does not cover an old binary against a new schema, a shared server, or mixed clients.
- **The 1.3.0 upgrade notes say to upgrade together.** From [CHANGELOG.md:741-826](https://github.com/gastownhall/beads/blob/v1.3.1/CHANGELOG.md#L741-L826): "A shared Dolt sql-server is never auto-migrated … **Upgrade every client that shares a store, together** … a mixed-version fleet a broken fleet". The same notes say to back up with the *old* binary first, because 1.3's `bd export` migrates before it exports.
- **Rollback guidance.** [`docs/recovery/accidental-1-2-1-release.md`](https://github.com/gastownhall/beads/blob/v1.3.1/docs/recovery/accidental-1-2-1-release.md) describes a schema-cursor rollback, not a data downgrade.

### 3.4 Conclusion

**Everyone switches at once.** Each database migrates independently, but a client binary is one version and our machines touch several databases. In practice, therefore, all four databases and all clients move in one maintenance window.

---

## 4. Permissions (varde-writer: SELECT, INSERT, UPDATE, EXECUTE on dolt_add/dolt_commit)

Privilege rules are from go-mysql-server's [`sql/planbuilder/auth_default.go`](https://github.com/dolthub/go-mysql-server/blob/main/sql/planbuilder/auth_default.go), at the version v1.3.1's `go.mod` pins:
- `DELETE` needs DELETE.
- **`REPLACE` needs INSERT and DELETE.**
- Any `CALL DOLT_*` other than the two granted procedures is denied.

**UNVERIFIED:** whether the GMS inside our deployed `dolt sql-server` behaves identically.

### 4.1 Normal commands that need more on 1.3.x than on 1.1.2

| command (1.3.1) | statement | where |
|---|---|---|
| `bd close` (every close) | `REPLACE INTO local_metadata` ×2, `DELETE FROM leases WHERE issue_id=?` | [`issueops/close.go:138-145`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/issueops/close.go#L138-L145), [`:369-372`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/issueops/close.go#L369-L372), [`lease.go:191-196`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/issueops/lease.go#L191-L196), [`dependency_coordination.go:41-43`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/issueops/dependency_coordination.go#L41-L43) |
| `bd update --status/--assignee` (other than keeping the same claim) | `DELETE FROM leases` | [`update.go:493-497`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/issueops/update.go#L493-L497) |
| `bd reopen`, unclaim, reclaim | `DELETE FROM leases` | [`reopen.go:100`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/issueops/reopen.go#L100), [`unclaim.go:118`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/issueops/unclaim.go#L118), [`lease.go:741`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/issueops/lease.go#L741) |
| `bd dep add --type parent-child`, `bd create --parent` | `REPLACE INTO local_metadata` | [`dependencies.go:300-304`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/issueops/dependencies.go#L300-L304), [`create.go:967`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/issueops/create.go#L967) |
| claim / heartbeat | `INSERT … ON DUPLICATE KEY UPDATE`, `UPDATE`: **fine** | [`lease.go:171-184`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/issueops/lease.go#L171-L184) |
| plain `bd create` | nothing extra | [`dolt/issues.go:42-50`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/dolt/issues.go#L42-L50) |

**Already needing DELETE or REPLACE on 1.1.2,** so varde cannot run them today either (inferred from code):
- label remove, dep remove;
- delete / cleanup / gc / prune / purge / wisp gc / mol burn / squash;
- config set/unset;
- `doctor --fix`.

**On 1.1.2, `RunInTransaction` commands always call `DOLT_CHECKOUT`** ([fork `transaction.go:101,159`](https://github.com/Robin831/beads/blob/9387fdb37/internal/storage/dolt/transaction.go#L101)). That means `bd label add`, delete and graph apply are denied to varde today. **UNVERIFIED at runtime.** 1.3.x usually avoids the call by borrowing a pooled connection ([`dolt/transaction.go:351-385`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/dolt/transaction.go#L351-L385)), so `label add` may start working.

### 4.2 Migration and open

- **Migration** (`bd migrate schema`) needs CREATE, ALTER, DROP, INDEX, REFERENCES and DELETE, plus:
  - writes to `dolt_ignore`;
  - `DOLT_ADD -f`, `DOLT_COMMIT`, `DOLT_RESET` (unstage, and the fresh-bootstrap heal at [`lock.go:342`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/lock.go#L342));
  - `DOLT_MERGE --ff-only` on the adopt path.

  **Run it as an admin user, never as varde.**
- **Steady-state open** is read-only for varde. dolt_ignore seeding reads first and writes only missing patterns, and the untrack heal treats privilege errors as advisory.
- **After a clone-local wipe**, a writable open runs the ignored series, which starts with `CREATE` ([`schema.go:1770`](https://github.com/gastownhall/beads/blob/v1.3.1/internal/storage/schema/schema.go#L1770)). varde's open fails, and reads then fail on `LEFT JOIN leases`. This is the beads#2 regression, made bigger (§1.3).
- **rc.2 is the same as v1.3.1** in its DDL, DELETE, REPLACE and `dolt_ignore` inventories.

### 4.3 What varde-writer needs on 1.3.x

1. **`DELETE` on the database**, minimum on `leases` and `local_metadata`. This is required for close and update.
2. **One of the following, for wipes:**
   - our #2–#4 fixes carried over, extended to `leases`/`events`/`local_metadata`; or
   - CREATE, ALTER, DROP, INDEX, REFERENCES for varde; or
   - an operator who reopens each database with an admin user after every wipe.

The first is the recommended option.

---

## 5. Upstream's upgrade-smoke script

**Upstream v1.3.1 passes its own smoke test: verified by running it.** Both trees built with `GOTOOLCHAIN=auto` (go1.26.7) and `.buildflags`. The scenarios use embedded Dolt, so no server was needed.

| script / candidate | previous version | result |
|---|---|---|
| v1.3.1 / bd 1.3.1 | `v1.1.2` | 7/7 pass |
| v1.3.1 / bd 1.3.1 | `v1.3.0` | 7/7 pass |
| v1.3.1 / bd 1.3.1 | argless (resolved to `v1.3.2-rc.1`) | 7/7 pass |
| fork / bd 1.1.2-fork | `v1.1.2` | 6/6 pass |
| fork / bd 1.1.2-fork | argless (resolved to `v1.3.2-rc.1`) | 7/7 **fail**, reproducing both messages |

**Our two failures are a downgrade, not a dependency bug.**
- **How the default is picked.** The script picks the "previous" version as the second-highest tag in the clone: `git tag --sort=-version:refname | grep '^v' | head -2 | tail -1`. It ignores the candidate's own version, and it sorts `-rc` above the final release. With upstream's tags fetched, it picks v1.3.2-rc.1.
- **What actually ran.** The "upgrade" opened a v66 database with a v53 binary, and every command was refused with `schema version mismatch: database is at v66, binary knows up to v53`.
- **Where the messages come from.** «migrated dependency schema regression» and «errno 1105 / stale depends_on_id regression» are **fixed `fail` labels** in Scenario 6 ([`scripts/upgrade-smoke-test.sh:478, 483, 500`](https://github.com/gastownhall/beads/blob/v1.3.1/scripts/upgrade-smoke-test.sh#L478)). The script sends stderr to `/dev/null`, so *any* failure of `ready`/`blocked`/`close` prints them. Run by hand, no errno 1105 and no `depends_on_id` error occurs. With `BD_IGNORE_SCHEMA_SKEW=1`, `close` succeeds.
- **The scenario is not new.** Scenario 6 dates from [`52a5ecf9e`](https://github.com/gastownhall/beads/commit/52a5ecf9e) ([#4238](https://github.com/gastownhall/beads/pull/4238)), already in v1.1.2, and is identical in both trees. The only script change in v1.3.1 is Scenario 7, wisps ([`362eecc6b`](https://github.com/gastownhall/beads/commit/362eecc6b), #6049).
- **Upstream has hit the same weakness.** [`release-gates/v1.2.2-recovery-release-gate.md:18`](https://github.com/gastownhall/beads/blob/v1.3.1/release-gates/v1.2.2-recovery-release-gate.md) notes it, and upstream main still has the same default.
- **Upstream CI never uses the default.** At v1.3.1, [`cross-version-smoke.yml`](https://github.com/gastownhall/beads/blob/v1.3.1/.github/workflows/cross-version-smoke.yml) passes each previous release explicitly from `gh release list`. Upstream main has since moved the smoke test to Bazel ([`bb9b9e502`](https://github.com/gastownhall/beads/commit/bb9b9e502)).
- **Our fork CI probably fails by construction.** Our copy of that workflow still lists *upstream's* releases. If its newest 5 are all 1.3.x, every PR leg is a downgrade. **UNVERIFIED:** this session could not read run logs.

**Recommendation for our fork's CI:** pass previous versions explicitly (for example `SMOKE_VERSIONS="v1.1.2 v1.1.1"`), or filter the release list to versions no newer than `cmd/bd/version.go`.

**Real dependency-schema fixes we gain by upgrading.** They are not what the smoke test hit. All are in v1.3.1:
- [`3f5e1ba3e`](https://github.com/gastownhall/beads/commit/3f5e1ba3e) — migration-chain hardening;
- [`680476c24`](https://github.com/gastownhall/beads/commit/680476c24) ([#5268](https://github.com/gastownhall/beads/pull/5268)) — convergent dependency re-key;
- [`2f9367d6a`](https://github.com/gastownhall/beads/commit/2f9367d6a) and [`634fd2825`](https://github.com/gastownhall/beads/commit/634fd2825) — `wisp_dependencies` constraints;
- [`a48e372c8`](https://github.com/gastownhall/beads/commit/a48e372c8) ([#5335](https://github.com/gastownhall/beads/pull/5335)) — narrower 1105 retry;
- [`4c3df8dd6`](https://github.com/gastownhall/beads/commit/4c3df8dd6) — ready/blocked/close assertions on migrated databases.

[`00846a0c1`](https://github.com/gastownhall/beads/commit/00846a0c1) (aux-rekey datetime rendering, [#6649](https://github.com/gastownhall/beads/pull/6649)) is on upstream main only.

---

## 6. Proposed order of steps

### Phase 0: prepare (no fleet impact)

1. **Build the target binary: our fork rebased onto v1.3.1**, or v1.3.2 final if it ships first. Carry the 7 changes listed in §1.1. The two that must land before cut-over:
   - #21, extended to `leases`/`events`/`local_metadata`;
   - #22+#23.

   Without them, varde breaks on the first wipe. #14/#15 are needed too if any client uses a non-`main` sync branch.
2. **Fix the fork's smoke CI** (§5). Then run `./scripts/upgrade-smoke-test.sh v1.1.2` against the new binary and require green.
3. **Agree varde-writer's new grant:** `DELETE` on all four databases (§4.3).
4. **Record facts we have not verified:**
   - whether any of the four databases has a Dolt remote, which changes the consent command (§2.3);
   - which branch the server serves;
   - whether `ignored_schema_migrations` is tracked at HEAD in any lineage;
   - the deployed Dolt and GMS version.

### Phase 1: dry run on a copy of the four databases

1. **Copy the data.** Stop nothing. Copy the four databases to a scratch server on another port, either with `dolt backup sync` / `CALL DOLT_BACKUP(...)` per database, or by copying the server's data directory from a filesystem snapshot. Start a second `dolt sql-server` on the copy with the same users and grants, varde-writer included.
2. **Baseline with the old binary (1.1.2).** Capture for each database:
   - `SELECT HASHOF('HEAD')`, `active_branch()`;
   - `MAX(version)` of both cursors;
   - row counts of `issues`, `dependencies`, `labels`, `comments`, `events`, `wisps`;
   - `bd list --json`, `bd ready --json`, `bd blocked --json`.
3. **Test the read-only window with 1.3.x before migrating.** Run `bd list`/`show`/`ready` against the copy, which resolves the UNVERIFIED claim in §3.2. Then check that writes are refused with the shared-store gate message.
4. **Migrate as an admin user.** `bd migrate schema --dry-run`, then `bd migrate schema` (or `--force` if a remote exists) per database. Time each one.
5. **Check after each database:**
   - both cursors read 66 / 26;
   - `dolt_ignore` contains `events`, `leases`, `bd_events_*`;
   - `dolt_log` shows the expected `schema: apply migration 00NN_…` commits on the served branch;
   - `bd doctor` is clean;
   - issue and label counts are unchanged;
   - `dependencies` count is unchanged or lower (0026 de-dupes); list the removed edges with `dolt_diff`;
   - comment and snapshot counts are unchanged (ids change);
   - `events` count is unchanged in the working set;
   - the `ready`/`blocked` JSON matches the baseline apart from expected `is_blocked` corrections.
6. **Exercise the restricted user.** As varde-writer, first without DELETE, then with it: create, update `--status`, `--claim`, close, reopen, `dep add` (parent-child), comment, `label add`. Confirm the failures listed in §4.1 and that the grant fixes them.
7. **Exercise the wipe case.** Empty the clone-local tables on the copy the way production does (push into the served branch), then open as varde with:
   - stock v1.3.1, which is expected to fail;
   - our carried-over build, which is expected to open and close.
8. **Confirm old binaries are locked out.** Point a 1.1.2 client at the migrated copy and confirm it refuses with the skew error, not something worse.
9. **Rehearse the rollback** (below) on the copy, and time it.

### Phase 2: production cut-over (one window, all four databases)

1. **Announce a freeze, then stop every client:** both Forges, Varde, devboxes, all three laptops. Disable any cron/agent loops that run `bd`. Commit pending work first: `bd dolt commit` on the old binary, because a migration refuses dirty tables.
2. **Back up with the old binary and server.** Stop `dolt-beads` briefly, snapshot or copy its data directory, and restart. Also record `HASHOF('HEAD')` per database and branch. If a remote exists, do **not** push anything until Phase 3 is green.
3. **Install the new binary everywhere, but do not start clients.** In a mixed state, old clients are refused by migrated databases and new clients by unmigrated ones, so nothing is corrupted. Still, keep clients stopped so the window is clean.
4. **Grant DELETE to varde-writer.** It is harmless on 1.1.2 as well.
5. **Migrate, as admin, from one operator machine, one database at a time.** Go from least to most critical: `beads` → `devinfra` → `heimdall` → `munin`. Run the Phase 1 step 5 checks after each one. Stop at the first anomaly; untouched databases stay on v53 and are still served to nobody.
6. **Bring clients back in order, verifying each:**
   1. the operator machine (`bd doctor`, `bd ready` on all four);
   2. **Varde**, because the restricted user is most likely to break: create / claim / close one test bead per database;
   3. both Forges;
   4. devboxes;
   5. laptops.

   On each machine, `bd version` must show the new build.
7. **Push to remotes**, if any, from the operator machine only, once Phase 3 is green.

### Rollback

**Before any 1.3.x client has written:**
- **Per database:** `CALL DOLT_RESET('--hard', '<recorded HEAD hash>')` as admin on the served branch. This restores the versioned `events` table and the pre-0055 `issues` shape.
- **Clone-local tables** (`wisps`, `leases`, the journal) are not versioned, so the reset does not touch them. The 1.1.2 binary's skew check reads only the main cursor, which would then be back at 53.
- **The ignored cursor is the open question.** It would still read 26, and the wisp tables would keep their 1.3 shape. **UNVERIFIED** whether 1.1.2 operates correctly on that, so rehearse it in Phase 1 step 9.
- **The safest full rollback** is to stop the server and restore the data-directory snapshot from Phase 2 step 2.
- Then reinstall 1.1.2 on all clients.

**After 1.3.x clients have written:** rolling back loses those writes. Prefer fixing forward. If rollback is unavoidable, export the new beads first (`bd export` on 1.3.x), restore the snapshot, and re-create them by hand. See also upstream's [`docs/recovery/accidental-1-2-1-release.md`](https://github.com/gastownhall/beads/blob/v1.3.1/docs/recovery/accidental-1-2-1-release.md). **UNVERIFIED** for our setup.

**Never mix versions to "test" in production.** `BD_IGNORE_SCHEMA_SKEW` on old clients breaks invariants silently (§3.1).

### Phase 3: what to check after the cut-over (day 0 and day 1)

- **Per database:**
  - `bd doctor`, including the migration-content-skew check;
  - cursors at 66/26;
  - `bd ready`/`bd blocked` sane.
- **varde:** close / update / claim work, and no `command denied` in logs.
- **Clone-local wipe:** after the first push into the served branch, varde can still open and read.
- **No client left on 1.1.2:** look for "schema version mismatch" errors in agent logs.
- **Auto-pull and GitHub auto-sync** (if carried over) behave as before. Watch for mass re-PATCHing on the first sync (§1.2).
- **Dolt history:** `dolt_log` shows no unexpected commits on `main` (sync-branch pinning, §1.2).

---

## Unverified items, collected

- The source of the bead's "25 commits" count.
- The emptying-on-push premise behind fork #2–#4, and its interaction with migration 0040's `dolt_nonlocal_tables`.
- Whether 1.3.x reads work against an unmigrated v53 database (§3.2). The code suggests they do not.
- Whether a higher layer auto-pushes after a migrating open.
- Whether any of our lineages has `ignored_schema_migrations` tracked at HEAD.
- What happens to pre-flip `events` rows on a clone adopting a flipped remote.
- Whether the deployed `dolt sql-server`'s privilege checks match the pinned GMS.
- Whether varde is already denied `bd label add` (`DOLT_CHECKOUT`) on 1.1.2.
- Whether 1.1.2 runs correctly after a `DOLT_RESET` rollback that leaves the clone-local tables in their 1.3 shape.
- Whether upstream's own v1.3.1 tag-push smoke run and our fork's CI smoke runs were green or red. Their logs could not be read from this session.
