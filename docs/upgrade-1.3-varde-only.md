# Upgrading to 1.3.x when Varde is the only writer

This follows up [docs/upgrade-1.1-to-1.3.md](https://github.com/Robin831/beads/blob/claude/beads-upgrade-analysis-3xrnb5/docs/upgrade-1.1-to-1.3.md) (draft [Robin831/beads#5](https://github.com/Robin831/beads/pull/5), bead DevInfra-o8m). The premise has changed, per bead **DevInfra-gbx.8**, decided 2026-10-28.

| | |
|---|---|
| **Varde** | The only writer of `dolt-beads` (databases `munin`, `heimdall`, `devinfra`, `beads`; served branch `beads-sync`). It runs bd in server mode as `varde-writer`. |
| **Devboxes** | A read-only SQL user; bd for reads only. |
| **Laptops** | Off bd entirely. No embedded clones. |
| **Forges** | Write through Varde's HTTP API, not bd. |
| **Emergency** | An admin write credential, used when Varde is down. |

**Refs examined:**
- fork `main` = [`9387fdb37`](https://github.com/Robin831/beads/commit/9387fdb374822ecb2ad38a8d169cfa5370a778de) (bd 1.1.2);
- upstream [v1.3.2](https://github.com/gastownhall/beads/tree/v1.3.2) (final, 2026-10-23, `9b65b9e19`; new since the first report);
- upstream `main` = [`e46de42b3`](https://github.com/gastownhall/beads/commit/e46de42b3) (2026-10-28). It still says version 1.3.2, carries 697 commits beyond the tag, and has **no 1.4 tag or RC**.

**Evidence labels:**
- **VERIFIED (code):** read in the source at the named ref.
- **VERIFIED (run):** reproduced locally against a `dolt sql-server` (dolt 2.6.1).
- **INFERRED:** follows from the code but was not run.

**Links:**
- fork file: `https://github.com/Robin831/beads/blob/9387fdb37/<path>`
- v1.3.2 file: `https://github.com/gastownhall/beads/blob/v1.3.2/<path>`
- upstream commit: `https://github.com/gastownhall/beads/commit/<sha>`

---

## TL;DR

1. **Stock upstream v1.3.2 needs no fork** under this premise, if four conditions hold:
   - `beads-sync` is each database's default branch on the server;
   - varde-writer gets `DELETE`;
   - nothing pushes into `dolt-beads` any more;
   - Varde, not bd, owns GitHub sync.

   v1.3.1 is not enough: v1.3.2 adds [`ddec5cc6c`](https://github.com/gastownhall/beads/commit/ddec5cc6c) ([#7322](https://github.com/gastownhall/beads/pull/7322)), which stops bd issuing a no-op `DOLT_CHECKOUT` that varde-writer is not allowed to run.
2. **The cut-over shrinks to one writer and a handful of readers**, all in one short window:
   - Varde's bd and the devboxes' read-only bd switch together.
   - Laptop clones, the Dolt-remote path, the wipe-on-push and the mixed-version risk all disappear.
   - The forward-skew guard is unchanged: any leftover 1.1.2 binary is locked out the moment a database is migrated.
3. **The `dep add` bug is upstream's and still open** (VERIFIED, run on fork 1.1.2, v1.3.1 and v1.3.2). The tracking issue is [gastownhall/beads#6647](https://github.com/gastownhall/beads/issues/6647) (open, p1), and the open fix is [gastownhall/beads#7929](https://github.com/gastownhall/beads/pull/7929). The cause is that the commit stages a fixed table list that leaves out `issues`. Migration 0059's one-time recompute is unrelated.
4. **What Varde needs:**
   - SELECT, INSERT, UPDATE, **DELETE** on each database, plus EXECUTE on `dolt_add` and `dolt_commit`. **Correction to the first report:** plain `bd create` needs DELETE too.
   - Devboxes need strict read-only mode (`readonly: true` / `BD_READONLY=1`), or `list`/`ready`/`show` exit non-zero.
   - `events` history in Dolt stops at migration 0062. `bd history` (which reads `dolt_history_issues`) is unaffected.
5. **v1.3.2 is the better target now.** "1.4" is not released yet. Its extra migrations 0067–0073 are additive, but it also requires Dolt ≥ 2.6.0 on the server, and moving 1.3.2 → 1.4 is another all-at-once cut-over. Re-evaluate when a 1.4 RC is tagged.

---

## 1. The cut-over under the new premise

### 1.1 What still switches, and what disappears

| Client / path | Before (first report) | Now |
|---|---|---|
| Varde | one writer among many | **the only writer**; switches in the window |
| Devboxes | writers | **read-only bd**; switch in the same window, because 1.1.2 cannot read a v66 database and 1.3.x probably cannot read a v53 one (first report §3) |
| Forges | bd writers | **no bd** (they use Varde's API). If a Forge still runs bd *reads*, treat it like a devbox. If it runs no bd, nothing switches. |
| Laptops | embedded clones + writers | **gone**. No clones to upgrade, no adopt/bootstrap, no `bd dolt push/pull` |
| Dolt remote / remotesapi pushes | the path that emptied the clone-local tables on `beads-sync` | **gone**, if remotesapi is closed or refuses writes (§2.2) |
| Mixed-version writers | the main risk | **gone** for writes. The only remaining risk is a forgotten 1.1.2 *reader*, which fails loudly with `schema version mismatch` and corrupts nothing |
| Emergency admin | — | must use **server-mode bd of the same version**, never an embedded clone and push |

Still unchanged from the first report:
- the migration content (0054–0066 / ignored 0012–0026; v1.3.2 adds none);
- the one-way data changes: primary-key re-key of comments, snapshots and events; de-duplication of dependencies; `events` leaves Dolt versioning;
- the rule that only an admin can run `bd migrate schema`.

### 1.2 Step list

**Preparation (no impact on anyone):**
1. **Inventory every bd binary and every SQL user** that touches `dolt-beads`: Varde, devboxes, Forges, cron jobs and any leftover laptop. Each must be either upgraded in the window or removed. Then delete any remaining laptop clones, or at least their remotes.
2. **Close the push path.** Close the remotesapi port, or give it no write user. Make sure no branch, remote or force push can land on `beads-sync`. This is what makes fork fixes #2–#4 unnecessary (§2.2).
3. **Decide who does GitHub sync.** Check whether `github.sync.auto=true` is set in any database's `config` table today. If it is, that fork-only behaviour stops at the cut-over, so Varde has to take it over: either its own GitHub client, or stock `bd github sync`, but then read §2.1 on `state:"open"`.
4. **Prepare the grants** (admin):
   - `GRANT DELETE ON <db>.* TO 'varde-writer'` for all four databases;
   - a SELECT-only devbox user.
5. **Make `beads-sync` the default branch.** Varde and the devboxes must land on `beads-sync` without a checkout. Per database, as admin: `SET PERSIST <db>_default_branch = 'beads-sync'` (for example `munin_default_branch`). Alternatively, check out `beads-sync` in each database directory with the server stopped.
   - Dolt selects a new session's branch from that variable ([`dsess/session.go:2063-2093`](https://github.com/dolthub/dolt/blob/main/go/libraries/doltcore/sqle/dsess/session.go) in the Dolt version v1.3.2 pins) (VERIFIED, code).
   - Whether it persists across a restart of the deployed server is INFERRED.
   - Verify: `SELECT active_branch()` as varde-writer and as the read-only user must both return `beads-sync`.
   - This replaces fork #14 (§2.1). It can be done now, on 1.1.2, and is harmless there.
6. **Configure devbox bd for strict read-only:** `readonly: true` in `.beads/config.yaml`, or `BD_READONLY=1` (§4.2).
7. **Dry run** on a copy of the four databases, served by a second `dolt sql-server` with the same users:
   - migrate as admin;
   - run Varde's own integration tests against the copy as varde-writer;
   - run devbox reads as the read-only user;
   - confirm that an old 1.1.2 binary is refused.

   Do all the checks listed in the first report's Phase 1. Also rehearse the rollback below and time it.

**The window:**

8. **Freeze writes.** Put Varde in maintenance mode, so the Forges' API calls queue or fail clearly, and stop Varde's bd. Commit anything pending as admin with `bd dolt commit`. Note that the `dep add` bug in §3 may have left `issues` dirty; check `SELECT * FROM dolt_status` and commit it, because a migration refuses dirty tables.
9. **Back up.** Stop `dolt-beads`, snapshot its data directory, and start it again. Record `SELECT HASHOF('HEAD')` and `active_branch()` for each database.
10. **Migrate as admin**, from one machine, one database at a time: `beads` → `devinfra` → `heimdall` → `munin`. The server has no remote, so the consent is plain `bd migrate schema`, not `--force`. After each database, check:
    - cursors 66/26;
    - the `dolt_ignore` rows;
    - `bd doctor`;
    - row counts, compared with the backup;
    - `dolt_log` on `beads-sync`.
11. **Install v1.3.2 for Varde.** Start it and run a smoke test as varde-writer on each database:
    - create, claim, update `--status`, close, reopen;
    - `dep add` (both blocks and parent-child), `dep remove`;
    - label add/remove, comment.

    After each, check `dolt_status` is clean. The exception is the known `dep add`/`dep remove` gap in §3, which Varde should work around.
12. **Install v1.3.2 on the devboxes** (strict read-only). Run `bd list`, `ready`, `show`, `blocked` and `search` as the read-only user, and confirm the exit code is 0.
13. **Lift the maintenance mode.** Forge traffic resumes through Varde.

**Rollback:**
- **Before step 13** (no 1.3.2 writes yet): stop the server, restore the step 9 snapshot, and reinstall 1.1.2 for Varde and the devboxes.
  - Prefer this to `DOLT_RESET --hard` to the recorded hash. A reset leaves the clone-local tables in their 1.3 shape and the ignored cursor at 26. Whether 1.1.2 then works was not verified.
  - The grants and the default-branch setting can stay; both are harmless on 1.1.2.
- **After step 13:** a rollback loses every write Varde accepted since. Fix forward if at all possible. If not, export the new beads with `bd export` on 1.3.2 before restoring, and re-create them.
- **Emergency writes during or after the window:** only with server-mode bd v1.3.2 and the admin credential.

### 1.3 Checks after the cut-over

- **No `command denied` in Varde's logs.** Note that a failed Dolt commit after a write is logged and swallowed (VERIFIED, code: `issue_operations_tx.go:61-114`). A missing grant shows up as missing history commits, not as failed commands, so watch `dolt_log` as well.
- **`dolt_status` is clean between Varde writes**, apart from the §3 gap until it is fixed.
- **Devbox reads exit 0.**
- **No client reports `schema version mismatch`.** If one does, it is a forgotten 1.1.2 binary.
- **`SELECT active_branch()` = `beads-sync`** for both users after a server restart.

---

## 2. Can we run an upstream release with no fork?

**Yes: stock v1.3.2, on the four conditions in the TL;DR.** Every commit the first report would carry over is now moot or replaced by configuration. The 14 it would drop stay dropped; none becomes needed under this premise.

### 2.1 The commits the first report would carry over, re-judged

| Fork commit | Still needed? | Upstream status (v1.3.2 / main) | Route |
|---|---|---|---|
| #1 [`97ee73664`](https://github.com/Robin831/beads/commit/97ee73664) + #19 [`78f74f6c2`](https://github.com/Robin831/beads/commit/78f74f6c2): GitHub auto-sync | Only if bd remains the thing that syncs GitHub. It fires from `PersistentPostRunE` → `maybeAutoGitHubSync` after a bd write (VERIFIED, code), so under the premise only Varde's own bd could run it | Not upstream (no `github.sync.auto`) | **Varde does GitHub sync itself.** Upstreaming the whole feature is a large PR and unlikely to be accepted |
| #6 [`4dcd92362`](https://github.com/Robin831/beads/commit/4dcd92362): never push `state:open` | Only if stock `bd github sync` is used | Still sends `state:"open"` ([`internal/github/mapping.go:191-195`](https://github.com/gastownhall/beads/blob/v1.3.2/internal/github/mapping.go#L191-L195)) on v1.3.2 and main, so a push reopens issues that were closed on GitHub | Moot if Varde syncs itself. Otherwise a **good small upstream PR** ("don't reopen on push"); it must also change `PushFieldsEqual`/`PushContentHash` |
| #2 [`9506cd85d`](https://github.com/Robin831/beads/commit/9506cd85d): auto-pull hook | **No.** No clones, nothing to pull | — | drop |
| #3 [`d2562300e`](https://github.com/Robin831/beads/commit/d2562300e): row-loss guard | **No.** No merges happen, and varde-writer cannot run `DOLT_MERGE`/`DOLT_PULL` anyway | — | drop |
| #14 [`ad9ce6d8d`](https://github.com/Robin831/beads/commit/ad9ce6d8d): pin server connections to the sync branch | **The goal remains** (Varde must hit `beads-sync`), **the code does not** | v1.3.2 and main still connect to the bare database name ([`dolt/store.go:2599`](https://github.com/gastownhall/beads/blob/v1.3.2/internal/storage/dolt/store.go#L2599)) and have no branch config key. bd rejects `munin/beads-sync` as a database name: `validDatabasePattern = ^[a-zA-Z_][a-zA-Z0-9_\-]*$` ([`dolt/history.go:19`](https://github.com/gastownhall/beads/blob/v1.3.2/internal/storage/dolt/history.go#L19)) is checked before `SHOW DATABASES` (VERIFIED, code) | **Server configuration:** make `beads-sync` the default branch (§1.2 step 5). Then `checkoutBranchIfNeeded` ([`dolt/store.go:1058-1073`](https://github.com/gastownhall/beads/blob/v1.3.2/internal/storage/dolt/store.go#L1058-L1073), #7322) never issues `DOLT_CHECKOUT`. Its own comment covers our case: a user with "EXECUTE on dolt_add/dolt_commit alone". `store.branch` stays `"main"` (`:2181`), but that only matters to push and pull, which Varde must not run |
| #15 [`1408c02fd`](https://github.com/Robin831/beads/commit/1408c02fd): embedded store uses the sync branch | **No.** No embedded clones | — | drop |
| #21 [`eed8c0517`](https://github.com/Robin831/beads/commit/eed8c0517): open without CREATE after a wipe ([beads#2](https://github.com/Robin831/beads/pull/2)) | **Not in steady state** (§2.2). Optional hardening | Not upstream: no `skipLocalSeries`, and the cursor `CREATE TABLE IF NOT EXISTS` is still unconditional ([`schema.go:1771`](https://github.com/gastownhall/beads/blob/v1.3.2/internal/storage/schema/schema.go#L1771); main `:2129`) | **Upstream PR, split in two.** (a) "Don't issue the cursor CREATE when the table exists": small, general, and in the spirit of #7322, which was a least-privilege fix upstream merged with tests run as a restricted user. (b) "Open without the clone-local series": likely to be refused, because upstream's [#6444](https://github.com/gastownhall/beads/pull/6444) (`12846c6a3`) chose to treat a missing `leases` table as a broken database. **Varde workaround:** if a wipe ever happens, an admin runs `bd migrate schema` (or varde-writer gets CREATE/DROP/ALTER/INDEX/REFERENCES and self-heals) |
| #22 [`7385ba71f`](https://github.com/Robin831/beads/commit/7385ba71f) + #23 [`9387fdb37`](https://github.com/Robin831/beads/commit/9387fdb374822ecb2ad38a8d169cfa5370a778de): close/recompute with wisp tables absent ([beads#3](https://github.com/Robin831/beads/pull/3), [#4](https://github.com/Robin831/beads/pull/4)) | **Not in steady state** (same cause as #21) | Not upstream. `blocked_state.go` is unchanged from v1.3.1 to v1.3.2; the mark/unmark exec at `:412/422/445` is unguarded. On main it would be a re-implementation | Upstream is unlikely to want it, since missing tables mean a broken database to them. **Workaround:** the same admin re-open as #21 |

The 14 dropped commits (the `--auto` resolver and its fixups, embedded auto-resolve, the release chores, `DOLT_CLONE --user`, the cursor-reality fix) stay dropped. Upstream either has them or they only served clones.

### 2.2 Why the wipe goes away

What empties the clone-local tables (VERIFIED, code, in the Dolt version v1.3.2 pins):
- A **push into a branch that keeps a working set on the server** fast-forwards it through `FastForwardWithWorkspaceCheck(…, allowDirtyWorking=onlyIgnored)`. That rewrites the branch's working set to the pushed root, so the ignored tables, which are never committed, vanish.
- A force push does the same through `SetHeadAndWorkingSetToCommit`.

Under the premise nobody pushes into `dolt-beads`:
- there are no laptops, embedded clones or Forge bd;
- varde-writer cannot call `DOLT_PULL`, `DOLT_MERGE`, `DOLT_RESET`, `DOLT_CHECKOUT` or `DOLT_GC`.

**The wipe therefore no longer has a trigger (INFERRED).** The ways it could come back:
- a forgotten clone pushing through an open remotesapi port;
- the emergency admin pushing from a clone instead of using server mode;
- restoring the server from a backup or a remote;
- the default branch flipping back to `main`, which has its own working set.

Those are covered by the "close the push path" step and by testing the runbook.

**If a wipe happens anyway on stock v1.3.2:**
- Varde's writable open fails on the cursor `CREATE`.
- Devbox reads fail on `LEFT JOIN leases`, which is part of every full-issue query.
- The working-set-only rows (`events` since 0062, `leases`, `local_metadata`) are lost.
- Recovery: an admin runs `bd migrate schema`; the events are not recoverable from Dolt.

**Alert on it** rather than patching bd.

---

## 3. The `dep add` bug: `is_blocked` left uncommitted

**What happens** (VERIFIED, run). In server mode, `bd dep add B A` (type blocks):
1. commits `dependency: add …`;
2. leaves `issues` modified in `dolt_status`, where `dolt_diff('HEAD','WORKING','issues')` shows only B's `is_blocked` 0→1;
3. after that, `bd recompute-blocked` refuses with "needs a clean working set".

The same result was reproduced on fork 1.1.2, v1.3.1 and v1.3.2 (built from the tag), against dolt 2.6.1 `sql-server` with `bd init --server --external`. `bd dep remove` on the fork leaves `issues` dirty with 1→0. Embedded mode is clean, because its auto-commit sweeps everything.

**Not run:**
- an upstream-main binary;
- `dep remove` on v1.3.2;
- wisp dependencies;
- the proxied-server topology, which commits `-Am` (`uow/doltserver_tx.go:104`) and would probably sweep the change in (INFERRED).

**Root cause on the fork** (VERIFIED, code):
1. [`cmd/bd/dep.go:378`](https://github.com/Robin831/beads/blob/9387fdb37/cmd/bd/dep.go#L378) calls `DoltStore.AddDependency`.
2. The transaction body `issueops.AddDependencyInTx` runs `markDirectBlockingDependencySourceInTx`, which issues `UPDATE issues SET is_blocked=1, updated_at=updated_at` ([`issueops/dependencies.go:234-251, 299-310`](https://github.com/Robin831/beads/blob/9387fdb37/internal/storage/issueops/dependencies.go#L234-L310)). Because `updated_at` is set to itself, `is_blocked` is the only column that changes.
3. The SQL transaction commits, then `s.doltAddAndCommit(ctx, []string{"dependencies"}, …)` ([`dolt/dependencies.go:55`](https://github.com/Robin831/beads/blob/9387fdb37/internal/storage/dolt/dependencies.go#L55)) stages only `dependencies`.
4. The sweep `commitPendingIfEmbedded` ([`dep.go:387`](https://github.com/Robin831/beads/blob/9387fdb37/cmd/bd/dep.go#L387)) does nothing in server mode.
5. `RemoveDependency` has the same gap ([`issueops/dependencies.go:738`](https://github.com/Robin831/beads/blob/9387fdb37/internal/storage/issueops/dependencies.go#L738), [`dolt/dependencies.go:88`](https://github.com/Robin831/beads/blob/9387fdb37/internal/storage/dolt/dependencies.go#L88)).

The bug comes from two upstream changes together: the fixed table list in [`67d6786d5`](https://github.com/gastownhall/beads/commit/67d6786d51d3675fecbed018221e036a647e6260) (GH#2455), and the in-transaction mark in [`8ae4c3c67`](https://github.com/gastownhall/beads/commit/8ae4c3c67b3d035b4b9d9efd90dbfeee14460107).

**On v1.3.2 and main it is still there, and upstream knows** (VERIFIED, code):
- The path was refactored to `DependencyEditor.AddDependencies` → `runIssueOperationTx` → `doltAddAndCommitPostTx(changed tables)`.
- `addDependencyEdgeInTx` reports only `{dependencies, events}`.
- The comment at [`issueops/dependency_editor.go:184-203`](https://github.com/gastownhall/beads/blob/v1.3.2/internal/storage/issueops/dependency_editor.go#L184-L203) describes exactly this bug: the flip "stays in the working set and is swept up by whatever command auto-commits next — including one that was REFUSED". It is tracked upstream as bead `bd-2y9ke`, and the same comment is still on main.
- **#6876 does not fix it.** [`d1840c043`](https://github.com/gastownhall/beads/commit/d1840c043) on 1.3.x ([#6876](https://github.com/gastownhall/beads/pull/6876)) only reorders `recheckBlockedAfterCommit`. That recheck runs only for writes that *unblock*: status change, dependency removal, delete. It never runs for `dep add`.
- **Migration 0059** recomputes `is_blocked` once, at upgrade. It has nothing to do with this per-command commit.

**Upstream issue and fix** (read from the GitHub web pages, not the API):
- [gastownhall/beads#6647](https://github.com/gastownhall/beads/issues/6647): "dep add leaves is_blocked uncommitted under auto-commit: issues never staged, recompute-blocked then refuses". Open, p1/bug, filed 2026-09-20 against v1.3.0 in server mode. Same root cause and repro; it also names `RemoveDependency` and the wisp path.
- [gastownhall/beads#7929](https://github.com/gastownhall/beads/pull/7929): "Fixes #6647". Opened 2026-10-28, with no human review yet. It stages `issues` whenever `is_blocked` flips, including the wisp paths.

**What to do:**
- **Until #7929 ships:** Varde commits explicitly after every `dep add`/`dep remove`. The stock route is `bd dolt commit` (INFERRED to stage `issues`); alternatively, `CALL DOLT_ADD('issues'); CALL DOLT_COMMIT('-m', …)` over Varde's own connection, using only the procedures varde-writer already has.
- **Before the migration** (step 8): check `dolt_status`.
- **Don't fork for it.** It is a one-line class of fix, but upstream has a PR that covers more paths.

---

## 4. What Varde needs on 1.3.x

### 4.1 Grants

**Per-command statements beyond SELECT/INSERT/UPDATE on v1.3.2** (VERIFIED, code; `git diff v1.3.1 v1.3.2` adds no new statements to normal paths). Remember that `REPLACE` needs INSERT **and** DELETE in go-mysql-server.

| Command | Needs |
|---|---|
| `bd create` | `REPLACE INTO local_metadata`: CLI → `ExecuteCreate` with `CreateOnly:true` ([`issueops/execution.go:42`](https://github.com/gastownhall/beads/blob/v1.3.2/internal/storage/issueops/execution.go#L42)) → [`create_only_guard.go:21`](https://github.com/gastownhall/beads/blob/v1.3.2/internal/storage/issueops/create_only_guard.go#L21). **This corrects the first report's §4.1**, which said plain create needed nothing; that was true of a store method the CLI does not use. Same on v1.3.1. |
| `bd close` | 2× `REPLACE INTO local_metadata` + `DELETE FROM leases` (`close.go:143, 370`) |
| `bd update --status/--assignee` (ending or transferring a claim), `reopen`, unclaim | `DELETE FROM leases` (`update.go:493`) |
| claim / heartbeat | `INSERT … ON DUPLICATE KEY UPDATE`: no extra grant |
| `dep add` | INSERT; parent-child also needs REPLACE |
| `dep remove`, `label remove` | DELETE |
| `label add` | INSERT IGNORE: no extra grant |
| comment | INSERT: no extra grant |
| `config set/unset` | REPLACE / DELETE |

**Minimum steady-state grant per database** (the GMS rule is INFERRED to hold in our deployed server):

```sql
GRANT SELECT, INSERT, UPDATE, DELETE ON <db>.* TO 'varde-writer'
-- plus the existing EXECUTE on dolt_add and dolt_commit
```

Nothing else is needed, **provided** `beads-sync` is the default branch, so that `DOLT_CHECKOUT` is never called. Migrations stay admin-only.

**Dolt procedures** (VERIFIED, code): the write verbs do a SQL transaction, then `DOLT_ADD` per table, then `DOLT_COMMIT`. If that commit fails, it is **logged and swallowed**. The consequence for monitoring is in §1.3.

### 4.2 Devboxes on a SELECT-only user

- **Classified read commands open read-only and skip schema init** (VERIFIED, code): `list`, `ready`, `show`, `stats`, `blocked`, `count`, `search`, `query`, `graph`, `duplicates`, `comments`, `export`, `backup` ([`cmd/bd/main.go:173-190`](https://github.com/gastownhall/beads/blob/v1.3.2/cmd/bd/main.go#L173-L190)).
- **⚠ But they can still fail** (VERIFIED, code). In human output, `list`/`ready`/`show` may display a tip. The post-run step then writes `REPLACE INTO local_metadata`, and only the embedded store's read-only error is tolerated. On a SELECT-only user that becomes a **non-zero exit after the output has printed** ([`cmd/bd/main.go:2040-2082`](https://github.com/gastownhall/beads/blob/v1.3.2/cmd/bd/main.go#L2040-L2082)).
- **Fix: strict read-only.** Set `readonly: true` in `.beads/config.yaml`, or `BD_READONLY=1`. Every command then opens read-only and skips post-command writes ([`main.go:222-229`](https://github.com/gastownhall/beads/blob/v1.3.2/cmd/bd/main.go#L222-L229)). Whether every read verb then succeeds is INFERRED; smoke-test them as the read-only user.
- **Reads on the migrated database need nothing special.** `leases` lives in `beads-sync`'s working set, which all sessions on that branch share.

### 4.3 `events` no longer versioned: what a reader of per-pool history loses

- **Where events live** (VERIFIED, code): after 0062, new events rows exist only in `beads-sync`'s working set on the server. Every session on that branch sees them once the writer's transaction commits.
- **What breaks** (INFERRED from how Dolt treats ignored tables):
  - `dolt_history_events`, `dolt_diff_events` and `AS OF` queries on `events` stop at the 0062 drop commit. Earlier history stays reachable.
  - Anything that reads per-pool event history from Dolt history, rather than from the live table, sees nothing new after the cut-over.
- **What is unaffected** (VERIFIED, code):
  - `dolt_log` and the other `dolt_diff_*` tables;
  - `bd history <id>`, which reads `dolt_history_issues` ([`issueops/history.go:32`](https://github.com/gastownhall/beads/blob/v1.3.2/internal/storage/issueops/history.go#L32));
  - `bd history --events` and `show`'s activity, which read the live table;
  - no bd code queries `dolt_history_events`.
- **There is no replacement on v1.3.2.**
  - `provenance_events` (0063) is versioned, but only `bd provenance` writes it, and it holds external references, not field changes.
  - `bd_events_journal` is clone-local, off by default, and pruned (7 days / 100k rows).
- **Upstream main's `issue_versions`** (0067–0071) is a versioned per-issue version log. It is opt-in (`versioned-history.enabled`), read with `bd versions <id>`, and single-writer only, which fits Varde. It is not released.
- **Fragility:** working-set-only rows (events, leases, local_metadata) are lost on a push into `beads-sync`, on `DOLT_RESET --hard`, or on a restore from backup or remote. That they survive an ordinary server restart is INFERRED, because Dolt persists working sets.
- **If audit matters:** have Varde record its own audit of what it changes, or export `events` on a schedule.

---

## 5. Is 1.4 (upstream main) the better target?

**Not yet.** Target **v1.3.2**, and re-evaluate when a 1.4 RC is tagged (INFERRED judgement; the facts are VERIFIED, code).

- **Release state.** There is no 1.4 tag or RC: `cmd/bd/version.go` on main still says 1.3.2, and main has 697 commits beyond the tag. The roadmap lists 1.4 priorities but no dates; RELEASING describes minor releases every 2–4 weeks. Running untagged main means a moving target with no release notes.
- **Extra migrations.** All additive and guarded, with no data rewrite. They still need an admin migration.

  | Migration | Change | Commit |
  |---|---|---|
  | 0067 | `issue_versions`, `store_epoch`, `issues.current_revision` | [`2bb1e20de`](https://github.com/gastownhall/beads/commit/2bb1e20de) |
  | 0068 | `attribution_status`; `durable_state` → LONGBLOB | [`da6263374`](https://github.com/gastownhall/beads/commit/da6263374) |
  | 0069 | DATETIME(6) timestamps | [`73702cc73`](https://github.com/gastownhall/beads/commit/73702cc73) |
  | 0070 | `participation_generation` | |
  | 0071 | `removed_restriction` | |
  | 0072 | `epoch_minted_addresses` | |
  | 0073 | `arrival_seq` | |
  | ignored/0027–0029 | wisp twins of the above | |

- **New operational requirements on main:**
  - **Dolt ≥ 2.6.0 on the server**, refused otherwise with no override. That is a one-way server upgrade of its own.
  - A new `__temp__%` `dolt_ignore` pattern, so the first writable open per database writes `dolt_ignore`. Plan one admin open each.
  - Classified `bd ready` runs the defer-wake sweep ([`9d646fc88`](https://github.com/gastownhall/beads/commit/9d646fc88)), which warns once per process on read-only users unless strict read-only is set.
  - New normal-path REPLACE/DELETE statements, all covered by the DELETE grant.
  - No change to branch targeting or wipe tolerance.
- **It does not avoid a second cut-over.** Because of the forward-skew guard, 1.3.2 → 1.4 is another all-at-once switch. With a single writer that switch is cheap, which argues for taking the released 1.3.2 now and 1.4 once it is tagged, not for jumping to main.
- **What would tip it towards 1.4:** if per-pool history matters to Varde, `issue_versions` is the one feature 1.3.2 cannot offer. That is worth tracking for when 1.4 ships.

---

## Verified vs inferred: summary

| Claim | Status |
|---|---|
| `dep add` leaves `issues.is_blocked` uncommitted on fork, v1.3.1, v1.3.2 | VERIFIED (run) |
| Same for `dep remove` on the fork | VERIFIED (run) |
| Same for `dep remove` on v1.3.2 | INFERRED |
| Upstream issue #6647 open, PR #7929 open | VERIFIED (GitHub web pages, 2026-10-28) |
| `munin/beads-sync` rejected as a database name | VERIFIED (code) |
| `DOLT_CHECKOUT` skipped when already on the branch (v1.3.2, not v1.3.1) | VERIFIED (code) |
| `<db>_default_branch` selects new sessions' branch | VERIFIED (Dolt code) |
| That setting persists on the deployed server | INFERRED |
| Wipe mechanism is a push into the served branch | VERIFIED (Dolt code) |
| No wipe without pushes | INFERRED |
| Grants per command, including create's REPLACE | VERIFIED (code) |
| GMS privilege rules match the deployed server | INFERRED |
| Devbox tip write fails without strict read-only | VERIFIED (code), not run |
| All read verbs succeed under strict read-only | INFERRED |
| `events` history in Dolt stops at 0062 | INFERRED (Dolt semantics) |
| `bd history` unaffected | VERIFIED (code) |
| 1.4 facts (no tag, migrations, Dolt ≥ 2.6.0) | VERIFIED (code at `e46de42b3`) |
| Recommendation to target v1.3.2 | INFERRED (judgement) |
| Rollback by `DOLT_RESET` leaving 1.1.2 working | Not verified; the plan uses a snapshot restore instead |
