# Orphaned Backup Detection

A backup taken by this controller is not one thing in one place. It is several
artifacts in several systems that must all agree, and any of them can drift.
This document explains where a backup actually lives, every way those places can
disagree, what the controller reports when they do, and what to do about it.

---

## 1. Where a backup actually lives

| # | Location | What it holds | Who writes it |
|---|---|---|---|
| 1 | **Controller's S3 bucket** | Metadata only: `latest-backup-id.txt` and `{history,incomplete,orphaned}/YYYY/MM/DD/{backupID}.json` | This controller |
| 2 | **Zeebe's backup store** | Partition snapshots and WAL segments | The Zeebe broker (`ZEEBE_BROKER_DATA_BACKUP_*`), a separate bucket |
| 3 | **Elasticsearch snapshot repository** | `camunda_{operate,tasklist,optimize}_{backupID}_{version}_part_k_of_n` | Operate / Tasklist / Optimize |
| 4 | **The same repository** | `{namePrefix-}{backupID}` — a full-cluster snapshot | This controller's `elasticsearch` component |
| 5 | **Controller's data volume** | `config.json`, `secrets.json`, per-backup log files | This controller |

Two things follow from this that drive the whole design.

**The controller's bucket holds no Camunda data at all.** It records that a
backup happened. If the artifacts in locations 2–4 vanish, every controller-side
status surface still reports the backup as available.

**The component backup APIs are a control plane, not storage.**
`POST {endpoint}` starts a backup and `GET {endpoint}/{id}` reports its status.
The data lands in 2–4. So a component can report a backup as `COMPLETED` while
its underlying snapshots are gone.

---

## 2. How detection works

A **sweep** enumerates every source, joins them on the backup ID, and reports
where they disagree. It is strictly **report-only** — it never deletes anything.

### Evidence sources

| Source | Enumerated by |
|---|---|
| `controller_s3` | `ListAllBackups` across `history/`, `incomplete/`, `orphaned/` |
| `zeebe`, `operate`, `tasklist`, `optimize` | `GET {backup_endpoint}` with no ID appended |
| `elasticsearch` | `GET /_cat/snapshots/{repo}?format=json` |
| `logs` | Log files on the data volume |

The backup ID is a `YYYYMMDDHHMMSS` timestamp, so every artifact has an
intrinsic age even when no metadata survives to describe it. That is what powers
the grace period and lets every report row show a real date.

### Distinguishing snapshots in a shared repository

One repository normally holds snapshots from several writers. A verified 8.6
environment held 115 snapshots in `camunda-backup`: 31 written by the controller
and 84 Operate parts. Names are classified in this order:

1. `camunda_{operate|tasklist|optimize}_{id}_*` → a **component** snapshot
2. `{namePrefix-}{YYYYMMDDHHMMSS}` → the **controller's** own
3. anything else → **foreign** (an SLM policy, another cluster, another tool)

The order matters: with no name prefix configured, rule 2 would otherwise
swallow names that rule 1 should have claimed.

---

## 3. The taxonomy

Severity: `info` (cosmetic) · `warn` (drift, no restore impact) ·
`blocks_restore` (this backup cannot be restored) · `critical` (data loss, or a
backup that looks healthy and is not).

### Group A — Untracked: the artifact exists, the controller has no record

| Code | Condition | Severity |
|---|---|---|
| `A1_UNTRACKED_COMPONENT_BACKUP` | A component holds a backup with no history record | warn |
| `A2_UNTRACKED_ES_SNAPSHOT` | A controller-named snapshot with no history record | warn |
| `A3_UNTRACKED_APP_ES_SNAPSHOT` | Component snapshots remain, but neither the controller nor the owning component knows the backup | warn |
| `A4_FOREIGN_SNAPSHOT` | A snapshot matching no known convention | info |
| `A5_UNTRACKED_LOG_FILE` | A log file with no history record | info |

Usual causes: backups triggered outside the controller; a changed `S3_PREFIX`,
bucket or instance ID; a wiped metadata bucket.

### Group B — Dangling: the controller has a record, the artifact is gone

| Code | Condition | Severity |
|---|---|---|
| `B1_DANGLING_COMPONENT_BACKUP` | Recorded `COMPLETED`, the component no longer lists it | blocks_restore |
| `B2_DANGLING_ES_SNAPSHOT` | Recorded `COMPLETED`, the snapshot is absent from the repository | blocks_restore |
| `B3_DANGLING_APP_ES_SNAPSHOT` | The component still lists the backup, but its snapshots are gone | **critical** |
| `B4_DANGLING_LATEST_POINTER` | `latest-backup-id.txt` names a backup with no record | warn |
| `B5_MISSING_LOG_FILE` | The recorded log file no longer exists | info |

`B3` is critical because every status surface reports the backup as healthy. It
fails only at the moment you try to restore it.

### Group C — Divergence: both sides exist and disagree

| Code | Condition | Severity |
|---|---|---|
| `C1_STATE_DIVERGENCE_COMPONENT` | Recorded `COMPLETED`, the component reports `FAILED`/`INCOMPLETE` | critical |
| `C2_STATE_DIVERGENCE_ES` | Recorded `COMPLETED`, the snapshot is `PARTIAL`/`FAILED` | critical |
| `C3_STALE_IN_PROGRESS_COMPONENT` | Still `IN_PROGRESS` past the polling window | warn |
| `C4_STALE_IN_PROGRESS_ES` | Snapshot still `IN_PROGRESS` past the window | warn |
| `C5_STALE_RUNNING_RECORD` | Record stuck in `RUNNING` with no backup in flight | warn |

`C5` appears after any ungraceful restart: no in-flight backup survives a
process restart, so a `RUNNING` record found afterwards is stale by definition.

### Group D — Partial and split sets

| Code | Condition | Severity |
|---|---|---|
| `D1_PARTIAL_SET` | Present in some, but not all, components enabled **at backup time** | blocks_restore |
| `D2_SPLIT_RESTORE_PAIR` | Zeebe survives without its Elasticsearch snapshot, or the reverse | **critical** |
| `D3_ORPHANED_COMPONENT_IN_FAILED_SET` | The backup failed overall, but components left real data behind | warn |
| `D4_RETENTION_RESIDUE` | Retention deleted the record but the artifact deletion failed | warn |

A Camunda restore needs every component at the same backup ID, which is why
`D2` is critical: the surviving half looks perfectly healthy on its own.

`D3` and `D4` are produced by the controller's own code paths, so expect real
instances of both on any long-running deployment:

- `cleanupIncompleteBackups` ([manager.go:289](../internal/retention/manager.go:289)) deletes only the S3 record, never the artifacts.
- Component and ES deletions during retention are best-effort — they alert and continue ([manager.go:246](../internal/retention/manager.go:246)) — while the record is deleted regardless.

### Group E — Anomalies in the controller's own metadata

| Code | Condition | Severity |
|---|---|---|
| `E1_DUPLICATE_RECORD` | The same backup ID filed in more than one directory | warn |
| `E2_DATE_PATH_MISMATCH` | Filed under a date that contradicts the record's own start time | warn |
| `E3_UNPARSEABLE_RECORD` | Not valid history JSON, or its contents contradict its key | warn ⚠️ |
| `E4_CROSS_INSTANCE_RECORD` | Names a different instance than the prefix it sits under | warn |
| `E5_ABANDONED_INSTANCE_PREFIX` | Backups under an instance ID no longer in config | warn ⚠️ |

⚠️ **`E3` and `E5` are defined but not yet detected.** Both need information a
per-instance sweep does not have:

- `E3` requires the storage layer to surface objects it failed to parse. `ListAllBackups` currently skips them silently, so an unreadable record is invisible rather than reported.
- `E5` requires enumerating bucket prefixes and comparing them against the configured instances. A sweep is scoped to one instance by definition, so it cannot see a prefix belonging to an instance that no longer exists.

Both are documented here because they are real ways a backup becomes orphaned;
they simply need a wider scan than this implementation performs. The reason
codes are reserved so adding detection later does not change the contract.

### Group F — Environment and configuration drift

| Code | Condition | Severity |
|---|---|---|
| `F1_REPO_REBOUND` | The backup's snapshots live in a repository the instance no longer uses | blocks_restore |
| `F2_NAME_PREFIX_DRIFT` | The configured name prefix no longer matches the recorded snapshot name | warn |
| `F3_ZEEBE_BACKUP_STORE_REBOUND` | Zeebe reports none of the recorded backups | **critical** |
| `F4_EXPORTER_LEFT_PAUSED` | Exporting is paused with no backup running | **critical** |

`F1` and `F2` matter because without them a changed repository or prefix would
be reported as missing data. That distinction depends on the backup recording
which repository and snapshot name it actually used, which
`createBackupHistory` now persists.

---

## 4. What is deliberately **not** reported

These look like orphans and must never be flagged. Each is enforced by a test.

| Guard | Rule |
|---|---|
| **Component disabled at backup time** | Judged against the backup's own component map, never current instance config. A component enabled later was legitimately never part of an older backup. |
| **Component newly enabled** | Same reasoning, from the other direction. |
| **Backup in flight** | A record still inside the polling window is skipped, and `F4` is suppressed entirely while a backup is running, because a backup pauses exporting by design. |
| **In-progress artifacts** | `IN_PROGRESS` inside the window is normal, not stuck. |
| **Source unreachable** | **The load-bearing guard.** A refused connection, 401, timeout or 5xx means nothing can be concluded about absence. Every `B*` and `D*` finding depending on that source is withheld. |
| **404 vs. empty** | Components answer `404` when they hold *no* backups. Decoded as an empty list, not an error. |
| **Shared ES repository** | A conventionally-named snapshot the controller cannot match is `A4` (foreign), not `A2`. |
| **Grace period** | Backups younger than `RECONCILE_GRACE_PERIOD_MINUTES` (default 15) are skipped entirely — their artifacts may still be landing. |
| **Deliberate manual backups** | Genuinely untracked, often intentional. Reported, never deleted. |
| **Non-conforming IDs** | An ID that is not `YYYYMMDDHHMMSS` belongs to another tool; classified `info`. |

The unreachable-source guard is why every report carries `sources_checked` and
why the UI shows a chip per source. **A short list of findings from a partial
scan is not a clean bill of health**, and the interface says so explicitly.

---

## 5. When several codes apply to one backup

They routinely do, and some codes *imply* others. The classifier emits every
applicable finding — evidence is never discarded — then a rollup pass produces
one row per backup.

### Implication rules

A suppressed code is not deleted; it moves to the row's `implied` list.

| Rule | Effect |
|---|---|
| `D2` present | Suppresses the `B1`(zeebe) / `B2` it was derived from, and `D1` |
| `D1` present | Suppresses the individual `B1`s that constitute it |
| `F3` present | Suppresses `B1`(zeebe) on **every** backup — one cause, not N orphans |
| `E5` present | Suppresses all per-backup findings under that prefix |
| `F1` or `F2` present | Suppresses `B2` — unreachable by name is not proven absent |
| `C1` present for a component | `B1` cannot also apply — present-but-wrong and absent are exclusive |

The `F1`/`F2` rule is the sharpest: reporting "snapshot missing" after a mere
repository rename would send someone hunting for data loss that never happened.

### Row shape

Each row's `primary_reason` is the highest-severity finding, ties broken by a
most-specific-first precedence order (`D2` > `D1` > `C1` > `B3` > `B2` > `B1` > `A1`).

Findings are also **scoped**: `instance` findings (`E5`, `F3`, `F4`) render as a
banner, `repository` findings (`A4`, `A5`) collapse into a secondary section, and
only `backup`-scoped findings become table rows.

---

## 6. Using it

### In the UI

Orphans appear as ordinary backup rows, because that is what they are: backups
that exist, just with nothing describing them.

**The All tab** lists them alongside real backups, sorted by time. Their status
is `ORPHANED`, and the columns a history record would have supplied — end time,
duration, trigger, components — show an em dash, because those values are
genuinely unknown rather than zero. The start time is real: it is decoded from
the backup ID. A one-line note says which scan the orphan rows came from.

**The Orphaned tab** shows only those rows, plus the scan header: per-source
health chips and any instance-wide findings.

**The refresh button**, at the right of the filter row, is present on every tab.
It runs a fresh scan and then reloads the current view. It rescans rather than
only re-fetching, because orphan rows come from a scan rather than from backup
history, so reloading the history alone would leave them stale. A scan that
fails does not block the reload: the backup history is still worth showing, just
without fresh orphan data.

**View Details** opens the same modal shell as any other backup, listing every
finding with its explanation, impact and what to do. Findings that share a
reason code are grouped into one card, so a backup orphaned in both Zeebe and
Elasticsearch reads as one problem with two observations rather than two
identical cards. For orphans only, a **Commands to remove it** section gives the
exact `DELETE` calls with the real repository, endpoint and snapshot name filled
in — a tracked backup deliberately omits them, because its Delete button goes
through the retention manager's safety guards and raw commands would bypass them.

**A tracked backup with findings** keeps its real status and gains a small `!`
marker. Its details modal grows an *Issues found by the last scan* section. A
backup recorded as `COMPLETED` whose data has actually gone should not look
simply fine.

### API

```bash
# Latest report (404 if no sweep has run yet)
curl -s http://localhost:8080/api/camundas/{id}/backups/reconcile

# Run a sweep now
curl -s -X POST -H 'X-Requested-With: XMLHttpRequest' \
  http://localhost:8080/api/camundas/{id}/backups/reconcile

# Reason catalogue: every code with label, impact and remediation
curl -s http://localhost:8080/api/reconcile/reasons
```

Reason codes are a **stable API contract** and safe to match on from scripts.

Note the difference between `404` (no sweep has run) and a `200` with no
findings (a sweep ran and found nothing) — they are genuinely different answers.

### When it runs

- **On demand**, via the endpoint or the refresh button on the Backups tab.
- **After every backup**, on all terminal states. This is hooked to the orchestrator rather than to retention on purpose: retention runs only for `COMPLETED` and `FAILED`, and an `INCOMPLETE` backup is exactly the case that leaves artifacts behind with no metadata.

Sweeps run asynchronously with their own timeout and can never delay or fail a
backup. Only one sweep runs per instance at a time: a post-backup sweep skips
quietly if one is already running, and the endpoint answers `409`.

### Configuration

| Variable | Default | Purpose |
|---|---|---|
| `RECONCILE_ENABLED` | `true` | Post-backup sweeps. On-demand stays available when off. |
| `RECONCILE_TIMEOUT_SECONDS` | `120` | Cap on one sweep. |
| `RECONCILE_GRACE_PERIOD_MINUTES` | `15` | Backups younger than this are skipped. Must exceed the polling window. |
| `RECONCILE_STALE_AFTER_MINUTES` | `30` | How long in-progress may last before it is called stuck. |

---

## 7. Acting on findings

Detection never deletes. Removing a backup stays deliberate.

For a backup that **has** a history record, use the existing endpoint. It
deletes the backup from every system that holds it — the ES snapshot, each
component's backup, the controller's record and the log file — and still
refuses to delete the most recent successful backup:

```bash
curl -X DELETE http://localhost:8080/api/camundas/{id}/backups/{backupId}
```

If any artifact cannot be deleted the endpoint answers `409 artifacts_remain`
and removes nothing, so the backup does not become an orphan of the kind this
report exists to find. Add `?force=true` to drop the controller's record anyway
once you accept that the leftovers will show up here.

**Known gap.** That endpoint resolves backups through `BackupHistory`, so it
cannot act on `A1`–`A4` — artifacts that by definition have no record. For
those, the UI shows the exact command to run by hand:

```bash
# Component backup
curl -X DELETE {component_backup_endpoint}/{backupId}

# Elasticsearch snapshot
curl -u elastic:$ES_PASSWORD -X DELETE \
  http://elasticsearch:9200/_snapshot/{repository}/{snapshot_name}
```

An artifact-level delete path would close this gap and is tracked separately.

---

## 8. Verifying it locally

```bash
docker compose -f deployments/docker-compose.yaml --profile all up -d --no-build
```

1. Take a backup through the UI, then scan — expect no findings.
2. Delete its snapshot out of band → expect `B2_DANGLING_ES_SNAPSHOT`.
3. Create a backup ID the controller never issued → expect `A1_UNTRACKED_COMPONENT_BACKUP`.
4. Stop Operate and re-scan → expect its findings **suppressed** and `sources_checked.operate.reachable == false`, not a flood of `B1`.
5. Delete a history record but leave the artifacts → expect `D4_RETENTION_RESIDUE`.
6. Delete both the snapshot and the Zeebe backup for one ID → expect **one** row with `D2_SPLIT_RESTORE_PAIR` and `B1`/`B2`/`D1` in `implied`.
