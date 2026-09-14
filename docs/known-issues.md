# Known Issues

Open findings not yet fixed, with enough context to act on without re-deriving
them. Each entry cites `file:line` plus the enclosing symbol, because line
numbers drift and symbol names usually do not.

This file is for what is still true. The one exception is the resolved-criticals
table below, kept only until PR #38 lands so a reviewer can see what the new
guards are for.

**How to use this file:** when you fix an entry, delete it. When a review turns
up something new, append it under the right area heading with the same shape:
what is wrong, why it matters, and the fix. Keep the severity honest — an entry
promoted to CRITICAL should be one that loses or strands data.

---

## Sources

| Date | Origin | Passes |
|---|---|---|
| 2026-09-09 | Pre-landing review of [PR #38](https://github.com/aitasadduq/camunda-backup-dr/pull/38) (orphan + bulk backup deletion) | 6 specialists (testing, maintainability, security, performance, api-contract, design), red team, Claude adversarial, Codex cross-model |
| 2026-09-13 | Pre-landing review of [PR #39](https://github.com/aitasadduq/camunda-backup-dr/pull/39) (outbound notifications) | 6 specialists (testing, maintainability, security, performance, api-contract, simplification) |

Forty-four findings in total. Ten mechanical ones were fixed during the review
and are not listed. The ten criticals were fixed on 2026-09-10 and are
summarised below. The twenty-four informational findings remain open.

The PR #39 review produced twenty-two findings; twenty were fixed during the
review. The two that remain, I25 and I26, are security design points shared
with `exporting_endpoint` and deferred so they can be fixed once, for every
admin-configured URL, rather than for notifications alone.

---

# CRITICAL — resolved

All ten criticals from the PR #38 review were fixed on 2026-09-10. They are
listed here as a record of what the guards exist for; delete this section once
the PR has landed and the reasons live in the code.

| # | Finding | Where the fix lives |
|---|---|---|
| C1 | Orphan deletion had no ownership check, so it could delete another instance's live backups | `retention.Manager.verifyOrphanOwnership` |
| C2 | No RUNNING guard on the orphan path | `DeleteOrphan`'s in-flight check, wired via `SetBackupRunningFunc` in `cmd/server/main.go` |
| C3 | `SnapshotNames` was empty for the commonest orphan shape, so snapshots survived a "successful" delete | `BackupIssue.AllSnapshotNames`, populated by `observedSnapshots` in `internal/reconcile/reconciler.go` |
| C4 | Report reachability was ignored, reporting success while artifacts remained | `OrphanArtifacts.Complete`, refused as `report_partial` |
| C5 | `errors.Is(ErrBackupNotFound)` as the tracked/orphan discriminator misrouted a successful delete | `Handlers.isTracked` — an explicit record probe |
| C6 | Endpoints resolved from current config rather than the sweep that found the orphan | `retention.Manager.verifyEndpointsUnchanged` |
| C7 | Snapshot names reached the Elasticsearch DELETE URL unvalidated | `isAddressableSnapshotName` |
| C8 | The bulk endpoint could not deliver its own result | Context threading, a 90s budget, a cap of 25, and the report read once per request |
| C9 | `DELETE /backups/{id}` stopped returning 404 for an unknown ID | `deleteFailureResponse` maps `no_report` to 404 |
| C10 | The Orphaned tab could delete a tracked backup while calling it an orphan | Untracked rules gated on `SourceControllerS3` reachability, `ListAllBackups` failing loudly on a partial read, and a live cross-check in `loadOrphanedBackups` |

---

# INFORMATIONAL — open

## Correctness and consistency

### I1 — `deleteSnapshot` and `deleteComponentBackup` report success on a half-wired manager

`internal/retention/manager.go:304` (`deleteSnapshot`), and
`deleteComponentBackup` in the same file

`deleteSnapshot` returns nil when `m.cfg == nil`; `deleteComponentBackup`
returns nil when `m.httpClient == nil`. `deleteOrphanSnapshots` and
`deleteOrphanComponents` pre-check the *endpoint* but not these, so a manager
missing a dependency reports an orphan as fully deleted having issued no
request — and `DeleteOrphan` logs `Deleted orphaned backup ... snapshots=<names>`
for snapshots it never touched.

Unreachable in production (`main.go` always supplies both), reachable in tests,
which is exactly where it would mask a regression.

**Fix:** make these return an error rather than nil, or drop the guards and let
each caller own the decision. One condition should not mean "no-op" in one place
and "hard failure" in another.

### I2 — The latest-backup-id pointer is never repointed after a deletion

`internal/retention/manager.go` (`DeleteBackup`, `DeleteOrphan`)

Only the orchestrator writes the pointer (`StoreLatestBackupID`), and the
classifier reports `B4_DANGLING_LATEST_POINTER` when it names an ID with no
record. The pointer names the most recent backup *attempt*, so it routinely
names a FAILED or INCOMPLETE backup — which the most-recent guard does not
protect, since that guard only checks `BackupStatusCompleted`.

Deleting that one — now easy via the bulk endpoint — leaves every subsequent
sweep reporting a dangling pointer that only hand-editing S3 can clear.

**Fix:** after a successful record deletion, read `GetLatestBackupID` and, if it
names the deleted ID, repoint it at the newest surviving record or delete the
pointer object.

### I3 — No mutual exclusion between manual deletion and the scheduled retention cycle

`internal/retention/manager.go:93` (`pruneByStatus`)

`ApplyRetention` runs from a goroutine after every terminal backup and takes no
lock; neither delete path takes one, and neither respects the orchestrator's
`atomic.Bool` or the reconciler's `sweepRunning`.

`pruneByStatus` computes its victim list from a snapshot (`toDelete :=
backups[keep:]`), so a concurrent delete that removes one of `backups[:keep]`
makes retention prune below its own configured floor: with
`SuccessRetention=3` and four COMPLETED backups, retention picks the oldest, and
a concurrent delete of the second-newest leaves two recovery points instead of
three. The bulk endpoint widens the window considerably.

**Fix:** serialise per-instance deletion and retention behind one mutex (a bulk
delete acquires it once for the batch), or re-verify immediately before each
record deletion that the backup is still outside the keep window.

### I4 — The reconciler's sweep lock is process-wide but reported as per-instance

`internal/reconcile/reconciler.go:120`, message at
`internal/api/reconcile.go:111`

`sweepRunning` is a single `atomic.Bool` on one shared `Reconciler`, so a sweep
on any instance blocks sweeps on every other one — and the API reports "A
reconciliation sweep is already running for this instance", which is false.

Cosmetic while sweeps were informational. Now the orphan delete flow depends on
an immediately-following sweep, so a routine post-backup sweep on instance B
turns an orphan deletion on instance A into a stale row (see I5), with a message
that sends the operator hunting on the wrong instance.

**Fix:** make the lock per-instance (a mutex-guarded map keyed by instance ID)
and correct the 409 message when it really is a different instance.

### I5 — A refused sweep resurrects a just-deleted orphan row

> Partly mitigated on 2026-09-10: `loadOrphanedBackups` now cross-checks the
> report against live backup history, so a row whose record exists is dropped.
> A deleted orphan has no record either way, so the resurrection below stands.


`web/js/app.js:1947` (`reloadAfterDelete`)

The orphan branch delegates staleness entirely to a fresh sweep and prunes
nothing locally, while the tracked branch does prune
`reconcileReport.backup_issues`. But `refreshBackups` swallows sweep failures —
its catch only shows a toast and leaves the stale report in place — so whenever
the sweep is refused the deleted row is re-rendered right after the success
toast.

Refusal is routine: `RunReconcileHandler` returns `409 sweep_in_progress`
whenever the process-wide lock is held, which is exactly the state during the
post-backup sweep goroutine. On the Orphaned tab it is worse, because
`loadOrphanedBackups` re-reads the stored server-side report with
`force: true`, discarding any local fix.

**Fix:** prune `reconcileReport.backup_issues` for the deleted IDs on *both*
branches before attempting the sweep, and have `loadOrphanedBackups` drop
locally-pruned IDs so a refused sweep cannot resurrect them.

## Security and abuse

### I6 — The API has no authentication or authorization

`internal/api/server.go:59`

The middleware chain is recovery → logging → CORS → CSRF → content-type with no
auth stage, and no route wraps one. Anyone who can reach the port can destroy up
to 200 backups across Elasticsearch and every Camunda component with one POST;
`X-Requested-With` is trivially set by a non-browser client, and CSRF only stops
cross-origin *browser* requests.

Pre-existing and consistent across the API (single delete, instance delete and
config writes are equally unauthenticated), but bulk deletion materially raises
the blast radius.

**Fix:** gate the API behind authentication ahead of CSRF in the chain. If that
stays out of scope, state the reliance on network-level restriction in
`docs/api.md` next to the destructive endpoints.

### I7 — `artifacts_remain` returns internal topology to the caller

`internal/api/backup_delete.go:274` (`deleteFailureResponse`)

The message is `err.Error()` — the joined output of every component and snapshot
failure: internal endpoint hosts and ports, any basic-auth username embedded in
an endpoint URL, the repository name, and the raw Elasticsearch response body
from a failed delete. The bulk response repeats it per backup, so one request
can enumerate the internal topology of every component and the ES cluster's
error output.

**Fix:** return a stable message naming only which components still hold
artifacts, and log the detailed joined error server-side. Stop propagating the
raw ES response body into the client-visible error chain.

### I8 — Alert storm: one bulk request can spawn ~1000 concurrent webhook goroutines

`internal/utils/alerts.go:102` (`go a.sendAsync(alert)`)

`purgeBackupArtifacts` alerts per *failed artifact* and `DeleteOrphan` alerts per
orphan, with no dedupe and no rate limit, each spawning a goroutine with a 10s
timeout. 200 backups x up to 5 artifacts with ES down is ~1000 concurrent POSTs
from a single HTTP request. Before bulk delete the only batch caller was
retention, bounded by the keep-N counts.

**Fix:** aggregate to one alert per batch.

### I9 — Bulk request ID length is uncapped

`internal/api/backup_delete.go:133` (`validateBulkDeleteIDs`)

The count is capped at 200 and the body at 64 KB, but individual ID length is
not. A body of 200 long bogus IDs is a cheap S3-cost amplifier on an endpoint
whose only gate is a header. Largely mitigated by the `IsBackupIDShaped` check
further down the path, but the S3 lookups in `DeleteBackup` happen first.

**Fix:** reject any ID longer than 14 characters in `validateBulkDeleteIDs`.

### I25 — Admin-configured URLs are SSRF-guarded only at save time

`internal/api/handlers.go` (`validateNotifications`, `validateExportingEndpoint`);
`internal/notify/notifier.go` (`NewNotifier`); `internal/orchestrator/orchestrator.go`
(`callExportingEndpoint`)

Both the notification URL and `exporting_endpoint` are checked against
private/loopback ranges when the instance is saved, by resolving the hostname
once (`isBlockedHost`). The request itself is sent later through a plain
`http.Client`. A hostname that resolves publicly at save time and privately when
a backup finishes lands an admin-shaped request on an internal address, and a
hostname that fails to resolve at save time is accepted outright — the guard
fails open because the probe endpoint it was written for has a follow-up
request to catch it, and these paths do not. The probe client in
`internal/api/endpoint_check.go` already closes this gap for itself with a
dial-time IP check on the connected `RemoteAddr`.

This is the existing threat model for every admin-configured URL, and the
person who can save an instance can already point S3 and Elasticsearch
anywhere, which is why it was deferred rather than fixed for one call site.

**Fix:** move `isPrivateIP`, `privateIPNets` and `isSSRFCheckDisabled` out of
`internal/api` into a shared package and build one guarded `http.Client`
(dial-time `RemoteAddr` check, `PROBE_ALLOW_PRIVATE_IPS` escape hatch) used by
the notifier and by `callExportingEndpoint` alike. Have `validateNotifications`
and `validateExportingEndpoint` treat a DNS failure as a rejection. The notifier
tests dial `httptest` servers on loopback and will need
`t.Setenv("PROBE_ALLOW_PRIVATE_IPS", "true")`.

### I26 — Notification URLs and bodies are stored and served in plaintext

`internal/models/notification.go` (`NotificationRequest`); `internal/storage/file.go`
(`SaveConfiguration`, mode 0644); `internal/api/handlers.go` (`ListCamundaInstancesHandler`,
`GetCamundaInstanceHandler`)

The notification URL is redacted before it is logged because webhook URLs
routinely carry tokens, but the same URL is persisted verbatim in
`config.json` and returned in full by the unauthenticated list and get
endpoints. `Validate` also accepts userinfo (`https://user:password@host/...`),
which Go's client turns into an `Authorization: Basic` header, so a real
password can be stored in `config.json` — against the rule that credentials go
to `internal/secrets`, never to config. The body has the same exposure and can
carry an API key of its own.

**Fix:** treat the URL (or at least its userinfo and a designated secret
header) like `ElasticsearchPassword`: a write-only field stripped by
`ClearTransientFields`, stored through `internal/secrets`, resolved by the
notifier at send time, and reported to the UI only as a `_set` marker. Until
then, refuse userinfo in `NotificationRequest.Validate` and say in `docs/api.md`
that notification URLs and bodies are visible to anyone who can read the API.

## Maintainability

### I10 — The component→endpoint mapping is stated three times, and two copies disagree on labels

`internal/retention/manager.go:649` (`componentEndpoint`) vs the inline switch at
`internal/retention/manager.go:248` (`purgeBackupArtifacts`), plus
`componentsHolding` in `internal/api/backup_delete.go` as a source filter

Adding a fifth component requires finding all three. The two switches also pass
different labels to `deleteComponentBackup` — `"Zeebe"` on the tracked path,
`"zeebe"` on the orphan path — so the same failure reads differently depending
on which path produced it.

Not auto-fixed because unifying the labels changes existing error text that
tests may assert, and the refactor touches the tracked retention path.

**Fix:** have `purgeBackupArtifacts` resolve its endpoint via
`componentEndpoint(instance, componentName)` and settle on one label form.

### I11 — The UI re-implements the server's backup-ID rule

`web/js/app.js:1633` (`isBackupIdShaped`), consumed by `isBackupDeletable`
(`web/js/app.js:1645`) and the orphan detail dialog

`/^\d{14}$/` duplicates `camunda.IsBackupIDShaped`
(`internal/camunda/backup_id.go`). Byte-identical today — both are "length 14,
all ASCII digits", and JS `\d` is ASCII-only — so nothing is broken now.

But it is the sole gate on two pieces of UI, and the server already ships every
field the browser needs for this decision except this one: `reconcile.BackupIssue`
carries `Tracked`, `PresentIn` and `SnapshotNames` and no deletability flag. If
the Go rule ever loosens, the UI silently hides Delete for orphans the server
would happily remove — and there is no test harness in `web/` that could catch
it.

**Verdict from review: drift hazard, not justified duplication.**

**Fix:** add a server-computed `deletable bool` to `reconcile.BackupIssue`, set
from `camunda.IsBackupIDShaped` where the issue is built, and have the UI read
it. Keep `isBackupDeletable`'s status rules, which have no Go counterpart, on
the client.

### I12 — `planning/checklist.md` is gitignored but referenced by CLAUDE.md

`.gitignore:37` ignores `planning/`, while CLAUDE.md says "For implementation
status, see `planning/checklist.md`".

Pre-existing, unrelated to PR #38. A fresh clone does not have the file the
conventions point at.

**Fix:** either track `planning/` or point CLAUDE.md at something that ships.

## Design and accessibility

### I13 — Checkbox hit target is 14px, below the WCAG minimum

`web/css/styles.css:548` (`.row-select`)

`width: 0.875rem; height: 0.875rem` with no enlarged hit area: the containing
`<td>` is not clickable and the input is not wrapped in a `<label>`, so the only
target is the 14px box — below WCAG 2.2 SC 2.5.8's 24px minimum and far below
the 44px touch guidance, on the control that arms an irreversible bulk delete.
On mobile the cell padding drops to 0.5rem, putting adjacent rows' boxes ~22px
apart.

**Fix:** keep the visual box but grow the target — wrap the input in a label
that fills the cell (`display: block; padding: 0.5rem;`) so the whole cell
toggles, moving the `aria-label` onto the label.

### I14 — The multi-backup confirmation names no backup IDs

`web/js/app.js:1804` (`renderSelectionSummary`)

The dialog shows a status badge and a count per status, so the user is told
"39 COMPLETED" with no way to see *which* 39 — the modal backdrop covers the
table they ticked, and the history is unpaginated, so select-all can span an
instance's entire restore history. The irreversibility is legible; the scope is
not.

**Fix:** list the IDs grouped under each status heading — `.delete-summary` is
already a scrolling list with `max-height: 14rem` built for it — or at minimum
name the oldest and newest start time in the selection.

### I15 — The most recent successful backup is always selectable but can never be deleted

`web/js/app.js:1645` (`isBackupDeletable`)

Every COMPLETED backup gets a checkbox, but `DeleteBackup` refuses the most
recent COMPLETED one unconditionally, before `force` is consulted. So select-all
always includes a row guaranteed to fail, and "select all → Delete N" always
ends in the "Backups left in place" modal reporting a refusal the UI could have
predicted. The frontend handles the refusal correctly; it just should not have
offered the control.

The adversarial pass also noted the flip side: select-all plus one click deletes
every backup except the newest, and the only thing between a mis-click and total
loss of DR capability is that single guard — which protects exactly one record
and never checks that it is actually restorable (its ES snapshot may be
`PARTIAL`).

**Fix:** compute the newest COMPLETED row from the rendered set and either
exclude it from `isBackupDeletable` or render its controls disabled with a
`title` explaining why. Consider requiring a typed confirmation when a selection
contains COMPLETED backups.

## Test coverage

All of these are gaps where the guard could be removed and the suite would stay
green.

### I16 — `DeleteOrphan`'s "is it really untracked?" guard has no negative test

`internal/retention/manager.go:542`

The `} else if !errors.Is(err, utils.ErrBackupNotFound)` branch is uncovered,
and `mockS3Storage.GetBackupHistory` can only return nil or
`ErrBackupNotFound` — so "a transient S3 read failure misread as *no record
exists*" is not expressible in the current suite. Flipping the guard to
proceed-anyway deletes the artifacts of a possibly-tracked backup with every
test still passing.

**Fix:** add a `historyErr error` field to `mockS3Storage`, return it from
`GetBackupHistory`, and assert `DeleteOrphan` refuses and issues zero DELETEs
when the lookup fails for a reason other than not-found.

### I17 — The Elasticsearch leg of `DeleteOrphan`'s invariant is unasserted

`internal/retention/manager.go:614`

`TestDeleteOrphan_ReportsSurvivingArtifacts` fails only a *component* (it passes
`Components` and no `SnapshotNames`), so no test proves a surviving snapshot
yields `ErrBackupArtifactsRemain` rather than success.

**Fix:** add a case passing `SnapshotNames` while the stub answers the snapshot
DELETE with 500 — `newOrphanTestEnv.failComponents` already returns the injected
status for `/_snapshot/` paths — asserting `ErrBackupArtifactsRemain` and that
the message names the snapshot.

### I18 — `deleteOrphanSnapshots`' refusals and config fallback are uncovered

`internal/retention/manager.go:614`

Neither "no Elasticsearch endpoint configured" nor "no snapshot repository
resolved" is tested, nor the `repository == "" && m.cfg != nil` fallback. The
component-side equivalent *is* tested
(`TestDeleteOrphan_ReportsUnconfiguredComponent`), so the asymmetry is the tell.

**Fix:** three cases — ES endpoint empty with `SnapshotNames` set; `Repository`
empty against a manager whose cfg resolves nothing; and `Repository` empty with
cfg set, pinning that the fallback reaches the DELETE path.

### I19 — `DeleteOrphan`'s alerting branch never runs in tests

`internal/retention/manager.go:573`

`newOrphanTestEnv` wires no alerter, so the orphan path has no equivalent of
`TestDeleteComponentBackup_AlertsOnBadStatus`. A lost alert on surviving orphan
artifacts would go unnoticed.

**Fix:** set the mock alerter in a `DeleteOrphan` failure test and assert
`AlertCleanupFailed` fires once with the backup ID and a reason naming the failed
component.

### I20 — No bulk test exercises the orphan branch or a mixed batch

`internal/api/backup_delete.go:103`

Every backup in the bulk tests resolves through `DeleteBackup`
(`deleteTestSetup` leaves `ret.deleteErr` nil), so `ret.deletedOrphans` is never
asserted on a bulk request and a batch mixing a tracked backup with an orphan is
untested.

**Fix:** one test where one ID is tracked and one is untracked but present in
the report as an orphan, asserting both land in `resp.Deleted` and that
`deletedOrphans` holds exactly the orphan with the report's artifacts.

### I21 — The empty-ID and `"."` validation guards are untested

`internal/api/backup_delete.go:133`

`TestBulkDeleteBackupsHandler_RejectsPathSeparators` iterates
`{"../other-instance/20260320080000", "a/b", "a\\b", ".."}` and never `""` or
`"."`. An empty ID otherwise reaches `DeleteBackup` and addresses the instance's
backup prefix rather than one backup.

**Fix:** add `""` and `"."` to the table, asserting 400 and zero deletion
attempts.

### I22 — The bulk unmapped-error path has no test

`internal/api/backup_delete.go:103`

The 500-class logging was added, but nothing asserts that an unmapped error
surfaces as `internal_error` in `resp.Failed`.

**Fix:** inject a plain error for one ID and assert the code.

### I23 — The UI selection logic has no automated coverage, and cannot get any here

`web/js/app.js:1645`

`syncSelectionToRows` prunes ticks to visible rows and `isBackupDeletable` gates
out RUNNING and non-controller-format orphan IDs, with no test framework or
toolchain in `web/` to exercise either. The only backstop is server-side.

**Fix:** do not add JS unit tests. Instead (a) add a bulk test asserting a
RUNNING backup in a batch comes back as `safety_refusal` while its siblings
delete, and (b) keep a manual check in the PR: select-all with a RUNNING row
present, change the filter, reload, and confirm the bulk bar count never
includes a row the user cannot see and that the confirmation counts ORPHANED
separately.

### I24 — Nothing covers concurrency, cancellation, or a batch at the cap

Raised by the adversarial pass from test names only.

No test covers concurrent or duplicate deletion of the same ID, request-context
cancellation, a partial batch, refusal while a backup is in flight, a report
with unreachable sources, snapshot-name validation, or a batch at the 200 cap.
Several of these are the mechanisms behind C2, C4, C5 and C8.
