# 05 — `prune_dead_hashes.py` (Code Reference)

> **Role:** Weekly housekeeping — deletes hashes that have been **scanned but found dead**
> (no real peers) for N+ days, so the tracked set and the main DB stay lean. **Mutates
> `hashes_v2.db`** (DELETE), so it has dry-run + age-guard safety rails.

---

## Where it runs
- **cron:** `Sun 04:00 UTC` → `prune_dead_hashes.py --days 7 --min-age-days 14 --vacuum --yes`.
- **Args:** `--days N` (dead-for-N-days threshold) · `--min-age-days N` (protect hashes newer than this) · `--category` (limit to one) · `--vacuum` (reclaim file space after) · `--yes` (non-interactive) · `--dry-run` (report only).

---

## What counts as "dead" (the precise rule)
A hash is prunable only if **all** hold:
1. **No real peer** (`ip != '_queried_'`) seen in the last `--days` days.
2. It **was actually scanned** — there's a `_queried_` sentinel row (so we don't prune hashes we simply never got to).
3. It's **old enough** — `first_seen <= today − min_age_days` (protects brand-new releases that haven't ramped yet).

That second condition is the key safety: "dead" = *"we looked and nobody's there"*, not *"we have no data"*.

---

## Functions

### `get_stats(conn, days, category, min_age_days=14) -> (total, never_count, dead_rows, protected)`  (34)
Pure **read/report** — runs the SQL above:
- `dead_sql` → the prunable rows (the 3-condition rule), returned for deletion.
- `never_sql` → count of hashes scanned but **never** found (subset insight).
- `protected_sql` → count too new to prune (guarded by `min_age_days`).
- plus the grand total. **Reviewer note:** uses `NOT EXISTS (real peer ≥ cutoff) AND EXISTS (_queried_ sentinel)` — both halves matter; dropping the sentinel check would prune un-scanned hashes.

### `prune(conn, dead_rows, dry_run) -> (deleted_hashes, deleted_peers)`  (95)
Deletes in batches of `BATCH_SIZE`: for each chunk, `DELETE FROM peers WHERE hash IN (…)` then `DELETE FROM hashes WHERE hash IN (…)`, committing every ~5000. Returns 0,0 immediately if `dry_run`. **Reviewer note:** peers deleted first, then hashes — order is cosmetic here (no FK), but keeps the two counts meaningful.

### `main()`  (120)
Parse args → open DB → `get_stats` → print the breakdown → if not dry-run and (`--yes` or confirmed) → `prune` → optional `VACUUM` to return freed pages to the OS.

---

## Gotchas / invariants (for reviewers)
- **Destructive** — always safe to `--dry-run` first; the cron uses `--yes` non-interactively.
- **`min-age-days` guard** prevents pruning new releases mid-ramp (they legitimately have no peers on day 0).
- **`--vacuum`** rewrites the DB file to reclaim space — it takes an exclusive lock briefly; fine in the Sunday quiet window but don't run it during heavy write periods.
- A pruned hash that becomes popular again is simply re-discovered by the collectors — pruning costs nothing if it comes back.

## Change history
Standard since the early pipeline; weekly cron in the consolidated root crontab (`00_OVERVIEW.md`).
