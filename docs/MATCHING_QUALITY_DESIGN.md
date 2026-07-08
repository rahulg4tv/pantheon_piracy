# Matching-Quality Overhaul — Design

Status: **DESIGN / not built.** Author target: a few focused sessions, phased.
Goal: stop torrents getting tagged to the wrong `ip_id` *by design*, instead of patching
`_name_matches_title` one rule per bug.

---

## 1. Problem & root cause
Every mis-map we've hit (One Piece / One Piece 4D, Michael / Michael Clayton, Masters 1987 vs
2026, Tongari Boushi no Atelier → Memole, Culpa nuestra → Our Fault) has **one** cause: we match a
torrent to **one canonical English title** via fuzzy word-overlap. That structurally fails on:

| Class | Example | Current patch |
|---|---|---|
| Franchise sibling (shared words) | One Piece / One Piece 4D | n≤3 all-words rule |
| Generic short title | Michael / Michael Clayton | short-title parsed-title equality |
| Same title, different year | Masters 1987 / 2026; Toy Story 5/3 | confident year-mismatch |
| **Foreign / alternate title** | Tongari Boushi no Atelier ⇄ Witch Hat Atelier | **none — unfixable by thresholds** |

The last class is unfixable by tuning thresholds: the torrent and the catalog title share **zero**
words. The model must change.

## 2. Three pillars (in dependency order)
```
Pillar 1  ID + alias matching      → match on imdb/MAL id and the FULL set of title variants
Pillar 2  best-match disambiguation→ assign to the single best candidate, not first-over-threshold
Pillar 3  active audit             → quarantine what the matcher can't justify; LLM judge on the residue
```
Pillar 1 shrinks what Pillars 2 & 3 must handle. Build in order.

---

## 3. Pillar 1 — ID + alias matching  (biggest leverage)

### 3.1 New table: `title_aliases`
```sql
CREATE TABLE title_aliases (
  ip_id      TEXT NOT NULL,
  alias      TEXT NOT NULL,        -- raw variant, e.g. "Tongari Boushi no Atelier"
  alias_norm TEXT NOT NULL,        -- _normalize()'d for matching
  source     TEXT,                 -- 'canonical' | 'tmdb_alt' | 'mal_synonym' | 'manual'
  lang       TEXT,                 -- 'en','ja-romaji','ja','es', ...
  PRIMARY KEY (ip_id, alias_norm)
);
CREATE INDEX idx_alias_norm ON title_aliases(alias_norm);
```

### 3.2 Builder (offline, runs with the catalog sync)
`build_title_aliases.py` — for every catalog `ip_id`:
- always insert the **canonical title** (`source='canonical'`).
- **movies/series:** pull TMDB **`/movie|tv/{id}/alternative_titles`** (we have `imdb_id` → TMDB id) →
  insert each (`source='tmdb_alt'`, with its `iso_3166_1`).
- **anime:** pull **AniList/MAL synonyms** (we have `mal_id`) — `title.romaji`, `title.english`,
  `title.native`, plus `synonyms[]` (`source='mal_synonym'`).
- `alias_norm = _normalize(alias)` (same normalizer the matcher uses).
- a `manual` source lets us hand-add fixes (e.g. known scene spellings).
Run it on the catalog-sync cadence; it's a catalog-side artifact (can build OFF-BOX and ship the table,
keeping the prod box clean). ~80–90k ip_ids × a few aliases ≈ a few hundred k rows — tiny.

### 3.3 Revised resolution order (in the collector)
For each torrent (`raw_name`, plus any source-provided ids):
1. **ID match (exact, highest confidence).** If the source row carries an `imdb_id`/`mal_id`
   (apibay & nyaa often do) → look up the catalog entry directly → **done**. No fuzzy step.
2. **Alias candidate generation.** Parse the torrent title (PTN). Find catalog `ip_id`s whose
   `alias_norm` shares significant words with the parsed title (index lookup on word tokens).
3. Hand the candidates to **Pillar 2** to pick the winner.
This makes fuzzy matching the *fallback*, and matches against *all* title variants — killing the
foreign-title/AKA class wholesale.

---

## 4. Pillar 2 — best-match disambiguation

Replace "accept the first catalog entry that clears the bar" with "**score all candidates, take the
single best, and only if it's clearly best.**" This subsumes the short-title and year-mismatch patches.

### 4.1 Per-candidate score
```
score(torrent, candidate) =
    w1 * best_alias_overlap        # max word-overlap across the candidate's aliases (0..1)
  + w2 * year_agreement            # +1 exact (±1), 0 unknown, −1 confident mismatch (movies/series)
  + w3 * specificity_bonus         # reward candidate whose alias is fully covered (no extra torrent words)
  − w4 * extra_word_penalty        # torrent title has significant words beyond the matched alias
```
Suggested start: `w1=1.0, w2=0.5, w3=0.3, w4=0.4` (tune on the shadow set, §7).

### 4.2 Assignment rule
```
best, second = top-2 candidates by score
assign IFF  best.score >= ACCEPT (e.g. 0.6)
        AND (best.score - second.score) >= MARGIN (e.g. 0.15)
else → leave UNMATCHED  (feeds the unmatched-mining path / review queue)
```
The **margin** is what fixes siblings: "One Piece" must beat "One Piece 4D" by a clear gap, else neither
wins. Year mismatch becomes a *score penalty*, not a special-case `return False`.

---

## 5. Pillar 3 — active audit (self-correcting backstop)

Turns `post_ingest_audit` from *log-only* into *act*, with an LLM judge for the gray zone.

### 5.1 New tables
```sql
CREATE TABLE hash_quarantine (         -- removed from the feed pending resolution
  hash TEXT, ip_id TEXT, raw_name TEXT, reason TEXT,
  flagged_at TEXT, verdict TEXT,       -- 'pending'|'reject'|'remap'|'keep'
  target_ip_id TEXT, confidence REAL, decided_by TEXT,  -- 'rule'|'llm'|'human'
  PRIMARY KEY (hash, ip_id)
);
CREATE TABLE match_review (            -- low-confidence items for a human
  ip_id TEXT, reason TEXT, detail TEXT, created_at TEXT, status TEXT
);
```

### 5.2 Tier 1 — deterministic, every collection cycle
For each newly-touched `ip_id`, compute (mostly already in `post_ingest_audit`):
- **title-consistency** — % of hashes whose parsed title carries the catalog's *distinguishing* word.
- **year-cluster** — fraction of hash years within ±1 of the catalog year (movies/series).
- **title-cluster count** — # of distinct parsed-title clusters among the hashes (>1 ⇒ likely merge/mis-map).

Hard actions (no LLM):
- distinguishing word absent from a hash **and** an alternative catalog entry exists → **quarantine** that hash.
- a title whose consistency `< 0.5` → **quarantine** the minority-cluster hashes; flag the `ip_id`.
Thresholds are config, start conservative. Every quarantine backs up the row first (jsonl, like the
manual cleanups) and is fully reversible.

### 5.3 Tier 2 — LLM judge on the flagged residue (cron, headless)
Only the items Tier 1 flags but can't decide. `audit_judge.py` (cron, e.g. 07:00Z):
1. read flagged `ip_id`s (cap **N≤50/run**).
2. per item, call the Claude API (Anthropic SDK — a **script**, not the interactive harness) with:
   - catalog: `title`, `year`, `category`, **aliases** (from Pillar 1), `imdb_id`/`mal_id`
   - the hashes' `raw_name`s (top ~20 by seeders)
3. force a structured verdict (StructuredOutput / JSON schema):
```json
{ "ip_id":"anime-3754", "verdict":"remap|reject|keep|split",
  "target_ip_id":"anime-51553", "confidence":0.0-1.0,
  "reason":"all raw_names are 'Tongari Boushi no Atelier' = Witch Hat Atelier" }
```
4. **apply policy:**
   - `confidence ≥ 0.90` → auto-apply (remap/quarantine), **back up rows first**.
   - `0.6 ≤ conf < 0.9` → write to `match_review` for a human.
   - `< 0.6` → leave as-is, log only.
   Hard cap on rows touched per run; never delete on low confidence.

Model: a small/cheap tier is plenty (title-equivalence is easy). Cost ≈ a few dozen calls/day ⇒ pennies.

### 5.4 Why this is safe
Judge, not matcher (bulk still deterministic) · only flagged residue · confidence-gated auto-apply ·
every write backed up & reversible · per-run caps · humans own the ambiguous tail.

---

## 6. Data-model summary
New: `title_aliases`, `hash_quarantine`, `match_review`. Existing `hashes`/`titles`/`peers` unchanged
except hashes gain an effective "in-feed" filter = `NOT EXISTS (quarantine row with verdict!='keep')`.
The merge/export must **exclude quarantined hashes** (one `WHERE` clause).

## 7. Rollout (phased, each independently shippable)
- **P0 — alias table.** Build `title_aliases` OFF-BOX, validate coverage, ship the table. No behaviour change.
- **P1 — best-match resolve in SHADOW.** Run the new resolver alongside the old one, log disagreements to a
  table; review the diff (this is the "scan blast radius" step that's saved us twice). Promote behind a flag.
- **P2 — Tier 1 active audit.** Quarantine on hard rules; export excludes quarantined. Monitor false-quarantine rate.
- **P3 — Tier 2 LLM judge.** Start in **dry-run** (write verdicts to `match_review`, auto-apply nothing);
  once verdict quality is trusted, enable auto-apply at ≥0.90.

## 8. Metrics to watch
- # disagreements (P1 shadow), # quarantined/run, false-quarantine rate (sampled), LLM auto-apply rate &
  override rate (humans correcting the LLM), and the headline: **# user-spotted mis-maps/week → target 0.**

## 9. Risks & mitigations
- **Alias false merges** (two different works share an alias) → keep `imdb_id`/`mal_id` as the tiebreaker;
  alias match alone never overrides an ID match.
- **Over-quarantine** → conservative thresholds, dry-run first, sampled human audit, easy un-quarantine.
- **LLM wrong with high confidence** → cap rows/run, back up every write, track override rate, keep the
  ≥0.90 gate adjustable.
- **Prod-box load / WAL** → build alias table off-box; audit/judge run as small scheduled jobs, not inline.

## 10. Open questions
- Which torrent sources expose `imdb_id`/`mal_id` today, and how reliably? (audit the collector's source rows)
- Episode-level signal for anime (S/E) as an extra disambiguator?
- Do we want a tiny review UI in the dashboard for `match_review`, or just a CSV?
