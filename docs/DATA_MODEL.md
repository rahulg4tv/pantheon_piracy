# Data model & core concepts

Read this before the per-component docs. Four ideas explain almost every design
decision in the pipeline: **the infohash**, **the `ip_id`**, **the distinct-IP
union**, and **the day window**. Most of the subtle bugs this system has had came
from getting one of them slightly wrong, so each section ends with the trap.

---

## 1. The infohash — what we actually track

A torrent is identified by its **infohash**: the SHA-1 of its bencoded `info`
dictionary. Two people sharing the same release compute the same infohash, which
is what makes swarm measurement possible at all.

One title has **many** infohashes — 1080p, 720p, a REMUX, a dubbed release, each
season pack. So the pipeline is a chain of one-to-many relationships:

```
title (ip_id)  ──1:N──▶  infohash  ──1:N──▶  peer IP (per country, per day)
```

Everything downstream is about collapsing that back down to **one number per
title per country per day** without counting a person twice.

---

## 2. `ip_id` — the title identity

`ip_id` is the catalog's stable identifier for a work. It is **never minted by
this pipeline** — it comes from the Pantheon catalog parquet. Forms:

| Form | Meaning |
|---|---|
| `film-tt0114709` | Movie, keyed on IMDb id |
| `series-tt0386676` | Series, keyed on IMDb id |
| `anime-21` | Anime, keyed on **MyAnimeList** id |
| `film-Q171048`, `series-Q109526557` | **Legacy** Wikidata ids, being migrated out |

Two rules that matter:

- **Match on `imdb_id`, not on the title string.** Titles collide constantly
  (*Robin Hood* the 1991 film and the 2025 series; two films both called *Scary
  Movie*). The imdb/MAL id is the only reliable key.
- **A missing `ip_id` is allowed and is not a failure.** The feed carries an
  `UNMAPPED` flag for real demand on a title that is not in the catalog yet. That
  is honest signal — it surfaces catalog gaps. Inventing an id, or attaching the
  demand to a *similar* title, is far more expensive than admitting the gap.

> **Trap.** `hashes.title` is a denormalised copy of `titles.title` and drifts
> from it. Group and join on `ip_id`; treat any title string as a display label.

---

## 3. The stores

| Store | Written by | Holds |
|---|---|---|
| `hashes_v2.db` → `hashes` | collectors | one row per infohash: `ip_id`, title, category, seeders, source, first/last seen |
| `hashes_v2.db` → `titles` | catalog refresh | one row per `ip_id`: canonical title, category, imdb id |
| `hashes_v2.db` → `peers` | DHT workers | DHT-observed peer IPs |
| `harvest_peers.db` | tracker-harvest | tracker-announce peer IPs |
| `pex_peers.db` | pex-harvest | PEX-gossiped peer IPs |
| `harvest_velocity_peers.db` | harvest-velocity | re-harvest lane for fast-moving new releases |

Every peer table has the same shape: `(hash, ip, country, first_seen, last_seen)`.

**Why four separate databases rather than one.** SQLite in WAL mode has exactly
**one** WAL file per database, shared by every connection. The tracker harvester
writes far more than the DHT workers; putting it in the same file would inflate a
WAL that the DHT readers keep pinned open, and a WAL can only be truncated when
no reader holds a snapshot. Separate files keep each collector's write volume off
the others' critical path.

---

## 4. Demand = a distinct-IP **union**, never a sum

One person seeding the 1080p *and* the 720p release of a film is **one** pirate,
not two. The same person seen by the DHT crawl *and* by a tracker announce is
still one. So the daily number is:

```
demand(ip_id, country, day)
  = | { ip : ip seen on ANY hash of that ip_id, from ANY source, that day } |
```

A set union per `(ip_id, country)` — computed after rolling hashes up to their
title, and after canonicalising duplicate ids. The per-source columns
(`IP_COUNT_DHT`, `IP_COUNT_HARVEST`, `IP_COUNT_PEX`) are **subset sizes of that
union**, so they deliberately overlap and do **not** add up to the total.

Each IP is also assigned exactly **one** country, so a peer whose geo differs
between sources cannot be counted in two country buckets.

> **Trap.** Any code that `SUM()`s per-hash or per-source counts is
> double-counting. If two numbers that should agree differ by a suspiciously
> round factor, this is usually why.

---

## 5. The day window — and why the past is not reproducible

`peers.last_seen` is a **DATE** (`2026-07-27`), not a timestamp, and it is
**updated in place** when a peer is seen again. A day's demand is therefore every
row whose `last_seen` equals that date.

Two consequences that have each caused a real incident:

- **`WHERE last_seen >= datetime('now','-5 minutes')` is always false.** It
  compares `'2026-07-27'` against `'2026-07-27 01:58:00'` as strings and silently
  returns 0 — a freshness check that can never pass and reports a phantom outage.
  Use `date('now')`, or sample the count twice and confirm it grew.
- **A past day drains.** A peer seen on the 27th and again on the 28th has its
  `last_seen` moved to the 28th, so it leaves the 27th's window. Re-exporting a
  past date returns a strictly smaller — and perfectly well-formed — file.
  Measured: **−10.9% within two hours**, every title shrinking. `export_nbcu.py`
  and `merge_and_upload.py` both refuse a past date unless forced. To correct a
  *label* in a shipped file, patch the CSV rows in place and assert the row count
  and total are unchanged.

---

## 6. The output row

One row per `ip_id × country × day`:

```
TITLE, IP_ID, IMDB_ID, ANIME_ID, DATE, CATEGORY, COUNTRY_4,
IP_COUNT, DC_IP_COUNT, UNMAPPED, IP_COUNT_DHT, IP_COUNT_HARVEST, IP_COUNT_PEX
```

- `IP_COUNT` — the distinct-IP union above. The headline number.
- `DC_IP_COUNT` — how many of those are datacenter/VPN exits (via GeoLite2-ASN).
  They are **not** dropped: a VPN user is a real pirate, and removing them
  understates demand. The column lets a consumer take a residential-only view.
- `UNMAPPED` — 1 when the title has no catalog `ip_id` (see §2).
- Countries below a small floor roll into a per-title `Other` bucket, so the long
  tail is retained without shipping thousands of 1-IP rows.

---

## 7. Where the numbers can go wrong

Ranked by how expensive each is, and what defends against it:

| Failure | Effect | Defence |
|---|---|---|
| Wrong title match | One title's demand booked to another | Year filter, divergence guard, category guard, alias guards |
| Cross-family canonical merge | A film's swarm reported as a TV series | Family check — a legacy id may only fold into the same family |
| Summing instead of unioning | Inflated demand, worst for multi-release titles | Set union per `(ip_id, country)` |
| Re-exporting a past date | Silent, uniform shrinkage | Past-date guard; patch in place instead |
| Collector looking at the same items forever | Permanent blind spot, everything looks healthy | Rotation by least-recently-visited + a visit ledger |

The recurring lesson: **a wrong number is more expensive than a missing one.**
Where the pipeline cannot decide safely, it is designed to leave the row
`UNMAPPED` rather than guess.
