"""
merge_and_upload.py — 6-hourly merge job (runs at 05, 11, 17, 23 UTC)

Flow:
  1. Load per-worker CSVs: YYYY-MM-DD_w0.csv, _w1.csv, _w2.csv, _w3.csv
     Falls back to YYYY-MM-DD.csv if no worker files exist (backward compat).
  1b. Supplement with announce-only peers from SQLite — hashes detected via
      passive announce_peer listener that had no active scan hits in the CSV.
      These are counted from the peers table (DISTINCT IPs per hash×country today).
  2. Deduplicate passes — same hash+country may appear in multiple passes per day;
     take MAX(peer_count) so we don't double-count IPs across passes.
  3. Join with SQLite hashes_v2.db to get source, first_seen, last_seen per hash.
  4. Aggregate to ip_id level — group all hashes for the same ip_id+country together.
     peer_count = SUM of per-hash MAX counts (best approximation of unique peers).
     hash_count = number of unique hashes active for this ip_id in this country.
     Result is unique per ip_id × country — episodes are never separate rows.
  5. Add peer_count_delta: change vs previous run's CSV (same-day or yesterday).
     Positive = swarm growing, negative = shrinking, 0 = stable or first day.
  6. Save as CSV → upload to S3.
  7. Fire SNS spike alerts for any ip_id with peer_count_delta > +50% (min 100 peers).

Date logic (auto, no --date flag needed):
  - 05 UTC run  → processes yesterday  (workers had all night, data complete)
  - 11/17/23 UTC → processes today     (intra-day snapshot)

Output schema (one row per ip_id × country × date):
    date, ip_id, title, category, source, country,
    hash_count, peer_count, peer_count_delta, seeders,
    bep33_seeders, bep33_leechers, first_seen, last_seen

Upload path: s3://YOUR_S3_BUCKET/merged/YYYY/MM/DD/YYYY-MM-DD.csv

Run: python3 merge_and_upload.py [--date YYYY-MM-DD]
"""

import argparse
import re
import sqlite3
import boto3
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone

# ── Config ────────────────────────────────────────────────────
BUCKET        = "YOUR_S3_BUCKET"
REGION        = "us-east-1"
DB_PATH       = "/data/db/hashes_v2.db"
# Additional peer-source DBs — each written by a separate collector, ATTACHed
# read-only. The S3 feed must UNION all of them so tracker-harvest + PEX IPs are
# counted (not DHT only), matching the validated export_nbcu.py distinct-IP union.
HARVEST_DB    = "/data/db/harvest_peers.db"           # tracker announce (BEP-15)
VELOCITY_DB   = "/data/db/harvest_velocity_peers.db"  # high-velocity re-harvest lane
PEX_DB        = "/data/db/pex_peers.db"               # PEX (BEP-11 ut_pex)
PEER_COUNTS_DIR = Path("/data/peer_counts")
MERGED_DIR    = Path("/data/merged")
MERGED_DIR.mkdir(parents=True, exist_ok=True)

# SNS alert config
SNS_TOPIC_ARN      = "arn:aws:sns:us-east-1:YOUR_AWS_ACCOUNT_ID:searchpantheon_Admin_Email"
SPIKE_MIN_PEERS    = 100    # ignore tiny swarms
SPIKE_PCT_THRESHOLD = 0.50  # 50% growth triggers alert


def get_date(args) -> str:
    if args.date:
        return args.date
    now = datetime.now(timezone.utc)
    # 05 UTC run → yesterday (workers had all night, data is complete)
    # 11/17/23 UTC runs → today (intra-day snapshot, data still accumulating)
    if now.hour < 6:
        return (now - timedelta(days=1)).strftime("%Y-%m-%d")
    return now.strftime("%Y-%m-%d")


def download_eu_csvs(date: str):
    """
    Download EU node CSVs (_w8.csv, _w9.csv) from S3 into PEER_COUNTS_DIR
    before the merge so load_peer_counts() picks them up automatically via glob.
    Safe to call even if no EU files exist yet (logs a warning, doesn't fail).
    """
    print(f"[merge] Downloading EU CSVs for {date} from S3...")
    s3 = boto3.client("s3", region_name=REGION)
    prefix = f"peer-counts-eu/{date}_w"
    try:
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        files = resp.get("Contents", [])
        if not files:
            print("[merge] No EU CSVs found in S3 yet — skipping")
            return
        for obj in files:
            key   = obj["Key"]
            local = PEER_COUNTS_DIR / Path(key).name
            s3.download_file(BUCKET, key, str(local))
            size_kb = local.stat().st_size / 1024
            print(f"[merge]   Downloaded EU: {local.name} ({size_kb:.0f} KB)")
    except Exception as e:
        print(f"[merge] WARNING: EU CSV download failed: {e} — continuing without EU data")


def load_peer_counts(date: str) -> pd.DataFrame:
    """
    Load raw per-worker CSVs for the given date and concat them.

    Priority:
      1. Per-worker files: YYYY-MM-DD_w0.csv, _w1.csv, _w2.csv, _w3.csv
         Written by multi-worker setup (Step 6 — slice-based workers).
      2. Fallback: YYYY-MM-DD.csv — legacy single-process output or
         manually merged file. Used if no worker files exist.

    All worker files are concatenated before dedup — dedup_passes() handles
    duplicate hash×country rows that arise when multiple workers or passes
    write overlapping data.
    """
    # Glob per-worker files first (e.g. 2026-05-28_w0.csv … _w3.csv)
    worker_files = sorted(PEER_COUNTS_DIR.glob(f"{date}_w*.csv"))

    if worker_files:
        print(f"[merge] Found {len(worker_files)} worker CSV(s): "
              f"{[f.name for f in worker_files]}")
        frames = []
        for wf in worker_files:
            wdf = pd.read_csv(wf)
            frames.append(wdf)
            print(f"[merge]   {wf.name}: {len(wdf):,} rows")
        df = pd.concat(frames, ignore_index=True)
        print(f"[merge] Combined worker CSVs: {len(df):,} rows total")
    else:
        # Backward compat: single shared CSV (pre-slice setup)
        csv_path = PEER_COUNTS_DIR / f"{date}.csv"
        if not csv_path.exists():
            raise FileNotFoundError(
                f"No peer count CSV(s) for {date} — "
                f"checked worker files ({date}_w*.csv) and {csv_path}"
            )
        print(f"[merge] Loading peer counts from {csv_path} (single-file fallback)...")
        df = pd.read_csv(csv_path)
        print(f"[merge] Loaded {len(df):,} rows")

    # Schema migration: CSVs written before 2026-05-25 lack bep33 columns.
    for col in ("bep33_seeders", "bep33_leechers"):
        if col not in df.columns:
            df[col] = 0
            print(f"[merge] WARNING: '{col}' missing — backfilled with 0 (old schema)")

    print(f"[merge] Total: {len(df):,} rows  ({df['hash'].nunique():,} unique hashes)")
    return df


def load_announce_only_peers(date: str, csv_hashes: set) -> pd.DataFrame:
    """
    Pull peers detected ONLY via passive announce_peer (not found by active scan).

    The announce_peer listener writes individual IPs directly to the SQLite peers
    table throughout the day. These never appear in the CSV because the CSV is
    written only by the active get_peers scan. Without this step, all passive
    detections are silently dropped from the daily Parquet.

    Strategy:
      - Query peers table for real IPs seen today (last_seen = date, ip != _queried_)
      - Join hashes table to get ip_id / title / category / seeders
      - COUNT(DISTINCT ip) per hash×country → same peer_count semantics as CSV
      - Filter OUT hashes already covered by the active scan CSV
      - Return a DataFrame with the same schema as load_peer_counts(), ready to
        concat and flow through dedup_passes → aggregate_to_ip_id unchanged.

    Note: for hashes present in BOTH the CSV (active) and announce hits, we keep
    only the active scan data. The active scan count may be lower, but mixing
    independent IP counts from two sources risks double-counting the same IP.
    A future improvement could union the IP sets per hash at the DB level.
    """
    print(f"[merge] Loading announce-only peers from SQLite for {date}...")
    conn = sqlite3.connect(DB_PATH)

    df = pd.read_sql_query("""
        SELECT
            p.last_seen                        AS date,
            'announce'                         AS run_time,
            p.hash,
            h.ip_id,
            h.title,
            h.category,
            COALESCE(h.seeders, 0)             AS seeders,
            COALESCE(p.country, 'XX')          AS country,
            COUNT(DISTINCT p.ip)               AS peer_count,
            0                                  AS bep33_seeders,
            0                                  AS bep33_leechers
        FROM peers p
        JOIN hashes h ON h.hash = p.hash
        WHERE p.last_seen = ?
          AND p.ip != '_queried_'
        GROUP BY p.hash, p.country
    """, conn, params=[date])

    conn.close()

    if df.empty:
        print("[merge] Announce-only peers: none found")
        return df

    # Only supplement hashes NOT already in the active-scan CSV.
    announce_only = df[~df["hash"].isin(csv_hashes)].copy()

    n_hashes  = announce_only["hash"].nunique()
    n_rows    = len(announce_only)
    n_skipped = df["hash"].nunique() - n_hashes

    print(f"[merge] Announce-only peers: {n_hashes:,} hashes / {n_rows:,} rows "
          f"(+{n_skipped:,} hashes skipped — already in active scan CSV)")

    return announce_only


def dedup_passes(df: pd.DataFrame) -> pd.DataFrame:
    """
    A hash may be scanned multiple times per day (multiple passes).
    Each pass writes a row per hash×country with the peer_count at that moment.
    Take MAX(peer_count) per (date, hash, ip_id, title, category, country)
    so the same IP isn't double-counted across passes.
    """
    print("[merge] Deduplicating passes (MAX peer_count per hash×country per day)...")
    before = len(df)
    df = (
        df.groupby(
            ["date", "hash", "ip_id", "title", "category", "country"],
            as_index=False
        )
        .agg(peer_count    =("peer_count",     "max"),
             seeders       =("seeders",        "max"),
             bep33_seeders =("bep33_seeders",  "max"),
             bep33_leechers=("bep33_leechers", "max"))
    )
    print(f"[merge] Rows: {before:,} → {len(df):,} after pass dedup")
    return df


def load_hash_metadata() -> pd.DataFrame:
    print("[merge] Loading hash metadata from SQLite...")
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT
            hash,
            source,
            first_seen,
            last_seen
        FROM hashes
    """, conn)
    conn.close()
    print(f"[merge] Loaded {len(df):,} hash records from DB")
    return df


def normalize_title(s) -> str:
    """Lowercase, collapse whitespace, strip — for matching duplicate catalog entries."""
    if not isinstance(s, str):
        return ""
    return re.sub(r"\s+", " ", s.strip().lower())


def load_titles_catalog() -> pd.DataFrame:
    """Load the catalog (one row per ip_id) — used for canonical title + dedup."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT ip_id, title, category, imdb_id, hashes_found FROM titles", conn
    )
    conn.close()
    print(f"[merge] Loaded {len(df):,} catalog titles")
    return df


def build_canonical_map(titles_df: pd.DataFrame,
                        extra_df: "pd.DataFrame | None" = None) -> tuple[dict, dict]:
    """
    Collapse duplicate catalog entries for the same show.

    Two fragmentation classes are fixed downstream of this map:
      A. Same ip_id appearing with inconsistent title casing across hashes
         ("Invincible" vs "INVINCIBLE") — fixed by always using the single
         canonical title returned here (canon_title[ip_id]).
      B. Same show under two different ip_ids (e.g. Spider-Noir =
         series-tt30460310 + series-Q123956515) — fixed by the alias map.

    Canonical ip_id within a (normalized_title, category) group is chosen by:
      1. has a real imdb_id (the IMDB 'tt' catalog entry is authoritative)
      2. most hashes_found
      3. lexically first ip_id (stable tie-break)

    imdb_id can't be the join key directly: the duplicate 'Q' entries usually
    have no imdb_id at all, so we group on normalized title + category instead.
    Grouping stays WITHIN category to avoid merging unrelated same-named titles.

    `extra_df` (optional): additional (ip_id, title, category) rows — typically
    the day's PEER rows. The titles table alone misses *orphan* ip_ids that live
    only in the peer stream (e.g. a legacy series-Q… still emitted by a
    long-running DHT worker whose hash→ip_id cache predates a Q→tt DB remap).
    Such an orphan never appears in `titles`, so without this it stays a separate
    row in the ranking and splits the show's count (e.g. "The Pitt" =
    series-tt31938062 + orphan series-Q131431817). Folding the peer rows into the
    grouping universe lets the orphan alias onto the imdb-authoritative tt id.
    Titles-table rows still win the canonical choice (they carry imdb / hashes).

    Returns:
      alias_map: {duplicate_ip_id -> canonical_ip_id}
      canon_title: {ip_id -> canonical title string}
    """
    df = titles_df[["ip_id", "title", "category", "imdb_id", "hashes_found"]].copy()
    if extra_df is not None and not extra_df.empty:
        known = set(df["ip_id"])
        ex = extra_df[["ip_id", "title", "category"]].drop_duplicates("ip_id").copy()
        ex = ex[~ex["ip_id"].isin(known)]          # only ids missing from titles
        if not ex.empty:
            ex["imdb_id"] = ""                     # orphans never win canonical
            ex["hashes_found"] = 0
            df = pd.concat([df, ex], ignore_index=True)
    df["norm"] = df["title"].map(normalize_title)
    df = df[df["norm"] != ""]
    df["has_imdb"] = df["imdb_id"].notna() & (
        df["imdb_id"].astype(str).str.strip().ne("")
    )
    df["hashes_found"] = pd.to_numeric(df["hashes_found"], errors="coerce").fillna(0)

    alias_map: dict = {}
    canon_title: dict = dict(zip(df["ip_id"], df["title"]))

    dup_groups = 0
    for (_norm, _cat), grp in df.groupby(["norm", "category"]):
        if grp["ip_id"].nunique() <= 1:
            continue
        dup_groups += 1
        grp = grp.sort_values(
            ["has_imdb", "hashes_found", "ip_id"], ascending=[False, False, True]
        )
        canon_ip = grp.iloc[0]["ip_id"]
        canon_name = grp.iloc[0]["title"]
        for ip in grp["ip_id"]:
            alias_map[ip] = canon_ip
            canon_title[ip] = canon_name

    print(f"[merge] Canonical map: {dup_groups:,} duplicate title groups → "
          f"{len(alias_map):,} ip_ids remapped")
    return alias_map, canon_title


def canonicalize(df: pd.DataFrame, alias_map: dict, canon_title: dict) -> pd.DataFrame:
    """
    Apply the canonical map to peer rows: remap aliased ip_ids to their canonical
    ip_id and overwrite the per-hash title with the single canonical title.

    After this, every row for the same show shares one ip_id and one title, so
    the later groupby never splits a show into multiple rows.
    """
    before_ids = df["ip_id"].nunique()
    df = df.copy()
    df["ip_id"] = df["ip_id"].map(lambda x: alias_map.get(x, x))
    df["title"] = df["ip_id"].map(canon_title).fillna(df["title"])
    after_ids = df["ip_id"].nunique()
    print(f"[merge] Canonicalized ip_ids: {before_ids:,} → {after_ids:,} "
          f"({before_ids - after_ids:,} merged)")
    return df


def merge_metadata(peer_df: pd.DataFrame, meta_df: pd.DataFrame) -> pd.DataFrame:
    """Join peer counts with hash metadata (source, first_seen, last_seen)."""
    print("[merge] Joining with hash metadata...")
    merged = peer_df.merge(meta_df, on="hash", how="left")
    print(f"[merge] Joined: {len(merged):,} rows")
    return merged


def aggregate_to_ip_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Collapse hash-level rows into ip_id-level rows.

    One ip_id (e.g. series-Q15659308 = Rick and Morty) may have many hashes
    (different seasons/episodes). Group them together per country:

      peer_count = SUM of per-hash MAX peer counts
                   (best approximation — note: same IP seeding two hashes
                    of the same show would be counted twice, but this is
                    unavoidable without storing individual IPs in the CSV)
      hash_count = number of distinct hashes active for this ip_id+country
      seeders    = MAX seeders across hashes (from Jackett metadata)
      first_seen = earliest first_seen across hashes
      last_seen  = latest last_seen across hashes
    """
    print("[merge] Aggregating to ip_id × country level...")
    agg = (
        df.groupby(["date", "ip_id", "title", "category", "country"], as_index=False)
        .agg(
            peer_count     =("peer_count",     "sum"),
            hash_count     =("hash",           "nunique"),
            seeders        =("seeders",        "max"),
            bep33_seeders  =("bep33_seeders",  "sum"),
            bep33_leechers =("bep33_leechers", "sum"),
            first_seen     =("first_seen",     "min"),
            last_seen      =("last_seen",      "max"),
            source         =("source",         "first"),
        )
    )
    # Reorder columns cleanly
    agg = agg[[
        "date", "ip_id", "title", "category", "source", "country",
        "hash_count", "peer_count", "seeders",
        "bep33_seeders", "bep33_leechers",
        "first_seen", "last_seen"
    ]]
    print(f"[merge] Aggregated: {len(agg):,} rows ({agg['ip_id'].nunique():,} unique ip_ids)")
    return agg


def load_union_ip_counts(date: str, alias_map: dict) -> pd.DataFrame:
    """
    TRUE distinct-IP union across ALL peer sources for `date`, per (ip_id, country).

    Sources (each a separate DB, ATTACHed read-only if present):
      hashes_v2.db `peers`        — DHT get_peers + passive announce
      harvest_peers.db            — tracker announce (BEP-15)
      harvest_velocity_peers.db   — high-velocity re-harvest lane
      pex_peers.db                — PEX (BEP-11 ut_pex)

    Each IP is assigned exactly ONE country (first non-'XX' wins) so it can't be
    counted in several country buckets, then DISTINCT ip per (canonical ip_id,
    country). ip_id is canonicalised via alias_map so a title split across ids is
    one row and shared IPs dedupe across the merged ids.

    This mirrors export_nbcu.py — the validated NBCU export — so the S3 feed stops
    undercounting (it was DHT-only) and dedupes the same IP seen by multiple
    sources instead of double-counting it.

    Returns columns: ip_id, country, ip_total, ip_dht, ip_harvest, ip_pex
    """
    import os
    conn = sqlite3.connect(DB_PATH, timeout=60, uri=True)  # uri=True → file:…?mode=ro ATTACH works
    union = ["SELECT hash, country, ip, last_seen, 'dht' AS src FROM peers"]
    for path, alias, tag in ((HARVEST_DB, "hv", "harv"),
                             (VELOCITY_DB, "vv", "harv"),
                             (PEX_DB, "pe", "pex")):
        if os.path.exists(path):
            conn.execute("ATTACH DATABASE ? AS %s" % alias, ("file:" + path + "?mode=ro",))
            union.append("SELECT hash, country, ip, last_seen, '%s' AS src FROM %s.peers" % (tag, alias))
    sql = ("SELECT h.ip_id, p.country, p.ip, p.src FROM (\n            "
           + "\n            UNION ALL\n            ".join(union)
           + "\n        ) p JOIN hashes h ON h.hash = p.hash "
             "WHERE p.last_seen = ? AND p.ip != '_queried_'")
    print("[merge] Union: reading distinct peer IPs across DHT + harvest + PEX...")
    cur = conn.execute(sql, (date,))

    ipctry: dict = {}   # (ip_id, ip) -> the single country it counts under
    ipsrc: dict = {}    # (ip_id, ip) -> set of sources that saw it
    n_rows = 0
    for ip_id, iso, ip, src in cur:          # stream rows — do not materialise all
        n_rows += 1
        ip_id = alias_map.get(ip_id, ip_id)  # canonicalise ip_id
        k = (ip_id, ip)
        s = ipsrc.get(k)
        if s is None:
            ipsrc[k] = {src}
        else:
            s.add(src)
        cur_c = ipctry.get(k)
        if cur_c is None or (cur_c == "XX" and iso and iso != "XX"):
            ipctry[k] = iso or "XX"
    conn.close()

    tot: dict = {}; dht: dict = {}; harv: dict = {}; pex: dict = {}
    for (ip_id, ip), iso in ipctry.items():
        key = (ip_id, iso)
        tot.setdefault(key, set()).add(ip)
        s = ipsrc.get((ip_id, ip), ())
        if "dht" in s:  dht.setdefault(key, set()).add(ip)
        if "harv" in s: harv.setdefault(key, set()).add(ip)
        if "pex" in s:  pex.setdefault(key, set()).add(ip)

    recs = [{"ip_id": k[0], "country": k[1],
             "ip_total": len(v),
             "ip_dht": len(dht.get(k, ())),
             "ip_harvest": len(harv.get(k, ())),
             "ip_pex": len(pex.get(k, ()))} for k, v in tot.items()]
    df = pd.DataFrame(recs, columns=["ip_id", "country", "ip_total",
                                     "ip_dht", "ip_harvest", "ip_pex"])
    print(f"[merge] Union: {n_rows:,} raw peer rows → {len(df):,} (ip_id×country); "
          f"distinct IPs total={int(df['ip_total'].sum()):,} "
          f"(dht={int(df['ip_dht'].sum()):,} "
          f"harvest={int(df['ip_harvest'].sum()):,} "
          f"pex={int(df['ip_pex'].sum()):,})")
    return df


def add_peer_velocity(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """
    Add peer_count_delta: day-over-day change in peer_count per ip_id × country.

    Reads yesterday's CSV (or legacy parquet) from MERGED_DIR.
    If yesterday's file doesn't exist, delta is set to 0 with a warning.

    delta = today_peer_count - yesterday_peer_count
      > 0 → growing swarm
      < 0 → shrinking swarm
      = 0 → stable (or first day seen)
    """
    from datetime import datetime, timedelta
    yesterday = (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

    # Support both CSV (new) and Parquet (legacy) for yesterday's file
    prev_csv     = MERGED_DIR / f"{yesterday}.csv"
    prev_parquet = MERGED_DIR / f"{yesterday}.parquet"

    if prev_csv.exists():
        prev_path = prev_csv
    elif prev_parquet.exists():
        prev_path = prev_parquet
        print("[merge] NOTE: using legacy parquet for yesterday's delta")
    else:
        print(f"[merge] No previous file for {yesterday} — peer_count_delta set to 0")
        df["peer_count_delta"] = 0
        return df

    print(f"[merge] Loading previous day ({yesterday}) for peer velocity...")
    # CRITICAL: prev MUST be unique per (ip_id, country) before the merge.
    # If prev has duplicate keys the left-merge FANS OUT — and because each run
    # writes its own output back as the next day's "previous" file, that fan-out
    # compounded every run and exploded the output to tens of millions of rows
    # (26.8M rows for ~102K real keys), OOM-killing the job. We collapse prev to
    # one row per (ip_id, country) — summing peer_count = the day's total demand
    # baseline — and read the CSV in bounded chunks so even a large/legacy
    # already-bloated prev file never blows up memory.
    # Rebuild a CLEAN baseline from a possibly-corrupt prev file:
    #   1. dedup to one row per (ip_id, country, title, category) — collapses the
    #      identical fan-out copies a legacy bloated file carries, while keeping
    #      legitimate category variants (which hold different peer_counts).
    #   2. sum peer_count per (ip_id, country) → the day's total demand baseline.
    # Read in bounded chunks and re-dedup after each concat so memory stays flat
    # even on a multi-GB legacy file.
    KEY  = ["ip_id", "country", "title", "category"]
    COLS = ["ip_id", "country", "title", "category", "peer_count"]

    if prev_path.suffix == ".parquet":
        # Legacy parquet predates the bloat era and may lack title/category.
        p = pd.read_parquet(prev_path, columns=["ip_id", "country", "peer_count"])
        uniq = p.drop_duplicates()
    else:
        uniq = None
        for chunk in pd.read_csv(prev_path, usecols=COLS, chunksize=500_000):
            part = chunk.drop_duplicates(subset=KEY)
            uniq = part if uniq is None else \
                pd.concat([uniq, part], ignore_index=True).drop_duplicates(subset=KEY)
        if uniq is None:
            uniq = pd.DataFrame(columns=COLS)

    prev = (uniq.groupby(["ip_id", "country"], as_index=False)["peer_count"].sum()
                .rename(columns={"peer_count": "prev_peer_count"}))

    df = df.merge(prev, on=["ip_id", "country"], how="left")
    df["prev_peer_count"] = df["prev_peer_count"].fillna(0).astype(int)
    df["peer_count_delta"] = df["peer_count"] - df["prev_peer_count"]
    df = df.drop(columns=["prev_peer_count"])

    rising  = (df["peer_count_delta"] > 0).sum()
    falling = (df["peer_count_delta"] < 0).sum()
    stable  = (df["peer_count_delta"] == 0).sum()
    print(f"[merge] Peer velocity: {rising:,} rising  {falling:,} falling  {stable:,} stable")
    return df


def send_spike_alerts(df: pd.DataFrame, date: str):
    """
    Fire SNS alerts for titles with a significant peer count spike.

    Criteria:
      - peer_count_delta / (peer_count - peer_count_delta) > SPIKE_PCT_THRESHOLD
      - peer_count >= SPIKE_MIN_PEERS
      - peer_count_delta > 0

    Groups by ip_id (across all countries) so one alert per title, not per country.
    Shows top 5 countries by peer_count for context.
    """
    spikes = df[
        (df["peer_count"] >= SPIKE_MIN_PEERS) &
        (df["peer_count_delta"] > 0)
    ].copy()

    if spikes.empty:
        print("[merge] Spike alerts: no qualifying rows")
        return

    spikes["prev_count"] = spikes["peer_count"] - spikes["peer_count_delta"]
    spikes = spikes[spikes["prev_count"] > 0].copy()
    spikes["pct_change"] = spikes["peer_count_delta"] / spikes["prev_count"]

    # Aggregate to ip_id level (sum across countries)
    ip_agg = (
        spikes.groupby(["ip_id", "title", "category"], as_index=False)
        .agg(
            peer_count     =("peer_count",     "sum"),
            peer_count_delta=("peer_count_delta","sum"),
            prev_count     =("prev_count",     "sum"),
        )
    )
    ip_agg["pct_change"] = ip_agg["peer_count_delta"] / ip_agg["prev_count"]
    alerts = ip_agg[ip_agg["pct_change"] >= SPIKE_PCT_THRESHOLD].sort_values(
        "pct_change", ascending=False
    )

    if alerts.empty:
        print("[merge] Spike alerts: no spikes above threshold")
        return

    print(f"[merge] Spike alerts: {len(alerts)} title(s) spiked >{SPIKE_PCT_THRESHOLD*100:.0f}%")

    # Build message
    lines = [
        f"🚨 Piracy Spike Alert — {date}",
        f"{'─'*45}",
        f"{len(alerts)} title(s) surged >{SPIKE_PCT_THRESHOLD*100:.0f}% since last run",
        "",
    ]
    for _, row in alerts.head(20).iterrows():
        pct = row["pct_change"] * 100
        lines.append(
            f"  {row['title']} ({row['category']})"
        )
        lines.append(
            f"    Peers: {int(row['prev_count']):,} → {int(row['peer_count']):,}  "
            f"(+{int(row['peer_count_delta']):,} / +{pct:.0f}%)"
        )
        # Top countries for this ip_id
        top_countries = (
            df[df["ip_id"] == row["ip_id"]]
            .nlargest(5, "peer_count")[["country", "peer_count"]]
        )
        country_str = "  ".join(
            f"{r['country']}:{r['peer_count']:,}" for _, r in top_countries.iterrows()
        )
        lines.append(f"    Top: {country_str}")
        lines.append("")

    message = "\n".join(lines)
    subject = f"[Piracy Spike] {len(alerts)} title(s) on {date}"

    try:
        sns = boto3.client("sns", region_name=REGION)
        sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=message)
        print(f"[merge] SNS alert sent: '{subject}'")
    except Exception as e:
        print(f"[merge] WARNING: SNS publish failed: {e}")


def upload_to_s3(local_path: Path, date: str):
    year, month, day = date.split("-")
    s3_key = f"merged/{year}/{month}/{day}/{local_path.name}"
    print(f"[merge] Uploading to s3://{BUCKET}/{s3_key}...")
    s3 = boto3.client("s3", region_name=REGION)
    s3.upload_file(str(local_path), BUCKET, s3_key)
    print(f"[merge] Upload complete: s3://{BUCKET}/{s3_key}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Date to merge (YYYY-MM-DD), default: yesterday")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute and write output to /tmp only — no S3 upload, "
                             "no SNS alerts, does not overwrite the production merged file.")
    args = parser.parse_args()

    date = get_date(args)
    print(f"[merge] Processing date: {date}")

    # 0. Pull EU node CSVs from S3 into local peer_counts dir
    download_eu_csvs(date)

    # 1. Load raw CSV (active DHT scan results — US + EU workers combined)
    peer_df   = load_peer_counts(date)
    csv_hashes = set(peer_df["hash"].unique())

    # 1b. Supplement with announce-only peers from SQLite peers table.
    #     These are hashes detected passively via announce_peer that had no
    #     active scan hits — completely absent from the CSV without this step.
    announce_df = load_announce_only_peers(date, csv_hashes)
    if not announce_df.empty:
        peer_df = pd.concat([peer_df, announce_df], ignore_index=True)
        print(f"[merge] Combined total: {len(peer_df):,} rows "
              f"({peer_df['hash'].nunique():,} unique hashes)")

    # 1c. Canonicalize ip_ids/titles — collapse duplicate catalog entries for the
    #     same show (casing splits + tt/Q duplicate ids) BEFORE any aggregation,
    #     otherwise the same title is split into multiple rows and undercounted.
    titles_df = load_titles_catalog()
    # Pass peer_df so orphan ip_ids present only in the peer stream (e.g. a stale
    # series-Q… still emitted by a long-running DHT worker) also fold into the
    # canonical map and don't split a show's ranking count.
    alias_map, canon_title = build_canonical_map(titles_df, extra_df=peer_df)
    peer_df = canonicalize(peer_df, alias_map, canon_title)

    # 2. Deduplicate passes (MAX peer_count per hash×country per day)
    peer_df = dedup_passes(peer_df)

    # 3. Join with DB metadata (source, first_seen, last_seen)
    meta_df = load_hash_metadata()
    merged  = merge_metadata(peer_df, meta_df)

    # 4. Aggregate to ip_id × country level — this supplies the metadata columns
    #    (title, category, source, hash_count, seeders, bep33_*, first/last_seen).
    #    Its peer_count is the OLD DHT-only, per-hash-summed approximation and is
    #    REPLACED below by the true distinct-IP union.
    final = aggregate_to_ip_id(merged)

    # 4b. P0 FIX — the S3 feed used to count DHT only. Replace peer_count with the
    #     TRUE distinct-IP union across DHT + tracker-harvest + PEX (dedupes the
    #     same IP seen by multiple sources) and add per-source breakdown columns.
    #     Outer-merge so (ip_id,country) rows only a non-DHT source saw are ADDED.
    union_df = load_union_ip_counts(date, alias_map)
    old_total = int(final["peer_count"].sum())
    final = final.merge(union_df, on=["ip_id", "country"], how="outer")
    final["peer_count"] = final["ip_total"].fillna(final["peer_count"]).fillna(0).astype(int)
    final = final.rename(columns={"ip_dht": "peer_count_dht",
                                  "ip_harvest": "peer_count_harvest",
                                  "ip_pex": "peer_count_pex"})
    for col in ("peer_count_dht", "peer_count_harvest", "peer_count_pex"):
        final[col] = final[col].fillna(0).astype(int)
    final = final.drop(columns=["ip_total"])
    # Backfill metadata for union-only rows (a peer DB saw the title but the DHT
    # CSV path produced no row for it): title + category come from the catalog.
    catmap = dict(zip(titles_df["ip_id"], titles_df["category"]))
    final["date"] = final["date"].fillna(date)
    final["title"] = final["ip_id"].map(canon_title).fillna(final["title"]).fillna(final["ip_id"])
    blank_cat = final["category"].isna() | (final["category"].astype(str).str.strip() == "")
    final.loc[blank_cat, "category"] = final.loc[blank_cat, "ip_id"].map(catmap)
    for col, dflt in (("category", ""), ("source", "harvest"), ("hash_count", 0),
                      ("seeders", 0), ("bep33_seeders", 0), ("bep33_leechers", 0),
                      ("first_seen", ""), ("last_seen", "")):
        if col in final.columns:
            final[col] = final[col].fillna(dflt)
    # Collapse to ONE row per (ip_id, country). aggregate_to_ip_id also groups by
    # title+category, so a title with a category/casing variant produced two
    # (ip_id,country) rows; after the union (peer_count is per ip_id×country) both
    # carried the SAME count → the title×country was double-listed. Merge them: the
    # count is per key so max (they're identical), first non-blank category wins.
    def _firstcat(s):
        for x in s:
            if str(x).strip():
                return x
        return ""
    final = (final.groupby(["date", "ip_id", "country"], as_index=False)
                  .agg(title=("title", "first"), category=("category", _firstcat),
                       source=("source", "first"), hash_count=("hash_count", "max"),
                       peer_count=("peer_count", "max"), seeders=("seeders", "max"),
                       bep33_seeders=("bep33_seeders", "max"),
                       bep33_leechers=("bep33_leechers", "max"),
                       first_seen=("first_seen", "min"), last_seen=("last_seen", "max"),
                       peer_count_dht=("peer_count_dht", "max"),
                       peer_count_harvest=("peer_count_harvest", "max"),
                       peer_count_pex=("peer_count_pex", "max")))
    final["hash_count"] = pd.to_numeric(final["hash_count"], errors="coerce").fillna(0).astype(int)
    # Canonical column order for the S3 feed (breakdown columns appended).
    final = final[["date", "ip_id", "title", "category", "source", "country",
                   "hash_count", "peer_count", "seeders", "bep33_seeders",
                   "bep33_leechers", "first_seen", "last_seen",
                   "peer_count_dht", "peer_count_harvest", "peer_count_pex"]]
    print(f"[merge] peer_count metric: DHT-only sum={old_total:,} → "
          f"distinct-IP union={int(final['peer_count'].sum()):,}  ({len(final):,} rows)")

    # 5. Add peer velocity (delta vs previous run) — requires previous CSV (or parquet)
    final = add_peer_velocity(final, date)

    # 5b. Spike alerts — SNS notify if any title grew >50% since last run
    if args.dry_run:
        print("[merge] DRY-RUN: skipping SNS spike alerts")
    else:
        send_spike_alerts(final, date)

    # 6. Save as CSV
    # Atomic write: write to .tmp first then rename so a mid-write kill never
    # leaves a corrupt file (or uploads one to S3).
    if args.dry_run:
        out_path = Path("/tmp") / f"merged_dryrun_{date}.csv"
    else:
        out_path = MERGED_DIR / f"{date}.csv"
    tmp_path = out_path.with_suffix(".tmp.csv")
    final.to_csv(tmp_path, index=False)
    tmp_path.replace(out_path)   # atomic on POSIX
    size_mb = out_path.stat().st_size / 1024 / 1024
    print(f"[merge] Saved: {out_path} ({size_mb:.1f} MB, {len(final):,} rows)")

    # 7. Upload to S3
    if args.dry_run:
        print("[merge] DRY-RUN: skipping S3 upload")
    else:
        upload_to_s3(out_path, date)
    print("[merge] Done.")


if __name__ == "__main__":
    main()

