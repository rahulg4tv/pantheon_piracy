#!/usr/bin/env python3
"""
transparency_ingest.py — memory-safe ingest of the Google Transparency Report
copyright-removals dump (the 12 GB set of CSVs) into two compact outputs:

  1. top_domains.tsv        "<takedown_url_count>\\t<domain>"  (for dmca_site_discovery.py)
  2. sender_intel.tsv       reporting-organization + copyright-owner request tallies

MEMORY SAFETY (hard requirement — never load the 12 GB into RAM):
  • CSVs are streamed row-by-row with csv.reader (no pandas / read-all).
  • Domain aggregation (millions of unique domains) is done in an ON-DISK SQLite
    temp DB (GROUP BY spills to disk), so resident memory stays flat regardless of
    input size. Sender/owner tallies are small (thousands) so a dict is fine.
  • Columns are detected by header name, so it tolerates schema variation.

RUN OFF-BOX ONLY (per project rule — never run the 12 GB job on the prod EC2 box):
  python3 transparency_ingest.py --csvdir /path/to/google_dump --out /path/to/out
Then ship the small top_domains.tsv to the box's /data/transparency/.
"""
import os, sys, csv, gzip, sqlite3, argparse, tempfile
from collections import Counter

csv.field_size_limit(1 << 24)  # some URL fields are large


def _open(path):
    return gzip.open(path, "rt", encoding="utf-8", errors="replace") if path.endswith(".gz") \
        else open(path, "rt", encoding="utf-8", errors="replace")


def _find_csv(csvdir, *name_hints):
    for fn in os.listdir(csvdir):
        low = fn.lower()
        if low.endswith((".csv", ".csv.gz")) and any(h in low for h in name_hints):
            return os.path.join(csvdir, fn)
    return None


def _col(header, *keywords):
    """Index of the first header cell containing ALL keywords (case-insensitive)."""
    for i, h in enumerate(header):
        hl = h.lower()
        if all(k in hl for k in keywords):
            return i
    return -1


def ingest_domains(csvdir, out_tsv, top=None):
    path = _find_csv(csvdir, "domain")
    if not path:
        print("[domains] no domains CSV found — skipping"); return
    tmp = tempfile.NamedTemporaryFile(prefix="trans_dom_", suffix=".db", delete=False).name
    agg = sqlite3.connect(tmp)
    agg.execute("PRAGMA journal_mode=OFF"); agg.execute("PRAGMA synchronous=OFF")
    agg.execute("CREATE TABLE d(domain TEXT, c INTEGER)")
    rows = 0; batch = []
    with _open(path) as fh:
        r = csv.reader(fh)
        header = next(r, [])
        di = _col(header, "domain")
        ci = _col(header, "url", "removed")
        if ci < 0: ci = _col(header, "url")          # fall back to any URL-count col
        if di < 0:
            print("[domains] no 'domain' column in", path); agg.close(); os.unlink(tmp); return
        for row in r:
            if di >= len(row): continue
            dom = (row[di] or "").strip().lower()
            if not dom: continue
            try:    cnt = int(row[ci]) if (0 <= ci < len(row) and row[ci]) else 1
            except ValueError: cnt = 1
            batch.append((dom, cnt)); rows += 1
            if len(batch) >= 50000:
                agg.executemany("INSERT INTO d VALUES(?,?)", batch); batch.clear()
                if rows % 1_000_000 == 0: print(f"[domains] streamed {rows:,} rows")
        if batch: agg.executemany("INSERT INTO d VALUES(?,?)", batch)
    agg.commit()
    q = "SELECT domain, SUM(c) s FROM d GROUP BY domain ORDER BY s DESC"
    if top: q += f" LIMIT {int(top)}"
    n = 0
    with open(out_tsv, "w") as o:
        for dom, s in agg.execute(q):
            o.write(f"{s}\t{dom}\n"); n += 1
    agg.close(); os.unlink(tmp)
    print(f"[domains] {rows:,} rows -> {n:,} unique domains -> {out_tsv}")


def ingest_senders(csvdir, out_tsv):
    path = _find_csv(csvdir, "request")
    if not path:
        print("[senders] no requests CSV found — skipping"); return
    orgs, owners = Counter(), Counter()
    rows = 0
    with _open(path) as fh:
        r = csv.reader(fh)
        header = next(r, [])
        oi = _col(header, "reporting", "name")
        wi = _col(header, "copyright", "owner", "name")
        ui = _col(header, "url", "removed")
        for row in r:
            rows += 1
            try:    w = int(row[ui]) if (0 <= ui < len(row) and row[ui]) else 1
            except ValueError: w = 1
            if 0 <= oi < len(row) and row[oi]: orgs[row[oi].strip()] += w
            if 0 <= wi < len(row) and row[wi]: owners[row[wi].strip()] += w
    with open(out_tsv, "w") as o:
        o.write("# reporting_organizations (sender) by URLs\n")
        for name, c in orgs.most_common(500): o.write(f"org\t{c}\t{name}\n")
        o.write("# copyright_owners by URLs\n")
        for name, c in owners.most_common(500): o.write(f"owner\t{c}\t{name}\n")
    print(f"[senders] {rows:,} requests -> {len(orgs):,} orgs / {len(owners):,} owners -> {out_tsv}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csvdir", required=True, help="dir with the Google dump CSVs")
    ap.add_argument("--out", default=".", help="output dir")
    ap.add_argument("--top", type=int, default=None, help="cap top_domains.tsv to N rows")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    ingest_domains(a.csvdir, os.path.join(a.out, "top_domains.tsv"), top=a.top)
    ingest_senders(a.csvdir, os.path.join(a.out, "sender_intel.tsv"))


if __name__ == "__main__":
    main()
