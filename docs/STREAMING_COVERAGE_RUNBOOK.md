# Streaming movie/series coverage — deploy runbook

Goal: fill the movie/series gap in the Channel-2 streaming registry (currently anime-heavy:
89 live sites, mostly anime). Built 2026-06-11 while SSO was logged out; **deploy when back.**

## What's already built & committed (ready to deploy)
1. **Alias-aware stream matcher** (`stream_demand_collector.py`, commit `518e251`) — `load_catalog()`
   now also keys on every alias in `title_aliases.db`, so a site listing a title under a foreign/alt
   name matches (same Pillar-1 win, Channel-2 side). Memory-safe. **Deploy = copy file to box** (cron
   05:15 picks it up next run).
2. **Memory-safe Transparency ingest** (`transparency_ingest.py`, commit `63d75c3`) — streams the 12 GB
   Google copyright-removals CSVs row-by-row, aggregates domains via on-disk SQLite (RAM stays flat),
   outputs `top_domains.tsv` (site discovery) + `sender_intel.tsv` (the Lumen "sender intel" for free).
   **RUN OFF-BOX only.**
3. **Site discovery** (`dmca_site_discovery.py`, already on box) — cross-references `top_domains.tsv` vs
   `stream_sites`, filters to VIDEO-streaming domains, inserts new ones as `status='candidate'`.

## The deploy sequence (≈ 15 min once you're back + have the 12 GB dump)
```
# 0. (off-box) get the Google copyright-removals dump (re-download if gone), then:
python3 transparency_ingest.py --csvdir /path/to/google_dump --out /tmp/trans --top 50000
#    -> /tmp/trans/top_domains.tsv  +  /tmp/trans/sender_intel.tsv   (memory-safe, flat RAM)

# 1. ship top_domains.tsv to the box
aws s3 cp /tmp/trans/top_domains.tsv s3://YOUR_S3_BUCKET/transparency/top_domains.tsv
#    (on box, via boto3 — the box aws CLI is broken)
sudo -u ec2-user venv/bin/python3 -c "import boto3;boto3.client('s3').download_file('YOUR_S3_BUCKET','transparency/top_domains.tsv','/data/transparency/top_domains.tsv')"

# 2. deploy the alias-aware stream collector (file >97KB? it's ~9KB, SSM-safe)
#    copy stream_demand_collector.py to /home/ec2-user/hash_trackerv2/, py_compile, chown

# 3. discover movie/series sites
cd /home/ec2-user/hash_trackerv2 && sudo -u ec2-user venv/bin/python3 dmca_site_discovery.py          # dry-run: review candidates
sudo -u ec2-user venv/bin/python3 dmca_site_discovery.py --apply                                       # insert as 'candidate'

# 4. validate liveness (candidate -> live), then the 05:15 collector scrapes them
sudo -u ec2-user venv/bin/python3 streaming_liveness.py     # or the existing liveness sweep

# 5. verify: live-site count up, movie/series titles now showing presence
```

## Manual candidate seeds (from web search 2026-06-11 — low yield, search suppresses pirate sites)
Only 2 surfaced cleanly; add via dmca_site_discovery's candidate path or manually:
```
fboxtv.stream        # FBoxTV — movies + series
watchseriestv.net    # WatchSeries — series
```
(`bestfreestreaming.org` is an AGGREGATOR that lists many pirate sites — a future seed to crawl, not a
site to scrape. NOT staged: the curated "best free" articles are legal-only ad services — exclude.)

## ⭐ THE REAL BOTTLENECK (confirmed 2026-06-12 via clean A/B)
A same-HTML A/B on live sites showed the movie/series gap is a **FETCHING problem, not a matching one**:
movie/series sites (voir-film.cc, voirfilms.club, seriesfree.to, …) **FETCH-FAIL** under the static
`curl` in `fetch()` — they're behind **Cloudflare / anti-bot**, or dead. The anime sites work because
they serve plain HTML. So extraction upgrades (now done) don't move movie/series until the page fetches.

DONE (deployed, help fetchable sites, NO regression):
- alias-aware matcher (+8,471 keys) — commit 518e251
- embedded-JSON title extraction (JSON-LD / __NEXT_DATA__, unicode-decoded) — commit fa253c9

DEFINITIVE FINDINGS (2026-06-12 experiments):
- **24 of 89 live sites FETCH-FAIL** (~27%): dead cyberlockers (streamango/streamcloud/upstream/estream/
  youwatch), Cloudflare-gated movie sites (bmovies.to, cuevana3.ch, seriesfree.to, voir-film.cc,
  lordfilmu, gudfilm2), some transient rate-limit (gogoanimes.fi — was hit 3× during testing).
- **Fetchable ≠ usable:** fboxtv.stream (59 KB) & watchseriestv.net (90 KB) fetch fine but yield **0
  catalog matches** — they serve a **JS app shell**; titles are client-rendered, not in static HTML.
- **cloudscraper is NOT sufficient** (didn't init cleanly; won't beat modern Cloudflare). The
  cloudscraper-fallback in `fetch()` stays as a dormant no-op (commit 5e46313); box left dependency-clean.

CONCLUSION: movie/series coverage is a **fetch+render** problem that needs a real JS-executing fetcher.
This is a deliberate infra/cost decision for the user — NOT bolted on unattended:
  1. **FlareSolverr** (Docker service, proxies requests through a headless Chromium that solves Cloudflare
     + renders JS) — best fit; wire `fetch()` to call its local endpoint on FETCH-FAIL. **Recommended.**
  2. **Playwright/headless Chromium** in-process (~hundreds of MB install) — heavier, more control.
  3. **Commercial streaming-intel feed** — if build cost isn't worth it.
PARALLEL low-effort win: **prune the dead cyberlockers** from the registry (streamango/streamcloud/
upstream/estream/youwatch are long dead) so the live-count reflects reality.

WHAT'S DONE & HELPING NOW (fetchable static-HTML sites, mostly anime): alias-aware matcher (+8,471 keys)
+ embedded-JSON extraction. These lifted anime/foreign-title coverage; movie/series stays blocked on (1).

## ⚠️ Standing constraint (user, 2026-06-11)
**Never load huge data into memory.** The 12 GB Transparency job streams + aggregates on disk
(`transparency_ingest.py` already does this). Keep that pattern for any large-data work, and keep the
12 GB job OFF the I/O-tight prod box.
