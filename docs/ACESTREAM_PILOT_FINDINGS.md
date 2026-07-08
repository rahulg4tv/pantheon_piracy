# AceStream live-IPTV pilot — findings & go/no-go

*2026-06-13. Pilot for the #1 "beat MUSO" channel: P2P live IPTV/sports demand.
Code: `acestream_pilot.py`. Background research: `docs/COMPETITIVE_RESEARCH_BEAT_MUSO.md`.*

## Verdict: ✅ GO — and cheaper than planned (no engine, no new box, no SG change)

The pilot's core risk was: *can we get an AceStream channel's BitTorrent infohash and count
its peers with our existing pipeline, without running an AceStream engine?* **Yes, on both counts.**

## What we proved (empirical, 2026-06-13)

### 1. Channels resolve to real infohashes via a server-side API — no engine
`https://search.acestream.net/` (301 → `api.acestream.me`), key `test_api_key`:
```
GET /?method=search&api_version=1&api_key=test_api_key&query=sport&page_size=50
→ results[] = { infohash(40-hex), name, categories[], country[], language[],
                availability(0-1), bitrate, availability_updated_at }
```
So enumeration gives us the **infohash AND rich metadata** (channel name, **country**, **category**,
availability) in one call. That's both the worklist and a built-in geo/category prior. 109 results
for "sport" alone.

### 2. AceStream live channels are on the PUBLIC MAINLINE BitTorrent DHT
A self-contained BEP-5 `get_peers` prober (mainline bootstrap nodes), each ~15–18s:
```
CONTROL (Ubuntu 24.04, known mainline torrent):  peers=10   ← prober sanity-check
Sky Sport F1 [DE]         peers=169   avail=1  [de]  [sport]
Sky Sports Main Event[UK] peers=233   avail=1  [gb]  [sport]
BT Sport 3 [UK]           peers=156   avail=1  [gb]  [sport]
Eleven Sports 1 HD [PL]   peers=1     avail=1  [pl]
BT Sport 2 [UK]           peers=0              (idle right now / prober short-budget)
Polsat Sport [PL]         peers=0
```
The big live channels return **hundreds of concurrent peers on the mainline DHT** — more than a
typical VOD torrent — using a crude short-budget prober. So this is a **floor**: production
`dht_peer_count.get_peers_by_country` (shared node-pool, concurrency, BEP-33) will see more.

### Why this is the good outcome
Running an AceStream engine would have meant: ~GB of RAM, a new box (cost + SG change needing
your OK), and our node **uploading** infringing live chunks (legal exposure). **Direct mainline-DHT
scraping avoids all three** — it's passive observation of publicly-announced peers, the *same legal
posture as our existing BitTorrent collection.*

## Architecture (final)
```
AceStream search API ──► infohash worklist + {name, country, category, availability}
                              │
                              ▼
        our mainline-DHT get_peers (dht_peer_count) ──► distinct peer IPs
                              │
                              ▼
              GeoLite2 (same mmdb) ──► distinct peers per CHANNEL per COUNTRY
                              │
                              ▼
        acestream_demand(run_ts, infohash, name, category, ch_country, peer_country, peers)
```
**Demand unit = distinct concurrent peer IPs per channel** — true, geo-located *live* demand, which
no competitor publicly produces (MUSO is visit-based and explicitly misses IPTV).

## The one real difference vs the VOD pipeline: cadence
Live events are short. Daily sampling is meaningless for a match. Production must sample on a
**minute / 5-min cadence** during events and report **peak + area-under-curve per match**.
`content_id/channel → event → league` mapping needs an **EPG** layer (later step;
github.com/iptv-org/epg). For always-on channels (Sky Sport F1) periodic sampling is already useful.

## Pilot status / what `acestream_pilot.py` does
Self-contained: enumerate (search API) → probe (built-in BEP-5, swappable for our production
counter) → geolocate (GeoLite2 if present; graceful `??` fallback locally) → write
`acestream_demand` rows + ranked console summary. Memory-safe (one channel at a time).

### Local prototype snapshot (2026-06-13, 12 channels, geo OFF locally)
`python3 acestream_pilot.py --limit 12 --budget 12` → **419 distinct live peers across 9/12
channels** in a single pass (crude prober; production counter will see more):
```
168  [gb]  BT Sport 3 [UK]             sport
123  [gb]  Sky Sports Main Event [UK]  sport
 75  [gb]  BT Sport 2 [UK]             sport
 21  [pl]  Canal+ Sport 2 HD [PL]      regional
 12  [de]  Sky Sport 1 HD [DE]         regional,sport
  9  [pl]  Eleven Sports 1 4k [PL]     regional,sport
  6  [pl]  Polsat Sport [PL]           regional,sport
  4  [us]  FOX Sports 2 HD [US]        sport
  1  [gb]  Sky Sports Cricket [UK]     sport
```
Peer-country breakdown shows `??` locally (no GeoLite2 mmdb on the dev box); on the prod box it
resolves to real per-country counts via the existing mmdb. Channel-declared country (`ch_country`)
is captured regardless and already gives a usable geo prior.

## SHIPPED 2026-06-13 — on the box + in the dashboard
- `acestream_pilot.py` deployed to `/home/ec2-user/hash_trackerv2/`, writing `acestream_demand`
  in `/data/db/acestream_pilot.db`.
- **Hourly cron** (ec2-user, `:05`): `acestream_pilot.py --limit 30 --budget 10 --geo
  /data/geoip/GeoLite2-Country.mmdb`.
- **Dashboard panel LIVE**: new **"Live Sport"** tab in `pantheon_web.py` + `/api/acestream`
  endpoint. First box run: **13 channels, 64 peer-countries, 447 distinct peers**, geolocated.
- Sanity: PL/TR channels lead with their origin country ✓. The big UK channel (Sky Sports Main
  Event) leads with **VN** over GB — the expected VPN/datacenter/international-audience signature;
  surfaced with an honest caveat in the panel, to be discounted later by the residential-weighting
  methodology step.

## UPDATE 2026-06-13 — BEP-33 added (fixes "peers look low")
The first run headlined the raw `get_peers` **sample** (a weak DHT slice) — counts looked low. Fixed
by upgrading the pilot prober to a **converging Kademlia walk + BEP-33 scrape** (`scrape=1`, OR-merge
BFsd/BFpe, `_estimate_bloom` lifted verbatim from `dht_peer_count`). Headline demand is now
**`bep33_leechers`** — the swarm-size estimate, the SAME metric the title feed ranks on (task #24).
Local proof (demand vs old raw sample): Sky Sports Main Event **472 vs 124 (3.8×)**, BT Sport 3
**301 vs 32 (9.4×)**, Eleven Sports 1 HD **181 vs 15 (12×)**. `seeders=0` is correct for live (no
complete copy — all leechers). Dashboard panel now shows **Demand (leechers)** + Seeders columns;
raw sample retained only for the geo split. Cron bumped to `--budget 12` for BEP-33 convergence.

## Known pilot limitations (follow-ups)
- **Still pilot-grade walk** (single socket, no shared 200k-node pool). The remaining upgrade is to
  reuse `dht_peer_count.run()` / `get_peers_by_country` so AceStream rides the production node-pool +
  concurrency for even higher recall. BEP-33 already closed most of the gap.
- **Raw geo includes VPN/datacenter IPs** — Sky Sports' VN-over-GB. Apply residential weighting /
  ASN datacenter discount (methodology layer) before the geo split is a headline metric. (Demand/
  leechers is swarm-wide so less affected; the per-country split is the part that needs hardening.)
- ~~`acestream_pilot.db` accumulates with no retention~~ **DONE 2026-06-13** — daily cron (ec2-user,
  04:17) prunes `run_ts` older than 7 days (`DELETE … WHERE substr(run_ts,1,10) < date('now','-7 days')`).
  DB self-caps at ~7 days. Dashboard reads MAX(run_ts) so unaffected.

## Recommended productionization (next steps, in order)
1. **Swap the pilot prober for `dht_peer_count.get_peers_by_country`** so AceStream infohashes ride
   the production node-pool/concurrency/BEP-33 (higher recall, same geo path). Lowest effort, biggest
   accuracy gain.
2. **Run on the prod box (or a tiny worker)** as a scheduled sampler. Decision needed: cadence + whether
   it shares the box with the VOD DHT collector or runs separately (it's the same UDP/DHT workload —
   likely fine to co-locate at a modest channel count; revisit if it pressures the box).
3. **EPG layer** — map channels→events→leagues for per-match demand (the highest-value studio cut).
4. **Catalog join** — AceStream channels are linear TV, not titles; key them to a channel/league
   catalog (new dimension) rather than imdb_id/MAL. Keep as a separate `acestream_demand` table; blend
   only at the index/ranking layer, never summed into the title feed.
5. **Decoy/sybil hardening** reuses the methodology-layer work (residential weighting, behavioral
   verification) — live swarms are a juicy decoy target, so this matters here too.

## Legal/ethical
Passive peer observation only (no engine, no subscribe, no upload, no relay). Same posture as the
existing DHT collector. Store aggregate per-country counts; no PII. The search API is a public
server-side endpoint. No infra/SG change required for the direct-scrape pilot.
