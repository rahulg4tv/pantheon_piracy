# streaming/ — the second demand channel

Torrents are not the whole picture. A large share of piracy is **streamed** from
web sites and IPTV, where there is no swarm to measure and therefore no distinct
peer-IP count. This folder is the separate channel for that demand.

It is deliberately **not** merged into the torrent feed: the two measure
different things by different methods, and averaging them would produce a number
that means nothing precise.

## `stream_demand_collector.py`

Tracks unlicensed streaming sites carrying catalog titles and records demand
signals into `stream_demand.db`. `stream_seed_sites.txt` is the seed list it
expands from; site availability churns constantly as domains are seized and
re-registered, so coverage is re-checked rather than assumed.

## `acestream_pilot.py`

AceStream is a P2P streaming protocol — content is identified by a `content_id`
rather than an infohash, and it is the main channel for **live sport**, which
torrents barely capture because the demand is concentrated in the minutes a match
is being played.

This pilot resolves AceStream `content_id`s to swarms and counts distinct peers,
reusing the same measurement idea as the torrent side. Findings and the go/no-go
assessment: [`../docs/ACESTREAM_PILOT_FINDINGS.md`](../docs/ACESTREAM_PILOT_FINDINGS.md).

Runbook: [`../docs/STREAMING_COVERAGE_RUNBOOK.md`](../docs/STREAMING_COVERAGE_RUNBOOK.md).
