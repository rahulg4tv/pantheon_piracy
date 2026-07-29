# intel/ — turning the feed into something a person reads

Steps 1–3 produce a correct number per title per country per day. This folder is
about **making that legible** and **not trusting it blindly**.

## `pantheon_intel.py` — the dashboard database

Runs hourly. It invokes [`../export_nbcu.py`](../export_nbcu.py) for the current
day, ingests that CSV plus the historical daily files, and builds
`pantheon_intel.db` — the store [`../pantheon_web.py`](../pantheon_web.py) serves.

Two things worth knowing:

- **It re-exports only the CURRENT day.** Historical days are read from their
  existing files, never regenerated, because a past day is not reproducible
  (see [`../docs/DATA_MODEL.md`](../docs/DATA_MODEL.md) §5).
- **Its rollups are sums, and sums are not distinct counts.** Per-title numbers
  come from the distinct-IP union and are exact. But a country total, or a
  headline "total demand" figure, adds those per-title numbers together — so one
  person pirating three titles counts three times. That is fine as a *trend*
  line; it is not a distinct-person count, and should not be labelled as one.

## `decoy_detect.py` — is the demand real?

Public swarms contain deliberate poison: fake peers injected to inflate or
disrupt a swarm, and datacenter ranges that are one seedbox rather than many
people. This flags titles whose peer mix looks manufactured — heavy datacenter
concentration, implausible peer patterns — and alerts when a **high-demand**
title is affected, since that is when a distorted number would actually mislead.

Datacenter IPs are flagged, **not removed**: a VPN user is a real pirate, and
dropping them understates demand. The signal is there so a consumer can take a
residential-only view deliberately.
