# One title, end to end

Follow a single film through the whole pipeline. Real structure, tiny numbers, so
you can check every step by hand.

Our title: **Colony**, a 2026 film. In the catalog it is `film-tt34385135`.

---

## Step 1 — Which torrents carry it?

People upload the same film many times. We search the public indexers and find
three releases:

```
A   Colony.2026.1080p.WEBRip.x265-ABC        infohash  a1b2c3…
B   Colony.2026.720p.WEB-DL.x264-XYZ         infohash  d4e5f6…
C   Colony (2026) [2160p]                    infohash  99aa88…
```

An **infohash** is the torrent's fingerprint. Everyone sharing the same release
computes the same one, which is what lets us find them.

**One title → many infohashes.** Remember that; it matters in Step 4.

---

## Step 2 — Is it really our film?

We only have the file name. We have to decide which catalog title it belongs to.

```
"Colony.2026.1080p.WEBRip.x265-ABC"
        ↓  strip the release junk
   title = "Colony"        year = 2026
        ↓  look up the catalog
   Colony (2026)  → film-tt34385135   ✅ year matches
```

There is also an older **Colony (2010)** in the catalog. Same title, different
film. The year is what separates them — without it we would book 2026's audience
onto a 2010 film.

And this one gets rejected:

```
"The.Colony.Survivors.2019.1080p"  →  title = "The Colony Survivors"
   different film, not in the catalog  →  UNMAPPED  ❌ no guess
```

> When we cannot tell, we mark it **UNMAPPED** rather than attach it to something
> that looks close. A wrong number is worse than a missing one.

---

## Step 3 — Who is sharing it?

For each infohash we ask three different sources who is in the swarm. They
overlap, and that is deliberate — each sees peers the others miss.

```
infohash A  ├─ DHT      → 11.11.11.11 , 22.22.22.22
            ├─ Tracker  → 22.22.22.22 , 33.33.33.33
            └─ PEX      → 44.44.44.44

infohash B  ├─ DHT      → 11.11.11.11
            └─ Tracker  → 55.55.55.55

infohash C  └─ Tracker  → 33.33.33.33
```

---

## Step 4 — Count people, not rows

Add those up naively and you get **8**. But look closer:

- `11.11.11.11` appears on infohash A *and* B — one person who grabbed both the
  1080p and the 720p. **One pirate, not two.**
- `22.22.22.22` was seen by the DHT *and* by the tracker. Same person, two
  sources. **One pirate, not two.**
- `33.33.33.33` appears on A and C. **One pirate.**

So we take the **union of distinct IPs across every infohash and every source**:

```
{ 11.11.11.11 , 22.22.22.22 , 33.33.33.33 , 44.44.44.44 , 55.55.55.55 }

   8 observations  →  5 actual people
```

**This is the single most important idea in the system.** Demand is a set union,
never a sum. Any code that adds counts together is counting people twice.

---

## Step 5 — Where are they?

Each IP is looked up in a geo database and assigned to **exactly one** country:

```
11.11.11.11  →  US
22.22.22.22  →  US
33.33.33.33  →  US
44.44.44.44  →  IN
55.55.55.55  →  IN
```

One country per person, even if different sources disagree about where they are —
otherwise the same pirate would be counted in two countries.

---

## Step 6 — The output

One row per **title × country × day**:

```
TITLE   IP_ID             DATE        COUNTRY         IP_COUNT
Colony  film-tt34385135   2026-07-28  United States   3
Colony  film-tt34385135   2026-07-28  India           2
```

That is the product. *"On 28 July, 3 distinct people in the US and 2 in India
were sharing Colony."*

Real days look the same, just bigger: thousands of IPs per title, ~200 countries.

---

## The whole thing in one picture

```
   catalog title                    Colony (2026) = film-tt34385135
        │
        ▼  search indexers
   3 infohashes                     A, B, C
        │
        ▼  match name → title (year check; unsure ⇒ UNMAPPED)
   confirmed as ours
        │
        ▼  ask DHT + tracker + PEX who is sharing
   8 peer observations
        │
        ▼  UNION distinct IPs across hashes and sources
   5 distinct people
        │
        ▼  geo-locate, one country each
   US: 3    IN: 2
        │
        ▼
   2 rows in today's CSV → S3 + dashboard
```

---

## What would have gone wrong without the guards

Each of these is a real bug that was found and fixed:

| If we had… | The damage |
|---|---|
| ignored the year in Step 2 | Colony 2026's audience reported as the 2010 film's |
| added counts in Step 4 | 8 instead of 5 — inflated by 60% |
| let one IP sit in two countries | US and IN both overstated |
| re-run yesterday's export today | ~11% of the day silently missing, file still looks fine |

The pattern behind all four: **the pipeline is a series of chances to
double-count or mis-attribute a person.** Every guard in the code is defending
one of those steps.

---

**Next:** [`DATA_MODEL.md`](DATA_MODEL.md) for the same ideas stated precisely,
with the database schemas and the exact failure modes.
