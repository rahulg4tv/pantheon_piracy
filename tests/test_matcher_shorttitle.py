import sys; sys.path.insert(0,'.')
import importlib, trending_hash_collector as t; importlib.reload(t)
m = t._name_matches_title
# (raw_name, title, year, category, expected)
CASES = [
  # ---- short-title equality (year omitted) ----
  ("Michael.2026.1080p.HDTS.h264.Dual.YG",              "Michael",        "", "Movies", True),
  ("Sinners 2025 1080p WEBRip x265-DH",                 "Sinners",        "", "Movies", True),
  ("Michael Clayton (2007) (1080p BluRay x265 Silence)","Michael",        "", "Movies", False),
  ("Fantastic Four (2015) [Michael B. Jordan] 1080p",   "Michael",        "", "Movies", False),
  ("Beast Of War 2025 1080p WEB-DL HEVC x265",          "Beast",          "", "Movies", False),
  ("Lilo & Stitch (2025) [1080p]",                      "Lilo & Stitch",  "", "Movies", True),
  # ---- NEW: confident year-mismatch reject (same title / sequels / remakes) ----
  ("Masters of the Universe.2026.1080p.TeleCine",  "Masters of the Universe","2026","Movies", True),
  ("Masters.Of.The.Universe.1987.1080p.BluRay.HEVC","Masters of the Universe","2026","Movies", False),
  ("Michael 1996 1080p BluRay HEVC x265 5.1 BONE",      "Michael",   "2026","Movies", False),
  ("Michael.2026.1080p.TELESYNC.V2.x264-SyncUP",        "Michael",   "2026","Movies", True),
  ("Toy Story 3 (2010) 1080p BrRip x264 - YIFY",        "Toy Story 5","2026","Movies", False),
  ("Toy.Story.5.2026.1080p.WEB-DL",                     "Toy Story 5","2026","Movies", True),
  ("Despicable Me 2 (2013) 1080p BrRip x264",           "Despicable Me 4","2024","Movies", False),
  ("The Running Man 1987 REMASTERED 1080p BluRay",      "The Running Man","2025","Movies", False),
  ("The Running Man (2025) [1080p] [WEBRip] [5.1]",     "The Running Man","2025","Movies", True),
  ("How to Train Your Dragon (2010) 1080p BrRip x264",  "How to Train Your Dragon","2025","Movies", False),
  ("How to Train Your Dragon 2025 1080p WEB",           "How to Train Your Dragon","2025","Movies", True),
  # year within +/-1 must NOT reject on year (then title rule decides)
  ("Lisa Frankenstein 2024 1080p WEBRip",               "Frankenstein","2025","Movies", False),  # title rule rejects (extra word)
  ("Frankenstein.2025.1080p.NF.WEB-DL",                 "Frankenstein","2025","Movies", True),
  ("Sinners 2025 1080p",                                "Sinners",   "2025","Movies", True),
  # catalog year None -> no year reject (falls through)
  ("Lilo & Stitch (1789) weird",                        "Lilo & Stitch", "", "Movies", True),
  # ---- SERIES unaffected (year present but category Series -> no year reject) ----
  ("The Boys S05E06 1080p WEB h264-ETHEL",              "The Boys",  "2019","Series", True),
  ("Euphoria US S03E05 1080p AMZN WEB",                 "Euphoria",  "2019","Series", True),
  # ---- longer movie unchanged ----
  ("Jurassic World Rebirth 2025 1080p WEB",        "Jurassic World Rebirth","2025","Movies", True),
]
fails=0
for raw,title,yr,cat,exp in CASES:
    got=m(raw,title,yr,cat); ok=(got==exp); fails+=0 if ok else 1
    print(f"  {'PASS' if ok else 'FAIL'} exp={str(exp):5} got={str(got):5} [{cat} y={yr or '-'}] '{title}' <= {raw[:42]}")
print(f"\n{'ALL PASS' if fails==0 else str(fails)+' FAILED'}  ({len(CASES)} cases)")
sys.exit(1 if fails else 0)
