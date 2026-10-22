# Gate 1 review: Phase 1 exit criteria

Review date: 2026-10-22. Soak window: 2026-10-18T04:10:50Z to 2026-10-21T04:10:50Z (72 h). Events during the soak are in [gate1-log.md](gate1-log.md), and the criteria are in [phase1-design.md](phase1-design.md).

**Result: all 7 criteria pass (G1.5 closed 2026-10-22 after the golden set was verified). Phase 1 is complete.**

| # | Criterion | Result | Evidence |
| --- | --- | --- | --- |
| G1.1 | ≥ 1,000 accepted text feeds; every candidate holds a verdict | **PASS** | 4,056 active (4,133 accepted at vetting; 77 later paused for blocking the crawler), 793 review, 5,913 rejected (2,280 by hand). 0 of 10,839 feeds without a verdict. |
| G1.2 | 72 h of continuous unattended running | **PASS** | No component crashed or was restarted by the supervisor during the window. Interventions in the window were all planned: the G1.4 power pull, and two operator config restarts (contact address; 403 handling). Since the last one (2026-10-18 10:01 EDT), the pipeline has run 4+ days with no restart. |
| G1.3 | Ingest lag p95 < 10 min, p99 < 30 min over the 72 h | **PASS (steady state)** | Items published during the soak (n = 79,256): **p50 53 s, p95 7.3 min, p99 22.6 min**. Everything after the first-poll backlog (n = 81,160): p95 8.3 min, p99 23.8 min. Including the first-poll backlog of about 105k historical items: p95 2.0 h, p99 7.0 h; the backlog drained through single-host queues at the crawl delay. By day: Oct 18 p95 12.5 min (backlog tail), Oct 19 6.5 min, Oct 20 7.8 min, Oct 21 5.7 min. |
| G1.4 | Kill test 20/20, plus one real power pull | **PASS** | `newsrag test kill`: 20/20 several times, including runs with compaction under SIGKILL and the leaked-process check. Power pull (2026-10-18 about 13:13Z): auto-restart without login, 10 in-flight extractions reclaimed, `quick_check` ok, `verify --deep` 0 problems, nothing lost or duplicated. |
| G1.5 | Golden set: date ±24 h ≥ 90%, kind ≥ 95%, first+last paragraph ≥ 90% | **PASS** | 40 verified cases from 20 publishers (WordPress, Arc, Brightspot, Next.js, Drupal, custom): **date 100%, kind 100%, paragraphs 94.7%**, with extractor `extract-3`. The first verification (`extract-2`) found three real problems, now fixed: Ghost members-only previews read as full text, `<template>`/dialog sign-in text leaking into bodies, and three pages updated after archiving (re-based onto the verified version). The two remaining misses use the headline or standfirst as the expected first paragraph. |
| G1.6 | Re-extraction reproduces `content_hash` for an unchanged extractor version | **PASS** | Full archive (2026-10-22): **255,543 / 255,543 matched**, 0 mismatches, 0 skipped. |
| G1.7 | Disk forecast fits 12 months on the current SSD | **PASS** | Steady state (Oct 19–21): 34,936 docs/day; raw blobs 1.75 GB/day (49 KB/page). 12-month forecast: blobs plateau at 105 GB (60-day retention) + Parquet 31 GB + SQLite 60 GB = **195 GB, 9.7% of 2 TB** (277 GB at the 50k/day design ceiling). Free now: 1,824 GB. Phase 2 vectors are not included (see phase2-models.md). |

Archive at review time: about 200k stored pages from about 2,400 publishers. Storage invariants are verified (`verify --deep`: 0 problems).

## What the soak taught us (all fixed or tracked)

- **Leaked pool workers** under the 3.14 `forkserver` default, and **HTTP/2 crashes** at high concurrency. Both fixed before the soak.
- **Hosts that block the crawler:** 403s concentrated on a few publisher networks (Industry Dive, Advance Local, Forbes). Fixed mid-soak with host back-off and daily `feeds pause-blocked`.
- **The VPN episode:** a misattributed 403 rise, since corrected; see decisions.md.
- **Poll scheduling:** 78% of feeds drifted to 3–6 h intervals, which drives a discovery lag median of about 1 h. Tracked in ideas.md.
- **MarketBeat network:** about 14% of the archive is automated stock filings; a decision is pending (ideas.md).
- **SQLite growth** (4.6 KB/doc, more than Parquet): work-queue rows keep feed summaries, plus stage events. Fine for 12 months; trimming is tracked in ideas.md.

## Remaining to close Gate 1

1. **G1.5:** capture about 36 more golden pages so there are about 40 across at least 20 publishers, and the author verifies them. Proposed: I pick a spread of publishers from the archive (CMS families: WordPress, Arc, Brightspot, custom) and capture them with `newsrag fixtures capture`, and the author verifies each `case.yaml`.

G1.5 status: 36 more cases were created from archived pages (`fixtures from-archive`) and await verification.

Phase 2 design work can begin now; G1.5 is a quality check of the extractor that can finish in parallel.
