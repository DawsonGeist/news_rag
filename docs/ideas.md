# Ideas backlog

Ideas that came up while building, parked so they don't derail the current phase. The
design plan (overview.md, phase docs) comes first. An idea moves into a phase design,
and its decisions into decisions.md, only when the author picks it up.

## Pipelines

### SEC EDGAR pipeline: the anchor for Wikidata entities
- **What:** a pluggable pipeline beside RSS, YouTube and CourtListener that ingests SEC EDGAR filings as structured company events: Form 4 insider trades, 13F institutional holdings, 8-K material events, and possibly XBRL financials.
- **Why:**
  - Filings are structured (exact dates, amounts, parties) and authoritative.
  - Every filer has a **CIK**. Wikidata maps CIKs (P5531) and tickers (P249) to entities, which makes public companies near-exact anchors for the entity map, the hardest part of entity resolution.
  - The SEC publishes free APIs with fair-access rules (about 10 req/s, a User-Agent with contact details) that fit the crawler's politeness rules.
- **Use:** company timelines. Link filings to the news that mentions the same company, and build "what happened to X, and what was said about it" views.
- **Open:**
  - Whether to also generate templated sentences from filings for the text pipeline. Decide when the time comes.
  - Analyst ratings aren't in EDGAR.
- Came out of the MarketBeat discussion (2026-10-20).

### MarketBeat network feeds (pending decision)
- marketbeat.com, defenseworld.net, tickerreport.com and americanbankingnews.com: 4 active feeds, about 4,600 machine-generated stock-filing articles a day, about 14% of the archive.
- They restate EDGAR data (13F, Form 4, dividends) plus analyst ratings, with no stance content.
- **Leaning:** stop ingesting them (`newsrag feeds set-status rejected <url> --reason "automated stock filings"`) once EDGAR is planned. **Not done:** the author is still deciding. If they're kept, tag them as automated in Phase 2 so they don't swamp entity statistics.

### Aggregators
- madmadnews.com and freedommail.us mostly rewrite other outlets' stories: opinionated, but secondhand. Review before deciding. Phase 2 near-duplicate detection (`near_dup_of`) may handle them anyway.

## Crawling
- **Poll scheduling from each feed's rhythm.** Proposed for straight after the soak. 78% of feeds sit at 3 to 6 h intervals because the ×0.7/×1.5 rule drifts to the cap. Schedule from each feed's observed gap between items, within 5 to 60 min, and honour `Cache-Control`/`<ttl>`. Expected effect: median discovery lag about 1 h → about 10 min.
- **WebSub push** for feeds that advertise a hub. Needs the VPS edge (see decisions.md, 2026-10-19).
- **Lag report:** exclude hosts in back-off from "oldest ready item", so the line only flags truly stuck work.

## Operations
- **Stage-event rollups** into hourly percentiles after 30 days. Deferred until the table's size matters.
- **A `newsrag` shim on PATH** (`~/.local/bin`), so it works without activating the venv.
