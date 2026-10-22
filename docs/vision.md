# Product vision

What the author wants from newsrag, in priority order (brainstorm of 2026-10-22). Phase
designs should trace their choices back to this page. Open questions are marked **Open**.

## 1. Opinion search: core
A free-text opinion query ("Trump is good for energy") returns the articles and the exact sentences taking that stance, with opposing views shown separately, plus entity and date filters. This is unchanged from the overview and is the core product.

## 2. Visual entity graph: core
- Explore the entity map **visually**, as a subgraph around what you're looking at, not the whole graph at once.
- **Lineage queries:** "how does entity A relate to entity B, if at all?" Show the connecting paths, and let any node be **expanded**.
- **Entity priorities:** people in the news, companies, places, legislation and organizations.
- **The interesting part is background entities:** relationships that run through entities that are rarely or never in the news (a company's owner, a spokesperson's employer, a board seat). So the graph can't be built only from news co-mentions. It needs **knowledge-base relations**, chiefly from Wikidata: employer, owner, member of, position held, subsidiary, family, and so on.
- **Open:** how far to expand (hops, edge types), and how to rank paths.

## 3. Entity timeline: a tab
- Each entity gets a timeline page: articles over time, stance over time, and structured events (SEC filings first, via the EDGAR pipeline in ideas.md; later court records and video).
- It's navigated to from the entity or graph view, not the centrepiece.

## 4. Publisher lens: low priority, except staff
- Comparing outlets is a means to an end, not a goal.
- **Exception:** who works for a publisher (journalists, editors and owners, from Wikidata), because those links surface non-obvious relationships in the graph.

## 5. Notifications: the end goal and likely selling point
- **Email subscriptions** for breaking news on:
  - a **topic**
  - a **stance** ("alert me when someone argues X")
  - an **entity**
  - a **relationship between entities** (a new link appears, or coverage of an A–B connection spikes)
- **Open:**
  - what counts as "breaking" (burst detection?), digest versus instant, and rate limits
  - multi-user accounts: the decision log already plans for users beyond the author
- **Implications:**
  - Freshness matters: discovery lag feeds straight into notification latency, which raises the priority of the poll-scheduling fix (ideas.md).
  - Matching must run incrementally as Documents arrive, not only at query time.
  - Sending email needs the VPS edge (decisions.md, 2026-10-19).

## 6. Evaluation queries
The author is writing the first opinion queries. They become the Phase 2 evaluation set (30 labelled queries; see overview.md, "How good results are judged").

## Design consequences to carry into Phase 2 and 3
- **Entity linking** should target Wikidata QIDs, as the contracts already plan with `wd:<QID>`, and the graph needs a **local Wikidata subset** for entity-to-entity relations: properties such as P108 employer, P127 owned by, P463 member of, P39 position held, P355 subsidiary. Keep it local, per the local-first rule.
- **Graph storage and path queries** (shortest or all paths up to k hops, plus expansion) become an explicit engine choice alongside the vector-store decision.
- **Edges keep their provenance:** each edge says whether it came from Wikidata, a filing, or co-mention or extracted relations in news, with confidence, consistent with "links are scored edges".
