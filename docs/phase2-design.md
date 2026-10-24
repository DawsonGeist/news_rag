# Phase 2 design: segment, embed, extract entities, link, evaluate retrieval

Status: **approved design, 2026-10-22** · build not started

Phase 2 turns the archive into searchable, entity-aware data. Every stored Document gets:
- **segments:** paragraphs, sentences and passages, each with locators
- **vectors**
- **entity mentions**, linked to Wikidata or kept as local entities
- **snippets** around each mention

It also brings a local Wikidata subset that supplies entity attributes and relationships, and the evaluation tooling that decides Gate 2. All of it is derived data: rebuildable from the archive and versioned.

Read first: [vision.md](vision.md) (what the author wants), [decisions.md](decisions.md) (especially 2026-10-22, Wikidata and entity linking), [phase2-models.md](phase2-models.md) (model shortlist and VRAM plan), and [contracts/v1/README.md](../contracts/v1/README.md).

## Scope

| # | Component | Phase 2 deliverable |
| --- | --- | --- |
| 5 | Segmenter | Paragraph, sentence and **passage** segments with code-point locators into `extracted_text` |
| 6 | Embedder | Passage vectors, quantized, in the vector store chosen by the prototype |
| 7 | NER and entity linker | Mentions, MentionLinks to Wikidata or local entities, snippets, and coreference within each document |
| — | Knowledge base | `newsrag kb build`: a local Wikidata subset (1 hop) of labels, aliases, attributes and relations, as a versioned snapshot |
| — | Entity graph store | Entity nodes plus scored edges (Wikidata relations, co-mentions, later filings), with path queries for the vision's "how does A relate to B" |
| 8 | Storage writer (extended) | Derived tables written with the same durability protocol, one writer per table group |
| 13 | Observability and eval | Retrieval eval (the author's opinion queries), the entity-linking test set, and index lag |

**Not in Phase 2:** the query service, stance judgment at query time and the frontend (Phase 3). Notifications come after Phase 3. Stance is only *prepared* here: snippets exist, so stance can run on them later.

## What the archive looks like (measured 2026-10-22)

| | |
| --- | --- |
| Documents | 256,130, of which 97% `full_text` and 99.2% English |
| Volume | about 35k/day on weekdays, about 17k/day at weekends |
| Words per document | mean 770, median 544 |
| Paragraphs per document | mean 27, median 18, p90 46, p99 177. **About 28 words each:** news paragraphs are one or two sentences. |
| Sentences per document | about 41 |

**Consequence:** embedding each paragraph would mean about 950k vectors/day, many too short to carry meaning ("He declined to comment."). The retrieval unit should be a **passage**: consecutive paragraphs packed to about 120 to 200 words. That cuts the vector count about 4× and gives each vector enough context. The prototype confirms this on the eval queries (passage vs. paragraph recall).

## Process model

```
 storage writer ──(doc_revisions, Parquet)──▶ enricher (GPU worker, micro-batches every ~60 s)
                                              segment → embed → NER → coref → link → snippets
                                              ├─▶ derived Parquet tables (segments, mentions, links, snippets)
                                              ├─▶ vector store (passages)
                                              └─▶ graph store (entities, edges)   ◀── kb build (monthly)
```

- **A new long-running process, `newsrag run enricher`,** under the supervisor. It's the "GPU worker" of the residency plan, and it yields the GPU when the Phase 3 query service needs it.
- **Work discovery:** the enricher reads `doc_revisions` (writer-owned; enrichers may read it) for revisions with no completed enrichment at the current `pipeline_version`. Progress lives in a new enricher-owned table, `enrichments(doc_id, revision, pipeline_version, state, lease_owner, lease_expires_at, …)`, with the same lease pattern as the frontier. Re-running with a new model is just a new `pipeline_version`.
- **Micro-batches** (decision 2026-10-22): claim up to N revisions or whatever arrived in the last about 60 s, run the stages as GPU batches, and commit. Target: index lag (stored → searchable) p95 under 15 min, the Gate 2 criterion.
- **Durability:** derived tables follow the Phase 1 protocol (stage in a `TmpArea`, then install plus manifest rows plus the `enrichments` state in one transaction). **Change from Phase 1:** "only the storage writer touches `tables/`" becomes "**one writer per table group**". The enricher is the single writer of the derived tables and takes its own writer lock. Recovery, compaction and `verify` cover every table group. This matches the overview's "one writer per table".

## Segmenter (component 5)

- **A pure function**, `Document → [Segment]`, CPU-only, run inside the enricher.
- **Paragraphs** are exact, since `extracted_text` separates them with `\n\n`.
- **Sentences:** a rule-based splitter tuned for news (abbreviations, quotes, initials), such as pySBD or spaCy's `senter`. Measured on a hand-checked sample before choosing.
- **Passages:** greedily pack consecutive paragraphs up to about 200 words, never splitting a paragraph. A very long paragraph is its own passage, split at sentence boundaries beyond about 400 words. There's no overlap at first; the prototype tests a one-paragraph overlap.
- **Locators** are `TextLocator(field="body", start, end)` in code points; titles are a `field="title"` segment. Invariant (tested): `segment.text == extracted_text[start:end]`.
- `segment_id = hash32(doc_id:revision:level:start:end:chunker_version)`, which the contract already specifies.
- **Contract change (provisional record, allowed):** add `SegmentLevel.PASSAGE`, with `parent_id` unused for passages. Sentences point to their paragraph, and a passage lists the paragraph range it covers through its locator.

## Embedder (component 6)

- **Embed passages, plus the title** as its own short segment. Sentences are scored at query time (Phase 3, step 4 of the query path) rather than stored as vectors, as decided in phase2-models.md.
- **Model:** the prototype compares bge-m3 and Qwen3-Embedding-0.6B at 1024 and 512 dimensions on the eval queries; the ceiling is Qwen3-Embedding-4B. Every vector records `model_id` (with revision) and `pipeline_version`.
- **Storage budget** at about 35k docs/day, about 7 passages/doc (to be measured) ≈ 245k vectors/day:

  | Format | Per year |
  | --- | --- |
  | 1024 × int8 | about 92 GB |
  | 512 × int8 | about 46 GB |

  Both fit the 2 TB disk for many years, unlike the paragraph-level estimate of 177 to 355 GB/yr. `report disk` adds vectors to its forecast.

## Vector store: Qdrant (decided 2026-10-23 after the P2.1 prototype, see p2.1-vector-store.md)

What it must do: kNN over passages, **filtered** by entity (passages in documents that mention entity X) and by date range. The overview names filtered search as the deciding factor. It must also add vectors continuously with the micro-batches, and survive power loss, recovering to a consistent state.

| Option | Pros | Cons |
| --- | --- | --- |
| **Neo4j vector index** (the graph store already chosen) | One service for graph and vectors; vectors next to entities | Filtered kNN at millions of passages needs measuring |
| **LanceDB** (embedded, on-disk) | Columnar like the archive; vector index plus scalar filters; no server; versioned datasets | Write path and crash behaviour must be checked against our durability protocol |
| **Qdrant** (server, Docker) | Mature filtered HNSW (payload indexes), snapshots | A separate service and its own durability model; Docker |
| **Parquet + FAISS/hnswlib** (DIY) | Full control | We'd own filtered search, incremental updates and crash safety. The overview advises against hand-building. |

Prototype protocol, on the real archive (all 256k docs, about 1.8M passages):
- **Measure:** ingest rate, index build time, disk size, filtered and unfiltered query latency at p50 and p95, recall versus exact search, and behaviour after a SIGKILL mid-write.
- **Decide** with the eval queries' recall@50 in view.
- **Record** the result in decisions.md.

## Entity extraction and linking (component 7)

**Steps per micro-batch:**
1. **NER on sentences:** GLiNER large, zero-shot labels mapped to `EntityType`: person, organization, location, event, law, product, work. spaCy `trf` is the comparison. Output: `Mention`s with title mentions flagged (`locator.field == "title"`).
2. **Coreference within the document** (fastcoref): resolves "he", "the senator" and "the company" to a named mention, so short or bare references inherit links. Pronoun-only snippets get their target this way, which addresses the overview's "He signed the order Tuesday" problem.
3. **Candidate generation:** the alias index from the KB subset (Wikidata labels and aliases, normalized), plus ReFinED's own candidates.
4. **Scoring** (decision 2026-10-22): popularity prior, type agreement, context match, coherence with the document's other links, time validity (positions held with dates, compared with `published_at`) and locality (publisher region against the place). ReFinED goes first, and our signals are added where measurement shows they help.
5. **Decide:**
   - Link the top candidate if it clears the **high threshold** (precision first).
   - Keep the close runners-up as lower-confidence `MentionLink`s.
   - Otherwise a `local:` entity, `hash32(type:normalized surface)`. Local entities with the same name and similar contexts are clustered in a later pass, not merged now.
6. **Snippets:** about N words either side of each linked mention (N decided by the stance needs; start with the sentence plus one neighbour each side), which is where stance will run.

**Manual corrections** are final, like feed decisions. A `link_overrides` table (entity linker-owned) holds rules like "mention X in doc D is entity E" and "never link surface S to E". They're applied in step 5 and survive re-linking.

**Contract changes (provisional records):**
- `MentionLink` and `Entity` gain `kb_snapshot`, the Wikidata snapshot they were built from.
- A new **`EntityRelation`** record: `src_entity_id`, `dst_entity_id`, `relation` (a Wikidata property or `co_mention`), `confidence`, `source` (`wikidata`, `news`, later `sec_edgar`), `valid_from`/`valid_to` (from Wikidata qualifiers), `evidence` (doc_ids for co-mentions), and the versions.

## Knowledge base: local Wikidata subset

`newsrag kb build --dump <path>` streams the Wikidata JSON dump (about 130 GB compressed) once a month and keeps only what's needed:
1. **Seed entities:** instances or subclasses of people, organizations (companies, agencies, parties, publishers), places, legislation and events, with an English label, plus at least one sitelink or a selected identifier (CIK P5531, ticker P249, official website P856).
2. **1 hop** (decision 2026-10-22): every entity directly related to a seed through the selected properties: P108 employer, P127 owned by, P749 parent, P355 subsidiary, P463 member of, P39 position held, P102 party, P26 spouse, P40 child, P22/P25 parents, P3373 sibling, P488 chair, P169 CEO, P112 founded by, P131 located in, P17 country, P159 headquarters.
3. **Store** labels, aliases and descriptions; the selected claims *with qualifiers* (start and end time); sitelink counts (the popularity prior); and identifiers. The format is Parquet in `data/kb/<snapshot>/`, queried with DuckDB, plus an alias index for fast lookup.

The expected size is a few million entities and a few tens of GB, measured on the first build. Every link and relation records the snapshot, so a refresh is just a new `kb_snapshot` and a re-link.

**Publishers:** each `source_id` domain is matched to Wikidata's official website (P856), which makes the publisher an entity, bringing in its owner and staff (vision §4).

## Entity graph store: Neo4j (decided 2026-10-22)

**What it must do** (vision §2): get the subgraph around an entity; find the paths between A and B up to k hops (start with k ≤ 4); expand any node; keep per-edge confidence for the client-side slider; and absorb continuous updates from the micro-batches.

| Option | Notes |
| --- | --- |
| **DuckDB** with recursive CTEs, or its property-graph extension (DuckPGQ) | Already in the stack; path queries over Parquet edge tables. Performance at multi-hop depth needs measuring. |
| **An embedded graph database** (e.g. Kùzu: Cypher, columnar, embedded) | Built for path queries. **Verify project status first:** check whether it's still maintained. |
| **Neo4j Community** (server) | Mature, Cypher, a rich ecosystem; another service to run |
| **In-memory** (NetworkX/rustworkx) over a loaded subgraph | Fine for expanding around a node; not for global path search over millions of edges |

**Decided: Neo4j Community, run locally** (decisions.md 2026-10-22). P2.2 now sets up its schema rather than choosing an engine:
- **Nodes:** `Entity` (`entity_id` `wd:Q…`/`local:…`, type, label, `kb_snapshot`), plus `Publisher`.
- **Edges:** `RELATION` (from `EntityRelation`, with `relation`, `source`, `confidence`, `valid_from`/`valid_to`), and `CO_MENTION` (aggregated per entity pair, with document count, combined confidence, and first/last seen).
- **Load and test:** load the 1-hop KB graph plus co-mention edges from the archive, then time the subgraph, path (up to 4 hops) and expand queries, and the micro-batch update cost.
- **Operations:** Neo4j runs as its own service (Docker or a system package), with backups via `neo4j-admin dump`. It's a **derived store**: rebuildable from the archive plus the KB snapshot, so losing it costs only time.

## Evaluation (component 13) and Gate 2

- **Opinion queries:** the author writes them (in progress). Claude proposes silver labels (supporting, opposing, off-topic) from retrieval candidates; **the author verifies them**, because they're the ground truth. File: `eval/queries.yaml` (committed; labels reference `doc_id`s, not copyrighted text).
- **Entity-linking test set:** about 300 mentions sampled across publishers and types, labelled with the correct QID or "none". Same workflow: Claude proposes, the author verifies.
- **Tools:** `newsrag eval retrieval` reports recall@50 and nDCG@10; `newsrag eval linking` reports precision and recall at the chosen threshold.

**Gate 2 (proposed; targets set once the labels exist):**

| # | Criterion |
| --- | --- |
| G2.1 | Retrieval: recall@50 of supporting articles at or above the target on the labelled queries (target set after the first measurement) |
| G2.2 | Entity linking: precision at or above 0.9 at the default threshold on the linking test set; recall reported |
| G2.3 | Index lag (stored → searchable) p95 under 15 min over a 72 h run with the full pipeline |
| G2.4 | The kill test extended to the enricher passes 20/20, and re-enrichment is reproducible for unchanged versions |
| G2.5 | The disk forecast, now including vectors and the graph, fits 12 months |

## Build order

1. **P2.0 Contracts** (done): `SegmentLevel.PASSAGE`/`TITLE`, `EntityRelation`, `kb_snapshot`; the `enrichments` and `link_overrides` DDL (`enrichment_state.sql`); table groups with one writer and lock each, plus scoped recovery and verification in storage.
2. **P2.1 Segmenter** (done, `seg-1`), plus the vector-store prototype on the real archive (passages, two embedders) → decision.
3. **P2.2 KB build** (1 hop): `newsrag kb build` (dump → shards → snapshot) built and tested; the first full snapshot (`20261019`) builds after the dump download. Graph store decided: Neo4j.
4. **P2.3 Enricher process** (built, `enrich-1`): segment and embed in micro-batches into Qdrant, newest first, with index lag in `report lag`; backfill of the archive under `run all`.
5. **P2.4 NER, coreference, linking and snippets** (built: GLiNER, fastcoref, `link-2` fitted on the 300-mention author-verified test set, precision 0.91 at recall 0.62 cross-validated), backfilling under `run all`.
6. **P2.5 Eval tooling,** with the author's queries labelled.
7. **P2.6 Soak** (enricher under `run all`), kill test, and Gate 2 review.

P2.1 and P2.2 are prototypes whose results go back to the author before the code they decide on is written.

## Risks

- **VRAM sharing** with the Phase 3 query service: the yield protocol is designed here, but tested in Phase 3.
- **Kill-test coverage:** the vector store's crash behaviour may not fit "file plus manifest" semantics, so the prototype must test it, not assume it.
- **Entity-linking quality on local news** (small places, local officials not in Wikidata): many `local:` entities. The local-entity clustering pass matters for the graph.
- **The size of the Wikidata dump download:** a monthly transfer of about 130 GB. Check that the ISP or VPS bandwidth is fine.
- **The MarketBeat network** (if kept) would dominate company mentions. Tag it as automated before computing entity statistics (ideas.md).

## Answered by the author (2026-10-22)

1. **Passages** are the retrieval unit; sentences are located at query time.
2. **Gate 2's recall@50 target** is set after the first measurement.
3. **Graph store: Neo4j.** The vector store is still chosen by the P2.1 prototype, among LanceDB, Qdrant and Neo4j's own vector index.

The design is approved in principle. Build follows the order above, with the P2.1 prototype results going to the author before the code that depends on them.
