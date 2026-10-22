# Prototypes

Throwaway measurement code for design decisions. It's not part of the `newsrag` package and
not covered by the contracts. Results and decisions go into `docs/decisions.md`. Data
lives under `$NEWSRAG_DATA_DIR/prototype/`.

- `vector_store/`: P2.1 vector-store choice (LanceDB vs Qdrant vs Neo4j's vector index)
  on the archive's real passages: `embed_corpus.py` (segment + embed), `bench.py` (load,
  append, filtered search, recall@50 against exact search, disk).
