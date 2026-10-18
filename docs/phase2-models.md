# Phase 2 model options on 16 GB VRAM

Status: draft · 2026-10-17 · decided: quality-first stance (14B cascade OK), vectors kept for the full archive. Model picks are still open.

The backend GPU is an RTX 5060 Ti with 16 GB (Blackwell, `sm_120`). This note lists the local models that now fit and proposes a shortlist to evaluate on the 30 labeled queries once Gate 1 is met. Model choices are made by measurement on that set, not by leaderboard.

**Verify before use.** The candidates below reflect what was available as of mid-2026. Check Hugging Face for newer releases and confirm each runtime supports `sm_120` (PyTorch built for CUDA 12.8+, a recent llama.cpp or vLLM build).

## What the GPU has to do

| Workload | When | Volume at the 50k articles/day design point | Latency need |
| --- | --- | --- | --- |
| Embed paragraphs (and maybe sentences) | Ingest, batched | About 1.25M paragraphs/day, roughly 15/s on average | Index lag p95 under 15 min (Gate 2) |
| NER, entity linking, snippet building | Ingest, batched | About 1M snippets/day | Same |
| Query embedding | Query | 1 per query | Interactive |
| Stance NLI over candidates | Query | 200 to 500 pairs per query | Under about 1 s |
| LLM cascade for implicit stance | Query | Top 30 to 50 | A few seconds |
| Whisper and diarization (YouTube, later) | Ingest, per video | — | Hours are fine |

Ingest throughput isn't the constraint. Even the larger encoders below process hundreds of segments a second on this card. The constraints are which models stay **resident at the same time**, and **vector storage on disk**.

## Candidates

Sizes are approximate fp16 weights. Activations and KV cache come on top.

| Role | 8 GB era (implied) | Now feasible | Size | Notes |
| --- | --- | --- | --- | --- |
| Embedder | bge-small-en-v1.5 (384-d) | **BAAI/bge-m3** (1024-d, dense+sparse, 8k context) | 1.1 GB | Its sparse output can feed hybrid lexical search without a separate BM25 index |
| | | **Qwen3-Embedding-0.6B** (1024-d, Matryoshka) | 1.2 GB | Matryoshka truncation to 256 or 512 dimensions cuts storage. Instruction-tuned queries. |
| | | Qwen3-Embedding-4B | 8 GB | Upper bound for the eval. Probably too big to keep resident next to the LLM. |
| Reranker | — | BAAI/bge-reranker-v2-m3, or Qwen3-Reranker-0.6B | 1.1 to 1.2 GB | Topic reranker, run before stance |
| Stance (NLI) | deberta-v3-base NLI | **MoritzLaurer/deberta-v3-large-zeroshot-v2.0** (or `…-mnli-fever-anli-ling-wanli`) | 0.9 GB | Query as hypothesis, snippet as premise |
| NER | spaCy `en_core_web_trf` | **GLiNER large (v2.x)** | 1 GB | Zero-shot labels, so it covers the full `EntityType` set (law, event, product, work) |
| Coreference | — | fastcoref (LingMess) | 0.5 GB | Resolves "He signed the order", the overview's unresolved-subject problem |
| Entity linking | alias table | **ReFinED** (Wikidata), with the LLM for hard cases | 1 GB GPU plus several GB RAM | 32 GB RAM fits its entity index |
| LLM cascade | 3–4B at 4-bit | **Qwen3-8B** (Q5/Q6) or **Qwen3-14B** (Q4_K_M); Gemma 3 12B is an alternative | 6 to 10 GB | Through llama.cpp. A 24B+ model at Q4 is about 14 GB and leaves nothing else room. |
| ASR (later) | whisper small | faster-whisper large-v3-turbo, plus pyannote 3.x | 3 to 5 GB | Runs only in batch, never alongside the LLM |

## Proposed residency plan

There are two processes. A Phase 2 GPU worker serves ingest and a query service serves queries. The user is single, so they rarely compete.

| Process | Resident models | Approx. VRAM |
| --- | --- | --- |
| Query service | embedder (query side), NLI large, reranker, LLM 8B Q5 | 1.2 + 0.9 + 1.2 + about 7 = **about 10 GB** |
| GPU worker (ingest) | embedder, GLiNER, coref; ReFinED (GPU part) | 1.2 + 1 + 0.5 + 1 = **about 4 GB** at batch size |
| Display | Xorg/Wayland | 0.3 GB, which is 0 if the monitor moves to the motherboard's Intel graphics output |

That totals about 14 to 15 GB. Stance quality comes first (see decisions.md), so the default plan is a **14B Q4 LLM (about 9 to 10 GB with KV cache)**. With it, the query service needs about 13 GB, so the ingest GPU worker must **yield the GPU while queries are active**. It pauses between batches when the query service holds a GPU lease and resumes afterwards. Ingest falls behind only during query bursts, and Gate 2's index-lag target is measured with this in place. Drop to 8B only if the labeled set shows no gain.

**Easy win:** plug the monitor into the motherboard (Arrow Lake iGPU) instead of the 5060 Ti. That frees the display's VRAM and stops desktop use from stalling CUDA work.

## The real constraint: vector storage

At 50k articles/day with about 25 paragraphs each, paragraph vectors alone take:

| Dimensions × type | Per day | Per year |
| --- | --- | --- |
| 1024 × float32 | 5.1 GB | 1.9 TB |
| 1024 × int8 | 1.3 GB | 470 GB |
| 512 × int8 (Matryoshka) | 0.64 GB | 235 GB |
| 1024 × binary + int8 rescoring | 0.16 GB + rescoring copy | — |

Sentence vectors would double or triple these numbers. Vectors are kept for the **full archive** (see decisions.md), so the stored format has to fit years of data on the 2 TB disk. At 512 × int8, 10 years is about 2.4 TB at the 50k/day ceiling, and proportionally less at the real rate. That points to a second disk eventually, or to binary vectors with int8 rescoring. Proposal:

- Store paragraph vectors only. Score sentences at query time (step 4 of the query path), because only the top few hundred paragraphs need them.
- Quantize, and pick the stored dimension in the Parquet-versus-LanceDB prototype.
- Store snippet vectors only if stance-by-retrieval needs them.

## Shortlist to evaluate first

1. Embedder: bge-m3 vs Qwen3-Embedding-0.6B (at 1024 and 512 dimensions), with Qwen3-Embedding-4B as a ceiling.
2. Stance: deberta-v3-large zero-shot NLI, with and without an 8B LLM cascade on the top 30.
3. NER: GLiNER large vs spaCy trf, on a hand-checked sample of 200 sentences.

## Decided

- **Stance quality over latency:** a 14B cascade at about 3 to 6 s per query is acceptable.
- **Vectors for the full archive:** no rolling window. The storage prototype must pick a dimension and quantization that fit, and `report disk` must forecast vector growth alongside blobs and Parquet.
