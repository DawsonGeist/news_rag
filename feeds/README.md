# Feeds

| File | In git | What it is |
| --- | --- | --- |
| `source/feeds_world.yaml` | no (11 MB) | The original world feed list. Only its `United States` section is used. |
| `supplement_us.yaml` | yes | Hand-curated US outlets missing from the source list. Unverified until vetted. |
| `candidates_us.yaml` | yes | **Generated** by `newsrag feeds candidates`: one `FeedCandidate` (contract v1) per unique feed. |
| `vetted_us.yaml` | yes (later) | **Generated** by `newsrag feeds export`: accepted feeds after vetting. |

To add a feed, put it in `supplement_us.yaml` (same shape as the source list) and run `newsrag feeds candidates`. Never edit the generated files by hand.
