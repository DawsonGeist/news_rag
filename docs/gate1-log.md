# Gate 1 log

What happened during the Phase 1 soak, for the Gate 1 review (see `phase1-design.md`).
Times are UTC.

## Soak

- **Start:** 2026-10-18T04:10:50Z. `newsrag run all` as a systemd user service, on 4,133 accepted feeds, over wired Ethernet.
- **Planned end:** 2026-10-21T04:10:50Z (72 h).

## Events

| When (UTC) | Event | Effect |
| --- | --- | --- |
| 04:10–05:00 | First poll of every feed | A backlog of about 110k already-published items. Discovery lag is meaningless for these, and ingest lag is inflated while single-host queues drain (tools.cdc.gov alone held 641 URLs at a 5 s crawl delay). |
| about 13:13 | **Power-pull test (G1.4):** the author pulled the power cord mid-run | Booted at 13:15:00. The service started by itself at 13:15:32 without a login. Writer recovery reclaimed 10 in-flight extractions. SQLite `quick_check` ok, `storage verify --deep` 0 problems, nothing lost or duplicated (110,588 stored before the cut, 111,079 after). No manual intervention. |
| 13:15–13:45 | ProtonVPN (free tier) active after the reboot; crawler traffic went through it | Network errors 3.4% (the connection was unstable while the VPN was set up). The author turned the VPN off. 39 URLs that failed for good in this window were re-queued by hand; 38 failed again with 403 from the direct IP. **The 403s were not caused by the VPN:** they come from a few hosts that block regardless of IP (see below). |
| from 13:39 | bhg.com, which served 51 pages overnight, started answering 403 | Likely rate-based bot protection reacting to the crawler on the home IP |
| 13:58 | **Operator change:** contact address switched to `dawsonnewsrag@gmail.com`; graceful service restart | Planned config change, not a failure. The writer flushed before exit. |
| 14:01 | **Operator change:** 403 handling deployed (host back-off on repeated blocks; daily `feeds pause-blocked` job); graceful restart; 54 blocking feeds paused right away | The fetcher stops knocking on hosts that block it. Paused feeds include Industry Dive's network (about 15 `*dive.com` feeds, 20/20 blocked, though biopharmadive passed vetting) and the Advance Local sites. |

## Hosts that block the crawler

3,556 HTTP 403s over the first 10 h (roughly 2–3% of fetches), concentrated on a few hosts:
- **Partial blockers:** al.com, silive.com, oregonlive.com (Advance Local) mostly 403, some 200.
- **Full blockers:** forbes.com, thestreet.com, realclearinvestigations.com.
- **Started blocking mid-soak:** bhg.com.

All of them passed vetting, because sampling 3 to 5 articles didn't trigger their bot protection. Until the fix, the fetcher retried each 403 once and kept trying the host's other URLs at the crawl delay. Fixed mid-soak (see Events): repeated 403s now back off the whole host (1 h, doubling, up to 24 h), and feeds whose article fetches are at least 80% blocked over 7 days get paused daily.

## Notes for the review

- Report G1.3 lag both over everything and over items **published during the soak**, so the first-poll backlog and the VPN window can be told apart from steady-state performance.
