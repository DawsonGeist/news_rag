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
| 13:15–13:45 | ProtonVPN (free tier) active after the reboot; crawler traffic went through it | HTTP 403 went from 2.0% to 11.5% of fetches, and network errors from 0% to 3.4%. The author turned the VPN off. The 39 URLs that failed for good in this window were re-queued by hand (an operator re-fetch, allowed by the revision policy). |

## Notes for the review

- Report G1.3 lag both over everything and over items **published during the soak**, so the first-poll backlog and the VPN window can be told apart from steady-state performance.
