# Keepers Orchestrator — Missing Features Analysis

> Based on the existing codebase (v0.24.2, ~14 400 lines) vs. the vision of the
> **"ultimate blend of all storage utilities and multitool for torrents"** guided
> by the Keepers philosophy of long-term preservation and community seeding.

---

## Tier 1 — High Impact, Core Gaps

### 1. Dashboard / Overview Tab ✅ *Implemented*
- Currently you jump straight into tabs with no "home base"
- Missing: total storage used, active seeds count, top categories by size/count, network stats (upload ratio), Keepers health score
- Would be the first thing opened to assess the health of the seeding operation

### 2. Scheduled Automation / Task Scheduler
- You have manual operations everywhere, but no "run Keepers scan every Sunday at 3am" or "run bitrot check on the 1st of each month"
- Missing: cron-like scheduler per tab function, with configurable triggers

### 3. RSS / Auto-Add from Rutracker Categories
- You can manually add and Keepers scrapes low-seeded content, but there is no subscription to categories for new releases
- Missing: poll Rutracker category pages for new torrents matching rules (category + min/max size + seeder count), auto-add them

### 4. System Tray + Windows Notifications ✅ *Implemented*
- App ran but had no background presence
- Missing: minimize to tray, toast notifications for PM received, Keepers added, bitrot found, SMART warning

### 5. Ratio & Seeding Rules Engine
- Nothing governs *when to stop* seeding or what to prioritize
- Missing: per-category or per-torrent rules — stop seeding after ratio X, or after Y days, or when disk space < Z GB
- For Keepers philosophy: "keep seeding forever unless disk pressure" rules

### 6. Stalled / Stuck Torrent Detection
- No monitoring for torrents stuck in error, missing files, or broken tracker announces
- Missing: scan all clients for error-state torrents, show reason, offer fix (recheck, re-announce, re-add)

### 7. Topic Watchlist
- You keep rare torrents alive but have no way to *watch* specific topics for updates or status changes
- Missing: bookmark Rutracker topic IDs, poll for seeder count changes, notify when a watched topic drops to 0 seeders

### 8. SMART Disk Monitoring *(already in TODO)*
- Already planned, implementation file exists at `TODOs/smart_TODO.md`
- Critical for a storage utility — warn before a disk fails and you lose seeded data

---

## Tier 2 — Significant Additions

### 9. Duplicate File / Content Detection
- As a storage utility, finding duplicates across torrent data is essential
- Missing: scan all save paths, detect files with identical size + hash across different torrents, report wasted space
- Bonus: hard-link duplicates to save space (Windows `mklink /h`)

### 10. Storage Analytics Tab
- You move and balance drives but have no visual breakdown
- Missing: pie/bar chart of space per disk, space per category, growth over time (using mover history), top 10 biggest torrents

### 11. Cross-Seeding Support
- Keepers keep Rutracker content alive, but the same files may exist on other trackers
- Missing: given a torrent already downloaded, search alternate trackers (by infohash or file list) and inject their `.torrent` files to cross-seed without re-downloading

### 12. Torrent File Backup Manager
- If `.torrent` files are lost, you lose the ability to re-add the content
- Missing: auto-backup all `.torrent` files from qBittorrent to a designated backup folder, with periodic sync

### 13. Magnet Link Support
- Add tab only supports `.torrent` files
- Missing: accept magnet links (paste or from clipboard), resolve to `.torrent` via Rutracker or DHT, then add normally

### 14. Bulk Tracker Edit / Announce
- Missing: force re-announce all torrents, or replace a tracker URL across multiple torrents (useful when a tracker changes domain)

### 15. Export / Reporting
- No way to export lists, stats, or reports out of the app
- Missing: export torrent list to CSV/JSON, export Keepers stats, generate a "seeding report" for sharing

---

## Tier 3 — Polish & Power-User

### 16. PAR2 / Recovery Data Integration
- For true archival/preservation philosophy: create PAR2 sets for critical content so bitrot can be *repaired*, not just detected
- Missing: integrate the `par2` tool — create recovery sets, verify and repair corrupt files

### 17. Config Backup & Restore
- Settings, client configs, preferred categories — all stored in JSON but no in-app backup/restore UI
- Missing: one-click export of all config + DB to a `.zip`, with import/restore from that archive

### 18. Rutracker Search Integration ✅ *Implemented (Search tab)*
- Now available: search Rutracker by keyword, topic ID, or infohash directly from the app

### 19. Keeper Score / Leaderboard Tab
- The Keepers philosophy is community-driven; show where you rank among all Keepers
- Missing: fetch public Keepers leaderboard, show your stats vs community, which categories you dominate

### 20. File Priority / Selective Download Management
- Missing: for multi-file torrents, set file priorities (download only specific files) from within the app without opening qBittorrent's UI

### 21. Piece Error Monitor
- Bitrot tab exists but no real-time piece-fail monitoring
- Missing: poll qBittorrent for `num_incomplete_pieces` increases on seeding torrents (indicates in-flight corruption)

### 22. Speed / Bandwidth Scheduler
- Missing: set upload/download speed limits by time window (e.g., full speed 02:00–08:00, 50% otherwise) via the qBittorrent API

---

## Summary Table

| # | Feature | Category | Priority | Status |
|---|---------|----------|----------|--------|
| 1 | Dashboard / Overview | UX | 🔴 High | ✅ Done |
| 2 | Task Scheduler | Automation | 🔴 High | ⬜ Pending |
| 3 | RSS / Category Auto-Add | Automation | 🔴 High | ⬜ Pending |
| 4 | System Tray + Notifications | UX | 🔴 High | ✅ Done |
| 5 | Seeding Rules Engine | Core Logic | 🔴 High | ⬜ Pending |
| 6 | Stalled Torrent Detection | Core Logic | 🔴 High | ⬜ Pending |
| 7 | Topic Watchlist | Core Logic | 🔴 High | ⬜ Pending |
| 8 | SMART Disk Monitoring | Storage | 🔴 High | 📝 TODO |
| 9 | Duplicate File Detection | Storage | 🟡 Medium | ⬜ Pending |
| 10 | Storage Analytics Tab | Storage | 🟡 Medium | ⬜ Pending |
| 11 | Cross-Seeding Support | Torrent | 🟡 Medium | ⬜ Pending |
| 12 | .torrent Backup Manager | Preservation | 🟡 Medium | ⬜ Pending |
| 13 | Magnet Link Support | Core Logic | 🟡 Medium | ⬜ Pending |
| 14 | Bulk Tracker Edit / Announce | Core Logic | 🟡 Medium | ⬜ Pending |
| 15 | Export / Reporting | UX | 🟡 Medium | ⬜ Pending |
| 16 | PAR2 Integration | Preservation | 🟢 Low | ⬜ Pending |
| 17 | Config Backup & Restore | UX | 🟢 Low | ⬜ Pending |
| 18 | Rutracker Search | Rutracker | 🟢 Low | ✅ Done |
| 19 | Keeper Leaderboard | Community | 🟢 Low | ⬜ Pending |
| 20 | File Priority Manager | Torrent | 🟢 Low | ⬜ Pending |
| 21 | Piece Error Monitor | Integrity | 🟢 Low | ⬜ Pending |
| 22 | Bandwidth Scheduler | Torrent | 🟢 Low | ⬜ Pending |

---

## Philosophical Summary

The biggest gaps relative to the **Keepers philosophy**:

1. **No automation scheduling** — all operations are manual; there is no way to run Keepers scans, bitrot checks, or updates on a timer
2. **No watchlist for at-risk topics** — you cannot monitor when a seeded topic drops to 0 other seeders (meaning *you* become the last seed)
3. **No seeding rules to handle disk pressure gracefully** — when drives fill up, there is no automated policy for what to stop or pause
4. **No PAR2 recovery integration** — bitrot is *detected* but cannot be *repaired* without external tooling

Fixing those four would complete the preservation loop: **track → archive → verify → recover → automate**.
