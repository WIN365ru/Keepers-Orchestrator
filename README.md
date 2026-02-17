# qBit Adder Python

A powerful GUI tool to manage and automate torrent operations with qBittorrent and Rutracker integration.

## Key Features

### 1. **Add Torrents**
- **Bulk Add**: Select multiple `.torrent` files or drag-and-drop.
- **Category Management**: Automatically fetches and caches categories from Rutracker via API.
- **Auto-Path**: Suggests save paths based on resolved categories.
- **Full Breadcrumb**: Displays the complete category tree (e.g., "Спорт > Футбол > Еврокубки 2011-2024").
- **ZIP Support**: Add torrents from ZIP archives with automatic category detection, size calculation, and progress tracking.

### 2. **Update Torrents**
- **Scan & Update**: Automatically compares your current qBittorrent list with Rutracker to find updated torrents (e.g., v2 repacks, added episodes).
- **Tracker Status Check**: Identifies torrents that are "not registered" on the tracker.
- **Actions**:
    - **Re-add (Keep Data)**: Updates the torrent file while preserving downloaded data (forces recheck).
    - **Re-add (Redownload)**: Deletes old data and starts fresh.
    - **Hash Handling**: Detects hash changes and handles migration.

### 3. **Remove Torrents**
- **Bulk Removal**: Select and remove multiple torrents from qBittorrent.
- **Advanced Filters**: Filter list by name.
- **Sortable Columns**: Sort by Name, Size, Category, State, or Save Path.
- **Match from Files**: Select local `.torrent` files to automatically find and select their corresponding entries in the list (calculates Info Hash).
- **Delete Data Option**: Choose whether to delete content files or just remove the torrent entry.

### 4. **Repair Categories**
- **4-Phase Scan**: Fetches all torrents, batch-resolves topic IDs via Rutracker API, resolves correct categories through forum tree lookup, and compares with current qBit categories.
- **Auto-Fix**: Sets the correct category and moves files to the proper folder path in one click.
- **Bulk Actions**: Repair Selected or Repair All mismatched torrents.
- **Smart Path Handling**: Swaps only the category folder segment in the save path, preserving the rest of the directory structure.

### 5. **Move Torrents**
- **Move by Category**: Select a category, set a new root path, and move all its torrents. Supports limiting the number of moves.
- **Disk Auto-Balancer**:
    - Auto-detect disks from existing torrent paths with free space display.
    - Add/remove custom disk paths.
    - Three balance strategies:
        - **Balance by Size** — equalizes total GB across disks.
        - **Balance by Seeded** — equalizes total uploaded bytes so one disk isn't hammered by heavy seeders.
        - **Both (recommended)** — weighted 50/50 combination.
    - **Preview** the plan before executing (shows Name, Size, Uploaded, From, To).
    - **Execute** with progress tracking and per-torrent logging.
    - Greedy algorithm assigns heaviest torrents first, respects free disk space constraints.

### 6. **Search Rutracker**
- **In-App Search**: Search Rutracker by name or Topic ID/Hash directly within the app.
- **One-Click Download**: Download `.torrent` files to a temporary folder or add them directly to qBittorrent.

### 7. **Integration & Settings**
- **Multiple Clients**: Manage multiple qBittorrent instances.
- **Auth Management**: Centralized login for Rutracker (with cookie/key extraction).
- **Rutracker API**: All category lookups use `api.rutracker.cc/v1/` — no HTML scraping, instant responses.
- **Category Cache**: Full forum tree cached locally with 3-month TTL, single API call refresh (<1 second).
- **Auto-Updates**:
    - **App Updates**: Checks GitHub for new releases on startup.
    - **List Updates**: Auto-refresh torrent lists.

## Requirements
- Python 3.8+
- qBittorrent (with Web UI enabled)
- Rutracker account (for searching/downloading)

## Screenshots
![Main UI](screenshots/main_ui.png)
*The Main Interface showing the Add Tab.*

## User Manual

### 1. Initial Setup
1.  **Launch** the application (`qbit_gui.pyw`).
2.  Go to the **Settings** tab.
3.  **Clients Config**:
    *   Enter your qBittorrent Web UI URL (e.g., `http://localhost:8080`).
    *   Enter username/password if authentication is enabled.
    *   Set the "Base Save Path" (default download location).
4.  **Rutracker Auth**:
    *   Enter your Rutracker username and password.
    *   Click "Save Rutracker Settings".
5.  **Restart** the app to ensure all settings are loaded correctly.

### 2. Adding Torrents
1.  Go to the **Add Torrents from file** tab.
2.  Click **"Select File"** to choose a `.torrent` file, multiple files, or a ZIP archive.
3.  The app resolves the category automatically via Rutracker API and displays the full breadcrumb path.
4.  Click **"Add to qBittorrent"** — the torrent is added with the correct category and save path.
5.  For ZIP files: all torrents inside are processed in batch with progress percentage and success/fail counts.

### 3. Updating Torrents
1.  Go to the **Update Torrents** tab (auto-scans when you switch to it).
2.  Select your client from the dropdown.
3.  The app checks all torrents for "not registered" tracker status, resolves topic IDs, and checks current status via Rutracker API.
4.  **Review the list** and choose an action:
    *   **Re-add (Keep Data)**: Preserves files, forces recheck.
    *   **Re-add (Redownload)**: Deletes old data and downloads fresh.
    *   **Skip / Delete**: Remove the entry from the list.

### 4. Removing Torrents
1.  Go to the **Remove Torrents** tab.
2.  Click **"Refresh List"** to load current torrents.
3.  **Filter**: Type in the filter box to find torrents by name.
4.  **Sorting**: Click column headers (Name, Size, Path) to sort.
5.  **Select from Files**: Click "Select from .torrent files..." to match local files by hash.
6.  **Delete**: Select items and click "Remove Selected Torrents".
    *   Check **"Also delete content files"** to permanently delete the data from disk.

### 5. Repairing Categories
1.  Go to the **Repair Categories** tab.
2.  Select your client and click **"Scan Now"**.
3.  The app compares current qBit categories against the correct Rutracker categories resolved via API.
4.  Mismatches appear in the treeview with Current Cat / Correct Cat / Current Path / New Path.
5.  Click **"Repair Selected"** or **"Repair All"** to fix categories and move files to the correct folders.

### 6. Moving Torrents & Disk Balancing
1.  Go to the **Move Torrents** tab.
2.  Click **"Load Torrents"** to fetch the full list from qBittorrent.

**Move by Category:**
3.  Select a category from the dropdown (shows torrent count and total size).
4.  Enter a new root path and optionally set a limit.
5.  Click **"Move Category"** — qBittorrent physically moves the files.

**Auto-Balance Across Disks:**
3.  Click **"Detect Disks"** to auto-discover drives and their free space, or manually add paths.
4.  Choose a balance strategy (Size / Seeded / Both).
5.  Click **"Preview Balance"** to see the planned moves without executing.
6.  Review the preview treeview, then click **"Execute Balance"** to perform the moves.

### 7. Searching & Downloading
1.  Go to the **Search Torrents** tab.
2.  **Search by Name**: Enter a query and press Enter or click "Search".
3.  **Search by Hash/ID**: Enter a Topic ID or Hash to find a specific release.
4.  **Download Actions**:
    *   **Download**: Saves the `.torrent` file to the `temp_torrents` folder.
    *   **Download & Add**: Downloads and immediately opens the Add tab with the file pre-selected.

### 8. Settings & Updates
- **Check for Updates**: In the Settings tab, click "Check for updates" to see if a new version is available on GitHub.
- **Refresh Categories**: One-click refresh loads the full Rutracker forum tree via API in under 1 second.
- **Category TTL**: Default 3 months (2160 hours). Configurable in Settings.

