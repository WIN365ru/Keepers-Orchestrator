# qBit Adder Python

A powerful GUI tool to manage and automate torrent operations with qBittorrent and Rutracker integration.

## Key Features

### 1. **Add Torrents**
- **Bulk Add**: Select multiple `.torrent` files or drag-and-drop.
- **Custom Folder Structure**: Choose whether to create category subfolders and/or ID subfolders when saving.
- **Override Save Path**: Optionally specify a custom save path instead of the client's default base path.
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

### 6. **Folder Scanner**
- **Scan Local Folders**: Point the app to a local directory to identify torrent folders by their numeric topic IDs.
- **qBittorrent Sync**: Compares local folders against your qBittorrent client to see which ones are already added and which are missing.
- **Alive Check**: Automatically checks Rutracker API to ensure the topics are still alive/approved before adding them.
- **Deep Scan (Verify Files)**: Reads local files and compares them against the `.torrent` metadata (downloaded and cached automatically) to flag **Missing** or **Mismatched** files (e.g., incorrect sizes).
- **Deep Scan+ (Verify Hashes)**: Performs full cryptographic SHA-1 verification of file chunks against the pieces array in the `.torrent` metadata to ensure absolute data integrity. (Note: CPU/Disk intensive). Downloads `.torrent` files temporarily to `.torrent_deep_scan` folder.
- **Add Missing**: Easily add all missing, alive, and fully verified local torrents back into your client with the correct category and ID paths.

### 7. **Keepers (Хранители)**
- **Automated Forum Scraping**: Scrapes Rutracker forums (e.g., "Хранители") to identify topics needing seeds.
- **Filtering**: Filters topics based on the number of current seeders (e.g., find topics with < 3 seeds).
- **Batch Download & Add**: Automatically downloads the `.torrent` files for these topics and adds them to your qBittorrent client to help keep them alive.

### 8. **Search Rutracker**
- **In-App Search**: Search Rutracker by name or Topic ID/Hash directly within the app.
- **One-Click Download**: Download `.torrent` files to a temporary folder or add them directly to qBittorrent.

### 9. **Integration & Settings**
- **Multiple Clients**: Manage multiple qBittorrent instances.
- **Auth Management**: Centralized login for Rutracker (with cookie/key extraction).
- **Rutracker API**: All category lookups use `api.rutracker.cc/v1/` — no HTML scraping, instant responses (unless scraping for Keepers).
- **Category Cache**: Full forum tree cached locally with 3-month TTL, single API call refresh (<1 second).
- **SQLite Database Caches**: Uses robust SQLite databases (`q_adder_data.db` and `q_adder_hashes.db`) for caching torrent metadata, file lists, and piece hashes for maximum performance during Deep Scans.
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

### 7. Search & Keepers
1.  **Search Torrents**: Enter a query or Topic ID to search Rutracker. Download to `temp_torrents` or add directly.
2.  **Keepers**: Enter a Forum ID and max seeds to scan for dying torrents. Select them to batch-add to your client.

### 8. Folder Scanner & Deep Scan
1.  Go to the **Folder Scanner** tab.
2.  Enter the path to your local torrents folder. Make sure folders are named by their Rutracker Topic ID (e.g., `1234567`).
3.  Choose your target qBittorrent client.
4.  Optionally enable **Deep Scan (Verify Files)** to check that all files inside match the `.torrent` exact byte sizes.
5.  Optionally enable **Deep Scan+ (Verify Hashes)** to cryptographically hash your local files against the `.torrent` piece signatures to guarantee data integrity. (Downloaded `.torrent` files for this process are saved in `.torrent_deep_scan`).
6.  Click **Scan**. 
7.  Review the results tree. Rows will indicate if the topic is missing from your client, if files are missing, or if there are hash mismatches.
8.  Select rows and click **Add Selected to qBit** or use **Add All Missing** to bulk re-add verified torrents.

### 9. Settings & Updates
- **Check for Updates**: In the Settings tab, click "Check for updates" to see if a new version is available on GitHub.
- **Refresh Categories**: One-click refresh loads the full Rutracker forum tree via API.
- **Custom Paths**: When adding torrents, you can toggle "Override Save Path", "Create Category Subfolder", and "Create ID Subfolder" depending on how you want your disk structured.

