# qBit Adder Python

A powerful GUI tool to manage and automate torrent operations with qBittorrent and Rutracker integration.

## Key Features

### 1. **Update Torrents**
- **Scan & Update**: Automatically compares your current qBittorrent list with Rutracker to find updated torrents (e.g., v2 repacks, added episodes).
- **Tracker Status Check**: Identifies torrents that are "not registered" on the tracker.
- **Actions**:
    - **Re-add (Keep Data)**: Updates the torrent file while preserving downloaded data (forces recheck).
    - **Re-add (Redownload)**: Deletes old data and starts fresh.
    - **Hash Handling**: Detects hash changes and handles migration.

### 2. **Add Torrents**
- **Bulk Add**: Select multiple `.torrent` files or drag-and-drop.
- **Category Management**: automatically fetches and caches categories from Rutracker.
- **Auto-Path**: Suggests save paths based on selected categories.

### 3. **Search Rutracker**
- **In-App Search**: Search Rutracker by name or Topic ID/Hash directly within the app.
- **One-Click Download**: Download `.torrent` files to a temporary folder or add them directly to qBittorrent.

### 4. **Remove Torrents**
- **Bulk Removal**: Select and remove multiple torrents from qBittorrent.
- **Advanced Filters**: Filter list by name.
- **Sortable Columns**: Sort by Name, Size, Category, State, or Save Path.
- **Match from Files**: Select local `.torrent` files to automatically find and select their corresponding entries in the list (calculates Info Hash).
- **Delete Data Option**: Choose whether to delete content files or just remove the torrent entry.

### 5. **Repair / Path Fixer**
- **Category Sync**: Scans for torrents that are in the wrong save path for their category.
- **Auto-Move**: Moves data files to the correct category folder and updates the save path in qBittorrent.

### 6. **Integration & Settings**
- **Multiple Clients**: Manage multiple qBittorrent instances.
- **Auth Management**: Centralized login for Rutracker (with cookie/key extraction).
- **Auto-Updates**:
    - **App Updates**: Checks GitHub for new releases on startup.
    - **List Updates**: Auto-refresh torrent lists.

## Requirements
- Python 3.8+
- qBittorrent (with Web UI enabled)
- Rutracker account (for searching/downloading)

## Configuration
Settings are stored in `q_adder_config.json`. You can configure:
- qBittorrent connection (URL, User, Pass).
- Rutracker credentials.
- Category cache TTL.
