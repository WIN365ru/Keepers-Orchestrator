import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
import requests
import os
import json
import threading
import re
import zipfile
import time
import io
import datetime
import html
import webbrowser
import hashlib
import shutil
import configparser
import tempfile
import concurrent.futures
import subprocess
import sys
import base64
import copy
import queue
import gc
from PIL import Image as PILImage, ImageTk, ImageDraw
from requests.adapters import HTTPAdapter

original_scrolled_text = scrolledtext.ScrolledText

class CopyableScrolledText(original_scrolled_text):
    def __init__(self, *args, **kwargs):
        kwargs['state'] = 'normal'
        super().__init__(*args, **kwargs)
        self.bind("<Key>", lambda e: "break" if e.char and e.char not in ('\x03', '\x01', '\x0f', '\x16') else None)
        self.bind("<<Paste>>", lambda e: "break")
        self.bind("<<Cut>>", lambda e: "break")
        self.bind("<BackSpace>", lambda e: "break")
        self.bind("<Delete>", lambda e: "break")
        self.bind("<Return>", lambda e: "break")

    def config(self, *args, **kwargs):
        if 'state' in kwargs:
            del kwargs['state']
        if kwargs:
            super().config(*args, **kwargs)

    def configure(self, *args, **kwargs):
        self.config(*args, **kwargs)

scrolledtext.ScrolledText = CopyableScrolledText
import sqlite3
import csv

# Default Configuration (New Structure)
DEFAULT_CONFIG = {
    "global_auth": {
        "enabled": False,
        "username": "admin",
        "password": "adminadmin"
    },
    "rutracker_auth": {
        "username": "",
        "password": ""
    },
    "proxy": {
        "enabled": False,
        "url": "socks5://127.0.0.1:10808",
        "username": "",
        "password": ""
    },
    "category_ttl_hours": 2160,
    "clients": [
        {
            "name": "Localhost",
            "url": "http://localhost:8080",
            "use_global_auth": True,
            "username": "",
            "password": "",
            "base_save_path": "C:/Torrents/Sport/",
            "enabled": False
        }
    ],
    "last_selected_client_index": 0,
    "torrent_cache_ttl_hours": 6,
    "pm_polling_enabled": False,
    "pm_poll_interval_sec": 300,
    "pm_toast_enabled": False,
    "tray_enabled": True,
    "minimize_to_tray": True,
    "tray_notifications_enabled": True,
    "github_app_auto_update_enabled": False,
    "keepers_preferred_categories": [],
    "theme": "Default",
    "language": "en",
    "keeper_nickname": "",
    "log_retention_days": 14,
    "auto_keeper": {
        "categories": [],
        "target_clients": [],
        "qbit_category": "AutoKeeper",
        "start_paused": True,
        "disk_reserve_gb": 50,
        "max_total_size_gb": 500,
        "skip_already_kept": True,
        "skip_zero_size": True
    }
}

_APP_DIR = os.path.dirname(os.path.abspath(__file__))
_CONFIG_DIR = os.path.join(_APP_DIR, "config")
_DATA_DIR = os.path.join(_APP_DIR, "data")
_LOGS_DIR = os.path.join(_APP_DIR, "logs")

# Ensure subdirectories exist
os.makedirs(_CONFIG_DIR, exist_ok=True)
os.makedirs(_DATA_DIR, exist_ok=True)
os.makedirs(_LOGS_DIR, exist_ok=True)

CONFIG_FILE = os.path.join(_CONFIG_DIR, "keepers_orchestrator_config.json")
CATEGORY_CACHE_FILE = os.path.join(_DATA_DIR, "keepers_orchestrator_categories.json")
DATA_DB_FILE = os.path.join(_DATA_DIR, "keepers_orchestrator_data.db")
HASHES_DB_FILE = os.path.join(_DATA_DIR, "keepers_orchestrator_hashes.db")

# App Version & Update Info
APP_VERSION = "0.27.0"
GITHUB_REPO = "WIN365ru/Keepers-Orchestrator"

# --- Theme Definitions ---
THEMES = {
    "Default": {
        "bg": "#f0f0f0",
        "fg": "#000000",
        "entry_bg": "#ffffff",
        "entry_fg": "#000000",
        "select_bg": "#0078d7",
        "select_fg": "#ffffff",
        "tree_bg": "#ffffff",
        "tree_fg": "#000000",
        "tree_field_bg": "#ffffff",
        "log_bg": "#ffffff",
        "log_fg": "#000000",
        "btn_bg": "#e1e1e1",
        "btn_fg": "#000000",
        "lf_fg": "#000000",
        "trough": "#e0e0e0",
        "menu_bg": "#f0f0f0",
        "menu_fg": "#000000",
        "tab_bg": "#e8e8e8",
        "tab_fg": "#000000",
        "tab_sel_bg": "#f0f0f0",
        "cb_select": "#ffffff",
        "insert": "#000000",
        # Restore Night Mode remap outputs back to originals
        "fg_remap": {
            "#9898b0": "gray",
            "#a8a8c0": "#333333",
            "#a0a0b8": "#444444",
            "#9090a8": "#666666",
            "#8888a0": "#888888",
            "#8585a0": "#999999",
            "#6cacff": "blue",
            "#60d060": "green",
            "#50c878": "#006633",
        },
    },
    "Steel Blue": {
        "bg": "#cfd8e3",
        "fg": "#1a2633",
        "entry_bg": "#e4eaf2",
        "entry_fg": "#1a2633",
        "select_bg": "#5a7fa3",
        "select_fg": "#ffffff",
        "tree_bg": "#dce3ed",
        "tree_fg": "#1a2633",
        "tree_field_bg": "#dce3ed",
        "log_bg": "#d4dce8",
        "log_fg": "#1a2633",
        "btn_bg": "#adbfd2",
        "btn_fg": "#1a2633",
        "lf_fg": "#2c3e50",
        "trough": "#9fb3c8",
        "menu_bg": "#bfcbda",
        "menu_fg": "#1a2633",
        "tab_bg": "#b8c7d6",
        "tab_fg": "#1a2633",
        "tab_sel_bg": "#cfd8e3",
        "cb_select": "#dce3ed",
        "insert": "#1a2633",
        # Restore Night Mode remap outputs back to originals
        "fg_remap": {
            "#9898b0": "gray",
            "#a8a8c0": "#333333",
            "#a0a0b8": "#444444",
            "#9090a8": "#666666",
            "#8888a0": "#888888",
            "#8585a0": "#999999",
            "#6cacff": "blue",
            "#60d060": "green",
            "#50c878": "#006633",
        },
    },
    "Night Mode": {
        "bg": "#2b2b3d",
        "fg": "#d4d4e8",
        "entry_bg": "#363649",
        "entry_fg": "#d4d4e8",
        "select_bg": "#525270",
        "select_fg": "#e8e8f0",
        "tree_bg": "#32324a",
        "tree_fg": "#d4d4e8",
        "tree_field_bg": "#32324a",
        "log_bg": "#252538",
        "log_fg": "#b8b8d0",
        "btn_bg": "#404058",
        "btn_fg": "#d4d4e8",
        "lf_fg": "#b0b0d0",
        "trough": "#404058",
        "menu_bg": "#363649",
        "menu_fg": "#d4d4e8",
        "tab_bg": "#363649",
        "tab_fg": "#b0b0d0",
        "tab_sel_bg": "#45456a",
        "cb_select": "#363649",
        "insert": "#d4d4e8",
        # Remap dark fg colors that become invisible on dark bg
        "fg_remap": {
            "gray":    "#9898b0",
            "#808080": "#9898b0",
            "#333333": "#a8a8c0",
            "#444444": "#a0a0b8",
            "#555555": "#9898b0",
            "#666666": "#9090a8",
            "#888888": "#8888a0",
            "#999999": "#8585a0",
            "blue":    "#6cacff",
            "#0000ff": "#6cacff",
            "green":   "#60d060",
            "#008000": "#60d060",
            "#006633": "#50c878",
            "#00b300": "#60d060",
        },
    },
}
# fg values considered "default" and safe to override with theme fg.
# Includes each theme's main fg so switching between themes re-colors labels properly.
_DEFAULT_FG = {
    "black", "systemwindowtext", "systembuttontext", "#000000", "#000",
    "#d4d4e8", "#1a2633",   # Night Mode fg, Steel Blue fg
}

# --- Internationalization ---
_current_lang = "en"
_i18n_registry = []  # [(key, widget, prop), ...]

TRANSLATIONS = {
    "en": {
        # Window
        "app.title": "Keepers Orchestrator",
        # Menu
        "menu.help": "Help",
        "menu.help.docs": "Documentation & Colors",
        # Status bar
        "status.ready": "Ready",
        # Tab names
        "tab.dashboard": "Dashboard",
        "tab.add_torrents": "Add Torrents",
        # Tray settings
        "settings.tray": "Tray & Notifications",
        "settings.tray_enable": "Enable system tray icon",
        "settings.tray_minimize": "Minimize to tray on close",
        "settings.tray_notifications": "Event notifications",
        "settings.save_tray": "Save",
        "tab.keepers": "Keepers",
        "tab.update_torrents": "Update Torrents",
        "tab.remove_torrents": "Remove Torrents",
        "tab.repair_categories": "Repair Categories",
        "tab.move_torrents": "Move Torrents",
        "tab.folder_scanner": "Folder Scanner",
        "tab.bitrot_scanner": "Bitrot Scanner",
        "tab.settings": "Settings",
        "tab.search_torrents": "Search Torrents",
        # Common
        "common.client": "Client:",
        "common.stop": "Stop",
        "common.browse": "Browse...",
        "common.filter": "Filter:",
        "common.name": "Name",
        "common.size": "Size",
        "common.category": "Category",
        "common.path": "Path",
        "common.status": "Status",
        "common.log": "Log",
        "common.scan_now": "Scan Now",
        "common.scan": "Scan",
        "common.refresh": "Refresh",
        "common.add": "Add",
        "common.remove": "Remove",
        "common.copy": "Copy",
        "common.select_all": "Select All",
        "common.download_torrent": "Download .torrent",
        "common.list_updated_never": "List updated: never",
        "common.list_updated": "List updated: {time}",
        "common.scan_controls": "Scan Controls",
        "common.select_client": "Select Client",
        "common.refresh_list": "Refresh List",
        # Adder tab
        "adder.target": "Target",
        "adder.select_client": "Select Client:",
        "adder.selection": "Selection",
        "adder.no_file_selected": "No file/folder selected",
        "adder.select_torrent_zip": "Select Torrent/ZIP File",
        "adder.select_folder": "Select Folder",
        "adder.additional_info": "Additional Info",
        "adder.custom_options": "Custom Options",
        "adder.folder_structure": "Folder Structure",
        "adder.create_cat_subfolder": "Create Category Subfolder",
        "adder.create_id_subfolder": "Create ID Subfolder",
        "adder.override_cat_path": "Override Category & Path",
        "adder.category": "Category:",
        "adder.save_path": "Save Path:",
        "adder.tags": "Tags:",
        "adder.tags_hint": "(comma separated)",
        "adder.add_to_qbit": "Add to qBittorrent",
        "adder.pause": "Pause",
        "adder.all_clients": "All Clients",
        # Remover tab
        "remover.delete_files": "Also delete content files (DATA)",
        "remover.select_from_torrents": "Select from .torrent files...",
        "remover.options_actions": "Options & Actions",
        "remover.saved_path": "Saved Path",
        "remover.state": "State",
        "remover.hash": "Hash",
        "remover.remove_selected": "Remove Selected Torrents",
        # Settings tab
        "settings.migration": "Migration / Import Data",
        "settings.import_webtlo": "Import from webtlo config.ini",
        "settings.export_setup": "Export Full Setup (.zip)",
        "settings.import_setup": "Import Full Setup (.zip)",
        "settings.proxy": "Proxy Settings (HTTP/HTTPS/SOCKS5)",
        "settings.proxy_enable": "Enable Proxy (useful for bypassing regional blocks like Rutracker)",
        "settings.proxy_url": "Proxy URL:",
        "settings.username": "Username:",
        "settings.password": "Password:",
        "settings.save_proxy": "Save Proxy Settings",
        "settings.global_auth": "Global Authentication",
        "settings.global_auth_enable": "Use Global Authentication for All Clients",
        "settings.global_user": "Global User:",
        "settings.global_pass": "Global Pass:",
        "settings.save_global": "Save Global Settings",
        "settings.clients": "qBittorrent Clients",
        "settings.name": "Name:",
        "settings.url": "URL:",
        "settings.base_path": "Base Path:",
        "settings.use_global_auth": "Use Global Auth",
        "settings.client_enabled": "Enabled",
        "settings.save_client": "Save Client Details",
        "settings.rt_login": "Rutracker Forum Login (for downloading .torrents and failover category fetching)",
        "settings.update_keys": "Update Keys",
        "settings.cat_cache_ttl": "Category cache TTL (hours):",
        "settings.cat_cache_hint": "(how long to reuse loaded torrent lists before fetching fresh data)",
        "settings.save_cache": "Save Cache Settings",
        "settings.clear_cache": "Clear All Cached Lists",
        "settings.pm": "Private Messages",
        "settings.pm_enable": "Enable PM inbox polling",
        "settings.pm_interval": "Interval (sec):",
        "settings.pm_toast": "Windows notifications",
        "settings.save_pm": "Save PM Settings",
        "settings.data_sources": "Data Sources",
        "settings.refresh_cats": "Refresh Rutracker Categories",
        "settings.appearance": "Appearance",
        "settings.theme": "Theme:",
        "settings.theme_hint": "(applied instantly)",
        "settings.language": "Language:",
        "settings.language_hint": "(applied instantly)",
        "settings.app_updates": "App Updates (GitHub)",
        "settings.auto_update_enable": "Enable app auto-update from GitHub releases",
        "settings.save_update": "Save App Update Settings",
        "settings.check_updates": "Check for updates",
        "settings.logging": "Logging",
        "settings.log_retention": "Keep logs for (days):",
        "settings.log_save": "Save",
        "settings.log_purge": "Purge All Logs",
        "settings.log_purged": "All log files deleted.",
        "settings.statistics": "Statistics",
        "settings.stats_kept": "Torrents Kept: {count}",
        "settings.stats_size": "Total Size Saved: {size}",
        "settings.stats_active": "Active Seeding Size: {size}",
        "settings.stats_net": "Global UL: {ul} | DL: {dl}",
        "settings.stats_total": "Total Torrents: {count} ({size})",
        "settings.stats_bitrot": "Torrents Checked for Bitrot: {count}",
        "settings.stats_mover": "Torrents Auto-Balanced: {count}",
        # Updater tab
        "updater.only_unreg": "Only Unregistered",
        "updater.unreg_torrents": "Unregistered Torrents",
        "updater.torrent_name": "Torrent Name",
        "updater.reason": "Reason",
        "updater.topic_id": "Topic ID",
        "updater.new_topic": "New Topic",
        "updater.switch_hint": "Switch to this tab to scan.",
        "updater.keep_files": "Update (Keep Files)",
        "updater.consumed": "Update Consumed",
        "updater.redownload": "Update (Re-download)",
        "updater.remove_qbit": "Remove from qBit",
        "updater.delete_files": "Delete with Files",
        "updater.update_log": "Update Log",
        # Search tab
        "search.search": "Search",
        "search.query": "Query:",
        "search.type": "Type:",
        "search.type_name": "Name (Scrape)",
        "search.type_topic": "Topic ID (API)",
        "search.type_hash": "Hash (API)",
        "search.results": "Results",
        "search.id": "ID",
        "search.seeds": "S",
        "search.leech": "L",
        "search.download": "Download",
        "search.download_add": "Download & Add",
        # Bitrot tab
        "bitrot.older_than": "Older than (Days):",
        "bitrot.load_torrents": "Load 100% Torrents",
        "bitrot.start_check": "Start Bitrot Check (Selected)",
        "bitrot.stop_check": "Stop Check",
        "bitrot.topic_id": "Topic ID",
        "bitrot.added": "Added",
        "bitrot.last_active": "Last Active",
        "bitrot.up_speed": "UP Speed",
        "bitrot.seeds": "Seeds",
        "bitrot.last_checked": "Last Checked",
        "bitrot.progress": "Progress",
        "bitrot.bitrot_state": "Bitrot State",
        "bitrot.ready": "Ready",
        "bitrot.total_stats": "Total: {count} torrents ({size})",
        # Repair tab
        "repair.mismatches": "Category Mismatches",
        "repair.current_cat": "Current Cat",
        "repair.correct_cat": "Correct Cat",
        "repair.current_path": "Current Path",
        "repair.new_path": "New Path",
        "repair.scan_hint": "Click Scan to check categories.",
        "repair.move_files": "Also correct save path (move files)",
        "repair.selected": "Repair Selected",
        "repair.all": "Repair All",
        "repair.log": "Repair Log",
        # Mover tab
        "mover.client": "Client",
        "mover.load_torrents": "Load Torrents",
        "mover.by_category": "Move by Category",
        "mover.new_root": "New root path:",
        "mover.max_torrents": "Max torrents to move (0 = all):",
        "mover.folder_structure": "Folder structure:",
        "mover.cat_folder": "Category folder:",
        "mover.id_folder": "ID folder:",
        "mover.create_cat_sub": "Create /Category/ subfolder in target path",
        "mover.keep_id": "Keep existing /ID/ folder",
        "mover.create_id": "Create /ID/ folder (if missing)",
        "mover.remove_id": "Remove /ID/ folder (flatten)",
        "mover.cat_summary_hint": "Select a category to see details.",
        "mover.move_category": "Move Category",
        "mover.resume": "Resume",
        "mover.auto_balance": "Auto-Balance Across Disks",
        "mover.disk_path": "Disk Path",
        "mover.free_space": "Free Space",
        "mover.current_load": "Current Load",
        "mover.target_load": "Target Load",
        "mover.detect_disks": "Detect Disks",
        "mover.add_path": "Add path:",
        "mover.strategy": "Strategy:",
        "mover.bal_size": "Balance by Size",
        "mover.bal_seeded": "Balance by Seeded",
        "mover.bal_both": "Both (recommended)",
        "mover.preserve_cat": "Preserve /Category/ subfolder in target path",
        "mover.preview": "Preview Balance",
        "mover.execute": "Execute Balance",
        "mover.preview_frame": "Balance Preview",
        "mover.col_uploaded": "Uploaded",
        "mover.col_from": "From",
        "mover.col_to": "To",
        "mover.move_log": "Move Log",
        # Keepers tab
        "keepers.save_category": "Save Category:",
        "keepers.forum_cat": "Forum Category",
        "keepers.custom": "Custom:",
        "keepers.skip_zero_topics": "Skip 0 B topics",
        "keepers.skip_zero_cats": "Skip 0 B categories",
        "keepers.max_seeds": "Max Seeds:",
        "keepers.max_keepers": "Max Keepers:",
        "keepers.preferred_cats": "Preferred Categories",
        "keepers.add_pref": "+ Add",
        "keepers.remove_pref": "- Remove",
        "keepers.scan_all": "Scan All",
        "keepers.stats_title": "Category Stats:",
        "keepers.stats_topics": "Topics: --",
        "keepers.stats_size": "Total Size: --",
        "keepers.stats_seeds": "Seeds: --",
        "keepers.stats_avg": "Avg Seeds: --",
        "keepers.stats_leechers": "Leechers: --",
        "keepers.col_id": "ID",
        "keepers.col_seeds": "Seeds",
        "keepers.col_leech": "Leech",
        "keepers.col_link": "Link",
        "keepers.col_k_count": "# Keepers",
        "keepers.col_priority": "Priority",
        "keepers.col_last_seen": "Last Seen",
        "keepers.col_poster": "Poster",
        "keepers.col_tor_status": "Topic Status",
        "keepers.col_reg_time": "Registered",
        "keepers.max_leech": "Max Leech:",
        "keepers.min_reg_days": "Min age (days):",
        "keepers.hide_my_kept": "Hide kept by me",
        "keepers.start_paused": "Start Paused",
        "keepers.add_selected": "Add Selected",
        "keepers.export_csv": "Export to CSV",
        # Keepers sub-tabs
        "keepers.sub_scanner": "Scanner",
        "keepers.sub_auto_keeper": "Auto Keeper",
        # Auto Keeper
        "autokeeper.categories": "Category Configuration",
        "autokeeper.add_cat": "+ Add",
        "autokeeper.remove_cat": "Remove",
        "autokeeper.edit_thresholds": "Edit Thresholds",
        "autokeeper.target_clients": "Target Clients",
        "autokeeper.refresh_disk": "Refresh Disk Space",
        "autokeeper.reserve_gb": "Reserve per client:",
        "autokeeper.max_total_size": "Max total plan size:",
        "autokeeper.scan_plan": "Scan && Plan",
        "autokeeper.distribution_preview": "Distribution Preview",
        "autokeeper.approve_add": "Approve && Add",
        "autokeeper.clear_plan": "Clear Plan",
        "autokeeper.col_client": "Client",
        "autokeeper.col_save_path": "Save Path",
        "common.gb": "GB",
        "common.enabled": "Enabled",
        # Scanner tab
        "scanner.folder": "Folder:",
        "scanner.recursive": "Scan subfolders recursively",
        "scanner.use_parent": "Use parent folder as save_path (content lives in /ID/ folder)",
        "scanner.skip_subid": "Skip subfolders inside /ID/",
        "scanner.start_paused": "Start paused",
        "scanner.deep_scan": "Deep Scan (Verify Files)",
        "scanner.deep_scan_plus": "Deep Scan+ (Verify Hashes)",
        "scanner.custom_add": "Custom Add Options",
        "scanner.override_path": "Override Save Path",
        "scanner.results": "Scan Results",
        "scanner.disk_size": "Disk Size",
        "scanner.seeds": "Seeds",
        "scanner.leech": "Leech",
        "scanner.rt_status": "RT Status",
        "scanner.in_qbit": "In qBit",
        "scanner.extra": "Extra",
        "scanner.missing": "Missing",
        "scanner.mismatch": "Mismatch",
        "scanner.pieces": "Pieces (Bad/Total)",
        "scanner.disk_path": "Disk Path",
        "scanner.scan_hint": "Enter a folder path and click Scan.",
        "scanner.show_zero": "Show only 0 B on disk",
        "scanner.add_selected": "Add Selected to qBit",
        "scanner.add_all": "Add All Missing",
        "scanner.del_qbit": "Delete from qBit",
        "scanner.del_data": "Delete Data",
        "scanner.del_os": "Delete OS Data",
        # Help dialog
        "help.title": "Help & Documentation",
        "help.content": """# === KEEPERS ORCHESTRATOR - HELP & DOCUMENTATION ===

## APP TABS & FUNCTIONS
--------------------------------------------------
[Add Torrents]
- Takes a Rutracker Topic ID or a Folder Name, searches Rutracker,
  downloads the active .torrent file, and injects it into qBittorrent
  pointing to your specified directory.
- Deep Checkbox searches deeper matches natively.

[Keepers]
- Fetches all user profiles cached in your 'keepers_orchestrator_data.db' Keepers
  list to instantly scan for their latest torrents.
- Useful for automating downloads from favorite uploaders.
- Filter by category, size, seeds. Preferred categories can be
  configured to highlight relevant content.

[Update Torrents]
- Compares torrents currently in qBittorrent to the live Rutracker API
  to find newly updated matching topics. Automatically downloads the
  updated file and points it to the exact same disk location.
- "Only errored" filter focuses on torrents that failed verification.

[Remove Torrents]
- Cleans up inactive torrents by comparing qBit against Rutracker API.
  Flags Dead / Unknown topics for easy deletion.
- Optional "Also delete content files (DATA)" checkbox.

[Repair Categories]
- Rescans all Torrents in qBittorrent against the Rutracker Database.
  Corrects empty or inaccurate Categories.
- "Also correct save path (move files)" checkbox controls whether
  files are physically relocated to match the new category folder
  structure, or only the qBittorrent category label is updated.
- The repair works even when the configured Base Path does not match
  the actual torrent paths (fallback folder-segment replacement).

[Move Torrents]
- Mass-moves actively seeding Torrents between physical drives without
  manually entering qBittorrent's GUI.
- Category Mover: migrate all torrents of a category to a new root
  using dynamic folder naming /{Category}/{Topic_ID}/.
- Balance Mover: automatically distribute torrents across multiple
  disks to balance free space evenly.

[Folder Scanner]
- Scans a physical Windows folder on your drive and maps every
  sub-directory against the Rutracker API.
- Use this to detect unseeded collections, identify missing downloads,
  and re-inject disconnected folders back into qBittorrent.
- Deep Scan: verifies file names and counts against .torrent metadata.
- Deep Scan+: full SHA-1 piece hash verification (slow but thorough).
- "Show only 0 B on disk" filter: instantly isolate empty folders
  that exist on disk but contain no actual data.
- Size columns (Size, Disk Size) are sortable by actual byte value.

[Bitrot Scanner]
- Scans ALL payload files across every active Torrent in qBittorrent
  and subjects them to cryptographic SHA-1 piece verification.
  Perfect for discovering silent data corruption or failing drives.

[Search Torrents]
- Search Rutracker directly from the app by name (scrapes the forum),
  by Topic ID, or by info-hash (via API).
- Double-click a result to open its Rutracker page, or inject it
  into qBittorrent with one click.

[Settings]
- Proxy: HTTP/HTTPS/SOCKS5 proxy for bypassing regional blocks.
- Global Authentication: shared login for all qBittorrent clients.
- Clients: manage multiple qBittorrent instances with individual
  URLs, credentials, and base save paths. Status traffic lights
  show connection health at a glance.
- Rutracker Login: forum credentials, category cache TTL,
  extracted API keys (ID / BT / API).
- Private Messages: enable inbox polling, set interval, toggle
  Windows toast notifications.
- Appearance: switch between Default, Steel Blue, and Night Mode
  themes. The change is applied instantly and saved to config.
- App Updates: auto-update from GitHub releases.
- Statistics: torrents kept, total size saved, global speeds.


## USEFUL COMMANDS & HOTKEYS
--------------------------------------------------
<Control-1> through <Control-0>
  Instantly switch between the 10 application tabs.
  (1=Adder, 2=Keepers, ... 9=Settings, 0=Search)

<F5>
  Universal Action Key - starts the primary operation on the
  current tab:
    Adder     -> Process Torrent
    Keepers   -> Start Scan
    Updater   -> Start Scan
    Remover   -> Refresh Client List
    Repair    -> Start Scan
    Mover     -> Refresh Client List
    Scanner   -> Start Scan
    Bitrot    -> Start Scan
    Search    -> Search Rutracker

<Control-C>
  Universal Copy - works on EVERY Treeview across the app.
  Select 1 or 1,000 rows, press Ctrl+C, and paste into Excel
  or Notepad with tab-separated columns.

Log Highlighting
  All log consoles are unlocked for text selection. Drag, highlight,
  and Ctrl+C / Ctrl+V error text freely.

Double-Click Rows
  Opens the Rutracker forum topic for that torrent in your browser.

Right-Click Rows
  Context menu on path columns lets you copy the folder path
  to clipboard instantly.

PM Indicator (bottom-right)
  Click the PM badge to open the Private Messages inbox.
  Unread messages trigger a color change on the badge.


## TREEVIEW COLOR LEGEND (Folder Scanner)
--------------------------------------------------
TEXT COLORS:
  Dark Green  - Actively mapped / seeding in qBittorrent.
  Dark Red    - "Missing" - file has missing pieces vs. API.
  Gray        - "Dead" - topic no longer exists on Rutracker.
  Default     - Healthy torrent, not connected to your client.

SIZE COMPARISON BACKGROUNDS:
  Light Red (Pink)   - 0 B on disk. Folder exists but is empty.
  Light Orange       - Smaller: < 95% of the expected API size.
  Light Blue         - Larger: > 105% of expected size (extra files).


## TREEVIEW COLOR LEGEND (Repair Categories)
--------------------------------------------------
  Dark Red    - Category mismatch detected (needs repair).
  Dark Green  - Successfully repaired.
  Red         - Repair failed (error).


## TREEVIEW COLOR LEGEND (Bitrot Scanner)
--------------------------------------------------
  Light Green  - Clean: all pieces passed SHA-1 verification.
  Light Red    - Rot detected: one or more pieces are corrupt.
  Light Yellow - Currently being checked.
""",
        # Auth Gate
        "auth.title": "Keeper Authentication",
        "auth.prompt": "Enter your Keeper nickname:",
        "auth.password_prompt": "Enter your Rutracker password:",
        "auth.checking": "Verifying...",
        "auth.captcha_prompt": "Enter the code from the image:",
        "auth.unlock": "Access Granted!",
        "auth.lock": "Access Denied — Not a Keeper",
        "auth.fetch_error": "Could not fetch keeper list. Check your connection.",
        "auth.locked_title": "Session Locked",
        "auth.locked_msg": "Your nickname was removed from the Keepers list.\nThe application will now close.",
        "auth.login_btn": "Login",
        "auth.import_config": "Import Config",
    },
    "ru": {
        # Window
        "app.title": "Хранительский Оркестратор",
        # Menu
        "menu.help": "Справка",
        "menu.help.docs": "Документация и цвета",
        # Status bar
        "status.ready": "Готов",
        # Tab names
        "tab.dashboard": "Обзор",
        "tab.add_torrents": "Добавление",
        # Tray settings
        "settings.tray": "Трей и уведомления",
        "settings.tray_enable": "Иконка в трее",
        "settings.tray_minimize": "Сворачивать в трей",
        "settings.tray_notifications": "Уведомления о событиях",
        "settings.save_tray": "Сохранить",
        "tab.keepers": "Хранители",
        "tab.update_torrents": "Обновление",
        "tab.remove_torrents": "Удаление",
        "tab.repair_categories": "Категории",
        "tab.move_torrents": "Перемещение",
        "tab.folder_scanner": "Сканер папок",
        "tab.bitrot_scanner": "Bitrot сканер",
        "tab.settings": "Настройки",
        "tab.search_torrents": "Поиск",
        # Common
        "common.client": "Клиент:",
        "common.stop": "Стоп",
        "common.browse": "Обзор...",
        "common.filter": "Фильтр:",
        "common.name": "Название",
        "common.size": "Размер",
        "common.category": "Категория",
        "common.path": "Путь",
        "common.status": "Статус",
        "common.log": "Журнал",
        "common.scan_now": "Сканировать",
        "common.scan": "Сканировать",
        "common.refresh": "Обновить",
        "common.add": "Добавить",
        "common.remove": "Удалить",
        "common.copy": "Копировать",
        "common.select_all": "Выделить все",
        "common.download_torrent": "Скачать .torrent",
        "common.list_updated_never": "Список обновлён: никогда",
        "common.list_updated": "Список обновлён: {time}",
        "common.scan_controls": "Управление сканированием",
        "common.select_client": "Выбор клиента",
        "common.refresh_list": "Обновить список",
        # Adder tab
        "adder.target": "Цель",
        "adder.select_client": "Выбрать клиент:",
        "adder.selection": "Выбор файла",
        "adder.no_file_selected": "Файл или папка не выбраны",
        "adder.select_torrent_zip": "Выбрать Torrent/ZIP",
        "adder.select_folder": "Выбрать папку",
        "adder.additional_info": "Доп. информация",
        "adder.custom_options": "Пользовательские опции",
        "adder.folder_structure": "Структура папок",
        "adder.create_cat_subfolder": "Создать подпапку категории",
        "adder.create_id_subfolder": "Создать подпапку ID",
        "adder.override_cat_path": "Переопределить категорию и путь",
        "adder.category": "Категория:",
        "adder.save_path": "Путь сохранения:",
        "adder.tags": "Теги:",
        "adder.tags_hint": "(через запятую)",
        "adder.add_to_qbit": "Добавить в qBittorrent",
        "adder.pause": "Пауза",
        "adder.all_clients": "Все клиенты",
        # Remover tab
        "remover.delete_files": "Также удалить файлы содержимого (ДАННЫЕ)",
        "remover.select_from_torrents": "Выбрать из .torrent файлов...",
        "remover.options_actions": "Опции и действия",
        "remover.saved_path": "Путь сохранения",
        "remover.state": "Состояние",
        "remover.hash": "Хеш",
        "remover.remove_selected": "Удалить выбранные торренты",
        # Settings tab
        "settings.migration": "Миграция / Импорт данных",
        "settings.import_webtlo": "Импорт из webtlo config.ini",
        "settings.export_setup": "Экспорт полной настройки (.zip)",
        "settings.import_setup": "Импорт полной настройки (.zip)",
        "settings.proxy": "Настройки прокси (HTTP/HTTPS/SOCKS5)",
        "settings.proxy_enable": "Включить прокси (для обхода блокировок, например Rutracker)",
        "settings.proxy_url": "URL прокси:",
        "settings.username": "Логин:",
        "settings.password": "Пароль:",
        "settings.save_proxy": "Сохранить настройки прокси",
        "settings.global_auth": "Глобальная аутентификация",
        "settings.global_auth_enable": "Использовать глобальную аутентификацию для всех клиентов",
        "settings.global_user": "Глоб. логин:",
        "settings.global_pass": "Глоб. пароль:",
        "settings.save_global": "Сохранить глобальные настройки",
        "settings.clients": "Клиенты qBittorrent",
        "settings.name": "Имя:",
        "settings.url": "URL:",
        "settings.base_path": "Базовый путь:",
        "settings.use_global_auth": "Глобальная авторизация",
        "settings.client_enabled": "Включён",
        "settings.save_client": "Сохранить клиент",
        "settings.rt_login": "Вход на Rutracker (для скачивания .torrent и резервного получения категорий)",
        "settings.update_keys": "Обновить ключи",
        "settings.cat_cache_ttl": "TTL кеша категорий (часы):",
        "settings.cat_cache_hint": "(как долго использовать загруженные списки перед обновлением)",
        "settings.save_cache": "Сохранить настройки кеша",
        "settings.clear_cache": "Очистить весь кеш",
        "settings.pm": "Личные сообщения",
        "settings.pm_enable": "Включить опрос входящих ЛС",
        "settings.pm_interval": "Интервал (сек):",
        "settings.pm_toast": "Уведомления Windows",
        "settings.save_pm": "Сохранить настройки ЛС",
        "settings.data_sources": "Источники данных",
        "settings.refresh_cats": "Обновить категории Rutracker",
        "settings.appearance": "Оформление",
        "settings.theme": "Тема:",
        "settings.theme_hint": "(применяется мгновенно)",
        "settings.language": "Язык:",
        "settings.language_hint": "(применяется мгновенно)",
        "settings.app_updates": "Обновление приложения (GitHub)",
        "settings.auto_update_enable": "Включить автообновление из GitHub releases",
        "settings.save_update": "Сохранить настройки обновлений",
        "settings.check_updates": "Проверить обновления",
        "settings.logging": "Журналирование",
        "settings.log_retention": "Хранить логи (дней):",
        "settings.log_save": "Сохранить",
        "settings.log_purge": "Очистить все логи",
        "settings.log_purged": "Все файлы логов удалены.",
        "settings.statistics": "Статистика",
        "settings.stats_kept": "Торрентов сохранено: {count}",
        "settings.stats_size": "Общий размер: {size}",
        "settings.stats_active": "Активная раздача: {size}",
        "settings.stats_net": "Отдача: {ul} | Загрузка: {dl}",
        "settings.stats_total": "Всего торрентов: {count} ({size})",
        "settings.stats_bitrot": "Проверено на Bitrot: {count}",
        "settings.stats_mover": "Авто-балансировка: {count}",
        # Updater tab
        "updater.only_unreg": "Только незарегистрированные",
        "updater.unreg_torrents": "Незарегистрированные торренты",
        "updater.torrent_name": "Название торрента",
        "updater.reason": "Причина",
        "updater.topic_id": "ID темы",
        "updater.new_topic": "Новая тема",
        "updater.switch_hint": "Переключитесь на эту вкладку для сканирования.",
        "updater.keep_files": "Обновить (сохранить файлы)",
        "updater.consumed": "Обновить поглощённые",
        "updater.redownload": "Обновить (перекачать)",
        "updater.remove_qbit": "Удалить из qBit",
        "updater.delete_files": "Удалить с файлами",
        "updater.update_log": "Журнал обновлений",
        # Search tab
        "search.search": "Поиск",
        "search.query": "Запрос:",
        "search.type": "Тип:",
        "search.type_name": "Имя (парсинг)",
        "search.type_topic": "ID темы (API)",
        "search.type_hash": "Хеш (API)",
        "search.results": "Результаты",
        "search.id": "ID",
        "search.seeds": "С",
        "search.leech": "Л",
        "search.download": "Скачать",
        "search.download_add": "Скачать и добавить",
        # Bitrot tab
        "bitrot.older_than": "Старше (дней):",
        "bitrot.load_torrents": "Загрузить 100% торренты",
        "bitrot.start_check": "Проверка Bitrot (выбранные)",
        "bitrot.stop_check": "Остановить проверку",
        "bitrot.topic_id": "ID темы",
        "bitrot.added": "Добавлен",
        "bitrot.last_active": "Посл. активность",
        "bitrot.up_speed": "Скорость отдачи",
        "bitrot.seeds": "Сиды",
        "bitrot.last_checked": "Посл. проверка",
        "bitrot.progress": "Прогресс",
        "bitrot.bitrot_state": "Состояние Bitrot",
        "bitrot.ready": "Готов",
        "bitrot.total_stats": "Всего: {count} торрентов ({size})",
        # Repair tab
        "repair.mismatches": "Несоответствия категорий",
        "repair.current_cat": "Текущая кат.",
        "repair.correct_cat": "Верная кат.",
        "repair.current_path": "Текущий путь",
        "repair.new_path": "Новый путь",
        "repair.scan_hint": "Нажмите Сканировать для проверки.",
        "repair.move_files": "Также исправить путь (переместить файлы)",
        "repair.selected": "Исправить выбранные",
        "repair.all": "Исправить все",
        "repair.log": "Журнал исправлений",
        # Mover tab
        "mover.client": "Клиент",
        "mover.load_torrents": "Загрузить торренты",
        "mover.by_category": "Перемещение по категории",
        "mover.new_root": "Новый корневой путь:",
        "mover.max_torrents": "Макс. торрентов (0 = все):",
        "mover.folder_structure": "Структура папок:",
        "mover.cat_folder": "Папка категории:",
        "mover.id_folder": "Папка ID:",
        "mover.create_cat_sub": "Создать подпапку /Категория/ в целевом пути",
        "mover.keep_id": "Сохранить существующую папку /ID/",
        "mover.create_id": "Создать папку /ID/ (если нет)",
        "mover.remove_id": "Убрать папку /ID/ (объединить)",
        "mover.cat_summary_hint": "Выберите категорию для просмотра деталей.",
        "mover.move_category": "Переместить категорию",
        "mover.resume": "Продолжить",
        "mover.auto_balance": "Авто-балансировка между дисками",
        "mover.disk_path": "Путь к диску",
        "mover.free_space": "Свободно",
        "mover.current_load": "Текущая нагрузка",
        "mover.target_load": "Целевая нагрузка",
        "mover.detect_disks": "Обнаружить диски",
        "mover.add_path": "Добавить путь:",
        "mover.strategy": "Стратегия:",
        "mover.bal_size": "По размеру",
        "mover.bal_seeded": "По раздаче",
        "mover.bal_both": "Оба (рекомендуется)",
        "mover.preserve_cat": "Сохранить подпапку /Категория/ в целевом пути",
        "mover.preview": "Предпросмотр",
        "mover.execute": "Выполнить балансировку",
        "mover.preview_frame": "Предпросмотр балансировки",
        "mover.col_uploaded": "Отдано",
        "mover.col_from": "Откуда",
        "mover.col_to": "Куда",
        "mover.move_log": "Журнал перемещений",
        # Keepers tab
        "keepers.save_category": "Категория сохр.:",
        "keepers.forum_cat": "Категория форума",
        "keepers.custom": "Своя:",
        "keepers.skip_zero_topics": "Пропускать 0 Б темы",
        "keepers.skip_zero_cats": "Пропускать 0 Б категории",
        "keepers.max_seeds": "Макс. сидов:",
        "keepers.max_keepers": "Макс. хранителей:",
        "keepers.preferred_cats": "Избранные категории",
        "keepers.add_pref": "+ Доб.",
        "keepers.remove_pref": "- Убрать",
        "keepers.scan_all": "Сканировать все",
        "keepers.stats_title": "Стат. категории:",
        "keepers.stats_topics": "Тем: --",
        "keepers.stats_size": "Общий размер: --",
        "keepers.stats_seeds": "Сиды: --",
        "keepers.stats_avg": "Ср. сиды: --",
        "keepers.stats_leechers": "Личеры: --",
        "keepers.col_id": "ID",
        "keepers.col_seeds": "Сиды",
        "keepers.col_leech": "Личи",
        "keepers.col_link": "Ссылка",
        "keepers.col_k_count": "Хранителей",
        "keepers.col_priority": "Приоритет",
        "keepers.col_last_seen": "Посл. визит",
        "keepers.col_poster": "Автор",
        "keepers.col_tor_status": "Статус темы",
        "keepers.col_reg_time": "Зарегистрирован",
        "keepers.max_leech": "Макс. личей:",
        "keepers.min_reg_days": "Мин. возраст (дней):",
        "keepers.hide_my_kept": "Скрыть мои хранимые",
        "keepers.start_paused": "Добавить на паузе",
        "keepers.add_selected": "Добавить выбранные",
        "keepers.export_csv": "Экспорт в CSV",
        # Keepers sub-tabs
        "keepers.sub_scanner": "Сканер",
        "keepers.sub_auto_keeper": "Авто Хранитель",
        # Auto Keeper
        "autokeeper.categories": "Конфигурация категорий",
        "autokeeper.add_cat": "+ Доб.",
        "autokeeper.remove_cat": "Убрать",
        "autokeeper.edit_thresholds": "Редактировать пороги",
        "autokeeper.target_clients": "Целевые клиенты",
        "autokeeper.refresh_disk": "Обновить место на диске",
        "autokeeper.reserve_gb": "Резерв на клиент:",
        "autokeeper.max_total_size": "Макс. размер плана:",
        "autokeeper.scan_plan": "Сканировать и планировать",
        "autokeeper.distribution_preview": "Предпросмотр распределения",
        "autokeeper.approve_add": "Подтвердить и добавить",
        "autokeeper.clear_plan": "Очистить план",
        "autokeeper.col_client": "Клиент",
        "autokeeper.col_save_path": "Путь сохранения",
        "common.gb": "ГБ",
        "common.enabled": "Вкл",
        # Scanner tab
        "scanner.folder": "Папка:",
        "scanner.recursive": "Сканировать подпапки рекурсивно",
        "scanner.use_parent": "Родительская папка как save_path (содержимое в /ID/)",
        "scanner.skip_subid": "Пропускать подпапки внутри /ID/",
        "scanner.start_paused": "Добавить на паузе",
        "scanner.deep_scan": "Глубокое сканирование (проверка файлов)",
        "scanner.deep_scan_plus": "Глубокое сканирование+ (проверка хешей)",
        "scanner.custom_add": "Опции добавления",
        "scanner.override_path": "Переопределить путь",
        "scanner.results": "Результаты сканирования",
        "scanner.disk_size": "Размер на диске",
        "scanner.seeds": "Сиды",
        "scanner.leech": "Личи",
        "scanner.rt_status": "Статус RT",
        "scanner.in_qbit": "В qBit",
        "scanner.extra": "Лишние",
        "scanner.missing": "Нет",
        "scanner.mismatch": "Несовп.",
        "scanner.pieces": "Части (плох./всего)",
        "scanner.disk_path": "Путь на диске",
        "scanner.scan_hint": "Укажите папку и нажмите Сканировать.",
        "scanner.show_zero": "Показать только 0 Б на диске",
        "scanner.add_selected": "Добавить выбранные в qBit",
        "scanner.add_all": "Добавить все отсутствующие",
        "scanner.del_qbit": "Удалить из qBit",
        "scanner.del_data": "Удалить данные",
        "scanner.del_os": "Удалить с диска",
        # Help dialog
        "help.title": "Справка и документация",
        "help.content": """# === ХРАНИТЕЛЬСКИЙ ОРКЕСТРАТОР — СПРАВКА И ДОКУМЕНТАЦИЯ ===

## ВКЛАДКИ И ФУНКЦИИ ПРИЛОЖЕНИЯ
--------------------------------------------------
[Добавление]
- Принимает ID темы Rutracker или имя папки, ищет на Rutracker,
  скачивает актуальный .torrent-файл и добавляет его в qBittorrent
  с указанием нужной директории.
- Галочка «Глубокий поиск» ищет совпадения глубже.

[Хранители]
- Загружает все профили пользователей из вашего списка Хранителей
  в базе данных 'keepers_orchestrator_data.db' для мгновенного сканирования
  их последних раздач.
- Удобно для автоматизации загрузок от избранных раздающих.
- Фильтр по категории, размеру, сидам. Предпочтительные категории
  можно настроить для подсветки нужного контента.

[Обновление]
- Сравнивает торренты в qBittorrent с актуальными данными API Rutracker,
  чтобы найти обновлённые темы. Автоматически скачивает обновлённый
  файл и указывает его на прежнее расположение на диске.
- Фильтр «Только с ошибками» показывает торренты, не прошедшие проверку.

[Удаление]
- Очищает неактивные торренты путём сравнения qBit с API Rutracker.
  Помечает мёртвые / неизвестные темы для удобного удаления.
- Опционально «Также удалить файлы содержимого (DATA)».

[Исправление категорий]
- Пересканирует все торренты в qBittorrent по базе данных Rutracker.
  Исправляет пустые или неточные категории.
- Галочка «Также исправить путь сохранения (переместить файлы)»
  определяет, будут ли файлы физически перемещены в соответствии
  с новой структурой папок категории, или только обновлена метка
  категории в qBittorrent.
- Исправление работает даже когда настроенный базовый путь
  не совпадает с фактическими путями торрентов (замена сегмента папки).

[Перемещение]
- Массовое перемещение активно раздаваемых торрентов между физическими
  дисками без ручного использования интерфейса qBittorrent.
- Перемещение по категории: переносит все торренты категории в новый
  корень с динамическим именованием папок /{Категория}/{ID_темы}/.
- Балансировка: автоматически распределяет торренты между несколькими
  дисками для равномерного распределения свободного пространства.

[Сканер папок]
- Сканирует физическую папку Windows на вашем диске и сопоставляет
  каждую поддиректорию с API Rutracker.
- Используйте для обнаружения нераздаваемых коллекций, выявления
  недостающих загрузок и повторного подключения папок к qBittorrent.
- Глубокое сканирование: проверяет имена и количество файлов
  по метаданным .torrent.
- Глубокое сканирование+: полная проверка хешей SHA-1 (медленно,
  но тщательно).
- Фильтр «Показать только 0 Б на диске»: мгновенно выделяет пустые
  папки, существующие на диске, но не содержащие данных.
- Столбцы размера (Размер, Размер на диске) сортируются по
  фактическому значению в байтах.

[Сканер Bitrot]
- Сканирует ВСЕ файлы каждого активного торрента в qBittorrent
  и подвергает их криптографической проверке хешей SHA-1.
  Идеально для обнаружения скрытого повреждения данных или
  неисправных дисков.

[Поиск]
- Поиск на Rutracker прямо из приложения: по имени (парсинг форума),
  по ID темы или по инфо-хешу (через API).
- Двойной клик по результату открывает страницу на Rutracker,
  или добавляет в qBittorrent одним нажатием.

[Настройки]
- Прокси: HTTP/HTTPS/SOCKS5 прокси для обхода региональных блокировок.
- Глобальная авторизация: общий логин для всех клиентов qBittorrent.
- Клиенты: управление несколькими экземплярами qBittorrent
  с индивидуальными URL, учётными данными и базовыми путями.
  Индикаторы-светофоры показывают состояние подключения.
- Вход Rutracker: учётные данные форума, TTL кеша категорий,
  извлечённые API-ключи (ID / BT / API).
- Личные сообщения: включение опроса входящих, интервал, переключение
  всплывающих уведомлений Windows.
- Оформление: переключение между темами Default, Steel Blue
  и Night Mode. Изменение применяется мгновенно и сохраняется.
- Обновления: автоматическое обновление с GitHub-релизов.
- Статистика: сохранённые торренты, общий размер, глобальные скорости.


## ПОЛЕЗНЫЕ КОМАНДЫ И ГОРЯЧИЕ КЛАВИШИ
--------------------------------------------------
<Control-1> до <Control-0>
  Мгновенное переключение между 10 вкладками приложения.
  (1=Добавление, 2=Хранители, ... 9=Настройки, 0=Поиск)

<F5>
  Универсальная клавиша действия — запускает основную операцию
  на текущей вкладке:
    Добавление    -> Обработать торрент
    Хранители     -> Начать сканирование
    Обновление    -> Начать сканирование
    Удаление      -> Обновить список клиентов
    Исправление   -> Начать сканирование
    Перемещение   -> Обновить список клиентов
    Сканер папок  -> Начать сканирование
    Bitrot        -> Начать сканирование
    Поиск         -> Искать на Rutracker

<Control-C>
  Универсальное копирование — работает в КАЖДОЙ таблице приложения.
  Выберите от 1 до 1 000 строк, нажмите Ctrl+C, и вставьте
  в Excel или Блокнот с разделением табуляцией.

Выделение в логе
  Все консоли логов разблокированы для выделения текста. Выделяйте
  и копируйте текст ошибок через Ctrl+C / Ctrl+V.

Двойной клик по строке
  Открывает тему Rutracker для этого торрента в вашем браузере.

Правый клик по строке
  Контекстное меню на столбцах с путями позволяет мгновенно
  скопировать путь к папке в буфер обмена.

Индикатор ЛС (внизу справа)
  Нажмите на значок ЛС для открытия входящих сообщений.
  Непрочитанные сообщения меняют цвет индикатора.


## ЦВЕТОВАЯ ЛЕГЕНДА ТАБЛИЦЫ (Сканер папок)
--------------------------------------------------
ЦВЕТА ТЕКСТА:
  Тёмно-зелёный  - Активно подключён / раздаётся в qBittorrent.
  Тёмно-красный  - «Отсутствует» — файл имеет недостающие части.
  Серый           - «Мёртвый» — тема больше не существует на Rutracker.
  По умолчанию    - Здоровый торрент, не подключён к клиенту.

ФОНОВЫЕ ЦВЕТА СРАВНЕНИЯ РАЗМЕРОВ:
  Светло-красный (розовый) - 0 Б на диске. Папка существует, но пуста.
  Светло-оранжевый         - Меньше: < 95% ожидаемого размера API.
  Светло-голубой            - Больше: > 105% ожидаемого размера (лишние файлы).


## ЦВЕТОВАЯ ЛЕГЕНДА ТАБЛИЦЫ (Исправление категорий)
--------------------------------------------------
  Тёмно-красный  - Обнаружено несоответствие категории (требуется исправление).
  Тёмно-зелёный  - Успешно исправлено.
  Красный        - Ошибка исправления.


## ЦВЕТОВАЯ ЛЕГЕНДА ТАБЛИЦЫ (Сканер Bitrot)
--------------------------------------------------
  Светло-зелёный  - Чисто: все части прошли проверку SHA-1.
  Светло-красный  - Обнаружена деградация: одна или несколько частей повреждены.
  Светло-жёлтый   - Проверяется в данный момент.
""",
        # Auth Gate
        "auth.title": "Авторизация хранителя",
        "auth.prompt": "Введите ваш ник хранителя:",
        "auth.password_prompt": "Введите ваш пароль Rutracker:",
        "auth.checking": "Проверка...",
        "auth.captcha_prompt": "Введите код с картинки:",
        "auth.unlock": "Доступ разрешён!",
        "auth.lock": "Доступ запрещён — Не хранитель",
        "auth.fetch_error": "Не удалось получить список хранителей. Проверьте соединение.",
        "auth.locked_title": "Сессия заблокирована",
        "auth.locked_msg": "Ваш ник был удалён из списка хранителей.\nПриложение будет закрыто.",
        "auth.login_btn": "Войти",
        "auth.import_config": "Импорт настроек",
    },
}


def t(key, **kwargs):
    """Look up a translation key, with fallback to English. Supports {placeholder} interpolation."""
    text = TRANSLATIONS.get(_current_lang, TRANSLATIONS["en"]).get(key)
    if text is None:
        text = TRANSLATIONS["en"].get(key, key)
    if kwargs:
        try:
            text = text.format(**kwargs)
        except (KeyError, IndexError):
            pass
    return text


# --- Locale-aware date formatting ---
_DATE_FORMATS = {
    "en": {"date": "%m/%d/%Y", "datetime": "%m/%d/%Y %H:%M", "datetime_sec": "%m/%d/%Y %H:%M:%S"},
    "ru": {"date": "%d.%m.%Y", "datetime": "%d.%m.%Y %H:%M", "datetime_sec": "%d.%m.%Y %H:%M:%S"},
}

def fmt_dt(dt_obj, fmt="datetime"):
    """Format a datetime object using locale-aware pattern.
    fmt: 'date', 'datetime', or 'datetime_sec'."""
    patterns = _DATE_FORMATS.get(_current_lang, _DATE_FORMATS["en"])
    return dt_obj.strftime(patterns.get(fmt, patterns["datetime"]))


# --- File logging ---
import logging as _logging

_LOG_RETENTION_DAYS = 14  # default, overridden from config at startup
_log_lock = threading.Lock()
_file_loggers = {}

def _get_file_logger(name):
    """Get or create a file logger for the given tab name."""
    if name in _file_loggers:
        return _file_loggers[name]
    with _log_lock:
        if name in _file_loggers:
            return _file_loggers[name]
        logger = _logging.getLogger(f"ko.{name}")
        logger.setLevel(_logging.DEBUG)
        logger.propagate = False
        fh = _logging.FileHandler(
            os.path.join(_LOGS_DIR, f"{name}.log"), encoding="utf-8"
        )
        fh.setFormatter(_logging.Formatter("%(asctime)s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
        logger.addHandler(fh)
        _file_loggers[name] = logger
        return logger

def _log_to_file(tab_name, message):
    """Write a message to the tab's log file."""
    try:
        _get_file_logger(tab_name).info(message)
    except Exception:
        pass

def _write_startup_separator():
    """Write a visual separator to every existing log file on startup."""
    sep = f"\n{'=' * 60}\n  Session started  {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'=' * 60}\n"
    for name in ("main", "updater", "bitrot", "repair", "mover", "keepers", "scanner"):
        log_path = os.path.join(_LOGS_DIR, f"{name}.log")
        if os.path.exists(log_path):
            try:
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(sep)
            except Exception:
                pass

def _cleanup_old_logs(retention_days=None):
    """Delete log files (or trim lines) older than retention_days."""
    days = retention_days if retention_days is not None else _LOG_RETENTION_DAYS
    if days <= 0:
        return
    cutoff = datetime.datetime.now() - datetime.timedelta(days=days)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    for fname in os.listdir(_LOGS_DIR):
        if not fname.endswith(".log"):
            continue
        fpath = os.path.join(_LOGS_DIR, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                lines = f.readlines()
            # Keep lines that are recent or non-timestamped (separators, blanks)
            kept = []
            for ln in lines:
                # Timestamped lines start with "YYYY-MM-DD HH:MM:SS"
                if len(ln) >= 19 and ln[4] == '-' and ln[7] == '-' and ln[10] == ' ':
                    if ln[:19] >= cutoff_str:
                        kept.append(ln)
                else:
                    # Separator / blank lines — keep if adjacent to kept content
                    kept.append(ln)
            if len(kept) < len(lines):
                with open(fpath, "w", encoding="utf-8") as f:
                    f.writelines(kept)
        except Exception:
            pass


# --- Simple Bencode Decoder ---
def bdecode(data, idx=0):
    """Minimal bencode decoder. Returns (decoded_value, next_index)."""
    if data[idx:idx+1] == b'd':
        idx += 1
        d = {}
        while data[idx:idx+1] != b'e':
            key, idx = bdecode(data, idx)
            val, idx = bdecode(data, idx)
            if isinstance(key, bytes):
                key = key.decode('utf-8', errors='replace')
            d[key] = val
        return d, idx + 1
    elif data[idx:idx+1] == b'l':
        idx += 1
        lst = []
        while data[idx:idx+1] != b'e':
            val, idx = bdecode(data, idx)
            lst.append(val)
        return lst, idx + 1
    elif data[idx:idx+1] == b'i':
        end = data.index(b'e', idx)
        return int(data[idx+1:end]), end + 1
    elif data[idx:idx+1].isdigit():
        colon = data.index(b':', idx)
        length = int(data[idx:colon])
        start = colon + 1
        return data[start:start+length], start + length
    else:
        raise ValueError(f"Invalid bencode at index {idx}")

def cloak(text):
    if not text: return ""
    try:
        return base64.b64encode(text.encode('utf-8')).decode('utf-8')
    except:
        return text

def uncloak(text):
    if not isinstance(text, str) or not text:
        return text
    
    # Backward compatibility for 'obf:' prefix
    if text.startswith("obf:"):
        try:
            return base64.b64decode(text[4:]).decode('utf-8')
        except:
            pass
            
    # Attempt to decode as base64
    try:
        return base64.b64decode(text, validate=True).decode('utf-8')
    except:
        return text

def format_size(size_bytes):
    """Format bytes to human-readable size."""
    if size_bytes <= 0:
        return "0 B"
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    i = 0
    size = float(size_bytes)
    while size >= 1024 and i < len(units) - 1:
        size /= 1024
        i += 1
    return f"{size:.2f} {units[i]}" if i > 0 else f"{int(size)} B"

def parse_torrent_info(file_path_or_bytes):
    """Parse a .torrent file and return dict with name, comment, topic_id."""
    try:
        if isinstance(file_path_or_bytes, bytes):
            data = file_path_or_bytes
        else:
            with open(file_path_or_bytes, 'rb') as f:
                data = f.read()
        
        torrent, _ = bdecode(data)
        
        result = {}
        
        # Get name from info dict
        info = torrent.get('info', {})
        name = info.get('name', b'')
        if isinstance(name, bytes):
            try:
                name = name.decode('utf-8')
            except UnicodeDecodeError:
                name = name.decode('cp1251', errors='replace')
        result['name'] = name
        
        # Get comment (usually contains rutracker URL)
        comment = torrent.get('comment', b'')
        if isinstance(comment, bytes):
            try:
                comment = comment.decode('utf-8')
            except UnicodeDecodeError:
                comment = comment.decode('cp1251', errors='replace')
        result['comment'] = comment
        
        # Extract topic ID from comment
        topic_match = re.search(r'viewtopic\.php\?t=(\d+)', comment)
        if not topic_match:
            topic_match = re.search(r'rutracker\.org/forum/.*?t=(\d+)', comment)
        result['topic_id'] = topic_match.group(1) if topic_match else None
        
        # Get total download size
        if 'length' in info:
            # Single-file torrent
            result['total_size'] = info['length']
            result['file_count'] = 1
        elif 'files' in info:
            # Multi-file torrent
            result['total_size'] = sum(f.get('length', 0) for f in info['files'])
            result['file_count'] = len(info['files'])
        else:
            result['total_size'] = 0
            result['file_count'] = 0
        
        # Additional metadata
        created_by = torrent.get('created by', b'')
        if isinstance(created_by, bytes):
            created_by = created_by.decode('utf-8', errors='replace')
        result['created_by'] = created_by
        
        creation_date = torrent.get('creation date', 0)
        if creation_date:
            try:
                result['creation_date'] = fmt_dt(datetime.datetime.fromtimestamp(creation_date), "datetime_sec")
            except:
                result['creation_date'] = str(creation_date)
        else:
            result['creation_date'] = ''
        
        announce = torrent.get('announce', b'')
        if isinstance(announce, bytes):
            announce = announce.decode('utf-8', errors='replace')
        result['tracker'] = announce
        
        piece_length = info.get('piece length', 0)
        result['piece_length'] = piece_length
        result['piece_size'] = piece_length # alias for legacy
        
        pieces_bytes = info.get('pieces', b'')
        if isinstance(pieces_bytes, bytes):
            result['pieces_hex'] = pieces_bytes.hex()
        else:
            result['pieces_hex'] = ""
            
        result['private'] = bool(info.get('private', 0))
        
        source = info.get('source', b'')
        if isinstance(source, bytes):
            try:
                source = source.decode('utf-8')
            except UnicodeDecodeError:
                source = source.decode('cp1251', errors='replace')
        result['source'] = source
        
        # File list
        files_list = []
        if 'files' in info:
            for f in info['files']:
                path_parts = f.get('path', [])
                decoded_parts = []
                for p in path_parts:
                    if isinstance(p, bytes):
                        try:
                            decoded_parts.append(p.decode('utf-8'))
                        except UnicodeDecodeError:
                            decoded_parts.append(p.decode('cp1251', errors='replace'))
                    else:
                        decoded_parts.append(p)
                # Prepend the torrent directory name to relative paths matching qBittorrent disk structure
                path_str = result['name'] + '/' + '/'.join(decoded_parts) if result['name'] else '/'.join(decoded_parts)
                files_list.append({'path': path_str, 'size': f.get('length', 0)})
        elif 'length' in info:
            # Single-file torrent — add the single file to the list
            files_list.append({'path': result['name'], 'size': info['length']})
        result['files'] = files_list
        
        return result
    except Exception as e:
        return {'name': '', 'comment': '', 'topic_id': None, 'total_size': 0, 'file_count': 0, 'created_by': '', 'creation_date': '', 'tracker': '', 'piece_size': 0, 'private': False, 'source': '', 'files': [], 'error': str(e)}


class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self._init_db()

    def _get_conn(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        try:
            with self._get_conn() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS kept_torrents (
                        topic_id INTEGER PRIMARY KEY,
                        info_hash TEXT,
                        name TEXT,
                        size INTEGER,
                        seeds_snapshot INTEGER,
                        leechers_snapshot INTEGER,
                        added_date TIMESTAMP,
                        category_id INTEGER
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS scan_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TIMESTAMP,
                        category_id INTEGER,
                        torrents_scanned INTEGER,
                        torrents_added INTEGER,
                        found_low_seeds INTEGER
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS torrent_cache (
                        client_name TEXT PRIMARY KEY,
                        timestamp REAL,
                        torrents_json TEXT
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS torrent_files_cache (
                        topic_id INTEGER PRIMARY KEY,
                        timestamp REAL,
                        files_json TEXT
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS bitrot_history (
                        hash TEXT PRIMARY KEY,
                        last_checked REAL,
                        status TEXT
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS mover_history (
                        hash TEXT PRIMARY KEY,
                        timestamp REAL,
                        from_disk TEXT,
                        to_disk TEXT
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS pvc_cache (
                        forum_id INTEGER PRIMARY KEY,
                        timestamp REAL,
                        json_data TEXT
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS keepers_users (
                        user_id INTEGER PRIMARY KEY,
                        username TEXT
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS category_stats_cache (
                        forum_id INTEGER PRIMARY KEY,
                        timestamp REAL,
                        total_topics INTEGER,
                        total_size INTEGER,
                        total_seeds INTEGER,
                        total_leechers INTEGER,
                        avg_seeds REAL,
                        filtered_topics INTEGER,
                        filtered_size INTEGER,
                        filtered_seeds INTEGER
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS auto_keeper_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp REAL,
                        topic_id INTEGER,
                        client_name TEXT,
                        category_id INTEGER,
                        size_bytes INTEGER,
                        status TEXT
                    )
                """)
        except Exception as e:
            print(f"DB Init Error: {e}")

    def save_bitrot_history(self, info_hash, status):
        try:
            with self._get_conn() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO bitrot_history (hash, last_checked, status)
                    VALUES (?, ?, ?)
                """, (info_hash, time.time(), status))
                conn.commit()
        except Exception as e:
            print(f"Error saving bitrot history: {e}")

    def get_bitrot_history(self):
        try:
            with self._get_conn() as conn:
                cursor = conn.execute("SELECT hash, last_checked, status FROM bitrot_history")
                return {row[0]: {"last_checked": row[1], "status": row[2]} for row in cursor.fetchall()}
        except Exception as e:
            print(f"Error loading bitrot history: {e}")
            return {}

    def log_mover_success(self, info_hash, from_disk, to_disk):
        try:
            with self._get_conn() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO mover_history (hash, timestamp, from_disk, to_disk)
                    VALUES (?, ?, ?, ?)
                """, (info_hash, time.time(), from_disk, to_disk))
        except Exception as e:
            print(f"Mover DB Error: {e}")

    def get_mover_stats(self):
        try:
            with self._get_conn() as conn:
                row = conn.execute("SELECT COUNT(*) FROM mover_history").fetchone()
                return row[0] if row else 0
        except:
            return 0

    def save_pvc_data(self, forum_id, json_str):
        try:
            with self._get_conn() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO pvc_cache (forum_id, timestamp, json_data)
                    VALUES (?, ?, ?)
                """, (forum_id, time.time(), json_str))
        except Exception as e:
            print(f"Error saving PVC cache: {e}")

    def get_pvc_data(self, forum_id):
        try:
            with self._get_conn() as conn:
                row = conn.execute("SELECT json_data, timestamp FROM pvc_cache WHERE forum_id = ?", (forum_id,)).fetchone()
                if row:
                    return row[0], row[1]
        except Exception as e:
            print(f"Error loading PVC cache: {e}")
        return None

    def save_keepers_users(self, user_dict):
        try:
            with self._get_conn() as conn:
                conn.executemany("""
                    INSERT OR REPLACE INTO keepers_users (user_id, username)
                    VALUES (?, ?)
                """, [(int(uid), data[0]) for uid, data in user_dict.items()])
        except Exception as e:
            print(f"Error saving Keepers Users: {e}")
            
    def get_keepers_user(self, user_id):
        try:
            with self._get_conn() as conn:
                row = conn.execute("SELECT username FROM keepers_users WHERE user_id = ?", (user_id,)).fetchone()
                if row:
                    return row[0]
        except Exception as e:
            pass
        return f"Unknown ({user_id})"

    def save_category_stats(self, forum_id, stats):
        try:
            with self._get_conn() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO category_stats_cache
                    (forum_id, timestamp, total_topics, total_size, total_seeds, total_leechers, avg_seeds,
                     filtered_topics, filtered_size, filtered_seeds)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (forum_id, time.time(),
                      stats.get('total_topics', 0), stats.get('total_size', 0),
                      stats.get('total_seeds', 0), stats.get('total_leechers', 0),
                      stats.get('avg_seeds', 0.0),
                      stats.get('filtered_topics', 0), stats.get('filtered_size', 0),
                      stats.get('filtered_seeds', 0)))
        except Exception as e:
            print(f"Error saving category stats: {e}")

    def get_category_stats(self, forum_id, max_age=21600):
        try:
            with self._get_conn() as conn:
                row = conn.execute("""
                    SELECT timestamp, total_topics, total_size, total_seeds, total_leechers, avg_seeds,
                           filtered_topics, filtered_size, filtered_seeds
                    FROM category_stats_cache WHERE forum_id = ?
                """, (forum_id,)).fetchone()
                if row and (time.time() - row[0]) < max_age:
                    return {
                        'timestamp': row[0], 'total_topics': row[1], 'total_size': row[2],
                        'total_seeds': row[3], 'total_leechers': row[4], 'avg_seeds': row[5],
                        'filtered_topics': row[6], 'filtered_size': row[7], 'filtered_seeds': row[8]
                    }
        except Exception as e:
            print(f"Error loading category stats: {e}")
        return None

    def get_all_keeper_usernames(self):
        """Return a set of all known keeper usernames (lowercase for case-insensitive matching)."""
        try:
            with self._get_conn() as conn:
                rows = conn.execute("SELECT username FROM keepers_users").fetchall()
                return {row[0].lower() for row in rows if row[0]}
        except Exception as e:
            pass
        return set()

    def add_kept_torrent(self, topic_id, info_hash, name, size, seeds, leechers, cat_id):
        try:
            with self._get_conn() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO kept_torrents 
                    (topic_id, info_hash, name, size, seeds_snapshot, leechers_snapshot, added_date, category_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (topic_id, info_hash, name, size, seeds, leechers, datetime.datetime.now().isoformat(), cat_id))
        except Exception as e:
            print(f"DB Error (add_kept): {e}")

    def is_torrent_kept(self, topic_id):
        try:
            with self._get_conn() as conn:
                cu = conn.execute("SELECT 1 FROM kept_torrents WHERE topic_id=?", (topic_id,))
                return cu.fetchone() is not None
        except:
            return False

    def save_auto_keeper_batch(self, entries):
        """Save a batch of auto keeper plan entries.
        entries: list of (topic_id, client_name, category_id, size_bytes, status)
        """
        try:
            with self._get_conn() as conn:
                conn.executemany("""
                    INSERT INTO auto_keeper_history
                    (timestamp, topic_id, client_name, category_id, size_bytes, status)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, [(time.time(), *e) for e in entries])
        except Exception as e:
            print(f"DB Error (auto_keeper_batch): {e}")

    def is_auto_keeper_planned(self, topic_id):
        """Check if a topic was already processed by auto keeper."""
        try:
            with self._get_conn() as conn:
                row = conn.execute(
                    "SELECT 1 FROM auto_keeper_history WHERE topic_id=? AND status='added'",
                    (topic_id,)
                ).fetchone()
                return row is not None
        except:
            return False

    def get_kept_stats(self):
        try:
            with self._get_conn() as conn:
                # Count
                count = conn.execute("SELECT COUNT(*) FROM kept_torrents").fetchone()[0]
                # Total Size
                total_size = conn.execute("SELECT SUM(size) FROM kept_torrents").fetchone()[0] or 0
                return count, total_size
        except:
            return 0, 0
    
    def get_top_categories(self, limit=5):
        try:
            with self._get_conn() as conn:
                # Group by category_id
                rows = conn.execute(f"""
                    SELECT category_id, COUNT(*) as cnt 
                    FROM kept_torrents 
                    GROUP BY category_id 
                    ORDER BY cnt DESC 
                    LIMIT {limit}
                """).fetchall()
                return rows
        except:
            return []

    def log_scan(self, cat_id, scanned, added, low_seeds):
        try:
            with self._get_conn() as conn:
                conn.execute("""
                    INSERT INTO scan_history (timestamp, category_id, torrents_scanned, torrents_added, found_low_seeds)
                    VALUES (?, ?, ?, ?, ?)
                """, (datetime.datetime.now().isoformat(), cat_id, scanned, added, low_seeds))
        except Exception as e:
            print(f"DB Error (log_scan): {e}")

    def get_recent_activity(self, limit=10):
        try:
            with self._get_conn() as conn:
                rows = conn.execute(f"""
                    SELECT timestamp, category_id, torrents_scanned, torrents_added
                    FROM scan_history
                    ORDER BY id DESC
                    LIMIT {limit}
                """).fetchall()
                return rows
        except:
            return []

    # --- Torrent cache persistence ---

    def save_torrent_cache(self, client_name, timestamp, torrents):
        """Save torrent list for a client to DB."""
        try:
            with self._get_conn() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO torrent_cache (client_name, timestamp, torrents_json)
                    VALUES (?, ?, ?)
                """, (client_name, timestamp, json.dumps(torrents)))
        except Exception as e:
            print(f"DB Error (save_cache): {e}")

    def load_torrent_cache(self, ttl_hours=6):
        """Load all non-stale cached torrent lists from DB.
        Returns {client_name: {"torrents": [...], "timestamp": float}}"""
        result = {}
        try:
            cutoff = time.time() - (ttl_hours * 3600)
            with self._get_conn() as conn:
                rows = conn.execute(
                    "SELECT client_name, timestamp, torrents_json FROM torrent_cache WHERE timestamp > ?",
                    (cutoff,)).fetchall()
                for client_name, ts, torrents_json in rows:
                    try:
                        result[client_name] = {
                            "torrents": json.loads(torrents_json),
                            "timestamp": ts
                        }
                    except (json.JSONDecodeError, TypeError):
                        continue
        except Exception as e:
            print(f"DB Error (load_cache): {e}")
        return result

    def load_torrent_cache_meta(self):
        """Load only timestamps (no JSON parsing) for fast startup.
        Returns {client_name: {"timestamp": float}}"""
        result = {}
        try:
            with self._get_conn() as conn:
                rows = conn.execute(
                    "SELECT client_name, timestamp FROM torrent_cache").fetchall()
                for client_name, ts in rows:
                    result[client_name] = {"timestamp": ts}
        except Exception as e:
            print(f"DB Error (load_cache_meta): {e}")
        return result

    def load_torrent_cache_single(self, client_name, ttl_hours=6):
        """Load cached torrent list for a single client (lazy load).
        Returns {"torrents": [...], "timestamp": float} or None if stale/missing."""
        try:
            cutoff = time.time() - (ttl_hours * 3600)
            with self._get_conn() as conn:
                row = conn.execute(
                    "SELECT timestamp, torrents_json FROM torrent_cache WHERE client_name = ? AND timestamp > ?",
                    (client_name, cutoff)).fetchone()
                if row:
                    ts, torrents_json = row
                    try:
                        return {"torrents": json.loads(torrents_json), "timestamp": ts}
                    except (json.JSONDecodeError, TypeError):
                        pass
        except Exception as e:
            print(f"DB Error (load_cache_single): {e}")
        return None

    def delete_torrent_cache(self, client_name=None):
        """Delete cached torrent list for one client or all."""
        try:
            with self._get_conn() as conn:
                if client_name:
                    conn.execute("DELETE FROM torrent_cache WHERE client_name = ?", (client_name,))
                else:
                    conn.execute("DELETE FROM torrent_cache")
        except Exception as e:
            print(f"DB Error (delete_cache): {e}")

    # --- Torrent FILES cache persistence (for Deep Scan) ---

    def save_torrent_files_cache(self, topic_id, files_dict):
        """Save parsed file info {path: size} for a topic."""
        try:
            with self._get_conn() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO torrent_files_cache (topic_id, timestamp, files_json)
                    VALUES (?, ?, ?)
                """, (topic_id, time.time(), json.dumps(files_dict)))
        except Exception as e:
            print(f"DB Error (save_torrent_files_cache): {e}")

    def get_torrent_files_cache(self, topic_id, ttl_days=30):
        """Load parsed file info for a topic. Returns dict or None if not found/stale."""
        try:
            cutoff = time.time() - (ttl_days * 86400)
            with self._get_conn() as conn:
                row = conn.execute(
                    "SELECT timestamp, files_json FROM torrent_files_cache WHERE topic_id = ?",
                    (topic_id,)
                ).fetchone()
                if row:
                    ts, files_json = row
                    if ts > cutoff:
                        try:
                            return json.loads(files_json)
                        except:
                            pass
        except Exception as e:
            print(f"DB Error (get_torrent_files_cache): {e}")
        return None

# === SQLite Hash Database Manager (Deep Scan+) ===
class HashDatabaseManager:
    """Manages the standalone SQLite database used strictly for storing cryptographic piece hashes."""
    def __init__(self, db_path):
        self.db_path = db_path
        self._init_db()
        
    def _get_conn(self):
        return sqlite3.connect(self.db_path, timeout=10)
        
    def _init_db(self):
        try:
            with self._get_conn() as conn:
                # Store the parsed files dictionary, the piece size, and the massive hex strings cleanly separate from app DB
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS torrent_pieces_cache (
                        topic_id INTEGER PRIMARY KEY,
                        timestamp REAL,
                        piece_length INTEGER,
                        pieces_hex TEXT,
                        files_json TEXT
                    )
                """)
        except Exception as e:
            print(f"HashDB Init Error: {e}")
            
    def save_hash_cache(self, topic_id, piece_length, pieces_hex, files_dict):
        try:
            with self._get_conn() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO torrent_pieces_cache (topic_id, timestamp, piece_length, pieces_hex, files_json)
                    VALUES (?, ?, ?, ?, ?)
                """, (topic_id, time.time(), piece_length, pieces_hex, json.dumps(files_dict)))
        except Exception as e:
            print(f"HashDB Error (save): {e}")
            
    def get_hash_cache(self, topic_id, ttl_days=30):
        try:
            cutoff = time.time() - (ttl_days * 86400)
            with self._get_conn() as conn:
                row = conn.execute(
                    "SELECT timestamp, piece_length, pieces_hex, files_json FROM torrent_pieces_cache WHERE topic_id = ?",
                    (topic_id,)
                ).fetchone()
                if row:
                    ts, piece_length, pieces_hex, files_json = row
                    if ts > cutoff:
                        try:
                            # Also return files_dict so Deep Scan+ avoids hitting generic Deep Scan Cache sequentially
                            return {
                                "piece_length": piece_length,
                                "pieces_hex": pieces_hex,
                                "files": json.loads(files_json)
                            }
                        except Exception:
                            pass
        except Exception as e:
            print(f"HashDB Error (get): {e}")
        return None

class CategoryManager:
    def __init__(self, log_func, keys_callback=None, proxies_callback=None):
        self.log = log_func
        self.keys_callback = keys_callback
        self.proxies_callback = proxies_callback
        self.cache = self.load_cache()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        if self.proxies_callback:
            p = self.proxies_callback()
            if p:
                self.session.proxies.update(p)

    def load_cache(self):
        if os.path.exists(CATEGORY_CACHE_FILE):
            try:
                with open(CATEGORY_CACHE_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except:
                pass
        return {"last_updated": "", "categories": {}}

    def save_cache(self):
        try:
            with open(CATEGORY_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=4)
        except Exception as e:
            self.log(f"Error saving category cache: {e}")

    def _get_cache_age_seconds(self):
        last = self.cache.get("last_updated", "")
        if not last:
            return float('inf')
        try:
            dt = datetime.datetime.fromisoformat(last)
            return (datetime.datetime.now() - dt).total_seconds()
        except:
            return float('inf')

    def get_category_name(self, cat_id, ttl_seconds=86400):
        # Check TTL
        if self._get_cache_age_seconds() > ttl_seconds:
            self.refresh_cache()
        
        cat_name = self.cache["categories"].get(str(cat_id))
        if not cat_name:
            # Try forcing full refresh if cache wasn't just refreshed
            if self._get_cache_age_seconds() > 60:
                self.refresh_cache()
                cat_name = self.cache["categories"].get(str(cat_id))
            
            # Still not found? It's a sub-forum — fetch it directly
            if not cat_name:
                cat_name = self.fetch_single_category(cat_id)
        
        return cat_name if cat_name else f"Unknown_Cat_{cat_id}"

    def _build_breadcrumb_path(self, forum_id):
        """Reconstruct breadcrumb path from cached tree. Returns {"category": name, "full_path": "A > B > C"} or None."""
        tree = self.cache.get("tree", {})
        cats = self.cache.get("categories", {})
        fid = int(forum_id)

        for sec_id, sec_tree in tree.items():
            sec_name = cats.get(f"c{sec_id}", "")
            for parent_id, children in sec_tree.items():
                parent_name = cats.get(str(parent_id), "")
                if int(parent_id) == fid:
                    # forum_id is a parent-level forum
                    parts = [p for p in [sec_name, parent_name] if p]
                    return {"category": parent_name, "full_path": " > ".join(parts)} if parts else None
                if fid in children:
                    # forum_id is a child forum
                    child_name = cats.get(str(fid), "")
                    parts = [p for p in [sec_name, parent_name, child_name] if p]
                    return {"category": child_name, "full_path": " > ".join(parts)} if parts else None
        return None

    def fetch_single_category(self, cat_id):
        """Fetch a single category name via Rutracker API, with HTML scraping fallback."""
        try:
            self.log(f"Fetching forum name for category {cat_id} via API...")
            resp = requests.get(
                "https://api.rutracker.cc/v1/get_forum_name",
                params={"by": "forum_id", "val": str(cat_id)},
                proxies=self.proxies_callback() if self.proxies_callback else None,
                timeout=15
            )
            if resp.status_code == 200:
                result = resp.json().get("result", {})
                name = result.get(str(cat_id))
                if name:
                    name = html.unescape(name)
                    self.log(f"  Found via API: {cat_id} = {name}")
                    self.cache["categories"][str(cat_id)] = name
                    self.save_cache()
                    return name
        except Exception as e:
            self.log(f"  API lookup failed for category {cat_id}: {e}")

        # Fallback: scrape the forum page directly
        try:
            self.log(f"  Falling back to HTML scrape for category {cat_id}...")
            resp = self.session.get(f"https://rutracker.org/forum/viewforum.php?f={cat_id}", timeout=15)
            if resp.encoding == 'ISO-8859-1':
                resp.encoding = 'cp1251'
            match = re.search(r'<a\s+href="viewforum\.php\?f=' + str(cat_id) + r'"[^>]*>([^<]+)</a>', resp.text, re.IGNORECASE)
            if match:
                name = match.group(1).strip()
                if name:
                    self.log(f"  Found via HTML: {cat_id} = {name}")
                    self.cache["categories"][str(cat_id)] = name
                    self.save_cache()
                    return name
            title_match = re.search(r'<title>(.*?)\s*(?:\[.*?\])?\s*::', resp.text, re.IGNORECASE)
            if title_match:
                name = title_match.group(1).strip()
                if name:
                    self.log(f"  Found from title: {cat_id} = {name}")
                    self.cache["categories"][str(cat_id)] = name
                    self.save_cache()
                    return name
        except Exception as e:
            self.log(f"  HTML fallback also failed for category {cat_id}: {e}")
        return None

    def get_category_for_topic(self, topic_id):
        """Given a Rutracker topic ID, resolve its category via API + cached tree.
        Returns a dict: {"category": "deepest name", "full_path": "Parent > Child > Deepest"}
        or None on failure."""
        topic_key = str(topic_id)

        # Check topic cache first
        topic_cache = self.cache.get("topics", {})
        cached_cat = topic_cache.get(topic_key)

        try:
            self.log(f"Looking up category for topic {topic_id}...")

            # Use Rutracker API to get forum_id for this topic
            resp = requests.get(
                "https://api.rutracker.cc/v1/get_tor_topic_data",
                params={"by": "topic_id", "val": str(topic_id)},
                proxies=self.proxies_callback() if self.proxies_callback else None,
                timeout=15)

            if resp.status_code != 200:
                raise Exception(f"API returned HTTP {resp.status_code}")

            result = resp.json().get("result", {})
            topic_data = result.get(str(topic_id))

            if not topic_data:
                self.log(f"  Topic {topic_id} not found in API")
                if cached_cat:
                    return {"category": cached_cat, "full_path": cached_cat}
                return None

            forum_id = topic_data.get("forum_id")
            if not forum_id:
                self.log(f"  No forum_id for topic {topic_id}")
                if cached_cat:
                    return {"category": cached_cat, "full_path": cached_cat}
                return None

            # Reconstruct breadcrumb from cached tree
            breadcrumb = self._build_breadcrumb_path(forum_id)
            if breadcrumb:
                cat_name = breadcrumb["category"]
                full_path = breadcrumb["full_path"]
                self.log(f"  Topic {topic_id} -> Forum {forum_id}: {full_path}")

                # Cache topic → category mapping
                if "topics" not in self.cache:
                    self.cache["topics"] = {}
                self.cache["topics"][topic_key] = cat_name
                self.save_cache()
                return breadcrumb

            # Tree lookup failed — try flat cache
            cat_name = self.cache["categories"].get(str(forum_id))
            if not cat_name:
                cat_name = self.fetch_single_category(forum_id)
            if cat_name:
                self.log(f"  Topic {topic_id} -> Forum {forum_id}: {cat_name}")
                if "topics" not in self.cache:
                    self.cache["topics"] = {}
                self.cache["topics"][topic_key] = cat_name
                self.save_cache()
                return {"category": cat_name, "full_path": cat_name}

            fallback = f"Forum_{forum_id}"
            return {"category": fallback, "full_path": fallback}

        except Exception as e:
            self.log(f"  Error looking up topic {topic_id}: {e}")
            if cached_cat:
                self.log(f"  Using cached category: {cached_cat}")
                return {"category": cached_cat, "full_path": cached_cat}
        return None

    def fetch_topic_data(self, topic_id):
        """Fetch topic data from API to get info_hash and other details."""
        url = "https://api.t-ru.org/v2/get_tor_topic_data"
        params = {"by": "topic_id", "val": topic_id}
        try:
            resp = self.session.get(url, params=params, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                # API returns dict with topic_id keys: {"result": {"123": {...}}}
                if data and "result" in data and str(topic_id) in data["result"]:
                    return data["result"][str(topic_id)]
        except Exception as e:
            self.log(f"API Error for {topic_id}: {e}")
        return None

    def login(self, username, password):
        try:
            self.log(f"Logging in to Rutracker as {username}...")
            # First GET the login page to collect any hidden fields/cookies
            login_page = self.session.get("https://rutracker.org/forum/login.php", timeout=15)
            
            url = "https://rutracker.org/forum/login.php"
            data = {
                "login_username": username,
                "login_password": password,
                "login": "Вход",
                "redirect": "index.php"
            }
            resp = self.session.post(url, data=data, timeout=30)
            # Check for session cookie which confirms successful login
            if 'bb_session' in self.session.cookies.get_dict():
                 self.log("Logged in successfully.")
                 self._scrape_keys()
                 return True
            else:
                 self.log(f"Login failed (no session cookie). Response len: {len(resp.text)}")
                 return False
        except Exception as e:
            self.log(f"Login error: {e}")
            return False

    def _scrape_keys(self):
        """Scrape user ID, BT key, and API key from profile/index."""
        try:
            self.log("Scraping user keys...")
            # 1. Get User ID from index
            resp = self.session.get("https://rutracker.org/forum/index.php", timeout=15)
            if resp.encoding == 'ISO-8859-1': resp.encoding = 'cp1251'
            
            uid_match = re.search(r'profile\.php\?mode=viewprofile&amp;u=(\d+)', resp.text)
            if not uid_match:
                uid_match = re.search(r'profile\.php\?mode=viewprofile&u=(\d+)', resp.text)
            
            if uid_match:
                uid = uid_match.group(1)
                self.log(f"  Found User ID: {uid}")
                
                # 2. Get Profile for BT/API keys
                # Usually found in "editprofile" or similar?
                # Let's check "profile.php?mode=viewprofile&u={uid}" first
                prof_url = f"https://rutracker.org/forum/profile.php?mode=viewprofile&u={uid}"
                resp_prof = self.session.get(prof_url, timeout=15)
                if resp_prof.encoding == 'ISO-8859-1': resp_prof.encoding = 'cp1251'
                
                # Check for "bt:" and "api:" and "id:" patterns as requested
                # The user said "we can get bt: ... api: ... id: ... keys from there"
                # This implies they are visible text.
                # Common pattern in some modified profiles or specific layouts.
                # Let's try standard patterns first.
                
                # BitTorrent Passkey often in: <span class="editable">...</span> or raw text
                # "bt: " might be a label.
                
                keys_found = {}
                keys_found['id'] = uid
                
                # Regex for "bt: <b>VALUE</b>" as seen in screenshot
                # We try a few patterns to be safe.
                
                # 1. Try strict HTML pattern from screenshot
                bt_match = re.search(r'bt:\s*<b>\s*([^<]+)\s*</b>', resp_prof.text, re.IGNORECASE)
                if bt_match:
                     keys_found['bt'] = bt_match.group(1)
                else:
                     # 2. Try looser pattern (no closing tag check)
                     bt_match = re.search(r'bt:\s*(?:<[^>]+>)?\s*([a-zA-Z0-9]+)', resp_prof.text, re.IGNORECASE)
                     if bt_match: keys_found['bt'] = bt_match.group(1)

                # API Key
                api_match = re.search(r'api:\s*<b>\s*([^<]+)\s*</b>', resp_prof.text, re.IGNORECASE)
                if api_match:
                    keys_found['api'] = api_match.group(1)
                else:
                    api_match = re.search(r'api:\s*(?:<[^>]+>)?\s*([a-zA-Z0-9]+)', resp_prof.text, re.IGNORECASE)
                    if api_match: keys_found['api'] = api_match.group(1)

                if self.keys_callback:
                    self.keys_callback(keys_found)
                else:
                    self.cache['user_keys'] = keys_found
                    self.save_cache()
                
                log_msg = "  Keys found: "
                if 'bt' in keys_found: log_msg += "BT "
                if 'api' in keys_found: log_msg += "API "
                self.log(log_msg)
                
        except Exception as e:
            self.log(f"Error scraping keys: {e}")

    def refresh_cache(self, username=None, password=None, progress_callback=None):
        """Refresh the category cache via Rutracker API."""
        self.log("Refreshing Rutracker category cache via API...")

        try:
            if progress_callback:
                progress_callback(0, 1)

            resp = requests.get(
                "https://api.rutracker.cc/v1/static/cat_forum_tree",
                proxies=self.proxies_callback() if self.proxies_callback else None,
                timeout=30)
            if resp.status_code != 200:
                raise Exception(f"API returned HTTP {resp.status_code}")

            data = resp.json().get("result", {})
            sections = data.get("c", {})   # {"28": "Спорт", ...}
            forums = data.get("f", {})     # {"1987": "Еврокубки 2011-2024", ...}
            tree = data.get("tree", {})    # {"28": {"1608": [1987, ...], ...}}

            # Build flat categories dict (same format as before)
            sorted_cats = {}
            for fid in sorted(forums.keys(), key=lambda x: int(x)):
                sorted_cats[fid] = html.unescape(forums[fid])

            # Add sections with "c" prefix
            for sid, sname in sections.items():
                sorted_cats[f"c{sid}"] = html.unescape(sname)

            # Store tree for breadcrumb reconstruction
            self.cache["categories"] = sorted_cats
            self.cache["tree"] = tree
            self.cache["last_updated"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.save_cache()

            if progress_callback:
                progress_callback(1, 1)

            self.log(f"Category cache updated via API. {len(sections)} sections, {len(forums)} forums loaded.")

        except Exception as e:
            self.log(f"API refresh failed: {e}")


PM_FOLDERS = {
    "inbox":   "Входящие",
    "sentbox": "Отправленные",
    "outbox":  "Исходящие",
    "savebox": "Сохранённые",
}


class RutrackerPMScraper:
    """Scrapes Rutracker private message folders, reads messages, and sends replies."""

    def __init__(self, session_provider, log_func):
        self.get_session = session_provider
        self.log = log_func

    def _fix_encoding(self, response):
        if response.encoding == 'ISO-8859-1':
            response.encoding = 'cp1251'

    def _is_login_page(self, html_text):
        return 'login_username' in html_text

    def fetch_inbox(self, folder="inbox"):
        """Fetch message list for a PM folder. Returns list of dicts or None if login needed."""
        session = self.get_session()
        try:
            resp = session.get(
                f"https://rutracker.org/forum/privmsg.php?folder={folder}",
                timeout=15
            )
            self._fix_encoding(resp)

            if self._is_login_page(resp.text):
                return None

            messages = []
            rows = re.split(r'<tr\b[^>]*>', resp.text)
            folder_esc = re.escape(folder)

            for row in rows:
                link_match = re.search(
                    rf'privmsg\.php\?folder={folder_esc}&(?:amp;)?mode=read&(?:amp;)?p=(\d+)',
                    row
                )
                if not link_match:
                    continue

                msg_id = link_match.group(1)

                # Subject: capture everything inside <a> including nested tags like <b>, then strip HTML
                subj_match = re.search(
                    rf'<a[^>]*privmsg\.php\?folder={folder_esc}[^>]*mode=read[^>]*>(.*?)</a>',
                    row, re.DOTALL
                )
                if not subj_match:
                    subj_match = re.search(
                        r'<a[^>]*privmsg\.php[^>]*>(.*?)</a>',
                        row, re.DOTALL
                    )
                if subj_match:
                    subject = re.sub(r'<[^>]+>', '', subj_match.group(1))
                    subject = html.unescape(subject).strip()
                    if not subject:
                        subject = "No Subject"
                else:
                    subject = "No Subject"

                # Unread detection
                is_unread = bool(re.search(
                    r'(?:folder_new|pm_unread|topic_new|icon_unread|pm_new)',
                    row, re.IGNORECASE
                ))

                # Sender
                sender_match = re.search(
                    r'profile\.php\?mode=viewprofile&(?:amp;)?u=(\d+)[^>]*>([^<]+)</a>',
                    row
                )
                sender = html.unescape(sender_match.group(2).strip()) if sender_match else "Unknown"
                sender_id = sender_match.group(1) if sender_match else ""

                # Date
                date_match = re.search(
                    r'(\d{1,2}[-./]\w+[-./]\d{2,4}\s+\d{1,2}:\d{2})',
                    row
                )
                if not date_match:
                    date_match = re.search(r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})', row)
                date_str = date_match.group(1) if date_match else ""

                messages.append({
                    "msg_id": msg_id,
                    "subject": subject,
                    "sender": sender,
                    "sender_id": sender_id,
                    "date": date_str,
                    "is_unread": is_unread,
                })

            return messages
        except Exception as e:
            self.log(f"PM fetch error ({folder}): {e}")
            return None

    def fetch_message(self, msg_id, folder="inbox"):
        """Fetch a single message content. Returns dict or None if login needed."""
        session = self.get_session()
        try:
            resp = session.get(
                f"https://rutracker.org/forum/privmsg.php?folder={folder}&mode=read&p={msg_id}",
                timeout=15
            )
            self._fix_encoding(resp)

            if self._is_login_page(resp.text):
                return None

            result = {"msg_id": msg_id}

            # Subject: try multiple patterns for Rutracker's phpBB2 PM read page
            # Pattern 1: "ТЕМА" header row (Rutracker uses this label)
            subj_match = re.search(r'(?:ТЕМА|Тема|Subject)\s*(?:</[^>]+>)?\s*(?:<[^>]+>\s*)*(.+?)(?:</td|</div|</span)', resp.text, re.DOTALL | re.IGNORECASE)
            if not subj_match:
                # Pattern 2: link text on the read page itself
                subj_match = re.search(r'<a[^>]*privmsg\.php[^>]*>(.*?)</a>', resp.text, re.DOTALL)
            if not subj_match:
                # Pattern 3: page title
                subj_match = re.search(r'<title>(.*?)(?:\s*[-:]|</title>)', resp.text)
            if subj_match:
                subj_raw = re.sub(r'<[^>]+>', '', subj_match.group(1))
                result["subject"] = html.unescape(subj_raw).strip() or "No Subject"
            else:
                result["subject"] = "No Subject"

            # Sender
            sender_match = re.search(
                r'profile\.php\?mode=viewprofile&(?:amp;)?u=(\d+)[^>]*>([^<]+)</a>',
                resp.text
            )
            result["sender"] = html.unescape(sender_match.group(2).strip()) if sender_match else "Unknown"
            result["sender_id"] = sender_match.group(1) if sender_match else ""

            # Date: Rutracker uses format like "26-02-22 02:15" in the PM page
            date_match = re.search(r'(\d{2}-\d{2}-\d{2}\s+\d{2}:\d{2})', resp.text)
            if not date_match:
                date_match = re.search(r'(?:Sent|Date|Отправлено)[:\s]*([^<\n]+)', resp.text, re.IGNORECASE)
            if not date_match:
                date_match = re.search(r'(\d{1,2}[-./]\w+[-./]\d{2,4}\s+\d{1,2}:\d{2})', resp.text)
            result["date"] = date_match.group(1).strip() if date_match else ""

            # Message body
            body_match = re.search(
                r'<div[^>]*class="[^"]*(?:postbody|post_text|msg-body|post_body)[^"]*"[^>]*>(.*?)</div>',
                resp.text, re.DOTALL | re.IGNORECASE
            )
            if not body_match:
                body_match = re.search(
                    r'<td[^>]*class="[^"]*(?:postbody|row1|message)[^"]*"[^>]*>(.*?)</td>',
                    resp.text, re.DOTALL
                )
            body_html = body_match.group(1).strip() if body_match else ""
            result["body_html"] = body_html

            # Convert to plain text
            body_text = re.sub(r'<br\s*/?>', '\n', body_html)
            body_text = re.sub(r'<[^>]+>', '', body_text)
            result["body_text"] = html.unescape(body_text).strip() if body_text else "(Could not parse message body)"

            # Form token for reply — try HTML hidden field first, then JS
            token_match = re.search(r'name="form_token"\s+value="([^"]+)"', resp.text)
            if not token_match:
                token_match = re.search(r'name="sid"\s+value="([^"]+)"', resp.text)
            if token_match:
                result["form_token"] = token_match.group(1)
            else:
                # Fallback: extract from JavaScript (window.BB.form_token)
                result["form_token"] = self._extract_js_form_token(resp.text)

            return result
        except Exception as e:
            self.log(f"PM fetch_message error for {msg_id}: {e}")
            return None

    def send_reply(self, msg_id, subject, body):
        """Send a reply to a private message using two-step form flow.
        Returns True on success."""
        session = self.get_session()
        try:
            # Step 1: GET the reply compose page
            resp = session.get(
                f"https://rutracker.org/forum/privmsg.php?mode=reply&p={msg_id}",
                timeout=15
            )
            self._fix_encoding(resp)

            if self._is_login_page(resp.text):
                self.log("Reply failed: session expired")
                return False

            # Step 2: Extract JS form_token and find the compose form
            js_token = self._extract_js_form_token(resp.text)
            forms = self._extract_forms(resp.text)

            compose_form = None
            for form in forms:
                if re.search(r'<textarea[^>]*name="message"', form["raw"], re.IGNORECASE):
                    compose_form = form
                    break
            if not compose_form:
                for form in forms:
                    if "privmsg" in form["action"]:
                        compose_form = form
                        break
            if not compose_form:
                self.log(f"Reply failed: compose form not found for msg {msg_id}")
                return False

            # Step 3: Build POST data from hidden fields + user input
            # Override submit_mode (JS sets it to 'submit' before form.submit())
            post_data = []
            for k, v in compose_form["fields"]:
                if k in ("subject", "message"):
                    continue
                if k == "submit_mode":
                    post_data.append(("submit_mode", "submit"))
                else:
                    post_data.append((k, v))
            post_data.append(("subject", subject.encode("windows-1251", errors="replace")))
            post_data.append(("message", body.encode("windows-1251", errors="replace")))

            # Inject JS form_token
            field_names = {k for k, v in post_data}
            if js_token and "form_token" not in field_names:
                post_data.append(("form_token", js_token))

            # Step 4: POST the form
            url = self._resolve_url(compose_form["action"])
            resp2 = session.post(url, data=post_data, timeout=30)
            self._fix_encoding(resp2)

            if self._is_login_page(resp2.text):
                self.log("Reply failed: session expired on submit")
                return False

            # Check for success
            if re.search(r'(?:message.*sent|сообщение.*отправлено|privmsg\.php\?folder=sentbox)', resp2.text, re.IGNORECASE):
                return True

            # Try confirmation page if needed
            resp3 = self._submit_confirm_page(resp2.text)
            if resp3 and not self._is_login_page(resp3.text):
                if re.search(r'(?:message.*sent|сообщение.*отправлено|privmsg\.php\?folder=sentbox)', resp3.text, re.IGNORECASE):
                    return True

            error_match = re.search(r'class="[^"]*gen[^"]*"[^>]*>(.*?error.*?)</td>', resp2.text, re.IGNORECASE | re.DOTALL)
            if error_match:
                self.log(f"Reply error: {html.unescape(re.sub(r'<[^>]+>', '', error_match.group(1))).strip()}")
            else:
                self.log(f"Reply may have failed for msg {msg_id}")

            return False
        except Exception as e:
            self.log(f"PM send_reply error: {e}")
            return False

    def send_new_message(self, recipient, subject, body):
        """Send a new private message using two-step form flow.
        Returns True on success."""
        session = self.get_session()
        try:
            # Step 1: GET the compose page
            resp = session.get(
                "https://rutracker.org/forum/privmsg.php?mode=post",
                timeout=15
            )
            self._fix_encoding(resp)

            if self._is_login_page(resp.text):
                self.log("New PM failed: session expired")
                return False

            # Step 2: Extract JS form_token and find the compose form
            js_token = self._extract_js_form_token(resp.text)
            forms = self._extract_forms(resp.text)

            compose_form = None
            for form in forms:
                if re.search(r'<textarea[^>]*name="message"', form["raw"], re.IGNORECASE):
                    compose_form = form
                    break
            if not compose_form:
                for form in forms:
                    if "privmsg" in form["action"]:
                        compose_form = form
                        break
            if not compose_form:
                self.log("New PM failed: compose form not found")
                return False

            # Step 3: Build POST data from hidden fields + user input
            # Override submit_mode (JS sets it to 'submit' before form.submit())
            post_data = []
            for k, v in compose_form["fields"]:
                if k in ("subject", "message", "username"):
                    continue
                if k == "submit_mode":
                    post_data.append(("submit_mode", "submit"))
                else:
                    post_data.append((k, v))
            post_data.append(("username", recipient.encode("windows-1251", errors="replace")))
            post_data.append(("subject", subject.encode("windows-1251", errors="replace")))
            post_data.append(("message", body.encode("windows-1251", errors="replace")))

            # Inject JS form_token
            field_names = {k for k, v in post_data}
            if js_token and "form_token" not in field_names:
                post_data.append(("form_token", js_token))

            # Step 4: POST the form
            url = self._resolve_url(compose_form["action"])
            resp2 = session.post(url, data=post_data, timeout=30)
            self._fix_encoding(resp2)

            if self._is_login_page(resp2.text):
                self.log("New PM failed: session expired on submit")
                return False

            # Check for success
            if re.search(r'(?:message.*sent|сообщение.*отправлено|privmsg\.php\?folder=sentbox)', resp2.text, re.IGNORECASE):
                self.log(f"New PM to '{recipient}' sent OK")
                return True

            # Try confirmation page if needed
            resp3 = self._submit_confirm_page(resp2.text)
            if resp3 and not self._is_login_page(resp3.text):
                if re.search(r'(?:message.*sent|сообщение.*отправлено|privmsg\.php\?folder=sentbox)', resp3.text, re.IGNORECASE):
                    self.log(f"New PM to '{recipient}' sent OK")
                    return True

            error_match = re.search(r'class="[^"]*gen[^"]*"[^>]*>(.*?error.*?)</td>', resp2.text, re.IGNORECASE | re.DOTALL)
            if error_match:
                self.log(f"New PM error: {html.unescape(re.sub(r'<[^>]+>', '', error_match.group(1))).strip()}")
            else:
                self.log("New PM may have failed (no success marker found)")

            return False
        except Exception as e:
            self.log(f"PM send_new_message error: {e}")
            return False

    def _extract_js_form_token(self, html_text):
        """Extract form_token from Rutracker's JavaScript (window.BB.form_token).
        Returns the token string or empty string if not found."""
        m = re.search(r"""form_token:\s*['"]([a-f0-9]+)['"]""", html_text)
        if m:
            return m.group(1)
        # Fallback: try other patterns
        m2 = re.search(r"""form_token\s*=\s*['"]([a-f0-9]+)['"]""", html_text)
        return m2.group(1) if m2 else ""

    def _extract_forms(self, html_text):
        """Extract all <form> blocks with their action URLs and fields.
        fields = only hidden inputs. submits = submit buttons + <button> elements."""
        forms = []
        for form_match in re.finditer(r'<form\b([^>]*)>(.*?)</form>', html_text, re.DOTALL | re.IGNORECASE):
            attrs = form_match.group(1)
            body = form_match.group(2)

            action = ""
            a_match = re.search(r'action="([^"]*)"', attrs)
            if a_match:
                action = html.unescape(a_match.group(1))

            method = "get"
            m_match = re.search(r'method="([^"]*)"', attrs, re.IGNORECASE)
            if m_match:
                method = m_match.group(1).lower()

            # Collect submit button names so we can exclude them from fields
            submit_names = set()
            submits = []

            # <input type="submit"> buttons
            for inp in re.finditer(r'<input\b([^>]*type="submit"[^>]*)>', body, re.IGNORECASE):
                tag = inp.group(1)
                name_m = re.search(r'name="([^"]+)"', tag)
                value_m = re.search(r'value="([^"]*)"', tag)
                if name_m:
                    submits.append((name_m.group(1), value_m.group(1) if value_m else ""))
                    submit_names.add(name_m.group(1))

            # <button> elements (type="submit" or no type)
            for btn in re.finditer(r'<button\b([^>]*)>(.*?)</button>', body, re.DOTALL | re.IGNORECASE):
                btn_attrs = btn.group(1)
                btn_text = re.sub(r'<[^>]+>', '', btn.group(2)).strip()
                btn_type = ""
                type_m = re.search(r'type="([^"]*)"', btn_attrs, re.IGNORECASE)
                if type_m:
                    btn_type = type_m.group(1).lower()
                # Buttons with no type or type="submit" are submit buttons
                if btn_type in ("", "submit"):
                    name_m = re.search(r'name="([^"]+)"', btn_attrs)
                    value_m = re.search(r'value="([^"]*)"', btn_attrs)
                    s_name = name_m.group(1) if name_m else btn_text
                    s_val = value_m.group(1) if value_m else btn_text
                    submits.append((s_name, s_val))
                    if name_m:
                        submit_names.add(name_m.group(1))

            # Fields: only hidden inputs (exclude submit button names)
            fields = []
            for inp in re.finditer(r'<input\b([^>]*)>', body, re.IGNORECASE):
                tag = inp.group(1)
                # Skip submit/button type inputs
                type_m = re.search(r'type="([^"]*)"', tag, re.IGNORECASE)
                input_type = type_m.group(1).lower() if type_m else "text"
                if input_type in ("submit", "button", "reset", "image"):
                    continue
                name_m = re.search(r'name="([^"]+)"', tag)
                value_m = re.search(r'value="([^"]*)"', tag)
                if name_m and name_m.group(1) not in submit_names:
                    fields.append((name_m.group(1), value_m.group(1) if value_m else ""))

            forms.append({
                "action": action,
                "method": method,
                "fields": fields,
                "submits": submits,
                "raw": body,
            })
        return forms

    def _resolve_url(self, action):
        """Resolve a form action to a full URL."""
        if not action or action.startswith("http"):
            return action or "https://rutracker.org/forum/privmsg.php"
        return "https://rutracker.org/forum/" + action.lstrip("./")

    def _submit_confirm_page(self, resp_text):
        """Find and submit the confirmation form ('Да' button) on a Rutracker confirm page.
        Also handles bare <button> and <a> link confirmations.
        Injects JS form_token (CSRF) if present on the page.
        Returns the response or None."""
        session = self.get_session()

        # Extract JS form_token from the confirmation page
        js_token = self._extract_js_form_token(resp_text)

        # Try 1: form-based confirmation
        forms = self._extract_forms(resp_text)
        for form in forms:
            for s_name, s_val in form["submits"]:
                if s_name == "confirm" or "Да" in s_val or "Yes" in s_val:
                    post_data = list(form["fields"])
                    post_data.append((s_name, s_val))
                    # Inject JS form_token if not already in form fields
                    field_names = {k for k, v in post_data}
                    if js_token and "form_token" not in field_names:
                        post_data.append(("form_token", js_token))
                    url = self._resolve_url(form["action"])
                    resp = session.post(url, data=post_data, timeout=30)
                    self._fix_encoding(resp)
                    return resp

        # Try 2: standalone <button> outside of detected forms (malformed HTML)
        for btn in re.finditer(r'<button\b([^>]*)>(.*?)</button>', resp_text, re.DOTALL | re.IGNORECASE):
            btn_text = re.sub(r'<[^>]+>', '', btn.group(2)).strip()
            if btn_text in ("Да", "Yes", "Confirm"):
                post_data = []
                for hidden in re.finditer(r'<input[^>]*type="hidden"[^>]*name="([^"]+)"[^>]*value="([^"]*)"', resp_text):
                    post_data.append((hidden.group(1), hidden.group(2)))
                name_m = re.search(r'name="([^"]+)"', btn.group(1))
                if name_m:
                    post_data.append((name_m.group(1), btn_text))
                else:
                    post_data.append(("confirm", btn_text))
                # Inject JS form_token
                field_names = {k for k, v in post_data}
                if js_token and "form_token" not in field_names:
                    post_data.append(("form_token", js_token))
                resp = session.post("https://rutracker.org/forum/privmsg.php", data=post_data, timeout=30)
                self._fix_encoding(resp)
                return resp

        # Try 3: confirmation link <a href="...">Да</a>
        link_match = re.search(r'<a\b[^>]*href="([^"]*)"[^>]*>\s*(?:Да|Yes|Confirm)\s*</a>', resp_text, re.IGNORECASE)
        if link_match:
            url = html.unescape(link_match.group(1))
            if not url.startswith("http"):
                url = "https://rutracker.org/forum/" + url.lstrip("./")
            resp = session.get(url, timeout=15)
            self._fix_encoding(resp)
            return resp
        return None

    def delete_messages(self, msg_ids, folder="inbox"):
        """Delete private messages one by one via the read page 'Удалить сообщение' button.
        Returns number of successfully deleted."""
        session = self.get_session()
        deleted = 0

        for mid in msg_ids:
            try:
                # Step 1: GET the message read page
                resp = session.get(
                    f"https://rutracker.org/forum/privmsg.php?folder={folder}&mode=read&p={mid}",
                    timeout=15
                )
                self._fix_encoding(resp)

                if self._is_login_page(resp.text):
                    self.log(f"PM delete [{mid}]: session expired")
                    return deleted

                # Extract JS form_token (CSRF) from page JavaScript
                js_token = self._extract_js_form_token(resp.text)

                # Step 2: Find the form with "Удалить сообщение" submit button
                forms = self._extract_forms(resp.text)
                delete_submitted = False
                for form in forms:
                    for s_name, s_val in form["submits"]:
                        if "далить" in s_val or "delete" in s_name.lower():
                            post_data = list(form["fields"])
                            post_data.append((s_name, s_val))
                            # Inject JS form_token if not already present in form fields
                            field_names = {k for k, v in post_data}
                            if js_token and "form_token" not in field_names:
                                post_data.append(("form_token", js_token))
                            url = self._resolve_url(form["action"])

                            resp2 = session.post(url, data=post_data, timeout=30)
                            self._fix_encoding(resp2)

                            if self._is_login_page(resp2.text):
                                self.log(f"PM delete [{mid}]: session expired on confirm step")
                                return deleted

                            # Step 3: Handle confirmation page
                            resp3 = self._submit_confirm_page(resp2.text)
                            if resp3 and not self._is_login_page(resp3.text):
                                deleted += 1
                                self.log(f"PM deleted [{mid}] OK")
                            else:
                                self.log(f"PM delete [{mid}]: confirmation failed")

                            delete_submitted = True
                            break
                    if delete_submitted:
                        break

                if not delete_submitted:
                    self.log(f"PM delete [{mid}]: no delete button found")

            except Exception as e:
                self.log(f"PM delete error for {mid}: {e}")
        return deleted


    def save_messages(self, msg_ids, folder="inbox"):
        """Save private messages one by one via the read page 'Сохранить сообщение' button.
        Moves messages to the savebox folder. Returns number of successfully saved."""
        session = self.get_session()
        saved = 0

        for mid in msg_ids:
            try:
                # Step 1: GET the message read page
                resp = session.get(
                    f"https://rutracker.org/forum/privmsg.php?folder={folder}&mode=read&p={mid}",
                    timeout=15
                )
                self._fix_encoding(resp)

                if self._is_login_page(resp.text):
                    self.log(f"PM save [{mid}]: session expired")
                    return saved

                # Extract JS form_token (CSRF) from page JavaScript
                js_token = self._extract_js_form_token(resp.text)

                # Step 2: Find the form with "Сохранить сообщение" submit button
                forms = self._extract_forms(resp.text)
                save_submitted = False
                for form in forms:
                    for s_name, s_val in form["submits"]:
                        if "охранить" in s_val or "save" in s_name.lower():
                            post_data = list(form["fields"])
                            post_data.append((s_name, s_val))
                            # Inject JS form_token
                            field_names = {k for k, v in post_data}
                            if js_token and "form_token" not in field_names:
                                post_data.append(("form_token", js_token))
                            url = self._resolve_url(form["action"])

                            resp2 = session.post(url, data=post_data, timeout=30)
                            self._fix_encoding(resp2)

                            if self._is_login_page(resp2.text):
                                self.log(f"PM save [{mid}]: session expired on confirm step")
                                return saved

                            # Step 3: Handle confirmation page
                            resp3 = self._submit_confirm_page(resp2.text)
                            if resp3 and not self._is_login_page(resp3.text):
                                saved += 1
                                self.log(f"PM saved [{mid}] OK")
                            else:
                                # Some save actions succeed without confirmation
                                if re.search(r'(?:сохранен|saved|savebox)', resp2.text, re.IGNORECASE):
                                    saved += 1
                                    self.log(f"PM saved [{mid}] OK")
                                else:
                                    self.log(f"PM save [{mid}]: confirmation failed")

                            save_submitted = True
                            break
                    if save_submitted:
                        break

                if not save_submitted:
                    self.log(f"PM save [{mid}]: no save button found")

            except Exception as e:
                self.log(f"PM save error for {mid}: {e}")
        return saved


class ToolTip:
    def __init__(self, widget):
        self.widget = widget
        self.tip_window = None
        self.id = None
        self.x = self.y = 0

    def show_tip(self, text, x_offset=15, y_offset=20):
        self.text = text
        if self.tip_window or not self.text:
            return
        x_val = self.widget.winfo_rootx() + self.x + x_offset
        y_val = self.widget.winfo_rooty() + self.y + y_offset
        
        self.tip_window = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x_val}+{y_val}")
        
        label = tk.Label(tw, text=self.text, justify='left',
                         background="#ffffe0", relief='solid', borderwidth=1,
                         font=("Segoe UI", 9))
        label.pack(ipadx=4, ipady=1)

    def hide_tip(self):
        tw = self.tip_window
        self.tip_window = None
        if tw:
            tw.destroy()


class KeeperAuthDialog:
    """Modal startup dialog — verifies user is an authorized Keeper.
    Phase 1: Check keeper list + fetch login page with CAPTCHA.
    Phase 2: User solves CAPTCHA, submit Rutracker login.
    Shows only a generic denial on any failure (no hints for non-keepers).
    """

    KEEPERS_API_URL = "https://api.rutracker.cc/v1/static/keepers_user_data"
    KEEPER_AUTH_TEMP_FILE = os.path.join(_DATA_DIR, "_keeper_auth_tmp.json")
    RUTRACKER_LOGIN_URL = "https://rutracker.org/forum/login.php"

    LANG_DISPLAY = {"en": "English", "ru": "Русский"}

    def __init__(self, root, config):
        self.root = root
        self.config = config
        self.authenticated = False
        self.nickname = ""
        self._password = ""
        self._phase = 1
        self.keeper_nicknames_memory = set()
        self._login_session = None
        self._cap_sid = ""
        self._captcha_photo = None

        self.dialog = tk.Toplevel(root)
        self.dialog.title(t("auth.title"))
        self.dialog.resizable(False, False)
        self.dialog.protocol("WM_DELETE_WINDOW", self._on_close)

        # Center on screen
        sw = root.winfo_screenwidth()
        sh = root.winfo_screenheight()
        dw, dh = 400, 410
        x = (sw // 2) - (dw // 2)
        y = (sh // 2) - (dh // 2)
        self.dialog.geometry(f"{dw}x{dh}+{x}+{y}")

        self.dialog.lift()
        self.dialog.focus_force()
        self.dialog.grab_set()

        # Language selector (top-right corner)
        lang_frame = tk.Frame(self.dialog)
        lang_frame.pack(fill="x", padx=12, pady=(6, 0))
        lang_frame.columnconfigure(0, weight=1)

        self._lang_var = tk.StringVar(value=self.LANG_DISPLAY.get(_current_lang, "English"))
        self._lang_combo = ttk.Combobox(lang_frame, textvariable=self._lang_var,
                                        values=list(self.LANG_DISPLAY.values()),
                                        state="readonly", width=12, font=("Segoe UI", 9))
        self._lang_combo.pack(side="right")
        self._lang_combo.bind("<<ComboboxSelected>>", self._on_lang_change)

        # Import Config button (next to language dropdown)
        self._import_btn = tk.Button(lang_frame, text=t("auth.import_config"),
                                     font=("Segoe UI", 9), command=self._on_import)
        self._import_btn.pack(side="right", padx=(0, 8))

        # Title
        self.lbl_title = tk.Label(self.dialog, text=t("auth.title"), font=("Segoe UI", 16, "bold"))
        self.lbl_title.pack(pady=(8, 8))

        # Nickname
        self.lbl_nick = tk.Label(self.dialog, text=t("auth.prompt"), font=("Segoe UI", 10))
        self.lbl_nick.pack(pady=(0, 2))
        self.entry_nick = tk.Entry(self.dialog, font=("Segoe UI", 12), width=28, justify="center")
        self.entry_nick.pack(pady=(0, 8))
        self.entry_nick.focus_set()

        # Password
        self.lbl_pass = tk.Label(self.dialog, text=t("auth.password_prompt"), font=("Segoe UI", 10))
        self.lbl_pass.pack(pady=(0, 2))
        self.entry_pass = tk.Entry(self.dialog, font=("Segoe UI", 12), width=28, justify="center", show="\u2022")
        self.entry_pass.pack(pady=(0, 8))
        self.entry_pass.bind("<Return>", lambda e: self._on_login())

        # Login button
        self.login_btn = tk.Button(self.dialog, text=t("auth.login_btn"), font=("Segoe UI", 11, "bold"),
                                   width=14, command=self._on_login)
        self.login_btn.pack(pady=(0, 10))

        # Canvas for lock/unlock animation
        self.canvas = tk.Canvas(self.dialog, width=80, height=80, highlightthickness=0)
        self.canvas.pack(pady=(0, 4))

        # Status label
        self.status_label = tk.Label(self.dialog, text="", font=("Segoe UI", 11))
        self.status_label.pack(pady=(0, 10))

        # Version label (bottom)
        self._ver_label = tk.Label(self.dialog, text=f"v{APP_VERSION}  checking…",
                                   font=("Segoe UI", 8), fg="gray")
        self._ver_label.pack(side="bottom", pady=(0, 4))

        # Background update check (queue pattern for Python 3.13 thread safety)
        self._update_queue = queue.Queue()
        threading.Thread(target=self._check_update, daemon=True).start()
        self._poll_update()

    # ── Language ──────────────────────────────────────────────────────

    def _on_lang_change(self, event=None):
        global _current_lang
        display = self._lang_var.get()
        for code, name in self.LANG_DISPLAY.items():
            if name == display:
                _current_lang = code
                break
        self.dialog.title(t("auth.title"))
        self.lbl_title.config(text=t("auth.title"))
        self.lbl_nick.config(text=t("auth.prompt"))
        self.lbl_pass.config(text=t("auth.password_prompt"))
        self.login_btn.config(text=t("auth.login_btn"))
        self._import_btn.config(text=t("auth.import_config"))
        if hasattr(self, "lbl_cap"):
            self.lbl_cap.config(text=t("auth.captcha_prompt"))

    # ── Import config from ZIP ───────────────────────────────────────

    def _on_import(self):
        src = filedialog.askopenfilename(
            title=t("auth.import_config"),
            filetypes=[("ZIP Archive", "*.zip")],
            parent=self.dialog,
        )
        if not src:
            return
        try:
            _import_map = {
                "keepers_orchestrator_config.json": CONFIG_FILE,
                "keepers_orchestrator_categories.json": CATEGORY_CACHE_FILE,
                "keepers_orchestrator_data.db": DATA_DB_FILE,
                "keepers_orchestrator_hashes.db": HASHES_DB_FILE,
            }
            with zipfile.ZipFile(src, "r") as zf:
                names = zf.namelist()
                if "keepers_orchestrator_config.json" not in names:
                    messagebox.showerror(t("auth.import_config"),
                                         "ZIP must contain keepers_orchestrator_config.json",
                                         parent=self.dialog)
                    return
                for arc_name in names:
                    dest = _import_map.get(arc_name)
                    if dest:
                        with zf.open(arc_name) as sf, open(dest, "wb") as df:
                            df.write(sf.read())

            # Read imported config
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                imported_cfg = json.load(f)

            keeper_nick = imported_cfg.get("keeper_nickname", "")
            if keeper_nick:
                # Valid keeper — skip login entirely
                self.nickname = keeper_nick
                self.authenticated = True
                self.dialog.destroy()
            else:
                # No saved nickname — reload language/config and let user log in
                global _current_lang
                _current_lang = imported_cfg.get("language", _current_lang)
                self.config = imported_cfg
                self._lang_var.set(self.LANG_DISPLAY.get(_current_lang, "English"))
                self._on_lang_change()
                # Pre-fill rutracker username if available
                rt_user = imported_cfg.get("rutracker_auth", {}).get("username", "")
                if rt_user:
                    self.entry_nick.delete(0, "end")
                    self.entry_nick.insert(0, rt_user)
                messagebox.showinfo(t("auth.import_config"),
                                    "✓ Config imported. Please log in.",
                                    parent=self.dialog)
        except Exception as e:
            messagebox.showerror(t("auth.import_config"), str(e),
                                 parent=self.dialog)

    # ── Close ─────────────────────────────────────────────────────────

    def _on_close(self):
        self.authenticated = False
        self.dialog.destroy()

    # ── Login (two-phase) ─────────────────────────────────────────────

    def _on_login(self):
        if self._phase == 1:
            nickname = self.entry_nick.get().strip()
            password = self.entry_pass.get().strip()
            if not nickname or not password:
                return
            self.nickname = nickname
            self._password = password
            self.login_btn.config(state="disabled")
            self.entry_nick.config(state="disabled")
            self.entry_pass.config(state="disabled")
            self._lang_combo.config(state="disabled")
            self.status_label.config(text=t("auth.checking"), fg="#2196F3")
            self._result_queue = queue.Queue()
            threading.Thread(target=self._verify_keeper, daemon=True).start()
            self._poll_result()
        elif self._phase == 2:
            cap_code = self.entry_cap.get().strip()
            if not cap_code:
                return
            self.login_btn.config(state="disabled")
            self.entry_cap.config(state="disabled")
            self.status_label.config(text=t("auth.checking"), fg="#2196F3")
            self._result_queue = queue.Queue()
            threading.Thread(target=self._submit_captcha, args=(cap_code,), daemon=True).start()
            self._poll_result()

    def _poll_result(self):
        try:
            result = self._result_queue.get_nowait()
        except queue.Empty:
            self.dialog.after(100, self._poll_result)
            return
        kind, value = result
        if kind == "ok":
            self._show_result(value)
        elif kind == "captcha":
            self._show_captcha(value)
        else:
            self._show_result(False)

    # ── Phase 1: keeper list + fetch CAPTCHA ──────────────────────────

    def _verify_keeper(self):
        try:
            proxies = self._get_proxies()

            # Step 1: Fetch keeper list and check nickname (silently)
            resp = requests.get(self.KEEPERS_API_URL, proxies=proxies, timeout=30)
            data = resp.json()
            result = data.get("result", {})

            with open(self.KEEPER_AUTH_TEMP_FILE, "w", encoding="utf-8") as f:
                json.dump(result, f)

            self.keeper_nicknames_memory = {
                v[0].lower() for v in result.values() if v and len(v) >= 1
            }

            db = DatabaseManager(DATA_DB_FILE)
            db.save_keepers_users(result)
            db_nicknames = db.get_all_keeper_usernames()
            del db

            if os.path.exists(self.KEEPER_AUTH_TEMP_FILE):
                os.remove(self.KEEPER_AUTH_TEMP_FILE)

            nick_lower = self.nickname.lower()
            in_memory = nick_lower in self.keeper_nicknames_memory
            in_db = nick_lower in db_nicknames
            if not (in_memory and in_db):
                self._result_queue.put(("ok", False))
                return

            # Step 2: Create session and fetch login page
            self._login_session = requests.Session()
            self._login_session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/91.0.4472.124 Safari/537.36"
            })
            if proxies:
                self._login_session.proxies.update(proxies)

            get_resp = self._login_session.get(self.RUTRACKER_LOGIN_URL, timeout=15)
            html_text = get_resp.text

            # Look for CAPTCHA
            cap_sid_match = re.search(
                r'name=["\']cap_sid["\'][^>]*value=["\']([^"\']+)', html_text
            )
            if not cap_sid_match:
                cap_sid_match = re.search(
                    r'value=["\']([^"\']+)["\'][^>]*name=["\']cap_sid["\']', html_text
                )
            cap_img_match = re.search(
                r'<img[^>]+src=["\']([^"\']*captcha[^"\']*)["\']', html_text, re.IGNORECASE
            )

            if cap_sid_match and cap_img_match:
                # CAPTCHA required — send image to UI
                self._cap_sid = cap_sid_match.group(1)
                cap_img_url = cap_img_match.group(1)
                if cap_img_url.startswith("/"):
                    cap_img_url = "https://rutracker.org" + cap_img_url
                img_resp = self._login_session.get(cap_img_url, timeout=15)
                img_b64 = base64.b64encode(img_resp.content).decode("ascii")
                self._result_queue.put(("captcha", img_b64))
            else:
                # No CAPTCHA — try direct login
                login_data = {
                    "login_username": self.nickname,
                    "login_password": self._password,
                    "login": "\u0412\u0445\u043e\u0434",
                    "redirect": "index.php",
                }
                self._login_session.post(self.RUTRACKER_LOGIN_URL, data=login_data, timeout=30)
                if "bb_session" in self._login_session.cookies.get_dict():
                    self._result_queue.put(("ok", True))
                else:
                    self._result_queue.put(("ok", False))

        except Exception:
            self._result_queue.put(("ok", False))

    # ── Phase 2 UI: show CAPTCHA ──────────────────────────────────────

    def _show_captcha(self, img_b64):
        # Unpack bottom widgets so we can insert CAPTCHA before them
        self.login_btn.pack_forget()
        self.canvas.pack_forget()
        self.status_label.pack_forget()

        # CAPTCHA image (JPEG → Pillow → ImageTk)
        raw_bytes = base64.b64decode(img_b64)
        pil_img = PILImage.open(io.BytesIO(raw_bytes))
        self._captcha_photo = ImageTk.PhotoImage(pil_img)
        self.lbl_captcha_img = tk.Label(self.dialog, image=self._captcha_photo)
        self.lbl_captcha_img.pack(pady=(4, 4))

        # Code entry
        self.lbl_cap = tk.Label(self.dialog, text=t("auth.captcha_prompt"), font=("Segoe UI", 10))
        self.lbl_cap.pack(pady=(0, 2))
        self.entry_cap = tk.Entry(self.dialog, font=("Segoe UI", 12), width=16, justify="center")
        self.entry_cap.pack(pady=(0, 8))
        self.entry_cap.focus_set()
        self.entry_cap.bind("<Return>", lambda e: self._on_login())

        # Re-pack bottom widgets
        self.login_btn.pack(pady=(0, 10))
        self.canvas.pack(pady=(0, 4))
        self.status_label.pack(pady=(0, 10))

        self.login_btn.config(state="normal")
        self.status_label.config(text="")
        self._phase = 2

        # Resize dialog to fit CAPTCHA
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        dw, dh = 400, 540
        x = (sw // 2) - (dw // 2)
        y = (sh // 2) - (dh // 2)
        self.dialog.geometry(f"{dw}x{dh}+{x}+{y}")

    # ── Phase 2 submit: login with CAPTCHA code ──────────────────────

    def _submit_captcha(self, cap_code):
        try:
            login_data = {
                "login_username": self.nickname,
                "login_password": self._password,
                "login": "\u0412\u0445\u043e\u0434",
                "redirect": "index.php",
                "cap_sid": self._cap_sid,
                "cap_code": cap_code,
            }
            self._login_session.post(self.RUTRACKER_LOGIN_URL, data=login_data, timeout=30)
            if "bb_session" in self._login_session.cookies.get_dict():
                self._result_queue.put(("ok", True))
            else:
                self._result_queue.put(("ok", False))
        except Exception:
            self._result_queue.put(("ok", False))

    # ── Result handling ───────────────────────────────────────────────

    def _show_result(self, is_keeper):
        if is_keeper:
            self._animate_unlock()
            self.authenticated = True
            self.config["keeper_nickname"] = self.nickname
            try:
                if os.path.exists(CONFIG_FILE):
                    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                        disk_config = json.load(f)
                else:
                    disk_config = copy.deepcopy(DEFAULT_CONFIG)
                disk_config["keeper_nickname"] = self.nickname
                disk_config["language"] = _current_lang
                if "rutracker_auth" not in disk_config:
                    disk_config["rutracker_auth"] = {}
                disk_config["rutracker_auth"]["username"] = self.nickname
                disk_config["rutracker_auth"]["password"] = self._password
                with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                    json.dump(disk_config, f, indent=4, ensure_ascii=False)
            except Exception:
                pass
        else:
            self._animate_lock()

    # ── Animations ────────────────────────────────────────────────────

    def _draw_padlock(self, color="#888888", shackle_open=False):
        c = self.canvas
        c.delete("all")
        c.create_rectangle(20, 40, 60, 72, fill=color, outline="#333", width=2)
        c.create_oval(35, 48, 45, 58, fill="#333", outline="#333")
        c.create_rectangle(38, 55, 42, 65, fill="#333", outline="#333")
        if shackle_open:
            c.create_arc(22, 15, 58, 50, start=0, extent=180, style="arc", outline="#333", width=3)
            c.create_line(58, 32, 58, 20, fill="#333", width=3)
            c.create_line(22, 32, 22, 42, fill="#333", width=3)
        else:
            c.create_arc(25, 15, 55, 45, start=0, extent=180, style="arc", outline="#333", width=3)
            c.create_line(25, 30, 25, 42, fill="#333", width=3)
            c.create_line(55, 30, 55, 42, fill="#333", width=3)

    def _animate_unlock(self):
        self._draw_padlock(color="#4CAF50", shackle_open=True)
        self.status_label.config(text=f"\u2713 {t('auth.unlock')}", fg="#4CAF50")
        self._pulse_count = 0
        self._pulse_unlock()

    def _pulse_unlock(self):
        colors = ["#4CAF50", "#81C784", "#4CAF50"]
        if self._pulse_count < len(colors):
            self._draw_padlock(color=colors[self._pulse_count], shackle_open=True)
            self._pulse_count += 1
            self.dialog.after(300, self._pulse_unlock)
        else:
            self.dialog.after(600, self.dialog.destroy)

    def _animate_lock(self):
        self._draw_padlock(color="#F44336", shackle_open=False)
        self.status_label.config(text=f"\u2717 {t('auth.lock')}", fg="#F44336")
        self._pulse_count = 0
        self._shake_count = 0
        self._pulse_lock()

    def _pulse_lock(self):
        colors = ["#F44336", "#EF9A9A", "#F44336"]
        if self._pulse_count < len(colors):
            self._draw_padlock(color=colors[self._pulse_count], shackle_open=False)
            self._pulse_count += 1
            self.dialog.after(300, self._pulse_lock)
        else:
            self._do_shake()

    def _do_shake(self):
        if self._shake_count < 6:
            geom = self.dialog.geometry()
            plus_idx = geom.index("+")
            coords = geom[plus_idx:]
            parts = coords.split("+")
            x_pos = int(parts[1])
            y_pos = int(parts[2])
            offset = 5 if self._shake_count % 2 == 0 else -5
            self.dialog.geometry(f"+{x_pos + offset}+{y_pos}")
            self._shake_count += 1
            self.dialog.after(60, self._do_shake)
        else:
            self.dialog.after(800, self._close_denied)

    @staticmethod
    def _nuke_data_db():
        """Force-delete keepers_orchestrator_data.db, retrying after GC if the file is locked."""
        if not os.path.exists(DATA_DB_FILE):
            return
        gc.collect()
        for attempt in range(5):
            try:
                os.remove(DATA_DB_FILE)
                return
            except PermissionError:
                time.sleep(0.3)
        try:
            import ctypes
            ctypes.windll.kernel32.MoveFileExW(DATA_DB_FILE, None, 4)
        except Exception:
            pass

    def _close_denied(self):
        self._nuke_data_db()
        messagebox.showerror(t("auth.lock"), t("auth.lock"))
        self.dialog.destroy()
        sys.exit()

    def _get_proxies(self):
        proxy_conf = self.config.get("proxy", {})
        if proxy_conf.get("enabled") and proxy_conf.get("url"):
            url = proxy_conf["url"]
            user = proxy_conf.get("username", "")
            pwd = proxy_conf.get("password", "")
            if user and pwd:
                scheme, rest = url.split("://", 1)
                return {"http": f"{scheme}://{user}:{pwd}@{rest}",
                        "https": f"{scheme}://{user}:{pwd}@{rest}"}
            return {"http": url, "https": url}
        return None

    def _check_update(self):
        """Background thread: check GitHub, auto-download & apply if new version."""
        txt = f"v{APP_VERSION}"
        fg = "gray"
        tag = None
        try:
            session = requests.Session()
            proxies = self._get_proxies()
            if proxies:
                session.proxies.update(proxies)
            api_url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
            resp = session.get(api_url, timeout=5)
            if resp.status_code != 200:
                self._update_queue.put((f"v{APP_VERSION}  ✓ Latest", "green", None))
                return
            data = resp.json()
            tag = str(data.get("tag_name", "")).strip()
            curr_v = [int(x) for x in re.findall(r"\d+", APP_VERSION)]
            new_v = [int(x) for x in re.findall(r"\d+", tag)] if tag else curr_v
            if new_v <= curr_v:
                self._update_queue.put((f"v{APP_VERSION}  ✓ Latest", "green", None))
                return

            # New version — show "downloading" then try auto-update
            self._update_queue.put((f"v{APP_VERSION}  ⬆ {tag} downloading…", "orange", None))

            script_bytes = self._download_script(session, data, tag)
            if not script_bytes:
                html_url = data.get("html_url", "")
                self._update_queue.put((f"v{APP_VERSION}  ⬆ Update {tag}", "red", html_url))
                return

            # Apply update
            script_path = os.path.abspath(__file__)
            with open(script_path, "rb") as f:
                current_bytes = f.read()
            if current_bytes == script_bytes:
                self._update_queue.put((f"v{APP_VERSION}  ✓ Latest", "green", None))
                return

            tmp_path = script_path + ".new"
            backup_path = script_path + ".bak"
            try:
                with open(tmp_path, "wb") as f:
                    f.write(script_bytes)
                shutil.copy2(script_path, backup_path)
                os.replace(tmp_path, script_path)
            finally:
                if os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except Exception:
                        pass

            # Signal main thread: update installed, ask to restart
            self._update_queue.put(("__installed__", tag, None))

        except Exception:
            self._update_queue.put((f"v{APP_VERSION}", "gray", None))

    @staticmethod
    def _download_script(session, release_data, tag):
        """Try to download the main script from release assets, zipball, or raw."""
        # 1) Release assets
        for asset in (release_data.get("assets", []) or []):
            name = str(asset.get("name", "")).lower()
            dl_url = asset.get("browser_download_url", "")
            if dl_url and "keepers_orchestrator" in name and \
               (name.endswith(".pyw") or name.endswith(".py")):
                r = session.get(dl_url, timeout=30)
                if r.status_code == 200 and b"class QBitAdderApp" in r.content:
                    return r.content
        # 2) Zipball
        zip_url = release_data.get("zipball_url", "")
        if zip_url:
            try:
                r = session.get(zip_url, timeout=45)
                if r.status_code == 200:
                    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                        for member in zf.namelist():
                            low = member.lower()
                            if low.endswith(".pyw") or low.endswith(".py"):
                                if "keepers_orchestrator" in low:
                                    payload = zf.read(member)
                                    if b"class QBitAdderApp" in payload:
                                        return payload
            except Exception:
                pass
        # 3) Raw source
        for ref in ([tag] if tag else []) + ["main", "master"]:
            try:
                r = session.get(
                    f"https://raw.githubusercontent.com/{GITHUB_REPO}/{ref}/keepers_orchestrator.pyw",
                    timeout=20)
                if r.status_code == 200 and b"class QBitAdderApp" in r.content:
                    return r.content
            except Exception:
                continue
        return None

    def _poll_update(self):
        try:
            txt, fg_or_tag, click_url = self._update_queue.get_nowait()
            if txt == "__installed__":
                # Update was applied — ask to restart
                tag = fg_or_tag
                self._ver_label.config(text=f"v{APP_VERSION} → {tag} ✓", fg="green")
                if messagebox.askyesno("Update Installed",
                                       f"Updated {APP_VERSION} → {tag}\n\nRestart now?",
                                       parent=self.dialog):
                    script_path = os.path.abspath(__file__)
                    subprocess.Popen([sys.executable, script_path],
                                     cwd=os.path.dirname(script_path))
                    self.dialog.destroy()
                    self.root.destroy()
                    return
            else:
                self._ver_label.config(text=txt, fg=fg_or_tag,
                                       cursor="hand2" if click_url else "")
                if click_url:
                    self._ver_label.bind("<Button-1>", lambda e: webbrowser.open(click_url))
                # If still downloading, keep polling for next update
                if "downloading" in txt:
                    self.dialog.after(200, self._poll_update)
        except queue.Empty:
            self.dialog.after(200, self._poll_update)


class QBitAdderApp:
    def __init__(self, root):
        self.root = root
        self.root.title(t("app.title"))
        # Center on usable work area (excludes taskbar)
        _ww, _wh = 1300, 900
        try:
            import ctypes
            from ctypes import wintypes
            _rect = wintypes.RECT()
            ctypes.windll.user32.SystemParametersInfoW(0x0030, 0, ctypes.byref(_rect), 0)
            _aw = _rect.right - _rect.left
            _ah = _rect.bottom - _rect.top
            _ox, _oy = _rect.left, _rect.top
        except Exception:
            _aw = self.root.winfo_screenwidth()
            _ah = self.root.winfo_screenheight()
            _ox, _oy = 0, 0
        _wh = min(_wh, _ah)
        _wx = max(0, _ox + (_aw - _ww) // 2)
        _wy = max(0, _oy + (_ah - _wh) // 2)
        self.root.geometry(f"{_ww}x{_wh}+{_wx}+{_wy}")

        # Global Menu Bar
        self._menubar = tk.Menu(self.root)
        self.root.config(menu=self._menubar)

        self._help_menu = tk.Menu(self._menubar, tearoff=0)
        self._menubar.add_cascade(label=t("menu.help"), menu=self._help_menu)
        self._help_menu.add_command(label=t("menu.help.docs"), command=self._show_help)

        # Custom progress bar styling
        style = ttk.Style()
        style.theme_use('clam')

        # Override layout to remove bouncing pbar animation
        style.layout("green.Horizontal.TProgressbar",
            [("Horizontal.Progressbar.trough", {"children":
                [("Horizontal.Progressbar.pbar", {"side": "left", "sticky": "ns"})],
                "sticky": "nswe"})])
        style.configure("green.Horizontal.TProgressbar",
            troughcolor="#e0e0e0", background="#4CAF50",
            darkcolor="#388E3C", lightcolor="#66BB6A",
            bordercolor="#bdbdbd", thickness=22)

        style.layout("blue.Horizontal.TProgressbar",
            [("Horizontal.Progressbar.trough", {"children":
                [("Horizontal.Progressbar.pbar", {"side": "left", "sticky": "ns"})],
                "sticky": "nswe"})])
        style.configure("blue.Horizontal.TProgressbar",
            troughcolor="#e0e0e0", background="#2196F3",
            darkcolor="#1976D2", lightcolor="#42A5F5",
            bordercolor="#bdbdbd", thickness=22)

        self.config = self.load_config()
        # Initialize language from config
        global _current_lang
        _current_lang = self.config.get("language", "en")
        self.db_manager = DatabaseManager(DATA_DB_FILE)
        self.hash_db_manager = HashDatabaseManager(HASHES_DB_FILE)
        self.selected_files = [] # List of file paths
        self.selected_folder_path = None
        self.stop_event = threading.Event()
        self.running_event = threading.Event()
        self.running_event.set()
        self.github_update_lock = threading.Lock()

        # Bind Universal Copy behavior
        self.root.bind_class("Treeview", "<Control-c>", self._copy_treeview_selection)
        self.root.bind_class("Treeview", "<Button-3>", self._on_treeview_right_click)
        
        # Bind Tab Navigation Hotkeys
        for i in range(1, 10):
            self.root.bind(f"<Control-Key-{i}>", lambda e, idx=i-1: self.notebook.select(idx))
        self.root.bind("<Control-Key-0>", lambda e: self.notebook.select(9))
        
        # Bind Global Action trigger
        self.root.bind("<F5>", self._handle_f5)

        self.is_initializing = True

        # UI Setup
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill="both", expand=True, padx=5, pady=5)

        self.dashboard_tab = tk.Frame(self.notebook)
        self.adder_tab = tk.Frame(self.notebook)
        self.keepers_tab = tk.Frame(self.notebook) # New Tab
        self.updater_tab = tk.Frame(self.notebook)
        self.remover_tab = tk.Frame(self.notebook)
        self.repair_tab = tk.Frame(self.notebook)
        self.mover_tab = tk.Frame(self.notebook)
        self.scanner_tab = tk.Frame(self.notebook)
        self.bitrot_tab = tk.Frame(self.notebook)
        self.settings_tab = tk.Frame(self.notebook)

        self.notebook.add(self.dashboard_tab, text=t("tab.dashboard"))
        self.notebook.add(self.adder_tab, text=t("tab.add_torrents"))
        self.notebook.add(self.keepers_tab, text=t("tab.keepers"))
        self.notebook.add(self.updater_tab, text=t("tab.update_torrents"))
        self.notebook.add(self.remover_tab, text=t("tab.remove_torrents"))
        self.notebook.add(self.repair_tab, text=t("tab.repair_categories"))
        self.notebook.add(self.mover_tab, text=t("tab.move_torrents"))
        self.notebook.add(self.scanner_tab, text=t("tab.folder_scanner"))
        self.notebook.add(self.bitrot_tab, text=t("tab.bitrot_scanner"))
        self.notebook.add(self.settings_tab, text=t("tab.settings"))

        # --- Torrent List Cache (per client) ---
        # Structure: {client_name: {"torrents": [...], "timestamp": float}}
        self.torrent_cache = {}
        self._cache_load_from_disk()

        # Dashboard state
        self.dashboard_refresh_active = False
        self._dash_cat_data = []
        self._dash_stor_data = []
        self.create_dashboard_ui()

        self.create_adder_ui()

        # Initialize Category Manager (needs log from adder_ui)
        # Pass callback to save keys to main config and callback for proxies
        self.cat_manager = CategoryManager(self.log, self.save_user_keys, self.get_requests_proxies)
        
        # KEY MIGRATION: Check if keys exist in cache and move them to config
        self.migrate_keys_from_cache()

        # Updater tab state
        self.updater_scanning = False
        self.updater_scan_results = []
        self.updater_qbit_session = None
        self.updater_selected_client = None
        self.updater_stop_event = threading.Event()
        self.updater_only_errored = tk.BooleanVar(value=True)
        self.create_updater_ui()

        # Remover tab state (New)
        self.remover_selected_client = None
        self.create_remover_ui()

        # Repair tab state
        self.repair_scanning = False
        self.repair_scan_results = []
        self.repair_selected_client = None
        self.repair_stop_event = threading.Event()
        self.create_repair_ui()

        # Keepers tab state
        self.keepers_stop_event = threading.Event()
        self.keepers_scan_active = False
        
        # Connection Status states and lock
        self.status_lock = threading.Lock()
        self.status_data = {
            "proxy": "gray", # gray, red, yellow, green
            "rutracker": "gray",
            "client": "gray"
        }
        self.client_statuses = [] # list of colors for each client
        self.status_loop_active = False

        # PM inbox state
        self.pm_unread_count = 0
        self.pm_last_known_ids = set()
        self.pm_poll_interval = self.config.get("pm_poll_interval_sec", 300)
        self.pm_poll_active = False
        self.pm_scraper = None
        self._pm_toast_available = True
        self._pm_current_message = None
        self._pm_current_folder = "inbox"
        self._pm_window = None

        self._create_keepers_sub_notebook()

        # Auto Keeper state
        self.ak_stop_event = threading.Event()
        self.ak_scan_active = False
        self.ak_distribution_plan = []
        self.ak_client_space = {}

        # Mover tab state
        self.mover_all_torrents = []
        self.mover_categories = {}  # {cat_name: [torrent_dicts]}
        self.mover_disk_list = []   # [{path: str, free: int, total: int, current_size: int}]
        self.mover_balance_plan = [] # [{hash, name, size, uploaded, from_path, to_path}]
        self.mover_selected_client = None
        self.mover_stop_event = threading.Event()
        self.mover_busy = False
        # Resume state for category mover
        self.mover_cat_remaining = []    # Torrents still to move after stop
        self.mover_cat_last_params = {}  # {new_root, cat_name, create_cat, keep_id, strip_id, create_id, hash_to_topic}
        self.create_mover_ui()

        # Folder Scanner tab state
        self.scanner_scanning = False
        self.scanner_scan_results = []
        self.scanner_selected_client = None
        self.scanner_stop_event = threading.Event()
        self.create_scanner_ui()

        # Bitrot Scanner tab state
        self.bitrot_scanning = False
        self.bitrot_scan_results = []
        self.bitrot_selected_client = None
        self.bitrot_stop_event = threading.Event()
        self.create_bitrot_ui()

        self.create_settings_ui()
        
        # Start background status checker
        self.status_loop_active = True
        threading.Thread(target=self._status_check_loop, daemon=True).start()
        
        # Search Tab (New)
        self.search_tab = tk.Frame(self.notebook)
        self.notebook.add(self.search_tab, text=t("tab.search_torrents"))
        self.create_search_ui()
        
        # Temp directory for downloads
        self.temp_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp_torrents")
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir)

        self.is_initializing = False
        self.trigger_status_check()

        # Start PM polling
        self._pm_start_polling()

        # System tray icon (must be after is_initializing = False)
        self._tray_icon = None
        self._tray_setup()

        # Auto-scan when switching to Update tab
        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)

        status_frame = tk.Frame(self.root)
        status_frame.pack(side="bottom", fill="x")

        self.status_bar = tk.Label(status_frame, text=t("status.ready"), bd=1, relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.pack(side="left", fill="x", expand=True)

        self.pm_indicator = tk.Label(
            status_frame, text="  PM  ", bd=1, relief=tk.RAISED,
            cursor="hand2", bg="#e0e0e0", fg="gray", padx=4
        )
        self.pm_indicator.pack(side="right", padx=2)
        self.pm_indicator.bind("<Button-1>", lambda e: self._pm_open_inbox_dialog())

        # Apply saved theme and language (after all UI is built)
        self.apply_theme()
        self.apply_language()

        # Start Category Manager (Auto-fetch if needed)
        threading.Thread(target=self._initial_category_fetch, daemon=True).start()

    def _status_check_loop(self):
        """Runs every 60 seconds to check connections."""
        while self.status_loop_active:
            self._run_all_status_checks()
            # Wait 60 seconds, checking stop flag periodically
            for _ in range(60):
                if not self.status_loop_active:
                    break
                time.sleep(1)

    def trigger_status_check(self):
        """Manually trigger a check (e.g., after saving settings)."""
        threading.Thread(target=self._run_all_status_checks, daemon=True).start()
        
    def _update_ui_status(self, key, color):
        """Thread-safe update of the traffic light canvases."""
        with self.status_lock:
            self.status_data[key] = color

        # Update tray icon color to reflect overall health
        statuses = list(self.status_data.values())
        if "red" in statuses:
            tray_color = "#f44336"
        elif all(s == "gray" for s in statuses):
            tray_color = "#9e9e9e"
        elif "green" in statuses:
            tray_color = "#4caf50"
        else:
            tray_color = "#ff9800"
        self._tray_update_icon(tray_color)

        def _update():
            try:
                if key == "proxy" and hasattr(self, 'canvas_proxy_status'):
                    self.canvas_proxy_status.itemconfig(self.oval_proxy_status, fill=color)
                elif key == "rutracker" and hasattr(self, 'canvas_rt_status'):
                    self.canvas_rt_status.itemconfig(self.oval_rt_status, fill=color)
                elif key == "client" and hasattr(self, 'canvas_client_status'):
                    self.canvas_client_status.itemconfig(self.oval_client_status, fill=color)
            except tk.TclError:
                pass # Window closed
        self.root.after(0, _update)

    def _run_all_status_checks(self):
        # 1. Proxy Check
        proxy_enabled = self.config.get("proxy", {}).get("enabled", False)
        if not proxy_enabled:
            self._update_ui_status("proxy", "gray")
        else:
            self._update_ui_status("proxy", "yellow")
            try:
                proxies = self.get_requests_proxies()
                # Use a widely accessible, lightweight endpoint
                resp = requests.get("https://1.1.1.1", proxies=proxies, timeout=10)
                if resp.status_code == 200:
                    self._update_ui_status("proxy", "green")
                else:
                    self._update_ui_status("proxy", "red")
            except Exception:
                self._update_ui_status("proxy", "red")

        # 2. Rutracker Check
        rt_user = self.config.get("rutracker_auth", {}).get("username", "")
        rt_pass = self.config.get("rutracker_auth", {}).get("password", "")
        if not rt_user or not rt_pass:
            self._update_ui_status("rutracker", "gray")
        else:
            self._update_ui_status("rutracker", "yellow")
            try:
                # Use the category manager credentials check mechanism or simply hit index
                session = requests.Session()
                proxies = self.get_requests_proxies()
                if proxies:
                    session.proxies.update(proxies)
                
                # Check auth by attempting to load the profile page
                # Actually, simpler: just try to post login and see if bbsession cookie is set
                login_data = {
                    "login_username": rt_user.encode("windows-1251"),
                    "login_password": rt_pass.encode("windows-1251"),
                    "login": "Вход"
                }
                resp = session.post(
                    "https://rutracker.org/forum/login.php",
                    data=login_data,
                    timeout=15
                )
                if 'bb_session' in session.cookies:
                     self._update_ui_status("rutracker", "green")
                else:
                     self._update_ui_status("rutracker", "red")
            except Exception:
                self._update_ui_status("rutracker", "red")

        # 3. Client Check for ALL clients
        statuses = []
        for i, client in enumerate(self.config.get("clients", [])):
            if not client.get("enabled", False):
                statuses.append("gray")
                continue
            url = client.get("url", "").rstrip("/")
            if not url:
                statuses.append("gray")
                continue
            
            # Use global auth or client auth
            if client.get("use_global_auth", True):
                auth = (self.config["global_auth"]["username"], self.config["global_auth"]["password"])
            else:
                auth = (client.get("username", ""), client.get("password", ""))

            try:
                # We can't update UI yellow here easily for individual list items without complexities, 
                # so we just do the check synchronously in this thread per client.
                resp = requests.get(f"{url}/api/v2/app/version", auth=auth, proxies={"http": None, "https": None}, timeout=5)
                if resp.status_code == 200:
                    statuses.append("green")
                else:
                    statuses.append("red")
            except Exception:
                statuses.append("red")
        
        with self.status_lock:
            self.client_statuses = statuses

        def _update_listbox_and_canvas():
            try:
                # Update listbox
                selection = self.client_listbox.curselection()
                self.client_listbox.delete(0, tk.END)
                for i in range(len(self.config["clients"])):
                    c = self.config["clients"][i]
                    client_name = c.get("name", "Unnamed")
                    enabled = c.get("enabled", False)
                    c_status = self.client_statuses[i] if i < len(self.client_statuses) else "gray"
                    prefix = "●" if enabled else "○"
                    self.client_listbox.insert(tk.END, f"{prefix} {client_name}")
                    if not enabled:
                        self.client_listbox.itemconfig(i, {'fg': '#bbbbbb'})
                    elif c_status == "green": self.client_listbox.itemconfig(i, {'fg': '#00b300'})
                    elif c_status == "red": self.client_listbox.itemconfig(i, {'fg': '#e60000'})
                    elif c_status == "yellow": self.client_listbox.itemconfig(i, {'fg': '#cccc00'})
                    else: self.client_listbox.itemconfig(i, {'fg': 'gray'})
                    
                if selection:
                    self.client_listbox.selection_set(selection[0])
                    
                # Update details canvas if selected
                if getattr(self, 'current_client_index', -1) != -1 and self.current_client_index < len(self.client_statuses):
                    selected_c_status = self.client_statuses[self.current_client_index]
                    self._update_ui_status("client", selected_c_status)
                else:
                    self._update_ui_status("client", "gray")
            except tk.TclError:
                pass
                
        self.root.after(0, _update_listbox_and_canvas)

    # --- Unified progress helpers ---

    def _update_progress(self, progressbar, label, current, total, phase, start_time=None):
        """Thread-safe progress update for any tab with optional elapsed/ETA."""
        pct = int(current / total * 100) if total > 0 else 0
        text = f"{phase}... {current}/{total} ({pct}%)"
        if start_time is not None:
            elapsed = time.time() - start_time
            elapsed_str = self._format_elapsed(elapsed)
            text += f"  [{elapsed_str}"
            if pct > 5 and elapsed > 3 and current > 0:
                rate = current / elapsed
                if rate > 0:
                    remaining = (total - current) / rate
                    text += f" | ~{self._format_elapsed(remaining)} left"
            text += "]"
        def _do(p=pct, t=text):
            progressbar.configure(value=p)
            label.config(text=t)
        self.root.after(0, _do)

    @staticmethod
    def _format_elapsed(seconds):
        """Format seconds into human-readable string."""
        m, s = divmod(int(seconds), 60)
        if m > 0:
            return f"{m}m {s}s"
        return f"{s}s"

    def save_user_keys(self, keys):
        """Callback to save user keys to global config."""
        self.config["user_keys"] = keys
        self.save_config()
        # Also update UI elements
        self.update_cats_ui()

    def migrate_keys_from_cache(self):
        """Move keys from categories cache to config if found."""
        cache_keys = self.cat_manager.cache.get("user_keys")
        if cache_keys:
            self.log("Migrating user keys from category cache to main config...")
            # Only overwrite if config keys are empty or force? 
            # Let's overwrite / set if they exist in cache.
            self.config["user_keys"] = cache_keys.copy()
            self.save_config()
            
            # Remove from cache and save cache
            del self.cat_manager.cache["user_keys"]
            self.cat_manager.save_cache()
            self.log("Keys migrated and removed from cache.")

    def _get_rutracker_creds(self):
        rt_auth = self.config.get("rutracker_auth", {})
        user = rt_auth.get("username", "")
        pwd = rt_auth.get("password", "")
        if user and pwd:
            return user, pwd
        return None, None

    # ======== System Tray Methods ========

    def _tray_make_icon(self, color="#9e9e9e"):
        """Create a 64x64 colored circle image for the tray icon."""
        img = PILImage.new("RGBA", (64, 64), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)
        # Outer circle (main)
        draw.ellipse([4, 4, 60, 60], fill=color)
        # Small inner highlight for depth
        draw.ellipse([16, 10, 34, 26], fill="#ffffff44")
        return img

    def _tray_setup(self):
        """Create and run the system tray icon. Gracefully skips if pystray unavailable."""
        if not self.config.get("tray_enabled", True):
            return
        try:
            import pystray
            from pystray import MenuItem as Item
        except ImportError:
            self.log("pystray not installed — system tray disabled. Run: pip install pystray")
            return

        img = self._tray_make_icon()
        menu = pystray.Menu(
            Item("Show Keepers Orchestrator", self._tray_show_window, default=True),
            pystray.Menu.SEPARATOR,
            Item("Exit", self._tray_quit),
        )
        self._tray_icon = pystray.Icon(
            "Keepers Orchestrator",
            img,
            "Keepers Orchestrator",
            menu,
        )
        self._tray_icon.run_detached()

        # Override close button: hide to tray instead of quitting
        if self.config.get("minimize_to_tray", True):
            self.root.protocol("WM_DELETE_WINDOW", self._on_window_close)

    def _on_window_close(self):
        """Called when the user clicks the window X button."""
        if self.config.get("minimize_to_tray", True) and self._tray_icon is not None:
            self.root.withdraw()  # Hide window, keep running
        else:
            self._tray_quit()

    def _tray_show_window(self, icon=None, item=None):
        """Restore the main window from the tray (called from pystray bg thread)."""
        self.root.after(0, self._do_show_window)

    def _do_show_window(self):
        """Show and raise the main window (must run on main thread)."""
        self.root.deiconify()
        self.root.lift()
        self.root.focus_force()

    def _tray_quit(self, icon=None, item=None):
        """Clean shutdown: stop background loops, stop tray, destroy window."""
        self.status_loop_active = False
        self.pm_poll_active = False
        if self._tray_icon is not None:
            try:
                self._tray_icon.stop()
            except Exception:
                pass
        self.root.after(0, self.root.destroy)

    def _tray_notify(self, title, body):
        """Send a Windows toast notification. Central dispatcher for all events."""
        if not self.config.get("tray_notifications_enabled", True):
            return
        if not getattr(self, '_pm_toast_available', True):
            return
        try:
            from winotify import Notification
            toast = Notification(
                app_id="Keepers Orchestrator",
                title=title,
                msg=body,
                duration="short",
            )
            toast.show()
        except ImportError:
            self._pm_toast_available = False
        except Exception as exc:
            self.log(f"Notification error: {exc}")

    def _tray_update_icon(self, color):
        """Change the tray icon color to reflect overall connection status."""
        if getattr(self, '_tray_icon', None) is None:
            return
        try:
            self._tray_icon.icon = self._tray_make_icon(color)
        except Exception:
            pass

    # ======== PM Inbox Methods ========

    def _pm_check_winotify(self):
        """Check if winotify is available. Returns True if importable."""
        try:
            import winotify
            return True
        except ImportError:
            return False

    def _pm_start_polling(self):
        """Start PM background polling thread if credentials available."""
        if self.pm_poll_active:
            return
        if not self.config.get("pm_polling_enabled", True):
            return
        user, pwd = self._get_rutracker_creds()
        if not user or not pwd:
            return

        # Check winotify on startup if toast is enabled
        if self.config.get("pm_toast_enabled", False):
            if not self._pm_check_winotify():
                self._pm_toast_available = False
                self.log("Warning: Windows notifications enabled but 'winotify' is not installed. "
                         "Install it with: pip install winotify")

        self.pm_scraper = RutrackerPMScraper(
            session_provider=lambda: self.cat_manager.session,
            log_func=self.log
        )
        self.pm_poll_active = True
        threading.Thread(target=self._pm_check_loop, daemon=True).start()

    def _pm_check_loop(self):
        """Background: polls inbox every pm_poll_interval seconds."""
        while self.pm_poll_active:
            self._pm_do_check()
            for _ in range(self.pm_poll_interval):
                if not self.pm_poll_active:
                    break
                time.sleep(1)

    def _pm_do_check(self):
        """Single inbox check iteration."""
        try:
            self.root.after(0, lambda: self._pm_update_indicator("#cccc00", " PM... "))

            if 'bb_session' not in self.cat_manager.session.cookies.get_dict():
                user, pwd = self._get_rutracker_creds()
                if user and pwd:
                    self.cat_manager.login(user, pwd)
                else:
                    self.root.after(0, lambda: self._pm_update_indicator("#e0e0e0", "  PM  "))
                    return

            messages = self.pm_scraper.fetch_inbox()

            if messages is None:
                user, pwd = self._get_rutracker_creds()
                if user and pwd and self.cat_manager.login(user, pwd):
                    messages = self.pm_scraper.fetch_inbox()
                if messages is None:
                    self.root.after(0, lambda: self._pm_update_indicator("#fdae62", " PM! ", "#d20903"))
                    return

            unread = [m for m in messages if m["is_unread"]]
            self.pm_unread_count = len(unread)

            current_ids = {m["msg_id"] for m in messages}
            new_ids = current_ids - self.pm_last_known_ids
            new_unread = [m for m in unread if m["msg_id"] in new_ids]
            self.pm_last_known_ids = current_ids

            if self.pm_unread_count > 0:
                text = f" PM({self.pm_unread_count}) "
                self.root.after(0, lambda t=text: self._pm_update_indicator("#ffd9b2", t, "#d20903"))
            else:
                self.root.after(0, lambda: self._pm_update_indicator("#90EE90", "  PM  "))

            if new_unread and self.config.get("pm_toast_enabled", True):
                self._pm_send_toast(new_unread)

        except Exception as e:
            self.log(f"PM check error: {e}")
            self.root.after(0, lambda: self._pm_update_indicator("#e0e0e0", "  PM  "))

    def _pm_update_indicator(self, bg_color, text, fg_color="black"):
        """Thread-safe indicator update."""
        try:
            self.pm_indicator.config(text=text, bg=bg_color, fg=fg_color)
        except tk.TclError:
            pass

    def _pm_send_toast(self, new_messages):
        """Windows toast notification for new PMs — delegates to _tray_notify."""
        if not self._pm_toast_available:
            return
        if len(new_messages) == 1:
            msg = new_messages[0]
            title = f"New PM from {msg['sender']}"
            body = msg['subject']
        else:
            title = f"{len(new_messages)} new private messages"
            senders = ", ".join(m['sender'] for m in new_messages[:3])
            body = f"From: {senders}"
            if len(new_messages) > 3:
                body += f" (+{len(new_messages) - 3} more)"
        self._tray_notify(title, body)

    def _pm_open_inbox_dialog(self):
        """Open the PM inbox popup dialog."""
        user, pwd = self._get_rutracker_creds()
        if not user or not pwd:
            messagebox.showwarning("Private Messages",
                "Please configure Rutracker credentials in Settings first.")
            return

        if self._pm_window and self._pm_window.winfo_exists():
            self._pm_window.lift()
            self._pm_window.focus_force()
            return

        if not self.pm_scraper:
            self.pm_scraper = RutrackerPMScraper(
                session_provider=lambda: self.cat_manager.session,
                log_func=self.log
            )

        win = tk.Toplevel(self.root)
        win.title(f"Private Messages - {PM_FOLDERS.get(self._pm_current_folder, 'Inbox')}")
        win.geometry("850x600")
        win.transient(self.root)
        self._pm_window = win

        # Folder button bar
        folder_bar = tk.Frame(win)
        folder_bar.pack(fill="x", padx=10, pady=(5, 0))

        self._pm_folder_buttons = {}
        for folder_key, folder_label in PM_FOLDERS.items():
            btn = tk.Button(
                folder_bar, text=folder_label,
                command=lambda f=folder_key: self._pm_switch_folder(f),
                relief="sunken" if folder_key == self._pm_current_folder else "raised",
                width=14
            )
            btn.pack(side="left", padx=2)
            self._pm_folder_buttons[folder_key] = btn

        # Top bar
        top_bar = tk.Frame(win)
        top_bar.pack(fill="x", padx=10, pady=5)

        self._pm_status_label = tk.Label(top_bar, text="", fg="gray")
        self._pm_status_label.pack(side="left", padx=(0, 15))
        tk.Button(top_bar, text="Refresh", command=self._pm_refresh_inbox).pack(side="right")
        tk.Button(top_bar, text="Delete Read", fg="#d20903",
                  command=self._pm_delete_all_read).pack(side="right", padx=(0, 5))
        tk.Button(top_bar, text="New Message", command=self._pm_open_compose_dialog).pack(side="right", padx=(0, 5))

        # Message list
        list_frame = tk.Frame(win)
        list_frame.pack(fill="both", expand=True, padx=10, pady=(0, 5))

        cols = ("msg_id", "subject", "sender", "date", "status")
        self._pm_tree = ttk.Treeview(list_frame, columns=cols, show="headings",
                                      selectmode="extended", height=8)

        self._pm_tree.heading("msg_id", text="ID")
        self._pm_tree.heading("subject", text="Subject")
        self._pm_tree.heading("sender", text="From")
        self._pm_tree.heading("date", text="Date")
        self._pm_tree.heading("status", text="Status")

        self._pm_tree.column("msg_id", width=70, stretch=False)
        self._pm_tree.column("subject", width=350)
        self._pm_tree.column("sender", width=120)
        self._pm_tree.column("date", width=130, stretch=False)
        self._pm_tree.column("status", width=70, stretch=False)

        self._pm_tree.tag_configure("unread", foreground="black", background="#ffd9b2", font=("Segoe UI", 9, "bold"))
        self._pm_tree.tag_configure("read", foreground="gray")
        self._pm_tree.tag_configure("keeper_unread", foreground="black", background="#b2ffd9", font=("Segoe UI", 9, "bold"))
        self._pm_tree.tag_configure("keeper_read", foreground="#006633", background="#d9ffec")

        # Selection highlight uses the contour color for unread emphasis
        pm_style = ttk.Style()
        pm_style.map("PM.Treeview",
                      background=[("selected", "#fdae62")],
                      foreground=[("selected", "black")])
        self._pm_tree.configure(style="PM.Treeview")

        scroll = ttk.Scrollbar(list_frame, orient="vertical", command=self._pm_tree.yview)
        self._pm_tree.configure(yscrollcommand=scroll.set)
        scroll.pack(side="right", fill="y")
        self._pm_tree.pack(side="left", fill="both", expand=True)

        self._pm_tree.bind("<<TreeviewSelect>>", self._pm_on_message_select)
        self._pm_tree.bind("<Double-1>", self._pm_on_message_select)
        self._pm_tree.bind("<Delete>", lambda e: self._pm_delete_selected())

        # Message preview pane
        preview_frame = tk.LabelFrame(win, text="Message", padx=5, pady=5)
        preview_frame.pack(fill="both", expand=True, padx=10, pady=(0, 5))

        header = tk.Frame(preview_frame)
        header.pack(fill="x")
        self._pm_preview_subject = tk.Label(header, text="", font=("Segoe UI", 10, "bold"), anchor="w")
        self._pm_preview_subject.pack(fill="x")
        self._pm_preview_meta = tk.Label(header, text="", fg="gray", anchor="w")
        self._pm_preview_meta.pack(fill="x")

        self._pm_preview_body = scrolledtext.ScrolledText(preview_frame, wrap="word",
                                                           height=10, state="disabled",
                                                           font=("Segoe UI", 10))
        self._pm_preview_body.pack(fill="both", expand=True, pady=5)

        btn_frame = tk.Frame(preview_frame)
        btn_frame.pack(fill="x")
        self._pm_reply_btn = tk.Button(btn_frame, text="Reply", command=self._pm_open_reply_dialog)
        self._pm_reply_btn.pack(side="left")
        tk.Button(btn_frame, text="Open in Browser",
                  command=self._pm_open_in_browser).pack(side="left", padx=10)
        self._pm_save_btn = tk.Button(btn_frame, text="Save", fg="#006633",
                  command=self._pm_save_selected)
        self._pm_save_btn.pack(side="left", padx=10)
        tk.Button(btn_frame, text="Delete", fg="#d20903",
                  command=self._pm_delete_selected).pack(side="right")

        self._pm_refresh_inbox()

    def _pm_switch_folder(self, folder):
        """Switch to a different PM folder and refresh."""
        self._pm_current_folder = folder

        # Update button visual state
        for key, btn in self._pm_folder_buttons.items():
            btn.config(relief="sunken" if key == folder else "raised")

        # Update window title
        self._pm_window.title(f"Private Messages - {PM_FOLDERS.get(folder, folder)}")

        # Update column header: "From" for inbox/savebox, "To" for sentbox/outbox
        if folder in ("sentbox", "outbox"):
            self._pm_tree.heading("sender", text="To")
        else:
            self._pm_tree.heading("sender", text="From")

        # Enable/disable buttons based on folder
        if folder in ("sentbox", "outbox"):
            self._pm_reply_btn.config(state="disabled")
        else:
            self._pm_reply_btn.config(state="normal")

        # Save button: disabled when already in savebox
        if folder == "savebox":
            self._pm_save_btn.config(state="disabled")
        else:
            self._pm_save_btn.config(state="normal")

        # Clear preview pane
        self._pm_current_message = None
        self._pm_preview_subject.config(text="")
        self._pm_preview_meta.config(text="")
        self._pm_preview_body.config(state="normal")
        self._pm_preview_body.delete("1.0", tk.END)
        self._pm_preview_body.config(state="disabled")

        # Refresh message list for this folder
        self._pm_refresh_inbox()

    def _pm_on_message_select(self, event=None):
        """Load selected message content."""
        sel = self._pm_tree.selection()
        if not sel:
            return
        vals = self._pm_tree.item(sel[0])["values"]
        # vals: (msg_id, subject, sender, date, status)
        msg_id = str(vals[0])
        inbox_meta = {
            "subject": str(vals[1]),
            "sender": str(vals[2]),
            "date": str(vals[3]),
        }

        label_prefix = "To" if self._pm_current_folder in ("sentbox", "outbox") else "From"
        self._pm_preview_subject.config(text="Loading...")
        self._pm_preview_meta.config(text=f"{label_prefix}: {inbox_meta['sender']}  |  Date: {inbox_meta['date']}")
        self._pm_preview_body.config(state="normal")
        self._pm_preview_body.delete("1.0", tk.END)
        self._pm_preview_body.config(state="disabled")
        threading.Thread(target=self._pm_load_message, args=(msg_id, inbox_meta), daemon=True).start()

    def _pm_load_message(self, msg_id, inbox_meta):
        """Background: fetch single message body. Uses inbox_meta for subject/sender/date."""
        folder = self._pm_current_folder
        try:
            msg = self.pm_scraper.fetch_message(msg_id, folder=folder)
            if msg is None:
                user, pwd = self._get_rutracker_creds()
                if user and pwd and self.cat_manager.login(user, pwd):
                    msg = self.pm_scraper.fetch_message(msg_id, folder=folder)

            if msg:
                # Use reliable metadata from inbox list, only body + form_token from the read page
                msg["subject"] = inbox_meta["subject"]
                msg["sender"] = inbox_meta["sender"]
                msg["date"] = inbox_meta["date"]
                self._pm_current_message = msg
                label_prefix = "To" if folder in ("sentbox", "outbox") else "From"
                def _update():
                    try:
                        self._pm_preview_subject.config(text=msg["subject"])
                        self._pm_preview_meta.config(
                            text=f"{label_prefix}: {msg['sender']}  |  Date: {msg['date']}")
                        self._pm_preview_body.config(state="normal")
                        self._pm_preview_body.delete("1.0", tk.END)
                        self._pm_preview_body.insert("1.0", msg["body_text"])
                        self._pm_preview_body.config(state="disabled")
                    except tk.TclError:
                        pass
                self.root.after(0, _update)
            else:
                self.root.after(0, lambda: self._pm_preview_subject.config(text="Failed to load message"))
        except Exception as e:
            self.root.after(0, lambda: self._pm_preview_subject.config(text=f"Error: {e}"))

    def _pm_refresh_inbox(self):
        """Refresh message list for the current folder."""
        self._pm_status_label.config(text="Loading...", fg="blue")
        folder = self._pm_current_folder
        threading.Thread(target=self._pm_refresh_inbox_thread, args=(folder,), daemon=True).start()

    def _pm_refresh_inbox_thread(self, folder="inbox"):
        """Background: refresh message list for the given folder."""
        try:
            if 'bb_session' not in self.cat_manager.session.cookies.get_dict():
                user, pwd = self._get_rutracker_creds()
                if user and pwd:
                    self.cat_manager.login(user, pwd)

            messages = self.pm_scraper.fetch_inbox(folder=folder)
            if messages is None:
                user, pwd = self._get_rutracker_creds()
                if user and pwd and self.cat_manager.login(user, pwd):
                    messages = self.pm_scraper.fetch_inbox(folder=folder)

            if messages is None:
                self.root.after(0, lambda: self._pm_status_label.config(
                    text="Login failed", fg="red"))
                return

            # Load keeper usernames for highlighting
            keeper_names = self.db_manager.get_all_keeper_usernames()

            def _update():
                # Discard stale data if user switched folders while loading
                if self._pm_current_folder != folder:
                    return
                try:
                    for item in self._pm_tree.get_children():
                        self._pm_tree.delete(item)

                    for msg in messages:
                        is_keeper = msg["sender"].lower() in keeper_names
                        if is_keeper:
                            tag = "keeper_unread" if msg["is_unread"] else "keeper_read"
                            status_text = "Keeper" if not msg["is_unread"] else "Keeper!"
                        else:
                            tag = "unread" if msg["is_unread"] else "read"
                            status_text = "Unread" if msg["is_unread"] else "Read"
                        self._pm_tree.insert("", "end", values=(
                            msg["msg_id"], msg["subject"], msg["sender"],
                            msg["date"], status_text
                        ), tags=(tag,))

                    unread_count = sum(1 for m in messages if m["is_unread"])
                    keeper_count = sum(1 for m in messages if m["sender"].lower() in keeper_names)
                    status_text = f"{len(messages)} messages ({unread_count} unread)"
                    if keeper_count:
                        status_text += f"  |  {keeper_count} from keepers"
                    self._pm_status_label.config(text=status_text, fg="black")
                except tk.TclError:
                    pass
            self.root.after(0, _update)
        except Exception as e:
            self.root.after(0, lambda: self._pm_status_label.config(
                text=f"Error: {e}", fg="red"))

    def _pm_open_reply_dialog(self):
        """Open reply composition dialog."""
        if not self._pm_current_message:
            messagebox.showinfo("Reply", "Select a message first.")
            return

        msg = self._pm_current_message

        reply_win = tk.Toplevel(self._pm_window)
        reply_win.title(f"Reply to: {msg['subject']}")
        reply_win.geometry("600x400")
        reply_win.transient(self._pm_window)

        tk.Label(reply_win, text=f"To: {msg['sender']}", anchor="w").pack(fill="x", padx=10, pady=5)

        subj_frame = tk.Frame(reply_win)
        subj_frame.pack(fill="x", padx=10)
        tk.Label(subj_frame, text="Subject:").pack(side="left")
        subj_entry = tk.Entry(subj_frame, width=50)
        subj_entry.pack(side="left", padx=5, fill="x", expand=True)

        subj_text = msg["subject"]
        if not subj_text.lower().startswith("re:"):
            subj_text = f"Re: {subj_text}"
        subj_entry.insert(0, subj_text)

        tk.Label(reply_win, text="Message:", anchor="w").pack(fill="x", padx=10, pady=(10, 0))
        body_text = original_scrolled_text(reply_win, wrap="word", height=15, font=("Segoe UI", 10))
        body_text.pack(fill="both", expand=True, padx=10, pady=5)

        btn_frame = tk.Frame(reply_win)
        btn_frame.pack(fill="x", padx=10, pady=5)

        send_btn = tk.Button(btn_frame, text="Send Reply")
        send_btn.pack(side="left")
        tk.Button(btn_frame, text="Cancel", command=reply_win.destroy).pack(side="right")

        reply_status = tk.Label(btn_frame, text="", fg="gray")
        reply_status.pack(side="left", padx=15)

        def _send():
            subject = subj_entry.get().strip()
            body = body_text.get("1.0", tk.END).strip()
            if not body:
                messagebox.showwarning("Reply", "Message body cannot be empty.")
                return

            send_btn.config(state="disabled")
            reply_status.config(text="Sending...", fg="blue")

            def _do_send():
                try:
                    success = self.pm_scraper.send_reply(
                        msg["msg_id"], subject, body)
                    if success:
                        def _on_success():
                            reply_status.config(text="Sent!", fg="green")
                            reply_win.after(1500, reply_win.destroy)
                        self.root.after(0, _on_success)
                    else:
                        def _on_fail():
                            reply_status.config(text="Send failed", fg="red")
                            send_btn.config(state="normal")
                        self.root.after(0, _on_fail)
                except Exception as e:
                    def _on_error():
                        reply_status.config(text=f"Error: {e}", fg="red")
                        send_btn.config(state="normal")
                    self.root.after(0, _on_error)

            threading.Thread(target=_do_send, daemon=True).start()

        send_btn.config(command=_send)

    def _pm_open_compose_dialog(self):
        """Open new message composition dialog."""
        compose_win = tk.Toplevel(self._pm_window)
        compose_win.title("New Message")
        compose_win.geometry("600x430")
        compose_win.transient(self._pm_window)

        # Recipient
        to_frame = tk.Frame(compose_win)
        to_frame.pack(fill="x", padx=10, pady=5)
        tk.Label(to_frame, text="To:").pack(side="left")
        to_entry = tk.Entry(to_frame, width=50)
        to_entry.pack(side="left", padx=5, fill="x", expand=True)

        # Subject
        subj_frame = tk.Frame(compose_win)
        subj_frame.pack(fill="x", padx=10)
        tk.Label(subj_frame, text="Subject:").pack(side="left")
        subj_entry = tk.Entry(subj_frame, width=50)
        subj_entry.pack(side="left", padx=5, fill="x", expand=True)

        # Message body
        tk.Label(compose_win, text="Message:", anchor="w").pack(fill="x", padx=10, pady=(10, 0))
        body_text = original_scrolled_text(compose_win, wrap="word", height=15, font=("Segoe UI", 10))
        body_text.pack(fill="both", expand=True, padx=10, pady=5)

        # Buttons
        btn_frame = tk.Frame(compose_win)
        btn_frame.pack(fill="x", padx=10, pady=5)

        send_btn = tk.Button(btn_frame, text="Send")
        send_btn.pack(side="left")
        tk.Button(btn_frame, text="Cancel", command=compose_win.destroy).pack(side="right")

        compose_status = tk.Label(btn_frame, text="", fg="gray")
        compose_status.pack(side="left", padx=15)

        to_entry.focus_set()

        def _send():
            recipient = to_entry.get().strip()
            subject = subj_entry.get().strip()
            body = body_text.get("1.0", tk.END).strip()
            if not recipient:
                messagebox.showwarning("New Message", "Recipient username cannot be empty.")
                return
            if not subject:
                messagebox.showwarning("New Message", "Subject cannot be empty.")
                return
            if not body:
                messagebox.showwarning("New Message", "Message body cannot be empty.")
                return

            send_btn.config(state="disabled")
            compose_status.config(text="Sending...", fg="blue")

            def _do_send():
                try:
                    success = self.pm_scraper.send_new_message(recipient, subject, body)
                    if success:
                        def _on_success():
                            compose_status.config(text="Sent!", fg="green")
                            compose_win.after(1500, compose_win.destroy)
                        self.root.after(0, _on_success)
                    else:
                        def _on_fail():
                            compose_status.config(text="Send failed", fg="red")
                            send_btn.config(state="normal")
                        self.root.after(0, _on_fail)
                except Exception as e:
                    def _on_error():
                        compose_status.config(text=f"Error: {e}", fg="red")
                        send_btn.config(state="normal")
                    self.root.after(0, _on_error)

            threading.Thread(target=_do_send, daemon=True).start()

        send_btn.config(command=_send)

    def _pm_open_in_browser(self):
        """Open current message in browser."""
        if self._pm_current_message:
            folder = self._pm_current_folder
            msg_id = self._pm_current_message["msg_id"]
            webbrowser.open(f"https://rutracker.org/forum/privmsg.php?folder={folder}&mode=read&p={msg_id}")

    def _pm_delete_all_read(self):
        """Delete all read (non-unread) messages in the current folder."""
        # Collect tree items with read tags
        read_items = []
        for item in self._pm_tree.get_children():
            tags = self._pm_tree.item(item)["tags"]
            if tags and tags[0] in ("read", "keeper_read"):
                read_items.append(item)

        if not read_items:
            messagebox.showinfo("Delete Read", "No read messages to delete.")
            return

        msg_ids = [str(self._pm_tree.item(item)["values"][0]) for item in read_items]
        count = len(read_items)

        if not messagebox.askyesno("Confirm Delete",
                f"Delete all {count} read message(s) in this folder?"):
            return

        self._pm_status_label.config(text=f"Deleting {count} read messages...", fg="blue")

        def _do_delete():
            try:
                deleted = self.pm_scraper.delete_messages(msg_ids, folder=self._pm_current_folder)
                def _after():
                    if deleted:
                        self._pm_status_label.config(
                            text=f"Deleted {deleted}/{count} read message(s). Refreshing...", fg="green")
                        # Clear preview if deleted message was being viewed
                        if self._pm_current_message and self._pm_current_message["msg_id"] in msg_ids:
                            self._pm_current_message = None
                            self._pm_preview_subject.config(text="")
                            self._pm_preview_meta.config(text="")
                            self._pm_preview_body.config(state="normal")
                            self._pm_preview_body.delete("1.0", tk.END)
                            self._pm_preview_body.config(state="disabled")
                        self._pm_refresh_inbox()
                    else:
                        self._pm_status_label.config(text="Delete failed", fg="red")
                self.root.after(0, _after)
            except Exception as e:
                self.root.after(0, lambda: self._pm_status_label.config(
                    text=f"Delete error: {e}", fg="red"))

        threading.Thread(target=_do_delete, daemon=True).start()

    def _pm_save_selected(self):
        """Save selected message(s) to savebox."""
        sel = self._pm_tree.selection()
        if not sel:
            messagebox.showinfo("Save", "Select one or more messages first.")
            return

        count = len(sel)
        msg_ids = [str(self._pm_tree.item(item)["values"][0]) for item in sel]
        subjects = [str(self._pm_tree.item(item)["values"][1]) for item in sel]

        if count == 1:
            confirm_text = f"Save message \"{subjects[0]}\" to Saved folder?"
        else:
            preview = "\n".join(f"  - {s}" for s in subjects[:5])
            if count > 5:
                preview += f"\n  ... and {count - 5} more"
            confirm_text = f"Save {count} messages to Saved folder?\n\n{preview}"

        if not messagebox.askyesno("Confirm Save", confirm_text):
            return

        self._pm_status_label.config(text="Saving...", fg="blue")

        def _do_save():
            try:
                saved = self.pm_scraper.save_messages(msg_ids, folder=self._pm_current_folder)
                if saved:
                    def _after():
                        self._pm_status_label.config(
                            text=f"Saved {saved} message(s). Refreshing...", fg="green")
                        self._pm_refresh_inbox()
                    self.root.after(0, _after)
                else:
                    self.root.after(0, lambda: self._pm_status_label.config(
                        text="Save failed", fg="red"))
            except Exception as e:
                self.root.after(0, lambda: self._pm_status_label.config(
                    text=f"Save error: {e}", fg="red"))

        threading.Thread(target=_do_save, daemon=True).start()

    def _pm_delete_selected(self):
        """Delete selected message(s) from inbox."""
        sel = self._pm_tree.selection()
        if not sel:
            messagebox.showinfo("Delete", "Select one or more messages first.")
            return

        count = len(sel)
        msg_ids = [str(self._pm_tree.item(item)["values"][0]) for item in sel]
        subjects = [str(self._pm_tree.item(item)["values"][1]) for item in sel]

        # Build confirmation text
        if count == 1:
            confirm_text = f"Delete message \"{subjects[0]}\"?"
        else:
            preview = "\n".join(f"  - {s}" for s in subjects[:5])
            if count > 5:
                preview += f"\n  ... and {count - 5} more"
            confirm_text = f"Delete {count} messages?\n\n{preview}"

        if not messagebox.askyesno("Confirm Delete", confirm_text):
            return

        self._pm_status_label.config(text="Deleting...", fg="blue")

        def _do_delete():
            try:
                deleted = self.pm_scraper.delete_messages(msg_ids, folder=self._pm_current_folder)
                if deleted:
                    def _after():
                        # Remove deleted rows from tree
                        for item in sel:
                            try:
                                self._pm_tree.delete(item)
                            except tk.TclError:
                                pass
                        self._pm_status_label.config(
                            text=f"Deleted {deleted} message(s). Refreshing...", fg="green")
                        # Clear preview if deleted message was being viewed
                        if self._pm_current_message and self._pm_current_message["msg_id"] in msg_ids:
                            self._pm_current_message = None
                            self._pm_preview_subject.config(text="")
                            self._pm_preview_meta.config(text="")
                            self._pm_preview_body.config(state="normal")
                            self._pm_preview_body.delete("1.0", tk.END)
                            self._pm_preview_body.config(state="disabled")
                        # Refresh to get accurate state
                        self._pm_refresh_inbox()
                    self.root.after(0, _after)
                else:
                    self.root.after(0, lambda: self._pm_status_label.config(
                        text="Delete failed", fg="red"))
            except Exception as e:
                self.root.after(0, lambda: self._pm_status_label.config(
                    text=f"Delete error: {e}", fg="red"))

        threading.Thread(target=_do_delete, daemon=True).start()

    def _pm_on_toast_toggle(self):
        """Called when the Windows notifications checkbox is toggled."""
        if self.pm_toast_var.get():
            if not self._pm_check_winotify():
                messagebox.showwarning("Missing Dependency",
                    "Windows notifications require the 'winotify' package.\n\n"
                    "Install it by running:\n"
                    "pip install winotify\n\n"
                    "Notifications will not work until the package is installed "
                    "and the application is restarted.")

    def _save_pm_settings(self):
        """Save PM polling configuration."""
        self.config["pm_polling_enabled"] = self.pm_enabled_var.get()
        try:
            interval = int(self.pm_interval_entry.get())
            if interval < 30:
                interval = 30
            self.config["pm_poll_interval_sec"] = interval
            self.pm_poll_interval = interval
        except ValueError:
            pass
        self.config["pm_toast_enabled"] = self.pm_toast_var.get()
        self.save_config()
        self.log("PM settings saved.")

        # Re-check winotify availability when toast is enabled
        if self.config["pm_toast_enabled"]:
            self._pm_toast_available = self._pm_check_winotify()

        if self.config["pm_polling_enabled"] and not self.pm_poll_active:
            self._pm_start_polling()
        elif not self.config["pm_polling_enabled"] and self.pm_poll_active:
            self.pm_poll_active = False
            self._pm_update_indicator("#e0e0e0", "  PM  ")

    def _save_tray_settings(self):
        """Save system tray and notification settings."""
        self.config["tray_enabled"] = self.tray_enabled_var.get()
        self.config["minimize_to_tray"] = self.tray_minimize_var.get()
        self.config["tray_notifications_enabled"] = self.tray_notif_var.get()
        self.save_config()
        self.log("Tray settings saved. Restart app to apply tray enable/disable changes.")
        # Apply minimize_to_tray immediately without restart
        if self._tray_icon is not None:
            if self.config["minimize_to_tray"]:
                self.root.protocol("WM_DELETE_WINDOW", self._on_window_close)
            else:
                self.root.protocol("WM_DELETE_WINDOW", self._tray_quit)

    # ======== End PM Methods ========

    def _initial_category_fetch(self):
        # If cache is empty or never updated, try to fetch
        if not self.cat_manager.cache.get('last_updated', ''):
            self.log("First run detected: Fetching Rutracker categories...")
            try:
                user, pwd = self._get_rutracker_creds()
                self.cat_manager.refresh_cache(username=user, password=pwd)
                self.log("Categories fetched successfully.")
            except Exception as e:
                self.log(f"Failed to fetch initial categories: {e}")
        self.root.after(0, self.update_cats_ui)

    def log(self, message):
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {message}"
        _log_to_file("main", message)
        if hasattr(self, 'log_area'):
            self.log_area.config(state="normal")
            self.log_area.insert(tk.END, line + "\n")
            self.log_area.see(tk.END)
            self.log_area.config(state="disabled")

    # --- Helper Methods ---
    # --- Helper Methods ---
    def sort_tree(self, tree, col, reverse):
        """Sort treeview contents when a column header is clicked."""
        try:
            l = [(tree.set(k, col), k) for k in tree.get_children('')]
            
            col_lower = col.lower()
            
            # Numeric size formatting
            if col_lower in ("size", "disk_size", "uploaded", "free space", "current load", "target load", "free", "current", "target", "up_speed", "up speed"):
                def size_to_bytes(s):
                    if not s or s == "?" or s == "-": return -1
                    if s == "0 B": return 0
                    parts = str(s).replace('\xa0', ' ').split()
                    if len(parts) != 2: return 0
                    try:
                        val = float(parts[0].replace(',', '.'))
                    except: return 0
                    units = {'B':1, 'KB':1024, 'MB':1024**2, 'GB':1024**3, 'TB':1024**4}
                    return val * units.get(parts[1], 1)
    
                l.sort(key=lambda t: size_to_bytes(t[0]), reverse=reverse)
                
            # Integer columns
            elif col_lower in ("id", "topic_id", "seeds", "leech", "seeds_snapshot", "leechers_snapshot", "k_count", "extra", "missing", "mismatch"):
                def safe_int(s):
                    try: return int(str(s).replace(',', ''))
                    except: return -1
                l.sort(key=lambda t: safe_int(t[0]), reverse=reverse)
                
            else:
                try:
                    l.sort(key=lambda t: str(t[0]).lower(), reverse=reverse)
                except:
                    l.sort(reverse=reverse)
    
            # Rearrange items in sorted positions
            for index, (val, k) in enumerate(l):
                tree.move(k, '', index)
    
            # Reverse sort next time
            tree.heading(col, command=lambda c=col, r=not reverse: self.sort_tree(tree, c, r))
        except Exception as e:
            self.log(f"Sorting error: {e}")

    def _handle_f5(self, event=None):
        """Universal Hotkey to trigger the primary Action on the current active tab."""
        idx = self.notebook.index("current")
        if idx == 0: self.process_torrent()
        elif idx == 1: self.keepers_start_scan()
        elif idx == 2: self.updater_start_scan()
        elif idx == 3: self.remover_load_torrents(force=True)
        elif idx == 4: self.repair_start_scan()
        elif idx == 5: self._mover_load_torrents(force=True)
        elif idx == 6: self.scanner_start_scan()
        elif idx == 7: self.bitrot_load_torrents()
        elif idx == 9: self.perform_search()
        return "break"

    def _copy_treeview_selection(self, event):
        """Universal Ctrl+C handler for all Treeviews."""
        widget = event.widget
        selection = widget.selection()
        if not selection:
            return
        
        lines = []
        for item in selection:
            vals = widget.item(item, "values")
            if vals:
                lines.append("\t".join(str(v) for v in vals))
        
        if lines:
            text = "\n".join(lines)
            self.root.clipboard_clear()
            self.root.clipboard_append(text)
            self.log(f"Copied {len(lines)} rows to clipboard.")

    def _on_treeview_right_click(self, event):
        """Universal Right-Click handler to copy path-like data from Treeviews."""
        widget = event.widget
        if not isinstance(widget, ttk.Treeview):
            return
            
        region = widget.identify("region", event.x, event.y)
        if region != "cell":
            return
            
        item = widget.identify_row(event.y)
        column_id = widget.identify_column(event.x)
        
        if not item or not column_id:
            return
            
        # Select the item
        widget.selection_set(item)
        widget.focus(item)
        
        col_idx = int(column_id.replace("#", "")) - 1
        cols = widget.cget("columns")
        
        if col_idx < 0 or col_idx >= len(cols):
            return
            
        col_name = str(cols[col_idx]).lower()
        valid_path_cols = ["path", "disk_path", "cur_path", "new_path", "from", "to", "target"]
        
        vals = widget.item(item, "values")
        if not vals:
            return
            
        # If user exactly right-clicked the path column cell, copy directly
        if col_name in valid_path_cols and len(vals) > col_idx:
            path_str = str(vals[col_idx])
            if path_str and path_str not in ("-", "?"):
                menu = tk.Menu(self.root, tearoff=0)
                menu.add_command(label="Copy Path", command=lambda p=path_str: self._copy_path_context(p))
                menu.post(event.x_root, event.y_root)
                return
                
        # Otherwise, search the row for any valid path columns and generate a menu
        menu = None
        for i, c_name in enumerate(cols):
            if str(c_name).lower() in valid_path_cols:
                if len(vals) > i and vals[i] and vals[i] not in ("-", "?"):
                    if not menu:
                        menu = tk.Menu(self.root, tearoff=0)
                    menu.add_command(label=f"Copy {str(c_name).capitalize()}", command=lambda p=str(vals[i]): self._copy_path_context(p))
        
        if menu:
            menu.post(event.x_root, event.y_root)

    def _copy_path_context(self, path_str):
        self.root.clipboard_clear()
        self.root.clipboard_append(path_str)
        self.log(f"Copied path to clipboard: {path_str}")

    def _show_help(self):
        """Interactive Help & Documentation window with sidebar navigation and themed styling."""
        th       = THEMES.get(self.config.get("theme", "Default"), THEMES["Default"])
        bg       = th["bg"]
        fg       = th["fg"]
        ent_bg   = th["entry_bg"]
        sel_bg   = th["select_bg"]
        sel_fg   = th["select_fg"]
        lf_fg    = th["lf_fg"]
        log_bg   = th.get("log_bg", ent_bg)
        log_fg   = th.get("log_fg", fg)
        is_night = "Night" in self.config.get("theme", "")

        # ── window ────────────────────────────────────────────────────
        help_win = tk.Toplevel(self.root)
        help_win.title(t("help.title"))
        help_win.geometry("980x720")
        help_win.configure(bg=bg)
        help_win.resizable(True, True)

        outer = tk.Frame(help_win, bg=bg)
        outer.pack(fill="both", expand=True, padx=8, pady=8)

        # ─────────────  LEFT SIDEBAR  ────────────────────────────────
        sidebar = tk.Frame(outer, bg=ent_bg, width=210)
        sidebar.pack(side="left", fill="y", padx=(0, 8))
        sidebar.pack_propagate(False)

        tk.Label(sidebar, text="   Navigation",
                 bg=ent_bg, fg=lf_fg,
                 font=("Segoe UI", 10, "bold"), anchor="w", pady=9
                 ).pack(fill="x")
        tk.Frame(sidebar, height=1, bg=sel_bg).pack(fill="x", padx=4)

        lb_scroll = tk.Scrollbar(sidebar, orient="vertical",
                                 bg=ent_bg, troughcolor=bg, relief="flat")
        lb = tk.Listbox(sidebar, bg=ent_bg, fg=fg,
                        selectbackground=sel_bg, selectforeground=sel_fg,
                        font=("Segoe UI", 9),
                        borderwidth=0, highlightthickness=0,
                        activestyle="none",
                        yscrollcommand=lb_scroll.set,
                        exportselection=False)
        lb_scroll.config(command=lb.yview)
        lb_scroll.pack(side="right", fill="y")
        lb.pack(fill="both", expand=True)

        # ─────────────  RIGHT CONTENT PANEL  ─────────────────────────
        right = tk.Frame(outer, bg=bg)
        right.pack(side="left", fill="both", expand=True)

        txt_scroll = tk.Scrollbar(right, orient="vertical",
                                  bg=ent_bg, troughcolor=bg, relief="flat")
        txt = tk.Text(right,
                      wrap="word", state="disabled",
                      bg=log_bg, fg=log_fg,
                      font=("Segoe UI", 10),
                      borderwidth=0,
                      highlightthickness=1, highlightbackground=sel_bg,
                      padx=18, pady=14,
                      yscrollcommand=txt_scroll.set,
                      insertbackground=fg,
                      selectbackground=sel_bg, selectforeground=sel_fg,
                      relief="flat")
        txt_scroll.config(command=txt.yview)
        txt_scroll.pack(side="right", fill="y")
        txt.pack(side="left", fill="both", expand=True)

        # ── text tags ──────────────────────────────────────────────────
        txt.tag_configure("h1",
                          font=("Segoe UI", 16, "bold"),
                          foreground=sel_fg, background=sel_bg,
                          spacing1=6, spacing3=6)
        txt.tag_configure("h2",
                          font=("Segoe UI", 11, "bold"),
                          foreground=lf_fg,
                          spacing1=10, spacing3=3)
        txt.tag_configure("body",
                          font=("Segoe UI", 10),
                          foreground=log_fg,
                          spacing1=2, spacing3=2)
        txt.tag_configure("kbd",
                          font=("Consolas", 10, "bold"),
                          foreground=sel_fg, background=sel_bg,
                          spacing1=3, spacing3=3)
        txt.tag_configure("tip",
                          font=("Segoe UI", 10, "italic"),
                          foreground=lf_fg,
                          spacing1=5, spacing3=5)

        # swatch colors — brighter variants for Night Mode
        _sw = {
            "green":   "#50c878" if is_night else "#007a3d",
            "dkred":   "#cc4444" if is_night else "#8b0000",
            "red":     "#ff6b6b" if is_night else "#cc2222",
            "orange":  "#ffaa44" if is_night else "#cc6600",
            "blue":    "#6cacff" if is_night else "#0055cc",
            "gray":    "#9898b0" if is_night else "#888888",
        }
        _bg_sw = {
            "pink":    ("#ffb3b3", "#1a1a1a"),
            "lorange": ("#ffe0b2", "#1a1a1a"),
            "lblue":   ("#bbdefb", "#1a1a1a"),
            "lgreen":  ("#b9f0c8" if is_night else "#c8e6c9", "#1a1a1a"),
            "lred":    ("#ffb3b3" if is_night else "#ffcdd2", "#1a1a1a"),
            "lyellow": ("#fff9c4", "#1a1a1a"),
        }
        for key, color in _sw.items():
            txt.tag_configure(f"sw_{key}", foreground=color,
                              font=("Segoe UI", 13, "bold"))
        for key, (color, cfg) in _bg_sw.items():
            txt.tag_configure(f"bg_{key}", background=color, foreground=cfg,
                              font=("Segoe UI", 9, "bold"))

        # ── helper: render content in right panel ──────────────────────
        def show(pairs):
            txt.config(state="normal")
            txt.delete("1.0", "end")
            for text, tag in pairs:
                txt.insert("end", text, tag)
            txt.config(state="disabled")
            txt.yview_moveto(0)

        S = "   "    # standard left indent for body lines

        # ── section definitions ────────────────────────────────────────
        SECTIONS = []   # list of (sidebar_label, render_fn | None)

        def s_overview():
            show([
                ("  Keepers Orchestrator  \n", "h1"),
                ("\n", "body"),
                ("Overview\n", "h2"),
                (S + "Keepers Orchestrator is a multi-tool for managing torrents via\n"
                 + S + "qBittorrent and Rutracker. It automates the full lifecycle: add,\n"
                 + S + "update, repair, move, scan, and remove — all from one window.\n\n", "body"),
                ("Key Capabilities\n", "h2"),
                (S + "•  Manage multiple qBittorrent clients simultaneously\n", "body"),
                (S + "•  Download & inject torrents from Rutracker automatically\n", "body"),
                (S + "•  Track the Keepers programme — favourite uploaders' latest releases\n", "body"),
                (S + "•  Detect and re-download updated torrent files automatically\n", "body"),
                (S + "•  Mass-move content across drives without interrupting seeding\n", "body"),
                (S + "•  Map filesystem folders to Rutracker topics\n", "body"),
                (S + "•  Detect silent bitrot via SHA-1 piece verification\n", "body"),
                (S + "•  Monitor Rutracker Private Messages with Windows toast alerts\n\n", "body"),
                (S + "💡  Use Ctrl+1 … Ctrl+0 to jump between any tab instantly.\n", "tip"),
            ])
        SECTIONS.append(("🏠  Overview", s_overview))

        def s_dashboard():
            show([
                ("  Dashboard  \n", "h1"),
                ("\n", "body"),
                ("What it shows\n", "h2"),
                (S + "Real-time overview of your entire setup across all connected clients:\n\n", "body"),
                (S + "•  Torrents  — total count, seeding, downloading, errored\n", "body"),
                (S + "•  Transfer  — current upload/download speeds and session totals\n", "body"),
                (S + "•  Database  — kept torrents, total preserved size, categories tracked\n", "body"),
                (S + "•  Storage   — disk usage bars for each client's save path drive\n", "body"),
                (S + "•  Activity  — last operations logged (adds, repairs, moves)\n\n", "body"),
                ("Refreshing\n", "h2"),
                (S + "The dashboard auto-refreshes when you switch to it.\n"
                 + S + "Hit the  ⟳ Refresh  button at any time for an instant update.\n\n", "body"),
                (S + "💡  Select a specific client or 'All Clients' to scope the stats.\n", "tip"),
            ])
        SECTIONS.append(("📊  Dashboard", s_dashboard))

        def s_add():
            show([
                ("  Add Torrents  \n", "h1"),
                ("\n", "body"),
                ("How to add\n", "h2"),
                (S + "Enter a Rutracker Topic ID (number) or a Folder Name, then\n"
                 + S + "press Process Torrent or F5. The app will:\n\n", "body"),
                (S + "  1.  Search Rutracker for the topic\n", "body"),
                (S + "  2.  Download the active .torrent file\n", "body"),
                (S + "  3.  Inject it into qBittorrent at your configured Base Save Path\n\n", "body"),
                ("Options\n", "h2"),
                (S + "•  Deep Checkbox  — searches deeper nested matches natively\n", "body"),
                (S + "•  Client selector — choose which qBittorrent instance to target\n\n", "body"),
                (S + "💡  Bulk-add by pasting multiple Topic IDs separated by commas or newlines.\n", "tip"),
            ])
        SECTIONS.append(("➕  Add Torrents", s_add))

        def s_keepers():
            show([
                ("  Keepers  \n", "h1"),
                ("\n", "body"),
                ("What is the Keepers Programme?\n", "h2"),
                (S + "Keepers are dedicated Rutracker uploaders who commit to long-term\n"
                 + S + "seeding of specific content. This tab scans all keeper profiles\n"
                 + S + "cached in your local database and shows their latest torrents.\n\n", "body"),
                ("Workflow\n", "h2"),
                (S + "  1.  Click Start Scan — fetches fresh data for all profiles\n", "body"),
                (S + "  2.  Browse results, filter by category / size / seeds\n", "body"),
                (S + "  3.  Select rows → click Add Selected to inject into qBittorrent\n\n", "body"),
                ("Filtering\n", "h2"),
                (S + "•  Category filter — narrow to a single Rutracker forum section\n", "body"),
                (S + "•  Min Seeds       — show only well-seeded torrents\n", "body"),
                (S + "•  Preferred categories are highlighted in the results list\n\n", "body"),
                (S + "💡  A toast notification fires after a successful batch add.\n", "tip"),
            ])
        SECTIONS.append(("🗂  Keepers", s_keepers))

        def s_update():
            show([
                ("  Update Torrents  \n", "h1"),
                ("\n", "body"),
                ("Purpose\n", "h2"),
                (S + "Compares every torrent in qBittorrent against the live Rutracker API.\n"
                 + S + "If the topic was re-uploaded (new version), the app downloads the\n"
                 + S + "updated .torrent and re-points it to the same disk location.\n"
                 + S + "No files are moved.\n\n", "body"),
                ("Filter: Only errored\n", "h2"),
                (S + "Enable to focus exclusively on torrents in an error state in\n"
                 + S + "qBittorrent (missing files, mismatched hashes). Faster for\n"
                 + S + "targeted repairs.\n\n", "body"),
                (S + "💡  Run after a Rutracker release wave to keep all tracked content current.\n", "tip"),
            ])
        SECTIONS.append(("🔄  Update Torrents", s_update))

        def s_remove():
            show([
                ("  Remove Torrents  \n", "h1"),
                ("\n", "body"),
                ("Purpose\n", "h2"),
                (S + "Cleans up torrents that are no longer active on Rutracker.\n"
                 + S + "The scanner compares every torrent in qBittorrent against the\n"
                 + S + "API and flags:\n\n", "body"),
                (S + "•  Dead    — topic deleted or closed on Rutracker\n", "body"),
                (S + "•  Unknown — topic status cannot be determined\n\n", "body"),
                ("Options\n", "h2"),
                (S + "•  Also delete content files (DATA) — physically removes files from\n"
                 + S + "   disk in addition to removing the torrent from qBittorrent\n\n", "body"),
                (S + "💡  Always preview the list carefully — DATA deletion is irreversible.\n", "tip"),
            ])
        SECTIONS.append(("🗑  Remove Torrents", s_remove))

        def s_repair():
            show([
                ("  Repair Categories  \n", "h1"),
                ("\n", "body"),
                ("Purpose\n", "h2"),
                (S + "Rescans all torrents in qBittorrent against the Rutracker database\n"
                 + S + "and corrects empty or inaccurate category labels.\n\n", "body"),
                ("Path Correction\n", "h2"),
                (S + "•  'Also correct save path (move files)' — files are physically\n"
                 + S + "   relocated to match the structure:  /{Category}/{Topic_ID}/\n", "body"),
                (S + "•  When disabled — only the qBittorrent category label is updated\n", "body"),
                (S + "•  Works even when Base Path doesn't match actual torrent paths\n\n", "body"),
                ("Color Feedback\n", "h2"),
                ("   ", "body"), ("■  ", "sw_dkred"), ("Dark Red    — category mismatch detected\n", "body"),
                ("   ", "body"), ("■  ", "sw_green"), ("Dark Green  — successfully repaired\n", "body"),
                ("   ", "body"), ("■  ", "sw_red"),   ("Red         — repair failed (error)\n", "body"),
            ])
        SECTIONS.append(("🔧  Repair Categories", s_repair))

        def s_move():
            show([
                ("  Move Torrents  \n", "h1"),
                ("\n", "body"),
                ("Purpose\n", "h2"),
                (S + "Mass-moves actively seeding torrents between physical drives without\n"
                 + S + "interrupting seeding. qBittorrent's save location is updated automatically.\n\n", "body"),
                ("Category Mover\n", "h2"),
                (S + "Migrates all torrents of a selected category to a new root drive.\n"
                 + S + "Files are placed into  /{Category}/{Topic_ID}/  automatically.\n\n", "body"),
                ("Balance Mover\n", "h2"),
                (S + "Automatically distributes torrents across multiple disks to equalize\n"
                 + S + "free space. Useful after adding a new drive to your storage array.\n\n", "body"),
                (S + "💡  Mover operations run in the background — monitor progress in the log panel.\n", "tip"),
            ])
        SECTIONS.append(("📦  Move Torrents", s_move))

        def s_scanner():
            show([
                ("  Folder Scanner  \n", "h1"),
                ("\n", "body"),
                ("Purpose\n", "h2"),
                (S + "Scans a physical Windows folder and maps every sub-directory against\n"
                 + S + "the Rutracker API. Use this to detect unseeded collections, identify\n"
                 + S + "missing downloads, and re-inject disconnected folders.\n\n", "body"),
                ("Scan Modes\n", "h2"),
                (S + "•  Normal      — fast name-based folder → topic matching\n", "body"),
                (S + "•  Deep Scan   — verifies file names and counts against .torrent metadata\n", "body"),
                (S + "•  Deep Scan+  — full SHA-1 piece hash verification (slow, very thorough)\n\n", "body"),
                ("Useful Filters\n", "h2"),
                (S + "•  Show only 0 B on disk — isolate empty folders with no actual data\n", "body"),
                (S + "•  Size / Disk Size columns are sortable by actual byte value\n\n", "body"),
                ("Text Colors\n", "h2"),
                ("   ", "body"), ("■  ", "sw_green"), ("Dark Green — actively seeding in qBittorrent\n", "body"),
                ("   ", "body"), ("■  ", "sw_dkred"), ("Dark Red   — 'Missing' — file has missing pieces vs API\n", "body"),
                ("   ", "body"), ("■  ", "sw_gray"),  ("Gray       — 'Dead' — topic no longer on Rutracker\n", "body"),
                ("   ", "body"), ("■  ", "body"),     ("Default    — healthy torrent, not connected to client\n", "body"),
                ("\n", "body"),
                ("Row Backgrounds\n", "h2"),
                ("   ", "body"), (" Pink / Light Red ", "bg_pink"),    ("  — 0 B on disk; folder exists but is empty\n", "body"),
                ("   ", "body"), (" Light Orange ",     "bg_lorange"), ("  — smaller than expected (< 95% of API size)\n", "body"),
                ("   ", "body"), (" Light Blue ",       "bg_lblue"),   ("  — larger than expected (> 105% of API size)\n", "body"),
            ])
        SECTIONS.append(("🔍  Folder Scanner", s_scanner))

        def s_bitrot():
            show([
                ("  Bitrot Scanner  \n", "h1"),
                ("\n", "body"),
                ("Purpose\n", "h2"),
                (S + "Scans ALL payload files across every active torrent in qBittorrent\n"
                 + S + "and subjects them to cryptographic SHA-1 piece verification.\n"
                 + S + "Perfect for discovering silent data corruption or failing drives.\n\n", "body"),
                ("How It Works\n", "h2"),
                (S + "  1.  Retrieves .torrent metadata for each seeded item\n", "body"),
                (S + "  2.  Reads each file piece-by-piece from disk\n", "body"),
                (S + "  3.  Computes SHA-1 hash and compares against the torrent's piece hashes\n", "body"),
                (S + "  4.  Any mismatch is flagged as potential bitrot\n\n", "body"),
                ("Row Background Colors\n", "h2"),
                ("   ", "body"), (" Light Green  ", "bg_lgreen"),  ("  — clean, all SHA-1 checks passed\n", "body"),
                ("   ", "body"), (" Light Red    ", "bg_lred"),    ("  — rot detected, one or more pieces corrupt\n", "body"),
                ("   ", "body"), (" Light Yellow ", "bg_lyellow"), ("  — currently being verified\n", "body"),
                ("\n", "body"),
                (S + "💡  A toast notification fires when the scan finishes and errors are found.\n", "tip"),
            ])
        SECTIONS.append(("🦠  Bitrot Scanner", s_bitrot))

        def s_search():
            show([
                ("  Search Torrents  \n", "h1"),
                ("\n", "body"),
                ("Search Modes\n", "h2"),
                (S + "•  By Name      — scrapes the Rutracker forum search results\n", "body"),
                (S + "•  By Topic ID  — direct lookup by numeric Rutracker ID\n", "body"),
                (S + "•  By Info-Hash — lookup via the Rutracker API\n\n", "body"),
                ("Actions on Results\n", "h2"),
                (S + "•  Double-click a result — opens Rutracker topic page in your browser\n", "body"),
                (S + "•  Click Inject — adds the torrent directly into qBittorrent\n", "body"),
            ])
        SECTIONS.append(("🔎  Search Torrents", s_search))

        def s_settings():
            show([
                ("  Settings  \n", "h1"),
                ("\n", "body"),
                ("Proxy\n", "h2"),
                (S + "HTTP / HTTPS / SOCKS5 proxy for bypassing regional blocks on Rutracker.\n\n", "body"),
                ("Global Authentication\n", "h2"),
                (S + "Shared login credentials applied to all qBittorrent clients at once.\n\n", "body"),
                ("Clients\n", "h2"),
                (S + "Manage multiple qBittorrent instances with individual URLs, credentials,\n"
                 + S + "and base save paths. Traffic light indicators show connection health.\n\n", "body"),
                ("Rutracker Login\n", "h2"),
                (S + "Forum credentials, category cache TTL, extracted API keys (ID / BT / API).\n\n", "body"),
                ("Private Messages\n", "h2"),
                (S + "Enable inbox polling, configure interval, toggle Windows toast notifications.\n\n", "body"),
                ("Tray & Notifications\n", "h2"),
                (S + "•  Enable system tray icon   — keeps app alive after closing the window\n", "body"),
                (S + "•  Minimize to tray on close — the X button hides window instead of quitting\n", "body"),
                (S + "•  Event notifications       — toast alerts for PM, Keepers adds, Bitrot\n\n", "body"),
                ("Appearance\n", "h2"),
                (S + "Switch between Default, Steel Blue, and Night Mode themes.\n"
                 + S + "Changes apply instantly and are saved to your config.\n\n", "body"),
                ("App Updates\n", "h2"),
                (S + "Auto-update from GitHub releases.\n\n", "body"),
                ("Statistics\n", "h2"),
                (S + "Torrents kept, total size saved, global upload/download speeds.\n", "body"),
            ])
        SECTIONS.append(("⚙  Settings", s_settings))

        # ── divider ────────────────────────────────────────────────────
        SECTIONS.append(("", None))

        def s_hotkeys():
            show([
                ("  Keyboard Shortcuts  \n", "h1"),
                ("\n", "body"),
                ("Tab Navigation\n", "h2"),
                ("   Ctrl+1  …  Ctrl+0\n", "kbd"),
                (S + "Switch between the 10 application tabs.\n"
                 + S + "(1=Add Torrents, 2=Keepers, … 9=Settings, 0=Search)\n\n", "body"),
                ("Universal Action Key\n", "h2"),
                ("   F5\n", "kbd"),
                (S + "Starts the primary operation on the current tab:\n\n", "body"),
                (S + "  Adder    →  Process Torrent\n", "body"),
                (S + "  Keepers  →  Start Scan\n", "body"),
                (S + "  Updater  →  Start Scan\n", "body"),
                (S + "  Remover  →  Refresh Client List\n", "body"),
                (S + "  Repair   →  Start Scan\n", "body"),
                (S + "  Mover    →  Refresh Client List\n", "body"),
                (S + "  Scanner  →  Start Scan\n", "body"),
                (S + "  Bitrot   →  Start Scan\n", "body"),
                (S + "  Search   →  Search Rutracker\n\n", "body"),
                ("Universal Copy\n", "h2"),
                ("   Ctrl+C\n", "kbd"),
                (S + "Works on EVERY Treeview across the app.\n"
                 + S + "Select any number of rows → paste into Excel / Notepad\n"
                 + S + "with tab-separated columns.\n\n", "body"),
                ("Other Interactions\n", "h2"),
                (S + "•  Double-click any Treeview row    — opens Rutracker topic in browser\n", "body"),
                (S + "•  Right-click a path column cell   — copies folder path to clipboard\n", "body"),
                (S + "•  Click PM badge (bottom-right)    — opens Private Messages inbox\n", "body"),
            ])
        SECTIONS.append(("⌨  Keyboard Shortcuts", s_hotkeys))

        def s_colors():
            show([
                ("  Color Guide  \n", "h1"),
                ("\n", "body"),
                ("Folder Scanner — Text Colors\n", "h2"),
                ("   ", "body"), ("■  ", "sw_green"), ("Dark Green  — actively seeding in qBittorrent\n", "body"),
                ("   ", "body"), ("■  ", "sw_dkred"), ("Dark Red    — 'Missing' — file has missing pieces\n", "body"),
                ("   ", "body"), ("■  ", "sw_gray"),  ("Gray        — 'Dead' — topic no longer on Rutracker\n", "body"),
                ("   ", "body"), ("■  ", "body"),     ("Default     — healthy torrent, not connected to client\n", "body"),
                ("\n", "body"),
                ("Folder Scanner — Row Backgrounds\n", "h2"),
                ("   ", "body"), (" Pink / Light Red ", "bg_pink"),    ("  — 0 B on disk; folder exists but empty\n", "body"),
                ("   ", "body"), (" Light Orange ",     "bg_lorange"), ("  — smaller than expected (< 95% of size)\n", "body"),
                ("   ", "body"), (" Light Blue ",       "bg_lblue"),   ("  — larger than expected (> 105% of size)\n", "body"),
                ("\n", "body"),
                ("Repair Categories — Text Colors\n", "h2"),
                ("   ", "body"), ("■  ", "sw_dkred"), ("Dark Red    — category mismatch detected\n", "body"),
                ("   ", "body"), ("■  ", "sw_green"), ("Dark Green  — successfully repaired\n", "body"),
                ("   ", "body"), ("■  ", "sw_red"),   ("Red         — repair failed (error)\n", "body"),
                ("\n", "body"),
                ("Bitrot Scanner — Row Backgrounds\n", "h2"),
                ("   ", "body"), (" Light Green  ", "bg_lgreen"),  ("  — clean, all SHA-1 checks passed\n", "body"),
                ("   ", "body"), (" Light Red    ", "bg_lred"),    ("  — rot detected; pieces are corrupt\n", "body"),
                ("   ", "body"), (" Light Yellow ", "bg_lyellow"), ("  — currently being verified\n", "body"),
                ("\n", "body"),
                ("Dashboard — Storage Bars\n", "h2"),
                ("   ", "body"), ("■  ", "sw_green"),  ("Green  — drive usage below 75%\n", "body"),
                ("   ", "body"), ("■  ", "sw_orange"), ("Amber  — drive usage 75–90%\n", "body"),
                ("   ", "body"), ("■  ", "sw_red"),    ("Red    — drive usage above 90% (critical)\n", "body"),
                ("\n", "body"),
                ("Connection Status Lights\n", "h2"),
                ("   ", "body"), ("●  ", "sw_green"),  ("Green  — connected and responding\n", "body"),
                ("   ", "body"), ("●  ", "sw_orange"), ("Orange — connection issue or unknown status\n", "body"),
                ("   ", "body"), ("●  ", "sw_red"),    ("Red    — error / unreachable\n", "body"),
                ("   ", "body"), ("●  ", "sw_gray"),   ("Gray   — not configured\n", "body"),
            ])
        SECTIONS.append(("🎨  Color Guide", s_colors))

        # ── populate sidebar ───────────────────────────────────────────
        for label, fn in SECTIONS:
            lb.insert("end", ("  " + label) if label else ("  " + "─" * 22))

        # dim the divider row
        for i, (label, fn) in enumerate(SECTIONS):
            if fn is None:
                lb.itemconfig(i, fg=sel_bg,
                              selectbackground=ent_bg, selectforeground=ent_bg)

        # ── sidebar selection handler ──────────────────────────────────
        def on_select(event=None):
            sel = lb.curselection()
            if not sel:
                return
            idx = sel[0]
            label, fn = SECTIONS[idx]
            if fn is None:
                nxt = idx + 1 if idx + 1 < len(SECTIONS) else idx - 1
                lb.selection_clear(0, "end")
                lb.selection_set(nxt)
                lb.event_generate("<<ListboxSelect>>")
                return
            fn()

        lb.bind("<<ListboxSelect>>", on_select)

        # ── open on Overview ──────────────────────────────────────────
        lb.selection_set(0)
        s_overview()
        help_win.focus_set()

    def _process_config_passwords(self, data, func):
        if "global_auth" in data and "password" in data["global_auth"]:
            data["global_auth"]["password"] = func(data["global_auth"]["password"])
        if "rutracker_auth" in data and "password" in data["rutracker_auth"]:
            data["rutracker_auth"]["password"] = func(data["rutracker_auth"]["password"])
        if "proxy" in data and "password" in data["proxy"]:
            data["proxy"]["password"] = func(data["proxy"]["password"])
        if "clients" in data:
            for client in data["clients"]:
                if "password" in client:
                    client["password"] = func(client["password"])

    def load_config(self):
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r") as f:
                    data = json.load(f)
                
                # Uncloak passwords for in-memory use
                self._process_config_passwords(data, uncloak)
                
                # Migration: Check if old format (has 'qbit_url' at top level)
                if "qbit_url" in data:
                    print("Migrating old config to new format...")
                    new_conf = DEFAULT_CONFIG.copy()
                    # Migrate old settings to first client
                    new_conf["clients"][0]["url"] = data.get("qbit_url", "http://localhost:8080")
                    new_conf["clients"][0]["username"] = data.get("qbit_user", "admin")
                    new_conf["clients"][0]["password"] = data.get("qbit_pass", "adminadmin")
                    new_conf["clients"][0]["base_save_path"] = data.get("base_save_path", "C:/Torrents/Sport/")
                    new_conf["clients"][0]["use_global_auth"] = False # Old config had specific auth
                    
                    # Also populate global auth defaults just in case
                    new_conf["global_auth"]["username"] = data.get("qbit_user", "admin")
                    new_conf["global_auth"]["password"] = data.get("qbit_pass", "adminadmin")
                    
                    return new_conf
                
                # Ensure keys exist (basic validation)
                if "clients" not in data: data["clients"] = DEFAULT_CONFIG["clients"]
                if "global_auth" not in data: data["global_auth"] = DEFAULT_CONFIG["global_auth"]
                if "rutracker_auth" not in data: data["rutracker_auth"] = DEFAULT_CONFIG["rutracker_auth"]
                if "auto_update_enabled" not in data: data["auto_update_enabled"] = False
                if "auto_update_interval_min" not in data: data["auto_update_interval_min"] = 60
                if "pm_polling_enabled" not in data: data["pm_polling_enabled"] = True
                if "pm_poll_interval_sec" not in data: data["pm_poll_interval_sec"] = 300
                if "pm_toast_enabled" not in data: data["pm_toast_enabled"] = False
                if "github_app_auto_update_enabled" not in data: data["github_app_auto_update_enabled"] = False
                if "keepers_preferred_categories" not in data: data["keepers_preferred_categories"] = []
                if "theme" not in data: data["theme"] = "Default"
                if "log_retention_days" not in data: data["log_retention_days"] = 14
                if "auto_keeper" not in data: data["auto_keeper"] = DEFAULT_CONFIG["auto_keeper"]

                return data
            except Exception as e:
                print(f"Error loading config: {e}")
                return DEFAULT_CONFIG
        return DEFAULT_CONFIG

    def save_config(self):
        try:
            # Cloak passwords for storage
            data_to_save = copy.deepcopy(self.config)
            self._process_config_passwords(data_to_save, cloak)
            
            with open(CONFIG_FILE, "w") as f:
                json.dump(data_to_save, f, indent=4)
            self.log("Configuration saved.")
            # messagebox.showinfo("Success", "Configuration saved successfully!") 
        except Exception as e:
            self.log(f"Error saving config: {e}")
            messagebox.showerror("Error", f"Could not save config: {e}")

    # --- Theme Engine ---
    def apply_theme(self, theme_name=None):
        """Apply a color theme to the entire application."""
        if theme_name is None:
            theme_name = self.config.get("theme", "Default")
        if theme_name not in THEMES:
            theme_name = "Default"
        th = THEMES[theme_name]
        self.config["theme"] = theme_name

        # --- Configure ttk styles ---
        style = ttk.Style()

        style.configure("TNotebook", background=th["bg"])
        style.configure("TNotebook.Tab", background=th["tab_bg"], foreground=th["tab_fg"], padding=[8, 4])
        style.map("TNotebook.Tab",
                  background=[("selected", th["tab_sel_bg"])],
                  foreground=[("selected", th["fg"])])

        style.configure("Treeview",
                        background=th["tree_bg"], foreground=th["tree_fg"],
                        fieldbackground=th["tree_field_bg"])
        style.map("Treeview",
                  background=[("selected", th["select_bg"])],
                  foreground=[("selected", th["select_fg"])])
        style.configure("Treeview.Heading",
                        background=th["btn_bg"], foreground=th["fg"])

        style.configure("PM.Treeview",
                        background=th["tree_bg"], foreground=th["tree_fg"],
                        fieldbackground=th["tree_field_bg"])
        style.map("PM.Treeview",
                  background=[("selected", "#fdae62")],
                  foreground=[("selected", "black")])

        style.configure("TScrollbar", background=th["btn_bg"], troughcolor=th["trough"])
        style.configure("TCombobox",
                        fieldbackground=th["entry_bg"],
                        background=th["btn_bg"], foreground=th["entry_fg"],
                        selectbackground=th["select_bg"], selectforeground=th["select_fg"])
        style.map("TCombobox",
                  fieldbackground=[("readonly", th["entry_bg"])],
                  foreground=[("readonly", th["entry_fg"])])
        # Theme combobox dropdown listbox
        self.root.option_add("*TCombobox*Listbox.background", th["entry_bg"])
        self.root.option_add("*TCombobox*Listbox.foreground", th["entry_fg"])
        self.root.option_add("*TCombobox*Listbox.selectBackground", th["select_bg"])
        self.root.option_add("*TCombobox*Listbox.selectForeground", th["select_fg"])
        style.configure("TCheckbutton", background=th["bg"], foreground=th["fg"])
        style.configure("TLabel", background=th["bg"], foreground=th["fg"])
        style.configure("TFrame", background=th["bg"])
        style.configure("TLabelframe", background=th["bg"])
        style.configure("TLabelframe.Label", background=th["bg"], foreground=th["lf_fg"])

        # Progress bars - keep accent colors, update trough only
        style.configure("green.Horizontal.TProgressbar", troughcolor=th["trough"])
        style.configure("blue.Horizontal.TProgressbar", troughcolor=th["trough"])
        style.configure("Horizontal.TProgressbar", troughcolor=th["trough"])

        # --- Walk all tk widgets ---
        self.root.configure(bg=th["bg"])
        self._apply_theme_to_widget(self.root, th)

        # --- Dashboard canvas redraws ---
        if hasattr(self, 'dash_cat_canvas'):
            self._dash_redraw_cat_chart()
        if hasattr(self, 'dash_stor_canvas'):
            self._dash_redraw_stor_chart()
        if hasattr(self, 'dash_activity_list'):
            self.dash_activity_list.configure(
                bg=th["entry_bg"], fg=th["entry_fg"])

        # --- Menu bar ---
        try:
            menu_name = self.root.cget("menu")
            if menu_name:
                menubar = self.root.nametowidget(menu_name)
                self._apply_theme_to_menu(menubar, th)
        except Exception:
            pass

    def _apply_theme_to_menu(self, menu, th):
        """Recursively theme a Menu and its sub-menus."""
        try:
            menu.configure(bg=th["menu_bg"], fg=th["menu_fg"],
                          activebackground=th["select_bg"], activeforeground=th["select_fg"])
        except tk.TclError:
            pass
        # Theme sub-menus
        last = menu.index("end")
        if last is not None:
            for i in range(last + 1):
                try:
                    submenu = menu.nametowidget(menu.entrycget(i, "menu"))
                    self._apply_theme_to_menu(submenu, th)
                except (tk.TclError, ValueError):
                    pass

    def _resolve_fg(self, current_fg, th):
        """Decide what fg a widget should get.
        Returns new fg string, or None to leave unchanged."""
        fg = current_fg.lower()
        if fg in _DEFAULT_FG:
            return th["fg"]
        remap = th.get("fg_remap")
        if remap and fg in remap:
            return remap[fg]
        return None

    def _apply_theme_to_widget(self, widget, th):
        """Recursively apply theme colors to a widget tree."""
        cls = widget.winfo_class()
        try:
            if cls == "Frame":
                widget.configure(bg=th["bg"])
            elif cls == "Labelframe":
                widget.configure(bg=th["bg"], fg=th["lf_fg"])
            elif cls == "Label":
                widget.configure(bg=th["bg"])
                new_fg = self._resolve_fg(str(widget.cget("fg")), th)
                if new_fg:
                    widget.configure(fg=new_fg)
            elif cls == "Button":
                widget.configure(bg=th["btn_bg"],
                                activebackground=th["select_bg"],
                                activeforeground=th["select_fg"])
                new_fg = self._resolve_fg(str(widget.cget("fg")), th)
                if new_fg:
                    widget.configure(fg=new_fg)
            elif cls == "Entry":
                state = str(widget.cget("state"))
                if state == "readonly":
                    widget.configure(readonlybackground=th["entry_bg"], fg=th["entry_fg"])
                else:
                    widget.configure(bg=th["entry_bg"], fg=th["entry_fg"],
                                    insertbackground=th["insert"])
            elif cls == "Listbox":
                widget.configure(bg=th["entry_bg"], fg=th["entry_fg"],
                                selectbackground=th["select_bg"],
                                selectforeground=th["select_fg"])
            elif cls == "Text":
                widget.configure(bg=th["log_bg"], fg=th["log_fg"],
                                insertbackground=th["insert"])
            elif cls == "Canvas":
                widget.configure(bg=th["bg"])
            elif cls == "Checkbutton":
                widget.configure(bg=th["bg"], activebackground=th["bg"],
                                selectcolor=th["cb_select"])
                new_fg = self._resolve_fg(str(widget.cget("fg")), th)
                if new_fg:
                    widget.configure(fg=new_fg)
            elif cls == "Radiobutton":
                widget.configure(bg=th["bg"], activebackground=th["bg"],
                                selectcolor=th["cb_select"])
                new_fg = self._resolve_fg(str(widget.cget("fg")), th)
                if new_fg:
                    widget.configure(fg=new_fg)
            elif cls == "Spinbox":
                widget.configure(bg=th["entry_bg"], fg=th["entry_fg"],
                                buttonbackground=th["btn_bg"],
                                insertbackground=th["insert"])
            elif cls == "Scrollbar":
                widget.configure(bg=th["btn_bg"], troughcolor=th["trough"])
            elif cls == "Menu":
                widget.configure(bg=th["menu_bg"], fg=th["menu_fg"],
                                activebackground=th["select_bg"],
                                activeforeground=th["select_fg"])
        except tk.TclError:
            pass

        for child in widget.winfo_children():
            self._apply_theme_to_widget(child, th)

    # --- Internationalization Helpers ---

    def _tr_register(self, key, widget, prop="text"):
        """Register a widget for language switching."""
        _i18n_registry.append((key, widget, prop))

    def _tl(self, parent, key, **opts):
        """Create a tk.Label with translated text and register it."""
        w = tk.Label(parent, text=t(key), **opts)
        self._tr_register(key, w)
        return w

    def _tb(self, parent, key, **opts):
        """Create a tk.Button with translated text and register it."""
        w = tk.Button(parent, text=t(key), **opts)
        self._tr_register(key, w)
        return w

    def _tlf(self, parent, key, **opts):
        """Create a tk.LabelFrame with translated text and register it."""
        w = tk.LabelFrame(parent, text=t(key), **opts)
        self._tr_register(key, w)
        return w

    def _tcb(self, parent, key, **opts):
        """Create a tk.Checkbutton with translated text and register it."""
        w = tk.Checkbutton(parent, text=t(key), **opts)
        self._tr_register(key, w)
        return w

    def _trb(self, parent, key, **opts):
        """Create a tk.Radiobutton with translated text and register it."""
        w = tk.Radiobutton(parent, text=t(key), **opts)
        self._tr_register(key, w)
        return w

    def _tr_heading(self, tree, col, key, **opts):
        """Set a treeview heading with translated text and register it."""
        tree.heading(col, text=t(key), **opts)
        _i18n_registry.append((key, tree, f"heading:{col}"))

    def apply_language(self, lang=None):
        """Apply language to all registered widgets, tabs, menus, and title."""
        global _current_lang
        if lang is None:
            lang = self.config.get("language", "en")
        if lang not in TRANSLATIONS:
            lang = "en"
        _current_lang = lang
        self.config["language"] = lang

        # Update all registered widgets
        for key, widget, prop in _i18n_registry:
            try:
                if not widget.winfo_exists():
                    continue
            except Exception:
                continue
            try:
                if prop == "text":
                    widget.config(text=t(key))
                elif prop.startswith("heading:"):
                    col = prop.split(":", 1)[1]
                    # Preserve existing command
                    widget.heading(col, text=t(key))
            except tk.TclError:
                pass

        # Update notebook tab titles
        tab_keys = [
            ("tab.dashboard", self.dashboard_tab),
            ("tab.add_torrents", self.adder_tab),
            ("tab.keepers", self.keepers_tab),
            ("tab.update_torrents", self.updater_tab),
            ("tab.remove_torrents", self.remover_tab),
            ("tab.repair_categories", self.repair_tab),
            ("tab.move_torrents", self.mover_tab),
            ("tab.folder_scanner", self.scanner_tab),
            ("tab.bitrot_scanner", self.bitrot_tab),
            ("tab.settings", self.settings_tab),
        ]
        if hasattr(self, 'search_tab'):
            tab_keys.append(("tab.search_torrents", self.search_tab))
        for tkey, tab_widget in tab_keys:
            try:
                self.notebook.tab(tab_widget, text=t(tkey))
            except tk.TclError:
                pass

        # Update menu bar labels, then force Windows to redraw
        try:
            self._menubar.entryconfigure(0, label=t("menu.help"))
            self._help_menu.entryconfigure(0, label=t("menu.help.docs"))
            self.root.config(menu='')
            self.root.config(menu=self._menubar)
        except Exception:
            pass

        # Update window title
        self.root.title(t("app.title"))

        # Update combobox values that depend on language
        if hasattr(self, 'search_type_combo'):
            cur = self.search_type_combo.current()
            self.search_type_combo['values'] = [t("search.type_name"), t("search.type_topic"), t("search.type_hash")]
            if cur >= 0:
                self.search_type_combo.current(cur)

        # Update "All Clients" label in client dropdowns
        if hasattr(self, 'client_selector'):
            cur = self.client_selector.current()
            self.update_client_dropdown()
            if cur >= 0:
                self.client_selector.current(cur)

    # --- Remover Tab UI (New) ---
    def create_remover_ui(self):
        # 0. State
        self.remover_all_torrents = []

        # 1. Client Selection
        client_frame = self._tlf(self.remover_tab, "common.select_client", padx=10, pady=5)
        client_frame.pack(fill="x", padx=10, pady=5)

        self._tl(client_frame, "common.client").pack(side="left")
        self.remover_client_selector = ttk.Combobox(client_frame, state="readonly", width=30)
        self.remover_client_selector.pack(side="left", padx=5)
        self.remover_client_selector.bind("<<ComboboxSelected>>", lambda e: self._remover_on_client_changed())

        self._tb(client_frame, "common.refresh_list", command=lambda: self.remover_load_torrents(force=True)).pack(side="left", padx=10)

        self.remover_cache_label = self._tl(client_frame, "common.list_updated_never", fg="gray")
        self.remover_cache_label.pack(side="left", padx=10)

        # 2. Filter & Options
        ctrl_frame = tk.Frame(self.remover_tab)
        ctrl_frame.pack(fill="x", padx=10, pady=5)

        # Filter
        self._tl(ctrl_frame, "common.filter").pack(side="left")
        self.remover_filter_var = tk.StringVar()
        self.remover_filter_var.trace("w", lambda name, index, mode, sv=self.remover_filter_var: self.remover_apply_filter())
        entry_filter = tk.Entry(ctrl_frame, textvariable=self.remover_filter_var, width=30)
        entry_filter.pack(side="left", padx=5)

        # 3. Features
        opts_frame = self._tlf(self.remover_tab, "remover.options_actions", padx=10, pady=5)
        opts_frame.pack(fill="x", padx=10, pady=5)

        self.delete_files_var = tk.BooleanVar(value=False)
        self._tcb(opts_frame, "remover.delete_files", variable=self.delete_files_var, fg="red").pack(side="left")

        self._tb(opts_frame, "remover.select_from_torrents", command=self.remover_match_from_files).pack(side="right", padx=5)

        # 4. Torrent List
        list_frame = tk.Frame(self.remover_tab)
        list_frame.pack(fill="both", expand=True, padx=10, pady=5)

        cols = ("Name", "Size", "Category", "State", "Path", "Hash")
        self.remover_tree = ttk.Treeview(list_frame, columns=cols, show="headings", selectmode="extended")

        self._tr_heading(self.remover_tree, "Name", "common.name", command=lambda: self.sort_tree(self.remover_tree, "Name", False))
        self._tr_heading(self.remover_tree, "Size", "common.size", command=lambda: self.sort_tree(self.remover_tree, "Size", False))
        self._tr_heading(self.remover_tree, "Category", "common.category", command=lambda: self.sort_tree(self.remover_tree, "Category", False))
        self._tr_heading(self.remover_tree, "State", "remover.state", command=lambda: self.sort_tree(self.remover_tree, "State", False))
        self._tr_heading(self.remover_tree, "Path", "remover.saved_path", command=lambda: self.sort_tree(self.remover_tree, "Path", False))
        self._tr_heading(self.remover_tree, "Hash", "remover.hash")
        
        self.remover_tree.column("Name", width=250)
        self.remover_tree.column("Size", width=80)
        self.remover_tree.column("Category", width=120)
        self.remover_tree.column("State", width=80)
        self.remover_tree.column("Path", width=300)
        self.remover_tree.column("Hash", width=0, stretch=False) # Hidden
        
        vsb = ttk.Scrollbar(list_frame, orient="vertical", command=self.remover_tree.yview)
        hsb = ttk.Scrollbar(list_frame, orient="horizontal", command=self.remover_tree.xview)
        self.remover_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        
        vsb.pack(side="right", fill="y")
        hsb.pack(side="bottom", fill="x")
        self.remover_tree.pack(side="left", fill="both", expand=True)
        self.remover_tree.bind("<Double-1>", self._remover_on_double_click)

        # 5. Status & Progress
        status_frame = tk.Frame(self.remover_tab, pady=5)
        status_frame.pack(fill="x", padx=10)
        
        self.remover_status = tk.Label(status_frame, text="", fg="gray", anchor="w")
        self.remover_status.pack(side="left", fill="x", expand=True)
        
        self.remover_progress = ttk.Progressbar(status_frame, mode='determinate', length=200, style="green.Horizontal.TProgressbar")
        # self.remover_progress.pack(side="right") # Pack only when busy
        
        # 6. Main Remove Button
        self._tb(self.remover_tab, "remover.remove_selected", bg="#ffcccc", command=self.remover_delete_selected).pack(pady=10)
        
        self.update_remover_client_dropdown()

    def update_remover_client_dropdown(self):
        if hasattr(self, 'remover_client_selector'):
            # Only single client selection makes sense here
            options = [c["name"] for c in self.config["clients"] if c.get("enabled", False)]
            self.remover_client_selector['values'] = options
            if options:
                self.remover_client_selector.current(0)
    
    def _remover_on_client_changed(self):
        """When client dropdown changes, show cache time and load from cache or API."""
        idx = self.remover_client_selector.current()
        if idx >= 0 and idx < len(self.config["clients"]):
            client_name = self.config["clients"][idx]["name"]
            self._show_cache_time_for_client(client_name, self.remover_cache_label)
        self.remover_load_torrents()

    def remover_load_torrents(self, force=False):
        idx = self.remover_client_selector.current()
        if idx < 0: return

        client_conf = self.config["clients"][idx]
        client_name = client_conf["name"]

        # Check cache first (unless force refresh)
        if not force:
            cached, ts = self._cache_get(client_name)
            if cached is not None:
                self.remover_all_torrents = cached
                for item in self.remover_tree.get_children():
                    self.remover_tree.delete(item)
                self._start_populate_determinate(cached)
                self._show_cache_time_for_client(client_name, self.remover_cache_label)
                return

        self.remover_status.config(text="Downloading list...", fg="black")
        self.remover_progress.pack(side="right")
        self.remover_progress.config(mode='indeterminate')
        self.remover_progress.start(10)

        # Clear tree & data
        self.remover_all_torrents = []
        for item in self.remover_tree.get_children():
            self.remover_tree.delete(item)

        def _thread():
            try:
                s = self._get_qbit_session(client_conf)
                if not s:
                    self.root.after(0, lambda: self._remover_load_done("Connection failed", "red"))
                    return

                resp = s.get(f"{client_conf['url'].rstrip('/')}/api/v2/torrents/info")
                if resp.status_code == 200:
                    torrents = resp.json()
                    self.remover_all_torrents = torrents
                    # Store in cache
                    ts = self._cache_put(client_name, torrents)
                    self.root.after(0, lambda: self._update_cache_labels(client_name, ts))
                    # Switch to determinate progress for population
                    self.root.after(0, lambda: self._start_populate_determinate(torrents))
                else:
                     self.root.after(0, lambda: self._remover_load_done(f"Error: {resp.status_code}", "red"))
            except Exception as e:
                self.root.after(0, lambda: self._remover_load_done(f"Error: {e}", "red"))
                
        threading.Thread(target=_thread, daemon=True).start()

    def _start_populate_determinate(self, torrents):
        self.remover_progress.stop()
        self.remover_progress.config(mode='determinate', value=0, maximum=len(torrents))
        self.remover_status.config(text=f"Processing {len(torrents)} items...", fg="blue")
        self.root.after(0, lambda: self._remover_populate_chunk(torrents, 0))

    def _remover_populate_chunk(self, torrents, idx):
        # Process a chunk of items
        CHUNK_SIZE = 50
        limit = min(idx + CHUNK_SIZE, len(torrents))
        
        query = self.remover_filter_var.get().lower()
        
        for i in range(idx, limit):
            t = torrents[i]
            name = t.get('name', '')
            
            # Apply filter if set
            if query and query not in name.lower():
                continue
                
            size_str = format_size(t.get('total_size', 0))
            # Get path
            save_path = t.get('save_path') or t.get('content_path') or ''
            
            self.remover_tree.insert("", "end", values=(
                name, 
                size_str, 
                t.get('category'), 
                t.get('state'), 
                save_path,
                t.get('hash')
            ))
            
        # Update progress
        self.remover_progress['value'] = limit
        self.root.update_idletasks() # Ensure UI redraws
        
        if limit < len(torrents):
            # Schedule next chunk
            self.root.after(1, lambda: self._remover_populate_chunk(torrents, limit))
        else:
            # Done
            self._remover_load_done(f"Loaded {len(torrents)} torrents.", "green")

    def _remover_load_done(self, msg, color):
        self.remover_status.config(text=msg, fg=color)
        self.remover_progress.stop()
        self.remover_progress.pack_forget()

    # Re-use population logic efficiently or just call clear+start?
    def remover_apply_filter(self):
        # Clear tree
        for item in self.remover_tree.get_children():
            self.remover_tree.delete(item)
        
        # If list is huge, we might want to chunk this too?
        # For now, let's reuse the chunk logic but with existing data
        if self.remover_all_torrents:
            self.remover_progress.pack(side="right")
            self._start_populate_determinate(self.remover_all_torrents)
        else:
            self.remover_status.config(text="List empty.", fg="gray")
    
    # Keeping old populate for reference or removing?
    # Renaming/removing _remover_populate_tree to avoid conflict/use new logic
    
    def remover_match_from_files(self):
        files = filedialog.askopenfilenames(title="Select .torrent files to match", filetypes=[("Torrent files", "*.torrent")])
        if not files: return
        
        if not self.remover_all_torrents:
            messagebox.showinfo("Info", "Please load the torrent list from the client first.")
            return

        self.remover_status.config(text="Calculating hashes...", fg="blue")
        
        def _thread():
            matched_count = 0
            hashes_to_select = set()
            
            for fp in files:
                try:
                    with open(fp, "rb") as f:
                        data = f.read()
                    
                    # Calculate Info Hash
                    h = self.calculate_torrent_hash(data)
                    if h:
                        hashes_to_select.add(h)
                except: pass
            
            # Find in tree
            # We need to map hash -> item_id
            # But the tree might be filtered.
            # So we should probably clear filter?
            # Or just select if visible?
            # Let's just iterate visible items in tree.
            
            # Better: Select matching items in the tree.
            
            def _select_in_ui():
                # Clear selection first? No, maybe add to it.
                # sel = self.remover_tree.selection()
                
                found_items = []
                for item in self.remover_tree.get_children():
                    vals = self.remover_tree.item(item)['values']
                    t_hash = vals[4].lower()
                    if t_hash in hashes_to_select:
                        found_items.append(item)
                
                if found_items:
                    self.remover_tree.selection_set(found_items)
                    self.remover_tree.see(found_items[0])
                    self.remover_status.config(text=f"Matched and selected {len(found_items)} torrents.", fg="green")
                else:
                    self.remover_status.config(text="No matches found in current list.", fg="orange")

            self.root.after(0, _select_in_ui)

        threading.Thread(target=_thread, daemon=True).start()

    def _remover_on_double_click(self, event):
        item = self.remover_tree.identify('item', event.x, event.y)
        if not item:
            return
        vals = self.remover_tree.item(item, "values")
        # cols: Name, Size, Category, State, Path, Hash
        if vals and len(vals) > 5 and vals[5]:
            self._open_topic_by_hash(vals[5])

    def remover_delete_selected(self):
        selected = self.remover_tree.selection()
        if not selected:
            messagebox.showwarning("Warning", "No torrents selected.")
            return

        delete_files = self.delete_files_var.get()
        count = len(selected)
        
        msg = f"Are you sure you want to remove {count} torrent(s)?"
        if delete_files:
            msg += "\n\nWARNING: Content files (DATA) will also be DELETED permanently!"
            
        if not messagebox.askyesno("Confirm Removal", msg, icon='warning'):
            return

        idx = self.remover_client_selector.current()
        client_conf = self.config["clients"][idx]
        
        hashes = []
        for item in selected:
            vals = self.remover_tree.item(item)['values']
            hashes.append(vals[4]) # Hash is 5th col

        def _thread():
            try:
                s = self._get_qbit_session(client_conf)
                if not s: return

                # Join hashes with |
                hash_str = "|".join(hashes)
                
                self.root.after(0, lambda: self.remover_status.config(text="Deleting...", fg="blue"))
                
                resp = s.post(f"{client_conf['url'].rstrip('/')}/api/v2/torrents/delete", data={
                    "hashes": hash_str,
                    "deleteFiles": "true" if delete_files else "false"
                })
                
                if resp.status_code == 200:
                    self._cache_invalidate(client_conf["name"])  # Invalidate stale cache
                    self.root.after(0, lambda: self.remover_status.config(text=f"Removed {len(hashes)} torrents.", fg="green"))
                    self.root.after(0, lambda: self.remover_load_torrents(force=True))  # Force refresh
                else:
                    self.root.after(0, lambda: self.remover_status.config(text=f"Delete failed: {resp.status_code}", fg="red"))
                    
            except Exception as e:
                self.root.after(0, lambda: self.remover_status.config(text=f"Error: {e}", fg="red"))

        threading.Thread(target=_thread, daemon=True).start()

    # --- Settings Tab UI ---
    def get_requests_proxies(self):
        """Returns a proxies dict for requests if proxy is enabled, else None."""
        proxy_conf = self.config.get("proxy", {})
        if proxy_conf.get("enabled") and proxy_conf.get("url"):
            url = proxy_conf["url"]
            user = proxy_conf.get("username", "")
            pwd = proxy_conf.get("password", "")
            
            if user and pwd:
                try:
                    scheme, rest = url.split("://", 1)
                    auth_url = f"{scheme}://{user}:{pwd}@{rest}"
                    return {"http": auth_url, "https": auth_url}
                except ValueError:
                    pass
            return {"http": url, "https": url}
        return None

    def import_webtlo_config(self):
        init_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".debug")
        if not os.path.exists(init_dir):
            init_dir = os.path.dirname(os.path.abspath(__file__))
            
        file_path = filedialog.askopenfilename(
            title="Select webtlo config.ini",
            initialdir=init_dir,
            filetypes=[("INI files", "*.ini"), ("All files", "*.*")]
        )
        if not file_path:
            return

        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
            
            parser = configparser.ConfigParser(interpolation=None)
            parser.read_string(content)

            import_count = 0

            # 1. Proxy
            if parser.has_section("proxy"):
                p_type = parser.get("proxy", "type", fallback="").strip('"')
                p_host = parser.get("proxy", "hostname", fallback="").strip('"')
                p_port = parser.get("proxy", "port", fallback="").strip('"')
                p_user = parser.get("proxy", "login", fallback="").strip('"')
                p_pass = parser.get("proxy", "password", fallback="").strip('"')

                if p_host and p_port:
                    proxy_url = f"{p_type}://{p_host}:{p_port}"
                    msg = f"Found Proxy settings:\nURL: {proxy_url}\nUser: {p_user}\n\nImport this proxy?"
                    if messagebox.askyesno("Import Proxy", msg, parent=self.root):
                        if "proxy" not in self.config:
                            self.config["proxy"] = {}
                        self.config["proxy"]["enabled"] = True
                        self.config["proxy"]["url"] = proxy_url
                        self.config["proxy"]["username"] = p_user
                        self.config["proxy"]["password"] = p_pass
                        
                        self.proxy_enabled_var.set(True)
                        self.entry_proxy_url.delete(0, tk.END)
                        self.entry_proxy_url.insert(0, proxy_url)
                        self.entry_proxy_user.delete(0, tk.END)
                        self.entry_proxy_user.insert(0, p_user)
                        self.entry_proxy_pass.delete(0, tk.END)
                        self.entry_proxy_pass.insert(0, p_pass)
                        import_count += 1

            # 2. Rutracker Auth
            if parser.has_section("torrent-tracker"):
                rt_user = parser.get("torrent-tracker", "login", fallback="").strip('"')
                rt_pass = parser.get("torrent-tracker", "password", fallback="").strip('"')
                if rt_user:
                    msg = f"Found Rutracker account:\nUser: {rt_user}\n\nImport this account?"
                    if messagebox.askyesno("Import Rutracker Account", msg, parent=self.root):
                        if "rutracker_auth" not in self.config:
                            self.config["rutracker_auth"] = {}
                        self.config["rutracker_auth"]["username"] = rt_user
                        self.config["rutracker_auth"]["password"] = rt_pass
                        
                        self.entry_rt_user.delete(0, tk.END)
                        self.entry_rt_user.insert(0, rt_user)
                        self.entry_rt_pass.delete(0, tk.END)
                        self.entry_rt_pass.insert(0, rt_pass)
                        import_count += 1

            # 3. Clients
            found_clients = []
            for sec in parser.sections():
                if sec.startswith("torrent-client-"):
                    c_name = parser.get(sec, "comment", fallback="").strip('"')
                    c_host = parser.get(sec, "hostname", fallback="").strip('"')
                    c_port = parser.get(sec, "port", fallback="").strip('"')
                    c_ssl = parser.get(sec, "ssl", fallback="0").strip('"')
                    c_user = parser.get(sec, "login", fallback="").strip('"')
                    c_pass = parser.get(sec, "password", fallback="").strip('"')
                    
                    if c_host and c_port:
                        scheme = "https" if c_ssl == "1" else "http"
                        c_url = f"{scheme}://{c_host}:{c_port}"
                        found_clients.append({
                            "name": c_name or f"Imported_{c_host}",
                            "url": c_url,
                            "use_global_auth": False if c_user else True,
                            "username": c_user,
                            "password": c_pass,
                            "base_save_path": "C:/Torrents/"
                        })
            
            if found_clients:
                client_names = "\n".join([c["name"] for c in found_clients])
                msg = f"Found {len(found_clients)} qBittorrent clients:\n{client_names}\n\nImport these clients?"
                if messagebox.askyesno("Import Clients", msg, parent=self.root):
                    existing_urls = [c.get("url", "") for c in self.config.get("clients", [])]
                    added_count = 0
                    for c in found_clients:
                        if c["url"] not in existing_urls:
                            self.config["clients"].append(c)
                            existing_urls.append(c["url"])
                            added_count += 1
                    
                    self.refresh_client_list()
                    if added_count > 0:
                        import_count += added_count
                        messagebox.showinfo("Import Clients", f"Added {added_count} new clients.", parent=self.root)

            # 4. Subsection categories → Keepers preferred categories
            found_cats = []
            for sec in parser.sections():
                if sec.isdigit():
                    cat_id = int(sec)
                    cat_label = parser.get(sec, "label", fallback="").strip('"')
                    cat_title = parser.get(sec, "title", fallback="").strip('"')
                    cat_name = html.unescape(cat_label or cat_title or f"Category {cat_id}")
                    found_cats.append({"id": cat_id, "name": cat_name})

            if found_cats:
                existing_prefs = self.config.get("keepers_preferred_categories", [])
                existing_ids = {c["id"] for c in existing_prefs}
                new_cats = [c for c in found_cats if c["id"] not in existing_ids]

                if new_cats:
                    cat_lines = "\n".join(f"  • {c['name']} ({c['id']})" for c in new_cats[:25])
                    if len(new_cats) > 25:
                        cat_lines += f"\n  ... and {len(new_cats) - 25} more"
                    msg = f"Found {len(new_cats)} new categories:\n{cat_lines}\n\nAdd to Keepers preferred categories?"
                    if messagebox.askyesno("Import Categories", msg, parent=self.root):
                        existing_prefs.extend(new_cats)
                        self.config["keepers_preferred_categories"] = existing_prefs
                        import_count += len(new_cats)
                        if hasattr(self, "keepers_pref_listbox"):
                            self._keepers_refresh_preferred_list()

            if import_count > 0:
                self.save_config()
                messagebox.showinfo("Import Complete", "Selected settings were imported and saved!", parent=self.root)
            else:
                messagebox.showinfo("Import Result", "Nothing was imported.", parent=self.root)

        except Exception as e:
            self.log(f"Error parsing webtlo config: {e}")
            messagebox.showerror("Error", f"Failed to parse config.ini:\n{e}", parent=self.root)

    def create_settings_ui(self):
        # Create a canvas and scrollbar for the settings tab
        self.settings_canvas = tk.Canvas(self.settings_tab, highlightthickness=0)
        self.settings_scrollbar = ttk.Scrollbar(self.settings_tab, orient="vertical", command=self.settings_canvas.yview)
        
        self.settings_scrollable_frame = tk.Frame(self.settings_canvas)
        
        self.settings_scrollable_window = self.settings_canvas.create_window((0, 0), window=self.settings_scrollable_frame, anchor="nw")
        
        self.settings_canvas.configure(yscrollcommand=self.settings_scrollbar.set)
        
        self.settings_canvas.pack(side="left", fill="both", expand=True)
        self.settings_scrollbar.pack(side="right", fill="y")
        
        # Configure scrolling region when frame size changes
        def on_frame_configure(event):
            self.settings_canvas.configure(scrollregion=self.settings_canvas.bbox("all"))
        self.settings_scrollable_frame.bind("<Configure>", on_frame_configure)
        
        # Configure canvas window size to expand horizontally
        def on_canvas_configure(event):
            self.settings_canvas.itemconfig(self.settings_scrollable_window, width=event.width)
        self.settings_canvas.bind("<Configure>", on_canvas_configure)
        
        # Mousewheel scrolling
        def _on_mousewheel(event):
            self.settings_canvas.yview_scroll(int(-1*(event.delta/120)), "units")
            
        def _bind_mousewheel(event):
            self.settings_canvas.bind_all("<MouseWheel>", _on_mousewheel)
            
        def _unbind_mousewheel(event):
            self.settings_canvas.unbind_all("<MouseWheel>")
            
        self.settings_canvas.bind("<Enter>", _bind_mousewheel)
        self.settings_canvas.bind("<Leave>", _unbind_mousewheel)


        # -1. Migration / Import Data
        migration_frame = self._tlf(self.settings_scrollable_frame, "settings.migration", padx=10, pady=5)
        migration_frame.pack(fill="x", padx=10, pady=5)
        self._tb(migration_frame, "settings.import_webtlo", command=self.import_webtlo_config).pack(anchor="w", pady=5)
        zip_row = tk.Frame(migration_frame)
        zip_row.pack(anchor="w", pady=5)
        self._tb(zip_row, "settings.export_setup", command=self.export_full_setup).pack(side="left", padx=(0, 6))
        self._tb(zip_row, "settings.import_setup", command=self.import_full_setup).pack(side="left")

        # 0. Proxy Settings Section
        proxy_frame = self._tlf(self.settings_scrollable_frame, "settings.proxy", padx=10, pady=10)
        proxy_frame.pack(fill="x", padx=10, pady=5)

        # Traffic Light for Proxy
        self.canvas_proxy_status = tk.Canvas(proxy_frame, width=20, height=20, highlightthickness=0)
        self.canvas_proxy_status.pack(side="right", padx=10)
        self.oval_proxy_status = self.canvas_proxy_status.create_oval(2, 2, 18, 18, fill=self.status_data["proxy"], outline="gray")

        self.proxy_enabled_var = tk.BooleanVar(value=self.config.get("proxy", {}).get("enabled", False))
        self._tcb(proxy_frame, "settings.proxy_enable",
                      variable=self.proxy_enabled_var, command=self.save_proxy_settings).pack(anchor="w", padx=5)

        p_auth_frame = tk.Frame(proxy_frame)
        p_auth_frame.pack(fill="x", padx=20, pady=2)

        self._tl(p_auth_frame, "settings.proxy_url").pack(side="left")
        self.entry_proxy_url = tk.Entry(p_auth_frame, width=30)
        self.entry_proxy_url.pack(side="left", padx=5)
        self.entry_proxy_url.insert(0, self.config.get("proxy", {}).get("url", "socks5://127.0.0.1:10808"))

        self._tl(p_auth_frame, "settings.username").pack(side="left", padx=(15, 0))
        self.entry_proxy_user = tk.Entry(p_auth_frame, width=15)
        self.entry_proxy_user.pack(side="left", padx=5)
        self.entry_proxy_user.insert(0, self.config.get("proxy", {}).get("username", ""))

        self._tl(p_auth_frame, "settings.password").pack(side="left", padx=(5, 0))
        self.entry_proxy_pass = tk.Entry(p_auth_frame, width=15, show="*")
        self.entry_proxy_pass.pack(side="left", padx=5)
        self.entry_proxy_pass.insert(0, self.config.get("proxy", {}).get("password", ""))

        self._tb(proxy_frame, "settings.save_proxy", command=lambda: [self.save_proxy_settings(), self.trigger_status_check()]).pack(pady=5)

        # 1. Global Auth Section
        global_frame = self._tlf(self.settings_scrollable_frame, "settings.global_auth", padx=10, pady=10)
        global_frame.pack(fill="x", padx=10, pady=5)
        self.global_auth_var = tk.BooleanVar(value=self.config["global_auth"]["enabled"])
        self._tcb(global_frame, "settings.global_auth_enable",
                      variable=self.global_auth_var, command=self.toggle_global_auth).pack(anchor="w", padx=5)

        auth_frame = tk.Frame(global_frame)
        auth_frame.pack(fill="x", padx=20, pady=2)

        self._tl(auth_frame, "settings.global_user").pack(side="left")
        self.entry_global_user = tk.Entry(auth_frame, width=15)
        self.entry_global_user.pack(side="left", padx=5)
        self.entry_global_user.insert(0, self.config["global_auth"]["username"])

        self._tl(auth_frame, "settings.global_pass").pack(side="left")
        self.entry_global_pass = tk.Entry(auth_frame, show="*", width=15)
        self.entry_global_pass.pack(side="left", padx=5)
        self.entry_global_pass.insert(0, self.config["global_auth"]["password"])

        self._tb(global_frame, "settings.save_global", command=self.save_global_settings).pack(pady=5)

        # 2. Clients List Section
        clients_frame = self._tlf(self.settings_scrollable_frame, "settings.clients", padx=10, pady=10)
        clients_frame.pack(fill="both", expand=True, padx=10, pady=5)

        list_frame = tk.Frame(clients_frame)
        list_frame.pack(side="left", fill="y", padx=5)

        self.client_listbox = tk.Listbox(list_frame, width=20)
        self.client_listbox.pack(side="top", fill="y", expand=True)
        self.client_listbox.bind("<<ListboxSelect>>", self.on_client_select)

        btn_box = tk.Frame(list_frame)
        btn_box.pack(side="bottom", fill="x", pady=5)
        tk.Button(btn_box, text="+", width=3, command=self.add_client).pack(side="left")
        tk.Button(btn_box, text="-", width=3, command=self.remove_client).pack(side="left")

        # 3. Client Details Editor
        details_frame = tk.Frame(clients_frame)
        details_frame.pack(side="left", fill="both", expand=True, padx=10)

        # Traffic light for Client
        self.canvas_client_status = tk.Canvas(details_frame, width=20, height=20, highlightthickness=0)
        self.canvas_client_status.grid(row=0, column=3, padx=0, sticky="e")
        self.oval_client_status = self.canvas_client_status.create_oval(2, 2, 18, 18, fill=self.status_data["client"], outline="gray")

        # Expand middle column so column 3 pushes to the right
        details_frame.grid_columnconfigure(2, weight=1)

        self._tl(details_frame, "settings.name").grid(row=0, column=0, sticky="w")
        self.entry_name = tk.Entry(details_frame, width=30)
        self.entry_name.grid(row=0, column=1, pady=2)
        self.entry_name.bind("<FocusOut>", lambda e: self.save_current_client())

        self._tl(details_frame, "settings.url").grid(row=1, column=0, sticky="w")
        self.entry_url = tk.Entry(details_frame, width=30)
        self.entry_url.grid(row=1, column=1, pady=2)
        self.entry_url.bind("<FocusOut>", lambda e: self.save_current_client())

        self._tl(details_frame, "settings.base_path").grid(row=2, column=0, sticky="w")
        self.entry_path = tk.Entry(details_frame, width=30)
        self.entry_path.grid(row=2, column=1, pady=2)
        self.entry_path.bind("<FocusOut>", lambda e: self.save_current_client())

        self._tl(details_frame, "settings.username").grid(row=4, column=0, sticky="w")
        self.entry_user = tk.Entry(details_frame, width=30)
        self.entry_user.grid(row=4, column=1, pady=2)
        self.entry_user.bind("<FocusOut>", lambda e: self.save_current_client())

        self._tl(details_frame, "settings.password").grid(row=5, column=0, sticky="w")
        self.entry_pass = tk.Entry(details_frame, width=30, show="*")
        self.entry_pass.grid(row=5, column=1, pady=2)
        self.entry_pass.bind("<FocusOut>", lambda e: self.save_current_client())

        self.client_enabled_var = tk.BooleanVar()
        self._tcb(details_frame, "settings.client_enabled", variable=self.client_enabled_var, command=self.save_current_client).grid(row=3, column=0, columnspan=2, sticky="w")

        self.client_use_global_auth_var = tk.BooleanVar()
        self._tcb(details_frame, "settings.use_global_auth", variable=self.client_use_global_auth_var, command=self.on_global_auth_check_toggle).grid(row=6, column=0, columnspan=2, sticky="w")

        self._tb(details_frame, "settings.save_client", command=lambda: [self.save_current_client(), self.trigger_status_check()]).grid(row=7, column=1, sticky="e", pady=10)


        # 3. Rutracker Auth Section
        rt_frame = self._tlf(self.settings_scrollable_frame, "settings.rt_login", padx=10, pady=10)
        rt_frame.pack(fill="x", padx=10, pady=5)

        # Traffic light for Rutracker
        self.canvas_rt_status = tk.Canvas(rt_frame, width=20, height=20, highlightthickness=0)
        self.canvas_rt_status.pack(side="right", anchor="ne", padx=5)
        self.oval_rt_status = self.canvas_rt_status.create_oval(2, 2, 18, 18, fill=self.status_data["rutracker"], outline="gray")

        rt_auth_frame = tk.Frame(rt_frame)
        rt_auth_frame.pack(fill="x", padx=5, pady=2)

        self._tl(rt_auth_frame, "settings.username").pack(side="left")
        self.entry_rt_user = tk.Entry(rt_auth_frame, width=20)
        self.entry_rt_user.pack(side="left", padx=5)
        self.entry_rt_user.insert(0, self.config.get("rutracker_auth", {}).get("username", ""))

        self._tl(rt_auth_frame, "settings.password").pack(side="left")
        self.entry_rt_pass = tk.Entry(rt_auth_frame, show="*", width=20)
        self.entry_rt_pass.pack(side="left", padx=5)
        self.entry_rt_pass.pack(side="left", padx=5)
        self.entry_rt_pass.insert(0, self.config.get("rutracker_auth", {}).get("password", ""))

        # Extracted Keys Display
        keys_frame = tk.Frame(rt_frame)
        keys_frame.pack(fill="x", padx=5, pady=5)

        # Helper to add read-only field
        def add_ro_field(parent, label, key):
            tk.Label(parent, text=label).pack(side="left")
            e = tk.Entry(parent, width=15, state="readonly")
            e.pack(side="left", padx=2)
            val = self.config.get("user_keys", {}).get(key, "")
            e.config(state="normal")
            e.insert(0, val)
            e.config(state="readonly")
            return e

        self.entry_key_id = add_ro_field(keys_frame, "ID:", "id")
        self.entry_key_bt = add_ro_field(keys_frame, "BT:", "bt")
        self.entry_key_api = add_ro_field(keys_frame, "API:", "api")

        self._tb(keys_frame, "settings.update_keys", command=self.update_keys_action).pack(side="left", padx=10)

        rt_ttl_frame = tk.Frame(rt_frame)
        rt_ttl_frame.pack(fill="x", padx=5, pady=2)

        self._tl(rt_ttl_frame, "settings.cat_cache_ttl").pack(side="left")
        self.entry_cat_ttl = tk.Entry(rt_ttl_frame, width=5)
        self.entry_cat_ttl.pack(side="left", padx=5)
        self.entry_cat_ttl.insert(0, str(self.config.get("category_ttl_hours", 24)))

        self._tl(rt_ttl_frame, "settings.cat_cache_hint", fg="gray").pack(side="left", padx=5)

        cache_btn_frame = tk.Frame(rt_frame)
        cache_btn_frame.pack(fill="x", padx=5, pady=2)

        self._tb(cache_btn_frame, "settings.save_cache", command=self._save_torrent_cache_settings).pack(side="left")
        self._tb(cache_btn_frame, "settings.clear_cache", command=self._clear_torrent_cache).pack(side="left", padx=10)

        # PM Inbox Settings
        pm_settings_frame = self._tlf(rt_frame, "settings.pm", padx=5, pady=5)
        pm_settings_frame.pack(fill="x", padx=5, pady=5)

        pm_row1 = tk.Frame(pm_settings_frame)
        pm_row1.pack(fill="x")

        self.pm_enabled_var = tk.BooleanVar(value=self.config.get("pm_polling_enabled", True))
        self._tcb(pm_row1, "settings.pm_enable",
                      variable=self.pm_enabled_var).pack(side="left")

        self._tl(pm_row1, "settings.pm_interval").pack(side="left", padx=(15, 0))
        self.pm_interval_entry = tk.Entry(pm_row1, width=6)
        self.pm_interval_entry.pack(side="left", padx=5)
        self.pm_interval_entry.insert(0, str(self.config.get("pm_poll_interval_sec", 300)))

        self.pm_toast_var = tk.BooleanVar(value=self.config.get("pm_toast_enabled", False))
        self._tcb(pm_row1, "settings.pm_toast",
                      variable=self.pm_toast_var,
                      command=self._pm_on_toast_toggle).pack(side="left", padx=(15, 0))

        self._tb(pm_row1, "settings.save_pm", command=self._save_pm_settings).pack(side="left", padx=10)

        # 4. Tray & Notifications Section
        tray_settings_frame = self._tlf(self.settings_scrollable_frame, "settings.tray")
        tray_settings_frame.pack(fill="x", padx=10, pady=5)

        tray_row = tk.Frame(tray_settings_frame)
        tray_row.pack(fill="x", padx=5, pady=5)

        self.tray_enabled_var = tk.BooleanVar(value=self.config.get("tray_enabled", True))
        self._tcb(tray_row, "settings.tray_enable",
                  variable=self.tray_enabled_var).pack(side="left")

        self.tray_minimize_var = tk.BooleanVar(value=self.config.get("minimize_to_tray", True))
        self._tcb(tray_row, "settings.tray_minimize",
                  variable=self.tray_minimize_var).pack(side="left", padx=(15, 0))

        self.tray_notif_var = tk.BooleanVar(value=self.config.get("tray_notifications_enabled", True))
        self._tcb(tray_row, "settings.tray_notifications",
                  variable=self.tray_notif_var).pack(side="left", padx=(15, 0))

        self._tb(tray_row, "settings.save_tray",
                 command=self._save_tray_settings).pack(side="left", padx=10)

        # 5. Data Sources Section
        data_frame = self._tlf(self.settings_scrollable_frame, "settings.data_sources")
        data_frame.pack(fill="x", padx=10, pady=5)

        top_row = tk.Frame(data_frame)
        top_row.pack(fill="x", padx=5, pady=5)

        self.refresh_cats_btn = self._tb(top_row, "settings.refresh_cats", command=self.refresh_categories)
        self.refresh_cats_btn.pack(side="left")

        self.cats_status_label = tk.Label(top_row, text=self.get_cats_status_text())
        self.cats_status_label.pack(side="left", padx=10)

        self.cats_progress = ttk.Progressbar(data_frame, mode='determinate', length=300, style="green.Horizontal.TProgressbar")
        self.cats_progress_label = tk.Label(data_frame, text="", fg="#333333", font=("Segoe UI", 9))

        # Appearance / Theme & Language
        appear_frame = self._tlf(self.settings_scrollable_frame, "settings.appearance", padx=10, pady=5)
        appear_frame.pack(fill="x", padx=10, pady=5)

        self._tl(appear_frame, "settings.theme").pack(side="left")
        self.theme_var = tk.StringVar(value=self.config.get("theme", "Default"))
        theme_combo = ttk.Combobox(appear_frame, textvariable=self.theme_var,
                                   values=list(THEMES.keys()), state="readonly", width=15)
        theme_combo.pack(side="left", padx=5)
        theme_combo.bind("<<ComboboxSelected>>", self._on_theme_change)
        self._tl(appear_frame, "settings.theme_hint", fg="gray").pack(side="left", padx=5)

        tk.Label(appear_frame, text="   ").pack(side="left")  # spacer

        self._tl(appear_frame, "settings.language").pack(side="left")
        self.lang_var = tk.StringVar(value=self.config.get("language", "en"))
        lang_combo = ttk.Combobox(appear_frame, textvariable=self.lang_var,
                                  values=list(TRANSLATIONS.keys()), state="readonly", width=5)
        lang_combo.pack(side="left", padx=5)
        lang_combo.bind("<<ComboboxSelected>>", self._on_language_change)
        self._tl(appear_frame, "settings.language_hint", fg="gray").pack(side="left", padx=5)

        # App update preferences (separate from torrent updater tab settings)
        app_update_frame = self._tlf(self.settings_scrollable_frame, "settings.app_updates", padx=10, pady=5)
        app_update_frame.pack(fill="x", padx=10, pady=5)

        self.github_app_auto_update_var = tk.BooleanVar(
            value=self.config.get("github_app_auto_update_enabled", False)
        )
        self._tcb(
            app_update_frame, "settings.auto_update_enable",
            variable=self.github_app_auto_update_var
        ).pack(side="left")
        self._tb(
            app_update_frame, "settings.save_update",
            command=self.save_github_update_settings
        ).pack(side="left", padx=10)

        # Logging settings
        logging_frame = self._tlf(self.settings_scrollable_frame, "settings.logging", padx=10, pady=5)
        logging_frame.pack(fill="x", padx=10, pady=5)

        log_row = tk.Frame(logging_frame)
        log_row.pack(anchor="w", pady=2)

        self._tl(log_row, "settings.log_retention").pack(side="left")
        self.log_retention_var = tk.StringVar(value=str(self.config.get("log_retention_days", 14)))
        tk.Spinbox(log_row, from_=1, to=365, width=5, textvariable=self.log_retention_var).pack(side="left", padx=4)
        self._tb(log_row, "settings.log_save", command=self.save_log_settings).pack(side="left", padx=(8, 0))
        self._tb(log_row, "settings.log_purge", command=self.purge_logs).pack(side="left", padx=(12, 0))

        # Version & Update
        v_frame = tk.Frame(self.settings_scrollable_frame)
        v_frame.pack(side="bottom", anchor="se", padx=10, pady=5)

        self.version_label = tk.Label(v_frame, text=f"version {APP_VERSION}", fg="gray")
        self.version_label.pack(side="right", padx=5)

        # Check Updates Button
        self._tb(v_frame, "settings.check_updates", command=lambda: threading.Thread(target=self.check_github_updates, args=(False,), daemon=True).start()).pack(side="right", padx=5)

        # GitHub Link
        lbl_gh = tk.Label(v_frame, text="GitHub", fg="blue", cursor="hand2")
        lbl_gh.pack(side="right", padx=5)
        lbl_gh.bind("<Button-1>", lambda e: webbrowser.open(f"https://github.com/{GITHUB_REPO}"))

        # Check for updates on startup (threaded, silent)
        threading.Thread(target=self.check_github_updates, args=(True,), daemon=True).start()

        self.current_client_index = -1
        self.refresh_client_list()

        # 5. Statistics (Bottom Left)
        stats_frame = self._tlf(self.settings_scrollable_frame, "settings.statistics")
        stats_frame.pack(side="bottom", anchor="sw", padx=10, pady=5, fill="x")

        s_grid = tk.Frame(stats_frame)
        s_grid.pack(fill="x", padx=5, pady=2)

        # Row 1
        self.stats_label_count = tk.Label(s_grid, text=t("settings.stats_kept", count=0), font=("", 9))
        self.stats_label_count.grid(row=0, column=0, sticky="w", padx=10)

        self.stats_label_size = tk.Label(s_grid, text=t("settings.stats_size", size="0 B"), font=("", 9))
        self.stats_label_size.grid(row=0, column=1, sticky="w", padx=10)

        self.stats_label_active = tk.Label(s_grid, text=t("settings.stats_active", size="0 B"), font=("", 9))
        self.stats_label_active.grid(row=0, column=2, sticky="w", padx=10)

        # Row 2
        self.stats_label_global_net = tk.Label(s_grid, text=t("settings.stats_net", ul="0 B/s", dl="0 B/s"), font=("", 9))
        self.stats_label_global_net.grid(row=1, column=0, sticky="w", padx=10)

        self.stats_label_global_client = tk.Label(s_grid, text=t("settings.stats_total", count=0, size="0 B"), font=("", 9))
        self.stats_label_global_client.grid(row=1, column=1, sticky="w", padx=10)

        self.stats_label_bitrot = tk.Label(s_grid, text=t("settings.stats_bitrot", count=0), font=("", 9))
        self.stats_label_bitrot.grid(row=1, column=2, sticky="w", padx=10)

        # Row 3
        self.stats_label_mover = tk.Label(s_grid, text=t("settings.stats_mover", count=0), font=("", 9))
        self.stats_label_mover.grid(row=2, column=0, sticky="w", padx=10)

        ref_btn = self._tb(s_grid, "common.refresh", command=self.refresh_statistics, height=1)
        ref_btn.grid(row=2, column=2, sticky="e", padx=5)

        # Refresh stats on load
        self.root.after(1000, self.refresh_statistics)

    @staticmethod
    def _parse_version(v_str):
        return [int(x) for x in re.findall(r"\d+", str(v_str))]

    def _set_update_available_label(self, tag, html_url):
        self.version_label.config(text=f"Update available: {tag}", fg="red", cursor="hand2")
        if html_url:
            self.version_label.bind("<Button-1>", lambda e: webbrowser.open(html_url))

    def _download_latest_app_script(self, session, release_data):
        assets = release_data.get("assets", []) or []
        for asset in assets:
            name = str(asset.get("name", "")).lower()
            dl_url = asset.get("browser_download_url", "")
            if not dl_url:
                continue
            if "keepers_orchestrator" in name and (name.endswith(".pyw") or name.endswith(".py")):
                resp = session.get(dl_url, timeout=30)
                if resp.status_code == 200 and b"class QBitAdderApp" in resp.content:
                    return resp.content, f"asset:{asset.get('name', '')}"

        zip_url = release_data.get("zipball_url", "")
        if zip_url:
            try:
                resp = session.get(zip_url, timeout=45)
                if resp.status_code == 200:
                    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                        for member in zf.namelist():
                            low_member = member.lower()
                            if "keepers_orchestrator" in low_member and \
                               (low_member.endswith(".pyw") or low_member.endswith(".py")):
                                payload = zf.read(member)
                                if b"class QBitAdderApp" in payload:
                                    return payload, f"zipball:{member}"
            except Exception:
                pass

        tag = str(release_data.get("tag_name", "")).strip()
        raw_candidates = []
        if tag:
            raw_candidates.append(f"https://raw.githubusercontent.com/{GITHUB_REPO}/{tag}/keepers_orchestrator.pyw")
        raw_candidates.append(f"https://raw.githubusercontent.com/{GITHUB_REPO}/main/keepers_orchestrator.pyw")
        raw_candidates.append(f"https://raw.githubusercontent.com/{GITHUB_REPO}/master/keepers_orchestrator.pyw")

        for raw_url in raw_candidates:
            try:
                resp = session.get(raw_url, timeout=20)
                if resp.status_code == 200 and b"class QBitAdderApp" in resp.content:
                    return resp.content, f"raw:{raw_url}"
            except Exception:
                continue

        raise RuntimeError("Could not download script from release assets, zipball, or raw source.")

    def _apply_script_update(self, script_bytes):
        if not script_bytes:
            raise RuntimeError("Downloaded script is empty.")
        script_path = os.path.abspath(__file__)
        backup_path = script_path + ".bak"
        tmp_path = script_path + ".new"

        with open(script_path, "rb") as f:
            current_bytes = f.read()
        if current_bytes == script_bytes:
            return False

        try:
            with open(tmp_path, "wb") as f:
                f.write(script_bytes)
            shutil.copy2(script_path, backup_path)
            os.replace(tmp_path, script_path)
            return True
        finally:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

    def _restart_application(self):
        script_path = os.path.abspath(__file__)
        try:
            subprocess.Popen([sys.executable, script_path], cwd=os.path.dirname(script_path))
            self.root.after(100, self.root.destroy)
        except Exception as e:
            messagebox.showerror("Restart Failed", f"Update was installed, but restart failed:\n{e}")

    def _on_update_installed(self, tag):
        self.version_label.config(text=f"Updated to {tag} (restart required)", fg="green", cursor="")
        self.version_label.unbind("<Button-1>")
        if messagebox.askyesno("Update Installed", f"Version {tag} was installed from GitHub.\n\nRestart now?"):
            self._restart_application()

    def check_github_updates(self, silent=True):
        """Check GitHub for new releases. silent=False for manual check feedback."""
        if not self.github_update_lock.acquire(blocking=False):
            return
        try:
            session = requests.Session()
            proxies = self.get_requests_proxies()
            if proxies:
                session.proxies.update(proxies)

            url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
            resp = session.get(url, timeout=8)
            if resp.status_code != 200:
                if not silent:
                    self.root.after(0, lambda code=resp.status_code: messagebox.showerror("Error", f"Failed to check updates: HTTP {code}"))
                return

            data = resp.json()
            tag = str(data.get("tag_name", "")).strip()
            html_url = data.get("html_url", "")
            if not tag:
                return

            curr_v = self._parse_version(APP_VERSION)
            new_v = self._parse_version(tag)

            if new_v > curr_v:
                self.root.after(0, lambda t=tag, h=html_url: self._set_update_available_label(t, h))
                auto_install = bool(self.config.get("github_app_auto_update_enabled", False))

                if auto_install:
                    try:
                        script_bytes, source = self._download_latest_app_script(session, data)
                        changed = self._apply_script_update(script_bytes)
                        if changed:
                            self.log(f"GitHub app auto-update installed: {tag} ({source})")
                            self.root.after(0, lambda t=tag: self._on_update_installed(t))
                        else:
                            self.log(f"GitHub app auto-update skipped: local file already matches {tag}.")
                    except Exception as install_err:
                        self.log(f"GitHub app auto-update failed: {install_err}")
                        if not silent:
                            self.root.after(0, lambda e=install_err: messagebox.showerror("Update Error", f"Could not auto-install update:\n{e}"))
                elif not silent:
                    def _prompt_open():
                        if messagebox.askyesno("Update Available", f"New version {tag} is available!\n\nOpen GitHub release page?"):
                            if html_url:
                                webbrowser.open(html_url)
                    self.root.after(0, _prompt_open)
            else:
                self.root.after(0, lambda: self.version_label.config(text=f"version {APP_VERSION}", fg="gray", cursor=""))
                self.root.after(0, lambda: self.version_label.unbind("<Button-1>"))
                if not silent:
                    self.root.after(0, lambda: messagebox.showinfo("No Updates", f"You are using the latest version ({APP_VERSION})."))

        except Exception as e:
            if not silent:
                self.root.after(0, lambda err=e: messagebox.showerror("Error", f"Failed to check updates: {err}"))
        finally:
            self.github_update_lock.release()
            
    def update_client_dropdown(self):
        if hasattr(self, 'client_selector'):
            options = [c["name"] for c in self.config["clients"] if c.get("enabled", False)] + [t("adder.all_clients")]
            self.client_selector['values'] = options

            # Simple restore logic
            target_idx = self.config.get("last_selected_client_index", 0)
            if target_idx < len(options):
                self.client_selector.current(target_idx)
            else:
                 self.client_selector.current(0)
        self.update_updater_client_dropdown()
        self.update_repair_client_dropdown()
        self.update_remover_client_dropdown()
        self.update_mover_client_dropdown()
        self.update_scanner_client_dropdown()
        self.update_bitrot_client_dropdown()

    def get_cats_status_text(self):
        last_updated = self.cat_manager.cache.get('last_updated', '')
        count = len(self.cat_manager.cache.get('categories', {}))
        if not last_updated:
            return "Categories: Not loaded"
        return f"Categories: {count} loaded (Updated: {last_updated})"

    def refresh_categories(self):
        if not messagebox.askyesno("Refresh Categories",
                "This will crawl all Rutracker forum pages to rebuild the category cache.\n"
                "It may take 1-2 minutes. Continue?"):
            return
        self.refresh_cats_btn.config(state="disabled", text="Refreshing...")
        self.cats_progress.pack(fill="x", padx=5, pady=(0, 2))
        self.cats_progress_label.pack(fill="x", padx=5, pady=(0, 5))
        self.cats_progress['value'] = 0
        self.cats_progress_label.config(text="Starting...")
        self._cats_start_time = time.time()
        threading.Thread(target=self._refresh_cats_thread, daemon=True).start()

    def _refresh_progress(self, current, total):
        """Called from refresh_cache thread to update progress bar."""
        self._update_progress(self.cats_progress, self.cats_progress_label,
            current, total, "Crawling sub-categories", self._cats_start_time)

    def _refresh_cats_thread(self):
        try:
            user, pwd = self._get_rutracker_creds()
            self.cat_manager.refresh_cache(username=user, password=pwd, progress_callback=self._refresh_progress)
            self.root.after(0, lambda: messagebox.showinfo("Success", "Categories refreshed successfully!"))
        except Exception as e:
            self.root.after(0, lambda: messagebox.showerror("Error", f"Failed to refresh categories: {e}"))
        finally:
            self.root.after(0, self.update_cats_ui)

    def update_cats_ui(self):
        self.refresh_cats_btn.config(state="normal", text=t("settings.refresh_cats"))
        self.cats_status_label.config(text=self.get_cats_status_text())
        self.cats_progress.pack_forget()
        self.cats_progress_label.pack_forget()
        
        # Update Keys Display
        keys = self.config.get("user_keys", {})
        if hasattr(self, 'entry_key_id'):
            self.entry_key_id.config(state="normal")
            self.entry_key_id.delete(0, tk.END)
            self.entry_key_id.insert(0, keys.get("id", ""))
            self.entry_key_id.config(state="readonly")
            
        if hasattr(self, 'entry_key_bt'):
            self.entry_key_bt.config(state="normal")
            self.entry_key_bt.delete(0, tk.END)
            self.entry_key_bt.insert(0, keys.get("bt", ""))
            self.entry_key_bt.config(state="readonly")

        if hasattr(self, 'entry_key_api'):
            self.entry_key_api.config(state="normal")
            self.entry_key_api.delete(0, tk.END)
            self.entry_key_api.insert(0, keys.get("api", ""))
            self.entry_key_api.config(state="readonly")

    def update_keys_action(self):
        """Independently update keys without full category refresh."""
        user = self.entry_rt_user.get()
        pwd = self.entry_rt_pass.get()
        
        if not user or not pwd:
            messagebox.showwarning("Missing Credentials", "Please enter Rutracker username and password first.")
            return

        def _thread():
            try:
                # Login if needed
                if 'bb_session' not in self.cat_manager.session.cookies:
                    if not self.cat_manager.login(user, pwd):
                        self.root.after(0, lambda: messagebox.showerror("Error", "Login failed."))
                        return
                
                # Scrape
                self.cat_manager._scrape_keys()
                self.root.after(0, lambda: messagebox.showinfo("Success", "Keys updated successfully."))
                self.root.after(0, self.update_cats_ui) # Refresh UI fields
            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror("Error", f"Key update failed: {e}"))

        threading.Thread(target=_thread, daemon=True).start()

    def toggle_global_auth(self):
        enabled = self.global_auth_var.get()
        state = "normal" if enabled else "disabled"
        self.entry_global_user.config(state=state)
        self.entry_global_pass.config(state=state)

        # When toggling off, re-enable individual fields if strictly needed,
        # but simpler to just save the pref.
        # self.save_global_settings() # Auto-save or wait for button? Wait for button.

    def save_global_settings(self):
        self.config["global_auth"]["enabled"] = self.global_auth_var.get()
        self.config["global_auth"]["username"] = self.entry_global_user.get()
        self.config["global_auth"]["password"] = self.entry_global_pass.get()
        self.save_config()
        self.log("Global settings saved.")

    def save_proxy_settings(self):
        if "proxy" not in self.config:
            self.config["proxy"] = {}
        self.config["proxy"]["enabled"] = self.proxy_enabled_var.get()
        self.config["proxy"]["url"] = self.entry_proxy_url.get().strip()
        self.config["proxy"]["username"] = self.entry_proxy_user.get()
        self.config["proxy"]["password"] = self.entry_proxy_pass.get()
        self.save_config()
        self.log("Proxy settings saved.")
        
        # Invalidate the CategoryManager session so it picks up the proxy
        if hasattr(self, 'cat_manager'):
            self.cat_manager.session = requests.Session()
            proxies = self.get_requests_proxies()
            if proxies:
                self.cat_manager.session.proxies.update(proxies)

    def _on_theme_change(self, event=None):
        """Handle theme combobox change — apply and save immediately."""
        name = self.theme_var.get()
        self.apply_theme(name)
        self.save_config()

    def _on_language_change(self, event=None):
        """Handle language combobox change — apply and save immediately."""
        lang = self.lang_var.get()
        self.apply_language(lang)
        self.save_config()

    def save_github_update_settings(self):
        self.config["github_app_auto_update_enabled"] = self.github_app_auto_update_var.get()
        self.save_config()
        messagebox.showinfo("Success", "GitHub app update settings saved.")

    def save_log_settings(self):
        global _LOG_RETENTION_DAYS
        try:
            days = int(self.log_retention_var.get())
            if days < 1:
                raise ValueError
        except ValueError:
            messagebox.showerror("Error", t("settings.log_retention") + " >= 1")
            return
        _LOG_RETENTION_DAYS = days
        self.config["log_retention_days"] = days
        self.save_config()
        _cleanup_old_logs(days)
        messagebox.showinfo("OK", f"{t('settings.log_retention')} {days}")

    def purge_logs(self):
        if not messagebox.askyesno("Confirm", t("settings.log_purge") + "?"):
            return
        global _file_loggers
        with _log_lock:
            for name, logger in list(_file_loggers.items()):
                for h in logger.handlers[:]:
                    h.close()
                    logger.removeHandler(h)
            _file_loggers.clear()
        for f in os.listdir(_LOGS_DIR):
            if f.endswith(".log"):
                try:
                    os.remove(os.path.join(_LOGS_DIR, f))
                except OSError:
                    pass
        messagebox.showinfo("OK", t("settings.log_purged"))

    def save_rt_settings(self):
        enabled = self.auto_update_var.get()
        interval_str = self.entry_au_interval.get()
        
        try:
            interval = int(interval_str)
            if interval < 0: raise ValueError
        except ValueError:
            messagebox.showerror("Error", "Interval must be a positive integer.")
            return

        self.config["auto_update_enabled"] = enabled
        self.config["auto_update_interval_min"] = interval
        self.save_config()
        messagebox.showinfo("Success", "Auto-update settings saved.")

    def _save_torrent_cache_settings(self):
        try:
            ttl = int(self.entry_cat_ttl.get()) # Changed from entry_torrent_cache_ttl to entry_cat_ttl
            if ttl < 0: raise ValueError
        except ValueError:
            messagebox.showerror("Error", "Cache TTL must be a non-negative integer.")
            return
        self.config["category_ttl_hours"] = ttl # Changed from torrent_cache_ttl_hours to category_ttl_hours
        self.save_config()
        messagebox.showinfo("Success", f"Torrent cache TTL set to {ttl} hours.")

    def _clear_torrent_cache(self):
        self._cache_invalidate()
        # Reset all cache labels
        for lbl_name in ('remover_cache_label', 'repair_cache_label', 'mover_cache_label', 'scanner_cache_label'):
            if hasattr(self, lbl_name):
                getattr(self, lbl_name).config(text=t("common.list_updated_never"), fg="gray")
        messagebox.showinfo("Cache Cleared", "All cached torrent lists have been cleared.")

    def save_rutracker_settings(self):
        if "rutracker_auth" not in self.config:
            self.config["rutracker_auth"] = {}
        self.config["rutracker_auth"]["username"] = self.entry_rt_user.get()
        self.config["rutracker_auth"]["password"] = self.entry_rt_pass.get()
        try:
            self.config["category_ttl_hours"] = max(1, int(self.entry_cat_ttl.get()))
        except ValueError:
            self.config["category_ttl_hours"] = 24
        self.save_config()
        # Clear session to force re-login with new credentials
        self.cat_manager.session.cookies.clear()
        self.log(f"Rutracker settings saved. Session cleared. TTL: {self.config['category_ttl_hours']}h")
        # Start PM polling if not yet active (creds might have just been added)
        self._pm_start_polling()

    def refresh_client_list(self):
        selection = self.client_listbox.curselection()
        self.client_listbox.delete(0, tk.END)
        for i, c in enumerate(self.config.get("clients", [])):
            with self.status_lock:
                c_status = self.client_statuses[i] if i < len(self.client_statuses) else "gray"
            enabled = c.get("enabled", False)
            prefix = "●" if enabled else "○"
            self.client_listbox.insert(tk.END, f"{prefix} {c.get('name', 'Unnamed')}")
            if not enabled:
                self.client_listbox.itemconfig(i, {'fg': '#bbbbbb'})
            elif c_status == "green": self.client_listbox.itemconfig(i, {'fg': '#00b300'})
            elif c_status == "red": self.client_listbox.itemconfig(i, {'fg': '#e60000'})
            elif c_status == "yellow": self.client_listbox.itemconfig(i, {'fg': '#cccc00'})
            else: self.client_listbox.itemconfig(i, {'fg': 'gray'})
        
        # Select first if exists or reset
        if self.config["clients"]:
            if getattr(self, 'current_client_index', -1) < 0 or self.current_client_index >= len(self.config["clients"]):
                self.current_client_index = 0
            if selection:
                self.client_listbox.selection_set(selection[0])
            else:
                self.client_listbox.selection_set(self.current_client_index)
            self.on_client_select(None)
        else:
            self.current_client_index = -1
            self.clear_client_details()

    def on_client_select(self, event):
        selection = self.client_listbox.curselection()
        if not selection:
            return
            
        index = selection[0]
        self.current_client_index = index
        client = self.config["clients"][index]
        
        self.entry_name.delete(0, tk.END)
        self.entry_name.insert(0, client["name"])
        
        self.entry_url.delete(0, tk.END)
        self.entry_url.insert(0, client["url"])
        
        self.entry_user.delete(0, tk.END)
        self.entry_user.insert(0, client.get("username", ""))
        
        self.entry_pass.delete(0, tk.END)
        self.entry_pass.insert(0, client.get("password", ""))
        
        self.entry_path.delete(0, tk.END)
        self.entry_path.insert(0, client["base_save_path"])

        self.client_enabled_var.set(client.get("enabled", False))
        self.client_use_global_auth_var.set(client.get("use_global_auth", True))
        self.toggle_client_auth_fields()
        
        # Immediately set status canvas color based on known state
        with self.status_lock:
            color = self.client_statuses[index] if index < len(self.client_statuses) else "gray"
        self._update_ui_status("client", color)

    def on_global_auth_check_toggle(self):
        self.toggle_client_auth_fields()
        self.save_current_client()

    def toggle_client_auth_fields(self):
        # If Using Global Auth, disable specific fields
        enabled = not self.client_use_global_auth_var.get()
        state = "normal" if enabled else "disabled"
        self.entry_user.config(state=state)
        self.entry_pass.config(state=state)
        # self.save_current_client() # Removed to prevent recursion loop

    def update_settings_model(self, event=None):
        if self.is_initializing: return
        self.config["global_auth"]["enabled"] = self.use_global_var.get()
        self.config["global_auth"]["username"] = self.global_user_entry.get()
        self.config["global_auth"]["password"] = self.global_pass_entry.get()
        self.save_config()

    def save_current_client(self, event=None):
        if self.current_client_index < 0: return
        # if self.is_initializing: return # This check might be problematic if called from FocusOut during init
        
        idx = self.current_client_index
        self.config["clients"][idx]["name"] = self.entry_name.get()
        self.config["clients"][idx]["url"] = self.entry_url.get()
        self.config["clients"][idx]["base_save_path"] = self.entry_path.get()
        self.config["clients"][idx]["enabled"] = self.client_enabled_var.get()
        self.config["clients"][idx]["use_global_auth"] = self.client_use_global_auth_var.get()
        self.config["clients"][idx]["username"] = self.entry_user.get()
        self.config["clients"][idx]["password"] = self.entry_pass.get()
        
        self.save_config()
        self.update_client_dropdown()
        
        # Manually set color to yellow instantly to provide feedback before the thread finishes
        with self.status_lock:
            if idx < len(self.client_statuses):
                self.client_statuses[idx] = "yellow"
        
        if event is None: 
             self.refresh_client_list()
             self.log(f"Client '{self.entry_name.get()}' details saved.")

    def add_client(self):
        new_client = {
            "name": "New Client",
            "url": "http://localhost:8080",
            "use_global_auth": True,
            "username": "",
            "password": "",
            "base_save_path": "C:/Downloads/",
            "enabled": False
        }
        self.config["clients"].append(new_client)
        self.current_client_index = len(self.config["clients"]) - 1
        self.save_config()
        self.refresh_client_list()
        # Select the new one
        self.client_listbox.selection_clear(0, tk.END)
        self.client_listbox.select_set(self.current_client_index)
        self.on_client_select(None)
        self.update_client_dropdown()

    def remove_client(self):
        selection = self.client_listbox.curselection()
        if not selection:
            return
            
        if messagebox.askyesno("Confirm", "Remove selected client?"):
            index = selection[0]
            del self.config["clients"][index]
            self.save_config()
            self.current_client_index = -1
            self.refresh_client_list()
            self.update_client_dropdown()

    def clear_client_details(self):
        self.entry_name.delete(0, tk.END)
        self.entry_url.delete(0, tk.END)
        self.entry_path.delete(0, tk.END)
        self.entry_user.delete(0, tk.END)
        self.entry_pass.delete(0, tk.END)
        # self.c_use_global.set(False) # This is not defined in the current context
        # self.toggle_client_auth_fields() # This is not defined in the current context


    # --- Dashboard Tab UI ---

    def create_dashboard_ui(self):
        """Build the Dashboard overview tab."""
        th = THEMES.get(self.config.get("theme", "Default"), THEMES["Default"])

        # ── Top bar ──────────────────────────────────────────────────
        top_bar = tk.Frame(self.dashboard_tab)
        top_bar.pack(fill="x", padx=10, pady=(8, 4))

        tk.Label(top_bar, text="Client:").pack(side="left")
        self.dash_client_var = tk.StringVar(value="All")
        client_names = ["All"] + [c["name"] for c in self.config.get("clients", [])]
        self.dash_client_combo = ttk.Combobox(
            top_bar, textvariable=self.dash_client_var,
            values=client_names, state="readonly", width=22)
        self.dash_client_combo.pack(side="left", padx=5)
        self.dash_client_combo.bind("<<ComboboxSelected>>",
                                    lambda e: self._dashboard_refresh())

        self.dash_refresh_btn = tk.Button(
            top_bar, text="\u27f3 Refresh", command=self._dashboard_refresh)
        self.dash_refresh_btn.pack(side="left", padx=10)

        self.dash_last_updated_lbl = tk.Label(
            top_bar, text="Not loaded yet", fg="gray")
        self.dash_last_updated_lbl.pack(side="left")

        # ── 4 Stat Cards row ─────────────────────────────────────────
        cards_frame = tk.Frame(self.dashboard_tab)
        cards_frame.pack(fill="x", padx=10, pady=4)
        cards_frame.columnconfigure((0, 1, 2, 3), weight=1, uniform="dash_card")

        self.dash_card_seeding = self._dash_make_card(cards_frame, "Seeding",  col=0)
        self.dash_card_keepers = self._dash_make_card(cards_frame, "Keepers",  col=1)
        self.dash_card_network = self._dash_make_card(cards_frame, "Network",  col=2)
        self.dash_card_health  = self._dash_make_card(cards_frame, "Health",   col=3)

        # ── Middle: charts (left) + activity feed (right) ────────────
        mid_frame = tk.Frame(self.dashboard_tab)
        mid_frame.pack(fill="both", expand=True, padx=10, pady=4)
        mid_frame.columnconfigure(0, weight=3)
        mid_frame.columnconfigure(1, weight=2)
        mid_frame.rowconfigure(0, weight=1)
        mid_frame.rowconfigure(1, weight=1)

        # Category bar chart
        cat_lf = tk.LabelFrame(mid_frame, text="Top Categories (by torrent count)")
        cat_lf.grid(row=0, column=0, sticky="nsew", padx=(0, 5), pady=(0, 4))
        self.dash_cat_canvas = tk.Canvas(cat_lf, highlightthickness=0, height=160)
        self.dash_cat_canvas.pack(fill="both", expand=True, padx=4, pady=4)
        self.dash_cat_canvas.bind("<Configure>",
                                  lambda e: self._dash_redraw_cat_chart())

        # Storage bar chart
        stor_lf = tk.LabelFrame(mid_frame, text="Storage Overview")
        stor_lf.grid(row=1, column=0, sticky="nsew", padx=(0, 5), pady=0)
        self.dash_stor_canvas = tk.Canvas(stor_lf, highlightthickness=0, height=130)
        self.dash_stor_canvas.pack(fill="both", expand=True, padx=4, pady=4)
        self.dash_stor_canvas.bind("<Configure>",
                                   lambda e: self._dash_redraw_stor_chart())

        # Recent Activity feed
        act_lf = tk.LabelFrame(mid_frame, text="Recent Activity")
        act_lf.grid(row=0, column=1, rowspan=2, sticky="nsew")
        act_lf.rowconfigure(0, weight=1)
        act_lf.columnconfigure(0, weight=1)
        self.dash_activity_list = tk.Listbox(
            act_lf, selectmode="none", activestyle="none",
            relief="flat", highlightthickness=0, font=("Courier", 8))
        act_scroll = ttk.Scrollbar(act_lf, orient="vertical",
                                   command=self.dash_activity_list.yview)
        self.dash_activity_list.configure(yscrollcommand=act_scroll.set)
        self.dash_activity_list.grid(row=0, column=0, sticky="nsew", padx=4, pady=4)
        act_scroll.grid(row=0, column=1, sticky="ns")

    def _dash_make_card(self, parent, title, col):
        """Create a stat card LabelFrame with 4 label lines. Returns {0: lbl, ...}."""
        lf = tk.LabelFrame(parent, text=title, padx=8, pady=6)
        lf.grid(row=0, column=col, sticky="nsew", padx=4)
        lines = {}
        for i in range(4):
            lbl = tk.Label(lf, text="\u2014", anchor="w", justify="left")
            lbl.pack(fill="x", pady=1)
            lines[i] = lbl
        return lines

    def _dashboard_refresh(self):
        """Spawn background thread to gather all dashboard data."""
        if getattr(self, 'dashboard_refresh_active', False):
            return
        self.dashboard_refresh_active = True
        self.dash_refresh_btn.config(state="disabled", text="Loading\u2026")

        def _thread():
            try:
                data = self._dashboard_gather_data()
                self.root.after(0, lambda: self._dashboard_apply_data(data))
            except Exception as exc:
                err = str(exc)
                self.root.after(0, lambda: self.dash_last_updated_lbl.config(
                    text=f"Error: {err}", fg="red"))
            finally:
                self.dashboard_refresh_active = False
                self.root.after(0, lambda: self.dash_refresh_btn.config(
                    state="normal", text="\u27f3 Refresh"))

        threading.Thread(target=_thread, daemon=True).start()

    def _dashboard_gather_data(self):
        """Collect all stats for the dashboard (runs in background thread)."""
        data = {
            "seeding":  {"active": 0, "paused": 0, "error": 0,
                         "total_size": 0, "uploaded": 0},
            "network":  {"up_speed": 0, "dl_speed": 0, "total_up": 0},
            "keepers":  {"count": 0, "size": 0, "last_scan_ts": 0},
            "health":   {"total_checked": 0, "ok": 0, "errors": 0,
                         "last_check_ts": 0},
            "categories": [],
            "storage":  [],
            "activity": [],
            "as_of":    time.time(),
        }

        # ── DB stats (local, fast) ───────────────────────────────────
        try:
            kept_count, kept_size = self.db_manager.get_kept_stats()
            data["keepers"]["count"] = kept_count
            data["keepers"]["size"]  = kept_size or 0
        except Exception:
            pass

        try:
            recent = self.db_manager.get_recent_activity(1)
            if recent:
                ts_str = recent[0][0]
                dt = datetime.datetime.fromisoformat(str(ts_str))
                data["keepers"]["last_scan_ts"] = dt.timestamp()
        except Exception:
            pass

        try:
            bitrot = self.db_manager.get_bitrot_history()
            ok_statuses = {"ok", "clean", "good"}
            ok_count  = sum(1 for v in bitrot.values()
                            if v.get("status", "").lower() in ok_statuses)
            err_count = sum(1 for v in bitrot.values()
                            if v.get("status", "").lower() not in ok_statuses | {""})
            last_chk  = max((v.get("last_checked", 0)
                             for v in bitrot.values()), default=0)
            data["health"]["total_checked"] = len(bitrot)
            data["health"]["ok"]            = ok_count
            data["health"]["errors"]        = err_count
            data["health"]["last_check_ts"] = last_chk
        except Exception:
            pass

        # ── Unified activity feed ────────────────────────────────────
        activity = []
        try:
            for row in self.db_manager.get_recent_activity(limit=10):
                ts_str, cat_id, scanned, added = row
                try:
                    dt = datetime.datetime.fromisoformat(str(ts_str))
                    ts = dt.timestamp()
                except Exception:
                    ts = 0
                activity.append((ts, f"Keepers scan: +{added} added, {scanned} checked"))
        except Exception:
            pass

        try:
            with self.db_manager._get_conn() as conn:
                rows = conn.execute(
                    "SELECT timestamp, from_disk, to_disk "
                    "FROM mover_history ORDER BY rowid DESC LIMIT 10"
                ).fetchall()
                for row in rows:
                    ts_raw, from_d, to_d = row
                    activity.append((float(ts_raw or 0),
                                     f"Moved: {from_d} \u2192 {to_d}"))
        except Exception:
            pass

        activity.sort(key=lambda x: x[0], reverse=True)
        data["activity"] = activity[:25]

        # ── qBit stats (network) ─────────────────────────────────────
        selected = self.dash_client_var.get() if hasattr(self, 'dash_client_var') else "All"
        clients = self.config.get("clients", [])
        if selected and selected != "All":
            clients_to_query = [c for c in clients if c.get("name") == selected]
        else:
            clients_to_query = [c for c in clients if c.get("enabled", True)]

        cat_counts = {}
        disk_bases = {}

        for client_conf in clients_to_query:
            try:
                torrents, _ts = self._cache_get(client_conf["name"])
                if torrents is None:
                    s = self._get_qbit_session(client_conf)
                    if not s:
                        continue
                    resp = s.get(
                        f"{client_conf['url'].rstrip('/')}/api/v2/torrents/info",
                        timeout=10)
                    if resp.status_code != 200:
                        continue
                    torrents = resp.json()
                    self._cache_put(client_conf["name"], torrents)

                # Transfer info for live speeds
                try:
                    s2 = self._get_qbit_session(client_conf)
                    if s2:
                        tresp = s2.get(
                            f"{client_conf['url'].rstrip('/')}/api/v2/transfer/info",
                            timeout=5)
                        if tresp.status_code == 200:
                            ti = tresp.json()
                            data["network"]["up_speed"] += ti.get("up_info_speed", 0)
                            data["network"]["dl_speed"] += ti.get("dl_info_speed", 0)
                            data["network"]["total_up"] += ti.get("up_info_data", 0)
                except Exception:
                    pass

                for torrent in torrents:
                    state    = torrent.get("state", "")
                    size     = torrent.get("size", 0) or 0
                    uploaded = torrent.get("uploaded", 0) or 0
                    cat      = torrent.get("category", "") or "Uncategorized"
                    sp       = torrent.get("save_path", "") or ""

                    data["seeding"]["total_size"] += size
                    data["seeding"]["uploaded"]   += uploaded

                    if state in ("uploading", "stalledUP", "forcedUP",
                                 "queuedUP", "checkingUP"):
                        data["seeding"]["active"] += 1
                    elif state in ("pausedUP", "pausedDL",
                                   "stoppedUP", "stoppedDL"):
                        data["seeding"]["paused"] += 1
                    elif state in ("error", "missingFiles", "unknown"):
                        data["seeding"]["error"] += 1

                    cat_counts[cat] = cat_counts.get(cat, 0) + 1

                    if sp:
                        base = self._mover_get_disk_base(sp.replace("\\", "/"))
                        if base not in disk_bases:
                            try:
                                usage = shutil.disk_usage(base)
                                disk_bases[base] = {
                                    "total": usage.total, "free": usage.free}
                            except Exception:
                                disk_bases[base] = {"total": 0, "free": 0}

            except Exception:
                continue

        if cat_counts:
            sorted_cats = sorted(cat_counts.items(),
                                 key=lambda x: x[1], reverse=True)[:10]
            data["categories"] = sorted_cats

        storage = []
        for path, info in sorted(disk_bases.items()):
            total = info["total"]
            free  = info["free"]
            if total > 0:
                storage.append((path, total - free, total))
        data["storage"] = storage

        return data

    def _dashboard_apply_data(self, data):
        """Update all dashboard widgets from gathered data (main thread)."""
        th = THEMES.get(self.config.get("theme", "Default"), THEMES["Default"])
        fg = th["fg"]

        # ── Seeding card ─────────────────────────────────────────────
        s = data["seeding"]
        self.dash_card_seeding[0].config(
            text=f"Active:   {s['active']:,}", font=("", 11, "bold"), fg=fg)
        self.dash_card_seeding[1].config(
            text=f"Paused:   {s['paused']:,}", fg=fg)
        err_col = "red" if s['error'] > 0 else fg
        self.dash_card_seeding[2].config(
            text=f"Errors:   {s['error']:,}", fg=err_col)
        self.dash_card_seeding[3].config(
            text=f"Size:     {format_size(s['total_size'])}", fg=fg)

        # ── Keepers card ─────────────────────────────────────────────
        k = data["keepers"]
        self.dash_card_keepers[0].config(
            text=f"Kept:     {k['count']:,}", font=("", 11, "bold"), fg=fg)
        self.dash_card_keepers[1].config(
            text=f"Total:    {format_size(k['size'])}", fg=fg)
        if k["last_scan_ts"] > 0:
            age_h = (time.time() - k["last_scan_ts"]) / 3600
            if age_h < 1:
                age_str = f"{int(age_h * 60)}m ago"
            elif age_h < 24:
                age_str = f"{age_h:.1f}h ago"
            else:
                age_str = f"{age_h / 24:.1f}d ago"
            self.dash_card_keepers[2].config(text=f"Last scan: {age_str}", fg=fg)
        else:
            self.dash_card_keepers[2].config(text="Last scan: Never", fg="gray")
        self.dash_card_keepers[3].config(text="", fg=fg)

        # ── Network card ─────────────────────────────────────────────
        n = data["network"]
        self.dash_card_network[0].config(
            text=f"\u2191  {format_size(n['up_speed'])}/s",
            font=("", 11, "bold"), fg=fg)
        self.dash_card_network[1].config(
            text=f"\u2193  {format_size(n['dl_speed'])}/s", fg=fg)
        self.dash_card_network[2].config(
            text=f"\u2191 Total: {format_size(s['uploaded'])}", fg=fg)
        self.dash_card_network[3].config(text="", fg=fg)

        # ── Health card ──────────────────────────────────────────────
        h = data["health"]
        if h["errors"] > 0:
            status_text  = f"\u26a0 {h['errors']} errors"
            status_color = "red"
        elif h["total_checked"] > 0:
            status_text  = "\u2713 Clean"
            status_color = "#28a745"
        else:
            status_text  = "Not scanned"
            status_color = "gray"
        self.dash_card_health[0].config(
            text=status_text, fg=status_color, font=("", 11, "bold"))
        self.dash_card_health[1].config(
            text=f"Checked:  {h['total_checked']:,}", fg=fg)
        if h["last_check_ts"] > 0:
            dt_str = datetime.datetime.fromtimestamp(
                h["last_check_ts"]).strftime("%Y-%m-%d")
            self.dash_card_health[2].config(text=f"Last: {dt_str}", fg=fg)
        else:
            self.dash_card_health[2].config(text="Last: Never", fg="gray")
        self.dash_card_health[3].config(text="", fg=fg)

        # ── Charts ───────────────────────────────────────────────────
        self._dash_cat_data  = data["categories"]
        self._dash_stor_data = data["storage"]
        self._dash_redraw_cat_chart()
        self._dash_redraw_stor_chart()

        # ── Activity feed ────────────────────────────────────────────
        self.dash_activity_list.delete(0, tk.END)
        for ts, desc in data["activity"]:
            if ts > 0:
                time_str = datetime.datetime.fromtimestamp(ts).strftime("%m-%d %H:%M")
            else:
                time_str = "     ?    "
            self.dash_activity_list.insert(tk.END, f" {time_str}  {desc}")

        # ── Timestamp ────────────────────────────────────────────────
        now_str = datetime.datetime.now().strftime("%H:%M:%S")
        self.dash_last_updated_lbl.config(
            text=f"Last updated: {now_str}", fg=fg)

    def _dash_redraw_cat_chart(self):
        """Draw horizontal bar chart for top categories on the canvas."""
        if not hasattr(self, 'dash_cat_canvas'):
            return
        c = self.dash_cat_canvas
        c.delete("all")
        data = getattr(self, '_dash_cat_data', [])
        th = THEMES.get(self.config.get("theme", "Default"), THEMES["Default"])
        bg = th["bg"]
        fg = th["fg"]
        bar_color = th["select_bg"]
        c.configure(bg=bg)

        if not data:
            c.create_text(10, 20, text="No data — click Refresh",
                          anchor="nw", fill="gray")
            return

        w = c.winfo_width() or 400
        h = c.winfo_height() or 160
        rows = min(len(data), 8)
        if rows == 0:
            return
        row_h = h / rows
        label_w = min(int(w * 0.44), 220)
        count_w = 48
        bar_area_w = max(10, w - label_w - count_w - 12)
        max_count = max(cnt for _, cnt in data[:rows])

        for i, (name, count) in enumerate(data[:rows]):
            y  = i * row_h
            cy = y + row_h / 2

            # Name label
            display = (name[:30] + "\u2026") if len(name) > 30 else name
            c.create_text(4, cy, text=display, anchor="w",
                          fill=fg, font=("", 8))

            # Bar
            bar_x = label_w
            if max_count > 0:
                bar_w = max(2, int((count / max_count) * bar_area_w))
            else:
                bar_w = 2
            c.create_rectangle(bar_x, y + 4, bar_x + bar_w, y + row_h - 4,
                                fill=bar_color, outline="")

            # Count text
            c.create_text(bar_x + bar_area_w + 4, cy,
                          text=f"{count:,}", anchor="w",
                          fill=fg, font=("", 8))

    def _dash_redraw_stor_chart(self):
        """Draw horizontal disk-usage bars on the canvas."""
        if not hasattr(self, 'dash_stor_canvas'):
            return
        c = self.dash_stor_canvas
        c.delete("all")
        data = getattr(self, '_dash_stor_data', [])
        th = THEMES.get(self.config.get("theme", "Default"), THEMES["Default"])
        bg       = th["bg"]
        fg       = th["fg"]
        ok_color = th["select_bg"]
        bg_color = th.get("trough", "#c8c8c8")
        c.configure(bg=bg)

        if not data:
            c.create_text(10, 20, text="No data — click Refresh",
                          anchor="nw", fill="gray")
            return

        w = c.winfo_width() or 400
        h = c.winfo_height() or 130
        rows = len(data)
        row_h = h / rows
        label_w = 80
        info_w  = 160
        bar_area_w = max(10, w - label_w - info_w - 12)

        for i, (path, used, total) in enumerate(data):
            y  = i * row_h
            cy = y + row_h / 2

            # Drive label
            display = path.rstrip("/\\")
            c.create_text(4, cy, text=display, anchor="w",
                          fill=fg, font=("", 8, "bold"))

            # Background (total capacity)
            bx = label_w
            bar_end = bx + bar_area_w
            c.create_rectangle(bx, y + 5, bar_end, y + row_h - 5,
                                fill=bg_color, outline="gray")

            # Used portion
            if total > 0:
                ratio = used / total
                used_px = max(2, int(ratio * bar_area_w))
                used_px = min(used_px, bar_area_w)
                if ratio > 0.90:
                    fill = "#e05555"
                elif ratio > 0.75:
                    fill = "#e0a000"
                else:
                    fill = ok_color
                c.create_rectangle(bx, y + 5, bx + used_px, y + row_h - 5,
                                   fill=fill, outline="")

            # Info text
            pct = f"{used / total * 100:.0f}%" if total > 0 else "?"
            info = f"{format_size(used)} / {format_size(total)}  ({pct})"
            c.create_text(bar_end + 5, cy, text=info, anchor="w",
                          fill=fg, font=("", 8))

    # --- Adder Tab UI ---
    def create_adder_ui(self):
        # Target Selection
        target_frame = self._tlf(self.adder_tab, "adder.target", padx=10, pady=5)
        target_frame.pack(fill="x", padx=10, pady=5)

        self._tl(target_frame, "adder.select_client").pack(side="left")
        self.client_selector = ttk.Combobox(target_frame, state="readonly", width=30)
        self.client_selector.pack(side="left", padx=5)
        self.update_client_dropdown()

        # File/Folder Selection
        sel_frame = self._tlf(self.adder_tab, "adder.selection", padx=10, pady=10)
        sel_frame.pack(fill="x", padx=10, pady=5)

        self.file_label = self._tl(sel_frame, "adder.no_file_selected", fg="gray", wraplength=500, justify="left")
        self.file_label.pack(pady=5)

        self.link_label = tk.Label(sel_frame, text="", fg="blue", cursor="hand2", wraplength=500, justify="left")
        self.link_label.pack(pady=(0, 5))
        self.link_label.bind("<Button-1>", self._open_link)
        self._current_link = None

        btn_frame = tk.Frame(sel_frame)
        btn_frame.pack(pady=5)
        self._tb(btn_frame, "adder.select_torrent_zip", command=self.select_file).pack(side="left", padx=5)
        self._tb(btn_frame, "adder.select_folder", command=self.select_folder).pack(side="left", padx=5)
        self.info_btn = self._tb(btn_frame, "adder.additional_info", command=self.show_additional_info, state="disabled")
        self.info_btn.pack(side="left", padx=5)
        self._current_torrent_info = None

        # Custom Options
        custom_frame = self._tlf(self.adder_tab, "adder.custom_options", padx=10, pady=5)
        custom_frame.pack(fill="x", padx=10, pady=5)

        # Folder Structure Options
        fs_frame = self._tlf(custom_frame, "adder.folder_structure", padx=5, pady=5)
        fs_frame.pack(fill="x", padx=5, pady=5)

        self.add_create_cat_var = tk.BooleanVar(value=True)
        self._tcb(fs_frame, "adder.create_cat_subfolder", variable=self.add_create_cat_var).pack(anchor="w")

        self.add_create_id_var = tk.BooleanVar(value=True)
        self._tcb(fs_frame, "adder.create_id_subfolder", variable=self.add_create_id_var).pack(anchor="w")

        # Custom Path/Cat/Tags
        opts_frame = tk.Frame(custom_frame)
        opts_frame.pack(fill="x", padx=5, pady=5)

        self.use_custom_var = tk.BooleanVar(value=False)
        self._tcb(opts_frame, "adder.override_cat_path", variable=self.use_custom_var, command=self.toggle_custom_options).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 5))

        self._tl(opts_frame, "adder.category").grid(row=1, column=0, sticky="w")
        self.custom_cat_entry = tk.Entry(opts_frame, width=30)
        self.custom_cat_entry.grid(row=1, column=1, padx=5, pady=2)

        self._tl(opts_frame, "adder.save_path").grid(row=2, column=0, sticky="w")
        self.custom_path_entry = tk.Entry(opts_frame, width=30)
        self.custom_path_entry.grid(row=2, column=1, padx=5, pady=2)

        self.browse_custom_path_btn = self._tb(opts_frame, "common.browse", command=self.browse_custom_path, width=10)
        self.browse_custom_path_btn.grid(row=2, column=2, padx=5)

        # Tags
        self._tl(opts_frame, "adder.tags").grid(row=3, column=0, sticky="w", pady=(5,0))
        self.add_custom_tags_entry = tk.Entry(opts_frame, width=30)
        self.add_custom_tags_entry.grid(row=3, column=1, padx=5, pady=(5,0))
        self._tl(opts_frame, "adder.tags_hint", fg="gray").grid(row=3, column=2, sticky="w", pady=(5,0))

        self.toggle_custom_options() # Initialize state

        # Actions
        action_frame = tk.Frame(self.adder_tab)
        action_frame.pack(fill="x", padx=10, pady=5)

        self.add_btn = self._tb(action_frame, "adder.add_to_qbit", command=self.process_torrent, bg="#dddddd", height=2)
        self.add_btn.pack(side="left", padx=5, fill="x", expand=True)

        self.pause_btn = self._tb(action_frame, "adder.pause", command=self.toggle_pause, state="disabled", height=2)
        self.pause_btn.pack(side="left", padx=5)

        self.stop_btn = self._tb(action_frame, "common.stop", command=self.stop_processing, state="disabled", fg="red", height=2)
        self.stop_btn.pack(side="left", padx=5)

        # --- Adder Progress (hidden until processing) ---
        self.adder_prog_frame = tk.Frame(self.adder_tab)
        self.adder_progress = ttk.Progressbar(self.adder_prog_frame, mode='determinate',
            style="green.Horizontal.TProgressbar")
        self.adder_progress.pack(fill="x", padx=5, pady=(2, 0))
        self.adder_progress_label = tk.Label(self.adder_prog_frame, text="",
            fg="#333333", font=("Segoe UI", 9))
        self.adder_progress_label.pack(fill="x", padx=5, pady=(0, 2))

        # Log
        log_frame = self._tlf(self.adder_tab, "common.log", padx=1, pady=1)
        log_frame.pack(fill="both", expand=True, padx=10, pady=5)

        self.log_area = scrolledtext.ScrolledText(log_frame, height=10, state="disabled")
        self.log_area.pack(fill="both", expand=True)

    def update_client_dropdown(self):
        names = [c["name"] for c in self.config["clients"] if c.get("enabled", False)]
        names.append(t("adder.all_clients"))
        self.client_selector['values'] = names

        # Restore selection
        idx = self.config.get("last_selected_client_index", 0)
        if idx >= len(names): idx = 0
        self.client_selector.current(idx)

        # Keep dashboard client combo in sync
        if hasattr(self, 'dash_client_combo'):
            all_names = ["All"] + [c["name"] for c in self.config.get("clients", [])]
            self.dash_client_combo['values'] = all_names

    def toggle_custom_options(self):
        state = "normal" if self.use_custom_var.get() else "disabled"
        self.custom_cat_entry.config(state=state)
        self.custom_path_entry.config(state=state)
        self.browse_custom_path_btn.config(state=state)
        self.add_custom_tags_entry.config(state=state)

    def browse_custom_path(self):
        path = filedialog.askdirectory()
        if path:
            self.custom_path_entry.delete(0, tk.END)
            self.custom_path_entry.insert(0, path)

    # --- Shared Actions (Select File/Folder) ---
    def select_file(self):
        filepaths = filedialog.askopenfilenames(filetypes=[("Torrent/ZIP", "*.torrent *.zip"), ("All files", "*.*")])
        if filepaths:
            self.selected_files = list(filepaths)
            self.selected_folder_path = None
            
            # Reset UI
            self._current_link = None
            self.link_label.config(text="")
            self._current_torrent_info = None
            self.info_btn.config(state="disabled")

            count = len(self.selected_files)
            if count == 1:
                # Single file logic (keep existing detail view)
                filepath = self.selected_files[0]
                filename = os.path.basename(filepath)
                
                if filepath.lower().endswith('.zip'):
                    self._handle_single_zip(filepath)
                else:
                    self._handle_single_torrent(filepath)
            else:
                # Multiple files selected
                self.file_label.config(text=f"Selected {count} files\nCalculating details...", fg="black")
                self.log(f"Selected {count} files. Analyzing...")
                threading.Thread(target=self._analyze_multiselect_thread, args=(self.selected_files,), daemon=True).start()

    def _analyze_multiselect_thread(self, filepaths, label_prefix=None):
        total_size = 0
        categories = set()
        
        for fp in filepaths:
            try:
                if fp.lower().endswith('.zip'):
                    # Handle ZIP
                    # 1. Size
                    with zipfile.ZipFile(fp, 'r') as z:
                        for name in z.namelist():
                            if name.lower().endswith('.torrent'):
                                try:
                                    info = parse_torrent_info(z.read(name))
                                    total_size += info.get('total_size', 0)
                                except: pass
                    
                    # 2. Category (from filename)
                    cat_name, cat_id, _ = self._extract_zip_info(os.path.basename(fp))
                    if cat_name != "Unknown":
                        categories.add(cat_name)
                    else:
                        categories.add("Unknown (ZIP)")
                        
                else:
                    # Handle Torrent
                    info = parse_torrent_info(fp)
                    total_size += info.get('total_size', 0)
                    
                    tid = info.get('topic_id')
                    if tid:
                        res = self.cat_manager.get_category_for_topic(tid)
                        if res:
                            categories.add(res['full_path'])
                        else:
                             categories.add("Unknown")
                    else:
                        categories.add("Unknown")
            except Exception as e:
                print(f"Error analyzing {fp}: {e}")

        # Update UI
        size_str = format_size(total_size)
        if label_prefix:
            lines = [label_prefix, f"Total Size: {size_str}"]
        else:
            lines = [f"Selected {len(filepaths)} files", f"Total Size: {size_str}"]
        
        if len(categories) > 1:
            lines.append("Categories:")
            # Sort and limit
            sorted_cats = sorted(list(categories))
            if len(sorted_cats) > 5:
                lines.extend([f" - {c}" for c in sorted_cats[:5]])
                lines.append(f" ... and {len(sorted_cats)-5} more")
            else:
                lines.extend([f" - {c}" for c in sorted_cats])
        elif len(categories) == 1:
            lines.append(f"Category: {list(categories)[0]}")
        else:
            lines.append("Category: Unknown")

        self.root.after(0, lambda: self.file_label.config(text="\n".join(lines)))
        self.root.after(0, lambda: self.log(f"Analysis complete. Size: {size_str}, Cats: {len(categories)}"))

    def _handle_single_zip(self, filepath):
        filename = os.path.basename(filepath)
        cat_name, cat_id, count = self._extract_zip_info(filename)

        info_text = f"ZIP: {filename}\nCategory: {cat_name} (ID: {cat_id})\nTorrents: {count}\nTotal size: calculating..."
        self.file_label.config(text=info_text, fg="black")
        self.log(f"Selected ZIP: {filepath}")
        self.log(f" -> Category: {cat_name}, Count: {count}")

        # Calculate total download size in background
        threading.Thread(target=self._calc_zip_size, args=(filepath, info_text), daemon=True).start()

    def _handle_single_torrent(self, filepath):
        # Parse torrent metadata
        filename = os.path.basename(filepath)
        info = parse_torrent_info(filepath)
        torrent_name = info.get('name', '') or filename
        comment = info.get('comment', '')
        topic_id = info.get('topic_id')
        
        # Build display text (without link - that goes in link_label)
        total_size = info.get('total_size', 0)
        file_count = info.get('file_count', 0)
        
        lines = [f"Name: {torrent_name}"]
        if total_size > 0:
            size_str = format_size(total_size)
            lines.append(f"Size: {size_str} ({file_count} file{'s' if file_count != 1 else ''})")
        if topic_id:
            lines.append(f"Topic ID: {topic_id}")
            lines.append("Category: Loading...")
        
        self.file_label.config(text="\n".join(lines), fg="black")
        
        # Store full info for Additional Info button
        self._current_torrent_info = info
        self.info_btn.config(state="normal")
        
        # Set clickable link
        if comment:
            self._current_link = comment
            self.link_label.config(text=comment, font=("TkDefaultFont", 9, "underline"))
        else:
            self._current_link = None
            self.link_label.config(text="", font=("TkDefaultFont", 9))
        
        self.log(f"Selected file: {filepath}")
        if torrent_name:
            self.log(f"  Name: {torrent_name}")
        
        # Look up category in background
        if topic_id:
            threading.Thread(target=self._lookup_torrent_category, args=(topic_id, lines), daemon=True).start()

    def select_folder(self):
        folderpath = filedialog.askdirectory()
        if folderpath:
            self.selected_folder_path = folderpath
            self._current_link = None
            self.link_label.config(text="")
            self._current_torrent_info = None
            self.info_btn.config(state="disabled")
            
            try:
                # Find all torrents
                files = []
                for f in os.listdir(folderpath):
                    if f.lower().endswith('.torrent'):
                        files.append(os.path.join(folderpath, f))
                
                self.selected_files = files
                prefix = f"Folder: {folderpath}\nFound {len(files)} files"
                
                self.file_label.config(text=f"{prefix}\nCalculating details...", fg="black")
                self.log(f"Selected folder: {folderpath} ({len(files)} torrents)")
                
                threading.Thread(target=self._analyze_multiselect_thread, args=(files, prefix), daemon=True).start()

            except Exception as e:
                self.file_label.config(text=f"Folder: {folderpath} (Error reading)", fg="red")
                self.log(f"Error reading folder: {e}")

    def _open_link(self, event=None):
        """Open the current link in the default browser."""
        if self._current_link:
            webbrowser.open(self._current_link)

    def show_additional_info(self):
        """Show a popup with all additional torrent metadata."""
        info = self._current_torrent_info
        if not info:
            return
        
        win = tk.Toplevel(self.root)
        win.title("Torrent Details")
        win.geometry("550x450")
        win.resizable(True, True)
        
        # Metadata section
        meta_frame = tk.LabelFrame(win, text="Metadata", padx=10, pady=5)
        meta_frame.pack(fill="x", padx=10, pady=5)
        
        fields = [
            ("Name", info.get('name', '')),
            ("Created by", info.get('created_by', '')),
            ("Creation date", info.get('creation_date', '')),
            ("Tracker", info.get('tracker', '')),
            ("Piece size", format_size(info.get('piece_size', 0))),
            ("Total size", format_size(info.get('total_size', 0))),
            ("Files", str(info.get('file_count', 0))),
            ("Private", "Yes" if info.get('private') else "No"),
            ("Source", info.get('source', '')),
            ("Comment", info.get('comment', '')),
        ]
        
        for i, (label, value) in enumerate(fields):
            if value:
                tk.Label(meta_frame, text=f"{label}:", font=("TkDefaultFont", 9, "bold"), anchor="w").grid(row=i, column=0, sticky="nw", padx=(0, 10), pady=1)
                val_label = tk.Label(meta_frame, text=value, anchor="w", wraplength=400, justify="left")
                val_label.grid(row=i, column=1, sticky="w", pady=1)
        
        # File list section (for multi-file torrents)
        files = info.get('files', [])
        if files:
            files_frame = tk.LabelFrame(win, text=f"File List ({len(files)} files)", padx=10, pady=5)
            files_frame.pack(fill="both", expand=True, padx=10, pady=5)
            
            file_text = scrolledtext.ScrolledText(files_frame, height=12, state="normal", font=("Consolas", 9))
            file_text.pack(fill="both", expand=True)
            
            for f in files:
                size_str = format_size(f.get('size', 0))
                file_text.insert("end", f"{f.get('path', '?')}  [{size_str}]\n")
            
            file_text.config(state="disabled")
        
        # Close button
        tk.Button(win, text="Close", command=win.destroy, width=10).pack(pady=10)

    def _lookup_torrent_category(self, topic_id, display_lines):
        """Background thread: look up category for a topic ID and update the file label."""
        try:
            result = self.cat_manager.get_category_for_topic(topic_id)
            if result:
                full_path = result["full_path"]
                # Replace the "Category: Loading..." line with full breadcrumb path
                for i, line in enumerate(display_lines):
                    if line.startswith("Category:"):
                        display_lines[i] = f"Category: {full_path}"
                        break
                else:
                    display_lines.append(f"Category: {full_path}")
                self.log(f"  Category: {full_path}")
            else:
                for i, line in enumerate(display_lines):
                    if line.startswith("Category:"):
                        display_lines[i] = "Category: Unknown"
                        break
        except Exception as e:
            self.log(f"  Category lookup error: {e}")
            for i, line in enumerate(display_lines):
                if line.startswith("Category:"):
                    display_lines[i] = "Category: Error"
                    break
        
        # Update label on main thread
        self.root.after(0, lambda: self.file_label.config(text="\n".join(display_lines)))

    # --- Logic ---
    # --- Logic ---
    def process_torrent(self):
        if not self.selected_files and not self.selected_folder_path:
            messagebox.showwarning("Warning", "Please select a torrent/zip file(s) or folder first.")
            return

        # Save selected client index
        self.config["last_selected_client_index"] = self.client_selector.current()
        self.save_config()

        self.stop_event.clear()
        self.running_event.set()
        self.add_btn.config(state="disabled")
        self.stop_btn.config(state="normal")
        self.pause_btn.config(state="normal", text="Pause")

        self.pause_btn.config(state="normal", text="Pause")

        # Capture Custom Options
        use_custom = self.use_custom_var.get()
        custom_cat = self.custom_cat_entry.get().strip()
        custom_path = self.custom_path_entry.get().strip()
        create_cat = self.add_create_cat_var.get()
        create_id = self.add_create_id_var.get()
        custom_tags = self.add_custom_tags_entry.get().strip()

        threading.Thread(target=self._process_thread, args=(use_custom, custom_cat, custom_path, create_cat, create_id, custom_tags)).start()

    def stop_processing(self):
        if messagebox.askyesno("Stop", "Are you sure you want to stop processing?"):
            self.stop_event.set()
            self.running_event.set()
            self.log("Stopping processing...")
            self.stop_btn.config(state="disabled")
            self.pause_btn.config(state="disabled")

    def toggle_pause(self):
        if self.running_event.is_set():
            self.running_event.clear()
            self.pause_btn.config(text="Resume")
            self.log("Pausing...")
        else:
            self.running_event.set()
            self.pause_btn.config(text="Pause")
            self.log("Resuming...")

    def _extract_zip_info(self, filename):
        # Format: torrents_UID_CID_[Count].zip
        # Regex: torrents_\d+_(\d+)_\[(\d+)\]
        match = re.search(r'torrents_\d+_(\d+)_\[(\d+)\]', filename)
        
        cat_name = "Unknown"
        cat_id = "?"
        count = "?"
        
        if match:
            cat_id = match.group(1)
            count_str = match.group(2)
            
            cat_name = self.cat_manager.get_category_name(cat_id, self.config.get("category_ttl_hours", 24) * 3600)
            # Remove characters invalid for Windows paths
            cat_name = re.sub(r'[<>:"/\\|?*]', '_', cat_name)
            count = count_str
        else:
            # Fallback for old format or just torrents_UID_CID_ without count in brackets?
            # Try just CID
            match_simple = re.search(r'torrents_\d+_(\d+)_', filename)
            if match_simple:
                cat_id = match_simple.group(1)
                cat_name = self.cat_manager.get_category_name(cat_id, self.config.get("category_ttl_hours", 24) * 3600)
                cat_name = re.sub(r'[<>:"/\\|?*]', '_', cat_name)
        
        return cat_name, cat_id, count

    def _calc_zip_size(self, zip_path, base_info_text):
        """Background: open ZIP, parse each .torrent, sum total download sizes."""
        try:
            total_size = 0
            count = 0
            with zipfile.ZipFile(zip_path, 'r') as z:
                for name in z.namelist():
                    if name.lower().endswith('.torrent'):
                        try:
                            content = z.read(name)
                            info = parse_torrent_info(content)
                            total_size += info.get('total_size', 0)
                            count += 1
                        except Exception:
                            pass
            size_str = format_size(total_size)
            updated_text = base_info_text.replace(
                "Total size: calculating...",
                f"Total size: {size_str} ({count} torrents)")
            self.root.after(0, lambda: self.file_label.config(text=updated_text))
            self.log(f" -> Total download size: {size_str}")
        except Exception as e:
            updated_text = base_info_text.replace("Total size: calculating...", "Total size: error")
            self.root.after(0, lambda: self.file_label.config(text=updated_text))

    def _process_thread(self, use_custom, custom_cat, custom_path, create_cat, create_id, custom_tags):
        # Determine targets
        selected_idx = self.client_selector.current()
        num_options = len(self.client_selector['values'])
        
        target_clients = []
        if selected_idx == num_options - 1: # "All Clients"
            target_clients = self.config["clients"]
        else:
            target_clients = [self.config["clients"][selected_idx]]

        if not target_clients:
            self.log("No clients configured!")
            self.reset_buttons()
            return
            
        # Prepare workload
        # Structure: list of items to process. Item = {'type': 'file'|'zip_entry', 'path': ..., 'content': bytes, 'category_subpath': str}
        work_items = []

        if self.selected_files:
            # Handle multiple selected files
            for fpath in self.selected_files:
                if fpath.lower().endswith('.zip'):
                    # Handle Single ZIP - Updated to use helper
                    items = self._parse_zip_file(fpath)
                    work_items.extend(items)
                else:
                    # Normal Torrent
                    work_items.append({'type': 'file', 'path': fpath, 'content': None, 'category_subpath': ''})

        elif self.selected_folder_path:
            try:
                for f in os.listdir(self.selected_folder_path):
                    full_path = os.path.join(self.selected_folder_path, f)
                    if f.lower().endswith(".torrent"):
                        work_items.append({'type': 'file', 'path': full_path, 'content': None, 'category_subpath': ''})
            except Exception as e:
                self.log(f"Error reading folder: {e}")
                self.reset_buttons()
                return

        if not work_items:
            self.log("No torrents found to process.")
            self.reset_buttons()
            return
            
        total_ops = len(work_items) * len(target_clients)
        self.log(f"Starting job: {len(work_items)} item(s) on {len(target_clients)} client(s)...")

        # Processing Loop
        job_start = time.time()
        self._adder_start_time = job_start
        self.root.after(0, lambda: self.adder_prog_frame.pack(fill="x", padx=10,
            before=self.log_area.master))
        success_count = 0
        fail_count = 0
        processed = 0
        total_items = len(work_items)
        for item in work_items:
            # Extract content if needed
            torrent_content = item.get('content')
            torrent_path = item.get('path')
            category_subpath = item.get('category_subpath', '')

            # Read file content if not already in memory (from ZIP)
            if not torrent_content and torrent_path:
                try:
                    with open(torrent_path, 'rb') as f:
                        torrent_content = f.read()
                except Exception as e:
                    self.log(f"Failed to read {os.path.basename(torrent_path)}: {e}")
                    continue

            # Check ID from content
            extracted_id = self.extract_id_from_bytes(torrent_content, os.path.basename(torrent_path))

            # If no category yet (individual file / folder), look it up from the topic
            if not category_subpath and extracted_id:
                try:
                    result = self.cat_manager.get_category_for_topic(extracted_id)
                    if result:
                        category_subpath = result["category"]
                except Exception as e:
                    self.log(f"Could not resolve category for topic {extracted_id}: {e}")

            for client in target_clients:
                # Flow Control
                if self.stop_event.is_set(): break
                if not self.running_event.is_set():
                    self.log("Paused...")
                    self.running_event.wait()
                    if self.stop_event.is_set(): break
                    self.log("Resuming...")

                ok = self._add_torrent_content_to_client(client, torrent_content, torrent_path, category_subpath, extracted_id, use_custom, custom_cat, custom_path, create_cat, create_id, custom_tags)
                if ok:
                    success_count += 1
                else:
                    fail_count += 1

            processed += 1
            self._update_progress(self.adder_progress, self.adder_progress_label,
                processed, total_items, "Adding", self._adder_start_time)

            if self.stop_event.is_set():
                self.log("Stopped by user.")
                break

        elapsed = time.time() - job_start
        minutes, seconds = divmod(int(elapsed), 60)
        time_str = f"{minutes}m {seconds}s" if minutes else f"{seconds}s"
        summary = f"Done: {success_count} added"
        if fail_count:
            summary += f", {fail_count} failed"
        summary += f" ({time_str})"
        self.log(summary)
        self.root.after(0, self.adder_prog_frame.pack_forget)
        self.root.after(0, lambda: self.status_bar.config(text="Ready"))
        messagebox.showinfo("Done", summary)

        self.reset_buttons()

    def _parse_zip_file(self, zip_path):
        items = []
        filename = os.path.basename(zip_path)
        
        # Use helper to get category info
        cat_name, cat_id, count = self._extract_zip_info(filename)
        
        category_subpath = ""
        if cat_name != "Unknown":
            category_subpath = cat_name
            self.log(f"ZIP Detected: Category ID {cat_id} -> '{cat_name}'")
        else:
            self.log(f"ZIP Detected: Could not parse category from filename. Using default path.")

        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                # Optional: Verify actual count vs filename count?
                # detected_count = 0
                for name in z.namelist():
                    if name.lower().endswith('.torrent'):
                        # detected_count += 1
                        content = z.read(name)
                        items.append({
                            'type': 'zip_entry',
                            'path': name, # just the name inside zip
                            'content': content,
                            'category_subpath': category_subpath
                        })
            self.log(f"Extracted {len(items)} torrents from ZIP.")
        except Exception as e:
            self.log(f"Error reading ZIP: {e}")
        
        return items

    def calculate_torrent_hash(self, file_content):
        """Calculate Info Hash (SHA1 of info dict) from raw bytes."""
        try:
            # We need the raw bytes of the 'info' dictionary.
            # bdecode returns a python dict, but we need the original bencoded substring.
            # A simple bdecode doesn't give us offsets easily unless modified.
            # BUT we can search for "4:info" and then decode just that part to find its length?
            
            # Better approach:
            # Find "4:info"
            # The value starts immediately after.
            # We can use our bdecode which returns end index!
            
            # Find first occurrence of "4:info"
            # Note: keys in bencode are sorted, but "info" is a key in the root dict.
            # It's safer to decode the whole thing and track offsets, 
            # OR honestly, just find "4:info" and check if it looks like a key.
            # Given it's a top level dict, it should be safe to finding "4:info"
            
            start_marker = b'4:info'
            idx = file_content.find(start_marker)
            if idx == -1: return None
            
            val_start = idx + len(start_marker)
            
            # Now decode from val_start to get the end index
            _, next_idx = bdecode(file_content, val_start)
            
            # The raw info dict bytes
            info_bytes = file_content[val_start:next_idx]
            
            # SHA1 hash
            sha1 = hashlib.sha1(info_bytes).hexdigest()
            return sha1.lower()
            
        except Exception as e:
            print(f"Hash calc error: {e}")
            return None

    def extract_id_from_bytes(self, content, filename_for_log):
        """Extract topic ID from torrent bytes using proper bencode parsing."""
        try:
            info = parse_torrent_info(content)
            topic_id = info.get('topic_id')
            if topic_id:
                return topic_id
        except Exception:
            pass
        return None

    def _add_torrent_content_to_client(self, client, content, filename_display, category_subpath, extracted_id, use_custom, custom_cat, custom_path, create_cat, create_id, custom_tags):
        name = client["name"]
        url = client["url"]
        base_path = client["base_save_path"]
        
        # Auth Resolution
        if client["use_global_auth"] and self.config["global_auth"]["enabled"]:
            user = self.config["global_auth"]["username"]
            pw = self.config["global_auth"]["password"]
        else:
            user = client["username"]
            pw = client["password"]
            
        display_name = os.path.basename(filename_display)
        if extracted_id:
            display_name = f"{display_name} (ID:{extracted_id})"
        
        try:
            self.log(f"[{name}] Adding: {display_name}")
            
            session = requests.Session()
            proxies = self.get_requests_proxies()
            if proxies:
                session.proxies.update(proxies)

            # Auth
            try:
                resp = session.get(f"{url}/api/v2/app/version", timeout=10)
                if resp.status_code != 200:
                    resp = session.post(f"{url}/api/v2/auth/login", data={"username": user, "password": pw}, timeout=10)
                    if resp.status_code != 200 or resp.text != "Ok.":
                        self.log(f"[{name}] Auth Failed!")
                        return False
            except Exception as e:
                self.log(f"[{name}] Connection Error: {e}")
                return False

            # Construct Path and Category
            save_path = ""
            final_cat = ""

            if use_custom:
                # Custom Override Logic
                # Use custom path if provided, else base
                save_path = custom_path.replace("\\", "/") if custom_path else base_path.replace("\\", "/")
                
                # Use custom cat if provided, else detected
                final_cat = custom_cat if custom_cat else category_subpath
                
                self.log(f"  -> Using Custom Path: {save_path}")
                self.log(f"  -> Using Custom Cat: {final_cat}")

            else:
                # Standard Logic with Folder Structure Options
                # Start with base path
                path_parts = [base_path]
                
                # Append category subfolder if enabled and available
                if create_cat and category_subpath:
                    path_parts.append(category_subpath)
                
                # Append ID subfolder if enabled and available
                if create_id and extracted_id:
                    path_parts.append(extracted_id)
                elif create_id and not extracted_id:
                    # Fallback if ID wanted but not found: check if filename resembles ID or just skip?
                    # Current behavior: fallback to filename for folder? 
                    # The original behavior was: if extracted_id, use it. Else use filename.
                    # Let's keep logic: if create_id is true, we want a per-torrent subfolder.
                    # If we have ID, use ID. If not, use filename (without ext).
                    fname = os.path.basename(filename_display)
                    path_parts.append(os.path.splitext(fname)[0])
                    
                save_path = os.path.join(*path_parts).replace("\\", "/")
                final_cat = category_subpath

            files = {'torrents': (os.path.basename(filename_display), content)}
            data = {'savepath': save_path, 'paused': 'false', 'root_folder': 'true'}
            
            # Set qBittorrent category
            if final_cat:
                data['category'] = final_cat

            # Set Tags
            if custom_tags:
                data['tags'] = custom_tags
            
            resp = session.post(f"{url}/api/v2/torrents/add", files=files, data=data, timeout=30)
            
            if resp.status_code == 200 and resp.text == "Ok.":
                self.log(f"[{name}] Success -> {save_path}")
                return True
            else:
                if resp.text.strip().lower() == "fails.":
                    self.log(f"[{name}] Failed: Already added")
                else:
                    self.log(f"[{name}] Failed: {resp.text}")
                return False

        except Exception as e:
            self.log(f"[{name}] Error: {e}")
            return False

    def reset_buttons(self):
        self.add_btn.config(state="normal")
        self.stop_btn.config(state="disabled")
        self.pause_btn.config(state="disabled", text="Pause")

    # ===================================================================
    # UPDATE TORRENTS TAB
    # ===================================================================

    def _get_qbit_session(self, client_conf):
        """Helper to get an authenticated requests.Session for a client."""
        try:
            url = client_conf["url"]
            if client_conf.get("use_global_auth") and self.config["global_auth"]["enabled"]:
                user = self.config["global_auth"]["username"]
                pw = self.config["global_auth"]["password"]
            else:
                user = client_conf.get("username", "")
                pw = client_conf.get("password", "")

            s = requests.Session()
            # Try version first to see if auth needed/valid
            try:
                resp = s.get(f"{url}/api/v2/app/version", timeout=5)
                if resp.status_code == 200:
                    return s
            except: pass # Proceed to login attempt
            
            # Login
            resp = s.post(f"{url}/api/v2/auth/login", data={"username": user, "password": pw}, timeout=10)
            if resp.status_code == 200 and resp.text != "Fails.":
                return s
            else:
                print(f"Login failed: {resp.status_code} {resp.text}")
                return None
        except Exception as e:
            print(f"Connection error: {e}")
            return None

    # --- Torrent Cache Helpers ---

    def _cache_get(self, client_name):
        """Get cached torrent list for a client. Lazy-loads from DB on first access.
        Returns (torrents, timestamp) or (None, None) if stale/missing."""
        entry = self.torrent_cache.get(client_name)
        if entry and "torrents" not in entry:
            # Metadata-only entry from startup — lazy-load the full data now
            ttl_hours = self.config.get("torrent_cache_ttl_hours", 6)
            full = self.db_manager.load_torrent_cache_single(client_name, ttl_hours)
            if full:
                self.torrent_cache[client_name] = full
                entry = full
            else:
                self.torrent_cache.pop(client_name, None)
                return None, None
        if not entry:
            return None, None
        ttl_hours = self.config.get("torrent_cache_ttl_hours", 6)
        age = time.time() - entry["timestamp"]
        if age > ttl_hours * 3600:
            return None, None  # Stale
        return entry["torrents"], entry["timestamp"]

    def _cache_put(self, client_name, torrents):
        """Store torrent list in cache for a client."""
        ts = time.time()
        self.torrent_cache[client_name] = {"torrents": torrents, "timestamp": ts}
        self.db_manager.save_torrent_cache(client_name, ts, torrents)
        return ts

    def _cache_load_from_disk(self):
        """Load torrent cache metadata from DB on startup (timestamps only, no JSON parsing).
        Full torrent lists are lazy-loaded on first _cache_get() call."""
        self.torrent_cache = self.db_manager.load_torrent_cache_meta()

    def _cache_format_time(self, timestamp):
        """Format a cache timestamp to human-readable string."""
        if not timestamp:
            return "never"
        dt = datetime.datetime.fromtimestamp(timestamp)
        return fmt_dt(dt, "datetime_sec")

    def _cache_invalidate(self, client_name=None):
        """Invalidate cache for a specific client, or all if None."""
        if client_name:
            self.torrent_cache.pop(client_name, None)
        else:
            self.torrent_cache.clear()
        self.db_manager.delete_torrent_cache(client_name)

    def _update_cache_labels(self, client_name, timestamp):
        """Update all 'last updated' labels across tabs for this client."""
        time_str = self._cache_format_time(timestamp)
        text = t("common.list_updated", time=time_str)

        # Update labels only if the tab is showing the same client
        if hasattr(self, 'remover_cache_label'):
            r_idx = self.remover_client_selector.current()
            if r_idx >= 0 and r_idx < len(self.config["clients"]):
                if self.config["clients"][r_idx]["name"] == client_name:
                    self.remover_cache_label.config(text=text, fg="gray")

        if hasattr(self, 'repair_cache_label'):
            r_idx = self.repair_client_selector.current()
            if r_idx >= 0 and r_idx < len(self.config["clients"]):
                if self.config["clients"][r_idx]["name"] == client_name:
                    self.repair_cache_label.config(text=text, fg="gray")

        if hasattr(self, 'mover_cache_label'):
            r_idx = self.mover_client_selector.current()
            if r_idx >= 0 and r_idx < len(self.config["clients"]):
                if self.config["clients"][r_idx]["name"] == client_name:
                    self.mover_cache_label.config(text=text, fg="gray")

        if hasattr(self, 'scanner_cache_label'):
            r_idx = self.scanner_client_selector.current()
            if r_idx >= 0 and r_idx < len(self.config["clients"]):
                if self.config["clients"][r_idx]["name"] == client_name:
                    self.scanner_cache_label.config(text=text, fg="gray")

    def _open_topic_by_hash(self, info_hash):
        """Resolve hash → topic_id via Rutracker API and open the forum topic in browser."""
        def _lookup():
            try:
                resp = requests.get("https://api.rutracker.cc/v1/get_topic_id",
                    params={"by": "hash", "val": info_hash.upper()},
                    proxies=self.get_requests_proxies(),
                    timeout=10)
                if resp.status_code == 200:
                    result = resp.json().get("result", {})
                    tid = result.get(info_hash.upper()) or result.get(info_hash.lower())
                    if tid:
                        webbrowser.open(f"https://rutracker.org/forum/viewtopic.php?t={tid}")
                        return
            except Exception:
                pass
            self.root.after(0, lambda: messagebox.showinfo("Info", "Could not resolve topic ID for this torrent."))
        threading.Thread(target=_lookup, daemon=True).start()

    def _show_cache_time_for_client(self, client_name, label_widget):
        """Show the cache time on a specific label for a given client name."""
        entry = self.torrent_cache.get(client_name)
        if entry:
            time_str = self._cache_format_time(entry["timestamp"])
            label_widget.config(text=t("common.list_updated", time=time_str), fg="gray")
        else:
            label_widget.config(text=t("common.list_updated_never"), fg="gray")

    def _remover_show_cache_label(self):
        if hasattr(self, 'remover_cache_label') and hasattr(self, 'remover_client_selector'):
            idx = self.remover_client_selector.current()
            if 0 <= idx < len(self.config["clients"]):
                self._show_cache_time_for_client(self.config["clients"][idx]["name"], self.remover_cache_label)

    def _repair_show_cache_label(self):
        if hasattr(self, 'repair_cache_label') and hasattr(self, 'repair_client_selector'):
            idx = self.repair_client_selector.current()
            if 0 <= idx < len(self.config["clients"]):
                self._show_cache_time_for_client(self.config["clients"][idx]["name"], self.repair_cache_label)

    def _mover_show_cache_label(self):
        if hasattr(self, 'mover_cache_label') and hasattr(self, 'mover_client_selector'):
            idx = self.mover_client_selector.current()
            if 0 <= idx < len(self.config["clients"]):
                self._show_cache_time_for_client(self.config["clients"][idx]["name"], self.mover_cache_label)

    def _scanner_show_cache_label(self):
        if hasattr(self, 'scanner_cache_label') and hasattr(self, 'scanner_client_selector'):
            idx = self.scanner_client_selector.current()
            if 0 <= idx < len(self.config["clients"]):
                self._show_cache_time_for_client(self.config["clients"][idx]["name"], self.scanner_cache_label)

    def create_updater_ui(self):
        # --- Scan Controls ---
        ctrl_frame = self._tlf(self.updater_tab, "common.scan_controls", padx=10, pady=5)
        ctrl_frame.pack(fill="x", padx=10, pady=5)

        self._tl(ctrl_frame, "common.client").pack(side="left")
        self.updater_client_selector = ttk.Combobox(ctrl_frame, state="readonly", width=25)
        self.updater_client_selector.pack(side="left", padx=5)

        self.updater_scan_btn = self._tb(ctrl_frame, "common.scan_now", command=self.updater_start_scan)
        self.updater_scan_btn.pack(side="left", padx=5)

        self.updater_stop_btn = self._tb(ctrl_frame, "common.stop", command=self.updater_stop_scan, state="disabled")
        self.updater_stop_btn.pack(side="left", padx=5)

        self.updater_only_errored_cb = self._tcb(
            ctrl_frame, "updater.only_unreg", variable=self.updater_only_errored)
        self.updater_only_errored_cb.pack(side="left", padx=(15, 5))

        # --- Progress (hidden until scan) ---
        self.updater_prog_frame = tk.Frame(self.updater_tab)
        self.updater_progress = ttk.Progressbar(self.updater_prog_frame, mode='determinate', style="green.Horizontal.TProgressbar")
        self.updater_progress.pack(fill="x", padx=5, pady=(2, 0))
        self.updater_progress_label = tk.Label(self.updater_prog_frame, text="", fg="#333333", font=("Segoe UI", 9))
        self.updater_progress_label.pack(fill="x", padx=5, pady=(0, 2))

        # --- Results Treeview ---
        results_frame = self._tlf(self.updater_tab, "updater.unreg_torrents", padx=5, pady=5)
        results_frame.pack(fill="both", expand=True, padx=10, pady=5)

        tree_container = tk.Frame(results_frame)
        tree_container.pack(fill="both", expand=True)

        self.updater_tree = ttk.Treeview(
            tree_container,
            columns=("name", "path", "status", "reason", "topic_id", "new_topic"),
            show="headings",
            selectmode="extended"
        )
        self._tr_heading(self.updater_tree, "name", "updater.torrent_name")
        self._tr_heading(self.updater_tree, "path", "common.path")
        self._tr_heading(self.updater_tree, "status", "common.status")
        self._tr_heading(self.updater_tree, "reason", "updater.reason")
        self._tr_heading(self.updater_tree, "topic_id", "updater.topic_id")
        self._tr_heading(self.updater_tree, "new_topic", "updater.new_topic")
        self.updater_tree.column("name", width=250, minwidth=150)
        self.updater_tree.column("path", width=200, minwidth=80)
        self.updater_tree.column("status", width=80, anchor="center")
        self.updater_tree.column("reason", width=120, anchor="center")
        self.updater_tree.column("topic_id", width=75, anchor="center")
        self.updater_tree.column("new_topic", width=75, anchor="center")

        tree_scroll = ttk.Scrollbar(tree_container, orient="vertical", command=self.updater_tree.yview)
        tree_scroll_x = ttk.Scrollbar(tree_container, orient="horizontal", command=self.updater_tree.xview)
        self.updater_tree.configure(yscrollcommand=tree_scroll.set, xscrollcommand=tree_scroll_x.set)
        
        tree_scroll.pack(side="right", fill="y")
        tree_scroll_x.pack(side="bottom", fill="x")
        self.updater_tree.pack(side="left", fill="both", expand=True)

        # Tag colors
        self.updater_tree.tag_configure("updated", foreground="dark green")
        self.updater_tree.tag_configure("consumed", foreground="dark orange")
        self.updater_tree.tag_configure("deleted", foreground="red")
        self.updater_tree.tag_configure("unknown", foreground="gray")

        # Double-click to open original topic; right-click menu for new topic
        self.updater_tree.bind("<Double-1>", self._updater_open_topic)
        self._updater_tree_menu = tk.Menu(self.updater_tree, tearoff=0)
        self.updater_tree.bind("<Button-3>", self._updater_tree_right_click)

        self.updater_summary_label = self._tl(results_frame, "updater.switch_hint", fg="gray")
        self.updater_summary_label.pack(anchor="w", pady=(5, 0))

        # --- Action Buttons ---
        action_frame = tk.Frame(self.updater_tab)
        action_frame.pack(fill="x", padx=10, pady=5)

        self.updater_readd_keep_btn = self._tb(
            action_frame, "updater.keep_files",
            command=lambda: self.updater_perform_action("readd_keep"), state="disabled")
        self.updater_readd_keep_btn.pack(side="left", padx=3, fill="x", expand=True)

        self.updater_consumed_btn = self._tb(
            action_frame, "updater.consumed",
            command=lambda: self.updater_perform_action("update_consumed"), state="disabled", fg="dark orange")
        self.updater_consumed_btn.pack(side="left", padx=3, fill="x", expand=True)

        self.updater_readd_redown_btn = self._tb(
            action_frame, "updater.redownload",
            command=lambda: self.updater_perform_action("readd_redownload"), state="disabled")
        self.updater_readd_redown_btn.pack(side="left", padx=3, fill="x", expand=True)

        self.updater_skip_btn = self._tb(
            action_frame, "updater.remove_qbit",
            command=lambda: self.updater_perform_action("skip_delete"), state="disabled")
        self.updater_skip_btn.pack(side="left", padx=3, fill="x", expand=True)

        self.updater_delete_btn = self._tb(
            action_frame, "updater.delete_files",
            command=lambda: self.updater_perform_action("delete_files"), state="disabled", fg="red")
        self.updater_delete_btn.pack(side="left", padx=3, fill="x", expand=True)

        # --- Updater Log ---
        log_frame = self._tlf(self.updater_tab, "updater.update_log", padx=1, pady=1)
        log_frame.pack(fill="both", expand=True, padx=10, pady=5)

        self.updater_log_area = scrolledtext.ScrolledText(log_frame, height=6, state="disabled")
        self.updater_log_area.pack(fill="both", expand=True)

        # Right-click context menu for copy
        updater_log_menu = tk.Menu(self.updater_log_area, tearoff=0)
        updater_log_menu.add_command(label=t("common.copy"), accelerator="Ctrl+C",
            command=lambda: self.updater_log_area.event_generate("<<Copy>>"))
        updater_log_menu.add_command(label=t("common.select_all"), accelerator="Ctrl+A",
            command=lambda: (self.updater_log_area.tag_add("sel", "1.0", "end"),))
        self.updater_log_area.bind("<Button-3>",
            lambda e: updater_log_menu.tk_popup(e.x_root, e.y_root))

        self.update_updater_client_dropdown()
        self.update_repair_client_dropdown()
        self.update_mover_client_dropdown()
        self.update_scanner_client_dropdown()

    def updater_log(self, message):
        """Log to the updater tab's own log area (thread-safe)."""
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {message}"
        _log_to_file("updater", message)
        def _write():
            self.updater_log_area.config(state="normal")
            self.updater_log_area.insert(tk.END, line + "\n")
            self.updater_log_area.see(tk.END)
            self.updater_log_area.config(state="disabled")
        self.root.after(0, _write)

    def _updater_open_topic(self, event):
        """Open original Rutracker topic page on double-click."""
        sel = self.updater_tree.selection()
        if not sel:
            return
        item = self.updater_tree.item(sel[0])
        vals = item.get("values", [])
        if not vals:
            return
        # vals: (name, path, status, reason, topic_id, new_topic)
        topic_id = vals[4] if len(vals) > 4 else ""
        if topic_id and str(topic_id) != "N/A":
            webbrowser.open(f"https://rutracker.org/forum/viewtopic.php?t={topic_id}")

    def _updater_tree_right_click(self, event):
        """Right-click context menu on treeview row."""
        row_id = self.updater_tree.identify_row(event.y)
        if not row_id:
            return
        self.updater_tree.selection_set(row_id)
        item = self.updater_tree.item(row_id)
        vals = item.get("values", [])
        if not vals:
            return

        menu = self._updater_tree_menu
        menu.delete(0, "end")

        # vals: (name, path, status, reason, topic_id, new_topic)
        topic_id = vals[4] if len(vals) > 4 else ""
        new_topic = vals[5] if len(vals) > 5 else ""

        if topic_id and str(topic_id) != "N/A":
            menu.add_command(label=f"Open original topic (t/{topic_id})",
                command=lambda t=topic_id: webbrowser.open(
                    f"https://rutracker.org/forum/viewtopic.php?t={t}"))
        if new_topic and str(new_topic).strip():
            menu.add_command(label=f"Open new topic (t/{new_topic})",
                command=lambda t=new_topic: webbrowser.open(
                    f"https://rutracker.org/forum/viewtopic.php?t={t}"))

        if menu.index("end") is not None:
            menu.tk_popup(event.x_root, event.y_root)

    def _on_tab_changed(self, event):
        if self.is_initializing:
            return
        selected = self.notebook.select()
        tab_widget = self.root.nametowidget(selected)

        # --- Dashboard: refresh on every visit ---
        if tab_widget is self.dashboard_tab:
            self._dashboard_refresh()
            return

        # --- Update Torrents tab: auto-scan logic ---
        if tab_widget is self.updater_tab:
            auto_enabled = self.config.get("auto_update_enabled", False)
            interval_min = self.config.get("auto_update_interval_min", 60)
            should_scan = False
            now = time.time()
            if not getattr(self, 'has_initial_scan_done', False):
                self.has_initial_scan_done = True
                if auto_enabled:
                    should_scan = True
            elif auto_enabled:
                 last_scan = getattr(self, 'last_update_scan_time', 0)
                 if (now - last_scan) > (interval_min * 60):
                     should_scan = True
            if should_scan:
                self.last_update_scan_time = now
                self.updater_start_scan()
            else:
                self.update_updater_client_dropdown()

        # --- Auto-populate tabs from cache when tree is empty ---
        if tab_widget is self.remover_tab:
            if not self.remover_tree.get_children():
                self.remover_load_torrents()
            else:
                self._remover_show_cache_label()

        elif tab_widget is self.repair_tab:
            self._repair_show_cache_label()

        elif tab_widget is self.mover_tab:
            self._mover_show_cache_label()

        elif tab_widget is self.scanner_tab:
            self._scanner_show_cache_label()

    def update_updater_client_dropdown(self):
        if hasattr(self, 'updater_client_selector'):
            names = [c["name"] for c in self.config["clients"] if c.get("enabled", False)]
            self.updater_client_selector['values'] = names
            idx = self.config.get("last_selected_client_index", 0)
            if idx >= len(names):
                idx = 0
            if names:
                self.updater_client_selector.current(idx)

    def _updater_set_action_buttons(self, state):
        self.updater_readd_keep_btn.config(state=state)
        self.updater_consumed_btn.config(state=state)
        self.updater_readd_redown_btn.config(state=state)
        self.updater_skip_btn.config(state=state)
        self.updater_delete_btn.config(state=state)

    # ===================================================================
    # SEARCH TAB
    # ===================================================================

    def create_search_ui(self):
        # Controls
        ctrl_frame = self._tlf(self.search_tab, "search.search", padx=10, pady=5)
        ctrl_frame.pack(fill="x", padx=10, pady=5)

        self._tl(ctrl_frame, "search.query").pack(side="left")
        self.search_entry = tk.Entry(ctrl_frame, width=40)
        self.search_entry.pack(side="left", padx=5)
        self.search_entry.bind("<Return>", lambda e: self.perform_search())
        
        self.search_entry.bind("<Return>", lambda e: self.perform_search())
        
        self._tl(ctrl_frame, "search.type").pack(side="left")
        self.search_type_combo = ttk.Combobox(ctrl_frame, state="readonly", width=15)
        self.search_type_combo['values'] = [t("search.type_name"), t("search.type_topic"), t("search.type_hash")]
        self.search_type_combo.current(0)
        self.search_type_combo.pack(side="left", padx=5)
        
        self.search_btn = self._tb(ctrl_frame, "search.search", command=self.perform_search)
        self.search_btn.pack(side="left", padx=10)

        # Results
        res_frame = self._tlf(self.search_tab, "search.results", padx=5, pady=5)
        res_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        cols = ("id", "name", "size", "seeds", "leech", "category")
        self.search_tree = ttk.Treeview(res_frame, columns=cols, show="headings", selectmode="browse")
        
        self._tr_heading(self.search_tree, "id", "search.id")
        self._tr_heading(self.search_tree, "name", "common.name")
        self._tr_heading(self.search_tree, "size", "common.size")
        self._tr_heading(self.search_tree, "seeds", "search.seeds")
        self._tr_heading(self.search_tree, "leech", "search.leech")
        self._tr_heading(self.search_tree, "category", "common.category")
        
        self.search_tree.column("id", width=60, stretch=False)
        self.search_tree.column("name", width=400)
        self.search_tree.column("size", width=80, stretch=False)
        self.search_tree.column("seeds", width=40, stretch=False)
        self.search_tree.column("leech", width=40, stretch=False)
        self.search_tree.column("category", width=150)
        
        scroll = ttk.Scrollbar(res_frame, orient="vertical", command=self.search_tree.yview)
        scroll_x = ttk.Scrollbar(res_frame, orient="horizontal", command=self.search_tree.xview)
        self.search_tree.configure(yscrollcommand=scroll.set, xscrollcommand=scroll_x.set)
        
        scroll.pack(side="right", fill="y")
        scroll_x.pack(side="bottom", fill="x")
        self.search_tree.pack(side="left", fill="both", expand=True)
        self.search_tree.bind("<Double-1>", self._search_on_double_click)

        # Actions
        act_frame = tk.Frame(self.search_tab)
        act_frame.pack(fill="x", padx=10, pady=5)
        
        self._tb(act_frame, "search.download", command=self.download_selected_torrent).pack(side="left", padx=5)
        self._tb(act_frame, "search.download_add", command=self.download_and_add_torrent).pack(side="left", padx=5)
        
        self.search_status = tk.Label(act_frame, text="", fg="gray")
        self.search_status.pack(side="left", padx=10)

    def create_bitrot_ui(self):
        # 1. Controls
        top_frame = tk.Frame(self.bitrot_tab)
        top_frame.pack(fill="x", padx=10, pady=5)
        
        self._tl(top_frame, "common.client").pack(side="left")
        self.bitrot_client_combo = ttk.Combobox(top_frame, state="readonly", width=15)
        self.bitrot_client_combo.pack(side="left", padx=5)
        self.bitrot_client_combo.bind("<<ComboboxSelected>>", self._bitrot_on_client_select)

        self._tl(top_frame, "bitrot.older_than").pack(side="left", padx=(15, 0))
        self.bitrot_age_spinbox = tk.Spinbox(top_frame, from_=0, to=9999, width=5)
        self.bitrot_age_spinbox.delete(0, "end")
        self.bitrot_age_spinbox.insert(0, "30")
        self.bitrot_age_spinbox.pack(side="left", padx=5)

        self.bitrot_load_btn = self._tb(top_frame, "bitrot.load_torrents", command=self.bitrot_load_torrents)
        self.bitrot_load_btn.pack(side="left", padx=15)

        self.bitrot_scan_btn = self._tb(top_frame, "bitrot.start_check", command=self.bitrot_start_check)
        self.bitrot_scan_btn.pack(side="left", padx=5)

        self.bitrot_cancel_btn = self._tb(top_frame, "bitrot.stop_check", state=tk.DISABLED, command=self.bitrot_cancel_check)
        self.bitrot_cancel_btn.pack(side="left", padx=5)

        # 2. Treeview
        tree_frame = tk.Frame(self.bitrot_tab)
        tree_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        cols = ("topic_id", "name", "size", "added_on", "last_active", "up_speed", "seed", "path", "last_checked", "status", "progress", "bitrot_state")
        self.bitrot_tree = ttk.Treeview(tree_frame, columns=cols, show="headings", selectmode="extended")
        
        self._tr_heading(self.bitrot_tree, "topic_id", "bitrot.topic_id")
        self._tr_heading(self.bitrot_tree, "name", "common.name", command=lambda: self.sort_tree(self.bitrot_tree, "name", False))
        self._tr_heading(self.bitrot_tree, "size", "common.size", command=lambda: self.sort_tree(self.bitrot_tree, "size", False))
        self._tr_heading(self.bitrot_tree, "added_on", "bitrot.added", command=lambda: self.sort_tree(self.bitrot_tree, "added_on", False))
        self._tr_heading(self.bitrot_tree, "last_active", "bitrot.last_active", command=lambda: self.sort_tree(self.bitrot_tree, "last_active", False))
        self._tr_heading(self.bitrot_tree, "up_speed", "bitrot.up_speed", command=lambda: self.sort_tree(self.bitrot_tree, "up_speed", False))
        self._tr_heading(self.bitrot_tree, "seed", "bitrot.seeds", command=lambda: self.sort_tree(self.bitrot_tree, "seed", False))
        self._tr_heading(self.bitrot_tree, "path", "common.path", command=lambda: self.sort_tree(self.bitrot_tree, "path", False))
        self._tr_heading(self.bitrot_tree, "last_checked", "bitrot.last_checked", command=lambda: self.sort_tree(self.bitrot_tree, "last_checked", False))
        self._tr_heading(self.bitrot_tree, "status", "common.status")
        self._tr_heading(self.bitrot_tree, "progress", "bitrot.progress")
        self._tr_heading(self.bitrot_tree, "bitrot_state", "bitrot.bitrot_state", command=lambda: self.sort_tree(self.bitrot_tree, "bitrot_state", False))

        self.bitrot_tree.column("topic_id", width=80, stretch=False)
        self.bitrot_tree.column("name", width=300)
        self.bitrot_tree.column("size", width=80, stretch=False)
        self.bitrot_tree.column("added_on", width=120, stretch=False)
        self.bitrot_tree.column("last_active", width=120, stretch=False)
        self.bitrot_tree.column("up_speed", width=80, stretch=False)
        self.bitrot_tree.column("seed", width=50, stretch=False)
        self.bitrot_tree.column("path", width=150, stretch=False)
        self.bitrot_tree.column("last_checked", width=120, stretch=False)
        self.bitrot_tree.column("status", width=100, stretch=False)
        self.bitrot_tree.column("progress", width=80, stretch=False)
        self.bitrot_tree.column("bitrot_state", width=100, stretch=False)

        # Tags for colors
        self.bitrot_tree.tag_configure("clean", background="#d4ffd4") # Light green
        self.bitrot_tree.tag_configure("rot", background="#ffd4d4") # Light red
        self.bitrot_tree.tag_configure("checking", background="#fffdd4") # Light yellow

        scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=self.bitrot_tree.yview)
        scroll_x = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.bitrot_tree.xview)
        self.bitrot_tree.configure(yscrollcommand=scroll.set, xscrollcommand=scroll_x.set)
        
        scroll.pack(side="right", fill="y")
        scroll_x.pack(side="bottom", fill="x")
        self.bitrot_tree.pack(side="left", fill="both", expand=True)
        
        # 3. Log Area & Progress bar
        bottom_frame = tk.Frame(self.bitrot_tab)
        bottom_frame.pack(fill="x", padx=10, pady=5)
        
        self.bitrot_log_text = tk.Text(bottom_frame, height=6, bg="#f5f5f5")
        self.bitrot_log_text.pack(side="top", fill="x")
        self.bitrot_log_text.bind("<Key>", lambda e: "break")

        progress_frame = tk.Frame(bottom_frame)
        progress_frame.pack(fill="x", pady=2)
        
        self.bitrot_progress_lbl = self._tl(progress_frame, "bitrot.ready", width=40, anchor="w")
        self.bitrot_progress_lbl.pack(side="left")
        
        self.bitrot_progress = ttk.Progressbar(progress_frame, orient="horizontal", mode="determinate")
        self.bitrot_progress.pack(side="left", fill="x", expand=True, padx=5)
        
        self.bitrot_stats_lbl = tk.Label(progress_frame, text=t("bitrot.total_stats", count=0, size="0 B"), fg="blue", anchor="e")
        self.bitrot_stats_lbl.pack(side="right", padx=10)
        
        # Populate client dropdown correctly on startup
        self.update_bitrot_client_dropdown()

    def bitrot_log(self, msg):
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        _log_to_file("bitrot", msg)
        self.bitrot_log_text.insert(tk.END, line + "\n")
        self.bitrot_log_text.see(tk.END)
        self.root.update_idletasks()
        
    def update_bitrot_client_dropdown(self):
        if not hasattr(self, 'bitrot_client_combo'):
            return
        vals = [f"{i}: {c['name']} ({c['url']})" for i, c in enumerate(self.config["clients"]) if c.get("enabled", False)]
        self.bitrot_client_combo['values'] = vals
        if vals:
            idx = self.config.get("last_selected_client_index", 0)
            if idx >= len(vals): idx = 0
            self.bitrot_client_combo.current(idx)
            self._bitrot_on_client_select()

    def _bitrot_on_client_select(self, event=None):
        idx = self.bitrot_client_combo.current()
        if idx >= 0 and idx < len(self.config["clients"]):
            self.bitrot_selected_client = self.config["clients"][idx]

    def bitrot_load_torrents(self):
        client = self.bitrot_selected_client
        if not client:
            self.bitrot_log("No client selected.")
            return

        try:
            age_days = int(self.bitrot_age_spinbox.get())
        except ValueError:
            self.bitrot_log("Invalid age filter. Please enter a valid number of days.")
            return
            
        self.bitrot_tree.delete(*self.bitrot_tree.get_children())
        self.bitrot_scan_results.clear()
        self.bitrot_log(f"Connecting to {client['name']} to fetch torrents...")
        
        def _fetch():
            session = self._get_qbit_session(client)
            if not session:
                self.bitrot_log("Failed to connect to qBittorrent.")
                return
                
            url = client['url']
            try:
                resp = session.get(f"{url}/api/v2/torrents/info", timeout=30)
                if resp.status_code != 200:
                    self.bitrot_log(f"HTTP Error {resp.status_code} fetching torrents.")
                    return
                torrents = resp.json()
            except Exception as e:
                self.bitrot_log(f"Connection error: {e}")
                return
                
            cutoff_timestamp = time.time() - (age_days * 86400)
            history = self.db_manager.get_bitrot_history()
            
            # Filter logically: progress == 1
            filtered = []
            for t in torrents:
                if t.get("progress", 0) == 1.0:
                    t_hash = t.get("hash")
                    hist = history.get(t_hash, {})
                    last_col = hist.get("last_checked", t.get("added_on", time.time()))
                    
                    if last_col <= cutoff_timestamp:
                        t["_bitrot_hist"] = hist
                        filtered.append(t)
                        
            # update UI Stats
            total_size = sum(t.get("size", 0) for t in filtered)
            size_str = format_size(total_size)
            self.root.after(0, lambda c=len(filtered), s=size_str: self.bitrot_stats_lbl.configure(text=f"Total: {c} torrents ({s})"))
            
            self.root.after(0, self._bitrot_populate_tree, filtered)
            
        threading.Thread(target=_fetch, daemon=True).start()

    def _bitrot_populate_tree(self, torrents):
        for t in torrents:
            added_on = fmt_dt(datetime.datetime.fromtimestamp(t.get("added_on", 0)))
            last_activity_ts = t.get("last_activity", 0)
            last_active = fmt_dt(datetime.datetime.fromtimestamp(last_activity_ts)) if last_activity_ts > 0 else "Never"

            up_speed = format_size(t.get("upspeed", 0)) + "/s"
            seed = t.get("num_seeds", 0)
            path = t.get("save_path", t.get("content_path", ""))

            history = t.get("_bitrot_hist", {})
            last_checked_ts = history.get("last_checked", 0)
            last_checked = fmt_dt(datetime.datetime.fromtimestamp(last_checked_ts)) if last_checked_ts > 0 else "Never"
            hist_state = history.get("status", "Pending Check")
            
            size_fmt = format_size(t.get("size", 0))
            name = t.get("name", "Unknown")
            status = t.get("state", "")
            progress = "100%"
            
            topic_id = ""
            cat = t.get("category", "")
            if cat and cat.isdigit():
                topic_id = cat
            else:
                comment = t.get("comment", "")
                if "viewtopic.php?t=" in comment:
                    topic_id = comment.split("t=")[-1]

            tags = ()
            if hist_state == "Clean":
                tags = ("clean",)
            elif hist_state == "Bitrot Detected":
                tags = ("rot",)

            item_id = self.bitrot_tree.insert("", "end", values=(
                topic_id, name, size_fmt, added_on, last_active, up_speed, seed, path, last_checked, status, progress, hist_state
            ), tags=tags)
            
            self.bitrot_scan_results.append({
                "hash": t.get("hash"),
                "tree_id": item_id,
            })
            
        self.bitrot_log(f"Populated {len(torrents)} torrents matching criteria.")

    def bitrot_start_check(self):
        client = self.bitrot_selected_client
        if not client:
            return
            
        selected_items = self.bitrot_tree.selection()
        
        # Determine which items to check (selected, or all if none selected)
        if selected_items:
            target_hashes = []
            for item in selected_items:
                for entry in self.bitrot_scan_results:
                    if entry["tree_id"] == item:
                        target_hashes.append(entry["hash"])
                        break
        else:
            target_hashes = [entry["hash"] for entry in self.bitrot_scan_results]
            
        if not target_hashes:
            self.bitrot_log("No torrents to check.")
            return

        self.bitrot_log(f"Starting check on {len(target_hashes)} torrents...")
        self.bitrot_scan_btn.config(state=tk.DISABLED)
        self.bitrot_load_btn.config(state=tk.DISABLED)
        self.bitrot_cancel_btn.config(state=tk.NORMAL)
        
        self.bitrot_scanning = True
        self.bitrot_stop_event.clear()
        
        threading.Thread(target=self._bitrot_monitor_thread, args=(client, target_hashes), daemon=True).start()

    def _bitrot_monitor_thread(self, client, target_hashes):
        session = self._get_qbit_session(client)
        if not session:
            self.bitrot_log("Connection failed.")
            self.root.after(0, self._bitrot_scan_finished)
            return
            
        url = client['url']
        hash_str = "|".join(target_hashes)

        # Trigger the recheck API
        try:
            resp = session.post(f"{url}/api/v2/torrents/recheck", data={"hashes": hash_str}, timeout=10)
            if resp.status_code != 200:
                self.bitrot_log(f"Failed to trigger recheck: HTTP {resp.status_code}")
                self.root.after(0, self._bitrot_scan_finished)
                return
        except Exception as e:
            self.bitrot_log(f"Error triggering recheck: {e}")
            self.root.after(0, self._bitrot_scan_finished)
            return

        # Poll until all target hashes finish checking
        pending_hashes = set(target_hashes)
        
        def _reset_ui():
            for entry in self.bitrot_scan_results:
                if entry["hash"] in pending_hashes:
                    self.bitrot_tree.set(entry["tree_id"], "status", "Queued/Checking")
                    self.bitrot_tree.set(entry["tree_id"], "progress", "...")
                    self.bitrot_tree.set(entry["tree_id"], "bitrot_state", "Checking")
                    self.bitrot_tree.item(entry["tree_id"], tags=("checking",))
        self.root.after(0, _reset_ui)

        total_checked = 0
        total_targets = len(pending_hashes)

        while self.bitrot_scanning and pending_hashes:
            if self.bitrot_stop_event.is_set():
                self.bitrot_log("Bitrot check stopped by user.")
                break

            try:
                info_resp = session.get(f"{url}/api/v2/torrents/info", params={"hashes": "|".join(pending_hashes)}, timeout=30)
                if info_resp.status_code == 200:
                    current_info = info_resp.json()
                    
                    for t in current_info:
                        if self.bitrot_stop_event.is_set(): break
                        t_hash = t.get("hash")
                        state = t.get("state", "")
                        progress = t.get("progress", 0.0)
                        
                        if "checking" in state:
                            def _upd(tid=t_hash, s=state, p=progress):
                                for entry in self.bitrot_scan_results:
                                    if entry["hash"] == tid:
                                        self.bitrot_tree.set(entry["tree_id"], "status", s)
                                        self.bitrot_tree.set(entry["tree_id"], "progress", f"{p*100:.1f}%")
                            self.root.after(0, _upd)
                            continue
                            
                        # If it's NOT checking anymore, evaluation
                        if "checking" not in state:
                            pending_hashes.remove(t_hash)
                            total_checked += 1
                            
                            def _fin(tid=t_hash, s=state, p=progress):
                                for entry in self.bitrot_scan_results:
                                    if entry["hash"] == tid:
                                        tree_id = entry["tree_id"]
                                        self.bitrot_tree.set(tree_id, "status", s)
                                        self.bitrot_tree.set(tree_id, "progress", f"{p*100:.1f}%")
                                        
                                        now_str = fmt_dt(datetime.datetime.now())
                                        self.bitrot_tree.set(tree_id, "last_checked", now_str)
                                        
                                        if p < 1.0:
                                            self.bitrot_tree.set(tree_id, "bitrot_state", "Bitrot Detected")
                                            self.bitrot_tree.item(tree_id, tags=("rot",))
                                            self.bitrot_log(f"! BITROT DETECTED: {entry.get('name')} dropped to {p*100:.1f}%")
                                            self.db_manager.save_bitrot_history(tid, "Bitrot Detected")
                                        else:
                                            self.bitrot_tree.set(tree_id, "bitrot_state", "Clean")
                                            self.bitrot_tree.item(tree_id, tags=("clean",))
                                            self.db_manager.save_bitrot_history(tid, "Clean")
                            self.root.after(0, _fin)
                            
                    self.root.after(0, lambda c=total_checked, t=total_targets: self.bitrot_progress.configure(value=(c/t*100) if t else 0))
                    self.root.after(0, lambda c=total_checked, t=total_targets: self.bitrot_progress_lbl.configure(text=f"Checking hashes... {c}/{t}"))

            except Exception as e:
                self.bitrot_log(f"Error polling status: {e}")
                time.sleep(2)
                
            time.sleep(1)
            
        self.bitrot_log("Bitrot check complete.")
        self.root.after(0, self._bitrot_scan_finished)
        
    def _bitrot_scan_finished(self):
        self.bitrot_scanning = False
        self.bitrot_scan_btn.config(state=tk.NORMAL)
        self.bitrot_load_btn.config(state=tk.NORMAL)
        self.bitrot_cancel_btn.config(state=tk.DISABLED)
        self.bitrot_progress_lbl.configure(text="Ready")
        self.bitrot_progress.configure(value=0)
        # Notify if bitrot was detected
        try:
            rot_items = self.bitrot_tree.tag_has("rot")
            rot_count = len(rot_items) if rot_items else 0
            if rot_count > 0:
                self._tray_notify(
                    "Bitrot Scanner: Errors Detected",
                    f"{rot_count} torrent{'s' if rot_count != 1 else ''} with potential bitrot detected.",
                )
        except Exception:
            pass

    def bitrot_cancel_check(self):
        self.bitrot_cancel_btn.config(state=tk.DISABLED)
        self.bitrot_stop_event.set()
        self.bitrot_log("Stopping check...")

    def perform_search(self):
        query = self.search_entry.get().strip()
        if not query: return
        
        # Map combobox value to internal type
        combo_val = self.search_type_combo.get()
        s_type = "name"
        if "Topic ID" in combo_val: s_type = "id"
        elif "Hash" in combo_val: s_type = "hash"

        self.search_status.config(text="Searching...", fg="blue")
        self.search_tree.delete(*self.search_tree.get_children())
        
        threading.Thread(target=self._search_thread, args=(query, s_type)).start()

    def _search_on_double_click(self, event):
        item = self.search_tree.identify('item', event.x, event.y)
        if not item:
            return
        vals = self.search_tree.item(item, "values")
        if vals and len(vals) > 0 and vals[0]:
            webbrowser.open(f"https://rutracker.org/forum/viewtopic.php?t={vals[0]}")

    def _search_thread(self, query, s_type):
        results = []
        try:
            if s_type == "name":
                results = self._search_by_name_scrape(query)
            else:
                results = self._search_by_api(query, s_type)
            
            self.root.after(0, lambda: self._update_search_results(results))
        except Exception as e:
            self.root.after(0, lambda: self.search_status.config(text=f"Error: {e}", fg="red"))
            print(f"Search error: {e}")

    def _update_search_results(self, results):
        for r in results:
            self.search_tree.insert("", "end", values=(
                r['id'], r['name'], r['size'], r['seeds'], r['leech'], r['category']
            ))
        self.search_status.config(text=f"Found {len(results)} results.", fg="green")

    def _search_by_api(self, query, s_type):
        # s_type: 'id' or 'hash'
        
        # Clean query: split by commas/spaces/newlines and join with comma
        raw_vals = re.split(r'[,\s\n]+', query)
        val = ",".join(v for v in raw_vals if v)
        
        by = "topic_id" if s_type == "id" else "hash"
        
        url_data = "https://api.rutracker.cc/v1/get_tor_topic_data"
        url_stats = "https://api.rutracker.cc/v1/get_peer_stats"
        params = {"by": by, "val": val}
        
        try:
            resp_data = requests.get(url_data, params=params, proxies=self.get_requests_proxies(), timeout=15)
            if resp_data.status_code != 200:
                raise Exception(f"API HTTP {resp_data.status_code}")
            data = resp_data.json().get("result", {})
            
            # Fetch peer stats natively mapped to the exact query array
            peer_data = {}
            resp_stats = requests.get(url_stats, params=params, proxies=self.get_requests_proxies(), timeout=15)
            if resp_stats.status_code == 200:
                peer_data = resp_stats.json().get("result", {})
        except Exception as e:
            raise Exception(f"API Error: {e}")
            
        results = []
        
        for tid, info in data.items():
            if not info: continue
            
            # Resolve category name
            cat_id = info.get("forum_id")
            cat_name = self.cat_manager.get_category_name(cat_id) if cat_id else "?"
            
            # Peer stats from `get_peer_stats`: [seeders, leechers, seeder_last_seen]
            p_stats = peer_data.get(tid, [])
            api_seeds = p_stats[0] if len(p_stats) > 0 else info.get("seeders", "?")
            api_leech = p_stats[1] if len(p_stats) > 1 else 0
            
            # If by="hash", the rutracker JSON key is the hash itself and we must extract the nested topic_id
            real_id = info.get("topic_id", tid)
            
            results.append({
                "id": real_id,
                "name": info.get("topic_title", "Unknown"),
                "size": format_size(info.get("size", 0)),
                "seeds": api_seeds,
                "leech": api_leech,
                "category": cat_name
            })
            
        return results

    def _search_by_name_scrape(self, query):
        # Requires Login
        user, pwd = self._get_rutracker_creds()
        if not user or not pwd:
            raise Exception("Login credentials required for Name search")
            
        # Ensure logged in
        if 'bb_session' not in self.cat_manager.session.cookies:
            self.cat_manager.login(user, pwd)
            
        url = "https://rutracker.org/forum/tracker.php"
        data = {"nm": query}
        
        resp = self.cat_manager.session.post(url, data=data, timeout=30)
        
        # Parse HTML merely for topic IDs, stripping all fragile Regex dependencies
        topic_ids = []
        rows = resp.text.split('<tr class="tCenter">')
        for row in rows[1:]: # Skip header
             try:
                 id_match = re.search(r'viewtopic\.php\?t=(\d+)', row)
                 if id_match:
                     topic_ids.append(id_match.group(1))
             except:
                 continue
                 
        if not topic_ids:
            return []
            
        # Hydrate perfect metadata arrays via the native `get_tor_topic_data` & `get_peer_stats` routines
        val_string = ",".join(topic_ids)
        return self._search_by_api(val_string, "id")

    def download_selected_torrent(self):
        sel = self.search_tree.selection()
        if not sel: return None
        
        item = self.search_tree.item(sel[0])
        tid = item['values'][0]
        
        # URL: https://rutracker.org/forum/dl.php?t=...
        url = f"https://rutracker.org/forum/dl.php?t={tid}"
        
        user, pwd = self._get_rutracker_creds()
        if 'bb_session' not in self.cat_manager.session.cookies:
            self.cat_manager.login(user, pwd)
            
        try:
            # Helper to attempt download
            def _attempt_dl():
                resp = self.cat_manager.session.get(url, timeout=30)
                ct = resp.headers.get('Content-Type', '').lower()
                if 'application/x-bittorrent' in ct:
                    return resp
                return None

            resp = _attempt_dl()
            
            # If failed, try re-login and retry
            if not resp:
                 print(f"DL failed (Content-Type: {self.cat_manager.session.get(url, stream=True).headers.get('Content-Type')}), retrying login...")
                 if self.cat_manager.login(user, pwd):
                     resp = _attempt_dl()

            if resp:
                # Save to temp
                fname = f"{tid}.torrent"
                # Try to get real name from header?
                if 'Content-Disposition' in resp.headers:
                    cd = resp.headers['Content-Disposition']
                    # filename="name.torrent"
                    m = re.search(r'filename="([^"]+)"', cd)
                    if m: fname = m.group(1)
                
                # Sanitize
                fname = re.sub(r'[<>:"/\\|?*]', '_', fname)
                path = os.path.join(self.temp_dir, fname)
                
                with open(path, "wb") as f:
                    f.write(resp.content)
                
                self.search_status.config(text=f"Downloaded: {fname}", fg="green")
                return path
            else:
                 # Debugging aid: Log what we got
                 fail_resp = self.cat_manager.session.get(url, timeout=10)
                 ct = fail_resp.headers.get('Content-Type', 'Unknown')
                 len_ = len(fail_resp.content)
                 print(f"Failed DL. Type: {ct}, Len: {len_}")
                 self.search_status.config(text=f"Error: Not a torrent (Type: {ct})", fg="red")
                 return None
        except Exception as e:
            self.search_status.config(text=f"DL Error: {e}", fg="red")
            return None

    def download_and_add_torrent(self):
        path = self.download_selected_torrent()
        if path:
            # Switch to Add tab
            self.notebook.select(self.adder_tab)
            
            # Pre-select file
            self.selected_files = [path]
            self.selected_folder_path = None
            
            # Trigger logic similar to select_file
            info = parse_torrent_info(path)
            
            filename = os.path.basename(path)
            torrent_name = info.get('name', '') or filename
            comment = info.get('comment', '')
            topic_id = info.get('topic_id')
            
            # Build display text
            total_size = info.get('total_size', 0)
            file_count = info.get('file_count', 0)
            
            lines = [f"Name: {torrent_name}"]
            if total_size > 0:
                size_str = format_size(total_size)
                lines.append(f"Size: {size_str} ({file_count} file{'s' if file_count != 1 else ''})")
            if topic_id:
                lines.append(f"Topic ID: {topic_id}")
                lines.append("Category: Loading...")
            
            self.file_label.config(text="\n".join(lines), fg="black")
            
            # Store full info for Additional Info button
            self._current_torrent_info = info
            self.info_btn.config(state="normal")
            
            # Set clickable link
            if comment:
                self._current_link = comment
                self.link_label.config(text=comment, font=("TkDefaultFont", 9, "underline"))
            else:
                self._current_link = None
                self.link_label.config(text="", font=("TkDefaultFont", 9))
            
            self.log(f"Auto-selected downloaded torrent: {path}")
            if topic_id:
                # Look up category in background
                threading.Thread(target=self._lookup_torrent_category, args=(topic_id, lines), daemon=True).start()

    def _updater_update_progress(self, current, total, phase):
        self._update_progress(self.updater_progress, self.updater_progress_label,
            current, total, phase, getattr(self, '_updater_start_time', None))

    def _updater_add_tree_row(self, entry):
        status = entry.get("topic_status", "Unknown")
        reason = entry.get("reason", "")
        topic_id = entry.get("topic_id") or "N/A"
        new_topic = entry.get("new_topic_id") or ""
        path = entry.get("save_path", "")
        iid = entry["hash"]
        self.updater_tree.insert("", "end", iid=iid,
            values=(entry["name"], path, status, reason, topic_id, new_topic))
        tag = {"Updated": "updated", "Consumed": "consumed",
               "Deleted": "deleted"}.get(status, "unknown")
        self.updater_tree.item(iid, tags=(tag,))

    def _updater_remove_tree_row(self, t_hash):
        try:
            self.updater_tree.delete(t_hash)
        except tk.TclError:
            pass
        self.updater_scan_results = [e for e in self.updater_scan_results if e["hash"] != t_hash]

    def _updater_scan_finished(self):
        self.updater_scanning = False
        def _reset():
            self.updater_scan_btn.config(state="normal")
            self.updater_stop_btn.config(state="disabled")
            self.updater_prog_frame.pack_forget()
            if self.updater_scan_results:
                self._updater_set_action_buttons("normal")
        self.root.after(0, _reset)

    def updater_stop_scan(self):
        self.updater_stop_event.set()
        self.updater_log("Stopping scan...")
        self.updater_stop_btn.config(state="disabled")

    def _updater_ensure_rutracker_login(self):
        cookies = self.cat_manager.session.cookies.get_dict()
        if 'bb_session' in cookies:
            return True
        user, pwd = self._get_rutracker_creds()
        if not user or not pwd:
            self.updater_log("No Rutracker credentials configured. Go to Settings.")
            return False
        result = self.cat_manager.login(user, pwd)
        if result:
            self.updater_log("Rutracker login successful.")
        else:
            self.updater_log("Rutracker login FAILED. Check credentials in Settings.")
        return result

    # --- Scan ---

    def updater_start_scan(self):
        if self.updater_scanning:
            return

        sel = self.updater_client_selector.current()
        if sel < 0 or sel >= len(self.config["clients"]):
            self.updater_log("No client selected.")
            return

        self.updater_scanning = True
        self.updater_scan_results = []
        self.updater_stop_event.clear()

        # Clear treeview
        for item in self.updater_tree.get_children():
            self.updater_tree.delete(item)

        # UI state
        self.updater_scan_btn.config(state="disabled")
        self.updater_stop_btn.config(state="normal")
        self._updater_set_action_buttons("disabled")
        self.updater_summary_label.config(text="Scanning...", fg="black")

        # Show progress
        self.updater_prog_frame.pack(fill="x", padx=10, after=self.updater_tab.winfo_children()[0])
        self.updater_progress['value'] = 0
        self.updater_progress_label.config(text="Connecting...")

        self.updater_selected_client = self.config["clients"][sel]
        threading.Thread(target=self._updater_scan_thread, daemon=True).start()

    def _updater_scan_thread(self):
        self._updater_start_time = time.time()
        client = self.updater_selected_client
        url = client["url"]

        # Resolve auth
        if client["use_global_auth"] and self.config["global_auth"]["enabled"]:
            user = self.config["global_auth"]["username"]
            pw = self.config["global_auth"]["password"]
        else:
            user = client["username"]
            pw = client["password"]

        try:
            # Phase 1: Connect and get torrent list
            session = requests.Session()
            self.updater_qbit_session = session
            self.updater_log(f"Connecting to {client['name']} ({url})...")

            try:
                resp = session.get(f"{url}/api/v2/app/version", timeout=10)
                if resp.status_code != 200:
                    resp = session.post(f"{url}/api/v2/auth/login",
                        data={"username": user, "password": pw}, timeout=10)
                    if resp.status_code != 200 or resp.text != "Ok.":
                        self.updater_log("Authentication failed!")
                        self._updater_scan_finished()
                        return
            except Exception as e:
                self.updater_log(f"Connection error: {e}")
                self._updater_scan_finished()
                return

            self.updater_log("Connected. Fetching torrent list...")
            only_unregistered = self.updater_only_errored.get()
            resp = session.get(f"{url}/api/v2/torrents/info", timeout=30)
            if resp.status_code != 200:
                self.updater_log(f"Failed to get torrent list: HTTP {resp.status_code}")
                self._updater_scan_finished()
                return

            all_torrents = resp.json()
            total = len(all_torrents)
            self.updater_log(f"Found {total} torrents. Checking trackers...")

            # Messages indicating a torrent is unregistered at the tracker
            UNREG_PATTERNS = [
                "unregistered", "not found", "not registered", "not exist",
                "unknown torrent", "trump", "retitled", "truncated",
            ]

            # Increase connection pool for concurrent requests
            adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
            session.mount("http://", adapter)
            session.mount("https://", adapter)

            def _check_torrent(torrent):
                """Check a single torrent's trackers. Returns dict if unregistered, else None."""
                if self.updater_stop_event.is_set():
                    return None
                t_hash = torrent["hash"]
                try:
                    tr_resp = session.get(f"{url}/api/v2/torrents/trackers",
                        params={"hash": t_hash}, timeout=10)
                    if tr_resp.status_code != 200:
                        return None
                    for tracker in tr_resp.json():
                        # Skip DHT, PeX, LSD pseudo-trackers
                        t_url = tracker.get("url", "")
                        if t_url.startswith("** ["):
                            continue
                        msg = (tracker.get("msg", "") or tracker.get("message", "")).lower()
                        if any(p in msg for p in UNREG_PATTERNS):
                            return {
                                "hash": t_hash,
                                "name": torrent.get("name", "Unknown"),
                                "save_path": torrent.get("save_path", torrent.get("content_path", "")),
                                "category": torrent.get("category", ""),
                            }
                except Exception:
                    pass
                return None

            # Check trackers — concurrent when "Only Unregistered" is on, sequential otherwise
            unregistered = []
            if only_unregistered:
                checked = 0
                with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                    futures = {executor.submit(_check_torrent, t): t for t in all_torrents}
                    for future in concurrent.futures.as_completed(futures):
                        if self.updater_stop_event.is_set():
                            executor.shutdown(wait=False, cancel_futures=True)
                            break
                        result = future.result()
                        if result:
                            unregistered.append(result)
                        checked += 1
                        if checked % 100 == 0 or checked == total:
                            self._updater_update_progress(checked, total, "Checking trackers")
            else:
                for i, torrent in enumerate(all_torrents):
                    if self.updater_stop_event.is_set():
                        break
                    self._updater_update_progress(i + 1, total, "Checking trackers")
                    result = _check_torrent(torrent)
                    if result:
                        unregistered.append(result)

            if self.updater_stop_event.is_set():
                self.updater_log("Scan stopped by user.")
                self._updater_scan_finished()
                return

            self.updater_log(f"Found {len(unregistered)} unregistered torrents.")
            if not unregistered:
                self.root.after(0, lambda: self.updater_summary_label.config(
                    text=f"All {total} torrents OK. No unregistered found.", fg="green"))
                self._updater_scan_finished()
                return

            # Phase 2: Batch-resolve topic IDs via Rutracker API
            self.updater_log("Resolving topic IDs via Rutracker API...")
            self._updater_update_progress(0, 1, "Resolving topic IDs")

            hashes_upper = [e["hash"].upper() for e in unregistered]
            hash_to_topic = {}
            # Batch in groups of 100
            for batch_start in range(0, len(hashes_upper), 100):
                batch = hashes_upper[batch_start:batch_start + 100]
                try:
                    api_resp = requests.get(
                        "https://api.rutracker.cc/v1/get_topic_id",
                        params={"by": "hash", "val": ",".join(batch)},
                        proxies=self.get_requests_proxies(),
                        timeout=15)
                    if api_resp.status_code == 200:
                        result = api_resp.json().get("result", {})
                        hash_to_topic.update(result)
                except Exception as e:
                    self.updater_log(f"API error (get_topic_id): {e}")

            # Map back to entries
            for entry in unregistered:
                h = entry["hash"].upper()
                tid = hash_to_topic.get(h)
                entry["topic_id"] = str(tid) if tid else None

            # Fallback: get topic_id from qBit properties comment for entries without topic_id
            no_topic = [e for e in unregistered if not e.get("topic_id")]
            if no_topic:
                self.updater_log(f"Falling back to qBit properties for {len(no_topic)} torrents...")
                for entry in no_topic:
                    try:
                        prop_resp = session.get(f"{url}/api/v2/torrents/properties",
                            params={"hash": entry["hash"]}, timeout=10)
                        if prop_resp.status_code == 200:
                            comment = prop_resp.json().get("comment", "")
                            m = re.search(r'viewtopic\.php\?t=(\d+)', comment)
                            if not m:
                                m = re.search(r'rutracker\.org/forum/.*?t=(\d+)', comment)
                            if m:
                                entry["topic_id"] = m.group(1)
                    except Exception:
                        pass

            # Phase 3: Batch-check topic status
            topic_ids = list(set(str(e["topic_id"]) for e in unregistered if e.get("topic_id")))
            self.updater_log(f"Checking status for {len(topic_ids)} topics via Rutracker API...")
            self._updater_update_progress(0, 1, "Checking topic status")

            topic_data = {}
            for batch_start in range(0, len(topic_ids), 100):
                batch = topic_ids[batch_start:batch_start + 100]
                try:
                    api_resp = requests.get(
                        "https://api.rutracker.cc/v1/get_tor_topic_data",
                        params={"by": "topic_id", "val": ",".join(batch)},
                        proxies=self.get_requests_proxies(),
                        timeout=15)
                    if api_resp.status_code == 200:
                        result = api_resp.json().get("result", {})
                        topic_data.update(result)
                except Exception as e:
                    self.updater_log(f"API error (get_tor_topic_data): {e}")

            # Determine status for each entry
            updated_count = 0
            consumed_count = 0
            deleted_count = 0
            unknown_count = 0

            for entry in unregistered:
                tid = entry.get("topic_id")
                if not tid:
                    entry["topic_status"] = "No Topic ID"
                    entry["reason"] = "No topic ID found"
                    unknown_count += 1
                elif str(tid) in topic_data:
                    data = topic_data[str(tid)]
                    if data is None:
                        entry["topic_status"] = "Deleted"
                        entry["reason"] = "Topic removed"
                        deleted_count += 1
                        continue
                    current_hash = data.get("info_hash", "").upper()
                    qbit_hash = entry["hash"].upper()
                    if current_hash != qbit_hash:
                        entry["topic_status"] = "Updated"
                        entry["reason"] = "Hash changed"
                        entry["new_hash"] = current_hash
                        entry["new_topic_id"] = str(tid)
                        updated_count += 1
                    else:
                        entry["topic_status"] = "Unknown"
                        entry["reason"] = "Same hash"
                        unknown_count += 1
                else:
                    entry["topic_status"] = "Deleted"
                    entry["reason"] = "Topic removed"
                    deleted_count += 1

            # Phase 3.5: Check "Deleted" topics for ∑ поглощено (consumed)
            deleted_with_tid = [e for e in unregistered
                                if e.get("topic_status") == "Deleted" and e.get("topic_id")]
            if deleted_with_tid:
                self.updater_log(f"Checking {len(deleted_with_tid)} deleted topics for ∑ поглощено...")
                self._updater_update_progress(0, len(deleted_with_tid), "Checking consumed")

                if not self._updater_ensure_rutracker_login():
                    self.updater_log("  Skipping consumed check (not logged in).")
                else:
                    for ci, entry in enumerate(deleted_with_tid):
                        if self.updater_stop_event.is_set():
                            break
                        self._updater_update_progress(ci + 1, len(deleted_with_tid), "Checking consumed")
                        tid = entry["topic_id"]
                        try:
                            page_resp = self.cat_manager.session.get(
                                f"https://rutracker.org/forum/viewtopic.php?t={tid}",
                                timeout=15)
                            if page_resp.status_code == 200:
                                if page_resp.encoding and page_resp.encoding.lower() == 'iso-8859-1':
                                    page_resp.encoding = 'cp1251'
                                text = page_resp.text
                                if "поглощено" in text.lower():
                                    # Find the post body that contains "поглощено"
                                    # HTML: <a href="viewtopic.php?t=XXX" class="postLink">...</a>...поглощено
                                    new_tid = None
                                    posts = re.findall(
                                        r'<div[^>]*class="[^"]*post_body[^"]*"[^>]*>(.*?)</div>',
                                        text, re.DOTALL)
                                    for post in posts:
                                        if "поглощено" in post.lower():
                                            m = re.search(r'viewtopic\.php\?t=(\d+)', post)
                                            if m and m.group(1) != str(tid):
                                                new_tid = m.group(1)
                                            break
                                    entry["topic_status"] = "Consumed"
                                    entry["reason"] = "∑ поглощено"
                                    if new_tid:
                                        entry["new_topic_id"] = new_tid
                                    deleted_count -= 1
                                    consumed_count += 1
                                    self.updater_log(f"  {entry['name'][:50]} → consumed"
                                                     f"{' → t/' + new_tid if new_tid else ''}")
                        except Exception as e:
                            self.updater_log(f"  Error checking t/{tid}: {e}")

            if self.updater_stop_event.is_set():
                self.updater_log("Scan stopped by user.")
                self._updater_scan_finished()
                return

            # Phase 4: Populate treeview
            self.updater_scan_results = unregistered
            for entry in unregistered:
                self.root.after(0, lambda e=entry: self._updater_add_tree_row(e))

            summary = (f"Found {len(unregistered)} unregistered: "
                       f"{updated_count} updated, {consumed_count} consumed, "
                       f"{deleted_count} deleted, {unknown_count} unknown")
            self.root.after(0, lambda s=summary: self.updater_summary_label.config(text=s, fg="black"))
            self.updater_log(summary)

        except Exception as e:
            self.updater_log(f"Scan error: {e}")
        finally:
            self._updater_scan_finished()

    # --- Actions ---

    def updater_perform_action(self, action_type):
        selected = self.updater_tree.selection()
        if not selected:
            messagebox.showwarning("No Selection", "Select one or more torrents from the list.")
            return

        selected_entries = []
        for iid in selected:
            for entry in self.updater_scan_results:
                if entry["hash"] == iid:
                    selected_entries.append(entry)
                    break

        count = len(selected_entries)

        if action_type == "update_consumed":
            with_new = [e for e in selected_entries if e.get("new_topic_id")]
            if not with_new:
                messagebox.showwarning("No New Topic",
                    "None of the selected entries have a new topic ID.\n"
                    "Only consumed (∑ поглощено) entries with a detected replacement can be updated.")
                return
            selected_entries = with_new
            count = len(with_new)
            keep_files = messagebox.askyesno("Keep old files?",
                f"Update {count} consumed torrent(s) with new topic.\n\n"
                "Keep old downloaded files?\n\n"
                "Yes — keep files, recheck against new torrent\n"
                "No — delete old files, re-download from new topic")
            # Store choice for the action thread
            for e in selected_entries:
                e["_keep_files"] = keep_files
            msg = (f"Confirm: update {count} consumed torrent(s).\n\n"
                   f"Old files will be {'KEPT and rechecked' if keep_files else 'DELETED (re-download)'}.\n"
                   "New torrents from replacement topics will be added.")
        elif action_type == "readd_keep":
            msg = (f"Re-add {count} torrent(s) from Rutracker?\n\n"
                   "Old entries will be removed.\n"
                   "Downloaded files will be KEPT and rechecked.")
        elif action_type == "readd_redownload":
            msg = (f"Re-add {count} torrent(s) from Rutracker?\n\n"
                   "Old entries AND files will be DELETED.\n"
                   "Torrents will re-download from scratch.")
        elif action_type == "skip_delete":
            msg = (f"Remove {count} torrent(s) from qBittorrent?\n\n"
                   "Downloaded files will be KEPT on disk.\n"
                   "Only the torrent entry is removed.")
        elif action_type == "delete_files":
            msg = (f"DELETE {count} torrent(s) with all files?\n\n"
                   "⚠ Downloaded files will be PERMANENTLY DELETED!\n"
                   "This cannot be undone.")
        else:
            return

        if not messagebox.askyesno("Confirm Action", msg):
            return

        self._updater_set_action_buttons("disabled")
        self.updater_prog_frame.pack(fill="x", padx=10, after=self.updater_tab.winfo_children()[0])
        self.updater_progress['value'] = 0
        self.updater_progress_label.config(text="Starting action...")
        threading.Thread(target=self._updater_action_thread,
            args=(action_type, selected_entries), daemon=True).start()

    def _updater_action_thread(self, action_type, entries):
        action_start = time.time()
        client = self.updater_selected_client
        url = client["url"]
        session = self.updater_qbit_session

        # Re-auth if needed
        if client["use_global_auth"] and self.config["global_auth"]["enabled"]:
            user = self.config["global_auth"]["username"]
            pw = self.config["global_auth"]["password"]
        else:
            user = client["username"]
            pw = client["password"]

        try:
            resp = session.get(f"{url}/api/v2/app/version", timeout=10)
            if resp.status_code != 200:
                session.post(f"{url}/api/v2/auth/login",
                    data={"username": user, "password": pw}, timeout=10)
        except Exception:
            pass

        success = 0
        fail = 0
        total = len(entries)

        for i, entry in enumerate(entries):
            t_hash = entry["hash"]
            t_name = entry["name"]
            topic_id = entry.get("topic_id")
            save_path = entry.get("save_path", "")
            category = entry.get("category", "")

            try:
                if action_type in ("skip_delete", "delete_files"):
                    delete_files_flag = "true" if action_type == "delete_files" else "false"
                    label = "Deleting with files" if action_type == "delete_files" else "Removing entry"
                    self.updater_log(f"{label}: {t_name[:60]}")
                    resp = session.post(f"{url}/api/v2/torrents/delete",
                        data={"hashes": t_hash, "deleteFiles": delete_files_flag}, timeout=15)
                    if resp.status_code == 200:
                        success += 1
                        self.root.after(0, lambda h=t_hash: self._updater_remove_tree_row(h))
                    else:
                        fail += 1
                        self.updater_log(f"  Delete failed: HTTP {resp.status_code}")

                elif action_type == "update_consumed":
                    new_tid = entry.get("new_topic_id")
                    if not new_tid:
                        self.updater_log(f"Skipping {t_name[:60]}: no new topic ID")
                        fail += 1
                        continue

                    # Download .torrent from the NEW topic
                    self.updater_log(f"Downloading .torrent from new topic {new_tid}...")
                    if not self._updater_ensure_rutracker_login():
                        fail += 1
                        continue

                    dl_resp = self.cat_manager.session.get(
                        f"https://rutracker.org/forum/dl.php?t={new_tid}", timeout=30)

                    if dl_resp.status_code != 200 or len(dl_resp.content) < 100:
                        self.updater_log(f"  Download failed: HTTP {dl_resp.status_code}")
                        fail += 1
                        continue

                    torrent_content = dl_resp.content
                    if not torrent_content.startswith(b'd'):
                        self.updater_log(f"  Downloaded content is not a valid torrent")
                        fail += 1
                        continue

                    # Delete old torrent entry
                    keep_files = entry.get("_keep_files", True)
                    del_flag = "false" if keep_files else "true"
                    self.updater_log(f"Removing old entry: {t_name[:60]} ({'keep files' if keep_files else 'delete files'})")
                    session.post(f"{url}/api/v2/torrents/delete",
                        data={"hashes": t_hash, "deleteFiles": del_flag}, timeout=15)

                    time.sleep(1)

                    # Add new torrent from new topic
                    self.updater_log(f"Adding torrent from new topic {new_tid}...")
                    files = {'torrents': (f'{new_tid}.torrent', torrent_content)}
                    add_data = {
                        'savepath': save_path,
                        'root_folder': 'true',
                        'paused': 'true' if keep_files else 'false',
                    }
                    if category:
                        add_data['category'] = category

                    resp = session.post(f"{url}/api/v2/torrents/add",
                        files=files, data=add_data, timeout=30)

                    if resp.status_code == 200 and resp.text == "Ok.":
                        self.updater_log(f"  Added successfully -> {save_path}")

                        # Trigger recheck + resume (only when keeping files)
                        if keep_files:
                            time.sleep(2)
                            try:
                                new_info = parse_torrent_info(torrent_content)
                                new_name = new_info.get("name", "")
                                list_resp = session.get(f"{url}/api/v2/torrents/info", timeout=15)
                                if list_resp.status_code == 200:
                                    for t in list_resp.json():
                                        if t.get("name") == new_name:
                                            new_hash = t["hash"]
                                            session.post(f"{url}/api/v2/torrents/recheck",
                                                data={"hashes": new_hash}, timeout=15)
                                            self.updater_log(f"  Recheck triggered.")
                                            time.sleep(1)
                                            session.post(f"{url}/api/v2/torrents/resume",
                                                data={"hashes": new_hash}, timeout=15)
                                            break
                            except Exception as e:
                                self.updater_log(f"  Recheck/resume skipped: {e}")

                        success += 1
                        self.root.after(0, lambda h=t_hash: self._updater_remove_tree_row(h))
                    else:
                        fail += 1
                        self.updater_log(f"  Add failed: {resp.text}")

                elif action_type in ("readd_keep", "readd_redownload"):
                    if not topic_id:
                        self.updater_log(f"Skipping {t_name[:60]}: no topic ID")
                        fail += 1
                        continue

                    # Download new .torrent from Rutracker
                    self.updater_log(f"Downloading .torrent for topic {topic_id}...")
                    if not self._updater_ensure_rutracker_login():
                        fail += 1
                        continue

                    dl_resp = self.cat_manager.session.get(
                        f"https://rutracker.org/forum/dl.php?t={topic_id}", timeout=30)

                    if dl_resp.status_code != 200 or len(dl_resp.content) < 100:
                        self.updater_log(f"  Download failed: HTTP {dl_resp.status_code}")
                        fail += 1
                        continue

                    torrent_content = dl_resp.content
                    if not torrent_content.startswith(b'd'):
                        self.updater_log(f"  Downloaded content is not a valid torrent")
                        fail += 1
                        continue

                    # Delete old torrent
                    delete_files = "true" if action_type == "readd_redownload" else "false"
                    self.updater_log(f"Removing old entry: {t_name[:60]} (delete files={delete_files})")
                    session.post(f"{url}/api/v2/torrents/delete",
                        data={"hashes": t_hash, "deleteFiles": delete_files}, timeout=15)

                    time.sleep(1)

                    # Add new torrent
                    self.updater_log(f"Adding new torrent for topic {topic_id}...")
                    files = {'torrents': (f'{topic_id}.torrent', torrent_content)}
                    add_data = {
                        'savepath': save_path,
                        'root_folder': 'true',
                    }
                    if action_type == "readd_keep":
                        add_data['paused'] = 'true'
                    else:
                        add_data['paused'] = 'false'
                    if category:
                        add_data['category'] = category

                    resp = session.post(f"{url}/api/v2/torrents/add",
                        files=files, data=add_data, timeout=30)

                    if resp.status_code == 200 and resp.text == "Ok.":
                        self.updater_log(f"  Added successfully -> {save_path}")

                        # For keep-files: find new torrent and trigger recheck
                        if action_type == "readd_keep":
                            time.sleep(2)
                            try:
                                # Get new hash from the added torrent content
                                new_info = parse_torrent_info(torrent_content)
                                new_name = new_info.get("name", "")
                                # Find it in qBit by name
                                list_resp = session.get(f"{url}/api/v2/torrents/info", timeout=15)
                                if list_resp.status_code == 200:
                                    for t in list_resp.json():
                                        if t.get("name") == new_name:
                                            new_hash = t["hash"]
                                            session.post(f"{url}/api/v2/torrents/recheck",
                                                data={"hashes": new_hash}, timeout=15)
                                            self.updater_log(f"  Recheck triggered.")
                                            # Resume after recheck starts
                                            time.sleep(1)
                                            session.post(f"{url}/api/v2/torrents/resume",
                                                data={"hashes": new_hash}, timeout=15)
                                            break
                            except Exception as e:
                                self.updater_log(f"  Recheck/resume skipped: {e}")

                        success += 1
                        self.root.after(0, lambda h=t_hash: self._updater_remove_tree_row(h))
                    else:
                        fail += 1
                        self.updater_log(f"  Add failed: {resp.text}")

            except Exception as e:
                fail += 1
                self.updater_log(f"Error processing {t_name[:60]}: {e}")

            action_label = action_type.replace("_", " ").title()
            self._update_progress(self.updater_progress, self.updater_progress_label,
                i + 1, total, action_label, action_start)

        # Invalidate cache since torrents were re-added
        if success > 0:
            self._cache_invalidate(client["name"])

        summary = f"Done: {success} succeeded, {fail} failed"
        self.updater_log(summary)
        self.root.after(0, self.updater_prog_frame.pack_forget)
        self.root.after(0, lambda: messagebox.showinfo("Done", summary))
        self.root.after(0, lambda: self._updater_set_action_buttons(
            "normal" if self.updater_scan_results else "disabled"))

    # ===================================================================
    # REPAIR CATEGORIES TAB
    # ===================================================================

    def create_repair_ui(self):
        # --- Scan Controls ---
        ctrl_frame = self._tlf(self.repair_tab, "common.scan_controls", padx=10, pady=5)
        ctrl_frame.pack(fill="x", padx=10, pady=5)

        self._tl(ctrl_frame, "common.client").pack(side="left")
        self.repair_client_selector = ttk.Combobox(ctrl_frame, state="readonly", width=25)
        self.repair_client_selector.pack(side="left", padx=5)
        self.repair_client_selector.bind("<<ComboboxSelected>>", lambda e: self._repair_on_client_changed())

        self.repair_scan_btn = self._tb(ctrl_frame, "common.scan_now", command=self.repair_start_scan)
        self.repair_scan_btn.pack(side="left", padx=5)

        self.repair_stop_btn = self._tb(ctrl_frame, "common.stop", command=self.repair_stop_scan, state="disabled")
        self.repair_stop_btn.pack(side="left", padx=5)

        self.repair_cache_label = self._tl(ctrl_frame, "common.list_updated_never", fg="gray")
        self.repair_cache_label.pack(side="left", padx=10)

        # --- Progress (hidden until scan) ---
        self.repair_prog_frame = tk.Frame(self.repair_tab)
        self.repair_progress = ttk.Progressbar(self.repair_prog_frame, mode='determinate', style="green.Horizontal.TProgressbar")
        self.repair_progress.pack(fill="x", padx=5, pady=(2, 0))
        self.repair_progress_label = tk.Label(self.repair_prog_frame, text="", fg="#333333", font=("Segoe UI", 9))
        self.repair_progress_label.pack(fill="x", padx=5, pady=(0, 2))

        # --- Results Treeview ---
        results_frame = self._tlf(self.repair_tab, "repair.mismatches", padx=5, pady=5)
        results_frame.pack(fill="both", expand=True, padx=10, pady=5)

        tree_container = tk.Frame(results_frame)
        tree_container.pack(fill="both", expand=True)

        self.repair_tree = ttk.Treeview(
            tree_container,
            columns=("name", "cur_cat", "correct_cat", "cur_path", "new_path"),
            show="headings",
            selectmode="extended"
        )
        self._tr_heading(self.repair_tree, "name", "updater.torrent_name")
        self._tr_heading(self.repair_tree, "cur_cat", "repair.current_cat")
        self._tr_heading(self.repair_tree, "correct_cat", "repair.correct_cat")
        self._tr_heading(self.repair_tree, "cur_path", "repair.current_path")
        self._tr_heading(self.repair_tree, "new_path", "repair.new_path")
        self.repair_tree.column("name", width=250, minwidth=120)
        self.repair_tree.column("cur_cat", width=100, minwidth=60)
        self.repair_tree.column("correct_cat", width=100, minwidth=60)
        self.repair_tree.column("cur_path", width=200, minwidth=80)
        self.repair_tree.column("new_path", width=200, minwidth=80)

        tree_scroll = ttk.Scrollbar(tree_container, orient="vertical", command=self.repair_tree.yview)
        tree_scroll_x = ttk.Scrollbar(tree_container, orient="horizontal", command=self.repair_tree.xview)
        self.repair_tree.configure(yscrollcommand=tree_scroll.set, xscrollcommand=tree_scroll_x.set)
        
        tree_scroll.pack(side="right", fill="y")
        tree_scroll_x.pack(side="bottom", fill="x")
        self.repair_tree.pack(side="left", fill="both", expand=True)
        self.repair_tree.bind("<Double-1>", self._repair_on_double_click)

        # Tag colors
        self.repair_tree.tag_configure("mismatch", foreground="dark red")
        self.repair_tree.tag_configure("fixed", foreground="dark green")
        self.repair_tree.tag_configure("error", foreground="red")

        self.repair_summary_label = self._tl(results_frame, "repair.scan_hint", fg="gray")
        self.repair_summary_label.pack(anchor="w", pady=(5, 0))

        # --- Action Buttons ---
        action_frame = tk.Frame(self.repair_tab)
        action_frame.pack(fill="x", padx=10, pady=5)

        self.repair_move_files_var = tk.BooleanVar(value=True)
        self._tcb(action_frame, "repair.move_files",
                      variable=self.repair_move_files_var).pack(side="left", padx=(0, 10))

        self.repair_selected_btn = self._tb(
            action_frame, "repair.selected",
            command=lambda: self._repair_perform_action("selected"), state="disabled")
        self.repair_selected_btn.pack(side="left", padx=3, fill="x", expand=True)

        self.repair_all_btn = self._tb(
            action_frame, "repair.all",
            command=lambda: self._repair_perform_action("all"), state="disabled")
        self.repair_all_btn.pack(side="left", padx=3, fill="x", expand=True)

        # --- Repair Log ---
        log_frame = self._tlf(self.repair_tab, "repair.log", padx=1, pady=1)
        log_frame.pack(fill="both", expand=True, padx=10, pady=5)

        self.repair_log_area = scrolledtext.ScrolledText(log_frame, height=6, state="disabled")
        self.repair_log_area.pack(fill="both", expand=True)

        self.update_repair_client_dropdown()

    def repair_log(self, message):
        """Log to the repair tab's own log area (thread-safe)."""
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {message}"
        _log_to_file("repair", message)
        def _write():
            self.repair_log_area.config(state="normal")
            self.repair_log_area.insert(tk.END, line + "\n")
            self.repair_log_area.see(tk.END)
            self.repair_log_area.config(state="disabled")
        self.root.after(0, _write)

    def update_repair_client_dropdown(self):
        if hasattr(self, 'repair_client_selector'):
            names = [c["name"] for c in self.config["clients"] if c.get("enabled", False)]
            self.repair_client_selector['values'] = names
            idx = self.config.get("last_selected_client_index", 0)
            if idx >= len(names):
                idx = 0
            if names:
                self.repair_client_selector.current(idx)

    def _repair_on_client_changed(self):
        """When client dropdown changes, show cache time."""
        idx = self.repair_client_selector.current()
        if idx >= 0 and idx < len(self.config["clients"]):
            client_name = self.config["clients"][idx]["name"]
            self._show_cache_time_for_client(client_name, self.repair_cache_label)

    def _repair_set_action_buttons(self, state):
        self.repair_selected_btn.config(state=state)
        self.repair_all_btn.config(state=state)

    def _repair_update_progress(self, current, total, phase):
        self._update_progress(self.repair_progress, self.repair_progress_label,
            current, total, phase, getattr(self, '_repair_start_time', None))

    def _repair_scan_finished(self):
        self.repair_scanning = False
        def _reset():
            self.repair_scan_btn.config(state="normal")
            self.repair_stop_btn.config(state="disabled")
            self.repair_prog_frame.pack_forget()
            if self.repair_scan_results:
                self._repair_set_action_buttons("normal")
        self.root.after(0, _reset)

    def repair_stop_scan(self):
        self.repair_stop_event.set()
        self.repair_log("Stopping scan...")
        self.repair_stop_btn.config(state="disabled")

    # --- Scan ---

    def repair_start_scan(self):
        if self.repair_scanning:
            return

        sel = self.repair_client_selector.current()
        if sel < 0 or sel >= len(self.config["clients"]):
            self.repair_log("No client selected.")
            return

        self.repair_scanning = True
        self.repair_scan_results = []
        self.repair_stop_event.clear()

        # Clear treeview
        for item in self.repair_tree.get_children():
            self.repair_tree.delete(item)

        # UI state
        self.repair_scan_btn.config(state="disabled")
        self.repair_stop_btn.config(state="normal")
        self._repair_set_action_buttons("disabled")
        self.repair_summary_label.config(text="Scanning...", fg="black")

        # Show progress
        self.repair_prog_frame.pack(fill="x", padx=10, after=self.repair_tab.winfo_children()[0])
        self.repair_progress['value'] = 0
        self.repair_progress_label.config(text="Connecting...")

        self.repair_selected_client = self.config["clients"][sel]
        threading.Thread(target=self._repair_scan_thread, daemon=True).start()

    def _repair_compute_new_path(self, current_save_path, current_category, correct_category, base_save_path):
        """Compute the new save path by swapping the category folder segment."""
        cur = current_save_path.replace("\\", "/").rstrip("/")
        base = base_save_path.replace("\\", "/").rstrip("/")

        if cur.startswith(base):
            remainder = cur[len(base):].strip("/")
            parts = remainder.split("/") if remainder else []

            if current_category and len(parts) >= 1 and parts[0] == current_category:
                parts[0] = correct_category
            elif not current_category and len(parts) >= 1:
                parts = [correct_category] + parts
            else:
                parts = [correct_category] + parts

            return base + "/" + "/".join(parts)

        # Fallback: base doesn't match — find the category folder segment directly
        if current_category:
            segments = cur.split("/")
            for idx, seg in enumerate(segments):
                if seg == current_category:
                    segments[idx] = correct_category
                    return "/".join(segments)

        return None

    def _repair_scan_thread(self):
        self._repair_start_time = time.time()
        scan_start = self._repair_start_time
        client = self.repair_selected_client
        url = client["url"]
        base_save_path = client["base_save_path"]

        # Resolve auth
        if client["use_global_auth"] and self.config["global_auth"]["enabled"]:
            user = self.config["global_auth"]["username"]
            pw = self.config["global_auth"]["password"]
        else:
            user = client["username"]
            pw = client["password"]

        session = requests.Session()

        # Connect & auth
        try:
            resp = session.get(f"{url}/api/v2/app/version", timeout=10)
            if resp.status_code != 200:
                resp = session.post(f"{url}/api/v2/auth/login",
                    data={"username": user, "password": pw}, timeout=10)
                if resp.status_code != 200 or resp.text != "Ok.":
                    self.repair_log(f"Auth failed for {client['name']}")
                    self._repair_scan_finished()
                    return
        except Exception as e:
            self.repair_log(f"Connection failed: {e}")
            self._repair_scan_finished()
            return

        # Phase 1: Fetch all torrents (use cache if available)
        client_name = client["name"]
        cached, ts = self._cache_get(client_name)
        if cached is not None:
            all_torrents = cached
            self.repair_log(f"Using cached torrent list ({len(all_torrents)} torrents).")
            self.root.after(0, lambda: self._show_cache_time_for_client(client_name, self.repair_cache_label))
        else:
            self.repair_log("Connected. Fetching torrent list...")
            try:
                resp = session.get(f"{url}/api/v2/torrents/info", timeout=30)
                if resp.status_code != 200:
                    self.repair_log(f"Failed to get torrent list: HTTP {resp.status_code}")
                    self._repair_scan_finished()
                    return
            except Exception as e:
                self.repair_log(f"Failed to get torrent list: {e}")
                self._repair_scan_finished()
                return

            all_torrents = resp.json()
            # Store in cache
            ts = self._cache_put(client_name, all_torrents)
            self.root.after(0, lambda: self._update_cache_labels(client_name, ts))

        total = len(all_torrents)
        self.repair_log(f"Found {total} torrents. Resolving topic IDs...")

        # Build hash → torrent info map
        torrents_by_hash = {}
        for t in all_torrents:
            torrents_by_hash[t["hash"]] = {
                "hash": t["hash"],
                "name": t.get("name", "Unknown"),
                "category": t.get("category", ""),
                "save_path": t.get("save_path", t.get("content_path", "")),
            }

        if self.repair_stop_event.is_set():
            self.repair_log("Scan stopped by user.")
            self._repair_scan_finished()
            return

        # Phase 2: Batch-resolve topic IDs via Rutracker API
        hashes_upper = [t["hash"].upper() for t in all_torrents]
        hash_to_topic = {}

        for batch_start in range(0, len(hashes_upper), 100):
            if self.repair_stop_event.is_set():
                break
            batch = hashes_upper[batch_start:batch_start + 100]
            self._repair_update_progress(batch_start + len(batch), len(hashes_upper), "Resolving topic IDs")
            try:
                api_resp = requests.get(
                    "https://api.rutracker.cc/v1/get_topic_id",
                    params={"by": "hash", "val": ",".join(batch)},
                    proxies=self.get_requests_proxies(),
                    timeout=15)
                if api_resp.status_code == 200:
                    result = api_resp.json().get("result", {})
                    hash_to_topic.update(result)
            except Exception as e:
                self.repair_log(f"API error (get_topic_id): {e}")

        if self.repair_stop_event.is_set():
            self.repair_log("Scan stopped by user.")
            self._repair_scan_finished()
            return

        # Map topic IDs to torrents
        for t_hash, info in torrents_by_hash.items():
            h_upper = t_hash.upper()
            tid = hash_to_topic.get(h_upper)
            info["topic_id"] = str(tid) if tid else None

        # Fallback: extract topic_id from qBit properties comment
        no_topic = [h for h, info in torrents_by_hash.items() if not info.get("topic_id")]
        if no_topic:
            self.repair_log(f"Falling back to qBit properties for {len(no_topic)} torrents...")
            for i, t_hash in enumerate(no_topic):
                if self.repair_stop_event.is_set():
                    break
                if i % 50 == 0:
                    self._repair_update_progress(i, len(no_topic), "Reading properties")
                try:
                    prop_resp = session.get(f"{url}/api/v2/torrents/properties",
                        params={"hash": t_hash}, timeout=10)
                    if prop_resp.status_code == 200:
                        comment = prop_resp.json().get("comment", "")
                        m = re.search(r'viewtopic\.php\?t=(\d+)', comment)
                        if not m:
                            m = re.search(r'rutracker\.org/forum/.*?t=(\d+)', comment)
                        if m:
                            torrents_by_hash[t_hash]["topic_id"] = m.group(1)
                except Exception:
                    pass

        if self.repair_stop_event.is_set():
            self.repair_log("Scan stopped by user.")
            self._repair_scan_finished()
            return

        # Filter to only Rutracker torrents (have topic_id)
        rt_torrents = {h: info for h, info in torrents_by_hash.items() if info.get("topic_id")}
        self.repair_log(f"Found {len(rt_torrents)} Rutracker torrents out of {total} total.")

        if not rt_torrents:
            self.root.after(0, lambda: self.repair_summary_label.config(
                text="No Rutracker torrents found.", fg="gray"))
            self._repair_scan_finished()
            return

        # Phase 3: Batch-resolve categories via Rutracker API
        self.repair_log("Resolving correct categories...")
        unique_topics = list(set(info["topic_id"] for info in rt_torrents.values()))
        topic_to_forum = {}

        for batch_start in range(0, len(unique_topics), 100):
            if self.repair_stop_event.is_set():
                break
            batch = unique_topics[batch_start:batch_start + 100]
            self._repair_update_progress(batch_start + len(batch), len(unique_topics), "Resolving categories")
            try:
                api_resp = requests.get(
                    "https://api.rutracker.cc/v1/get_tor_topic_data",
                    params={"by": "topic_id", "val": ",".join(batch)},
                    proxies=self.get_requests_proxies(),
                    timeout=15)
                if api_resp.status_code == 200:
                    result = api_resp.json().get("result", {})
                    for tid, data in result.items():
                        if data and isinstance(data, dict):
                            forum_id = data.get("forum_id")
                            if forum_id:
                                topic_to_forum[str(tid)] = forum_id
            except Exception as e:
                self.repair_log(f"API error (get_tor_topic_data): {e}")

        if self.repair_stop_event.is_set():
            self.repair_log("Scan stopped by user.")
            self._repair_scan_finished()
            return

        # Build forum_id → correct category name mapping
        forum_to_category = {}
        for forum_id in set(topic_to_forum.values()):
            breadcrumb = self.cat_manager._build_breadcrumb_path(forum_id)
            if breadcrumb:
                forum_to_category[forum_id] = breadcrumb["category"]
            else:
                # Try flat cache lookup
                name = self.cat_manager.cache.get("categories", {}).get(str(forum_id))
                if name:
                    forum_to_category[forum_id] = name

        # Phase 4: Compare & find mismatches
        self.repair_log("Comparing categories...")
        mismatches = []

        for t_hash, info in rt_torrents.items():
            tid = info["topic_id"]
            forum_id = topic_to_forum.get(tid)
            if not forum_id:
                continue

            correct_cat = forum_to_category.get(forum_id)
            if not correct_cat:
                continue

            current_cat = info["category"]
            if current_cat == correct_cat:
                continue  # Already correct

            # Compute new path
            new_path = self._repair_compute_new_path(
                info["save_path"], current_cat, correct_cat, base_save_path)

            entry = {
                "hash": t_hash,
                "name": info["name"],
                "current_category": current_cat,
                "correct_category": correct_cat,
                "current_path": info["save_path"],
                "new_path": new_path or info["save_path"],
                "move_files": new_path is not None,
            }
            mismatches.append(entry)

        self.repair_scan_results = mismatches

        # Populate treeview
        def _populate():
            for entry in mismatches:
                iid = entry["hash"]
                cur_cat_display = entry["current_category"] or "(none)"
                self.repair_tree.insert("", "end", iid=iid, values=(
                    entry["name"],
                    cur_cat_display,
                    entry["correct_category"],
                    entry["current_path"],
                    entry["new_path"],
                ))
                self.repair_tree.item(iid, tags=("mismatch",))

            count = len(mismatches)
            rt_count = len(rt_torrents)
            self.repair_summary_label.config(
                text=f"Found {count} mismatched categories out of {rt_count} Rutracker torrents.",
                fg="red" if count > 0 else "green")
        self.root.after(0, _populate)

        elapsed = time.time() - scan_start
        self.repair_log(f"Scan complete. {len(mismatches)} mismatches found. (Elapsed: {elapsed:.1f}s)")
        self._repair_scan_finished()

    # --- Repair Actions ---

    def _repair_on_double_click(self, event):
        item = self.repair_tree.identify('item', event.x, event.y)
        if not item:
            return
        # In repair tree, iid = torrent hash
        self._open_topic_by_hash(item)

    def _repair_perform_action(self, mode):
        if mode == "selected":
            selected = self.repair_tree.selection()
            if not selected:
                messagebox.showwarning("No Selection", "Select torrents to repair.")
                return
            hashes = list(selected)
        else:
            hashes = [e["hash"] for e in self.repair_scan_results]
            if not hashes:
                return

        count = len(hashes)
        move_files = self.repair_move_files_var.get()
        if move_files:
            action_desc = "This will update categories and move files to correct paths."
        else:
            action_desc = "This will only update categories (files will NOT be moved)."
        if not messagebox.askyesno("Confirm Repair",
            f"Repair {count} torrent(s)?\n\n{action_desc}"):
            return

        self._repair_set_action_buttons("disabled")
        self.repair_scan_btn.config(state="disabled")
        self.repair_prog_frame.pack(fill="x", padx=10, after=self.repair_tab.winfo_children()[0])
        self.repair_progress['value'] = 0
        self.repair_progress_label.config(text="Starting repair...")
        threading.Thread(target=self._repair_action_thread, args=(hashes, move_files), daemon=True).start()

    def _repair_action_thread(self, hashes, move_files=True):
        repair_start = time.time()
        client = self.repair_selected_client
        url = client["url"]

        # Resolve auth
        if client["use_global_auth"] and self.config["global_auth"]["enabled"]:
            user = self.config["global_auth"]["username"]
            pw = self.config["global_auth"]["password"]
        else:
            user = client["username"]
            pw = client["password"]

        session = requests.Session()

        # Connect & auth
        try:
            resp = session.get(f"{url}/api/v2/app/version", timeout=10)
            if resp.status_code != 200:
                resp = session.post(f"{url}/api/v2/auth/login",
                    data={"username": user, "password": pw}, timeout=10)
                if resp.status_code != 200 or resp.text != "Ok.":
                    self.repair_log("Auth failed.")
                    self.root.after(0, lambda: self._repair_set_action_buttons(
                        "normal" if self.repair_scan_results else "disabled"))
                    return
        except Exception as e:
            self.repair_log(f"Connection failed: {e}")
            self.root.after(0, lambda: self._repair_set_action_buttons(
                "normal" if self.repair_scan_results else "disabled"))
            return

        # Build lookup from scan results
        results_map = {e["hash"]: e for e in self.repair_scan_results}

        # Collect all unique correct categories to create them first
        cats_to_create = set()
        for h in hashes:
            entry = results_map.get(h)
            if entry:
                cats_to_create.add(entry["correct_category"])

        for cat_name in cats_to_create:
            try:
                session.post(f"{url}/api/v2/torrents/createCategory",
                    data={"category": cat_name}, timeout=10)
                # 409 = already exists, that's OK
            except Exception:
                pass

        success = 0
        fail = 0

        for i, t_hash in enumerate(hashes):
            entry = results_map.get(t_hash)
            if not entry:
                continue

            t_name = entry["name"]
            correct_cat = entry["correct_category"]
            new_path = entry["new_path"]
            move = entry["move_files"]

            self.repair_log(f"[{i+1}/{len(hashes)}] Repairing: {t_name[:70]}")

            try:
                # Step 1: Set category
                resp = session.post(f"{url}/api/v2/torrents/setCategory",
                    data={"hashes": t_hash, "category": correct_cat}, timeout=15)
                if resp.status_code != 200:
                    self.repair_log(f"  Failed to set category: HTTP {resp.status_code}")
                    fail += 1
                    self.root.after(0, lambda h=t_hash: self.repair_tree.item(h, tags=("error",)))
                    continue

                # Step 2: Move files if path changed and user opted in
                if move_files and move and new_path != entry["current_path"]:
                    resp = session.post(f"{url}/api/v2/torrents/setLocation",
                        data={"hashes": t_hash, "location": new_path}, timeout=30)
                    if resp.status_code != 200:
                        self.repair_log(f"  Category set OK, but move failed: HTTP {resp.status_code}")
                        self.repair_log(f"  Tried moving to: {new_path}")
                    else:
                        self.repair_log(f"  Moved: {entry['current_path']} -> {new_path}")

                self.repair_log(f"  Category: {entry['current_category'] or '(none)'} -> {correct_cat}")
                success += 1

                # Update treeview to show fixed
                self.root.after(0, lambda h=t_hash: self.repair_tree.item(h, tags=("fixed",)))

            except Exception as e:
                fail += 1
                self.repair_log(f"  Error: {e}")
                self.root.after(0, lambda h=t_hash: self.repair_tree.item(h, tags=("error",)))

            self._update_progress(self.repair_progress, self.repair_progress_label,
                i + 1, len(hashes), "Repairing", repair_start)

        # Invalidate cache since torrents were modified
        if success > 0:
            self._cache_invalidate(client["name"])

        elapsed = time.time() - repair_start
        summary = f"Done: {success} repaired, {fail} failed (Elapsed: {elapsed:.1f}s)"
        self.repair_log(summary)
        self.root.after(0, self.repair_prog_frame.pack_forget)
        self.root.after(0, lambda: messagebox.showinfo("Repair Complete", summary))
        self.root.after(0, lambda: self._repair_set_action_buttons(
            "normal" if self.repair_scan_results else "disabled"))
        self.root.after(0, lambda: self.repair_scan_btn.config(state="normal"))


    # ===================================================================
    # MOVE TORRENTS TAB
    # ===================================================================

    def create_mover_ui(self):
        # Use a canvas with scrollbar for the whole tab (lots of controls)
        canvas = tk.Canvas(self.mover_tab, highlightthickness=0)
        scrollbar = ttk.Scrollbar(self.mover_tab, orient="vertical", command=canvas.yview)
        self.mover_inner = tk.Frame(canvas)

        self.mover_inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        self._mover_canvas_win = canvas.create_window((0, 0), window=self.mover_inner, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        # Keep inner frame width in sync with canvas width so fill="x" works
        def _on_canvas_resize(event):
            canvas.itemconfigure(self._mover_canvas_win, width=event.width)
        canvas.bind("<Configure>", _on_canvas_resize)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Mouse wheel scrolling — only when mouse is over the canvas
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        def _bind_wheel(event):
            canvas.bind_all("<MouseWheel>", _on_mousewheel)
        def _unbind_wheel(event):
            canvas.unbind_all("<MouseWheel>")
        canvas.bind("<Enter>", _bind_wheel)
        canvas.bind("<Leave>", _unbind_wheel)

        parent = self.mover_inner

        # --- Section A: Client + Load ---
        client_frame = self._tlf(parent, "mover.client", padx=10, pady=5)
        client_frame.pack(fill="x", padx=10, pady=5)

        self._tl(client_frame, "common.client").pack(side="left")
        self.mover_client_selector = ttk.Combobox(client_frame, state="readonly", width=25)
        self.mover_client_selector.pack(side="left", padx=5)
        self.mover_client_selector.bind("<<ComboboxSelected>>", lambda e: self._mover_on_client_changed())

        self.mover_load_btn = self._tb(client_frame, "mover.load_torrents", command=lambda: self._mover_load_torrents(force=True))
        self.mover_load_btn.pack(side="left", padx=5)

        self.mover_load_status = tk.Label(client_frame, text="", fg="gray")
        self.mover_load_status.pack(side="left", padx=10)

        self.mover_cache_label = self._tl(client_frame, "common.list_updated_never", fg="gray", anchor="w")
        self.mover_cache_label.pack(side="left", padx=5, fill="x", expand=True)

        # --- Section B: Category Mover ---
        cat_frame = self._tlf(parent, "mover.by_category", padx=10, pady=5)
        cat_frame.pack(fill="x", padx=10, pady=5)

        row1 = tk.Frame(cat_frame)
        row1.pack(fill="x", pady=2)
        self._tl(row1, "common.category").pack(side="left")
        self.mover_cat_selector = ttk.Combobox(row1, state="readonly")
        self.mover_cat_selector.pack(side="left", padx=5, fill="x", expand=True)
        self.mover_cat_selector.bind("<<ComboboxSelected>>", self._mover_on_cat_selected)

        row2 = tk.Frame(cat_frame)
        row2.pack(fill="x", pady=2)
        self._tl(row2, "mover.new_root").pack(side="left")
        self._tb(row2, "common.browse", command=self._mover_browse_path).pack(side="right")
        self.mover_new_path_var = tk.StringVar()
        self.mover_new_path_entry = tk.Entry(row2, textvariable=self.mover_new_path_var)
        self.mover_new_path_entry.pack(side="left", padx=5, fill="x", expand=True)

        row3 = tk.Frame(cat_frame)
        row3.pack(fill="x", pady=2)
        self._tl(row3, "mover.max_torrents").pack(side="left")
        self.mover_cat_limit_var = tk.IntVar(value=0)
        tk.Spinbox(row3, from_=0, to=99999, textvariable=self.mover_cat_limit_var, width=8).pack(side="left", padx=5)

        # Folder structure options
        opts_label = self._tl(cat_frame, "mover.folder_structure", font=("", 9, "bold"))
        opts_label.pack(anchor="w", pady=(5, 0))

        row4a = tk.Frame(cat_frame)
        row4a.pack(fill="x", pady=1)
        self._tl(row4a, "mover.cat_folder").pack(side="left")
        self.mover_cat_create_cat_var = tk.BooleanVar(value=True)
        self._tcb(row4a, "mover.create_cat_sub",
            variable=self.mover_cat_create_cat_var).pack(side="left", padx=5)

        row4b = tk.Frame(cat_frame)
        row4b.pack(fill="x", pady=1)
        self._tl(row4b, "mover.id_folder").pack(side="left")
        self.mover_cat_id_action = tk.StringVar(value="keep")
        self._trb(row4b, "mover.keep_id",
            variable=self.mover_cat_id_action, value="keep").pack(side="left", padx=5)
        self._trb(row4b, "mover.create_id",
            variable=self.mover_cat_id_action, value="create").pack(side="left", padx=5)
        self._trb(row4b, "mover.remove_id",
            variable=self.mover_cat_id_action, value="strip").pack(side="left", padx=5)

        self.mover_cat_summary = self._tl(cat_frame, "mover.cat_summary_hint", fg="gray")
        self.mover_cat_summary.pack(anchor="w", pady=3)

        cat_btn_frame = tk.Frame(cat_frame)
        cat_btn_frame.pack(fill="x", pady=3)

        self.mover_cat_move_btn = self._tb(cat_btn_frame, "mover.move_category", state="disabled",
            command=self._mover_start_category_move)
        self.mover_cat_move_btn.pack(side="left")

        self.mover_cat_stop_btn = self._tb(cat_btn_frame, "common.stop", state="disabled",
            command=self._mover_stop)
        self.mover_cat_stop_btn.pack(side="left", padx=5)

        self.mover_cat_resume_btn = self._tb(cat_btn_frame, "mover.resume", state="disabled",
            command=self._mover_resume_category_move)
        self.mover_cat_resume_btn.pack(side="left", padx=5)

        self.mover_cat_progress_label = tk.Label(cat_btn_frame, text="", fg="gray")
        self.mover_cat_progress_label.pack(side="left", padx=10)

        # --- Section C: Disk Auto-Balancer ---
        bal_frame = self._tlf(parent, "mover.auto_balance", padx=10, pady=5)
        bal_frame.pack(fill="x", padx=10, pady=5)

        # Disk list
        disk_top = tk.Frame(bal_frame)
        disk_top.pack(fill="x", pady=2)

        disk_tree_frame = tk.Frame(disk_top)
        disk_tree_frame.pack(side="left", fill="both", expand=True)

        self.mover_disk_tree = ttk.Treeview(
            disk_tree_frame,
            columns=("path", "free", "current", "target"),
            show="headings", height=4, selectmode="browse"
        )
        self._tr_heading(self.mover_disk_tree, "path", "mover.disk_path")
        self._tr_heading(self.mover_disk_tree, "free", "mover.free_space")
        self._tr_heading(self.mover_disk_tree, "current", "mover.current_load")
        self._tr_heading(self.mover_disk_tree, "target", "mover.target_load")
        self.mover_disk_tree.column("path", width=200, minwidth=100)
        self.mover_disk_tree.column("free", width=100, anchor="center")
        self.mover_disk_tree.column("current", width=100, anchor="center")
        self.mover_disk_tree.column("target", width=100, anchor="center")
        
        disk_scroll_y = ttk.Scrollbar(disk_tree_frame, orient="vertical", command=self.mover_disk_tree.yview)
        disk_scroll_x = ttk.Scrollbar(disk_tree_frame, orient="horizontal", command=self.mover_disk_tree.xview)
        self.mover_disk_tree.configure(yscrollcommand=disk_scroll_y.set, xscrollcommand=disk_scroll_x.set)
        
        disk_scroll_y.pack(side="right", fill="y")
        disk_scroll_x.pack(side="bottom", fill="x")
        self.mover_disk_tree.pack(side="left", fill="both", expand=True)

        disk_btn_frame = tk.Frame(disk_top)
        disk_btn_frame.pack(side="right", padx=5, fill="y")

        self._tb(disk_btn_frame, "mover.detect_disks", command=self._mover_detect_disks).pack(fill="x", pady=1)
        self._tb(disk_btn_frame, "common.remove", command=self._mover_remove_disk).pack(fill="x", pady=1)

        disk_add_frame = tk.Frame(bal_frame)
        disk_add_frame.pack(fill="x", pady=2)
        self._tl(disk_add_frame, "mover.add_path").pack(side="left")
        self._tb(disk_add_frame, "common.add", command=self._mover_add_disk).pack(side="right", padx=(5, 0))
        self._tb(disk_add_frame, "common.browse", command=self._mover_browse_disk).pack(side="right")
        self.mover_add_disk_var = tk.StringVar()
        tk.Entry(disk_add_frame, textvariable=self.mover_add_disk_var).pack(side="left", padx=5, fill="x", expand=True)

        # Strategy
        strat_frame = tk.Frame(bal_frame)
        strat_frame.pack(fill="x", pady=5)
        self._tl(strat_frame, "mover.strategy").pack(side="left")
        self.mover_strategy_var = tk.StringVar(value="both")
        self._trb(strat_frame, "mover.bal_size", variable=self.mover_strategy_var, value="size").pack(side="left", padx=5)
        self._trb(strat_frame, "mover.bal_seeded", variable=self.mover_strategy_var, value="uploaded").pack(side="left", padx=5)
        self._trb(strat_frame, "mover.bal_both", variable=self.mover_strategy_var, value="both").pack(side="left", padx=5)

        limit_frame = tk.Frame(bal_frame)
        limit_frame.pack(fill="x", pady=2)
        self._tl(limit_frame, "mover.max_torrents").pack(side="left")
        self.mover_bal_limit_var = tk.IntVar(value=0)
        tk.Spinbox(limit_frame, from_=0, to=99999, textvariable=self.mover_bal_limit_var, width=8).pack(side="left", padx=5)

        # Folder structure options
        bal_opts_label = self._tl(bal_frame, "mover.folder_structure", font=("", 9, "bold"))
        bal_opts_label.pack(anchor="w", pady=(5, 0))

        bal_opts1 = tk.Frame(bal_frame)
        bal_opts1.pack(fill="x", pady=1)
        self._tl(bal_opts1, "mover.cat_folder").pack(side="left")
        self.mover_bal_keep_cat_var = tk.BooleanVar(value=True)
        self._tcb(bal_opts1, "mover.preserve_cat",
            variable=self.mover_bal_keep_cat_var).pack(side="left", padx=5)

        bal_opts2 = tk.Frame(bal_frame)
        bal_opts2.pack(fill="x", pady=1)
        self._tl(bal_opts2, "mover.id_folder").pack(side="left")
        self.mover_bal_id_action = tk.StringVar(value="keep")
        self._trb(bal_opts2, "mover.keep_id",
            variable=self.mover_bal_id_action, value="keep").pack(side="left", padx=5)
        self._trb(bal_opts2, "mover.create_id",
            variable=self.mover_bal_id_action, value="create").pack(side="left", padx=5)
        self._trb(bal_opts2, "mover.remove_id",
            variable=self.mover_bal_id_action, value="strip").pack(side="left", padx=5)

        bal_btn_frame = tk.Frame(bal_frame)
        bal_btn_frame.pack(fill="x", pady=3)
        self.mover_preview_btn = self._tb(bal_btn_frame, "mover.preview", state="disabled",
            command=self._mover_preview_balance)
        self.mover_preview_btn.pack(side="left", padx=3)
        self.mover_execute_btn = self._tb(bal_btn_frame, "mover.execute", state="disabled",
            command=self._mover_start_execute_balance)
        self.mover_execute_btn.pack(side="left", padx=3)
        self.mover_bal_summary = tk.Label(bal_btn_frame, text="", fg="gray")
        self.mover_bal_summary.pack(side="left", padx=10)

        # Preview treeview
        preview_frame = self._tlf(parent, "mover.preview_frame", padx=5, pady=5)
        preview_frame.pack(fill="both", expand=True, padx=10, pady=5)

        prev_container = tk.Frame(preview_frame)
        prev_container.pack(fill="both", expand=True)

        self.mover_preview_tree = ttk.Treeview(
            prev_container,
            columns=("name", "size", "uploaded", "from", "to"),
            show="headings", selectmode="extended", height=8
        )
        self._tr_heading(self.mover_preview_tree, "name", "common.name",
            command=lambda: self.sort_tree(self.mover_preview_tree, "name", False))
        self._tr_heading(self.mover_preview_tree, "size", "common.size",
            command=lambda: self.sort_tree(self.mover_preview_tree, "size", False))
        self._tr_heading(self.mover_preview_tree, "uploaded", "mover.col_uploaded",
            command=lambda: self.sort_tree(self.mover_preview_tree, "uploaded", False))
        self._tr_heading(self.mover_preview_tree, "from", "mover.col_from")
        self._tr_heading(self.mover_preview_tree, "to", "mover.col_to")
        self.mover_preview_tree.column("name", width=220, minwidth=100)
        self.mover_preview_tree.column("size", width=80, anchor="center")
        self.mover_preview_tree.column("uploaded", width=80, anchor="center")
        self.mover_preview_tree.column("from", width=180, minwidth=80)
        self.mover_preview_tree.column("to", width=180, minwidth=80)

        prev_scroll = ttk.Scrollbar(prev_container, orient="vertical", command=self.mover_preview_tree.yview)
        prev_scroll_x = ttk.Scrollbar(prev_container, orient="horizontal", command=self.mover_preview_tree.xview)
        self.mover_preview_tree.configure(yscrollcommand=prev_scroll.set, xscrollcommand=prev_scroll_x.set)
        
        prev_scroll.pack(side="right", fill="y")
        prev_scroll_x.pack(side="bottom", fill="x")
        self.mover_preview_tree.pack(side="left", fill="both", expand=True)

        self.mover_preview_tree.tag_configure("moved", foreground="dark green")
        self.mover_preview_tree.tag_configure("error", foreground="red")

        # --- Progress ---
        self.mover_prog_frame = tk.Frame(parent)
        self.mover_progress = ttk.Progressbar(self.mover_prog_frame, mode='determinate', style="green.Horizontal.TProgressbar")
        self.mover_progress.pack(fill="x", padx=5, pady=(2, 0))
        self.mover_progress_label = tk.Label(self.mover_prog_frame, text="", fg="#333333", font=("Segoe UI", 9))
        self.mover_progress_label.pack(fill="x", padx=5, pady=(0, 2))

        # --- Log ---
        log_frame = self._tlf(parent, "mover.move_log", padx=1, pady=1)
        log_frame.pack(fill="both", expand=True, padx=10, pady=5)

        self.mover_log_area = scrolledtext.ScrolledText(log_frame, height=6, state="disabled")
        self.mover_log_area.pack(fill="both", expand=True)

        self.update_mover_client_dropdown()

    # --- Helpers ---

    def mover_log(self, message):
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {message}"
        _log_to_file("mover", message)
        def _write():
            self.mover_log_area.config(state="normal")
            self.mover_log_area.insert(tk.END, line + "\n")
            self.mover_log_area.see(tk.END)
            self.mover_log_area.config(state="disabled")
        self.root.after(0, _write)

    def _mover_update_progress(self, current, total, phase):
        self._update_progress(self.mover_progress, self.mover_progress_label,
            current, total, phase, getattr(self, '_mover_start_time', None))

    def _mover_show_progress(self):
        self.mover_prog_frame.pack(fill="x", padx=10, before=self.mover_log_area.master.master)
        self.mover_progress['value'] = 0
        self.mover_progress_label.config(text="")

    def _mover_hide_progress(self):
        self.root.after(0, self.mover_prog_frame.pack_forget)

    def update_mover_client_dropdown(self):
        if hasattr(self, 'mover_client_selector'):
            names = [c["name"] for c in self.config["clients"] if c.get("enabled", False)]
            self.mover_client_selector['values'] = names
            idx = self.config.get("last_selected_client_index", 0)
            if idx >= len(names):
                idx = 0
            if names:
                self.mover_client_selector.current(idx)

    def _mover_on_client_changed(self):
        """When client dropdown changes, show cache time."""
        idx = self.mover_client_selector.current()
        if idx >= 0 and idx < len(self.config["clients"]):
            client_name = self.config["clients"][idx]["name"]
            self._show_cache_time_for_client(client_name, self.mover_cache_label)

    def _mover_browse_path(self):
        path = filedialog.askdirectory(title="Select new root path")
        if path:
            self.mover_new_path_var.set(path.replace("\\", "/"))

    def _mover_browse_disk(self):
        path = filedialog.askdirectory(title="Select disk base path")
        if path:
            self.mover_add_disk_var.set(path.replace("\\", "/"))

    def _mover_get_disk_base(self, path):
        """Extract the drive/mount base from a path, e.g. 'D:/' from 'D:/Torrents/Sport/Cat/123'."""
        p = path.replace("\\", "/")
        # Windows: D:/ E:/ etc
        if len(p) >= 3 and p[1] == ':' and p[2] == '/':
            return p[:3]
        # Unix: first two components like /mnt/disk1/
        parts = p.split("/")
        if len(parts) >= 3 and parts[0] == '':
            return "/" + parts[1] + "/" + parts[2] + "/"
        return p

    def _mover_parse_path_parts(self, save_path, base_path):
        """Parse a torrent save_path into (base, category_part, id_part, rest).

        Given base_path='D:/Torrents/Sport/' and save_path='D:/Torrents/Sport/Formula1/12345':
        Returns ('D:/Torrents/Sport', 'Formula1', '12345', '')

        The 'id_part' is the last purely-numeric folder segment after the category.
        """
        sp = save_path.replace("\\", "/").rstrip("/")
        bp = base_path.replace("\\", "/").rstrip("/")

        if not sp.startswith(bp):
            return bp, "", "", sp

        remainder = sp[len(bp):].strip("/")
        parts = remainder.split("/") if remainder else []

        cat_part = ""
        id_part = ""
        rest_parts = []

        if len(parts) >= 1:
            cat_part = parts[0]
        if len(parts) >= 2 and parts[1].isdigit():
            id_part = parts[1]
            rest_parts = parts[2:]
        elif len(parts) >= 2:
            # Second part is not numeric — treat as part of the rest
            rest_parts = parts[1:]

        rest = "/".join(rest_parts) if rest_parts else ""
        return bp, cat_part, id_part, rest

    def _mover_build_path(self, base, category, id_part, rest, create_cat, keep_id, strip_id,
                          create_id=False, topic_id=None):
        """Build a new path from parts based on folder structure options.

        create_cat: include category subfolder
        keep_id: keep existing ID subfolder (if present)
        strip_id: remove ID subfolder (flatten contents up)
        create_id: force-create an /ID/ folder even if not present in original path
        topic_id: the topic ID to use when create_id=True and id_part is empty
        """
        parts = [base]
        if create_cat and category:
            parts.append(category)
        if strip_id:
            # Explicitly strip ID — don't add it
            pass
        elif keep_id and id_part:
            parts.append(id_part)
        elif create_id:
            # Create ID folder: use existing id_part or fallback to topic_id
            folder_id = id_part or (str(topic_id) if topic_id else "")
            if folder_id:
                parts.append(folder_id)
        if rest:
            parts.append(rest)
        return "/".join(parts)

    # --- Load Torrents ---

    def _mover_populate_from_torrents(self, torrents, source=""):
        """Populate mover UI (categories, dropdowns) from a torrent list."""
        self.mover_all_torrents = torrents

        # Group by category
        cats = {}
        for t in torrents:
            cat = t.get("category", "") or "(no category)"
            if cat not in cats:
                cats[cat] = []
            cats[cat].append(t)
        self.mover_categories = cats

        # Populate category dropdown
        cat_options = []
        for cat_name in sorted(cats.keys()):
            cat_torrents = cats[cat_name]
            total_size = sum(t.get("size", 0) for t in cat_torrents)
            cat_options.append(f"{cat_name} ({len(cat_torrents)} torrents, {format_size(total_size)})")

        def _update_ui():
            self.mover_cat_selector['values'] = cat_options
            if cat_options:
                self.mover_cat_selector.current(0)
                self._mover_on_cat_selected(None)
            src_text = f" ({source})" if source else ""
            self.mover_load_status.config(
                text=f"Loaded {len(torrents)} torrents in {len(cats)} categories{src_text}", fg="green")
            self.mover_load_btn.config(state="normal")
            self.mover_cat_move_btn.config(state="normal")
            self.mover_preview_btn.config(state="normal")
        self.root.after(0, _update_ui)

    def _mover_load_torrents(self, force=False):
        sel = self.mover_client_selector.current()
        if sel < 0 or sel >= len(self.config["clients"]):
            self.mover_log("No client selected.")
            return

        self.mover_selected_client = self.config["clients"][sel]
        client_name = self.mover_selected_client["name"]

        # Check cache first (unless force refresh)
        if not force:
            cached, ts = self._cache_get(client_name)
            if cached is not None:
                self._mover_populate_from_torrents(cached, source="cached")
                self._show_cache_time_for_client(client_name, self.mover_cache_label)
                self.mover_log(f"Loaded {len(cached)} torrents from cache.")
                return

        self.mover_load_btn.config(state="disabled")
        self.mover_load_status.config(text="Loading...", fg="blue")

        def _thread():
            try:
                s = self._get_qbit_session(self.mover_selected_client)
                if not s:
                    self.mover_log("Connection failed.")
                    self.root.after(0, lambda: self.mover_load_status.config(text="Failed", fg="red"))
                    self.root.after(0, lambda: self.mover_load_btn.config(state="normal"))
                    return

                url = self.mover_selected_client["url"].rstrip("/")
                resp = s.get(f"{url}/api/v2/torrents/info", timeout=30)
                if resp.status_code != 200:
                    self.mover_log(f"Failed: HTTP {resp.status_code}")
                    self.root.after(0, lambda: self.mover_load_status.config(text="Failed", fg="red"))
                    self.root.after(0, lambda: self.mover_load_btn.config(state="normal"))
                    return

                torrents = resp.json()
                # Store in cache
                ts = self._cache_put(client_name, torrents)
                self.root.after(0, lambda: self._update_cache_labels(client_name, ts))

                self._mover_populate_from_torrents(torrents)
                self.mover_log(f"Loaded {len(torrents)} torrents from qBittorrent.")

            except Exception as e:
                self.mover_log(f"Error loading: {e}")
                self.root.after(0, lambda: self.mover_load_status.config(text="Error", fg="red"))
                self.root.after(0, lambda: self.mover_load_btn.config(state="normal"))

        threading.Thread(target=_thread, daemon=True).start()

    def _mover_on_cat_selected(self, event):
        sel = self.mover_cat_selector.current()
        if sel < 0:
            return
        cat_display = self.mover_cat_selector.get()
        # Extract category name (everything before the last " (N torrents, X)")
        cat_name = cat_display.rsplit(" (", 1)[0]

        cat_torrents = self.mover_categories.get(cat_name, [])
        total_size = sum(t.get("size", 0) for t in cat_torrents)
        paths = set(self._mover_get_disk_base(t.get("save_path", "")) for t in cat_torrents)
        paths_str = ", ".join(sorted(paths)) if paths else "unknown"

        self.mover_cat_summary.config(
            text=f"{len(cat_torrents)} torrents, {format_size(total_size)} total. Current disk(s): {paths_str}",
            fg="black")

    # --- Category Move ---

    def _mover_start_category_move(self):
        if self.mover_busy:
            return

        sel = self.mover_cat_selector.current()
        if sel < 0:
            messagebox.showwarning("No Category", "Select a category first.")
            return

        new_path = self.mover_new_path_var.get().strip().replace("\\", "/").rstrip("/")
        if not new_path:
            messagebox.showwarning("No Path", "Enter a new root path.")
            return

        cat_display = self.mover_cat_selector.get()
        cat_name = cat_display.rsplit(" (", 1)[0]
        cat_torrents = self.mover_categories.get(cat_name, [])

        limit = self.mover_cat_limit_var.get()
        if limit > 0:
            cat_torrents = cat_torrents[:limit]

        if not cat_torrents:
            messagebox.showinfo("Nothing", "No torrents in this category.")
            return

        total_size = sum(t.get("size", 0) for t in cat_torrents)
        if not messagebox.askyesno("Confirm Move",
            f"Move {len(cat_torrents)} torrents ({format_size(total_size)}) "
            f"from category '{cat_name}' to:\n{new_path}\n\n"
            f"qBittorrent will physically move the files."):
            return

        # Read folder structure options
        create_cat = self.mover_cat_create_cat_var.get()
        id_action = self.mover_cat_id_action.get()  # "keep", "create", "strip"
        keep_id = id_action in ("keep", "create")
        strip_id = id_action == "strip"
        create_id = id_action == "create"

        self.mover_busy = True
        self.mover_cat_move_btn.config(state="disabled")
        self.mover_cat_stop_btn.config(state="normal")
        self.mover_cat_resume_btn.config(state="disabled")
        self.mover_cat_remaining = []
        self.mover_stop_event.clear()
        self._mover_show_progress()

        threading.Thread(target=self._mover_category_thread,
            args=(cat_torrents, new_path, cat_name, create_cat, keep_id, strip_id, create_id), daemon=True).start()

    def _mover_stop(self):
        """Stop the current mover operation."""
        self.mover_stop_event.set()
        self.mover_cat_stop_btn.config(state="disabled")

    def _mover_resume_category_move(self):
        """Resume a previously stopped category move from where it left off."""
        if self.mover_busy or not self.mover_cat_remaining:
            return

        remaining = self.mover_cat_remaining
        params = self.mover_cat_last_params
        total_size = sum(t.get("size", 0) for t in remaining)

        if not messagebox.askyesno("Resume Move",
            f"Resume moving {len(remaining)} remaining torrents ({format_size(total_size)}) "
            f"to: {params['new_root']}?"):
            return

        self.mover_busy = True
        self.mover_cat_move_btn.config(state="disabled")
        self.mover_cat_stop_btn.config(state="normal")
        self.mover_cat_resume_btn.config(state="disabled")
        self.mover_stop_event.clear()
        self._mover_show_progress()

        threading.Thread(target=self._mover_category_thread,
            args=(remaining, params["new_root"], params["cat_name"],
                  params["create_cat"], params["keep_id"], params["strip_id"],
                  params["create_id"]),
            kwargs={"preresolved_topics": params.get("hash_to_topic", {})},
            daemon=True).start()

    def _mover_category_thread(self, torrents, new_root, cat_name, create_cat, keep_id, strip_id, create_id,
                                preresolved_topics=None):
        self._mover_start_time = time.time()
        start_time = self._mover_start_time
        s = self._get_qbit_session(self.mover_selected_client)
        if not s:
            self.mover_log("Connection failed.")
            self.mover_busy = False
            self._mover_hide_progress()
            self.root.after(0, self._mover_cat_reset_buttons)
            return

        url = self.mover_selected_client["url"].rstrip("/")
        base_save_path = self.mover_selected_client.get("base_save_path", "").replace("\\", "/").rstrip("/")
        new_root = new_root.rstrip("/")
        success = 0
        fail = 0
        stopped_at = -1  # Track where we stopped for resume

        opts = []
        if create_cat: opts.append("+ category folder")
        if keep_id: opts.append("keep ID folder")
        if create_id: opts.append("+ create ID folder")
        if strip_id: opts.append("- strip ID folder")
        self.mover_log(f"Moving {len(torrents)} torrents of '{cat_name}' to {new_root} [{', '.join(opts)}]")

        # --- Pre-resolve topic_ids when "Create ID folder" is needed ---
        hash_to_topic = dict(preresolved_topics) if preresolved_topics else {}
        if create_id and not preresolved_topics:
            # First check which torrents lack an ID in their path
            need_id_hashes = []
            for t in torrents:
                old_path = t.get("save_path", "").replace("\\", "/").rstrip("/")
                _, _, old_id, _ = self._mover_parse_path_parts(old_path, base_save_path)
                if not old_id:
                    need_id_hashes.append(t["hash"].upper())

            if need_id_hashes:
                self.mover_log(f"Resolving topic IDs for {len(need_id_hashes)} torrents without ID folders...")
                # Batch API: get_topic_id by hash (100 per request)
                for batch_start in range(0, len(need_id_hashes), 100):
                    if self.mover_stop_event.is_set():
                        break
                    batch = need_id_hashes[batch_start:batch_start + 100]
                    self._mover_update_progress(batch_start + len(batch), len(need_id_hashes), "Resolving IDs")
                    try:
                        api_resp = requests.get(
                            "https://api.rutracker.cc/v1/get_topic_id",
                            params={"by": "hash", "val": ",".join(batch)},
                            proxies=self.get_requests_proxies(),
                            timeout=15)
                        if api_resp.status_code == 200:
                            result = api_resp.json().get("result", {})
                            hash_to_topic.update(result)
                    except Exception as e:
                        self.mover_log(f"API error (get_topic_id): {e}")

                # Fallback: read qBit properties comment for unresolved hashes
                unresolved = [h for h in need_id_hashes if h not in hash_to_topic]
                if unresolved and not self.mover_stop_event.is_set():
                    self.mover_log(f"Fallback: reading qBit properties for {len(unresolved)} unresolved...")
                    for j, h_upper in enumerate(unresolved):
                        if self.mover_stop_event.is_set():
                            break
                        try:
                            prop_resp = s.get(f"{url}/api/v2/torrents/properties",
                                params={"hash": h_upper.lower()}, timeout=10)
                            if prop_resp.status_code == 200:
                                comment = prop_resp.json().get("comment", "")
                                m = re.search(r'viewtopic\.php\?t=(\d+)', comment)
                                if not m:
                                    m = re.search(r'rutracker\.org/forum/.*?t=(\d+)', comment)
                                if m:
                                    hash_to_topic[h_upper] = m.group(1)
                        except Exception:
                            pass

                resolved = sum(1 for h in need_id_hashes if h in hash_to_topic)
                self.mover_log(f"Resolved {resolved}/{len(need_id_hashes)} topic IDs.")

        if self.mover_stop_event.is_set():
            self.mover_log("Stopped by user during ID resolution.")
            # Save state for resume — all torrents still need moving
            self._mover_save_resume_state(torrents, 0, new_root, cat_name,
                create_cat, keep_id, strip_id, create_id, hash_to_topic)
            self.mover_busy = False
            self._mover_hide_progress()
            self.root.after(0, self._mover_cat_reset_buttons)
            return

        # --- Move torrents ---
        for i, t in enumerate(torrents):
            if self.mover_stop_event.is_set():
                stopped_at = i
                self.mover_log(f"Stopped by user at {i}/{len(torrents)}.")
                break

            self._mover_update_progress(i + 1, len(torrents), "Moving")
            t_hash = t["hash"]
            t_name = t.get("name", "?")
            old_path = t.get("save_path", "").replace("\\", "/").rstrip("/")

            # Parse current path to extract category/ID parts
            _, old_cat, old_id, rest = self._mover_parse_path_parts(old_path, base_save_path)

            # Resolve topic_id for this torrent
            topic_id = old_id  # Use existing ID from path if available
            if create_id and not old_id:
                # Look up from resolved map
                resolved_tid = hash_to_topic.get(t_hash.upper())
                topic_id = str(resolved_tid) if resolved_tid else ""

            # Build new location based on options
            new_location = self._mover_build_path(new_root, cat_name, old_id, rest,
                create_cat, keep_id, strip_id, create_id=create_id, topic_id=topic_id)

            try:
                resp = s.post(f"{url}/api/v2/torrents/setLocation",
                    data={"hashes": t_hash, "location": new_location}, timeout=30)
                if resp.status_code == 200:
                    success += 1
                    self.mover_log(f"  [{i+1}/{len(torrents)}] {t_name[:50]} -> {new_location}")
                else:
                    fail += 1
                    self.mover_log(f"  [{i+1}/{len(torrents)}] Failed ({resp.status_code}): {t_name[:50]}")
            except Exception as e:
                fail += 1
                self.mover_log(f"  [{i+1}/{len(torrents)}] Error: {e}")

        # Invalidate cache since torrent paths changed
        if success > 0:
            self._cache_invalidate(self.mover_selected_client["name"])

        # Save resume state if stopped mid-move
        if stopped_at >= 0:
            remaining = torrents[stopped_at:]
            self._mover_save_resume_state(remaining, 0, new_root, cat_name,
                create_cat, keep_id, strip_id, create_id, hash_to_topic)

        elapsed = time.time() - start_time
        if stopped_at >= 0:
            summary = f"Stopped: {success} moved, {fail} failed, {len(torrents) - stopped_at} remaining (Elapsed: {elapsed:.1f}s)"
        else:
            summary = f"Category move done: {success} moved, {fail} failed (Elapsed: {elapsed:.1f}s)"
        self.mover_log(summary)
        self.mover_busy = False
        self._mover_hide_progress()
        self.root.after(0, self._mover_cat_reset_buttons)
        if stopped_at < 0:
            self.root.after(0, lambda: messagebox.showinfo("Move Complete", summary))

    def _mover_save_resume_state(self, remaining, offset, new_root, cat_name,
                                  create_cat, keep_id, strip_id, create_id, hash_to_topic):
        """Save state so the category move can be resumed later."""
        self.mover_cat_remaining = remaining
        self.mover_cat_last_params = {
            "new_root": new_root, "cat_name": cat_name,
            "create_cat": create_cat, "keep_id": keep_id,
            "strip_id": strip_id, "create_id": create_id,
            "hash_to_topic": hash_to_topic,
        }

    def _mover_cat_reset_buttons(self):
        """Reset mover category buttons based on current state."""
        self.mover_cat_move_btn.config(state="normal")
        self.mover_cat_stop_btn.config(state="disabled")
        if self.mover_cat_remaining:
            self.mover_cat_resume_btn.config(state="normal")
            self.mover_cat_progress_label.config(
                text=f"{len(self.mover_cat_remaining)} torrents remaining", fg="orange")
        else:
            self.mover_cat_resume_btn.config(state="disabled")
            self.mover_cat_progress_label.config(text="")

    # --- Disk Auto-Balancer ---

    def _mover_detect_disks(self):
        """Detect disk base paths from current torrent save_paths."""
        if not self.mover_all_torrents:
            messagebox.showinfo("No Data", "Load torrents first.")
            return

        bases = set()
        for t in self.mover_all_torrents:
            sp = t.get("save_path", "").replace("\\", "/")
            if sp:
                base = self._mover_get_disk_base(sp)
                bases.add(base)

        self.mover_disk_list = []
        for base in sorted(bases):
            try:
                usage = shutil.disk_usage(base)
                free = usage.free
                total = usage.total
            except Exception:
                free = 0
                total = 0

            # Current load = sum of torrent sizes on this disk
            current_size = sum(
                t.get("size", 0) for t in self.mover_all_torrents
                if t.get("save_path", "").replace("\\", "/").startswith(base)
            )

            self.mover_disk_list.append({
                "path": base, "free": free, "total": total, "current_size": current_size
            })

        self._mover_refresh_disk_tree()
        self.mover_log(f"Detected {len(self.mover_disk_list)} disk(s): {', '.join(d['path'] for d in self.mover_disk_list)}")

    def _mover_add_disk(self):
        path = self.mover_add_disk_var.get().strip().replace("\\", "/").rstrip("/")
        if not path:
            return
        # Add trailing slash
        if not path.endswith("/"):
            path += "/"

        # Check if already in list
        for d in self.mover_disk_list:
            if d["path"] == path:
                messagebox.showinfo("Exists", "This path is already in the disk list.")
                return

        try:
            usage = shutil.disk_usage(path)
            free = usage.free
            total = usage.total
        except Exception:
            free = 0
            total = 0

        current_size = sum(
            t.get("size", 0) for t in self.mover_all_torrents
            if t.get("save_path", "").replace("\\", "/").startswith(path)
        )

        self.mover_disk_list.append({
            "path": path, "free": free, "total": total, "current_size": current_size
        })
        self._mover_refresh_disk_tree()
        self.mover_add_disk_var.set("")
        self.mover_log(f"Added disk: {path} (Free: {format_size(free)})")

    def _mover_remove_disk(self):
        sel = self.mover_disk_tree.selection()
        if not sel:
            return
        iid = sel[0]
        self.mover_disk_list = [d for d in self.mover_disk_list if d["path"] != iid]
        self._mover_refresh_disk_tree()

    def _mover_refresh_disk_tree(self):
        for item in self.mover_disk_tree.get_children():
            self.mover_disk_tree.delete(item)
        for d in self.mover_disk_list:
            self.mover_disk_tree.insert("", "end", iid=d["path"], values=(
                d["path"],
                format_size(d["free"]),
                format_size(d["current_size"]),
                ""  # Target filled during preview
            ))

    def _mover_preview_balance(self):
        """Compute balance plan and show in preview tree."""
        if not self.mover_all_torrents:
            messagebox.showinfo("No Data", "Load torrents first.")
            return
        if len(self.mover_disk_list) < 2:
            messagebox.showinfo("Need Disks", "Add at least 2 disk paths to balance between.")
            return

        strategy = self.mover_strategy_var.get()
        limit = self.mover_bal_limit_var.get()

        # Map each torrent to its current disk
        disk_paths = [d["path"] for d in self.mover_disk_list]
        torrents_with_disk = []

        for t in self.mover_all_torrents:
            sp = t.get("save_path", "").replace("\\", "/")
            current_disk = None
            # Find which disk this torrent is on (longest prefix match)
            best_len = 0
            for dp in disk_paths:
                if sp.startswith(dp) and len(dp) > best_len:
                    current_disk = dp
                    best_len = len(dp)

            if current_disk is None:
                continue  # Torrent is not on any managed disk

            torrents_with_disk.append({
                "hash": t["hash"],
                "name": t.get("name", "?"),
                "size": t.get("size", 0),
                "uploaded": t.get("uploaded", 0),
                "save_path": sp,
                "current_disk": current_disk,
                "category": t.get("category", ""),
            })

        if not torrents_with_disk:
            self.mover_log("No torrents found on the managed disks.")
            return

        # Calculate scores for each torrent
        total_size = sum(t["size"] for t in torrents_with_disk) or 1
        total_uploaded = sum(t["uploaded"] for t in torrents_with_disk) or 1

        for t in torrents_with_disk:
            if strategy == "size":
                t["score"] = t["size"]
            elif strategy == "uploaded":
                t["score"] = t["uploaded"]
            else:  # both
                t["score"] = 0.5 * (t["size"] / total_size) + 0.5 * (t["uploaded"] / total_uploaded)

        # Target per disk
        total_score = sum(t["score"] for t in torrents_with_disk)
        n_disks = len(disk_paths)
        target_per_disk = total_score / n_disks

        # Get free space for each disk
        disk_free = {}
        for d in self.mover_disk_list:
            disk_free[d["path"]] = d["free"]

        # Greedy assignment: sort by score descending (heaviest first)
        sorted_torrents = sorted(torrents_with_disk, key=lambda x: x["score"], reverse=True)

        # Track assigned score per disk
        disk_assigned_score = {dp: 0.0 for dp in disk_paths}
        disk_assigned_size = {dp: 0 for dp in disk_paths}
        assignments = {}  # hash -> target_disk

        for t in sorted_torrents:
            # Find the disk with the most remaining budget
            best_disk = None
            best_remaining = -float('inf')
            for dp in disk_paths:
                remaining = target_per_disk - disk_assigned_score[dp]
                # Also check free space if moving to this disk
                if dp != t["current_disk"]:
                    if disk_free.get(dp, 0) - disk_assigned_size.get(dp, 0) < t["size"]:
                        continue  # Not enough free space
                if remaining > best_remaining:
                    best_remaining = remaining
                    best_disk = dp

            if best_disk is None:
                best_disk = t["current_disk"]  # Keep on current if no disk has space

            assignments[t["hash"]] = best_disk
            disk_assigned_score[best_disk] += t["score"]
            if best_disk != t["current_disk"]:
                disk_assigned_size[best_disk] += t["size"]

        # Read folder structure options
        keep_cat = self.mover_bal_keep_cat_var.get()
        bal_id_action = self.mover_bal_id_action.get()  # "keep", "create", "strip"
        keep_id = bal_id_action in ("keep", "create")
        strip_id = bal_id_action == "strip"
        create_id = bal_id_action == "create"

        # Pre-resolve topic_ids if "Create ID folder" is selected
        bal_hash_to_topic = {}
        if create_id:
            need_ids = []
            for t in sorted_torrents:
                target_disk = assignments[t["hash"]]
                if target_disk == t["current_disk"]:
                    continue
                old_disk = t["current_disk"]
                relative = t["save_path"][len(old_disk):].strip("/")
                rel_parts = relative.split("/") if relative else []
                id_part = rel_parts[1] if len(rel_parts) >= 2 and rel_parts[1].isdigit() else ""
                if not id_part:
                    need_ids.append(t["hash"].upper())

            if need_ids:
                self.mover_log(f"Balance preview: resolving {len(need_ids)} topic IDs...")
                for batch_start in range(0, len(need_ids), 100):
                    batch = need_ids[batch_start:batch_start + 100]
                    try:
                        api_resp = requests.get(
                            "https://api.rutracker.cc/v1/get_topic_id",
                            params={"by": "hash", "val": ",".join(batch)},
                            proxies=self.get_requests_proxies(),
                            timeout=15)
                        if api_resp.status_code == 200:
                            result = api_resp.json().get("result", {})
                            bal_hash_to_topic.update(result)
                    except Exception as e:
                        self.mover_log(f"API error (get_topic_id): {e}")
                resolved = sum(1 for h in need_ids if h in bal_hash_to_topic)
                self.mover_log(f"Resolved {resolved}/{len(need_ids)} topic IDs for balance preview.")

        # Build move plan (only torrents that need to move)
        plan = []
        for t in sorted_torrents:
            target_disk = assignments[t["hash"]]
            if target_disk == t["current_disk"]:
                continue  # No move needed

            # Parse old path into parts relative to old disk base
            old_disk = t["current_disk"]
            relative = t["save_path"][len(old_disk):].strip("/")
            rel_parts = relative.split("/") if relative else []

            # Extract category and ID from relative path
            cat_part = rel_parts[0] if len(rel_parts) >= 1 else ""
            id_part = rel_parts[1] if len(rel_parts) >= 2 and rel_parts[1].isdigit() else ""
            if id_part:
                rest_parts = rel_parts[2:]
            else:
                rest_parts = rel_parts[1:] if len(rel_parts) > 1 else []
            rest = "/".join(rest_parts)

            # Resolve topic_id for Create ID folder
            topic_id = id_part
            if create_id and not id_part:
                resolved_tid = bal_hash_to_topic.get(t["hash"].upper())
                topic_id = str(resolved_tid) if resolved_tid else ""

            new_path = self._mover_build_path(target_disk.rstrip("/"), cat_part, id_part, rest,
                                               keep_cat, keep_id, strip_id,
                                               create_id=create_id, topic_id=topic_id)

            plan.append({
                "hash": t["hash"],
                "name": t["name"],
                "size": t["size"],
                "uploaded": t["uploaded"],
                "from_path": t["save_path"],
                "to_path": new_path,
                "from_disk": old_disk,
                "to_disk": target_disk,
            })

        # Apply limit
        if limit > 0:
            plan = plan[:limit]

        self.mover_balance_plan = plan

        # Update disk tree target column
        for d in self.mover_disk_list:
            dp = d["path"]
            target_score = disk_assigned_score.get(dp, 0)
            # Show target as size for readability
            target_size_on_disk = sum(
                t["size"] for t in torrents_with_disk if assignments.get(t["hash"]) == dp
            )
            try:
                self.mover_disk_tree.set(dp, "target", format_size(target_size_on_disk))
            except tk.TclError:
                pass

        # Populate preview tree
        for item in self.mover_preview_tree.get_children():
            self.mover_preview_tree.delete(item)

        for entry in plan:
            self.mover_preview_tree.insert("", "end", iid=entry["hash"], values=(
                entry["name"],
                format_size(entry["size"]),
                format_size(entry["uploaded"]),
                entry["from_path"],
                entry["to_path"],
            ))

        total_move_size = sum(e["size"] for e in plan)
        self.mover_bal_summary.config(
            text=f"{len(plan)} torrents to move ({format_size(total_move_size)})", fg="black")
        self.mover_execute_btn.config(state="normal" if plan else "disabled")

        # Log per-disk balance info
        self.mover_log(f"Balance preview: {len(plan)} moves planned ({format_size(total_move_size)} total)")
        for dp in disk_paths:
            current = sum(t["size"] for t in torrents_with_disk if t["current_disk"] == dp)
            target = sum(t["size"] for t in torrents_with_disk if assignments.get(t["hash"]) == dp)
            current_up = sum(t["uploaded"] for t in torrents_with_disk if t["current_disk"] == dp)
            target_up = sum(t["uploaded"] for t in torrents_with_disk if assignments.get(t["hash"]) == dp)
            self.mover_log(f"  {dp}: Size {format_size(current)} -> {format_size(target)} | "
                          f"Uploaded {format_size(current_up)} -> {format_size(target_up)}")

    # --- Execute Balance ---

    def _mover_start_execute_balance(self):
        if self.mover_busy:
            return
        if not self.mover_balance_plan:
            messagebox.showinfo("No Plan", "Run Preview first.")
            return

        total_size = sum(e["size"] for e in self.mover_balance_plan)
        if not messagebox.askyesno("Confirm Balance",
            f"Move {len(self.mover_balance_plan)} torrents ({format_size(total_size)})?\n\n"
            "qBittorrent will physically move the files. This may take a while."):
            return

        self.mover_busy = True
        self.mover_execute_btn.config(state="disabled")
        self.mover_preview_btn.config(state="disabled")
        self.mover_stop_event.clear()
        self._mover_show_progress()

        threading.Thread(target=self._mover_execute_balance_thread, daemon=True).start()

    def _mover_execute_balance_thread(self):
        start_time = time.time()
        s = self._get_qbit_session(self.mover_selected_client)
        if not s:
            self.mover_log("Connection failed.")
            self.mover_busy = False
            self._mover_hide_progress()
            self.root.after(0, lambda: self.mover_execute_btn.config(state="normal"))
            self.root.after(0, lambda: self.mover_preview_btn.config(state="normal"))
            return

        url = self.mover_selected_client["url"].rstrip("/")
        plan = self.mover_balance_plan
        success = 0
        fail = 0

        self.mover_log(f"Executing balance: {len(plan)} moves...")

        for i, entry in enumerate(plan):
            if self.mover_stop_event.is_set():
                self.mover_log("Stopped by user.")
                break

            self._mover_update_progress(i + 1, len(plan), "Moving")
            t_hash = entry["hash"]
            new_path = entry["to_path"]

            try:
                resp = s.post(f"{url}/api/v2/torrents/setLocation",
                    data={"hashes": t_hash, "location": new_path}, timeout=30)
                if resp.status_code == 200:
                    success += 1
                    self.db_manager.log_mover_success(t_hash, entry.get("from_disk", ""), entry.get("to_disk", ""))
                    self.root.after(0, lambda h=t_hash: self.mover_preview_tree.item(h, tags=("moved",)))
                    self.mover_log(f"  [{i+1}/{len(plan)}] Moved: {entry['name'][:55]} -> {entry['to_disk']}")
                else:
                    fail += 1
                    self.root.after(0, lambda h=t_hash: self.mover_preview_tree.item(h, tags=("error",)))
                    self.mover_log(f"  [{i+1}/{len(plan)}] Failed ({resp.status_code}): {entry['name'][:55]}")
            except Exception as e:
                fail += 1
                self.root.after(0, lambda h=t_hash: self.mover_preview_tree.item(h, tags=("error",)))
                self.mover_log(f"  [{i+1}/{len(plan)}] Error: {e}")

        # Invalidate cache since torrent paths changed
        if success > 0:
            self._cache_invalidate(self.mover_selected_client["name"])

        elapsed = time.time() - start_time
        summary = f"Balance done: {success} moved, {fail} failed (Elapsed: {elapsed:.1f}s)"
        self.mover_log(summary)
        self.mover_busy = False
        self._mover_hide_progress()
        self.root.after(0, lambda: self.mover_execute_btn.config(state="disabled"))
        self.root.after(0, lambda: self.mover_preview_btn.config(state="normal"))
        self.root.after(0, lambda: messagebox.showinfo("Balance Complete", summary))



    # ===================================================================
    # KEEPERS TAB
    # ===================================================================

    def _create_keepers_sub_notebook(self):
        """Create a sub-notebook inside the Keepers tab with Scanner and Auto Keeper sub-tabs."""
        self.keepers_sub_notebook = ttk.Notebook(self.keepers_tab)
        self.keepers_sub_notebook.pack(fill="both", expand=True)

        self.keepers_scanner_frame = tk.Frame(self.keepers_sub_notebook)
        self.keepers_sub_notebook.add(self.keepers_scanner_frame, text=t("keepers.sub_scanner"))

        self.keepers_auto_frame = tk.Frame(self.keepers_sub_notebook)
        self.keepers_sub_notebook.add(self.keepers_auto_frame, text=t("keepers.sub_auto_keeper"))

        self.create_keepers_ui()
        self._create_auto_keeper_ui()

    def create_keepers_ui(self):
        # Top controls
        top_frame = tk.Frame(self.keepers_scanner_frame)
        top_frame.pack(fill="x", padx=5, pady=5)

        self._tl(top_frame, "common.category").pack(side="left")
        
        # Category Combobox with Search
        self.keepers_cat_var = tk.StringVar()
        self.keepers_cat_combo = ttk.Combobox(top_frame, textvariable=self.keepers_cat_var, width=50)
        self.keepers_cat_combo.pack(side="left", padx=5)
        
        # Populate categories
        cats = self.cat_manager.cache.get("categories", {})
        # Sort by name, exclude sections 'c'
        self.keepers_all_cats = sorted(
            [f"{name} ({cid})" for cid, name in cats.items() if not str(cid).startswith("c")],
            key=lambda x: x.lower()
        )
        self.keepers_cat_combo['values'] = self.keepers_all_cats
        if self.keepers_all_cats:
            self.keepers_cat_combo.current(0)
            
        # Bind key release for filtering
        self.keepers_cat_combo.bind('<KeyRelease>', self._keepers_filter_cats)
        self.keepers_cat_combo.bind('<<ComboboxSelected>>', self._keepers_on_cat_selected)
        self.keepers_cat_combo.bind('<Button-3>', self._keepers_cat_right_click)

        # Client Selector
        self._tl(top_frame, "common.client").pack(side="left", padx=5)
        self.keepers_client_combo = ttk.Combobox(top_frame, width=15, state="readonly")
        self.keepers_client_combo['values'] = [c['name'] for c in self.config['clients']]
        if self.keepers_client_combo['values']:
             # Try to select last used
             try:
                 self.keepers_client_combo.current(self.config.get('last_selected_client_index', 0))
             except:
                 self.keepers_client_combo.current(0)
        self.keepers_client_combo.pack(side="left")

        # Category Options
        cat_frame = tk.Frame(self.keepers_scanner_frame)
        cat_frame.pack(fill="x", padx=5, pady=2)
        
        self._tl(cat_frame, "keepers.save_category").pack(side="left", padx=5)
        self.keepers_cat_mode = tk.StringVar(value="preserve") # preserve / custom

        self._trb(cat_frame, "keepers.forum_cat", variable=self.keepers_cat_mode, value="preserve", command=self._keepers_toggle_cat_input).pack(side="left")
        self._trb(cat_frame, "keepers.custom", variable=self.keepers_cat_mode, value="custom", command=self._keepers_toggle_cat_input).pack(side="left")
        
        self.keepers_custom_cat_entry = tk.Entry(cat_frame, width=15, state="disabled")
        self.keepers_custom_cat_entry.pack(side="left", padx=5)
        self.keepers_custom_cat_entry.insert(0, "Keepers")

        self.keepers_skip_zero_topics = tk.BooleanVar(value=True)
        self._tcb(cat_frame, "keepers.skip_zero_topics", variable=self.keepers_skip_zero_topics).pack(side="left", padx=(15, 0))

        self.keepers_skip_zero_cats = tk.BooleanVar(value=True)
        self._tcb(cat_frame, "keepers.skip_zero_cats", variable=self.keepers_skip_zero_cats).pack(side="left", padx=(8, 0))

        self._tl(top_frame, "keepers.max_seeds").pack(side="left", padx=5)
        self.keepers_max_seeds = tk.IntVar(value=5)
        seeds_spin = tk.Spinbox(top_frame, from_=0, to=100, textvariable=self.keepers_max_seeds, width=5)
        seeds_spin.pack(side="left")

        self._tl(top_frame, "keepers.max_keepers").pack(side="left", padx=5)
        self.keepers_max_keepers = tk.IntVar(value=-1)
        keepers_spin = tk.Spinbox(top_frame, from_=-1, to=100, textvariable=self.keepers_max_keepers, width=5)
        keepers_spin.pack(side="left")

        self._tl(top_frame, "keepers.max_leech").pack(side="left", padx=5)
        self.keepers_max_leech = tk.IntVar(value=-1)
        leech_spin = tk.Spinbox(top_frame, from_=-1, to=1000, textvariable=self.keepers_max_leech, width=5)
        leech_spin.pack(side="left")

        self._tl(top_frame, "keepers.min_reg_days").pack(side="left", padx=5)
        self.keepers_min_reg_days = tk.IntVar(value=-1)
        reg_spin = tk.Spinbox(top_frame, from_=-1, to=9999, textvariable=self.keepers_min_reg_days, width=5)
        reg_spin.pack(side="left")

        self.keepers_hide_my_kept = tk.BooleanVar(value=False)
        self._tcb(top_frame, "keepers.hide_my_kept", variable=self.keepers_hide_my_kept).pack(side="left", padx=(10, 0))

        self.keepers_scan_btn = self._tb(top_frame, "common.scan", command=self.keepers_start_scan, bg="#dddddd")
        self.keepers_scan_btn.pack(side="left", padx=5)

        self.keepers_stop_btn = self._tb(top_frame, "common.stop", command=self.keepers_stop_scan, state="disabled")
        self.keepers_stop_btn.pack(side="left")

        # --- Preferred Categories ---
        pref_frame = self._tlf(self.keepers_scanner_frame, "keepers.preferred_cats", padx=5, pady=3)
        pref_frame.pack(fill="x", padx=5, pady=(3, 0))

        pref_inner = tk.Frame(pref_frame)
        pref_inner.pack(fill="x")

        self.keepers_pref_listbox = tk.Listbox(pref_inner, height=4, selectmode="single",
            font=("Segoe UI", 9))
        self.keepers_pref_listbox.pack(side="left", fill="x", expand=True)

        pref_scroll = ttk.Scrollbar(pref_inner, orient="vertical", command=self.keepers_pref_listbox.yview)
        self.keepers_pref_listbox.configure(yscrollcommand=pref_scroll.set)
        pref_scroll.pack(side="left", fill="y")

        pref_btn_frame = tk.Frame(pref_inner)
        pref_btn_frame.pack(side="left", padx=(5, 0))

        self._tb(pref_btn_frame, "keepers.add_pref", width=10,
            command=self._keepers_add_preferred).pack(pady=1)
        self._tb(pref_btn_frame, "keepers.remove_pref", width=10,
            command=self._keepers_remove_preferred).pack(pady=1)

        id_row = tk.Frame(pref_btn_frame)
        id_row.pack(pady=1, fill="x")
        self.keepers_pref_id_entry = tk.Entry(id_row, width=7, font=("Segoe UI", 9))
        self.keepers_pref_id_entry.pack(side="left")
        self.keepers_pref_id_entry.bind('<Return>', self._keepers_add_preferred_by_id)
        tk.Button(id_row, text="+", width=2,
            command=self._keepers_add_preferred_by_id).pack(side="left", padx=(2, 0))

        self.keepers_scan_all_btn = self._tb(pref_btn_frame, "keepers.scan_all", width=10,
            command=self._keepers_start_scan_all, bg="#dddddd")
        self.keepers_scan_all_btn.pack(pady=(4, 1))

        self._keepers_refresh_preferred_list()

        # --- Keepers Progress (hidden until scanning) ---
        self.keepers_prog_frame = tk.Frame(self.keepers_scanner_frame)
        self.keepers_progress = ttk.Progressbar(self.keepers_prog_frame, mode='indeterminate',
            style="blue.Horizontal.TProgressbar")
        self.keepers_progress.pack(fill="x", padx=5, pady=(2, 0))
        self.keepers_progress_label = tk.Label(self.keepers_prog_frame, text="",
            fg="#333333", font=("Segoe UI", 9))
        self.keepers_progress_label.pack(fill="x", padx=5, pady=(0, 2))

        # --- Category Statistics Bar ---
        self.keepers_stats_frame = tk.Frame(self.keepers_scanner_frame, relief="groove", bd=1)
        self.keepers_stats_frame.pack(fill="x", padx=5, pady=(2, 0))

        stats_inner = tk.Frame(self.keepers_stats_frame)
        stats_inner.pack(fill="x", padx=8, pady=4)

        self.keepers_stats_lbl_title = self._tl(stats_inner, "keepers.stats_title",
            font=("Segoe UI", 9, "bold"), fg="#444444")
        self.keepers_stats_lbl_title.pack(side="left")

        self.keepers_stats_lbl_topics = self._tl(stats_inner, "keepers.stats_topics",
            font=("Segoe UI", 9), fg="#555555")
        self.keepers_stats_lbl_topics.pack(side="left", padx=(12, 0))

        self.keepers_stats_lbl_size = self._tl(stats_inner, "keepers.stats_size",
            font=("Segoe UI", 9), fg="#555555")
        self.keepers_stats_lbl_size.pack(side="left", padx=(12, 0))

        self.keepers_stats_lbl_seeds = self._tl(stats_inner, "keepers.stats_seeds",
            font=("Segoe UI", 9), fg="#555555")
        self.keepers_stats_lbl_seeds.pack(side="left", padx=(12, 0))

        self.keepers_stats_lbl_avg = self._tl(stats_inner, "keepers.stats_avg",
            font=("Segoe UI", 9), fg="#555555")
        self.keepers_stats_lbl_avg.pack(side="left", padx=(12, 0))

        self.keepers_stats_lbl_leechers = self._tl(stats_inner, "keepers.stats_leechers",
            font=("Segoe UI", 9), fg="#555555")
        self.keepers_stats_lbl_leechers.pack(side="left", padx=(12, 0))

        self.keepers_stats_lbl_filtered = tk.Label(stats_inner, text="",
            font=("Segoe UI", 9, "italic"), fg="#888888")
        self.keepers_stats_lbl_filtered.pack(side="right")

        self.keepers_stats_lbl_cached = tk.Label(stats_inner, text="",
            font=("Segoe UI", 8), fg="#999999")
        self.keepers_stats_lbl_cached.pack(side="right", padx=(0, 8))

        # Results Treeview
        tree_frame = tk.Frame(self.keepers_scanner_frame)
        tree_frame.pack(fill="both", expand=True, padx=5, pady=5)

        cols = ("id", "name", "size", "seeds", "leech", "status", "link", "k_count", "priority", "last_seen", "poster", "tor_status", "reg_time")
        self.keepers_tree = ttk.Treeview(tree_frame, columns=cols, show="headings")
        self._tr_heading(self.keepers_tree, "id", "keepers.col_id", command=lambda: self.sort_tree(self.keepers_tree, "id", False))
        self._tr_heading(self.keepers_tree, "name", "common.name", command=lambda: self.sort_tree(self.keepers_tree, "name", False))
        self._tr_heading(self.keepers_tree, "size", "common.size", command=lambda: self.sort_tree(self.keepers_tree, "size", False))
        self._tr_heading(self.keepers_tree, "seeds", "keepers.col_seeds", command=lambda: self.sort_tree(self.keepers_tree, "seeds", False))
        self._tr_heading(self.keepers_tree, "leech", "keepers.col_leech", command=lambda: self.sort_tree(self.keepers_tree, "leech", False))
        self._tr_heading(self.keepers_tree, "status", "common.status", command=lambda: self.sort_tree(self.keepers_tree, "status", False))
        self._tr_heading(self.keepers_tree, "link", "keepers.col_link", command=lambda: self.sort_tree(self.keepers_tree, "link", False))
        self._tr_heading(self.keepers_tree, "k_count", "keepers.col_k_count", command=lambda: self.sort_tree(self.keepers_tree, "k_count", False))
        self._tr_heading(self.keepers_tree, "priority", "keepers.col_priority", command=lambda: self.sort_tree(self.keepers_tree, "priority", False))
        self._tr_heading(self.keepers_tree, "last_seen", "keepers.col_last_seen", command=lambda: self.sort_tree(self.keepers_tree, "last_seen", False))
        self._tr_heading(self.keepers_tree, "poster", "keepers.col_poster", command=lambda: self.sort_tree(self.keepers_tree, "poster", False))
        self._tr_heading(self.keepers_tree, "tor_status", "keepers.col_tor_status", command=lambda: self.sort_tree(self.keepers_tree, "tor_status", False))
        self._tr_heading(self.keepers_tree, "reg_time", "keepers.col_reg_time", command=lambda: self.sort_tree(self.keepers_tree, "reg_time", False))

        self.keepers_tree.column("id", width=60)
        self.keepers_tree.column("name", width=350)
        self.keepers_tree.column("size", width=70)
        self.keepers_tree.column("seeds", width=50)
        self.keepers_tree.column("leech", width=50)
        self.keepers_tree.column("status", width=80)
        self.keepers_tree.column("link", width=50)
        self.keepers_tree.column("k_count", width=60)
        self.keepers_tree.column("priority", width=80)
        self.keepers_tree.column("last_seen", width=120)
        self.keepers_tree.column("poster", width=80)
        self.keepers_tree.column("tor_status", width=80)
        self.keepers_tree.column("reg_time", width=100)

        # Bind double-click to open link or profile
        self.keepers_tree.bind("<Double-1>", self._keepers_on_double_click)
        self.keepers_tree.bind("<Button-3>", self._keepers_on_right_click)
        
        # Tooltip tracking bindings for # Keepers column
        self.keepers_tooltip = ToolTip(self.keepers_tree)
        self.keepers_tree.bind("<Motion>", self._keepers_on_mouse_motion)
        self.keepers_tree.bind("<Leave>", self._keepers_on_mouse_leave)

        top_scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=self.keepers_tree.yview)
        top_scroll_x = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.keepers_tree.xview)
        self.keepers_tree.configure(yscrollcommand=top_scroll.set, xscrollcommand=top_scroll_x.set)
        
        top_scroll.pack(side="right", fill="y")
        top_scroll_x.pack(side="bottom", fill="x")
        self.keepers_tree.pack(side="left", fill="both", expand=True)

        # Bottom Actions
        action_frame = tk.Frame(self.keepers_scanner_frame)
        action_frame.pack(fill="x", padx=5, pady=5)

        self.keepers_paused_var = tk.BooleanVar(value=True)
        self._tcb(action_frame, "keepers.start_paused", variable=self.keepers_paused_var).pack(side="left")

        self.keepers_add_btn = self._tb(action_frame, "keepers.add_selected", command=self._keepers_add_selected, state="normal")
        self.keepers_add_btn.pack(side="left", padx=5)

        self.keepers_dl_btn = self._tb(action_frame, "common.download_torrent", command=self._keepers_download_torrent)
        self.keepers_dl_btn.pack(side="left", padx=5)

        self.keepers_csv_btn = self._tb(action_frame, "keepers.export_csv", command=self._keepers_export_csv)
        self.keepers_csv_btn.pack(side="right", padx=5)

        # Log
        self.keepers_log_area = scrolledtext.ScrolledText(self.keepers_scanner_frame, height=6, state='disabled')
        self.keepers_log_area.pack(fill="x", padx=5, pady=5)

    def _keepers_filter_cats(self, event):
        typed = self.keepers_cat_var.get().lower()
        if not typed:
            self.keepers_cat_combo['values'] = self.keepers_all_cats
        else:
            filtered = [c for c in self.keepers_all_cats if typed in c.lower()]
            self.keepers_cat_combo['values'] = filtered

    def _keepers_on_cat_selected(self, event=None):
        """Load cached stats when a category is selected from dropdown."""
        cat_str = self.keepers_cat_combo.get()
        if not cat_str:
            return
        try:
            cat_id = int(re.search(r'\((\d+)\)$', cat_str).group(1))
        except:
            return
        cached = self.db_manager.get_category_stats(cat_id)
        if cached:
            self._keepers_update_stats_ui(cached, from_cache=True)
        else:
            self._keepers_reset_stats_ui()

    def _keepers_update_stats_ui(self, stats, from_cache=False):
        """Update stats bar labels from stats dict."""
        def _update():
            self.keepers_stats_lbl_topics.config(text=f"Topics: {stats.get('total_topics', 0):,}")
            self.keepers_stats_lbl_size.config(text=f"Total Size: {format_size(stats.get('total_size', 0))}")
            self.keepers_stats_lbl_seeds.config(text=f"Seeds: {stats.get('total_seeds', 0):,}")
            avg = stats.get('avg_seeds', 0)
            self.keepers_stats_lbl_avg.config(text=f"Avg Seeds: {avg:.1f}")
            self.keepers_stats_lbl_leechers.config(text=f"Leechers: {stats.get('total_leechers', 0):,}")

            ft = stats.get('filtered_topics', 0)
            fs = stats.get('filtered_size', 0)
            fseeds = stats.get('filtered_seeds', 0)
            if ft > 0:
                self.keepers_stats_lbl_filtered.config(
                    text=f"Filtered: {ft:,} topics / {format_size(fs)} / {fseeds:,} seeds")
            else:
                self.keepers_stats_lbl_filtered.config(text="")

            if from_cache:
                ts = stats.get('timestamp', 0)
                if ts > 0:
                    age_min = int((time.time() - ts) / 60)
                    self.keepers_stats_lbl_cached.config(text=f"(cached {age_min}m ago)")
                else:
                    self.keepers_stats_lbl_cached.config(text="(cached)")
            else:
                self.keepers_stats_lbl_cached.config(text="(live)")
        self.root.after(0, _update)

    def _keepers_reset_stats_ui(self):
        """Reset stats bar to default state."""
        def _reset():
            self.keepers_stats_lbl_topics.config(text="Topics: --")
            self.keepers_stats_lbl_size.config(text="Total Size: --")
            self.keepers_stats_lbl_seeds.config(text="Seeds: --")
            self.keepers_stats_lbl_avg.config(text="Avg Seeds: --")
            self.keepers_stats_lbl_leechers.config(text="Leechers: --")
            self.keepers_stats_lbl_filtered.config(text="")
            self.keepers_stats_lbl_cached.config(text="")
        self.root.after(0, _reset)

    # --- Preferred Categories Management ---

    def _keepers_refresh_preferred_list(self):
        """Repopulate the preferred categories listbox from config."""
        self.keepers_pref_listbox.delete(0, tk.END)
        for cat in self.config.get("keepers_preferred_categories", []):
            self.keepers_pref_listbox.insert(tk.END, f"{cat['name']} ({cat['id']})")

    def _keepers_add_preferred(self):
        """Add currently selected combobox category to preferred list."""
        cat_str = self.keepers_cat_combo.get()
        if not cat_str:
            return
        try:
            cat_id = int(re.search(r'\((\d+)\)$', cat_str).group(1))
            cat_name = cat_str[:cat_str.rfind('(')].strip()
        except:
            return

        prefs = self.config.get("keepers_preferred_categories", [])
        if any(c['id'] == cat_id for c in prefs):
            return  # Already in list
        prefs.append({"id": cat_id, "name": cat_name})
        self.config["keepers_preferred_categories"] = prefs
        self.save_config()
        self._keepers_refresh_preferred_list()

    def _keepers_remove_preferred(self):
        """Remove selected item from preferred list."""
        sel = self.keepers_pref_listbox.curselection()
        if not sel:
            return
        idx = sel[0]
        prefs = self.config.get("keepers_preferred_categories", [])
        if 0 <= idx < len(prefs):
            del prefs[idx]
            self.config["keepers_preferred_categories"] = prefs
            self.save_config()
            self._keepers_refresh_preferred_list()

    def _keepers_add_preferred_by_id(self, event=None):
        """Add category to preferred list by typing its numeric ID."""
        raw = self.keepers_pref_id_entry.get().strip()
        if not raw:
            return
        try:
            cat_id = int(raw)
        except ValueError:
            self.keepers_pref_id_entry.delete(0, tk.END)
            return

        # Look up name from category cache
        cats = self.cat_manager.cache.get("categories", {})
        cat_name = cats.get(str(cat_id), cats.get(cat_id, ""))
        if not cat_name:
            cat_name = f"Unknown ({cat_id})"

        prefs = self.config.get("keepers_preferred_categories", [])
        if any(c['id'] == cat_id for c in prefs):
            self.keepers_pref_id_entry.delete(0, tk.END)
            return  # Already in list

        prefs.append({"id": cat_id, "name": cat_name})
        self.config["keepers_preferred_categories"] = prefs
        self.save_config()
        self._keepers_refresh_preferred_list()
        self.keepers_pref_id_entry.delete(0, tk.END)

    def _keepers_cat_right_click(self, event):
        """Right-click on category combobox to add to preferred."""
        cat_str = self.keepers_cat_combo.get()
        if not cat_str:
            return
        menu = tk.Menu(self.root, tearoff=0)
        menu.add_command(label="Add to Preferred", command=self._keepers_add_preferred)
        menu.tk_popup(event.x_root, event.y_root)

    # --- Scan All Preferred Categories ---

    def _keepers_start_scan_all(self):
        """Launch batch scan of all preferred categories."""
        prefs = self.config.get("keepers_preferred_categories", [])
        if not prefs:
            messagebox.showwarning("Scan All", "No preferred categories. Add some first using '+ Add'.")
            return

        try:
            max_seeds = self.keepers_max_seeds.get()
        except:
            max_seeds = 5
        try:
            max_keepers = self.keepers_max_keepers.get()
        except:
            max_keepers = -1
        try:
            max_leech = self.keepers_max_leech.get()
        except:
            max_leech = -1
        try:
            min_reg_days = self.keepers_min_reg_days.get()
        except:
            min_reg_days = -1
        hide_my_kept = self.keepers_hide_my_kept.get()

        client_idx = self.keepers_client_combo.current()
        if client_idx < 0: client_idx = 0
        client_conf = self.config["clients"][client_idx]

        # Clear tree and log
        for item in self.keepers_tree.get_children():
            self.keepers_tree.delete(item)

        self.keepers_stop_event.clear()
        self._keepers_batch_mode = True
        self.keepers_scan_active = True
        self.keepers_scan_btn.config(state="disabled")
        self.keepers_scan_all_btn.config(state="disabled")
        self.keepers_stop_btn.config(state="normal")
        self.keepers_log_area.config(state='normal')
        self.keepers_log_area.delete(1.0, tk.END)
        self.keepers_log_area.config(state='disabled')

        self.keepers_prog_frame.pack(fill="x", padx=5, after=self.keepers_scanner_frame.winfo_children()[0])
        self.keepers_progress.start(15)
        self.keepers_progress_label.config(text="Scanning all preferred categories...")

        t = threading.Thread(target=self._keepers_scan_all_thread,
                             args=(prefs, max_seeds, max_keepers, client_conf, max_leech, min_reg_days, hide_my_kept))
        t.daemon = True
        t.start()

    def _keepers_scan_all_thread(self, prefs, max_seeds, max_keepers, client_conf, max_leech=-1, min_reg_days=-1, hide_my_kept=False):
        """Iterate through preferred categories and scan each one."""
        total = len(prefs)
        grand_total = 0
        skipped = 0
        skip_zero_cats = self.keepers_skip_zero_cats.get()

        for i, cat in enumerate(prefs):
            if self.keepers_stop_event.is_set():
                self.keepers_log("Batch scan stopped by user.")
                break

            cat_id = cat['id']
            cat_name = cat['name']

            # Pre-check: skip 0 B categories using cached PVC data
            if skip_zero_cats:
                pvc_cache = self.db_manager.get_pvc_data(cat_id)
                if pvc_cache:
                    try:
                        pvc_json = json.loads(pvc_cache[0])
                        cat_total_size = 0
                        for vals in pvc_json.values():
                            if isinstance(vals, list) and len(vals) >= 10:
                                cat_total_size += vals[3] if vals[3] else 0
                        if cat_total_size <= 0:
                            self.keepers_log(f"=== Skipping category {i+1}/{total}: {cat_name} ({cat_id}) — 0 B total size ===")
                            skipped += 1
                            continue
                    except:
                        pass  # Can't pre-check, scan normally

            self.keepers_log(f"=== Scanning category {i+1}/{total}: {cat_name} ({cat_id}) ===")
            self.root.after(0, lambda n=cat_name, idx=i+1, tot=total:
                self.keepers_progress_label.config(
                    text=f"Scanning category {idx}/{tot}: {n}..."))

            # Run the existing scan thread logic (synchronously within this thread)
            self._keepers_scan_thread(cat_id, max_seeds, max_keepers, client_conf, max_leech, min_reg_days, hide_my_kept)

            # After scan, check if PVC showed 0 B and skip was enabled
            # (for categories with no prior cache, check after first fetch)
            if skip_zero_cats and hasattr(self, 'keepers_pvc_data') and self.keepers_pvc_data:
                cat_total = sum(d.get('size_bytes', 0) for d in self.keepers_pvc_data.values())
                if cat_total <= 0:
                    self.keepers_log(f"  Category has 0 B total size — no results to show.")

            # Count items in tree so far
            grand_total = len(self.keepers_tree.get_children())

            if i < total - 1 and not self.keepers_stop_event.is_set():
                time.sleep(2)  # Pause between categories

        skip_msg = f" ({skipped} skipped as 0 B)" if skipped > 0 else ""
        self.keepers_log(f"=== Batch scan complete. {grand_total} total candidates across {total} categories{skip_msg}. ===")

        self._keepers_batch_mode = False
        self.keepers_scan_active = False
        def _batch_finish():
            self.keepers_progress.stop()
            self.keepers_prog_frame.pack_forget()
            self.keepers_scan_btn.config(state="normal")
            self.keepers_scan_all_btn.config(state="normal")
            self.keepers_stop_btn.config(state="disabled")
        self.root.after(0, _batch_finish)

    def keepers_log(self, msg):
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        _log_to_file("keepers", msg)
        def _log():
            try:
                self.keepers_log_area.config(state='normal')
                self.keepers_log_area.insert(tk.END, line + "\n")
                self.keepers_log_area.see(tk.END)
                self.keepers_log_area.config(state='disabled')
            except: pass
        self.root.after(0, _log)

    def keepers_start_scan(self):
        cat_str = self.keepers_cat_combo.get()
        if not cat_str:
            messagebox.showwarning("Scan", "Please select a category.")
            return

        try:
            cat_id = int(re.search(r'\((\d+)\)$', cat_str).group(1))
        except:
            messagebox.showerror("Error", "Invalid Category format.")
            return

        try:
            max_seeds = self.keepers_max_seeds.get()
        except:
            max_seeds = 5

        try:
            max_keepers = self.keepers_max_keepers.get()
        except:
            max_keepers = 0

        try:
            max_leech = self.keepers_max_leech.get()
        except:
            max_leech = -1
        try:
            min_reg_days = self.keepers_min_reg_days.get()
        except:
            min_reg_days = -1
        hide_my_kept = self.keepers_hide_my_kept.get()

        # Get selected client
        client_idx = self.keepers_client_combo.current()
        if client_idx < 0: client_idx = 0
        client_conf = self.config["clients"][client_idx]

        # Clear previous
        for item in self.keepers_tree.get_children():
            self.keepers_tree.delete(item)

        self.keepers_stop_event.clear()
        self._keepers_batch_mode = False
        self.keepers_scan_active = True
        self.keepers_scan_btn.config(state="disabled")
        self.keepers_scan_all_btn.config(state="disabled")
        self.keepers_stop_btn.config(state="normal")
        self.keepers_log_area.config(state='normal')
        self.keepers_log_area.delete(1.0, tk.END)
        self.keepers_log_area.config(state='disabled')

        # Show indeterminate progress
        self.keepers_prog_frame.pack(fill="x", padx=5, after=self.keepers_scanner_frame.winfo_children()[0])
        self.keepers_progress.start(15)
        self.keepers_progress_label.config(text="Scanning...")

        t = threading.Thread(target=self._keepers_scan_thread, args=(cat_id, max_seeds, max_keepers, client_conf, max_leech, min_reg_days, hide_my_kept))
        t.daemon = True
        t.start()

    def keepers_stop_scan(self):
        self.keepers_stop_event.set()
        self.keepers_log("Stopping scan...")

    def _keepers_parse_forum_page(self, html_content):
        """Parse viewforum HTML to extract topic list with seeds/leech."""
        topics = []
        
        # Split by row start to avoid global regex backtracking
        # Each row starts with <tr id="tr-{tid}"
        chunks = html_content.split('<tr id="tr-')
        
        for chunk in chunks[1:]: # Skip preamble
            # chunk starts with "12345"...
            try:
                # 1. Extract ID (at start)
                m_id = re.match(r'^(\d+)', chunk)
                if not m_id: continue
                tid_str = m_id.group(1)
                tid = int(tid_str)
                
                # Limit scan window to likely row length (e.g. 5000 chars) to prevent runaway regex
                row_html = chunk[:5000]
                
                # 2. Title: <a id="tt-{tid}" ...>(.*?)</a>
                # Using explicit ID in regex makes it safer
                m_title = re.search(f'<a id="tt-{tid}"[^>]*>(.*?)</a>', row_html, re.DOTALL | re.IGNORECASE)
                if m_title:
                    title_raw = m_title.group(1)
                    title = re.sub(r'<[^>]+>', '', title_raw).strip()
                    title = html.unescape(title)
                else:
                    title = "Unknown"

                # 3. Size: data-ts_text="(\d+)"
                # Try data-ts_text first (sortable raw bytes)
                m_size = re.search(r'data-ts_text=["\']?(\d+)["\']?', row_html)
                if m_size:
                    size = int(m_size.group(1))
                else:
                    # Fallback: try to find size in class="tor-size"
                    # <td class="tor-size" ...><a ...>1.2 GB</a></td> or similar
                    # This is harder to parse exact bytes from, so we default 0 if data-ts_text missing
                    size = 0
                
                # 4. Seeds & Leech (Populated from PVC Data later)
                # 5. Size (Populated from PVC Data later, fallback to regex size)
                seeds = 0
                leech = 0
                
                topics.append({
                    'id': tid,
                    'name': title,
                    'size_str': format_size(size),
                    'raw_size': size,
                    'seeds': seeds,
                    'leech': leech
                })
            except Exception:
                continue # Skip malformed row
            
        return topics


    def _keepers_scan_thread(self, cat_id, max_seeds, max_keepers, client_conf, max_leech=-1, min_reg_days=-1, hide_my_kept=False):
        # 0. Fetch Client Data (in thread)
        client_data = {}
        try:
            self.keepers_log("Fetching client status...")
            s = self._get_qbit_session(client_conf)
            if s:
                url = client_conf["url"].rstrip("/")
                resp = s.get(f"{url}/api/v2/torrents/info", timeout=10)
                if resp.status_code == 200:
                    torrents = resp.json()
                    for t in torrents:
                        h = t.get('hash', '').lower()
                        if h:
                            client_data[h] = {
                                'state': t.get('state', 'unknown'),
                                'progress': t.get('progress', 0),
                                'category': t.get('category', '')
                            }
            else:
                 self.keepers_log("Could not connect to client. Real-time status disabled.")
        except Exception as e:
            self.keepers_log(f"Error fetching client torrents: {e}")

        # 0.5 Fetch PVC Data (Cached 6 hours)
        self.keepers_log(f"Fetching PVC metadata for category {cat_id}...")
        self.keepers_pvc_data = {}
        
        pvc_cache = self.db_manager.get_pvc_data(cat_id)
        pvc_needs_update = True
        pvc_result = {}
        
        if pvc_cache:
            json_str, ts = pvc_cache
            if time.time() - ts < 21600:  # 6 hours
                try:
                    pvc_result = json.loads(json_str)
                    pvc_needs_update = False
                    self.keepers_log("Loaded PVC metadata from cache.")
                except Exception as e:
                    self.keepers_log(f"PVC Cache decode error: {e}")
        
        if pvc_needs_update:
            try:
                pvc_resp = self.cat_manager.session.get(f"https://api.rutracker.cc/v1/static/pvc/f/{cat_id}", timeout=10)
                if pvc_resp.status_code == 200:
                    data = pvc_resp.json()
                    pvc_result = data.get("result", {})
                    if pvc_result:
                        self.db_manager.save_pvc_data(cat_id, json.dumps(pvc_result))
                        self.keepers_log("Fetched and cached new PVC metadata from Rutracker API.")
            except Exception as e:
                self.keepers_log(f"Error fetching PVC metadata: {e}")
                
        # Hydrate dictionary
        if pvc_result:
            for tid_str, vals in pvc_result.items():
                try:
                    tid_int = int(tid_str)
                    if len(vals) >= 10:
                        kf_list = vals[5] if isinstance(vals[5], list) else []
                        self.keepers_pvc_data[tid_int] = {
                            "tor_status": vals[0],
                            "seeds": vals[1],
                            "reg_time": vals[2],
                            "size_bytes": vals[3],
                            "keeping_priority": vals[4],
                            "keepers_count": len(kf_list),
                            "keepers_list": kf_list,
                            "seeder_last_seen": vals[6],
                            "topic_poster": vals[8],
                            "leechers": vals[9]
                        }
                except:
                    pass

        # 0.55 Compute & cache category statistics from PVC data
        if self.keepers_pvc_data:
            total_topics = len(self.keepers_pvc_data)
            total_size = sum(d.get('size_bytes', 0) for d in self.keepers_pvc_data.values())
            total_seeds = sum(d.get('seeds', 0) for d in self.keepers_pvc_data.values())
            total_leechers = sum(d.get('leechers', 0) for d in self.keepers_pvc_data.values())
            avg_seeds = total_seeds / total_topics if total_topics > 0 else 0.0

            # Filtered stats (seeds <= max_seeds, keepers <= max_keepers, leech, age, skip 0 B)
            skip_zero = self.keepers_skip_zero_topics.get()
            def _pvc_matches(d):
                if skip_zero and d.get('size_bytes', 0) <= 0:
                    return False
                if d.get('seeds', 0) > max_seeds:
                    return False
                if max_keepers >= 0 and d.get('keepers_count', 0) > max_keepers:
                    return False
                if max_leech >= 0 and d.get('leechers', 0) > max_leech:
                    return False
                if min_reg_days >= 0:
                    reg_ts = d.get('reg_time', 0)
                    if reg_ts and reg_ts > 0:
                        if (time.time() - reg_ts) / 86400 < min_reg_days:
                            return False
                return True

            filtered_items = [d for d in self.keepers_pvc_data.values() if _pvc_matches(d)]
            filtered_topics = len(filtered_items)
            filtered_size = sum(d.get('size_bytes', 0) for d in filtered_items)
            filtered_seeds = sum(d.get('seeds', 0) for d in filtered_items)

            cat_stats = {
                'total_topics': total_topics, 'total_size': total_size,
                'total_seeds': total_seeds, 'total_leechers': total_leechers,
                'avg_seeds': avg_seeds,
                'filtered_topics': filtered_topics, 'filtered_size': filtered_size,
                'filtered_seeds': filtered_seeds
            }
            self.db_manager.save_category_stats(cat_id, cat_stats)
            self._keepers_update_stats_ui(cat_stats, from_cache=False)
            self.keepers_log(f"Category stats: {total_topics:,} topics, {format_size(total_size)}, "
                             f"{total_seeds:,} seeds (avg {avg_seeds:.1f}), "
                             f"filtered: {filtered_topics:,} topics ({format_size(filtered_size)})")

        # 0.6 Fetch Nickname Data (Cached in DB)
        self.keepers_log("Fetching Keeper User metadata...")
        try:
            users_resp = self.cat_manager.session.get("https://api.rutracker.cc/v1/static/keepers_user_data", timeout=10)
            if users_resp.status_code == 200:
                u_data = users_resp.json()
                u_result = u_data.get("result", {})
                if u_result:
                    self.db_manager.save_keepers_users(u_result)
                    self.keepers_log("Updated Keeper User nicknames in database.")
        except Exception as e:
            self.keepers_log(f"Error fetching Keeper Users metadata: {e}")

        page = 0
        limit_pages = 5 # Safety limit
        found_count = 0 
        login_retried = False 
        
        try:
            while page < limit_pages and not self.keepers_stop_event.is_set():
                url = f"https://rutracker.org/forum/viewforum.php?f={cat_id}&start={page*50}"
                self.keepers_log(f"Fetching page {page+1}...")
                self.root.after(0, lambda p=page+1, fc=found_count:
                    self.keepers_progress_label.config(
                        text=f"Scanning page {p}/{limit_pages}... ({fc} found)"))

                resp = self.cat_manager.session.get(url, timeout=15)
                if resp.encoding == 'ISO-8859-1': resp.encoding = 'cp1251'
                
                # Check if we are on login page? 
                if 'login_username' in resp.text:
                     if not login_retried:
                         self.keepers_log("Session expired. Attempting login...")
                         user, pwd = self._get_rutracker_creds()
                         if user and pwd:
                             if self.cat_manager.login(user, pwd):
                                 login_retried = True
                                 continue
                             else:
                                 self.keepers_log("Login failed.")
                         else:
                             self.keepers_log("No Rutracker credentials configured.")
                         break
                     else:
                         self.keepers_log("Scraping failed: redirect to login page (persistent).")
                         break

                # Reset retry flag on success
                login_retried = False

                try:
                    # 1. Scrape candidates from HTML (get topic IDs and names)
                    candidates = self._keepers_parse_forum_page(resp.text)
                    if not candidates:
                        self.keepers_log("No topics found on this page (or parse error).")
                        break

                    self.keepers_log(f"  Found {len(candidates)} topics via scraping.")

                    # 2. Enrich ALL candidates with API data (size, seeds, leechers, hash)
                    ids_to_fetch = [str(c['id']) for c in candidates]
                    fetched_hashes = {}
                    api_data = {}
                    peer_stats = {}

                    if ids_to_fetch:
                         api_url = "https://api.rutracker.cc/v1/get_tor_topic_data"
                         stats_url = "https://api.rutracker.cc/v1/get_peer_stats"
                         params = {"by": "topic_id", "val": ",".join(ids_to_fetch)}

                         try:
                             # 2a. Fetch Title, Size, Forum IDs
                             api_resp = self.cat_manager.session.get(api_url, params=params, timeout=10)
                             if api_resp.status_code == 200:
                                 res = api_resp.json().get("result")
                                 if res:
                                     for tid_str, info in res.items():
                                         if info:
                                             tid_int = int(tid_str)
                                             if "info_hash" in info:
                                                 fetched_hashes[tid_int] = info["info_hash"]
                                             api_data[tid_int] = info
                             
                             # 2b. Fetch Live Seeds & Leechers
                             stats_resp = self.cat_manager.session.get(stats_url, params=params, timeout=10)
                             if stats_resp.status_code == 200:
                                 peer_stats = stats_resp.json().get("result", {})

                         except Exception as e:
                             self.keepers_log(f"  API Warning: {e}")

                    # Overwrite scraped values with API data (more reliable)
                    for c in candidates:
                        tid_int = int(c['id'])
                        
                        # 1. Base API Hydration
                        info = api_data.get(tid_int)
                        if info:
                            api_size = int(info.get('size', 0) or 0)
                            if api_size > 0:
                                c['raw_size'] = api_size
                                c['size_str'] = format_size(api_size)
                            if info.get('topic_title'):
                                c['name'] = html.unescape(info['topic_title'])
                        
                        # 2. Live Peer Stats Hydration
                        p_stats = peer_stats.get(str(tid_int), [])
                        if p_stats:
                            c['seeds'] = p_stats[0] if len(p_stats) > 0 else c['seeds']
                            c['leech'] = p_stats[1] if len(p_stats) > 1 else c['leech']
                            
                        # 3. Static PVC Fallback (Ensures Keepers always have data)
                        if hasattr(self, 'keepers_pvc_data') and tid_int in self.keepers_pvc_data:
                            pd = self.keepers_pvc_data[tid_int]
                            c['seeds'] = pd.get('seeds', c['seeds'])
                            c['leech'] = pd.get('leechers', c['leech'])
                            pvc_size = pd.get('size_bytes', 0)
                            if pvc_size > 0:
                                c['raw_size'] = pvc_size
                                c['size_str'] = format_size(pvc_size)

                    # Filter by seeds, keepers count, leechers, age, "my kept" AFTER enrichment
                    skip_zero = self.keepers_skip_zero_topics.get()
                    _nick_lower = self.keeper_nickname.lower() if hide_my_kept and getattr(self, 'keeper_nickname', '') else ""
                    # Pre-build user_id→nickname map for "hide my kept" filter
                    _my_uid_set = set()
                    if _nick_lower and hasattr(self, 'keepers_pvc_data'):
                        for _pd in self.keepers_pvc_data.values():
                            for _kid in _pd.get('keepers_list', []):
                                if self.db_manager.get_keepers_user(_kid).lower() == _nick_lower:
                                    _my_uid_set.add(_kid)
                                    break  # found my UID, no need to keep searching

                    def _matches_filter(c):
                        if skip_zero and c.get('raw_size', 0) <= 0:
                            return False
                        if c['seeds'] > max_seeds:
                            return False
                        if max_leech >= 0 and c.get('leech', 0) > max_leech:
                            return False
                        tid_int = int(c['id'])
                        pvc = self.keepers_pvc_data.get(tid_int) if hasattr(self, 'keepers_pvc_data') else None
                        if max_keepers >= 0:
                            k_count = pvc.get('keepers_count', 0) if pvc else 0
                            if k_count > max_keepers:
                                return False
                        if min_reg_days >= 0 and pvc:
                            reg_ts = pvc.get('reg_time', 0)
                            if reg_ts and reg_ts > 0:
                                age_days = (time.time() - reg_ts) / 86400
                                if age_days < min_reg_days:
                                    return False
                        if _my_uid_set and pvc:
                            k_list = pvc.get('keepers_list', [])
                            if _my_uid_set.intersection(k_list):
                                return False
                        return True

                    filtered = [c for c in candidates if _matches_filter(c)]
                    filter_desc = f"seeds <= {max_seeds}"
                    if max_keepers >= 0:
                        filter_desc += f", keepers <= {max_keepers}"
                    if max_leech >= 0:
                        filter_desc += f", leech <= {max_leech}"
                    if min_reg_days >= 0:
                        filter_desc += f", age >= {min_reg_days}d"
                    if _nick_lower:
                        filter_desc += ", hide my kept"
                    if skip_zero:
                        filter_desc += ", size > 0"
                    self.keepers_log(f"  {len(filtered)} match criteria ({filter_desc}).")

                    # 3. Add to Tree
                    for t in filtered:
                         if self.keepers_stop_event.is_set(): break
                         
                         status = "Not Kept"
                         if self.db_manager.is_torrent_kept(t['id']):
                             status = "Kept (DB)"
                         
                         # Check Client
                         tid = t['id']
                         if tid in fetched_hashes:
                             t_hash = fetched_hashes[tid]
                             if t_hash:
                                 h_lower = t_hash.lower()
                                 if h_lower in client_data:
                                     c_info = client_data[h_lower]
                                     state = c_info['state']
                                     progress = c_info['progress'] * 100
                                     status = f"{state} ({progress:.1f}%)"
                         
                         self.root.after(0, self._keepers_insert_tree, t, status)
                         found_count += 1
                     
                except Exception as e:
                    self.keepers_log(f"Error scanning page {page}: {e}")
                    break

                # Find "Next" button
                if 'class="pg">След.' not in resp.text and '&nbsp;След.&nbsp;' not in resp.text:
                   self.keepers_log("End of forum reached.")
                   break

                page += 1
                time.sleep(1) # Be polite
                
        except Exception as e:
            self.keepers_log(f"Scan error: {e}")
        
        self.keepers_log(f"Scan finished. Found {found_count} candidates.")
        self.db_manager.log_scan(cat_id, page*50, 0, found_count)

        # In batch mode, let the batch thread handle UI cleanup
        if not getattr(self, '_keepers_batch_mode', False):
            self.keepers_scan_active = False
            def _keepers_finish():
                self.keepers_progress.stop()
                self.keepers_prog_frame.pack_forget()
                self.keepers_scan_btn.config(state="normal")
                self.keepers_scan_all_btn.config(state="normal")
                self.keepers_stop_btn.config(state="disabled")
            self.root.after(0, _keepers_finish)

    def _keepers_insert_tree(self, t, status):
        link = f"https://rutracker.org/forum/viewtopic.php?t={t['id']}"
        
        # Pull generated hover data if we fetched PVC payload
        k_count, priority, str_last_seen, poster = 0, "", "", ""
        str_tor_status, str_reg_time = "", ""
        _TOR_STATUS_SYMBOLS = {0: "∗", 2: "√", 3: "?", 8: "#"}
        if hasattr(self, 'keepers_pvc_data'):
            try:
                tid_int = int(t['id'])
                if tid_int in self.keepers_pvc_data:
                    d = self.keepers_pvc_data[tid_int]
                    k_count = d.get('keepers_count', 0)

                    p_num = d.get('keeping_priority', 0)
                    if p_num == 0:
                        priority = "0 (Low)"
                    elif p_num == 1:
                        priority = "1 (Normal)"
                    elif p_num == 2:
                        priority = "2 (High)"
                    else:
                        priority = str(p_num)

                    poster = str(d.get('topic_poster', ''))

                    last_seen_ts = d.get("seeder_last_seen", 0)
                    if last_seen_ts > 0:
                        str_last_seen = fmt_dt(datetime.datetime.fromtimestamp(last_seen_ts), "datetime_sec")
                    else:
                        str_last_seen = "Never"

                    ts = d.get("tor_status", -1)
                    str_tor_status = _TOR_STATUS_SYMBOLS.get(ts, "—")

                    reg_ts = d.get("reg_time", 0)
                    if reg_ts and reg_ts > 0:
                        str_reg_time = fmt_dt(datetime.datetime.fromtimestamp(reg_ts), "date")
                    else:
                        str_reg_time = ""
            except:
                pass

        self.keepers_tree.insert("", "end", values=(
            t['id'], t['name'], t['size_str'], t['seeds'], t['leech'], status, link,
            k_count, priority, str_last_seen, poster, str_tor_status, str_reg_time
        ))

    def _keepers_toggle_cat_input(self):
        if self.keepers_cat_mode.get() == "custom":
            self.keepers_custom_cat_entry.config(state="normal")
        else:
            self.keepers_custom_cat_entry.config(state="disabled")

    def _keepers_on_double_click(self, event):
        item = self.keepers_tree.identify('item', event.x, event.y)
        col = self.keepers_tree.identify_column(event.x)
        if not item:
            return
        vals = self.keepers_tree.item(item, "values")
        if vals and len(vals) > 0:
            if col == '#11':  # Poster column
                poster_id = str(vals[10])
                if poster_id and poster_id != "Unknown":
                    webbrowser.open(f"https://rutracker.org/forum/profile.php?mode=viewprofile&u={poster_id}")
            else:
                tid = vals[0]
                webbrowser.open(f"https://rutracker.org/forum/viewtopic.php?t={tid}")

    def _keepers_on_right_click(self, event):
        item = self.keepers_tree.identify('item', event.x, event.y)
        col = self.keepers_tree.identify_column(event.x)
        if not item or not col:
            return
            
        self.keepers_tree.selection_set(item)
        col_idx = int(col.replace('#', '')) - 1
        vals = self.keepers_tree.item(item, "values")
        
        if vals and 0 <= col_idx < len(vals):
            col_name = self.keepers_tree.heading(col)["text"]
            cell_val = str(vals[col_idx])
            
            menu = tk.Menu(self.root, tearoff=0)
            menu.add_command(label=f"Copy {col_name}", command=lambda: self._keepers_copy_to_clipboard(cell_val))
            menu.tk_popup(event.x_root, event.y_root)
            
    def _keepers_copy_to_clipboard(self, text):
        self.root.clipboard_clear()
        self.root.clipboard_append(text)

    def _keepers_on_mouse_motion(self, event):
        item = self.keepers_tree.identify_row(event.y)
        col = self.keepers_tree.identify_column(event.x)
        
        if item and col == '#8': # Keepers Count Column
            vals = self.keepers_tree.item(item, "values")
            if vals and len(vals) > 0:
                try:
                    tid_int = int(vals[0])
                    if hasattr(self, 'keepers_pvc_data') and tid_int in self.keepers_pvc_data:
                        k_list = self.keepers_pvc_data[tid_int].get('keepers_list', [])
                        if k_list:
                            names = []
                            for kid in k_list:
                                n = self.db_manager.get_keepers_user(kid)
                                names.append(n)
                                
                            tip_text = "Keepers:\n" + "\n".join(names)
                            self.keepers_tooltip.x = event.x
                            self.keepers_tooltip.y = event.y
                            if self.keepers_tooltip.tip_window:
                                self.keepers_tooltip.tip_window.wm_geometry(
                                    f"+{self.keepers_tree.winfo_rootx() + event.x + 15}"
                                    f"+{self.keepers_tree.winfo_rooty() + event.y + 20}"
                                )
                                label = self.keepers_tooltip.tip_window.winfo_children()[0]
                                if label.cget("text") != tip_text:
                                    label.config(text=tip_text)
                            else:
                                self.keepers_tooltip.show_tip(tip_text)
                            return
                except:
                    pass
                    
        self.keepers_tooltip.hide_tip()

    def _keepers_on_mouse_leave(self, event):
        self.keepers_tooltip.hide_tip()

    def _keepers_export_csv(self):
        items = self.keepers_tree.get_children()
        if not items:
            messagebox.showinfo("Export", "No data to export.")
            return

        file_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title="Export Keepers to CSV"
        )
        
        if not file_path:
            return
            
        try:
            with open(file_path, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                
                # Write headers
                headers = [self.keepers_tree.heading(col)["text"] for col in self.keepers_tree["columns"]]
                writer.writerow(headers)
                
                # Write rows
                for item in items:
                    row_data = self.keepers_tree.item(item, "values")
                    writer.writerow(row_data)
                    
            messagebox.showinfo("Export Complete", f"Data exported successfully to:\n{file_path}")
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export data to CSV.\n\n{e}")

    def _keepers_scrape_ids(self, html_content):
        """Extract just topic IDs from viewforum."""
        # Look for <a id="tt-123" ... or viewtopic.php?t=123
        ids = []
        # Robust regex for topic links
        matches = re.findall(r'viewtopic\.php\?t=(\d+)', html_content)
        # Deduplicate and convert to int
        for m in matches:
            if m not in ids:
                ids.append(int(m))
        return ids

    def _get_rutracker_creds(self):
        """Extract Rutracker credentials from config."""
        auth = self.config.get("rutracker_auth", {})
        if auth:
            return auth.get("username", ""), auth.get("password", "")
        return "", ""

    def _download_torrent_content(self, tid, log_func=None):
        """Helper to download .torrent file content with auth retry."""
        _log = log_func or self.keepers_log
        try:
            dl_url = f"https://rutracker.org/forum/dl.php?t={tid}"
            t_content = self.cat_manager.session.get(dl_url).content

            if b'login_username' in t_content or b'login.php' in t_content:
                    _log(f"  Session expired. Logging in...")
                    user, pwd = self._get_rutracker_creds()
                    if user and pwd and self.cat_manager.login(user, pwd):
                        t_content = self.cat_manager.session.get(dl_url).content
                    else:
                        _log(f"  Login failed. Skipping {tid}.")
                        return None

            if b'bbtitle' in t_content: # Login page or error remaining
                    _log(f"  Failed to download .torrent for {tid}")
                    return None

            return t_content
        except Exception as e:
            _log(f"  Download error for {tid}: {e}")
            return None

    def _keepers_add_selected(self):
        selected = self.keepers_tree.selection()
        if not selected:
            return

        # Get client from combo
        client_idx = self.keepers_client_combo.current()
        if client_idx < 0: client_idx = 0
        client_conf = self.config["clients"][client_idx]
        
        s = self._get_qbit_session(client_conf)
        if not s:
            self.keepers_log("Could not connect to qBittorrent.")
            return

        count = 0
        for item in selected:
            vals = self.keepers_tree.item(item, "values")
            tid = vals[0]
            name = vals[1]
            try: seeds = int(vals[3])
            except: seeds = 0
            try: leech = int(vals[4])
            except: leech = 0
            
            self.keepers_log(f"Adding {tid}...")
            
            # Download .torrent
            t_content = self._download_torrent_content(tid)
            if not t_content:
                continue
                
            # Send to qBit
            save_path = f"{client_conf['base_save_path']}"
            
            # Determine Category
            qbit_cat = "Keepers" # Default
            if self.keepers_cat_mode.get() == "custom":
                custom_cat = self.keepers_custom_cat_entry.get().strip()
                if custom_cat: qbit_cat = custom_cat
            else:
                # Preserve Forum Category
                # We need to lookup category name for this TID
                # It might be in cache or we fetch it
                cat_info = self.cat_manager.get_category_for_topic(tid)
                if cat_info:
                    qbit_cat = cat_info.get("category", "Keepers")
                
            # Append category to save path if configured?
            # qBit usually handles category save paths if 'Automatic Torrent Management' is on or we set save_path manually.
            # If we set save_path manually, we might want to append category?
            # User didn't specify save path logic, but standard qBit behavior is:
            # If category has a save path, use it. If not, use default.
            # We are sending `savepath` param which OVERRIDES category path usually.
            # Let's append category to base path for organization
            save_path = os.path.join(save_path, qbit_cat).replace("\\", "/")
            
            start_paused = self.keepers_paused_var.get()
            
            files = {'torrents': (f'{tid}.torrent', t_content, 'application/x-bittorrent')}
            data = {
                'savepath': save_path,
                'category': qbit_cat,
                'paused': 'true' if start_paused else 'false',
                'tags': 'Keepers'
            }
            
            url = client_conf["url"].rstrip("/")
            try:
                resp = s.post(f"{url}/api/v2/torrents/add", files=files, data=data)
                
                if resp.status_code == 200:
                    self.keepers_log(f"  Added successfully.")
                    
                    # Parse info for DB size
                    t_info = parse_torrent_info(t_content) 
                    size_bytes = t_info.get('total_size', 0)
                    
                    # Add to DB
                    # Extract Category ID from combobox string "Name (123)"
                    try:
                        cat_str = self.keepers_cat_combo.get()
                        cat_id = int(re.search(r'\((\d+)\)$', cat_str).group(1))
                    except:
                        cat_id = 0

                    self.db_manager.add_kept_torrent(tid, "", name, size_bytes, seeds, leech, cat_id)
                    
                    self.keepers_tree.set(item, "status", "Added")
                    count += 1
                else:
                    self.keepers_log(f"  qBit Error: {resp.status_code} - {resp.text}")

            except Exception as e:
                self.keepers_log(f"  Add Error: {e}")

        # Update stats
        self.refresh_statistics()

        # Notify if any torrents were added
        if count > 0:
            self._tray_notify(
                "Keepers: Torrents Added",
                f"{count} torrent{'s' if count != 1 else ''} added to client successfully.",
            )

    def _keepers_download_torrent(self):
        selected = self.keepers_tree.selection()
        if not selected: return
        
        temp_dir = tempfile.gettempdir()
        count = 0
        for item in selected:
            vals = self.keepers_tree.item(item, "values")
            tid = vals[0]
            name = vals[1]
            
            self.keepers_log(f"Downloading {tid} to temp...")
            t_content = self._download_torrent_content(tid)
            if t_content:
                 # Clean name for filename
                 safe_name = "".join([c for c in name if c.isalpha() or c.isdigit() or c in " ._-()"]).strip()
                 fname = f"[{tid}] {safe_name}.torrent"
                 path = os.path.join(temp_dir, fname)
                 try:
                     with open(path, "wb") as f:
                         f.write(t_content)
                     count += 1
                 except Exception as e:
                     self.keepers_log(f"Error saving {fname}: {e}")
        
        if count > 0:
             messagebox.showinfo("Saved", f"Saved {count} torrents to:\n{temp_dir}")
             try:
                 os.startfile(temp_dir)
             except: pass

    # ================================================================
    # AUTO KEEPER — Automated multi-client distribution
    # ================================================================

    def _create_auto_keeper_ui(self):
        parent = self.keepers_auto_frame

        # --- Category Configuration ---
        cat_lf = self._tlf(parent, "autokeeper.categories", padx=5, pady=3)
        cat_lf.pack(fill="x", padx=5, pady=(5, 2))

        cat_tree_frame = tk.Frame(cat_lf)
        cat_tree_frame.pack(fill="x", padx=3, pady=3)

        ak_cat_cols = ("id", "name", "max_seeds", "max_keepers", "max_leech", "min_reg_days", "enabled")
        self.ak_cat_tree = ttk.Treeview(cat_tree_frame, columns=ak_cat_cols, show="headings", height=5)
        self.ak_cat_tree.heading("id", text=t("keepers.col_id"))
        self.ak_cat_tree.heading("name", text=t("common.name"))
        self.ak_cat_tree.heading("max_seeds", text=t("keepers.max_seeds"))
        self.ak_cat_tree.heading("max_keepers", text=t("keepers.max_keepers"))
        self.ak_cat_tree.heading("max_leech", text=t("keepers.max_leech"))
        self.ak_cat_tree.heading("min_reg_days", text=t("keepers.min_reg_days"))
        self.ak_cat_tree.heading("enabled", text=t("common.enabled"))

        self.ak_cat_tree.column("id", width=60)
        self.ak_cat_tree.column("name", width=280)
        self.ak_cat_tree.column("max_seeds", width=80)
        self.ak_cat_tree.column("max_keepers", width=90)
        self.ak_cat_tree.column("max_leech", width=80)
        self.ak_cat_tree.column("min_reg_days", width=90)
        self.ak_cat_tree.column("enabled", width=60)

        ak_cat_scroll = ttk.Scrollbar(cat_tree_frame, orient="vertical", command=self.ak_cat_tree.yview)
        self.ak_cat_tree.configure(yscrollcommand=ak_cat_scroll.set)
        ak_cat_scroll.pack(side="right", fill="y")
        self.ak_cat_tree.pack(side="left", fill="x", expand=True)

        # Double-click to edit thresholds
        self.ak_cat_tree.bind("<Double-1>", lambda e: self._ak_edit_thresholds())

        cat_btn_frame = tk.Frame(cat_lf)
        cat_btn_frame.pack(fill="x", padx=3, pady=(0, 3))

        self.ak_cat_var = tk.StringVar()
        self.ak_cat_combo = ttk.Combobox(cat_btn_frame, textvariable=self.ak_cat_var, width=40)
        cats = self.cat_manager.cache.get("categories", {})
        self.ak_all_cats = sorted(
            [f"{name} ({cid})" for cid, name in cats.items() if not str(cid).startswith("c")],
            key=lambda x: x.lower()
        )
        self.ak_cat_combo['values'] = self.ak_all_cats
        self.ak_cat_combo.pack(side="left", padx=(0, 5))
        self.ak_cat_combo.bind('<KeyRelease>', self._ak_filter_cats)

        self._tb(cat_btn_frame, "autokeeper.add_cat", command=self._ak_add_category).pack(side="left", padx=2)
        self._tb(cat_btn_frame, "autokeeper.remove_cat", command=self._ak_remove_category).pack(side="left", padx=2)
        self._tb(cat_btn_frame, "autokeeper.edit_thresholds", command=self._ak_edit_thresholds).pack(side="left", padx=2)

        # --- Target Clients ---
        client_lf = self._tlf(parent, "autokeeper.target_clients", padx=5, pady=3)
        client_lf.pack(fill="x", padx=5, pady=2)

        self.ak_client_checks = {}
        self.ak_client_frame = tk.Frame(client_lf)
        self.ak_client_frame.pack(fill="x", padx=3, pady=3)
        self._ak_build_client_checkboxes()

        client_btn_frame = tk.Frame(client_lf)
        client_btn_frame.pack(fill="x", padx=3, pady=(0, 3))

        self._tb(client_btn_frame, "autokeeper.refresh_disk", command=self._ak_refresh_disk_space).pack(side="left", padx=2)

        tk.Label(client_btn_frame, text=t("autokeeper.reserve_gb")).pack(side="left", padx=(15, 5))
        self.ak_reserve_var = tk.IntVar(value=self.config.get("auto_keeper", {}).get("disk_reserve_gb", 50))
        tk.Spinbox(client_btn_frame, from_=0, to=5000, textvariable=self.ak_reserve_var, width=6).pack(side="left")
        tk.Label(client_btn_frame, text=t("common.gb")).pack(side="left", padx=2)

        # --- Scan & Plan Row ---
        scan_frame = tk.Frame(parent)
        scan_frame.pack(fill="x", padx=5, pady=5)

        self.ak_scan_btn = self._tb(scan_frame, "autokeeper.scan_plan", command=self._ak_start_scan, bg="#dddddd")
        self.ak_scan_btn.pack(side="left", padx=5)

        self.ak_stop_btn = self._tb(scan_frame, "common.stop", command=self._ak_stop_scan, state="disabled")
        self.ak_stop_btn.pack(side="left", padx=2)

        tk.Label(scan_frame, text=t("autokeeper.max_total_size")).pack(side="left", padx=(15, 5))
        self.ak_max_total_var = tk.IntVar(value=self.config.get("auto_keeper", {}).get("max_total_size_gb", 500))
        tk.Spinbox(scan_frame, from_=1, to=99999, textvariable=self.ak_max_total_var, width=7).pack(side="left")
        tk.Label(scan_frame, text=t("common.gb")).pack(side="left", padx=2)

        # --- Progress Bar (hidden) ---
        self.ak_prog_frame = tk.Frame(parent)
        self.ak_progress = ttk.Progressbar(self.ak_prog_frame, mode='determinate')
        self.ak_progress.pack(fill="x", padx=5, pady=(2, 0))
        self.ak_progress_label = tk.Label(self.ak_prog_frame, text="", fg="#333333", font=("Segoe UI", 9))
        self.ak_progress_label.pack(fill="x", padx=5, pady=(0, 2))

        # --- Distribution Preview ---
        preview_lf = self._tlf(parent, "autokeeper.distribution_preview", padx=5, pady=3)
        preview_lf.pack(fill="both", expand=True, padx=5, pady=2)

        preview_tree_frame = tk.Frame(preview_lf)
        preview_tree_frame.pack(fill="both", expand=True, padx=3, pady=3)

        preview_cols = ("topic_id", "name", "size", "seeds", "keepers", "cat_name", "client", "save_path")
        self.ak_preview_tree = ttk.Treeview(preview_tree_frame, columns=preview_cols, show="headings")
        self.ak_preview_tree.heading("topic_id", text=t("keepers.col_id"))
        self.ak_preview_tree.heading("name", text=t("common.name"))
        self.ak_preview_tree.heading("size", text=t("common.size"))
        self.ak_preview_tree.heading("seeds", text=t("keepers.col_seeds"))
        self.ak_preview_tree.heading("keepers", text=t("keepers.col_k_count"))
        self.ak_preview_tree.heading("cat_name", text=t("common.category"))
        self.ak_preview_tree.heading("client", text=t("autokeeper.col_client"))
        self.ak_preview_tree.heading("save_path", text=t("autokeeper.col_save_path"))

        self.ak_preview_tree.column("topic_id", width=60)
        self.ak_preview_tree.column("name", width=300)
        self.ak_preview_tree.column("size", width=70)
        self.ak_preview_tree.column("seeds", width=50)
        self.ak_preview_tree.column("keepers", width=60)
        self.ak_preview_tree.column("cat_name", width=120)
        self.ak_preview_tree.column("client", width=100)
        self.ak_preview_tree.column("save_path", width=150)

        prev_scroll_y = ttk.Scrollbar(preview_tree_frame, orient="vertical", command=self.ak_preview_tree.yview)
        prev_scroll_x = ttk.Scrollbar(preview_tree_frame, orient="horizontal", command=self.ak_preview_tree.xview)
        self.ak_preview_tree.configure(yscrollcommand=prev_scroll_y.set, xscrollcommand=prev_scroll_x.set)
        prev_scroll_y.pack(side="right", fill="y")
        prev_scroll_x.pack(side="bottom", fill="x")
        self.ak_preview_tree.pack(side="left", fill="both", expand=True)

        # Summary label
        self.ak_summary_label = tk.Label(parent, text="", font=("Segoe UI", 9, "italic"), fg="#555555")
        self.ak_summary_label.pack(fill="x", padx=10, pady=(0, 2))

        # --- Approve & Add Row ---
        approve_frame = tk.Frame(parent)
        approve_frame.pack(fill="x", padx=5, pady=3)

        self.ak_approve_btn = self._tb(approve_frame, "autokeeper.approve_add",
            command=self._ak_approve_and_add, state="disabled", bg="#dddddd")
        self.ak_approve_btn.pack(side="left", padx=5)

        self.ak_paused_var = tk.BooleanVar(value=self.config.get("auto_keeper", {}).get("start_paused", True))
        self._tcb(approve_frame, "keepers.start_paused", variable=self.ak_paused_var).pack(side="left", padx=5)

        self._tb(approve_frame, "autokeeper.clear_plan", command=self._ak_clear_plan).pack(side="left", padx=5)

        # --- Log Area ---
        self.ak_log_area = scrolledtext.ScrolledText(parent, height=6, state='disabled')
        self.ak_log_area.pack(fill="x", padx=5, pady=5)

        # Load saved category config into tree
        self._ak_refresh_cat_tree()

    # --- Auto Keeper: Logging ---

    def _ak_log(self, msg):
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        _log_to_file("auto_keeper", msg)
        def _log():
            try:
                self.ak_log_area.config(state='normal')
                self.ak_log_area.insert(tk.END, line + "\n")
                self.ak_log_area.see(tk.END)
                self.ak_log_area.config(state='disabled')
            except: pass
        self.root.after(0, _log)

    # --- Auto Keeper: Category Management ---

    def _ak_filter_cats(self, event):
        typed = self.ak_cat_var.get().lower()
        if not typed:
            self.ak_cat_combo['values'] = self.ak_all_cats
        else:
            filtered = [c for c in self.ak_all_cats if typed in c.lower()]
            self.ak_cat_combo['values'] = filtered

    def _ak_add_category(self):
        cat_str = self.ak_cat_combo.get()
        if not cat_str:
            return
        match = re.search(r'\((\d+)\)$', cat_str)
        if not match:
            return
        cat_id = int(match.group(1))
        cat_name = cat_str[:match.start()].strip()

        ak_conf = self.config.setdefault("auto_keeper", {})
        cats = ak_conf.setdefault("categories", [])
        if any(c['id'] == cat_id for c in cats):
            return

        cats.append({
            "id": cat_id, "name": cat_name,
            "max_seeds": 3, "max_keepers": 0,
            "max_leech": -1, "min_reg_days": -1,
            "enabled": True
        })
        self.save_config()
        self._ak_refresh_cat_tree()

    def _ak_remove_category(self):
        sel = self.ak_cat_tree.selection()
        if not sel:
            return
        cat_id = int(self.ak_cat_tree.item(sel[0], "values")[0])
        ak_cats = self.config.get("auto_keeper", {}).get("categories", [])
        self.config["auto_keeper"]["categories"] = [c for c in ak_cats if c['id'] != cat_id]
        self.save_config()
        self._ak_refresh_cat_tree()

    def _ak_edit_thresholds(self):
        sel = self.ak_cat_tree.selection()
        if not sel:
            return
        cat_id = int(self.ak_cat_tree.item(sel[0], "values")[0])
        ak_cats = self.config.get("auto_keeper", {}).get("categories", [])
        cat_entry = next((c for c in ak_cats if c['id'] == cat_id), None)
        if not cat_entry:
            return

        dlg = tk.Toplevel(self.root)
        dlg.title(f"Edit Thresholds: {cat_entry['name']}")
        dlg.geometry("350x260")
        dlg.transient(self.root)
        dlg.grab_set()

        fields = [
            ("Max Seeds:", "max_seeds", 0, 100, cat_entry.get("max_seeds", 3)),
            ("Max Keepers:", "max_keepers", -1, 100, cat_entry.get("max_keepers", 0)),
            ("Max Leechers:", "max_leech", -1, 1000, cat_entry.get("max_leech", -1)),
            ("Min Age (days):", "min_reg_days", -1, 9999, cat_entry.get("min_reg_days", -1)),
        ]
        vars_dict = {}
        for i, (label, key, from_, to_, default) in enumerate(fields):
            tk.Label(dlg, text=label).grid(row=i, column=0, padx=10, pady=5, sticky="w")
            var = tk.IntVar(value=default)
            tk.Spinbox(dlg, from_=from_, to=to_, textvariable=var, width=8).grid(row=i, column=1, padx=10, pady=5)
            vars_dict[key] = var

        enabled_var = tk.BooleanVar(value=cat_entry.get("enabled", True))
        tk.Checkbutton(dlg, text="Enabled", variable=enabled_var).grid(row=len(fields), column=0, columnspan=2, pady=5)

        def _save():
            for key, var in vars_dict.items():
                cat_entry[key] = var.get()
            cat_entry["enabled"] = enabled_var.get()
            self.save_config()
            self._ak_refresh_cat_tree()
            dlg.destroy()

        tk.Button(dlg, text="Save", command=_save, width=10).grid(row=len(fields)+1, column=0, columnspan=2, pady=10)

    def _ak_refresh_cat_tree(self):
        for item in self.ak_cat_tree.get_children():
            self.ak_cat_tree.delete(item)
        for cat in self.config.get("auto_keeper", {}).get("categories", []):
            self.ak_cat_tree.insert("", "end", values=(
                cat['id'], cat['name'], cat.get('max_seeds', 3),
                cat.get('max_keepers', 0), cat.get('max_leech', -1),
                cat.get('min_reg_days', -1),
                "Yes" if cat.get('enabled', True) else "No"
            ))

    # --- Auto Keeper: Client Checkboxes & Disk Space ---

    def _ak_build_client_checkboxes(self):
        for w in self.ak_client_frame.winfo_children():
            w.destroy()
        self.ak_client_checks = {}

        target_names = self.config.get("auto_keeper", {}).get("target_clients", [])

        for client_conf in self.config.get("clients", []):
            if not client_conf.get("enabled", True):
                continue
            name = client_conf["name"]
            var = tk.BooleanVar(value=(name in target_names))
            row = tk.Frame(self.ak_client_frame)
            row.pack(fill="x", pady=1)

            cb = tk.Checkbutton(row, text=f"{name} ({client_conf['url']})", variable=var,
                command=self._ak_save_target_clients)
            cb.pack(side="left")

            space_lbl = tk.Label(row, text="Free: --", font=("Segoe UI", 9), fg="#666666")
            space_lbl.pack(side="left", padx=(15, 0))

            self.ak_client_checks[name] = {
                "var": var, "label": space_lbl, "conf": client_conf
            }

    def _ak_save_target_clients(self):
        selected = [name for name, info in self.ak_client_checks.items() if info["var"].get()]
        self.config.setdefault("auto_keeper", {})["target_clients"] = selected
        self.save_config()

    def _ak_refresh_disk_space(self):
        threading.Thread(target=self._ak_refresh_disk_space_thread, daemon=True).start()

    def _ak_refresh_disk_space_thread(self):
        self._ak_log("Refreshing disk space for target clients...")
        self.ak_client_space = {}

        for name, info in self.ak_client_checks.items():
            if not info["var"].get():
                continue
            client_conf = info["conf"]
            url = client_conf["url"].rstrip("/")
            base_path = client_conf.get("base_save_path", "")
            is_local = self._ak_is_local_client(url)

            free_bytes = 0

            if is_local and base_path:
                try:
                    usage = shutil.disk_usage(base_path)
                    free_bytes = usage.free
                    self._ak_log(f"  {name}: Local disk — Free: {format_size(free_bytes)}")
                except Exception as e:
                    self._ak_log(f"  {name}: shutil error: {e}, falling back to API...")
                    is_local = False

            if not is_local or free_bytes == 0:
                try:
                    s = self._get_qbit_session(client_conf)
                    if s:
                        resp = s.get(f"{url}/api/v2/sync/maindata", timeout=10)
                        if resp.status_code == 200:
                            data = resp.json()
                            server_state = data.get("server_state", {})
                            free_bytes = server_state.get("free_space_on_disk", 0)
                            self._ak_log(f"  {name}: API — Free: {format_size(free_bytes)}")
                        else:
                            self._ak_log(f"  {name}: API error {resp.status_code}")
                    else:
                        self._ak_log(f"  {name}: Could not connect.")
                except Exception as e:
                    self._ak_log(f"  {name}: API error: {e}")

            self.ak_client_space[name] = {
                "free": free_bytes,
                "base_save_path": base_path,
                "is_local": is_local, "conf": client_conf
            }

            lbl = info["label"]
            free_str = format_size(free_bytes) if free_bytes > 0 else "N/A"
            self.root.after(0, lambda l=lbl, s=free_str: l.config(text=f"Free: {s}"))

        self._ak_log("Disk space refresh complete.")

    @staticmethod
    def _ak_is_local_client(url):
        try:
            # Simple hostname extraction
            host_part = url.split("://")[-1].split("/")[0].split(":")[0]
            return host_part in ("localhost", "127.0.0.1", "::1", "0.0.0.0")
        except:
            return False

    # --- Auto Keeper: Scan & Plan ---

    def _ak_start_scan(self):
        ak_cats = [c for c in self.config.get("auto_keeper", {}).get("categories", []) if c.get("enabled", True)]
        if not ak_cats:
            messagebox.showwarning("Auto Keeper", "No enabled categories configured.")
            return

        targets = [name for name, info in self.ak_client_checks.items() if info["var"].get()]
        if not targets:
            messagebox.showwarning("Auto Keeper", "No target clients selected.")
            return

        if not self.ak_client_space:
            messagebox.showwarning("Auto Keeper", "Please refresh disk space first.")
            return

        self._ak_clear_plan()
        self.ak_stop_event.clear()
        self.ak_scan_active = True
        self.ak_scan_btn.config(state="disabled")
        self.ak_stop_btn.config(state="normal")
        self.ak_approve_btn.config(state="disabled")

        self.ak_prog_frame.pack(fill="x", padx=5, pady=2)
        self.ak_progress['value'] = 0
        self.ak_progress_label.config(text="Starting scan...")

        threading.Thread(target=self._ak_scan_plan_thread, args=(ak_cats, targets), daemon=True).start()

    def _ak_stop_scan(self):
        self.ak_stop_event.set()
        self._ak_log("Stop requested...")

    def _ak_scan_plan_thread(self, ak_cats, target_client_names):
        all_candidates = []
        total_cats = len(ak_cats)
        skip_kept = self.config.get("auto_keeper", {}).get("skip_already_kept", True)
        skip_zero = self.config.get("auto_keeper", {}).get("skip_zero_size", True)

        # Phase 1: Scan all categories via PVC
        for i, cat_conf in enumerate(ak_cats):
            if self.ak_stop_event.is_set():
                break

            cat_id = cat_conf['id']
            cat_name = cat_conf['name']
            max_seeds = cat_conf.get('max_seeds', 3)
            max_keepers = cat_conf.get('max_keepers', 0)
            max_leech = cat_conf.get('max_leech', -1)
            min_reg_days = cat_conf.get('min_reg_days', -1)

            progress_pct = int((i / total_cats) * 50)
            self.root.after(0, lambda p=progress_pct, n=cat_name, idx=i+1, tot=total_cats: [
                self.ak_progress.configure(value=p),
                self.ak_progress_label.config(text=f"Scanning {idx}/{tot}: {n}...")
            ])

            self._ak_log(f"Scanning category: {cat_name} ({cat_id})...")

            pvc_data = self._ak_fetch_pvc(cat_id)
            if not pvc_data:
                self._ak_log(f"  No PVC data for {cat_name}. Skipping.")
                continue

            cat_candidates = []
            for tid_str, d in pvc_data.items():
                tid = int(tid_str)

                size_bytes = d.get('size_bytes', 0)
                if skip_zero and size_bytes <= 0:
                    continue

                seeds = d.get('seeds', 0)
                if seeds > max_seeds:
                    continue

                keepers_count = d.get('keepers_count', 0)
                if max_keepers >= 0 and keepers_count > max_keepers:
                    continue

                leechers = d.get('leechers', 0)
                if max_leech >= 0 and leechers > max_leech:
                    continue

                if min_reg_days >= 0:
                    reg_ts = d.get('reg_time', 0)
                    if reg_ts and reg_ts > 0:
                        if (time.time() - reg_ts) / 86400 < min_reg_days:
                            continue

                if skip_kept and self.db_manager.is_torrent_kept(tid):
                    continue

                if self.db_manager.is_auto_keeper_planned(tid):
                    continue

                cat_candidates.append({
                    "topic_id": tid,
                    "name": f"Topic #{tid}",
                    "size_bytes": size_bytes,
                    "seeds": seeds,
                    "keepers_count": keepers_count,
                    "leechers": leechers,
                    "cat_id": cat_id,
                    "cat_name": cat_name,
                    "keeping_priority": d.get('keeping_priority', 0)
                })

            self._ak_log(f"  {len(cat_candidates)} candidates from {cat_name} (of {len(pvc_data)} total).")
            all_candidates.extend(cat_candidates)

            if not self.ak_stop_event.is_set():
                time.sleep(0.5)

        if self.ak_stop_event.is_set():
            self._ak_log("Scan stopped by user.")
            self._ak_finish_scan()
            return

        self._ak_log(f"Total candidates across all categories: {len(all_candidates)}")

        if not all_candidates:
            self._ak_log("No candidates found matching filters.")
            self._ak_finish_scan()
            return

        # Phase 2: Hydrate topic names via API
        self.root.after(0, lambda: [
            self.ak_progress.configure(value=55),
            self.ak_progress_label.config(text="Fetching topic names...")
        ])

        all_tids = [str(c['topic_id']) for c in all_candidates]
        tid_to_name = {}
        for chunk_start in range(0, len(all_tids), 100):
            if self.ak_stop_event.is_set():
                break
            chunk = all_tids[chunk_start:chunk_start + 100]
            try:
                resp = self.cat_manager.session.get(
                    "https://api.rutracker.cc/v1/get_tor_topic_data",
                    params={"by": "topic_id", "val": ",".join(chunk)},
                    proxies=self.get_requests_proxies(),
                    timeout=15
                )
                if resp.status_code == 200:
                    result = resp.json().get("result", {})
                    for tid_str, info in result.items():
                        if info and info.get("topic_title"):
                            tid_to_name[int(tid_str)] = html.unescape(info["topic_title"])
            except Exception as e:
                self._ak_log(f"  Name fetch error: {e}")
            time.sleep(0.3)

        for c in all_candidates:
            if c['topic_id'] in tid_to_name:
                c['name'] = tid_to_name[c['topic_id']]

        # Phase 3: Sort by urgency
        all_candidates.sort(key=lambda c: (c['seeds'], c['keepers_count'], -c.get('keeping_priority', 0)))

        # Phase 4: Distribute proportionally across clients
        self.root.after(0, lambda: [
            self.ak_progress.configure(value=70),
            self.ak_progress_label.config(text="Computing distribution plan...")
        ])

        max_total_bytes = self.ak_max_total_var.get() * (1024 ** 3)
        reserve_bytes = self.ak_reserve_var.get() * (1024 ** 3)

        client_avail = {}
        for cname in target_client_names:
            cinfo = self.ak_client_space.get(cname)
            if cinfo:
                avail = max(0, cinfo["free"] - reserve_bytes)
                if avail > 0:
                    client_avail[cname] = avail

        total_avail = sum(client_avail.values())
        if total_avail <= 0:
            self._ak_log("No available space on any target client after reserve. Aborting.")
            self._ak_finish_scan()
            return

        client_weights = {name: avail / total_avail for name, avail in client_avail.items()}

        client_assigned_bytes = {name: 0 for name in client_avail}
        plan = []
        total_planned_bytes = 0

        for candidate in all_candidates:
            if self.ak_stop_event.is_set():
                break

            size = candidate['size_bytes']
            if total_planned_bytes + size > max_total_bytes:
                break

            best_client = None
            best_deficit = -float('inf')

            for cname in client_avail:
                if client_assigned_bytes[cname] + size > client_avail[cname]:
                    continue
                ideal_share = client_weights[cname] * (total_planned_bytes + size)
                deficit = ideal_share - client_assigned_bytes[cname]
                if deficit > best_deficit:
                    best_deficit = deficit
                    best_client = cname

            if best_client is None:
                continue

            client_conf = self.ak_client_checks[best_client]["conf"]
            base_path = client_conf.get("base_save_path", "/").rstrip("/")
            qbit_cat = self.config.get("auto_keeper", {}).get("qbit_category", "AutoKeeper")
            save_path = f"{base_path}/{qbit_cat}".replace("\\", "/")

            plan_entry = {
                **candidate,
                "client_name": best_client,
                "save_path": save_path
            }
            plan.append(plan_entry)
            client_assigned_bytes[best_client] += size
            total_planned_bytes += size

        self.ak_distribution_plan = plan

        # Phase 5: Populate preview treeview
        self.root.after(0, lambda: [
            self.ak_progress.configure(value=90),
            self.ak_progress_label.config(text="Building preview...")
        ])

        for entry in plan:
            self.root.after(0, lambda e=entry: self.ak_preview_tree.insert("", "end", values=(
                e['topic_id'], e['name'], format_size(e['size_bytes']),
                e['seeds'], e['keepers_count'], e['cat_name'],
                e['client_name'], e['save_path']
            )))

        per_client = {}
        for e in plan:
            cn = e['client_name']
            per_client.setdefault(cn, {"count": 0, "size": 0})
            per_client[cn]["count"] += 1
            per_client[cn]["size"] += e['size_bytes']

        summary_parts = [f"{cn}: {info['count']} topics ({format_size(info['size'])})" for cn, info in per_client.items()]
        summary = f"{len(plan)} topics, {format_size(total_planned_bytes)} total  |  " + "  |  ".join(summary_parts)

        self._ak_log(f"Distribution plan ready: {summary}")
        self.root.after(0, lambda s=summary: self.ak_summary_label.config(text=s))

        if plan:
            self.root.after(0, lambda: self.ak_approve_btn.config(state="normal"))

        self._ak_finish_scan()

    def _ak_finish_scan(self):
        self.ak_scan_active = False
        def _finish():
            self.ak_progress.configure(value=100)
            self.ak_prog_frame.pack_forget()
            self.ak_scan_btn.config(state="normal")
            self.ak_stop_btn.config(state="disabled")
        self.root.after(0, _finish)

    def _ak_fetch_pvc(self, cat_id):
        """Fetch PVC data for a category, with cache. Returns dict {tid_str: parsed_dict} or None."""
        pvc_cache = self.db_manager.get_pvc_data(cat_id)
        raw_result = {}

        if pvc_cache:
            json_str, ts = pvc_cache
            if time.time() - ts < 21600:  # 6 hours
                try:
                    raw_result = json.loads(json_str)
                except:
                    pass

        if not raw_result:
            try:
                resp = self.cat_manager.session.get(
                    f"https://api.rutracker.cc/v1/static/pvc/f/{cat_id}",
                    proxies=self.get_requests_proxies(),
                    timeout=15)
                if resp.status_code == 200:
                    data = resp.json()
                    raw_result = data.get("result", {})
                    if raw_result:
                        self.db_manager.save_pvc_data(cat_id, json.dumps(raw_result))
            except Exception as e:
                self._ak_log(f"  PVC fetch error: {e}")
                return None

        if not raw_result:
            return None

        parsed = {}
        for tid_str, vals in raw_result.items():
            try:
                if isinstance(vals, list) and len(vals) >= 10:
                    kf_list = vals[5] if isinstance(vals[5], list) else []
                    parsed[tid_str] = {
                        "tor_status": vals[0],
                        "seeds": vals[1],
                        "reg_time": vals[2],
                        "size_bytes": vals[3],
                        "keeping_priority": vals[4],
                        "keepers_count": len(kf_list),
                        "keepers_list": kf_list,
                        "seeder_last_seen": vals[6],
                        "topic_poster": vals[8],
                        "leechers": vals[9]
                    }
            except:
                pass

        return parsed

    # --- Auto Keeper: Approve & Add ---

    def _ak_approve_and_add(self):
        if not self.ak_distribution_plan:
            return

        count = len(self.ak_distribution_plan)
        client_count = len(set(e['client_name'] for e in self.ak_distribution_plan))
        total_size = sum(e['size_bytes'] for e in self.ak_distribution_plan)

        if not messagebox.askyesno("Auto Keeper",
            f"Add {count} torrents ({format_size(total_size)}) to {client_count} client(s)?"):
            return

        self.ak_approve_btn.config(state="disabled")
        self.ak_scan_btn.config(state="disabled")
        self.ak_stop_event.clear()

        self.ak_prog_frame.pack(fill="x", padx=5, pady=2)
        self.ak_progress['value'] = 0

        threading.Thread(target=self._ak_add_thread, daemon=True).start()

    def _ak_add_thread(self):
        plan = self.ak_distribution_plan
        total = len(plan)
        success = 0
        failed = 0

        # Pre-create sessions per client
        client_sessions = {}
        for entry in plan:
            cn = entry['client_name']
            if cn not in client_sessions:
                conf = self.ak_client_checks[cn]["conf"]
                s = self._get_qbit_session(conf)
                if s:
                    client_sessions[cn] = (s, conf)
                else:
                    self._ak_log(f"Could not connect to {cn}. Its torrents will be skipped.")

        paused = self.ak_paused_var.get()
        qbit_cat = self.config.get("auto_keeper", {}).get("qbit_category", "AutoKeeper")
        db_entries = []

        for i, entry in enumerate(plan):
            if self.ak_stop_event.is_set():
                self._ak_log("Add process stopped by user.")
                break

            tid = entry['topic_id']
            cn = entry['client_name']
            self._ak_log(f"[{i+1}/{total}] Adding topic {tid} to {cn}...")

            pct = int((i / total) * 100)
            self.root.after(0, lambda p=pct, idx=i+1, tot=total: [
                self.ak_progress.configure(value=p),
                self.ak_progress_label.config(text=f"Adding {idx}/{tot}...")
            ])

            if cn not in client_sessions:
                self._ak_log(f"  Skipped (no session for {cn}).")
                db_entries.append((tid, cn, entry['cat_id'], entry['size_bytes'], 'failed'))
                failed += 1
                continue

            s, conf = client_sessions[cn]
            url = conf["url"].rstrip("/")

            t_content = self._download_torrent_content(tid, log_func=self._ak_log)
            if not t_content:
                db_entries.append((tid, cn, entry['cat_id'], entry['size_bytes'], 'failed'))
                failed += 1
                continue

            files = {'torrents': (f'{tid}.torrent', t_content, 'application/x-bittorrent')}
            data = {
                'savepath': entry['save_path'],
                'category': qbit_cat,
                'paused': 'true' if paused else 'false',
                'tags': 'AutoKeeper'
            }

            try:
                resp = s.post(f"{url}/api/v2/torrents/add", files=files, data=data, timeout=30)
                if resp.status_code == 200:
                    self._ak_log(f"  Added successfully to {cn}.")
                    success += 1
                    db_entries.append((tid, cn, entry['cat_id'], entry['size_bytes'], 'added'))

                    t_info = parse_torrent_info(t_content)
                    real_size = t_info.get('total_size', entry['size_bytes'])
                    self.db_manager.add_kept_torrent(
                        tid, "", entry['name'], real_size,
                        entry['seeds'], entry.get('leechers', 0), entry['cat_id']
                    )
                else:
                    self._ak_log(f"  qBit error: {resp.status_code} {resp.text}")
                    db_entries.append((tid, cn, entry['cat_id'], entry['size_bytes'], 'failed'))
                    failed += 1
            except Exception as e:
                self._ak_log(f"  Error: {e}")
                db_entries.append((tid, cn, entry['cat_id'], entry['size_bytes'], 'failed'))
                failed += 1

            time.sleep(0.3)

        if db_entries:
            self.db_manager.save_auto_keeper_batch(db_entries)

        self._ak_log(f"Auto Keeper complete: {success} added, {failed} failed out of {total} planned.")

        if success > 0:
            self._tray_notify(
                "Auto Keeper: Torrents Added",
                f"{success} torrent{'s' if success != 1 else ''} distributed across clients."
            )

        self.refresh_statistics()

        def _done():
            self.ak_approve_btn.config(state="disabled")
            self.ak_scan_btn.config(state="normal")
            self.ak_prog_frame.pack_forget()
        self.root.after(0, _done)

    def _ak_clear_plan(self):
        self.ak_distribution_plan = []
        for item in self.ak_preview_tree.get_children():
            self.ak_preview_tree.delete(item)
        self.ak_summary_label.config(text="")
        self.ak_approve_btn.config(state="disabled")

    def refresh_statistics(self):
        count, size = self.db_manager.get_kept_stats()
        self.stats_label_count.config(text=t("settings.stats_kept", count=count))
        self.stats_label_size.config(text=t("settings.stats_size", size=format_size(size)))
        
        # External Fetchers
        active_size = 0
        global_count = 0
        global_size = 0
        up_speed = 0
        dl_speed = 0
        bitrot_checked = 0
        bitrot_rot = 0
        
        
        try:
            # Local Database Lookups
            history = self.db_manager.get_bitrot_history()
            bitrot_checked = len(history)
            bitrot_rot = sum(1 for v in history.values() if v.get("status") == "Bitrot Detected")
            self.stats_label_bitrot.config(text=t("settings.stats_bitrot", count=f"{bitrot_checked} ({bitrot_rot})"))

            mover_count = self.db_manager.get_mover_stats()
            self.stats_label_mover.config(text=t("settings.stats_mover", count=mover_count))
            
            # API Lookups
            client_conf = self.config["clients"][self.config["last_selected_client_index"]]
            s = self._get_qbit_session(client_conf)
            
            if s:
                url = client_conf["url"].rstrip("/")
                
                # Fetch global transfer rates
                try:
                    t_resp = s.get(f"{url}/api/v2/transfer/info", timeout=5)
                    if t_resp.status_code == 200:
                        t_data = t_resp.json()
                        dl_speed = t_data.get("dl_info_speed", 0)
                        up_speed = t_data.get("up_info_speed", 0)
                except: pass
                
                self.stats_label_global_net.config(text=t("settings.stats_net", ul=f"{format_size(up_speed)}/s", dl=f"{format_size(dl_speed)}/s"))

                # Get aggregate torrent sums
                try:
                    resp = s.get(f"{url}/api/v2/torrents/info", timeout=10)
                    if resp.status_code == 200:
                        torrents = resp.json()
                        global_count = len(torrents)
                        for torr in torrents:
                            global_size += torr.get('size', 0)
                            if torr.get('category') == "Keepers":
                                active_size += torr.get('size', 0)
                                
                        self.stats_label_active.config(text=t("settings.stats_active", size=format_size(active_size)))
                        self.stats_label_global_client.config(text=t("settings.stats_total", count=global_count, size=format_size(global_size)))
                    else:
                        self.stats_label_active.config(text=t("settings.stats_active", size=f"(Error {resp.status_code})"))
                except:
                    self.stats_label_active.config(text=t("settings.stats_active", size="(Error)"))
            else:
                self.stats_label_active.config(text=t("settings.stats_active", size="N/A"))
                self.stats_label_global_net.config(text=t("settings.stats_net", ul="?", dl="?"))
                self.stats_label_global_client.config(text=t("settings.stats_total", count=0, size="N/A"))
                
        except Exception as e:
            self.stats_label_active.config(text=f"Active Seeding Size: (Error)")
            print(f"Stats Error: {e}")


    # ===================================================================
    # FOLDER SCANNER TAB
    # ===================================================================

    def create_scanner_ui(self):
        # --- Scan Controls ---
        ctrl_frame = self._tlf(self.scanner_tab, "common.scan_controls", padx=10, pady=5)
        ctrl_frame.pack(fill="x", padx=10, pady=5)

        row1 = tk.Frame(ctrl_frame)
        row1.pack(fill="x", pady=2)
        self._tl(row1, "scanner.folder").pack(side="left")
        self.scanner_folder_var = tk.StringVar()
        tk.Entry(row1, textvariable=self.scanner_folder_var, width=60).pack(side="left", padx=5)
        self._tb(row1, "common.browse", command=self._scanner_browse_folder).pack(side="left")

        row2 = tk.Frame(ctrl_frame)
        row2.pack(fill="x", pady=2)
        self._tl(row2, "common.client").pack(side="left")
        self.scanner_client_selector = ttk.Combobox(row2, state="readonly", width=25)
        self.scanner_client_selector.pack(side="left", padx=5)
        self.scanner_client_selector.bind("<<ComboboxSelected>>", lambda e: self._scanner_on_client_changed())

        self.scanner_scan_btn = self._tb(row2, "common.scan", command=self.scanner_start_scan)
        self.scanner_scan_btn.pack(side="left", padx=5)

        self.scanner_stop_btn = self._tb(row2, "common.stop", command=self.scanner_stop_scan, state="disabled")
        self.scanner_stop_btn.pack(side="left", padx=5)

        self.scanner_cache_label = self._tl(row2, "common.list_updated_never", fg="gray")
        self.scanner_cache_label.pack(side="left", padx=10)

        # --- Options ---
        opts_frame = tk.Frame(self.scanner_tab)
        opts_frame.pack(fill="x", padx=10, pady=2)

        self.scanner_recursive_var = tk.BooleanVar(value=True)
        self._tcb(opts_frame, "scanner.recursive",
            variable=self.scanner_recursive_var).pack(side="left", padx=5)

        self.scanner_use_parent_var = tk.BooleanVar(value=True)
        self._tcb(opts_frame, "scanner.use_parent",
            variable=self.scanner_use_parent_var).pack(side="left", padx=5)

        self.scanner_skip_subid_var = tk.BooleanVar(value=True)
        self._tcb(opts_frame, "scanner.skip_subid",
            variable=self.scanner_skip_subid_var).pack(side="left", padx=5)

        self.scanner_start_paused_var = tk.BooleanVar(value=True)
        self._tcb(opts_frame, "scanner.start_paused",
            variable=self.scanner_start_paused_var).pack(side="left", padx=5)

        self.scanner_deep_scan_var = tk.BooleanVar(value=False)
        self._tcb(opts_frame, "scanner.deep_scan",
            variable=self.scanner_deep_scan_var, fg="darkblue").pack(side="left", padx=5)

        self.scanner_deep_scan_plus_var = tk.BooleanVar(value=False)
        self._tcb(opts_frame, "scanner.deep_scan_plus",
            variable=self.scanner_deep_scan_plus_var, fg="purple").pack(side="left", padx=5)

        # --- Custom Add Options ---
        custom_frame = self._tlf(self.scanner_tab, "scanner.custom_add", padx=10, pady=5)
        custom_frame.pack(fill="x", padx=10, pady=5)

        fs_frame = self._tlf(custom_frame, "adder.folder_structure", padx=5, pady=5)
        fs_frame.pack(fill="x", padx=5, pady=5)

        self.scanner_create_cat_var = tk.BooleanVar(value=True)
        self._tcb(fs_frame, "adder.create_cat_subfolder", variable=self.scanner_create_cat_var).pack(anchor="w")

        self.scanner_create_id_var = tk.BooleanVar(value=True)
        self._tcb(fs_frame, "adder.create_id_subfolder", variable=self.scanner_create_id_var).pack(anchor="w")

        path_frame = tk.Frame(custom_frame)
        path_frame.pack(fill="x", padx=5, pady=5)

        self.scanner_use_custom_var = tk.BooleanVar(value=False)
        self._tcb(path_frame, "scanner.override_path", variable=self.scanner_use_custom_var, command=self._scanner_toggle_custom_options).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 5))

        self._tl(path_frame, "adder.save_path").grid(row=1, column=0, sticky="w")
        self.scanner_custom_path_entry = tk.Entry(path_frame, width=30)
        self.scanner_custom_path_entry.grid(row=1, column=1, padx=5, pady=2)

        self.scanner_browse_custom_path_btn = self._tb(path_frame, "common.browse", command=self._scanner_browse_custom_path, width=10)
        self.scanner_browse_custom_path_btn.grid(row=1, column=2, padx=5)

        self._scanner_toggle_custom_options()

        # --- Progress (hidden until scan) ---
        self.scanner_prog_frame = tk.Frame(self.scanner_tab)
        self.scanner_progress = ttk.Progressbar(self.scanner_prog_frame, mode='determinate', style="green.Horizontal.TProgressbar")
        self.scanner_progress.pack(fill="x", padx=5, pady=(2, 0))
        self.scanner_progress_label = tk.Label(self.scanner_prog_frame, text="", fg="#333333", font=("Segoe UI", 9))
        self.scanner_progress_label.pack(fill="x", padx=5, pady=(0, 2))

        # --- Results Treeview ---
        results_frame = self._tlf(self.scanner_tab, "scanner.results", padx=5, pady=5)
        results_frame.pack(fill="both", expand=True, padx=10, pady=5)

        tree_container = tk.Frame(results_frame)
        tree_container.pack(fill="both", expand=True)

        cols = ("topic_id", "name", "size", "disk_size", "seeds", "leech", "status", "category", "in_qbit", "extra", "missing", "mismatch", "pieces", "disk_path")
        self.scanner_tree = ttk.Treeview(tree_container, columns=cols, show="headings", selectmode="extended")

        self._tr_heading(self.scanner_tree, "topic_id", "updater.topic_id",
            command=lambda: self.sort_tree(self.scanner_tree, "topic_id", False))
        self._tr_heading(self.scanner_tree, "name", "common.name",
            command=lambda: self.sort_tree(self.scanner_tree, "name", False))
        self._tr_heading(self.scanner_tree, "size", "common.size",
            command=lambda: self.sort_tree(self.scanner_tree, "size", False))
        self._tr_heading(self.scanner_tree, "disk_size", "scanner.disk_size",
            command=lambda: self.sort_tree(self.scanner_tree, "disk_size", False))
        self._tr_heading(self.scanner_tree, "seeds", "scanner.seeds",
            command=lambda: self.sort_tree(self.scanner_tree, "seeds", False))
        self._tr_heading(self.scanner_tree, "leech", "scanner.leech",
            command=lambda: self.sort_tree(self.scanner_tree, "leech", False))
        self._tr_heading(self.scanner_tree, "status", "scanner.rt_status",
            command=lambda: self.sort_tree(self.scanner_tree, "status", False))
        self._tr_heading(self.scanner_tree, "category", "common.category",
            command=lambda: self.sort_tree(self.scanner_tree, "category", False))
        self._tr_heading(self.scanner_tree, "in_qbit", "scanner.in_qbit",
            command=lambda: self.sort_tree(self.scanner_tree, "in_qbit", False))
        self._tr_heading(self.scanner_tree, "extra", "scanner.extra",
            command=lambda: self.sort_tree(self.scanner_tree, "extra", False))
        self._tr_heading(self.scanner_tree, "missing", "scanner.missing",
            command=lambda: self.sort_tree(self.scanner_tree, "missing", False))
        self._tr_heading(self.scanner_tree, "mismatch", "scanner.mismatch",
            command=lambda: self.sort_tree(self.scanner_tree, "mismatch", False))
        self._tr_heading(self.scanner_tree, "pieces", "scanner.pieces",
            command=lambda: self.sort_tree(self.scanner_tree, "pieces", False))
        self._tr_heading(self.scanner_tree, "disk_path", "scanner.disk_path",
            command=lambda: self.sort_tree(self.scanner_tree, "disk_path", False))

        self.scanner_tree.column("topic_id", width=70, minwidth=50)
        self.scanner_tree.column("name", width=280, minwidth=120)
        self.scanner_tree.column("size", width=80, minwidth=50)
        self.scanner_tree.column("disk_size", width=85, minwidth=55)
        self.scanner_tree.column("seeds", width=50, minwidth=35)
        self.scanner_tree.column("leech", width=50, minwidth=35)
        self.scanner_tree.column("status", width=80, minwidth=60)
        self.scanner_tree.column("category", width=120, minwidth=60)
        self.scanner_tree.column("in_qbit", width=55, minwidth=40)
        self.scanner_tree.column("extra", width=50, minwidth=40)
        self.scanner_tree.column("missing", width=50, minwidth=40)
        self.scanner_tree.column("mismatch", width=60, minwidth=50)
        self.scanner_tree.column("pieces", width=120, minwidth=80)
        self.scanner_tree.column("disk_path", width=200, minwidth=80)

        tree_scroll = ttk.Scrollbar(tree_container, orient="vertical", command=self.scanner_tree.yview)
        tree_scroll_x = ttk.Scrollbar(tree_container, orient="horizontal", command=self.scanner_tree.xview)
        self.scanner_tree.configure(yscrollcommand=tree_scroll.set, xscrollcommand=tree_scroll_x.set)
        
        tree_scroll.pack(side="right", fill="y")
        tree_scroll_x.pack(side="bottom", fill="x")
        self.scanner_tree.pack(side="left", fill="both", expand=True)

        self.scanner_tree.tag_configure("in_client", foreground="dark green")
        self.scanner_tree.tag_configure("missing", foreground="dark red")
        self.scanner_tree.tag_configure("dead", foreground="gray")
        self.scanner_tree.tag_configure("size_empty", background="#ffcccc")
        self.scanner_tree.tag_configure("size_smaller", background="#ffe8cc")
        self.scanner_tree.tag_configure("size_larger", background="#e6f2ff")

        self.scanner_tree.bind("<Double-1>", self._scanner_on_double_click)

        self.scanner_summary_label = self._tl(results_frame, "scanner.scan_hint", fg="gray")
        self.scanner_summary_label.pack(anchor="w", padx=5)

        # --- Filter ---
        filter_frame = tk.Frame(results_frame)
        filter_frame.pack(anchor="w", padx=5, pady=(2, 0))
        self.scanner_filter_zero_var = tk.BooleanVar(value=False)
        self._tcb(filter_frame, "scanner.show_zero",
            variable=self.scanner_filter_zero_var,
            command=self._scanner_apply_filter).pack(side="left")
        self.scanner_filter_count_label = tk.Label(filter_frame, text="", fg="gray")
        self.scanner_filter_count_label.pack(side="left", padx=8)

        # --- Action Buttons ---
        btn_frame = tk.Frame(self.scanner_tab)
        btn_frame.pack(fill="x", padx=10, pady=3)

        self.scanner_add_btn = self._tb(btn_frame, "scanner.add_selected",
            command=self._scanner_add_selected, state="disabled")
        self.scanner_add_btn.pack(side="left", padx=3)

        self.scanner_add_all_btn = self._tb(btn_frame, "scanner.add_all",
            command=self._scanner_add_all_missing, state="disabled")
        self.scanner_add_all_btn.pack(side="left", padx=3)

        self.scanner_dl_btn = self._tb(btn_frame, "common.download_torrent",
            command=self._scanner_download_torrent, state="disabled")
        self.scanner_dl_btn.pack(side="left", padx=3)

        self.scanner_del_qbit_btn = self._tb(btn_frame, "scanner.del_qbit",
            command=self._scanner_delete_from_qbit, state="disabled")
        self.scanner_del_qbit_btn.pack(side="right", padx=3)

        self.scanner_del_data_var = tk.IntVar(value=0)
        self.scanner_del_data_chk = self._tcb(btn_frame, "scanner.del_data", variable=self.scanner_del_data_var, state="disabled")
        self.scanner_del_data_chk.pack(side="right", padx=0)

        self.scanner_del_os_btn = self._tb(btn_frame, "scanner.del_os",
            command=self._scanner_delete_os_data, state="disabled")
        self.scanner_del_os_btn.pack(side="right", padx=3)

        # --- Log ---
        log_frame = self._tlf(self.scanner_tab, "common.log", padx=5, pady=5)
        log_frame.pack(fill="x", padx=10, pady=5)

        self.scanner_log_area = scrolledtext.ScrolledText(log_frame, height=6, state="disabled", wrap="word")
        self.scanner_log_area.pack(fill="x")

        self.update_scanner_client_dropdown()

    # --- Utility Methods ---

    def _scanner_toggle_custom_options(self):
        state = "normal" if self.scanner_use_custom_var.get() else "disabled"
        if hasattr(self, 'scanner_custom_path_entry'):
            self.scanner_custom_path_entry.config(state=state)
            self.scanner_browse_custom_path_btn.config(state=state)

    def _scanner_browse_custom_path(self):
        path = filedialog.askdirectory()
        if path:
            self.scanner_custom_path_entry.delete(0, tk.END)
            self.scanner_custom_path_entry.insert(0, path)

    def _scanner_apply_filter(self):
        """Re-populate the scanner treeview based on the 0B filter checkbox."""
        results = getattr(self, "scanner_scan_results", [])
        if not results:
            self.scanner_filter_count_label.config(text="")
            return

        filter_zero = self.scanner_filter_zero_var.get()

        # Clear treeview
        for item in self.scanner_tree.get_children():
            self.scanner_tree.delete(item)

        shown = 0
        for entry in results:
            if filter_zero and entry.get("disk_size", -1) != 0:
                continue

            tags = [entry["tag"]]
            # Recompute size tag
            api_size = int(entry.get("size", 0) or 0)
            disk_size = entry.get("disk_size", 0)
            if api_size and disk_size == 0:
                tags.append("size_empty")
            elif api_size and disk_size < api_size * 0.95:
                tags.append("size_smaller")
            elif api_size and disk_size > api_size * 1.05:
                tags.append("size_larger")

            self.scanner_tree.insert("", "end",
                values=(entry["topic_id"], entry["name"], entry["size_str"],
                        entry["disk_size_str"], entry["seeds"], entry["leech"],
                        entry["rt_status"], entry["category"], entry["in_qbit"],
                        entry["extra"], entry["missing"], entry["mismatch"],
                        entry["pieces"], entry["disk_path"]),
                tags=tuple(tags))
            shown += 1

        if filter_zero:
            self.scanner_filter_count_label.config(text=f"Showing {shown} of {len(results)}")
        else:
            self.scanner_filter_count_label.config(text="")

    def scanner_log(self, message):
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {message}"
        _log_to_file("scanner", message)
        def _write():
            self.scanner_log_area.config(state="normal")
            self.scanner_log_area.insert(tk.END, line + "\n")
            self.scanner_log_area.see(tk.END)
            self.scanner_log_area.config(state="disabled")
        self.root.after(0, _write)

    def update_scanner_client_dropdown(self):
        if hasattr(self, 'scanner_client_selector'):
            names = [c["name"] for c in self.config["clients"] if c.get("enabled", False)]
            self.scanner_client_selector['values'] = names
            idx = self.config.get("last_selected_client_index", 0)
            if idx >= len(names):
                idx = 0
            if names:
                self.scanner_client_selector.current(idx)

    def _scanner_on_client_changed(self):
        idx = self.scanner_client_selector.current()
        if 0 <= idx < len(self.config["clients"]):
            client_name = self.config["clients"][idx]["name"]
            self._show_cache_time_for_client(client_name, self.scanner_cache_label)

    def _scanner_browse_folder(self):
        path = filedialog.askdirectory(title="Select root folder to scan")
        if path:
            self.scanner_folder_var.set(path.replace("\\", "/"))

    def _scanner_update_progress(self, current, total, phase):
        self._update_progress(self.scanner_progress, self.scanner_progress_label,
            current, total, phase, getattr(self, '_scanner_start_time', None))

    def _scanner_scan_finished(self):
        self.scanner_scanning = False
        def _reset():
            self.scanner_scan_btn.config(state="normal")
            self.scanner_stop_btn.config(state="disabled")
            self.scanner_prog_frame.pack_forget()
            if self.scanner_scan_results:
                self.scanner_add_btn.config(state="normal")
                self.scanner_add_all_btn.config(state="normal")
                self.scanner_dl_btn.config(state="normal")
                self.scanner_del_qbit_btn.config(state="normal")
                self.scanner_del_data_chk.config(state="normal")
                self.scanner_del_os_btn.config(state="normal")
            for col in self.scanner_tree['columns']:
                self.scanner_tree.heading(col, command=lambda c=col: self.sort_tree(self.scanner_tree, c, False))
        self.root.after(0, _reset)

    def scanner_stop_scan(self):
        self.scanner_stop_event.set()
        self.scanner_log("Stopping scan...")
        self.scanner_stop_btn.config(state="disabled")

    def _scanner_on_double_click(self, event):
        item = self.scanner_tree.identify('item', event.x, event.y)
        if not item:
            return
        vals = self.scanner_tree.item(item, "values")
        if vals:
            tid = vals[0]
            webbrowser.open(f"https://rutracker.org/forum/viewtopic.php?t={tid}")

    # --- Scan ---

    def scanner_start_scan(self):
        if self.scanner_scanning:
            return

        folder_path = self.scanner_folder_var.get().strip()
        if not folder_path or not os.path.isdir(folder_path):
            messagebox.showwarning("Folder Scanner", "Please select a valid folder path.")
            return

        sel = self.scanner_client_selector.current()
        if sel < 0 or sel >= len(self.config["clients"]):
            self.scanner_log("No client selected.")
            return

        # Warnings for time-consuming scan modes
        is_deep_plus = self.scanner_deep_scan_plus_var.get()
        is_deep = self.scanner_deep_scan_var.get()
        
        if is_deep_plus:
            msg = ("Deep Scan+ (Verify Hashes) explicitly enabled.\n\n"
                   "This operation reconstructs torrent piece payloads across "
                   "every matched folder and executes heavy SHA-1 cryptographic "
                   "hashing of the actual file data on disk.\n\n"
                   "WARNING: Depending on disk speed and size, this could take "
                   "hours! CPU utilization will also spike.\n\n"
                   "Do you want to proceed?")
            if not messagebox.askyesno("Deep Scan+ Warning", msg):
                return
        elif is_deep:
            msg = ("Deep Scan (Verify Files) is enabled.\n\n"
                   "This will download .torrent metadata and verify the exact "
                   "byte size of every file inside the folder.\n\n"
                   "Do you want to proceed?")
            if not messagebox.askyesno("Deep Scan Warning", msg):
                return

        self.scanner_scanning = True
        self.scanner_scan_results = []
        self.scanner_stop_event.clear()

        # Clear treeview
        for item in self.scanner_tree.get_children():
            self.scanner_tree.delete(item)

        # UI state
        self.scanner_scan_btn.config(state="disabled")
        self.scanner_stop_btn.config(state="normal")
        self.scanner_add_btn.config(state="disabled")
        self.scanner_add_all_btn.config(state="disabled")
        self.scanner_dl_btn.config(state="disabled")
        self.scanner_summary_label.config(text="Scanning...", fg="black")

        # Show progress
        self.scanner_prog_frame.pack(fill="x", padx=10, after=self.scanner_tab.winfo_children()[0])
        self.scanner_progress['value'] = 0
        self.scanner_progress_label.config(text="Scanning folders...")

        # Disable sort while scanning
        for col in self.scanner_tree['columns']:
            self.scanner_tree.heading(col, command=lambda: None)

        self.scanner_selected_client = self.config["clients"][sel]
        threading.Thread(target=self._scanner_scan_thread, args=(folder_path,), daemon=True).start()

    @staticmethod
    def _get_folder_size(path):
        """Fast recursive folder size using os.scandir (avoids os.walk overhead)."""
        total = 0
        try:
            with os.scandir(path) as it:
                for entry in it:
                    try:
                        if entry.is_file(follow_symlinks=False):
                            total += entry.stat(follow_symlinks=False).st_size
                        elif entry.is_dir(follow_symlinks=False):
                            total += QBitAdderApp._get_folder_size(entry.path)
                    except (OSError, PermissionError):
                        pass
        except (OSError, PermissionError):
            pass
        return total

    def _scanner_scan_thread(self, root_folder):
        self._scanner_start_time = time.time()
        scan_start = self._scanner_start_time
        client = self.scanner_selected_client

        STATUS_MAP = {
            0: "* Unchecked", 1: "Closed", 2: "Approved", 3: "? Need edit", 4: "Not formatted", 
            5: "Duplicate", 6: "Closed (CP)", 7: "Consumed", 8: "# Doubtful", 
            9: "Checking", 10: "T Temporary", 11: "Premod",
        }

        try:
            # === Phase 1: Walk folder tree (single pass — find ID folders + measure sizes) ===
            self.scanner_log(f"Scanning folder: {root_folder}")
            found_folders = []
            recursive = self.scanner_recursive_var.get()
            skip_subid = self.scanner_skip_subid_var.get()

            processed_dirs = 0
            total_disk_size = 0
            phase1_start = time.time()

            if recursive:
                self.scanner_log("Phase 1/5: Scanning directories & measuring folder sizes...")
                self.root.after(0, lambda: self.scanner_progress.config(mode='indeterminate'))
                self.root.after(0, lambda: self.scanner_progress.start(15))

                for dirpath, dirnames, filenames in os.walk(root_folder):
                    if self.scanner_stop_event.is_set():
                        break

                    processed_dirs += 1
                    basename = os.path.basename(dirpath)

                    if basename.isdigit():
                        folder_size = self._get_folder_size(dirpath)
                        total_disk_size += folder_size
                        found_folders.append({
                            "topic_id": basename,
                            "disk_path": dirpath.replace("\\", "/"),
                            "disk_size": folder_size,
                        })
                        elapsed = self._format_elapsed(time.time() - phase1_start)
                        self.root.after(0, lambda n=processed_dirs, f=len(found_folders), c=basename,
                                        sz=format_size(total_disk_size), el=elapsed:
                            self.scanner_progress_label.config(
                                text=f"Phase 1: {n} dirs | {f} found ({sz}) | Measuring /{c}  [{el}]"))
                        if skip_subid:
                            dirnames.clear()
                    elif processed_dirs % 20 == 0:
                        elapsed = self._format_elapsed(time.time() - phase1_start)
                        self.root.after(0, lambda n=processed_dirs, f=len(found_folders),
                                        c=basename[:30], el=elapsed:
                            self.scanner_progress_label.config(
                                text=f"Phase 1: {n} dirs | {f} found | Scanning: {c}  [{el}]"))

                self.root.after(0, lambda: self.scanner_progress.stop())
                self.root.after(0, lambda: self.scanner_progress.config(mode='determinate'))
            else:
                try:
                    entries = [e for e in os.listdir(root_folder)]
                    total_entries = len(entries)
                    self.scanner_log(f"Phase 1/5: Scanning {total_entries} entries & measuring folder sizes...")
                    for i, entry in enumerate(entries):
                        if self.scanner_stop_event.is_set():
                            break
                        full = os.path.join(root_folder, entry)
                        if os.path.isdir(full):
                            processed_dirs += 1
                            if entry.isdigit():
                                folder_size = self._get_folder_size(full)
                                total_disk_size += folder_size
                                found_folders.append({
                                    "topic_id": entry,
                                    "disk_path": full.replace("\\", "/"),
                                    "disk_size": folder_size,
                                })
                                elapsed = self._format_elapsed(time.time() - phase1_start)
                                self.root.after(0, lambda n=processed_dirs, t=total_entries, f=len(found_folders),
                                                c=entry, sz=format_size(total_disk_size), el=elapsed:
                                    self.scanner_progress_label.config(
                                        text=f"Phase 1: {n}/{t} | {f} found ({sz}) | Measuring /{c}  [{el}]"))
                            elif processed_dirs % 20 == 0:
                                elapsed = self._format_elapsed(time.time() - phase1_start)
                                self.root.after(0, lambda n=processed_dirs, t=total_entries,
                                                f=len(found_folders), el=elapsed:
                                    self.scanner_progress_label.config(
                                        text=f"Phase 1: {n}/{t} | {f} found | Scanning...  [{el}]"))
                except Exception as e:
                    self.scanner_log(f"Error listing folder: {e}")

            if self.scanner_stop_event.is_set():
                self.scanner_log("Scan stopped by user.")
                self._scanner_scan_finished()
                return

            phase1_elapsed = self._format_elapsed(time.time() - phase1_start)
            self.scanner_log(f"Phase 1 done: {processed_dirs} dirs, {len(found_folders)} ID folders, "
                             f"total disk size {format_size(total_disk_size)}  [{phase1_elapsed}]")

            if not found_folders:
                self.root.after(0, lambda: self.scanner_summary_label.config(
                    text="No numeric folders found.", fg="gray"))
                self._scanner_scan_finished()
                return

            # === Phase 2: Batch API - get_tor_topic_data + peer_stats ===
            topic_ids = list(set(f["topic_id"] for f in found_folders))
            total_batches = (len(topic_ids) + 99) // 100
            self.scanner_log(f"Phase 2/5: Fetching API data for {len(topic_ids)} topics ({total_batches} batches)...")
            topic_data = {}
            peer_stats = {}
            phase2_start = time.time()

            for batch_start in range(0, len(topic_ids), 100):
                if self.scanner_stop_event.is_set():
                    break
                batch = topic_ids[batch_start:batch_start + 100]
                batch_num = batch_start // 100 + 1
                done_count = batch_start + len(batch)
                elapsed = self._format_elapsed(time.time() - phase2_start)
                self._update_progress(self.scanner_progress, self.scanner_progress_label,
                    done_count, len(topic_ids),
                    f"Phase 2: API batch {batch_num}/{total_batches} ({done_count}/{len(topic_ids)} topics)",
                    phase2_start)
                try:
                    api_resp = requests.get(
                        "https://api.rutracker.cc/v1/get_tor_topic_data",
                        params={"by": "topic_id", "val": ",".join(batch)},
                        timeout=15)
                    if api_resp.status_code == 200:
                        result = api_resp.json().get("result", {})
                        topic_data.update(result)

                    stats_resp = requests.get(
                        "https://api.rutracker.cc/v1/get_peer_stats",
                        params={"by": "topic_id", "val": ",".join(batch)},
                        timeout=15)
                    if stats_resp.status_code == 200:
                        stats_res = stats_resp.json().get("result", {})
                        peer_stats.update(stats_res)
                except Exception as e:
                    self.scanner_log(f"API error (batch {batch_num}): {e}")

            if self.scanner_stop_event.is_set():
                self.scanner_log("Scan stopped by user.")
                self._scanner_scan_finished()
                return

            phase2_elapsed = self._format_elapsed(time.time() - phase2_start)
            self.scanner_log(f"Phase 2 done: got data for {len(topic_data)} topics, "
                             f"peer stats for {len(peer_stats)}  [{phase2_elapsed}]")

            # === Phase 3: Fetch qBit torrent list ===
            self.root.after(0, lambda: self.scanner_progress_label.config(
                text=f"Phase 3: Loading torrent list from qBittorrent ({client['name']})..."))
            self.scanner_log(f"Phase 3/5: Loading torrent list from qBittorrent ({client['name']})...")
            client_name = client["name"]
            cached, ts = self._cache_get(client_name)
            if cached is not None:
                all_torrents = cached
                self.scanner_log(f"Phase 3 done: using cached list ({len(all_torrents)} torrents).")
                self.root.after(0, lambda: self._show_cache_time_for_client(client_name, self.scanner_cache_label))
            else:
                self.root.after(0, lambda: self.scanner_progress.config(mode='indeterminate'))
                self.root.after(0, lambda: self.scanner_progress.start(15))
                session = self._get_qbit_session(client)
                if not session:
                    self.scanner_log("Could not connect to qBittorrent.")
                    self.root.after(0, lambda: self.scanner_progress.stop())
                    self.root.after(0, lambda: self.scanner_progress.config(mode='determinate'))
                    self._scanner_scan_finished()
                    return
                try:
                    resp = session.get(f"{client['url'].rstrip('/')}/api/v2/torrents/info", timeout=30)
                    if resp.status_code != 200:
                        self.scanner_log(f"Failed to get torrent list: HTTP {resp.status_code}")
                        self.root.after(0, lambda: self.scanner_progress.stop())
                        self.root.after(0, lambda: self.scanner_progress.config(mode='determinate'))
                        self._scanner_scan_finished()
                        return
                    all_torrents = resp.json()
                    ts = self._cache_put(client_name, all_torrents)
                    self.root.after(0, lambda: self._update_cache_labels(client_name, ts))
                    self.scanner_log(f"Phase 3 done: fetched {len(all_torrents)} torrents from qBittorrent.")
                except Exception as e:
                    self.scanner_log(f"Failed to get torrent list: {e}")
                    self.root.after(0, lambda: self.scanner_progress.stop())
                    self.root.after(0, lambda: self.scanner_progress.config(mode='determinate'))
                    self._scanner_scan_finished()
                    return
                self.root.after(0, lambda: self.scanner_progress.stop())
                self.root.after(0, lambda: self.scanner_progress.config(mode='determinate'))

            # Build hash lookup
            qbit_hashes = {}
            for t in all_torrents:
                qbit_hashes[t["hash"].lower()] = t

            # === Phase 4: Resolve categories ===
            forum_ids_needed = set()
            for tid, data in topic_data.items():
                if data and isinstance(data, dict):
                    fid = data.get("forum_id")
                    if fid:
                        forum_ids_needed.add(fid)

            self.scanner_log(f"Phase 4/5: Resolving {len(forum_ids_needed)} categories...")
            forum_to_category = {}
            for i, forum_id in enumerate(forum_ids_needed):
                if i % 10 == 0:
                    self.root.after(0, lambda n=i+1, t=len(forum_ids_needed):
                        self.scanner_progress_label.config(
                            text=f"Phase 4: Resolving categories {n}/{t}..."))
                try:
                    breadcrumb = self.cat_manager._build_breadcrumb_path(forum_id)
                    if breadcrumb:
                        forum_to_category[forum_id] = breadcrumb["category"]
                    else:
                        name = self.cat_manager.cache.get("categories", {}).get(str(forum_id))
                        if name:
                            forum_to_category[forum_id] = name
                except Exception:
                    pass
            self.scanner_log(f"Phase 4 done: resolved {len(forum_to_category)}/{len(forum_ids_needed)} categories.")

            # === Phase 4.5: Deep Scan / Deep Scan+ Verification ===
            deep_scan_plus = self.scanner_deep_scan_plus_var.get()
            deep_scan = self.scanner_deep_scan_var.get() or deep_scan_plus
            deep_scan_results = {} # topic_id -> {"extra": 0, "missing": 0, "mismatch": 0, "pieces": "0 / 0"}

            if deep_scan:
                mode_str = "Deep Scan+" if deep_scan_plus else "Deep Scan"
                ds_start = time.time()
                ds_cached = 0
                ds_downloaded = 0
                self.scanner_log(f"{mode_str}: Verifying {len(found_folders)} folders...")
                for idx, folder in enumerate(found_folders):
                    if self.scanner_stop_event.is_set():
                        break

                    tid = folder["topic_id"]
                    topic_name = topic_data.get(tid, {}).get("topic_title", tid) if topic_data.get(tid) else tid
                    if isinstance(topic_name, str) and len(topic_name) > 40:
                        topic_name = topic_name[:37] + "..."
                    self._update_progress(self.scanner_progress, self.scanner_progress_label,
                        idx + 1, len(found_folders),
                        f"{mode_str} {idx+1}/{len(found_folders)}: {topic_name}",
                        ds_start)
                    tid = folder["topic_id"]
                    disk_path = folder["disk_path"]
                    api_data = topic_data.get(tid)
                    
                    if not api_data or not isinstance(api_data, dict):
                        continue
                        
                    info_hash = api_data.get("info_hash")
                    if not info_hash:
                        continue
                        
                    # 1. Get Expected Files & Hashes
                    expected_files_dict = None
                    piece_length = 0
                    pieces_hex = ""
                    
                    if deep_scan_plus:
                        hash_cache = self.hash_db_manager.get_hash_cache(int(tid))
                        if hash_cache:
                            piece_length = hash_cache["piece_length"]
                            pieces_hex = hash_cache["pieces_hex"]
                            expected_files_dict = hash_cache["files"]
                    else:
                        expected_files_dict = self.db_manager.get_torrent_files_cache(int(tid))
                    
                    if expected_files_dict is None or (deep_scan_plus and not pieces_hex):
                        # Need to download and parse
                        try:
                            t_content = self._download_torrent_content(tid, log_func=self.scanner_log)
                            if t_content:
                                try:
                                    save_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".torrent_deep_scan")
                                    os.makedirs(save_dir, exist_ok=True)
                                    with open(os.path.join(save_dir, f"{tid}.torrent"), "wb") as f:
                                        f.write(t_content)
                                except Exception as save_err:
                                    self.scanner_log(f"Warning: Could not save .torrent to .torrent_deep_scan for {tid}: {save_err}")
                                parsed = parse_torrent_info(t_content)
                            else:
                                parsed = None
                            
                            if parsed and "files" in parsed:
                                expected_files_dict = {f["path"].replace("\\", "/"): f["size"] for f in parsed["files"]}
                                
                                # Save to caches
                                self.db_manager.save_torrent_files_cache(int(tid), expected_files_dict)
                                
                                piece_length = parsed.get("piece_length", 0)
                                pieces_hex = parsed.get("pieces_hex", "")
                                if piece_length and pieces_hex:
                                    self.hash_db_manager.save_hash_cache(int(tid), piece_length, pieces_hex, expected_files_dict)
                                    
                        except Exception as e:
                            self.scanner_log(f"{mode_str} error downloading {tid}: {e}")
                            
                    if not expected_files_dict:
                        continue
                        
                    # 2. Get Actual Files
                    actual_files_dict = {}
                    try:
                        for dirpath, _, filenames in os.walk(disk_path):
                            for f in filenames:
                                full_p = os.path.join(dirpath, f)
                                rel_p = os.path.relpath(full_p, disk_path).replace("\\", "/")
                                try:
                                    actual_files_dict[rel_p] = os.path.getsize(full_p)
                                except OSError:
                                    pass
                    except Exception as e:
                        self.scanner_log(f"{mode_str} error reading disk for {tid}: {e}")
                        continue
                        
                    # 3. Compare Standard Files (Deep Scan)
                    extra_count = 0
                    missing_count = 0
                    mismatch_count = 0
                    
                    for actual_path, actual_size in actual_files_dict.items():
                        if actual_path not in expected_files_dict:
                            extra_count += 1
                        elif expected_files_dict[actual_path] != actual_size:
                            mismatch_count += 1
                            
                    for expected_path in expected_files_dict.keys():
                        if expected_path not in actual_files_dict:
                            missing_count += 1
                            
                    pieces_str = "-"
                    
                    # 4. Hash Verification (Deep Scan+)
                    if deep_scan_plus and missing_count == 0 and mismatch_count == 0 and pieces_hex and piece_length:
                        try:
                            # Reconstruct byte array of pieces
                            pieces_bytes = bytes.fromhex(pieces_hex)
                            total_pieces = len(pieces_bytes) // 20
                            bad_pieces = 0
                            
                            ordered_paths = list(expected_files_dict.keys())
                            
                            file_idx = 0
                            current_file = None
                            
                            hasher = hashlib.sha1()
                            bytes_in_hasher = 0
                            piece_idx = 0
                            
                            # Read through the virtual payload contiguous block
                            while piece_idx < total_pieces:
                                if self.scanner_stop_event.is_set():
                                    break
                                    
                                remaining_for_piece = piece_length - bytes_in_hasher
                                
                                if current_file is None:
                                    if file_idx >= len(ordered_paths):
                                        break # Out of files
                                    fp = os.path.join(disk_path, ordered_paths[file_idx])
                                    try:
                                        current_file = open(fp, "rb")
                                    except OSError:
                                        bad_pieces += (total_pieces - piece_idx)
                                        break
                                
                                chunk = current_file.read(remaining_for_piece)
                                if chunk:
                                    hasher.update(chunk)
                                    bytes_in_hasher += len(chunk)
                                    
                                if not chunk or bytes_in_hasher == piece_length:
                                    if not chunk and current_file:
                                        current_file.close()
                                        current_file = None
                                        file_idx += 1
                                        if bytes_in_hasher < piece_length and file_idx < len(ordered_paths):
                                            continue # Piece spans boundary, read next file
                                        
                                    expected_hash = pieces_bytes[piece_idx*20 : (piece_idx+1)*20]
                                    if hasher.digest() != expected_hash:
                                        bad_pieces += 1
                                        
                                    hasher = hashlib.sha1()
                                    bytes_in_hasher = 0
                                    piece_idx += 1
                                    
                            if current_file:
                                current_file.close()
                                
                            pieces_str = f"{bad_pieces} / {total_pieces}"
                            if bad_pieces > 0:
                                mismatch_count += 1 # Overload mismatch row-tag highlighting
                        except Exception as e:
                            self.scanner_log(f"Deep Scan+ error hashing {tid}: {e}")
                            pieces_str = "Error"
                            
                    deep_scan_results[tid] = {
                        "extra": extra_count,
                        "missing": missing_count,
                        "mismatch": mismatch_count,
                        "pieces": pieces_str
                    }

            if deep_scan:
                ds_elapsed = self._format_elapsed(time.time() - ds_start)
                self.scanner_log(f"{mode_str} done: verified {len(deep_scan_results)} folders  [{ds_elapsed}]")

            # === Phase 5: Match and populate treeview ===
            self.scanner_log(f"Phase 5/5: Building results for {len(found_folders)} folders...")
            results = []
            in_qbit_count = 0
            missing_count = 0
            dead_count = 0
            size_mismatch_count = 0
            phase5_start = time.time()

            for idx, folder in enumerate(found_folders):
                if self.scanner_stop_event.is_set():
                    break
                if (idx + 1) % 10 == 0 or idx + 1 == len(found_folders):
                    self._update_progress(self.scanner_progress, self.scanner_progress_label,
                        idx + 1, len(found_folders), "Phase 5: Building results", phase5_start)

                tid = folder["topic_id"]
                disk_path = folder["disk_path"]
                api_data = topic_data.get(tid)

                disk_size = folder.get("disk_size", 0)
                entry = {
                    "topic_id": tid,
                    "disk_path": disk_path,
                    "name": "?",
                    "size": 0,
                    "size_str": "?",
                    "disk_size": disk_size,
                    "disk_size_str": format_size(disk_size) if disk_size > 0 else "0 B",
                    "seeds": "?",
                    "leech": "?",
                    "rt_status": "Not on RT",
                    "category": "?",
                    "in_qbit": "?",
                    "extra": "-",
                    "missing": "-",
                    "mismatch": "-",
                    "pieces": "-",
                    "info_hash": None,
                    "forum_id": None,
                    "tag": "dead",
                }
                
                ds_res = deep_scan_results.get(tid)
                if ds_res:
                    entry["extra"] = str(ds_res["extra"]) if ds_res["extra"] > 0 else "0"
                    entry["missing"] = str(ds_res["missing"]) if ds_res["missing"] > 0 else "0"
                    entry["mismatch"] = str(ds_res["mismatch"]) if ds_res["mismatch"] > 0 else "0"
                    
                    if ds_res["missing"] > 0 or ds_res["mismatch"] > 0:
                         entry["tag"] = "mismatch" # Use a visually distinct tag if incomplete
                
                if api_data and isinstance(api_data, dict):
                    entry["info_hash"] = api_data.get("info_hash")
                    entry["forum_id"] = api_data.get("forum_id")
                    entry["name"] = api_data.get("topic_title", "?")
                    entry["size"] = api_data.get("size", 0)
                    entry["size_str"] = format_size(int(api_data.get("size", 0)))
                    
                    p_stats = peer_stats.get(str(tid), [])
                    if p_stats:
                        entry["seeds"] = p_stats[0] if len(p_stats) > 0 else "?"
                        entry["leech"] = p_stats[1] if len(p_stats) > 1 else "?"
                    else:
                        entry["seeds"] = api_data.get("seeders", "?")
                        entry["leech"] = api_data.get("leechers", "?")

                    tor_status = api_data.get("tor_status")
                    entry["rt_status"] = STATUS_MAP.get(tor_status, f"Status {tor_status}")

                    fid = api_data.get("forum_id")
                    if fid and fid in forum_to_category:
                        entry["category"] = forum_to_category[fid]

                    h = entry["info_hash"]
                    if h and h.lower() in qbit_hashes:
                        entry["in_qbit"] = "Yes"
                        entry["tag"] = "in_client"
                        in_qbit_count += 1
                    else:
                        entry["in_qbit"] = "No"
                        if tor_status == 2:
                            entry["tag"] = "missing"
                            missing_count += 1
                        else:
                            entry["tag"] = "dead"
                            dead_count += 1
                else:
                    dead_count += 1

                # Size mismatch detection (>5% difference)
                size_tag = None
                rt_size = int(entry["size"] or 0)
                if disk_size == 0 and rt_size > 0:
                    size_tag = "size_empty"
                elif rt_size > 0 and disk_size > 0:
                    ratio = disk_size / rt_size
                    if ratio < 0.95:
                        size_tag = "size_smaller"
                        size_mismatch_count += 1
                    elif ratio > 1.05:
                        size_tag = "size_larger"
                        size_mismatch_count += 1

                results.append(entry)
                tags = [entry["tag"]]
                if size_tag:
                    tags.append(size_tag)
                self.root.after(0, lambda e=entry, t=tuple(tags): self.scanner_tree.insert("", "end",
                    values=(e["topic_id"], e["name"], e["size_str"], e["disk_size_str"],
                            e["seeds"], e["leech"],
                            e["rt_status"], e["category"], e["in_qbit"],
                            e["extra"], e["missing"], e["mismatch"], e["pieces"], e["disk_path"]),
                    tags=t))

            self.scanner_scan_results = results

            total_elapsed = self._format_elapsed(time.time() - scan_start)
            summary = (f"Done: {len(found_folders)} folders | "
                       f"{in_qbit_count} in client, "
                       f"{missing_count} missing, "
                       f"{dead_count} dead")
            if size_mismatch_count > 0:
                summary += f", {size_mismatch_count} size mismatch"
            summary += f"  | Disk: {format_size(total_disk_size)}  [{total_elapsed}]"
            self.scanner_log(summary)
            self.root.after(0, lambda s=summary: self.scanner_summary_label.config(text=s, fg="black"))

        except Exception as e:
            self.scanner_log(f"Scan error: {e}")

        self._scanner_scan_finished()

    # --- Actions ---

    def _scanner_add_selected(self):
        selected = self.scanner_tree.selection()
        if not selected:
            messagebox.showinfo("Folder Scanner", "No rows selected.")
            return

        to_add = []
        for item in selected:
            vals = self.scanner_tree.item(item, "values")
            # Tuple: (topic_id, name, size_str, disk_size_str, seeds, leech, rt_status, category, in_qbit, extra, missing, mismatch, pieces, disk_path)
            # in_qbit is at vals[8]. disk_path at vals[13], category at vals[7]
            if vals[8] == "No":
                to_add.append({"item_id": item, "topic_id": vals[0],
                               "category": vals[7], "disk_path": vals[13]})

        if not to_add:
            messagebox.showinfo("Folder Scanner", "No missing torrents in selection.")
            return

        self.scanner_add_btn.config(state="disabled")
        self.scanner_add_all_btn.config(state="disabled")
        self.scanner_del_qbit_btn.config(state="disabled")
        self.scanner_del_data_chk.config(state="disabled")
        self.scanner_del_os_btn.config(state="disabled")
        self.scanner_prog_frame.pack(fill="x", padx=10, after=self.scanner_tab.winfo_children()[0])
        self.scanner_progress['value'] = 0
        self.scanner_progress_label.config(text="Starting add...")
        threading.Thread(target=self._scanner_add_thread, args=(to_add,), daemon=True).start()

    def _scanner_add_all_missing(self):
        to_add = []
        for item in self.scanner_tree.get_children():
            vals = self.scanner_tree.item(item, "values")
            # vals[8] == "in_qbit", vals[6] == "rt_status"
            if vals[8] == "No" and vals[6] == "Approved":
                to_add.append({"item_id": item, "topic_id": vals[0],
                               "category": vals[7], "disk_path": vals[13]})

        if not to_add:
            messagebox.showinfo("Folder Scanner", "No missing alive torrents to add.")
            return

        if not messagebox.askyesno("Confirm", f"Add {len(to_add)} missing torrents to qBit?"):
            return

        self.scanner_add_btn.config(state="disabled")
        self.scanner_add_all_btn.config(state="disabled")
        self.scanner_del_qbit_btn.config(state="disabled")
        self.scanner_del_data_chk.config(state="disabled")
        self.scanner_del_os_btn.config(state="disabled")
        self.scanner_prog_frame.pack(fill="x", padx=10, after=self.scanner_tab.winfo_children()[0])
        self.scanner_progress['value'] = 0
        self.scanner_progress_label.config(text="Starting add...")
        threading.Thread(target=self._scanner_add_thread, args=(to_add,), daemon=True).start()

    def _scanner_add_thread(self, to_add):
        add_start = time.time()
        client = self.scanner_selected_client
        session = self._get_qbit_session(client)
        if not session:
            self.scanner_log("Could not connect to qBittorrent.")
            self.root.after(0, self.scanner_prog_frame.pack_forget)
            self.root.after(0, lambda: self.scanner_add_btn.config(state="normal"))
            self.root.after(0, lambda: self.scanner_add_all_btn.config(state="normal"))
            self.root.after(0, lambda: self.scanner_del_qbit_btn.config(state="normal"))
            self.root.after(0, lambda: self.scanner_del_data_chk.config(state="normal"))
            self.root.after(0, lambda: self.scanner_del_os_btn.config(state="normal"))
            return

        url = client["url"].rstrip("/")
        use_parent = self.scanner_use_parent_var.get()
        paused = self.scanner_start_paused_var.get()
        
        use_custom = self.scanner_use_custom_var.get()
        custom_path = self.scanner_custom_path_entry.get().strip()
        create_cat = self.scanner_create_cat_var.get()
        create_id = self.scanner_create_id_var.get()

        added = 0
        failed = 0

        self.scanner_log(f"Adding {len(to_add)} torrents to qBit...")

        for i, entry in enumerate(to_add):
            if self.scanner_stop_event.is_set():
                self.scanner_log("Stopped by user.")
                break

            tid = entry["topic_id"]
            disk_path = entry["disk_path"].replace("\\", "/").rstrip("/")
            category = entry["category"] if entry["category"] != "?" else ""

            # Download .torrent
            t_content = self._download_torrent_content(tid, log_func=self.scanner_log)
            if not t_content:
                failed += 1
                self.root.after(0, lambda item=entry["item_id"]:
                    self.scanner_tree.set(item, "in_qbit", "Error"))
                continue

            # Determine save_path
            if use_custom:
                base_save_path = custom_path.replace("\\", "/") if custom_path else disk_path
            elif use_parent:
                base_save_path = os.path.dirname(disk_path).replace("\\", "/")
            else:
                base_save_path = disk_path
                
            path_parts = [base_save_path]
            
            if create_cat and category:
                path_parts.append(category)
                
            if create_id and tid:
                path_parts.append(str(tid))
                
            save_path = os.path.join(*path_parts).replace("\\", "/")

            files = {'torrents': (f'{tid}.torrent', t_content, 'application/x-bittorrent')}
            data = {
                'savepath': save_path,
                'paused': 'true' if paused else 'false',
                'tags': 'FolderScan',
            }
            if category:
                data['category'] = category

            try:
                # Create category if needed (409 = exists, OK)
                if category:
                    try:
                        session.post(f"{url}/api/v2/torrents/createCategory",
                            data={"category": category}, timeout=10)
                    except Exception:
                        pass

                resp = session.post(f"{url}/api/v2/torrents/add", files=files, data=data, timeout=30)
                if resp.status_code == 200 and resp.text != "Fails.":
                    self.scanner_log(f"  [{i+1}/{len(to_add)}] Added: {tid} -> {save_path}")
                    self.root.after(0, lambda item=entry["item_id"]:
                        self.scanner_tree.set(item, "in_qbit", "Added"))
                    added += 1
                else:
                    self.scanner_log(f"  [{i+1}/{len(to_add)}] Failed: {tid} ({resp.text[:50]})")
                    failed += 1
            except Exception as e:
                self.scanner_log(f"  [{i+1}/{len(to_add)}] Error: {tid}: {e}")
                failed += 1

            self._update_progress(self.scanner_progress, self.scanner_progress_label,
                i + 1, len(to_add), "Adding torrents", add_start)

        if added > 0:
            self._cache_invalidate(client["name"])

        self.scanner_log(f"Done: {added} added, {failed} failed.")
        self.root.after(0, self.scanner_prog_frame.pack_forget)
        self.root.after(0, lambda: self.scanner_add_btn.config(state="normal"))
        self.root.after(0, lambda: self.scanner_add_all_btn.config(state="normal"))
        self.root.after(0, lambda: self.scanner_del_qbit_btn.config(state="normal"))
        self.root.after(0, lambda: self.scanner_del_data_chk.config(state="normal"))
        self.root.after(0, lambda: self.scanner_del_os_btn.config(state="normal"))

    def _scanner_delete_from_qbit(self):
        selected = self.scanner_tree.selection()
        if not selected:
            messagebox.showinfo("Folder Scanner", "No rows selected.")
            return

        to_delete = []
        for item in selected:
            vals = self.scanner_tree.item(item, "values")
            if getattr(self, "scanner_scan_results", None):
                if vals[8] == "Yes":
                    to_delete.append(vals[0])

        if not to_delete:
            messagebox.showinfo("Folder Scanner", "None of the selected torrents are in qBittorrent.")
            return

        hashes = []
        for res in self.scanner_scan_results:
            if str(res["topic_id"]) in [str(t) for t in to_delete] and res.get("info_hash"):
                hashes.append(res["info_hash"])

        if not hashes:
            messagebox.showinfo("Folder Scanner", "No valid info hashes found for deletion.")
            return

        is_delete_data = self.scanner_del_data_var.get()
        msg_extra = " AND DELETE downloaded files" if is_delete_data else " (keeping files on disk)"
        if not messagebox.askyesno("Confirm", f"Remove {len(hashes)} torrents from qBittorrent{msg_extra}?"):
            return
            
        delete_files_str = "true" if is_delete_data else "false"

        client_idx = self.scanner_client_selector.current() if hasattr(self, 'scanner_client_selector') else 0
        if client_idx < 0:
            return
        client = self.config["clients"][client_idx]
        session = self._get_qbit_session(client)
        if not session:
            self.scanner_log("Could not connect to qBittorrent for deletion.")
            return

        hashes_str = "|".join(hashes)
        resp = session.post(f"{client['url'].rstrip('/')}/api/v2/torrents/delete", data={"hashes": hashes_str, "deleteFiles": delete_files_str})
        
        if resp.status_code == 200:
            self.scanner_log(f"Successfully removed {len(hashes)} torrents from qBittorrent" + (" and deleted files." if is_delete_data else "."))
            for item in selected:
                vals = list(self.scanner_tree.item(item, "values"))
                if vals[8] == "Yes" or vals[8] == "Deleted":
                    self.scanner_tree.set(item, "in_qbit", "Deleted")
        else:
            self.scanner_log(f"Failed to delete torrents: HTTP {resp.status_code}")

    def _scanner_delete_os_data(self):
        selected = self.scanner_tree.selection()
        if not selected:
            messagebox.showinfo("Folder Scanner", "No rows selected.")
            return

        to_delete = []
        for item in selected:
            vals = self.scanner_tree.item(item, "values")
            # disk_path is at vals[13]
            disk_path = vals[13]
            if disk_path and os.path.exists(disk_path):
                to_delete.append((item, disk_path, vals[0]))

        if not to_delete:
            messagebox.showinfo("Folder Scanner", "None of the selected folders exist on the drive.")
            return

        if not messagebox.askyesno("Confirm OS Deletion", f"Permanently delete {len(to_delete)} physical folders from your drive?\n\nWARNING: This will wipe the OS data even if they are NOT in qBittorrent. This action cannot be undone!"):
            return

        success = 0
        failed = 0
        import shutil
        for item, path, tid in to_delete:
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
                success += 1
                self.scanner_log(f"Deleted OS folder for topic {tid}: {path}")
                # Visually update the tree to reflect empty size
                self.root.after(0, lambda i=item: self.scanner_tree.set(i, "disk_size", "0 B"))
                # Note: tag change requires getting old tags ignoring sizes, but for simplicity we just set it to size_empty
                self.root.after(0, lambda i=item: self.scanner_tree.item(i, tags=("size_empty",)))
            except Exception as e:
                self.scanner_log(f"Failed to delete {path}: {e}")
                failed += 1

        self.scanner_log(f"OS Deletion complete: {success} deleted, {failed} failed.")

    def _scanner_download_torrent(self):
        selected = self.scanner_tree.selection()
        if not selected:
            messagebox.showinfo("Folder Scanner", "No rows selected.")
            return

        folder = filedialog.askdirectory(title="Save .torrent files to...")
        if not folder:
            return

        count = 0
        for item in selected:
            vals = self.scanner_tree.item(item, "values")
            tid = vals[0]
            t_content = self._download_torrent_content(tid, log_func=self.scanner_log)
            if t_content:
                fpath = os.path.join(folder, f"{tid}.torrent")
                with open(fpath, 'wb') as f:
                    f.write(t_content)
                count += 1

        self.scanner_log(f"Downloaded {count} .torrent files to {folder}")
        messagebox.showinfo("Download Complete", f"Saved {count} .torrent files to:\n{folder}")

    # ── Keeper periodic re-verification ──────────────────────────────

    def _schedule_keeper_recheck(self):
        self._keeper_check_interval = 1440 * 60 * 1000  # 24h in ms
        self.root.after(self._keeper_check_interval, self._recheck_keeper_status)

    def _recheck_keeper_status(self):
        threading.Thread(target=self._do_keeper_recheck, daemon=True).start()

    def _do_keeper_recheck(self):
        try:
            resp = requests.get(KeeperAuthDialog.KEEPERS_API_URL,
                                proxies=self.get_requests_proxies(), timeout=30)
            data = resp.json().get("result", {})
            self.db_manager.save_keepers_users(data)
            nicknames = {v[0].lower() for v in data.values() if v}
            db_nicknames = self.db_manager.get_all_keeper_usernames()

            nick_lower = self.keeper_nickname.lower()
            if nick_lower not in nicknames or nick_lower not in db_nicknames:
                self.root.after(0, self._lock_application)
            else:
                self.root.after(0, self._schedule_keeper_recheck)
        except Exception:
            self.root.after(0, self._schedule_keeper_recheck)

    def _lock_application(self):
        KeeperAuthDialog._nuke_data_db()
        messagebox.showerror(t("auth.locked_title"), t("auth.locked_msg"))
        self.root.destroy()

    # ── Export / Import Full Setup ────────────────────────────────────

    _BACKUP_FILES = {
        "keepers_orchestrator_config.json": CONFIG_FILE,
        "keepers_orchestrator_categories.json": CATEGORY_CACHE_FILE,
        "keepers_orchestrator_data.db": DATA_DB_FILE,
        "keepers_orchestrator_hashes.db": HASHES_DB_FILE,
    }

    def export_full_setup(self):
        dest = filedialog.asksaveasfilename(
            title=t("settings.export_setup"),
            defaultextension=".zip",
            filetypes=[("ZIP Archive", "*.zip")],
            initialfile=f"keepers_orchestrator_backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
        )
        if not dest:
            return
        try:
            # Save current config to disk first so export is up-to-date
            self.save_config()
            with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as zf:
                for arc_name, full_path in self._BACKUP_FILES.items():
                    if os.path.exists(full_path):
                        zf.write(full_path, arc_name)
            messagebox.showinfo(t("settings.export_setup"),
                                f"✓ Exported to:\n{dest}")
        except Exception as e:
            messagebox.showerror(t("settings.export_setup"), str(e))

    _IMPORT_MAP = {
        "keepers_orchestrator_config.json": CONFIG_FILE,
        "keepers_orchestrator_categories.json": CATEGORY_CACHE_FILE,
        "keepers_orchestrator_data.db": DATA_DB_FILE,
        "keepers_orchestrator_hashes.db": HASHES_DB_FILE,
    }

    def import_full_setup(self):
        src = filedialog.askopenfilename(
            title=t("settings.import_setup"),
            filetypes=[("ZIP Archive", "*.zip")],
        )
        if not src:
            return
        try:
            with zipfile.ZipFile(src, "r") as zf:
                names = zf.namelist()
                if "keepers_orchestrator_config.json" not in names:
                    messagebox.showerror(t("settings.import_setup"),
                                         "ZIP must contain keepers_orchestrator_config.json")
                    return
                for arc_name in names:
                    dest = self._IMPORT_MAP.get(arc_name)
                    if dest:
                        with zf.open(arc_name) as src_f, open(dest, "wb") as dst_f:
                            dst_f.write(src_f.read())
            messagebox.showinfo(t("settings.import_setup"),
                                "✓ Import complete.\nPlease restart the application.")
        except Exception as e:
            messagebox.showerror(t("settings.import_setup"), str(e))


if __name__ == "__main__":
    root = tk.Tk()
    root.withdraw()  # Hide main window during auth

    # Load config early (for proxy & language)
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            _boot_config = json.load(f)
        # Uncloak passwords
        for key in ("proxy",):
            if key in _boot_config:
                for field in ("password",):
                    if field in _boot_config[key]:
                        _boot_config[key][field] = uncloak(_boot_config[key][field])
        # Set language
        _current_lang = _boot_config.get("language", "en")
    else:
        _boot_config = DEFAULT_CONFIG.copy()

    # Log maintenance: clean old entries, write session separator
    _LOG_RETENTION_DAYS = _boot_config.get("log_retention_days", 14)
    _cleanup_old_logs(_LOG_RETENTION_DAYS)
    _write_startup_separator()

    _saved_nickname = _boot_config.get("keeper_nickname", "")

    if _saved_nickname:
        # Already authenticated on a previous run — skip dialog
        root.deiconify()
        app = QBitAdderApp(root)
        app.keeper_nickname = _saved_nickname
        app._schedule_keeper_recheck()
        root.mainloop()
    else:
        # First run — show auth gate
        auth = KeeperAuthDialog(root, _boot_config)
        root.wait_window(auth.dialog)

        if auth.authenticated:
            root.deiconify()
            app = QBitAdderApp(root)
            app.keeper_nickname = auth.nickname
            app._schedule_keeper_recheck()
            root.mainloop()
        else:
            root.destroy()


