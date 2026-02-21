
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

# --- Copyable ScrolledText Monkey-Patch ---
# Tkinter prevents text selection when state="disabled".
# This overrides the state lock to allow users to copy Log Outputs while still preventing typing.
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
            "base_save_path": "C:/Torrents/Sport/"
        }
    ],
    "last_selected_client_index": 0,
    "torrent_cache_ttl_hours": 6,
    "pm_polling_enabled": True,
    "pm_poll_interval_sec": 300,
    "pm_toast_enabled": False
}

CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "q_adder_config.json")
CATEGORY_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rutracker_categories.json")
DATA_DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "q_adder_data.db")
HASHES_DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "q_adder_hashes.db")

# App Version & Update Info
APP_VERSION = "0.16.3"
GITHUB_REPO = "WIN365ru/qbit-adder-python"

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
                result['creation_date'] = datetime.datetime.fromtimestamp(creation_date).strftime('%Y-%m-%d %H:%M:%S')
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


class RutrackerPMScraper:
    """Scrapes Rutracker private message inbox, reads messages, and sends replies."""

    def __init__(self, session_provider, log_func):
        self.get_session = session_provider
        self.log = log_func

    def _fix_encoding(self, response):
        if response.encoding == 'ISO-8859-1':
            response.encoding = 'cp1251'

    def _is_login_page(self, html_text):
        return 'login_username' in html_text

    def fetch_inbox(self):
        """Fetch inbox message list. Returns list of dicts or None if login needed."""
        session = self.get_session()
        try:
            resp = session.get(
                "https://rutracker.org/forum/privmsg.php?folder=inbox",
                timeout=15
            )
            self._fix_encoding(resp)

            if self._is_login_page(resp.text):
                return None

            messages = []
            rows = re.split(r'<tr\b[^>]*>', resp.text)

            for row in rows:
                link_match = re.search(
                    r'privmsg\.php\?folder=inbox&(?:amp;)?mode=read&(?:amp;)?p=(\d+)',
                    row
                )
                if not link_match:
                    continue

                msg_id = link_match.group(1)

                # Subject: capture everything inside <a> including nested tags like <b>, then strip HTML
                subj_match = re.search(
                    r'<a[^>]*privmsg\.php\?folder=inbox[^>]*mode=read[^>]*>(.*?)</a>',
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
            self.log(f"PM fetch_inbox error: {e}")
            return None

    def fetch_message(self, msg_id):
        """Fetch a single message content. Returns dict or None if login needed."""
        session = self.get_session()
        try:
            resp = session.get(
                f"https://rutracker.org/forum/privmsg.php?folder=inbox&mode=read&p={msg_id}",
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

            # Form token for reply
            token_match = re.search(r'name="form_token"\s+value="([^"]+)"', resp.text)
            if not token_match:
                token_match = re.search(r'name="sid"\s+value="([^"]+)"', resp.text)
            result["form_token"] = token_match.group(1) if token_match else ""

            return result
        except Exception as e:
            self.log(f"PM fetch_message error for {msg_id}: {e}")
            return None

    def send_reply(self, msg_id, subject, body, form_token=""):
        """Send a reply to a private message. Returns True on success."""
        session = self.get_session()
        try:
            data = {
                "mode": "reply",
                "p": msg_id,
                "subject": subject.encode("windows-1251", errors="replace"),
                "message": body.encode("windows-1251", errors="replace"),
                "post": "Отправить",
            }
            if form_token:
                data["sid"] = form_token

            resp = session.post(
                "https://rutracker.org/forum/privmsg.php",
                data=data,
                timeout=30
            )
            self._fix_encoding(resp)

            if self._is_login_page(resp.text):
                self.log("Reply failed: session expired")
                return False

            if re.search(r'(?:message.*sent|сообщение.*отправлено|privmsg\.php\?folder=sentbox)', resp.text, re.IGNORECASE):
                return True

            error_match = re.search(r'class="[^"]*gen[^"]*"[^>]*>(.*?error.*?)</td>', resp.text, re.IGNORECASE | re.DOTALL)
            if error_match:
                self.log(f"Reply error: {html.unescape(re.sub(r'<[^>]+>', '', error_match.group(1))).strip()}")

            return False
        except Exception as e:
            self.log(f"PM send_reply error: {e}")
            return False


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


class QBitAdderApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Keepers Orchestrator")
        self.root.geometry("1200x870")

        # Global Menu Bar
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)
        
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Help", menu=help_menu)
        help_menu.add_command(label="Documentation & Colors", command=self._show_help)

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
        self.db_manager = DatabaseManager(DATA_DB_FILE)
        self.hash_db_manager = HashDatabaseManager(HASHES_DB_FILE)
        self.selected_files = [] # List of file paths
        self.selected_folder_path = None
        self.stop_event = threading.Event()
        self.running_event = threading.Event()
        self.running_event.set()

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

        self.adder_tab = tk.Frame(self.notebook)
        self.keepers_tab = tk.Frame(self.notebook) # New Tab
        self.updater_tab = tk.Frame(self.notebook)
        self.remover_tab = tk.Frame(self.notebook)
        self.repair_tab = tk.Frame(self.notebook)
        self.mover_tab = tk.Frame(self.notebook)
        self.scanner_tab = tk.Frame(self.notebook)
        self.bitrot_tab = tk.Frame(self.notebook)
        self.settings_tab = tk.Frame(self.notebook)

        self.notebook.add(self.adder_tab, text="Add Torrents")
        self.notebook.add(self.keepers_tab, text="Keepers")
        self.notebook.add(self.updater_tab, text="Update Torrents")
        self.notebook.add(self.remover_tab, text="Remove Torrents")
        self.notebook.add(self.repair_tab, text="Repair Categories")
        self.notebook.add(self.mover_tab, text="Move Torrents")
        self.notebook.add(self.scanner_tab, text="Folder Scanner")
        self.notebook.add(self.bitrot_tab, text="Bitrot Scanner")
        self.notebook.add(self.settings_tab, text="Settings")

        # --- Torrent List Cache (per client) ---
        # Structure: {client_name: {"torrents": [...], "timestamp": float}}
        self.torrent_cache = {}
        self._cache_load_from_disk()

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
        self._pm_window = None

        self.create_keepers_ui()



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
        self.notebook.add(self.search_tab, text="Search Torrents")
        self.create_search_ui()
        
        # Temp directory for downloads
        self.temp_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp_torrents")
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir)

        self.is_initializing = False
        self.trigger_status_check()

        # Start PM polling
        self._pm_start_polling()

        # Auto-scan when switching to Update tab
        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)

        status_frame = tk.Frame(self.root)
        status_frame.pack(side="bottom", fill="x")

        self.status_bar = tk.Label(status_frame, text="Ready", bd=1, relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.pack(side="left", fill="x", expand=True)

        self.pm_indicator = tk.Label(
            status_frame, text="  PM  ", bd=1, relief=tk.RAISED,
            cursor="hand2", bg="#e0e0e0", fg="gray", padx=4
        )
        self.pm_indicator.pack(side="right", padx=2)
        self.pm_indicator.bind("<Button-1>", lambda e: self._pm_open_inbox_dialog())

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
                    client_name = self.config["clients"][i].get("name", "Unnamed")
                    c_status = self.client_statuses[i] if i < len(self.client_statuses) else "gray"
                    
                    self.client_listbox.insert(tk.END, f"● {client_name}")
                    if c_status == "green": self.client_listbox.itemconfig(i, {'fg': '#00b300'}) # darker green for visibility
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
        """Move keys from rutracker_categories.json to q_adder_config.json if found."""
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
        """Windows toast notification for new PMs."""
        if not self._pm_toast_available:
            return
        try:
            from winotify import Notification

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

            toast = Notification(
                app_id="Keepers Orchestrator",
                title=title,
                msg=body,
                duration="short"
            )
            toast.show()
        except ImportError:
            self._pm_toast_available = False
            self.log("Toast notifications unavailable (install winotify: pip install winotify)")
        except Exception as e:
            self.log(f"Toast notification error: {e}")

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
        win.title("Private Messages - Inbox")
        win.geometry("850x600")
        win.transient(self.root)
        self._pm_window = win

        # Top bar
        top_bar = tk.Frame(win)
        top_bar.pack(fill="x", padx=10, pady=5)

        tk.Label(top_bar, text="Inbox", font=("Segoe UI", 12, "bold")).pack(side="left")
        self._pm_status_label = tk.Label(top_bar, text="", fg="gray")
        self._pm_status_label.pack(side="left", padx=15)
        tk.Button(top_bar, text="Refresh", command=self._pm_refresh_inbox).pack(side="right")

        # Message list
        list_frame = tk.Frame(win)
        list_frame.pack(fill="both", expand=True, padx=10, pady=(0, 5))

        cols = ("msg_id", "subject", "sender", "date", "status")
        self._pm_tree = ttk.Treeview(list_frame, columns=cols, show="headings",
                                      selectmode="browse", height=8)

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
        tk.Button(btn_frame, text="Reply", command=self._pm_open_reply_dialog).pack(side="left")
        tk.Button(btn_frame, text="Open in Browser",
                  command=self._pm_open_in_browser).pack(side="left", padx=10)

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

        self._pm_preview_subject.config(text="Loading...")
        self._pm_preview_meta.config(text=f"From: {inbox_meta['sender']}  |  Date: {inbox_meta['date']}")
        self._pm_preview_body.config(state="normal")
        self._pm_preview_body.delete("1.0", tk.END)
        self._pm_preview_body.config(state="disabled")
        threading.Thread(target=self._pm_load_message, args=(msg_id, inbox_meta), daemon=True).start()

    def _pm_load_message(self, msg_id, inbox_meta):
        """Background: fetch single message body. Uses inbox_meta for subject/sender/date."""
        try:
            msg = self.pm_scraper.fetch_message(msg_id)
            if msg is None:
                user, pwd = self._get_rutracker_creds()
                if user and pwd and self.cat_manager.login(user, pwd):
                    msg = self.pm_scraper.fetch_message(msg_id)

            if msg:
                # Use reliable metadata from inbox list, only body + form_token from the read page
                msg["subject"] = inbox_meta["subject"]
                msg["sender"] = inbox_meta["sender"]
                msg["date"] = inbox_meta["date"]
                self._pm_current_message = msg
                def _update():
                    try:
                        self._pm_preview_subject.config(text=msg["subject"])
                        self._pm_preview_meta.config(
                            text=f"From: {msg['sender']}  |  Date: {msg['date']}")
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
        """Refresh inbox message list."""
        self._pm_status_label.config(text="Loading...", fg="blue")
        threading.Thread(target=self._pm_refresh_inbox_thread, daemon=True).start()

    def _pm_refresh_inbox_thread(self):
        """Background: refresh inbox list."""
        try:
            if 'bb_session' not in self.cat_manager.session.cookies.get_dict():
                user, pwd = self._get_rutracker_creds()
                if user and pwd:
                    self.cat_manager.login(user, pwd)

            messages = self.pm_scraper.fetch_inbox()
            if messages is None:
                user, pwd = self._get_rutracker_creds()
                if user and pwd and self.cat_manager.login(user, pwd):
                    messages = self.pm_scraper.fetch_inbox()

            if messages is None:
                self.root.after(0, lambda: self._pm_status_label.config(
                    text="Login failed", fg="red"))
                return

            # Load keeper usernames for highlighting
            keeper_names = self.db_manager.get_all_keeper_usernames()

            def _update():
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
        body_text = scrolledtext.ScrolledText(reply_win, wrap="word", height=15, font=("Segoe UI", 10))
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
                        msg["msg_id"], subject, body, msg.get("form_token", ""))
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

    def _pm_open_in_browser(self):
        """Open current message in browser."""
        if self._pm_current_message:
            msg_id = self._pm_current_message["msg_id"]
            webbrowser.open(f"https://rutracker.org/forum/privmsg.php?folder=inbox&mode=read&p={msg_id}")

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
        if hasattr(self, 'log_area'):
            self.log_area.config(state="normal")
            self.log_area.insert(tk.END, message + "\n")
            self.log_area.see(tk.END)
            self.log_area.config(state="disabled")
        else:
            print(message)

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
                    unit = {'B':1, 'KB':1024, 'MB':1024**2, 'GB':1024**3, 'TB':1024**4}
                    return val * unit.get(unit, 1)
    
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
        """Displays a Help dialog with feature descriptions, hotkeys, and color legends."""
        help_win = tk.Toplevel(self.root)
        help_win.title("Help & Documentation")
        help_win.geometry("900x700")

        txt = scrolledtext.ScrolledText(help_win, wrap="word", state="normal", font=("Segoe UI", 10))
        txt.pack(fill="both", expand=True, padx=10, pady=10)

        help_content = """# === QBIT ADDER - HELP & DOCUMENTATION ===

## 📑 APP TABS & FUNCTIONS
--------------------------------------------------
[Add Torrents]
- Takes a Rutracker Topic ID or a Folder Name, searches Rutracker, downloads the active .torrent file, and injects it into qBittorrent pointing to your specified directory.
- Deep Checkbox searches deeper matches natively.

[Keepers]
- Fetches all user profiles cached in your 'q_adder_data.db' Keepers list to instantly scan for their latest torrents. 
- Useful for automating downloads from favorite uploaders.

[Update Torrents]
- Compares torrents currently in qBittorrent to the live Rutracker API to find newly updated matching topics. Automatically downloads the updated file and points it to the exact same disk location.

[Remove Torrents]
- Cleans up inactive torrents by comparing qBit against Rutracker API. Flags Dead / Unknown topics for easy deletion. 

[Repair Categories]
- Rescans all Torrents in qBittorrent against the Rutracker Database. Corrects empty or inaccurate Categories and renames invalid Tracker names.

[Move Torrents]
- Mass-moves actively seeding Torrents between physical drives without manually entering qBittorrent's GUI. Can also migrate Torrents using dynamic folder naming conventions like /{Category}/{Topic_ID}/.

[Folder Scanner]
- Scans a physical Windows folder on your drive and maps every sub-directory against the Rutracker API.
- Use this to automatically detect unseeded collections, identify missing downloads, and re-inject disconnected folders back into qBittorrent smoothly.

[Bitrot Scanner]
- Scans ALL payload files across every active Torrent in qBittorrent and subjects them to cryptographic SHA-1 Verification. Perfect for discovering silent data corruption or corrupt hard drives.


## ⌨️ USEFUL COMMANDS & HOTKEYS
--------------------------------------------------
<Control-1> through <Control-0>
- Instantly switch between the 10 application tabs without using the mouse. (1=Adder, 0=Search)

<F5>
- The Universal Action Key! Instantly start the primary operation of whatever tab you are looking at:
    • Adder: Process Torrent
    • Search: Search Rutracker
    • Folder/Bitrot/Keepers: Start Scan
    • Remover/Mover: Refresh Client List 

<Control-C>
- The Universal Copy! Applies to EVERY Treeview panel across the entire app (e.g. Folder Scanner findings, Missing Searches, Move targets).
- Highlight 1 or 1,000 rows in a table, press Ctrl+C, and paste them perfectly into Excel or Notepad!

Log Highlighting
- All text consoles are unlocked! You can drag, highlight, and Copy/Paste error text from the "Log" boxes freely without breaking the readout.

Double-Click Rows
- Double-clicking an entry in the Treeview (like in the Folder Scanner) will instantly open your Default Browser directly to its Rutracker.org forum topic!

Right-Click Rows
- Right-clicking directly on any Treeview column containing an OS Directory (Save Path, Disk Path, etc.) will dynamically spawn a popup menu letting you instantly copy that folder's physical path to your clipboard cleanly!


## 🎨 TREEVIEW COLOR LEGEND (Folder Scanner)
--------------------------------------------------
⚪ Default (White)  - Standard healthy torrent that isn't connected to your client.
🟢 Dark Green       - Actively mapped / seeding in qBittorrent.
🔴 Dark Red         - "Missing" - File is missing pieces compared to API.
🔘 Gray Text        - "Dead" - This topic no longer exists on Rutracker.

📁 SIZE COMPARISON BACKGROUNDS:
Light Red (Pink)    - 0 B Empty Folder! Exists on your drive but contains no data whatsoever.
Light Orange        - Size Mismatch (Smaller). Your downloaded folder has < 95% of the data the API expects.
Light Blue          - Size Mismatch (Larger). Your downloaded folder has > 105% of the data the API expects (likely extra junk files inside).
"""
        txt.insert("1.0", help_content)
        txt.config(state="disabled")

    def load_config(self):
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r") as f:
                    data = json.load(f)
                
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

                return data
            except Exception as e:
                print(f"Error loading config: {e}")
                return DEFAULT_CONFIG
        return DEFAULT_CONFIG

    def save_config(self):
        try:
            with open(CONFIG_FILE, "w") as f:
                json.dump(self.config, f, indent=4)
            self.log("Configuration saved.")
            # messagebox.showinfo("Success", "Configuration saved successfully!") 
        except Exception as e:
            self.log(f"Error saving config: {e}")
            messagebox.showerror("Error", f"Could not save config: {e}")

    # --- Remover Tab UI (New) ---
    def create_remover_ui(self):
        # 0. State
        self.remover_all_torrents = []

        # 1. Client Selection
        client_frame = tk.LabelFrame(self.remover_tab, text="Select Client", padx=10, pady=5)
        client_frame.pack(fill="x", padx=10, pady=5)
        
        tk.Label(client_frame, text="Client:").pack(side="left")
        self.remover_client_selector = ttk.Combobox(client_frame, state="readonly", width=30)
        self.remover_client_selector.pack(side="left", padx=5)
        self.remover_client_selector.bind("<<ComboboxSelected>>", lambda e: self._remover_on_client_changed())
        
        tk.Button(client_frame, text="Refresh List", command=lambda: self.remover_load_torrents(force=True)).pack(side="left", padx=10)

        self.remover_cache_label = tk.Label(client_frame, text="List updated: never", fg="gray")
        self.remover_cache_label.pack(side="left", padx=10)

        # 2. Filter & Options
        ctrl_frame = tk.Frame(self.remover_tab)
        ctrl_frame.pack(fill="x", padx=10, pady=5)
        
        # Filter
        tk.Label(ctrl_frame, text="Filter:").pack(side="left")
        self.remover_filter_var = tk.StringVar()
        self.remover_filter_var.trace("w", lambda name, index, mode, sv=self.remover_filter_var: self.remover_apply_filter())
        entry_filter = tk.Entry(ctrl_frame, textvariable=self.remover_filter_var, width=30)
        entry_filter.pack(side="left", padx=5)

        # 3. Features
        opts_frame = tk.LabelFrame(self.remover_tab, text="Options & Actions", padx=10, pady=5)
        opts_frame.pack(fill="x", padx=10, pady=5)
        
        self.delete_files_var = tk.BooleanVar(value=False)
        tk.Checkbutton(opts_frame, text="Also delete content files (DATA)", variable=self.delete_files_var, fg="red").pack(side="left")

        tk.Button(opts_frame, text="Select from .torrent files...", command=self.remover_match_from_files).pack(side="right", padx=5)

        # 4. Torrent List
        list_frame = tk.Frame(self.remover_tab)
        list_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        cols = ("Name", "Size", "Category", "State", "Path", "Hash")
        self.remover_tree = ttk.Treeview(list_frame, columns=cols, show="headings", selectmode="extended")
        
        self.remover_tree.heading("Name", text="Name", command=lambda: self.sort_tree(self.remover_tree, "Name", False))
        self.remover_tree.heading("Size", text="Size", command=lambda: self.sort_tree(self.remover_tree, "Size", False))
        self.remover_tree.heading("Category", text="Category", command=lambda: self.sort_tree(self.remover_tree, "Category", False))
        self.remover_tree.heading("State", text="State", command=lambda: self.sort_tree(self.remover_tree, "State", False))
        self.remover_tree.heading("Path", text="Saved Path", command=lambda: self.sort_tree(self.remover_tree, "Path", False))
        self.remover_tree.heading("Hash", text="Hash")
        
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
        tk.Button(self.remover_tab, text="Remove Selected Torrents", bg="#ffcccc", command=self.remover_delete_selected).pack(pady=10)
        
        self.update_remover_client_dropdown()

    def update_remover_client_dropdown(self):
        if hasattr(self, 'remover_client_selector'):
            # Only single client selection makes sense here
            options = [c["name"] for c in self.config["clients"]]
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
        migration_frame = tk.LabelFrame(self.settings_scrollable_frame, text="Migration / Import Data", padx=10, pady=5)
        migration_frame.pack(fill="x", padx=10, pady=5)
        tk.Button(migration_frame, text="Import from webtlo config.ini", command=self.import_webtlo_config).pack(anchor="w", pady=5)

        # 0. Proxy Settings Section
        proxy_frame = tk.LabelFrame(self.settings_scrollable_frame, text="Proxy Settings (HTTP/HTTPS/SOCKS5)", padx=10, pady=10)
        proxy_frame.pack(fill="x", padx=10, pady=5)
        
        # Traffic Light for Proxy
        self.canvas_proxy_status = tk.Canvas(proxy_frame, width=20, height=20, highlightthickness=0)
        self.canvas_proxy_status.pack(side="right", padx=10)
        self.oval_proxy_status = self.canvas_proxy_status.create_oval(2, 2, 18, 18, fill=self.status_data["proxy"], outline="gray")
        
        self.proxy_enabled_var = tk.BooleanVar(value=self.config.get("proxy", {}).get("enabled", False))
        tk.Checkbutton(proxy_frame, text="Enable Proxy (useful for bypassing regional blocks like Rutracker)", 
                      variable=self.proxy_enabled_var, command=self.save_proxy_settings).pack(anchor="w", padx=5)

        p_auth_frame = tk.Frame(proxy_frame)
        p_auth_frame.pack(fill="x", padx=20, pady=2)

        tk.Label(p_auth_frame, text="Proxy URL:").pack(side="left")
        self.entry_proxy_url = tk.Entry(p_auth_frame, width=30)
        self.entry_proxy_url.pack(side="left", padx=5)
        self.entry_proxy_url.insert(0, self.config.get("proxy", {}).get("url", "socks5://127.0.0.1:10808"))
        
        tk.Label(p_auth_frame, text="Username:").pack(side="left", padx=(15, 0))
        self.entry_proxy_user = tk.Entry(p_auth_frame, width=15)
        self.entry_proxy_user.pack(side="left", padx=5)
        self.entry_proxy_user.insert(0, self.config.get("proxy", {}).get("username", ""))

        tk.Label(p_auth_frame, text="Password:").pack(side="left", padx=(5, 0))
        self.entry_proxy_pass = tk.Entry(p_auth_frame, width=15, show="*")
        self.entry_proxy_pass.pack(side="left", padx=5)
        self.entry_proxy_pass.insert(0, self.config.get("proxy", {}).get("password", ""))
        
        tk.Button(proxy_frame, text="Save Proxy Settings", command=lambda: [self.save_proxy_settings(), self.trigger_status_check()]).pack(pady=5)

        # 1. Global Auth Section
        global_frame = tk.LabelFrame(self.settings_scrollable_frame, text="Global Authentication", padx=10, pady=10)
        self.global_auth_var = tk.BooleanVar(value=self.config["global_auth"]["enabled"])
        tk.Checkbutton(global_frame, text="Use Global Authentication for All Clients", 
                      variable=self.global_auth_var, command=self.toggle_global_auth).pack(anchor="w", padx=5)

        auth_frame = tk.Frame(global_frame)
        auth_frame.pack(fill="x", padx=20, pady=2)

        tk.Label(auth_frame, text="Global User:").pack(side="left")
        self.entry_global_user = tk.Entry(auth_frame, width=15)
        self.entry_global_user.pack(side="left", padx=5)
        self.entry_global_user.insert(0, self.config["global_auth"]["username"])

        tk.Label(auth_frame, text="Global Pass:").pack(side="left")
        self.entry_global_pass = tk.Entry(auth_frame, show="*", width=15)
        self.entry_global_pass.pack(side="left", padx=5)
        self.entry_global_pass.insert(0, self.config["global_auth"]["password"])
        
        tk.Button(global_frame, text="Save Global Settings", command=self.save_global_settings).pack(pady=5)

        # 2. Clients List Section
        clients_frame = tk.LabelFrame(self.settings_scrollable_frame, text="qBittorrent Clients", padx=10, pady=10)
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

        tk.Label(details_frame, text="Name:").grid(row=0, column=0, sticky="w")
        self.entry_name = tk.Entry(details_frame, width=30)
        self.entry_name.grid(row=0, column=1, pady=2)
        self.entry_name.bind("<FocusOut>", lambda e: self.save_current_client())

        tk.Label(details_frame, text="URL:").grid(row=1, column=0, sticky="w")
        self.entry_url = tk.Entry(details_frame, width=30)
        self.entry_url.grid(row=1, column=1, pady=2)
        self.entry_url.bind("<FocusOut>", lambda e: self.save_current_client())

        tk.Label(details_frame, text="Base Path:").grid(row=2, column=0, sticky="w")
        self.entry_path = tk.Entry(details_frame, width=30)
        self.entry_path.grid(row=2, column=1, pady=2)
        self.entry_path.bind("<FocusOut>", lambda e: self.save_current_client())

        tk.Label(details_frame, text="Username:").grid(row=4, column=0, sticky="w")
        self.entry_user = tk.Entry(details_frame, width=30)
        self.entry_user.grid(row=4, column=1, pady=2)
        self.entry_user.bind("<FocusOut>", lambda e: self.save_current_client())

        tk.Label(details_frame, text="Password:").grid(row=5, column=0, sticky="w")
        self.entry_pass = tk.Entry(details_frame, width=30, show="*")
        self.entry_pass.grid(row=5, column=1, pady=2)
        self.entry_pass.bind("<FocusOut>", lambda e: self.save_current_client())

        self.client_use_global_auth_var = tk.BooleanVar()
        tk.Checkbutton(details_frame, text="Use Global Auth", variable=self.client_use_global_auth_var, command=self.on_global_auth_check_toggle).grid(row=6, column=0, columnspan=2, sticky="w")

        tk.Button(details_frame, text="Save Client Details", command=lambda: [self.save_current_client(), self.trigger_status_check()]).grid(row=7, column=1, sticky="e", pady=10)


        # 3. Rutracker Auth Section
        rt_frame = tk.LabelFrame(self.settings_scrollable_frame, text="Rutracker Forum Login (for downloading .torrents and failover category fetching)", padx=10, pady=10)
        rt_frame.pack(fill="x", padx=10, pady=5)
        
        # Traffic light for Rutracker
        self.canvas_rt_status = tk.Canvas(rt_frame, width=20, height=20, highlightthickness=0)
        self.canvas_rt_status.pack(side="right", anchor="ne", padx=5)
        self.oval_rt_status = self.canvas_rt_status.create_oval(2, 2, 18, 18, fill=self.status_data["rutracker"], outline="gray")

        rt_auth_frame = tk.Frame(rt_frame)
        rt_auth_frame.pack(fill="x", padx=5, pady=2)

        tk.Label(rt_auth_frame, text="Username:").pack(side="left")
        self.entry_rt_user = tk.Entry(rt_auth_frame, width=20)
        self.entry_rt_user.pack(side="left", padx=5)
        self.entry_rt_user.insert(0, self.config.get("rutracker_auth", {}).get("username", ""))

        tk.Label(rt_auth_frame, text="Password:").pack(side="left")
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
        
        tk.Button(keys_frame, text="Update Keys", command=self.update_keys_action).pack(side="left", padx=10)

        rt_ttl_frame = tk.Frame(rt_frame)
        rt_ttl_frame.pack(fill="x", padx=5, pady=2)

        tk.Label(rt_ttl_frame, text="Category cache TTL (hours):").pack(side="left")
        self.entry_cat_ttl = tk.Entry(rt_ttl_frame, width=5)
        self.entry_cat_ttl.pack(side="left", padx=5)
        self.entry_cat_ttl.insert(0, str(self.config.get("category_ttl_hours", 24)))

        tk.Label(rt_ttl_frame, text="(how long to reuse loaded torrent lists before fetching fresh data)", fg="gray").pack(side="left", padx=5)

        cache_btn_frame = tk.Frame(rt_frame) # Changed from cache_frame to rt_frame
        cache_btn_frame.pack(fill="x", padx=5, pady=2)

        tk.Button(cache_btn_frame, text="Save Cache Settings", command=self._save_torrent_cache_settings).pack(side="left")
        tk.Button(cache_btn_frame, text="Clear All Cached Lists", command=self._clear_torrent_cache).pack(side="left", padx=10)

        # PM Inbox Settings
        pm_settings_frame = tk.LabelFrame(rt_frame, text="Private Messages", padx=5, pady=5)
        pm_settings_frame.pack(fill="x", padx=5, pady=5)

        pm_row1 = tk.Frame(pm_settings_frame)
        pm_row1.pack(fill="x")

        self.pm_enabled_var = tk.BooleanVar(value=self.config.get("pm_polling_enabled", True))
        tk.Checkbutton(pm_row1, text="Enable PM inbox polling",
                      variable=self.pm_enabled_var).pack(side="left")

        tk.Label(pm_row1, text="Interval (sec):").pack(side="left", padx=(15, 0))
        self.pm_interval_entry = tk.Entry(pm_row1, width=6)
        self.pm_interval_entry.pack(side="left", padx=5)
        self.pm_interval_entry.insert(0, str(self.config.get("pm_poll_interval_sec", 300)))

        self.pm_toast_var = tk.BooleanVar(value=self.config.get("pm_toast_enabled", False))
        tk.Checkbutton(pm_row1, text="Windows notifications",
                      variable=self.pm_toast_var,
                      command=self._pm_on_toast_toggle).pack(side="left", padx=(15, 0))

        tk.Button(pm_row1, text="Save PM Settings", command=self._save_pm_settings).pack(side="left", padx=10)

        # 4. Data Sources Section
        data_frame = tk.LabelFrame(self.settings_scrollable_frame, text="Data Sources")
        data_frame.pack(fill="x", padx=10, pady=5)

        top_row = tk.Frame(data_frame)
        top_row.pack(fill="x", padx=5, pady=5)

        self.refresh_cats_btn = tk.Button(top_row, text="Refresh Rutracker Categories", command=self.refresh_categories)
        self.refresh_cats_btn.pack(side="left")

        self.cats_status_label = tk.Label(top_row, text=self.get_cats_status_text())
        self.cats_status_label.pack(side="left", padx=10)

        self.cats_progress = ttk.Progressbar(data_frame, mode='determinate', length=300, style="green.Horizontal.TProgressbar")
        self.cats_progress_label = tk.Label(data_frame, text="", fg="#333333", font=("Segoe UI", 9))

        # Version & Update
        v_frame = tk.Frame(self.settings_scrollable_frame)
        v_frame.pack(side="bottom", anchor="se", padx=10, pady=5)
        
        self.version_label = tk.Label(v_frame, text=f"version {APP_VERSION}", fg="gray")
        self.version_label.pack(side="right", padx=5)
        
        # Check Updates Button
        tk.Button(v_frame, text="Check for updates", command=lambda: threading.Thread(target=self.check_github_updates, args=(False,), daemon=True).start()).pack(side="right", padx=5)
        
        # GitHub Link
        lbl_gh = tk.Label(v_frame, text="GitHub", fg="blue", cursor="hand2")
        lbl_gh.pack(side="right", padx=5)
        lbl_gh.bind("<Button-1>", lambda e: webbrowser.open(f"https://github.com/{GITHUB_REPO}"))
        
        # Check for updates on startup (threaded, silent)
        threading.Thread(target=self.check_github_updates, args=(True,), daemon=True).start()

        self.current_client_index = -1
        self.refresh_client_list()

        # 5. Statistics (Bottom Left)
        stats_frame = tk.LabelFrame(self.settings_scrollable_frame, text="Statistics")
        stats_frame.pack(side="bottom", anchor="sw", padx=10, pady=5, fill="x")

        s_grid = tk.Frame(stats_frame)
        s_grid.pack(fill="x", padx=5, pady=2)

        # Row 1
        self.stats_label_count = tk.Label(s_grid, text="Torrents Kept: 0", font=("", 9))
        self.stats_label_count.grid(row=0, column=0, sticky="w", padx=10)
        
        self.stats_label_size = tk.Label(s_grid, text="Total Size Saved: 0 B", font=("", 9))
        self.stats_label_size.grid(row=0, column=1, sticky="w", padx=10)

        self.stats_label_active = tk.Label(s_grid, text="Active Seeding Size: 0 B", font=("", 9))
        self.stats_label_active.grid(row=0, column=2, sticky="w", padx=10)
        
        # Row 2
        self.stats_label_global_net = tk.Label(s_grid, text="Global UL: 0 B/s | DL: 0 B/s", font=("", 9))
        self.stats_label_global_net.grid(row=1, column=0, sticky="w", padx=10)
        
        self.stats_label_global_client = tk.Label(s_grid, text="Total Torrents: 0 (0 B)", font=("", 9))
        self.stats_label_global_client.grid(row=1, column=1, sticky="w", padx=10)
        
        self.stats_label_bitrot = tk.Label(s_grid, text="Torrents Checked for Bitrot: 0", font=("", 9))
        self.stats_label_bitrot.grid(row=1, column=2, sticky="w", padx=10)
        
        # Row 3
        self.stats_label_mover = tk.Label(s_grid, text="Torrents Auto-Balanced: 0", font=("", 9))
        self.stats_label_mover.grid(row=2, column=0, sticky="w", padx=10)
        
        ref_btn = tk.Button(s_grid, text="Refresh", command=self.refresh_statistics, height=1)
        ref_btn.grid(row=2, column=2, sticky="e", padx=5)

        # Refresh stats on load
        self.root.after(1000, self.refresh_statistics)

    def check_github_updates(self, silent=True):
        """Check GitHub for new releases. silent=False for manual check feedback."""
        try:
            session = requests.Session()
            proxies = self.get_requests_proxies()
            if proxies:
                session.proxies.update(proxies)

            url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
            resp = session.get(url, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                tag = data.get("tag_name", "").strip().lower()
                html_url = data.get("html_url", "")
                
                def parse_v(v_str):
                    return [int(x) for x in re.sub(r'[^0-9.]', '', v_str).split('.') if x.isdigit()]

                curr_v = parse_v(APP_VERSION)
                new_v = parse_v(tag)
                
                if new_v > curr_v:
                    # found update
                    def _update_ui():
                        self.version_label.pack_forget()
                        # specific ID or just pack?
                        # If we repack, we mess order. 
                        # Better to just config the label or replace it IN PLACE?
                        # Or just popup if manual?
                        
                        # If manual, popup
                        if not silent:
                            if messagebox.askyesno("Update Available", f"New version {tag} is available!\n\nOpen GitHub release page?"):
                                webbrowser.open(html_url)
                        
                        # Always show button in UI if update found
                        # We can change the version label text/color
                        self.version_label.config(text=f"Update available: {tag}", fg="red", cursor="hand2")
                        self.version_label.bind("<Button-1>", lambda e: webbrowser.open(html_url))
                        
                    self.root.after(0, _update_ui)
                else:
                    if not silent:
                        self.root.after(0, lambda: messagebox.showinfo("No Updates", f"You are using the latest version ({APP_VERSION})."))
            else:
                if not silent:
                    self.root.after(0, lambda: messagebox.showerror("Error", f"Failed to check updates: HTTP {resp.status_code}"))

        except Exception as e:
            if not silent:
                 self.root.after(0, lambda: messagebox.showerror("Error", f"Failed to check updates: {e}"))
            
    def update_client_dropdown(self):
        if hasattr(self, 'client_selector'):
            options = [c["name"] for c in self.config["clients"]] + ["All Clients"]
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
        self.refresh_cats_btn.config(state="normal", text="Refresh Rutracker Categories")
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
                getattr(self, lbl_name).config(text="List updated: never", fg="gray")
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
            self.client_listbox.insert(tk.END, f"● {c.get('name', 'Unnamed')}")
            if c_status == "green": self.client_listbox.itemconfig(i, {'fg': '#00b300'})
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
            "base_save_path": "C:/Downloads/"
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


    # --- Adder Tab UI ---
    def create_adder_ui(self):
        # Target Selection
        target_frame = tk.LabelFrame(self.adder_tab, text="Target", padx=10, pady=5)
        target_frame.pack(fill="x", padx=10, pady=5)
        
        tk.Label(target_frame, text="Select Client:").pack(side="left")
        self.client_selector = ttk.Combobox(target_frame, state="readonly", width=30)
        self.client_selector.pack(side="left", padx=5)
        self.update_client_dropdown()
        
        # File/Folder Selection
        sel_frame = tk.LabelFrame(self.adder_tab, text="Selection", padx=10, pady=10)
        sel_frame.pack(fill="x", padx=10, pady=5)
        
        self.file_label = tk.Label(sel_frame, text="No file/folder selected", fg="gray", wraplength=500, justify="left")
        self.file_label.pack(pady=5)
        
        self.link_label = tk.Label(sel_frame, text="", fg="blue", cursor="hand2", wraplength=500, justify="left")
        self.link_label.pack(pady=(0, 5))
        self.link_label.bind("<Button-1>", self._open_link)
        self._current_link = None
        
        btn_frame = tk.Frame(sel_frame)
        btn_frame.pack(pady=5)
        tk.Button(btn_frame, text="Select Torrent/ZIP File", command=self.select_file).pack(side="left", padx=5)
        tk.Button(btn_frame, text="Select Folder", command=self.select_folder).pack(side="left", padx=5)
        self.info_btn = tk.Button(btn_frame, text="Additional Info", command=self.show_additional_info, state="disabled")
        self.info_btn.pack(side="left", padx=5)
        self._current_torrent_info = None

        # Custom Options
        custom_frame = tk.LabelFrame(self.adder_tab, text="Custom Options", padx=10, pady=5)
        custom_frame.pack(fill="x", padx=10, pady=5)

        # Folder Structure Options
        fs_frame = tk.LabelFrame(custom_frame, text="Folder Structure", padx=5, pady=5)
        fs_frame.pack(fill="x", padx=5, pady=5)

        self.add_create_cat_var = tk.BooleanVar(value=True)
        tk.Checkbutton(fs_frame, text="Create Category Subfolder", variable=self.add_create_cat_var).pack(anchor="w")
        
        self.add_create_id_var = tk.BooleanVar(value=True)
        tk.Checkbutton(fs_frame, text="Create ID Subfolder", variable=self.add_create_id_var).pack(anchor="w")

        # Custom Path/Cat/Tags
        opts_frame = tk.Frame(custom_frame)
        opts_frame.pack(fill="x", padx=5, pady=5)

        self.use_custom_var = tk.BooleanVar(value=False)
        tk.Checkbutton(opts_frame, text="Override Category & Path", variable=self.use_custom_var, command=self.toggle_custom_options).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 5))

        tk.Label(opts_frame, text="Category:").grid(row=1, column=0, sticky="w")
        self.custom_cat_entry = tk.Entry(opts_frame, width=30)
        self.custom_cat_entry.grid(row=1, column=1, padx=5, pady=2)

        tk.Label(opts_frame, text="Save Path:").grid(row=2, column=0, sticky="w")
        self.custom_path_entry = tk.Entry(opts_frame, width=30)
        self.custom_path_entry.grid(row=2, column=1, padx=5, pady=2)
        
        self.browse_custom_path_btn = tk.Button(opts_frame, text="Browse...", command=self.browse_custom_path, width=10)
        self.browse_custom_path_btn.grid(row=2, column=2, padx=5)

        # Tags
        tk.Label(opts_frame, text="Tags:").grid(row=3, column=0, sticky="w", pady=(5,0))
        self.add_custom_tags_entry = tk.Entry(opts_frame, width=30)
        self.add_custom_tags_entry.grid(row=3, column=1, padx=5, pady=(5,0))
        tk.Label(opts_frame, text="(comma separated)", fg="gray").grid(row=3, column=2, sticky="w", pady=(5,0))

        self.toggle_custom_options() # Initialize state

        # Actions
        action_frame = tk.Frame(self.adder_tab)
        action_frame.pack(fill="x", padx=10, pady=5)
        
        self.add_btn = tk.Button(action_frame, text="Add to qBittorrent", command=self.process_torrent, bg="#dddddd", height=2)
        self.add_btn.pack(side="left", padx=5, fill="x", expand=True)

        self.pause_btn = tk.Button(action_frame, text="Pause", command=self.toggle_pause, state="disabled", height=2)
        self.pause_btn.pack(side="left", padx=5)
        
        self.stop_btn = tk.Button(action_frame, text="Stop", command=self.stop_processing, state="disabled", fg="red", height=2)
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
        log_frame = tk.LabelFrame(self.adder_tab, text="Log", padx=1, pady=1)
        log_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        self.log_area = scrolledtext.ScrolledText(log_frame, height=10, state="disabled")
        self.log_area.pack(fill="both", expand=True)

    def update_client_dropdown(self):
        names = [c["name"] for c in self.config["clients"]]
        names.append("All Clients")
        self.client_selector['values'] = names
        
        # Restore selection
        idx = self.config.get("last_selected_client_index", 0)
        if idx >= len(names): idx = 0
        self.client_selector.current(idx)

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
        return dt.strftime("%H:%M:%S %d.%m.%Y")

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
        text = f"List updated: {time_str}"

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
            label_widget.config(text=f"List updated: {time_str}", fg="gray")
        else:
            label_widget.config(text="List updated: never", fg="gray")

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
        ctrl_frame = tk.LabelFrame(self.updater_tab, text="Scan Controls", padx=10, pady=5)
        ctrl_frame.pack(fill="x", padx=10, pady=5)

        tk.Label(ctrl_frame, text="Client:").pack(side="left")
        self.updater_client_selector = ttk.Combobox(ctrl_frame, state="readonly", width=25)
        self.updater_client_selector.pack(side="left", padx=5)

        self.updater_scan_btn = tk.Button(ctrl_frame, text="Scan Now", command=self.updater_start_scan)
        self.updater_scan_btn.pack(side="left", padx=5)

        self.updater_stop_btn = tk.Button(ctrl_frame, text="Stop", command=self.updater_stop_scan, state="disabled")
        self.updater_stop_btn.pack(side="left", padx=5)

        # --- Progress (hidden until scan) ---
        self.updater_prog_frame = tk.Frame(self.updater_tab)
        self.updater_progress = ttk.Progressbar(self.updater_prog_frame, mode='determinate', style="green.Horizontal.TProgressbar")
        self.updater_progress.pack(fill="x", padx=5, pady=(2, 0))
        self.updater_progress_label = tk.Label(self.updater_prog_frame, text="", fg="#333333", font=("Segoe UI", 9))
        self.updater_progress_label.pack(fill="x", padx=5, pady=(0, 2))

        # --- Results Treeview ---
        results_frame = tk.LabelFrame(self.updater_tab, text="Unregistered Torrents", padx=5, pady=5)
        results_frame.pack(fill="both", expand=True, padx=10, pady=5)

        tree_container = tk.Frame(results_frame)
        tree_container.pack(fill="both", expand=True)

        self.updater_tree = ttk.Treeview(
            tree_container,
            columns=("name", "status", "topic_id"),
            show="headings",
            selectmode="extended"
        )
        self.updater_tree.heading("name", text="Torrent Name")
        self.updater_tree.heading("status", text="Status")
        self.updater_tree.heading("topic_id", text="Topic ID")
        self.updater_tree.column("name", width=350, minwidth=150)
        self.updater_tree.column("status", width=80, anchor="center")
        self.updater_tree.column("topic_id", width=80, anchor="center")

        tree_scroll = ttk.Scrollbar(tree_container, orient="vertical", command=self.updater_tree.yview)
        tree_scroll_x = ttk.Scrollbar(tree_container, orient="horizontal", command=self.updater_tree.xview)
        self.updater_tree.configure(yscrollcommand=tree_scroll.set, xscrollcommand=tree_scroll_x.set)
        
        tree_scroll.pack(side="right", fill="y")
        tree_scroll_x.pack(side="bottom", fill="x")
        self.updater_tree.pack(side="left", fill="both", expand=True)

        # Tag colors
        self.updater_tree.tag_configure("updated", foreground="dark green")
        self.updater_tree.tag_configure("deleted", foreground="red")
        self.updater_tree.tag_configure("unknown", foreground="gray")

        self.updater_summary_label = tk.Label(results_frame, text="Switch to this tab to scan.", fg="gray")
        self.updater_summary_label.pack(anchor="w", pady=(5, 0))

        # --- Action Buttons ---
        action_frame = tk.Frame(self.updater_tab)
        action_frame.pack(fill="x", padx=10, pady=5)

        self.updater_readd_keep_btn = tk.Button(
            action_frame, text="Re-add (Keep Files)",
            command=lambda: self.updater_perform_action("readd_keep"), state="disabled")
        self.updater_readd_keep_btn.pack(side="left", padx=3, fill="x", expand=True)

        self.updater_readd_redown_btn = tk.Button(
            action_frame, text="Re-add (Re-download)",
            command=lambda: self.updater_perform_action("readd_redownload"), state="disabled")
        self.updater_readd_redown_btn.pack(side="left", padx=3, fill="x", expand=True)

        self.updater_skip_btn = tk.Button(
            action_frame, text="Skip / Delete Entry",
            command=lambda: self.updater_perform_action("skip_delete"), state="disabled", fg="red")
        self.updater_skip_btn.pack(side="left", padx=3, fill="x", expand=True)

        # --- Updater Log ---
        log_frame = tk.LabelFrame(self.updater_tab, text="Update Log", padx=1, pady=1)
        log_frame.pack(fill="both", expand=True, padx=10, pady=5)

        self.updater_log_area = scrolledtext.ScrolledText(log_frame, height=6, state="disabled")
        self.updater_log_area.pack(fill="both", expand=True)

        self.update_updater_client_dropdown()
        self.update_repair_client_dropdown()
        self.update_mover_client_dropdown()
        self.update_scanner_client_dropdown()

    def updater_log(self, message):
        """Log to the updater tab's own log area (thread-safe)."""
        def _write():
            self.updater_log_area.config(state="normal")
            self.updater_log_area.insert(tk.END, message + "\n")
            self.updater_log_area.see(tk.END)
            self.updater_log_area.config(state="disabled")
        self.root.after(0, _write)

    def _on_tab_changed(self, event):
        if self.is_initializing:
            return
        selected = self.notebook.select()
        tab_widget = self.root.nametowidget(selected)

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
            names = [c["name"] for c in self.config["clients"]]
            self.updater_client_selector['values'] = names
            idx = self.config.get("last_selected_client_index", 0)
            if idx >= len(names):
                idx = 0
            if names:
                self.updater_client_selector.current(idx)

    def _updater_set_action_buttons(self, state):
        self.updater_readd_keep_btn.config(state=state)
        # Add other buttons if needed

    # ===================================================================
    # SEARCH TAB
    # ===================================================================

    def create_search_ui(self):
        # Controls
        ctrl_frame = tk.LabelFrame(self.search_tab, text="Search", padx=10, pady=5)
        ctrl_frame.pack(fill="x", padx=10, pady=5)
        
        tk.Label(ctrl_frame, text="Query:").pack(side="left")
        self.search_entry = tk.Entry(ctrl_frame, width=40)
        self.search_entry.pack(side="left", padx=5)
        self.search_entry.bind("<Return>", lambda e: self.perform_search())
        
        self.search_entry.bind("<Return>", lambda e: self.perform_search())
        
        tk.Label(ctrl_frame, text="Type:").pack(side="left")
        self.search_type_combo = ttk.Combobox(ctrl_frame, state="readonly", width=15)
        self.search_type_combo['values'] = ["Name (Scrape)", "Topic ID (API)", "Hash (API)"]
        self.search_type_combo.current(0)
        self.search_type_combo.pack(side="left", padx=5)
        
        self.search_btn = tk.Button(ctrl_frame, text="Search", command=self.perform_search)
        self.search_btn.pack(side="left", padx=10)

        # Results
        res_frame = tk.LabelFrame(self.search_tab, text="Results", padx=5, pady=5)
        res_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        cols = ("id", "name", "size", "seeds", "leech", "category")
        self.search_tree = ttk.Treeview(res_frame, columns=cols, show="headings", selectmode="browse")
        
        self.search_tree.heading("id", text="ID")
        self.search_tree.heading("name", text="Name")
        self.search_tree.heading("size", text="Size")
        self.search_tree.heading("seeds", text="S")
        self.search_tree.heading("leech", text="L")
        self.search_tree.heading("category", text="Category")
        
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
        
        tk.Button(act_frame, text="Download", command=self.download_selected_torrent).pack(side="left", padx=5)
        tk.Button(act_frame, text="Download & Add", command=self.download_and_add_torrent).pack(side="left", padx=5)
        
        self.search_status = tk.Label(act_frame, text="", fg="gray")
        self.search_status.pack(side="left", padx=10)

    def create_bitrot_ui(self):
        # 1. Controls
        top_frame = tk.Frame(self.bitrot_tab)
        top_frame.pack(fill="x", padx=10, pady=5)
        
        tk.Label(top_frame, text="Client:").pack(side="left")
        self.bitrot_client_combo = ttk.Combobox(top_frame, state="readonly", width=15)
        self.bitrot_client_combo.pack(side="left", padx=5)
        self.bitrot_client_combo.bind("<<ComboboxSelected>>", self._bitrot_on_client_select)

        tk.Label(top_frame, text="Older than (Days):").pack(side="left", padx=(15, 0))
        self.bitrot_age_spinbox = tk.Spinbox(top_frame, from_=0, to=9999, width=5)
        self.bitrot_age_spinbox.delete(0, "end")
        self.bitrot_age_spinbox.insert(0, "30")
        self.bitrot_age_spinbox.pack(side="left", padx=5)

        self.bitrot_load_btn = tk.Button(top_frame, text="Load 100% Torrents", command=self.bitrot_load_torrents)
        self.bitrot_load_btn.pack(side="left", padx=15)

        self.bitrot_scan_btn = tk.Button(top_frame, text="Start Bitrot Check (Selected)", command=self.bitrot_start_check)
        self.bitrot_scan_btn.pack(side="left", padx=5)

        self.bitrot_cancel_btn = tk.Button(top_frame, text="Stop Check", state=tk.DISABLED, command=self.bitrot_cancel_check)
        self.bitrot_cancel_btn.pack(side="left", padx=5)

        # 2. Treeview
        tree_frame = tk.Frame(self.bitrot_tab)
        tree_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        cols = ("topic_id", "name", "size", "added_on", "last_active", "up_speed", "seed", "path", "last_checked", "status", "progress", "bitrot_state")
        self.bitrot_tree = ttk.Treeview(tree_frame, columns=cols, show="headings", selectmode="extended")
        
        self.bitrot_tree.heading("topic_id", text="Topic ID")
        self.bitrot_tree.heading("name", text="Name", command=lambda: self.sort_tree(self.bitrot_tree, "name", False))
        self.bitrot_tree.heading("size", text="Size", command=lambda: self.sort_tree(self.bitrot_tree, "size", False))
        self.bitrot_tree.heading("added_on", text="Added", command=lambda: self.sort_tree(self.bitrot_tree, "added_on", False))
        self.bitrot_tree.heading("last_active", text="Last Active", command=lambda: self.sort_tree(self.bitrot_tree, "last_active", False))
        self.bitrot_tree.heading("up_speed", text="UP Speed", command=lambda: self.sort_tree(self.bitrot_tree, "up_speed", False))
        self.bitrot_tree.heading("seed", text="Seeds", command=lambda: self.sort_tree(self.bitrot_tree, "seed", False))
        self.bitrot_tree.heading("path", text="Path", command=lambda: self.sort_tree(self.bitrot_tree, "path", False))
        self.bitrot_tree.heading("last_checked", text="Last Checked", command=lambda: self.sort_tree(self.bitrot_tree, "last_checked", False))
        self.bitrot_tree.heading("status", text="Status")
        self.bitrot_tree.heading("progress", text="Progress")
        self.bitrot_tree.heading("bitrot_state", text="Bitrot State", command=lambda: self.sort_tree(self.bitrot_tree, "bitrot_state", False))

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
        
        self.bitrot_progress_lbl = tk.Label(progress_frame, text="Ready", width=40, anchor="w")
        self.bitrot_progress_lbl.pack(side="left")
        
        self.bitrot_progress = ttk.Progressbar(progress_frame, orient="horizontal", mode="determinate")
        self.bitrot_progress.pack(side="left", fill="x", expand=True, padx=5)
        
        self.bitrot_stats_lbl = tk.Label(progress_frame, text="Total: 0 torrents (0 B)", fg="blue", anchor="e")
        self.bitrot_stats_lbl.pack(side="right", padx=10)
        
        # Populate client dropdown correctly on startup
        self.update_bitrot_client_dropdown()

    def bitrot_log(self, msg):
        self.bitrot_log_text.insert(tk.END, msg + "\n")
        self.bitrot_log_text.see(tk.END)
        self.root.update_idletasks()
        
    def update_bitrot_client_dropdown(self):
        if not hasattr(self, 'bitrot_client_combo'):
            return
        vals = [f"{i}: {c['name']} ({c['url']})" for i, c in enumerate(self.config["clients"])]
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
            added_on = datetime.datetime.fromtimestamp(t.get("added_on", 0)).strftime('%Y-%m-%d %H:%M')
            last_activity_ts = t.get("last_activity", 0)
            last_active = datetime.datetime.fromtimestamp(last_activity_ts).strftime('%Y-%m-%d %H:%M') if last_activity_ts > 0 else "Never"
            
            up_speed = format_size(t.get("upspeed", 0)) + "/s"
            seed = t.get("num_seeds", 0)
            path = t.get("save_path", t.get("content_path", ""))
            
            history = t.get("_bitrot_hist", {})
            last_checked_ts = history.get("last_checked", 0)
            last_checked = datetime.datetime.fromtimestamp(last_checked_ts).strftime('%Y-%m-%d %H:%M') if last_checked_ts > 0 else "Never"
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
                                        
                                        now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
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
        topic_id = entry.get("topic_id") or "N/A"
        iid = entry["hash"]
        self.updater_tree.insert("", "end", iid=iid,
            values=(entry["name"], status, topic_id))
        tag = {"Updated": "updated", "Deleted": "deleted"}.get(status, "unknown")
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
            resp = session.get(f"{url}/api/v2/torrents/info", timeout=30)
            if resp.status_code != 200:
                self.updater_log(f"Failed to get torrent list: HTTP {resp.status_code}")
                self._updater_scan_finished()
                return

            all_torrents = resp.json()
            total = len(all_torrents)
            self.updater_log(f"Found {total} torrents. Checking trackers...")

            # Check trackers for each torrent
            unregistered = []
            for i, torrent in enumerate(all_torrents):
                if self.updater_stop_event.is_set():
                    break

                self._updater_update_progress(i + 1, total, "Checking trackers")

                t_hash = torrent["hash"]
                try:
                    tr_resp = session.get(f"{url}/api/v2/torrents/trackers",
                        params={"hash": t_hash}, timeout=10)
                    if tr_resp.status_code != 200:
                        continue

                    for tracker in tr_resp.json():
                        msg = tracker.get("msg", "") or tracker.get("message", "")
                        if "not registered" in msg.lower():
                            unregistered.append({
                                "hash": t_hash,
                                "name": torrent.get("name", "Unknown"),
                                "save_path": torrent.get("save_path", torrent.get("content_path", "")),
                                "category": torrent.get("category", ""),
                            })
                            break
                except Exception:
                    pass

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
            deleted_count = 0
            unknown_count = 0

            for entry in unregistered:
                tid = entry.get("topic_id")
                if not tid:
                    entry["topic_status"] = "No Topic ID"
                    unknown_count += 1
                elif str(tid) in topic_data:
                    data = topic_data[str(tid)]
                    current_hash = data.get("info_hash", "").upper()
                    qbit_hash = entry["hash"].upper()
                    if current_hash != qbit_hash:
                        entry["topic_status"] = "Updated"
                        entry["new_hash"] = current_hash
                        updated_count += 1
                    else:
                        entry["topic_status"] = "Unknown"
                        unknown_count += 1
                else:
                    entry["topic_status"] = "Deleted"
                    deleted_count += 1

            # Phase 4: Populate treeview
            self.updater_scan_results = unregistered
            for entry in unregistered:
                self.root.after(0, lambda e=entry: self._updater_add_tree_row(e))

            summary = (f"Found {len(unregistered)} unregistered: "
                       f"{updated_count} updated, {deleted_count} deleted, "
                       f"{unknown_count} unknown")
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

        if action_type == "readd_keep":
            msg = (f"Re-add {count} torrent(s) from Rutracker?\n\n"
                   "Old entries will be removed.\n"
                   "Downloaded files will be KEPT and rechecked.")
        elif action_type == "readd_redownload":
            msg = (f"Re-add {count} torrent(s) from Rutracker?\n\n"
                   "Old entries AND files will be DELETED.\n"
                   "Torrents will re-download from scratch.")
        elif action_type == "skip_delete":
            msg = (f"Delete {count} torrent entry/entries from qBittorrent?\n\n"
                   "Downloaded files will be KEPT on disk.")
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
                if action_type == "skip_delete":
                    self.updater_log(f"Deleting entry: {t_name[:60]}")
                    resp = session.post(f"{url}/api/v2/torrents/delete",
                        data={"hashes": t_hash, "deleteFiles": "false"}, timeout=15)
                    if resp.status_code == 200:
                        success += 1
                        self.root.after(0, lambda h=t_hash: self._updater_remove_tree_row(h))
                    else:
                        fail += 1
                        self.updater_log(f"  Delete failed: HTTP {resp.status_code}")

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
        ctrl_frame = tk.LabelFrame(self.repair_tab, text="Scan Controls", padx=10, pady=5)
        ctrl_frame.pack(fill="x", padx=10, pady=5)

        tk.Label(ctrl_frame, text="Client:").pack(side="left")
        self.repair_client_selector = ttk.Combobox(ctrl_frame, state="readonly", width=25)
        self.repair_client_selector.pack(side="left", padx=5)
        self.repair_client_selector.bind("<<ComboboxSelected>>", lambda e: self._repair_on_client_changed())

        self.repair_scan_btn = tk.Button(ctrl_frame, text="Scan Now", command=self.repair_start_scan)
        self.repair_scan_btn.pack(side="left", padx=5)

        self.repair_stop_btn = tk.Button(ctrl_frame, text="Stop", command=self.repair_stop_scan, state="disabled")
        self.repair_stop_btn.pack(side="left", padx=5)

        self.repair_cache_label = tk.Label(ctrl_frame, text="List updated: never", fg="gray")
        self.repair_cache_label.pack(side="left", padx=10)

        # --- Progress (hidden until scan) ---
        self.repair_prog_frame = tk.Frame(self.repair_tab)
        self.repair_progress = ttk.Progressbar(self.repair_prog_frame, mode='determinate', style="green.Horizontal.TProgressbar")
        self.repair_progress.pack(fill="x", padx=5, pady=(2, 0))
        self.repair_progress_label = tk.Label(self.repair_prog_frame, text="", fg="#333333", font=("Segoe UI", 9))
        self.repair_progress_label.pack(fill="x", padx=5, pady=(0, 2))

        # --- Results Treeview ---
        results_frame = tk.LabelFrame(self.repair_tab, text="Category Mismatches", padx=5, pady=5)
        results_frame.pack(fill="both", expand=True, padx=10, pady=5)

        tree_container = tk.Frame(results_frame)
        tree_container.pack(fill="both", expand=True)

        self.repair_tree = ttk.Treeview(
            tree_container,
            columns=("name", "cur_cat", "correct_cat", "cur_path", "new_path"),
            show="headings",
            selectmode="extended"
        )
        self.repair_tree.heading("name", text="Torrent Name")
        self.repair_tree.heading("cur_cat", text="Current Cat")
        self.repair_tree.heading("correct_cat", text="Correct Cat")
        self.repair_tree.heading("cur_path", text="Current Path")
        self.repair_tree.heading("new_path", text="New Path")
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

        self.repair_summary_label = tk.Label(results_frame, text="Click Scan to check categories.", fg="gray")
        self.repair_summary_label.pack(anchor="w", pady=(5, 0))

        # --- Action Buttons ---
        action_frame = tk.Frame(self.repair_tab)
        action_frame.pack(fill="x", padx=10, pady=5)

        self.repair_selected_btn = tk.Button(
            action_frame, text="Repair Selected",
            command=lambda: self._repair_perform_action("selected"), state="disabled")
        self.repair_selected_btn.pack(side="left", padx=3, fill="x", expand=True)

        self.repair_all_btn = tk.Button(
            action_frame, text="Repair All",
            command=lambda: self._repair_perform_action("all"), state="disabled")
        self.repair_all_btn.pack(side="left", padx=3, fill="x", expand=True)

        # --- Repair Log ---
        log_frame = tk.LabelFrame(self.repair_tab, text="Repair Log", padx=1, pady=1)
        log_frame.pack(fill="both", expand=True, padx=10, pady=5)

        self.repair_log_area = scrolledtext.ScrolledText(log_frame, height=6, state="disabled")
        self.repair_log_area.pack(fill="both", expand=True)

        self.update_repair_client_dropdown()

    def repair_log(self, message):
        """Log to the repair tab's own log area (thread-safe)."""
        def _write():
            self.repair_log_area.config(state="normal")
            self.repair_log_area.insert(tk.END, message + "\n")
            self.repair_log_area.see(tk.END)
            self.repair_log_area.config(state="disabled")
        self.root.after(0, _write)

    def update_repair_client_dropdown(self):
        if hasattr(self, 'repair_client_selector'):
            names = [c["name"] for c in self.config["clients"]]
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

        if not cur.startswith(base):
            # Path doesn't start with base — can only fix category, not move
            return None

        remainder = cur[len(base):].strip("/")
        parts = remainder.split("/") if remainder else []

        if current_category and len(parts) >= 1 and parts[0] == current_category:
            # Replace old category segment with new one
            parts[0] = correct_category
        elif not current_category and len(parts) >= 1:
            # No category was set — insert category before existing path
            parts = [correct_category] + parts
        else:
            # Fallback: just prepend category
            parts = [correct_category] + parts

        return base + "/" + "/".join(parts)

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
        if not messagebox.askyesno("Confirm Repair",
            f"Repair {count} torrent(s)?\n\n"
            "This will update categories and move files to correct paths."):
            return

        self._repair_set_action_buttons("disabled")
        self.repair_scan_btn.config(state="disabled")
        self.repair_prog_frame.pack(fill="x", padx=10, after=self.repair_tab.winfo_children()[0])
        self.repair_progress['value'] = 0
        self.repair_progress_label.config(text="Starting repair...")
        threading.Thread(target=self._repair_action_thread, args=(hashes,), daemon=True).start()

    def _repair_action_thread(self, hashes):
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

                # Step 2: Move files if path changed
                if move and new_path != entry["current_path"]:
                    resp = session.post(f"{url}/api/v2/torrents/setLocation",
                        data={"hashes": t_hash, "location": new_path}, timeout=30)
                    if resp.status_code != 200:
                        self.repair_log(f"  Category set OK, but move failed: HTTP {resp.status_code}")
                        self.repair_log(f"  Tried moving to: {new_path}")
                        # Category was set, so partial success
                    else:
                        self.repair_log(f"  Moved: {new_path}")

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
        client_frame = tk.LabelFrame(parent, text="Client", padx=10, pady=5)
        client_frame.pack(fill="x", padx=10, pady=5)

        tk.Label(client_frame, text="Client:").pack(side="left")
        self.mover_client_selector = ttk.Combobox(client_frame, state="readonly", width=25)
        self.mover_client_selector.pack(side="left", padx=5)
        self.mover_client_selector.bind("<<ComboboxSelected>>", lambda e: self._mover_on_client_changed())

        self.mover_load_btn = tk.Button(client_frame, text="Load Torrents", command=lambda: self._mover_load_torrents(force=True))
        self.mover_load_btn.pack(side="left", padx=5)

        self.mover_load_status = tk.Label(client_frame, text="", fg="gray")
        self.mover_load_status.pack(side="left", padx=10)

        self.mover_cache_label = tk.Label(client_frame, text="List updated: never", fg="gray", anchor="w")
        self.mover_cache_label.pack(side="left", padx=5, fill="x", expand=True)

        # --- Section B: Category Mover ---
        cat_frame = tk.LabelFrame(parent, text="Move by Category", padx=10, pady=5)
        cat_frame.pack(fill="x", padx=10, pady=5)

        row1 = tk.Frame(cat_frame)
        row1.pack(fill="x", pady=2)
        tk.Label(row1, text="Category:").pack(side="left")
        self.mover_cat_selector = ttk.Combobox(row1, state="readonly")
        self.mover_cat_selector.pack(side="left", padx=5, fill="x", expand=True)
        self.mover_cat_selector.bind("<<ComboboxSelected>>", self._mover_on_cat_selected)

        row2 = tk.Frame(cat_frame)
        row2.pack(fill="x", pady=2)
        tk.Label(row2, text="New root path:").pack(side="left")
        tk.Button(row2, text="Browse...", command=self._mover_browse_path).pack(side="right")
        self.mover_new_path_var = tk.StringVar()
        self.mover_new_path_entry = tk.Entry(row2, textvariable=self.mover_new_path_var)
        self.mover_new_path_entry.pack(side="left", padx=5, fill="x", expand=True)

        row3 = tk.Frame(cat_frame)
        row3.pack(fill="x", pady=2)
        tk.Label(row3, text="Max torrents to move (0 = all):").pack(side="left")
        self.mover_cat_limit_var = tk.IntVar(value=0)
        tk.Spinbox(row3, from_=0, to=99999, textvariable=self.mover_cat_limit_var, width=8).pack(side="left", padx=5)

        # Folder structure options
        opts_label = tk.Label(cat_frame, text="Folder structure:", font=("", 9, "bold"))
        opts_label.pack(anchor="w", pady=(5, 0))

        row4a = tk.Frame(cat_frame)
        row4a.pack(fill="x", pady=1)
        tk.Label(row4a, text="Category folder:").pack(side="left")
        self.mover_cat_create_cat_var = tk.BooleanVar(value=True)
        tk.Checkbutton(row4a, text="Create /Category/ subfolder in target path",
            variable=self.mover_cat_create_cat_var).pack(side="left", padx=5)

        row4b = tk.Frame(cat_frame)
        row4b.pack(fill="x", pady=1)
        tk.Label(row4b, text="ID folder:").pack(side="left")
        self.mover_cat_id_action = tk.StringVar(value="keep")
        tk.Radiobutton(row4b, text="Keep existing /ID/ folder",
            variable=self.mover_cat_id_action, value="keep").pack(side="left", padx=5)
        tk.Radiobutton(row4b, text="Create /ID/ folder (if missing)",
            variable=self.mover_cat_id_action, value="create").pack(side="left", padx=5)
        tk.Radiobutton(row4b, text="Remove /ID/ folder (flatten)",
            variable=self.mover_cat_id_action, value="strip").pack(side="left", padx=5)

        self.mover_cat_summary = tk.Label(cat_frame, text="Select a category to see details.", fg="gray")
        self.mover_cat_summary.pack(anchor="w", pady=3)

        cat_btn_frame = tk.Frame(cat_frame)
        cat_btn_frame.pack(fill="x", pady=3)

        self.mover_cat_move_btn = tk.Button(cat_btn_frame, text="Move Category", state="disabled",
            command=self._mover_start_category_move)
        self.mover_cat_move_btn.pack(side="left")

        self.mover_cat_stop_btn = tk.Button(cat_btn_frame, text="Stop", state="disabled",
            command=self._mover_stop)
        self.mover_cat_stop_btn.pack(side="left", padx=5)

        self.mover_cat_resume_btn = tk.Button(cat_btn_frame, text="Resume", state="disabled",
            command=self._mover_resume_category_move)
        self.mover_cat_resume_btn.pack(side="left", padx=5)

        self.mover_cat_progress_label = tk.Label(cat_btn_frame, text="", fg="gray")
        self.mover_cat_progress_label.pack(side="left", padx=10)

        # --- Section C: Disk Auto-Balancer ---
        bal_frame = tk.LabelFrame(parent, text="Auto-Balance Across Disks", padx=10, pady=5)
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
        self.mover_disk_tree.heading("path", text="Disk Path")
        self.mover_disk_tree.heading("free", text="Free Space")
        self.mover_disk_tree.heading("current", text="Current Load")
        self.mover_disk_tree.heading("target", text="Target Load")
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

        tk.Button(disk_btn_frame, text="Detect Disks", command=self._mover_detect_disks).pack(fill="x", pady=1)
        tk.Button(disk_btn_frame, text="Remove", command=self._mover_remove_disk).pack(fill="x", pady=1)

        disk_add_frame = tk.Frame(bal_frame)
        disk_add_frame.pack(fill="x", pady=2)
        tk.Label(disk_add_frame, text="Add path:").pack(side="left")
        tk.Button(disk_add_frame, text="Add", command=self._mover_add_disk).pack(side="right", padx=(5, 0))
        tk.Button(disk_add_frame, text="Browse...", command=self._mover_browse_disk).pack(side="right")
        self.mover_add_disk_var = tk.StringVar()
        tk.Entry(disk_add_frame, textvariable=self.mover_add_disk_var).pack(side="left", padx=5, fill="x", expand=True)

        # Strategy
        strat_frame = tk.Frame(bal_frame)
        strat_frame.pack(fill="x", pady=5)
        tk.Label(strat_frame, text="Strategy:").pack(side="left")
        self.mover_strategy_var = tk.StringVar(value="both")
        tk.Radiobutton(strat_frame, text="Balance by Size", variable=self.mover_strategy_var, value="size").pack(side="left", padx=5)
        tk.Radiobutton(strat_frame, text="Balance by Seeded", variable=self.mover_strategy_var, value="uploaded").pack(side="left", padx=5)
        tk.Radiobutton(strat_frame, text="Both (recommended)", variable=self.mover_strategy_var, value="both").pack(side="left", padx=5)

        limit_frame = tk.Frame(bal_frame)
        limit_frame.pack(fill="x", pady=2)
        tk.Label(limit_frame, text="Max torrents to move (0 = all):").pack(side="left")
        self.mover_bal_limit_var = tk.IntVar(value=0)
        tk.Spinbox(limit_frame, from_=0, to=99999, textvariable=self.mover_bal_limit_var, width=8).pack(side="left", padx=5)

        # Folder structure options
        bal_opts_label = tk.Label(bal_frame, text="Folder structure:", font=("", 9, "bold"))
        bal_opts_label.pack(anchor="w", pady=(5, 0))

        bal_opts1 = tk.Frame(bal_frame)
        bal_opts1.pack(fill="x", pady=1)
        tk.Label(bal_opts1, text="Category folder:").pack(side="left")
        self.mover_bal_keep_cat_var = tk.BooleanVar(value=True)
        tk.Checkbutton(bal_opts1, text="Preserve /Category/ subfolder in target path",
            variable=self.mover_bal_keep_cat_var).pack(side="left", padx=5)

        bal_opts2 = tk.Frame(bal_frame)
        bal_opts2.pack(fill="x", pady=1)
        tk.Label(bal_opts2, text="ID folder:").pack(side="left")
        self.mover_bal_id_action = tk.StringVar(value="keep")
        tk.Radiobutton(bal_opts2, text="Keep existing /ID/ folder",
            variable=self.mover_bal_id_action, value="keep").pack(side="left", padx=5)
        tk.Radiobutton(bal_opts2, text="Create /ID/ folder (if missing)",
            variable=self.mover_bal_id_action, value="create").pack(side="left", padx=5)
        tk.Radiobutton(bal_opts2, text="Remove /ID/ folder (flatten)",
            variable=self.mover_bal_id_action, value="strip").pack(side="left", padx=5)

        bal_btn_frame = tk.Frame(bal_frame)
        bal_btn_frame.pack(fill="x", pady=3)
        self.mover_preview_btn = tk.Button(bal_btn_frame, text="Preview Balance", state="disabled",
            command=self._mover_preview_balance)
        self.mover_preview_btn.pack(side="left", padx=3)
        self.mover_execute_btn = tk.Button(bal_btn_frame, text="Execute Balance", state="disabled",
            command=self._mover_start_execute_balance)
        self.mover_execute_btn.pack(side="left", padx=3)
        self.mover_bal_summary = tk.Label(bal_btn_frame, text="", fg="gray")
        self.mover_bal_summary.pack(side="left", padx=10)

        # Preview treeview
        preview_frame = tk.LabelFrame(parent, text="Balance Preview", padx=5, pady=5)
        preview_frame.pack(fill="both", expand=True, padx=10, pady=5)

        prev_container = tk.Frame(preview_frame)
        prev_container.pack(fill="both", expand=True)

        self.mover_preview_tree = ttk.Treeview(
            prev_container,
            columns=("name", "size", "uploaded", "from", "to"),
            show="headings", selectmode="extended", height=8
        )
        self.mover_preview_tree.heading("name", text="Name",
            command=lambda: self.sort_tree(self.mover_preview_tree, "name", False))
        self.mover_preview_tree.heading("size", text="Size",
            command=lambda: self.sort_tree(self.mover_preview_tree, "size", False))
        self.mover_preview_tree.heading("uploaded", text="Uploaded",
            command=lambda: self.sort_tree(self.mover_preview_tree, "uploaded", False))
        self.mover_preview_tree.heading("from", text="From")
        self.mover_preview_tree.heading("to", text="To")
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
        log_frame = tk.LabelFrame(parent, text="Move Log", padx=1, pady=1)
        log_frame.pack(fill="both", expand=True, padx=10, pady=5)

        self.mover_log_area = scrolledtext.ScrolledText(log_frame, height=6, state="disabled")
        self.mover_log_area.pack(fill="both", expand=True)

        self.update_mover_client_dropdown()

    # --- Helpers ---

    def mover_log(self, message):
        def _write():
            self.mover_log_area.config(state="normal")
            self.mover_log_area.insert(tk.END, message + "\n")
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
            names = [c["name"] for c in self.config["clients"]]
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

    def create_keepers_ui(self):
        # Top controls
        top_frame = tk.Frame(self.keepers_tab)
        top_frame.pack(fill="x", padx=5, pady=5)

        tk.Label(top_frame, text="Category:").pack(side="left")
        
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

        # Client Selector
        tk.Label(top_frame, text="Client:").pack(side="left", padx=5)
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
        cat_frame = tk.Frame(self.keepers_tab)
        cat_frame.pack(fill="x", padx=5, pady=2)
        
        tk.Label(cat_frame, text="Save Category:").pack(side="left", padx=5)
        self.keepers_cat_mode = tk.StringVar(value="preserve") # preserve / custom
        
        tk.Radiobutton(cat_frame, text="Forum Category", variable=self.keepers_cat_mode, value="preserve", command=self._keepers_toggle_cat_input).pack(side="left")
        tk.Radiobutton(cat_frame, text="Custom:", variable=self.keepers_cat_mode, value="custom", command=self._keepers_toggle_cat_input).pack(side="left")
        
        self.keepers_custom_cat_entry = tk.Entry(cat_frame, width=15, state="disabled")
        self.keepers_custom_cat_entry.pack(side="left", padx=5)
        self.keepers_custom_cat_entry.insert(0, "Keepers")

        tk.Label(top_frame, text="Max Seeds:").pack(side="left", padx=5)
        self.keepers_max_seeds = tk.IntVar(value=5)
        seeds_spin = tk.Spinbox(top_frame, from_=0, to=100, textvariable=self.keepers_max_seeds, width=5)
        seeds_spin.pack(side="left")

        self.keepers_scan_btn = tk.Button(top_frame, text="Scan", command=self.keepers_start_scan, bg="#dddddd")
        self.keepers_scan_btn.pack(side="left", padx=5)

        self.keepers_stop_btn = tk.Button(top_frame, text="Stop", command=self.keepers_stop_scan, state="disabled")
        self.keepers_stop_btn.pack(side="left")

        # --- Keepers Progress (hidden until scanning) ---
        self.keepers_prog_frame = tk.Frame(self.keepers_tab)
        self.keepers_progress = ttk.Progressbar(self.keepers_prog_frame, mode='indeterminate',
            style="blue.Horizontal.TProgressbar")
        self.keepers_progress.pack(fill="x", padx=5, pady=(2, 0))
        self.keepers_progress_label = tk.Label(self.keepers_prog_frame, text="",
            fg="#333333", font=("Segoe UI", 9))
        self.keepers_progress_label.pack(fill="x", padx=5, pady=(0, 2))

        # Results Treeview
        tree_frame = tk.Frame(self.keepers_tab)
        tree_frame.pack(fill="both", expand=True, padx=5, pady=5)

        cols = ("id", "name", "size", "seeds", "leech", "status", "link", "k_count", "priority", "last_seen", "poster")
        self.keepers_tree = ttk.Treeview(tree_frame, columns=cols, show="headings")
        self.keepers_tree.heading("id", text="ID", command=lambda: self.sort_tree(self.keepers_tree, "id", False))
        self.keepers_tree.heading("name", text="Name", command=lambda: self.sort_tree(self.keepers_tree, "name", False))
        self.keepers_tree.heading("size", text="Size", command=lambda: self.sort_tree(self.keepers_tree, "size", False))
        self.keepers_tree.heading("seeds", text="Seeds", command=lambda: self.sort_tree(self.keepers_tree, "seeds", False))
        self.keepers_tree.heading("leech", text="Leech", command=lambda: self.sort_tree(self.keepers_tree, "leech", False))
        self.keepers_tree.heading("status", text="Status", command=lambda: self.sort_tree(self.keepers_tree, "status", False))
        self.keepers_tree.heading("link", text="Link", command=lambda: self.sort_tree(self.keepers_tree, "link", False))
        self.keepers_tree.heading("k_count", text="# Keepers", command=lambda: self.sort_tree(self.keepers_tree, "k_count", False))
        self.keepers_tree.heading("priority", text="Priority", command=lambda: self.sort_tree(self.keepers_tree, "priority", False))
        self.keepers_tree.heading("last_seen", text="Last Seen", command=lambda: self.sort_tree(self.keepers_tree, "last_seen", False))
        self.keepers_tree.heading("poster", text="Poster", command=lambda: self.sort_tree(self.keepers_tree, "poster", False))

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
        action_frame = tk.Frame(self.keepers_tab)
        action_frame.pack(fill="x", padx=5, pady=5)

        self.keepers_paused_var = tk.BooleanVar(value=True)
        tk.Checkbutton(action_frame, text="Start Paused", variable=self.keepers_paused_var).pack(side="left")

        self.keepers_add_btn = tk.Button(action_frame, text="Add Selected", command=self._keepers_add_selected, state="normal")
        self.keepers_add_btn.pack(side="left", padx=5)

        self.keepers_dl_btn = tk.Button(action_frame, text="Download .torrent", command=self._keepers_download_torrent)
        self.keepers_dl_btn.pack(side="left", padx=5)
        
        self.keepers_csv_btn = tk.Button(action_frame, text="Export to CSV", command=self._keepers_export_csv)
        self.keepers_csv_btn.pack(side="right", padx=5)

        # Log
        self.keepers_log_area = scrolledtext.ScrolledText(self.keepers_tab, height=6, state='disabled')
        self.keepers_log_area.pack(fill="x", padx=5, pady=5)

    def _keepers_filter_cats(self, event):
        typed = self.keepers_cat_var.get().lower()
        if not typed:
            self.keepers_cat_combo['values'] = self.keepers_all_cats
        else:
            filtered = [c for c in self.keepers_all_cats if typed in c.lower()]
            self.keepers_cat_combo['values'] = filtered

    def keepers_log(self, msg):
        def _log():
            try:
                self.keepers_log_area.config(state='normal')
                self.keepers_log_area.insert(tk.END, f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}\n")
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
            
        # Get selected client
        client_idx = self.keepers_client_combo.current()
        if client_idx < 0: client_idx = 0
        client_conf = self.config["clients"][client_idx]

        # Clear previous
        for item in self.keepers_tree.get_children():
            self.keepers_tree.delete(item)

        self.keepers_stop_event.clear()
        self.keepers_scan_active = True
        self.keepers_scan_btn.config(state="disabled")
        self.keepers_stop_btn.config(state="normal")
        self.keepers_log_area.config(state='normal')
        self.keepers_log_area.delete(1.0, tk.END)
        self.keepers_log_area.config(state='disabled')

        # Show indeterminate progress
        self.keepers_prog_frame.pack(fill="x", padx=5, after=self.keepers_tab.winfo_children()[0])
        self.keepers_progress.start(15)
        self.keepers_progress_label.config(text="Scanning...")

        t = threading.Thread(target=self._keepers_scan_thread, args=(cat_id, max_seeds, client_conf))
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


    def _keepers_scan_thread(self, cat_id, max_seeds, client_conf):
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
                            "seeds": vals[1],
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

                    # Filter by seeds AFTER enrichment
                    filtered = [c for c in candidates if c['seeds'] <= max_seeds]
                    self.keepers_log(f"  {len(filtered)} match criteria (seeds <= {max_seeds}).")

                    # 3. Add to Tree
                    for t in filtered:
                         if self.keepers_stop_event.is_set(): break
                         
                         status = "Normal"
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
        
        self.keepers_scan_active = False
        def _keepers_finish():
            self.keepers_progress.stop()
            self.keepers_prog_frame.pack_forget()
            self.keepers_scan_btn.config(state="normal")
            self.keepers_stop_btn.config(state="disabled")
        self.root.after(0, _keepers_finish)

    def _keepers_insert_tree(self, t, status):
        link = f"https://rutracker.org/forum/viewtopic.php?t={t['id']}"
        
        # Pull generated hover data if we fetched PVC payload
        k_count, priority, str_last_seen, poster = 0, "", "", ""
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
                    else:
                        priority = str(p_num)
                    
                    poster = str(d.get('topic_poster', ''))
                    
                    last_seen_ts = d.get("seeder_last_seen", 0)
                    if last_seen_ts > 0:
                        str_last_seen = datetime.datetime.fromtimestamp(last_seen_ts).strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        str_last_seen = "Never"
            except:
                pass
                
        self.keepers_tree.insert("", "end", values=(
            t['id'], t['name'], t['size_str'], t['seeds'], t['leech'], status, link,
            k_count, priority, str_last_seen, poster
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




    def refresh_statistics(self):
        count, size = self.db_manager.get_kept_stats()
        self.stats_label_count.config(text=f"Torrents Kept: {count}")
        self.stats_label_size.config(text=f"Total Size Saved: {format_size(size)}")
        
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
            self.stats_label_bitrot.config(text=f"Checked for Bitrot: {bitrot_checked} (Corrupted: {bitrot_rot})")
            
            mover_count = self.db_manager.get_mover_stats()
            self.stats_label_mover.config(text=f"Torrents Auto-Balanced: {mover_count}")
            
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
                
                self.stats_label_global_net.config(text=f"Global UL: {format_size(up_speed)}/s | DL: {format_size(dl_speed)}/s")

                # Get aggregate torrent sums
                try:
                    resp = s.get(f"{url}/api/v2/torrents/info", timeout=10)
                    if resp.status_code == 200:
                        torrents = resp.json()
                        global_count = len(torrents)
                        for t in torrents:
                            global_size += t.get('size', 0)
                            if t.get('category') == "Keepers":
                                active_size += t.get('size', 0)
                                
                        self.stats_label_active.config(text=f"Active Seeding Size: {format_size(active_size)}")
                        self.stats_label_global_client.config(text=f"Total Torrents: {global_count} ({format_size(global_size)})")
                    else:
                        self.stats_label_active.config(text=f"Active Seeding Size: (Client Error {resp.status_code})")
                except:
                    self.stats_label_active.config(text="Active Seeding Size: (Error)")
            else:
                self.stats_label_active.config(text="Active Seeding Size: (Not connected)")
                self.stats_label_global_net.config(text="Global UL: ? | DL: ? (Not connected)")
                self.stats_label_global_client.config(text="Total Torrents: 0 (Not connected)")
                
        except Exception as e:
            self.stats_label_active.config(text=f"Active Seeding Size: (Error)")
            print(f"Stats Error: {e}")


    # ===================================================================
    # FOLDER SCANNER TAB
    # ===================================================================

    def create_scanner_ui(self):
        # --- Scan Controls ---
        ctrl_frame = tk.LabelFrame(self.scanner_tab, text="Scan Controls", padx=10, pady=5)
        ctrl_frame.pack(fill="x", padx=10, pady=5)

        row1 = tk.Frame(ctrl_frame)
        row1.pack(fill="x", pady=2)
        tk.Label(row1, text="Folder:").pack(side="left")
        self.scanner_folder_var = tk.StringVar()
        tk.Entry(row1, textvariable=self.scanner_folder_var, width=60).pack(side="left", padx=5)
        tk.Button(row1, text="Browse...", command=self._scanner_browse_folder).pack(side="left")

        row2 = tk.Frame(ctrl_frame)
        row2.pack(fill="x", pady=2)
        tk.Label(row2, text="Client:").pack(side="left")
        self.scanner_client_selector = ttk.Combobox(row2, state="readonly", width=25)
        self.scanner_client_selector.pack(side="left", padx=5)
        self.scanner_client_selector.bind("<<ComboboxSelected>>", lambda e: self._scanner_on_client_changed())

        self.scanner_scan_btn = tk.Button(row2, text="Scan", command=self.scanner_start_scan)
        self.scanner_scan_btn.pack(side="left", padx=5)

        self.scanner_stop_btn = tk.Button(row2, text="Stop", command=self.scanner_stop_scan, state="disabled")
        self.scanner_stop_btn.pack(side="left", padx=5)

        self.scanner_cache_label = tk.Label(row2, text="List updated: never", fg="gray")
        self.scanner_cache_label.pack(side="left", padx=10)

        # --- Options ---
        opts_frame = tk.Frame(self.scanner_tab)
        opts_frame.pack(fill="x", padx=10, pady=2)

        self.scanner_recursive_var = tk.BooleanVar(value=True)
        tk.Checkbutton(opts_frame, text="Scan subfolders recursively",
            variable=self.scanner_recursive_var).pack(side="left", padx=5)

        self.scanner_use_parent_var = tk.BooleanVar(value=True)
        tk.Checkbutton(opts_frame, text="Use parent folder as save_path (content lives in /ID/ folder)",
            variable=self.scanner_use_parent_var).pack(side="left", padx=5)

        self.scanner_skip_subid_var = tk.BooleanVar(value=True)
        tk.Checkbutton(opts_frame, text="Skip subfolders inside /ID/",
            variable=self.scanner_skip_subid_var).pack(side="left", padx=5)

        self.scanner_start_paused_var = tk.BooleanVar(value=True)
        tk.Checkbutton(opts_frame, text="Start paused",
            variable=self.scanner_start_paused_var).pack(side="left", padx=5)

        self.scanner_deep_scan_var = tk.BooleanVar(value=False)
        tk.Checkbutton(opts_frame, text="Deep Scan (Verify Files)",
            variable=self.scanner_deep_scan_var, fg="darkblue").pack(side="left", padx=5)

        self.scanner_deep_scan_plus_var = tk.BooleanVar(value=False)
        tk.Checkbutton(opts_frame, text="Deep Scan+ (Verify Hashes)",
            variable=self.scanner_deep_scan_plus_var, fg="purple").pack(side="left", padx=5)

        # --- Custom Add Options ---
        custom_frame = tk.LabelFrame(self.scanner_tab, text="Custom Add Options", padx=10, pady=5)
        custom_frame.pack(fill="x", padx=10, pady=5)

        fs_frame = tk.LabelFrame(custom_frame, text="Folder Structure", padx=5, pady=5)
        fs_frame.pack(fill="x", padx=5, pady=5)

        self.scanner_create_cat_var = tk.BooleanVar(value=True)
        tk.Checkbutton(fs_frame, text="Create Category Subfolder", variable=self.scanner_create_cat_var).pack(anchor="w")

        self.scanner_create_id_var = tk.BooleanVar(value=True)
        tk.Checkbutton(fs_frame, text="Create ID Subfolder", variable=self.scanner_create_id_var).pack(anchor="w")

        path_frame = tk.Frame(custom_frame)
        path_frame.pack(fill="x", padx=5, pady=5)

        self.scanner_use_custom_var = tk.BooleanVar(value=False)
        tk.Checkbutton(path_frame, text="Override Save Path", variable=self.scanner_use_custom_var, command=self._scanner_toggle_custom_options).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 5))

        tk.Label(path_frame, text="Save Path:").grid(row=1, column=0, sticky="w")
        self.scanner_custom_path_entry = tk.Entry(path_frame, width=30)
        self.scanner_custom_path_entry.grid(row=1, column=1, padx=5, pady=2)

        self.scanner_browse_custom_path_btn = tk.Button(path_frame, text="Browse...", command=self._scanner_browse_custom_path, width=10)
        self.scanner_browse_custom_path_btn.grid(row=1, column=2, padx=5)

        self._scanner_toggle_custom_options()

        # --- Progress (hidden until scan) ---
        self.scanner_prog_frame = tk.Frame(self.scanner_tab)
        self.scanner_progress = ttk.Progressbar(self.scanner_prog_frame, mode='determinate', style="green.Horizontal.TProgressbar")
        self.scanner_progress.pack(fill="x", padx=5, pady=(2, 0))
        self.scanner_progress_label = tk.Label(self.scanner_prog_frame, text="", fg="#333333", font=("Segoe UI", 9))
        self.scanner_progress_label.pack(fill="x", padx=5, pady=(0, 2))

        # --- Results Treeview ---
        results_frame = tk.LabelFrame(self.scanner_tab, text="Scan Results", padx=5, pady=5)
        results_frame.pack(fill="both", expand=True, padx=10, pady=5)

        tree_container = tk.Frame(results_frame)
        tree_container.pack(fill="both", expand=True)

        cols = ("topic_id", "name", "size", "disk_size", "seeds", "leech", "status", "category", "in_qbit", "extra", "missing", "mismatch", "pieces", "disk_path")
        self.scanner_tree = ttk.Treeview(tree_container, columns=cols, show="headings", selectmode="extended")

        self.scanner_tree.heading("topic_id", text="Topic ID",
            command=lambda: self.sort_tree(self.scanner_tree, "topic_id", False))
        self.scanner_tree.heading("name", text="Name",
            command=lambda: self.sort_tree(self.scanner_tree, "name", False))
        self.scanner_tree.heading("size", text="Size",
            command=lambda: self.sort_tree(self.scanner_tree, "size", False))
        self.scanner_tree.heading("disk_size", text="Disk Size",
            command=lambda: self.sort_tree(self.scanner_tree, "disk_size", False))
        self.scanner_tree.heading("seeds", text="Seeds",
            command=lambda: self.sort_tree(self.scanner_tree, "seeds", False))
        self.scanner_tree.heading("leech", text="Leech",
            command=lambda: self.sort_tree(self.scanner_tree, "leech", False))
        self.scanner_tree.heading("status", text="RT Status",
            command=lambda: self.sort_tree(self.scanner_tree, "status", False))
        self.scanner_tree.heading("category", text="Category",
            command=lambda: self.sort_tree(self.scanner_tree, "category", False))
        self.scanner_tree.heading("in_qbit", text="In qBit",
            command=lambda: self.sort_tree(self.scanner_tree, "in_qbit", False))
        self.scanner_tree.heading("extra", text="Extra",
            command=lambda: self.sort_tree(self.scanner_tree, "extra", False))
        self.scanner_tree.heading("missing", text="Missing",
            command=lambda: self.sort_tree(self.scanner_tree, "missing", False))
        self.scanner_tree.heading("mismatch", text="Mismatch",
            command=lambda: self.sort_tree(self.scanner_tree, "mismatch", False))
        self.scanner_tree.heading("pieces", text="Pieces (Bad/Total)",
            command=lambda: self.sort_tree(self.scanner_tree, "pieces", False))
        self.scanner_tree.heading("disk_path", text="Disk Path",
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

        self.scanner_summary_label = tk.Label(results_frame, text="Enter a folder path and click Scan.", fg="gray")
        self.scanner_summary_label.pack(anchor="w", padx=5)

        # --- Action Buttons ---
        btn_frame = tk.Frame(self.scanner_tab)
        btn_frame.pack(fill="x", padx=10, pady=3)

        self.scanner_add_btn = tk.Button(btn_frame, text="Add Selected to qBit",
            command=self._scanner_add_selected, state="disabled")
        self.scanner_add_btn.pack(side="left", padx=3)

        self.scanner_add_all_btn = tk.Button(btn_frame, text="Add All Missing",
            command=self._scanner_add_all_missing, state="disabled")
        self.scanner_add_all_btn.pack(side="left", padx=3)

        self.scanner_dl_btn = tk.Button(btn_frame, text="Download .torrent",
            command=self._scanner_download_torrent, state="disabled")
        self.scanner_dl_btn.pack(side="left", padx=3)

        self.scanner_del_qbit_btn = tk.Button(btn_frame, text="Delete from qBit",
            command=self._scanner_delete_from_qbit, state="disabled")
        self.scanner_del_qbit_btn.pack(side="right", padx=3)

        self.scanner_del_data_var = tk.IntVar(value=0)
        self.scanner_del_data_chk = tk.Checkbutton(btn_frame, text="Delete Data", variable=self.scanner_del_data_var, state="disabled")
        self.scanner_del_data_chk.pack(side="right", padx=0)

        self.scanner_del_os_btn = tk.Button(btn_frame, text="Delete OS Data",
            command=self._scanner_delete_os_data, state="disabled")
        self.scanner_del_os_btn.pack(side="right", padx=3)

        # --- Log ---
        log_frame = tk.LabelFrame(self.scanner_tab, text="Log", padx=5, pady=5)
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

    def scanner_log(self, message):
        def _write():
            self.scanner_log_area.config(state="normal")
            self.scanner_log_area.insert(tk.END, f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {message}\n")
            self.scanner_log_area.see(tk.END)
            self.scanner_log_area.config(state="disabled")
        self.root.after(0, _write)

    def update_scanner_client_dropdown(self):
        if hasattr(self, 'scanner_client_selector'):
            names = [c["name"] for c in self.config["clients"]]
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


if __name__ == "__main__":
    root = tk.Tk()
    app = QBitAdderApp(root)
    root.mainloop()


