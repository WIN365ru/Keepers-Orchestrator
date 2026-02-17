
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
    "last_selected_client_index": 0 
}

CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "q_adder_config.json")
CATEGORY_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rutracker_categories.json")

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
        
        result['piece_size'] = info.get('piece length', 0)
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
                path_str = '/'.join(decoded_parts)
                files_list.append({'path': path_str, 'size': f.get('length', 0)})
        elif 'length' in info:
            # Single-file torrent — add the single file to the list
            files_list.append({'path': result['name'], 'size': info['length']})
        result['files'] = files_list
        
        return result
    except Exception as e:
        return {'name': '', 'comment': '', 'topic_id': None, 'total_size': 0, 'file_count': 0, 'created_by': '', 'creation_date': '', 'tracker': '', 'piece_size': 0, 'private': False, 'source': '', 'files': [], 'error': str(e)}

class CategoryManager:
    def __init__(self, log_func):
        self.log = log_func
        self.cache = self.load_cache()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

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
                 return True
            else:
                 self.log(f"Login failed (no session cookie). Response len: {len(resp.text)}")
                 return False
        except Exception as e:
            self.log(f"Login error: {e}")
            return False

    def refresh_cache(self, username=None, password=None, progress_callback=None):
        """Refresh the category cache via Rutracker API."""
        self.log("Refreshing Rutracker category cache via API...")

        try:
            if progress_callback:
                progress_callback(0, 1)

            resp = requests.get("https://api.rutracker.cc/v1/static/cat_forum_tree", timeout=30)
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



class QBitAdderApp:
    def __init__(self, root):
        self.root = root
        self.root.title("qBittorrent Auto-Adder")
        self.root.geometry("600x650")
        
        self.config = self.load_config()
        self.selected_file_path = None
        self.selected_folder_path = None
        self.stop_event = threading.Event()
        self.running_event = threading.Event()
        self.running_event.set()

        self.is_initializing = True

        # UI Setup
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill="both", expand=True, padx=5, pady=5)

        self.adder_tab = tk.Frame(self.notebook)
        self.updater_tab = tk.Frame(self.notebook)
        self.repair_tab = tk.Frame(self.notebook)
        self.settings_tab = tk.Frame(self.notebook)

        self.notebook.add(self.adder_tab, text="Add Torrents from file")
        self.notebook.add(self.updater_tab, text="Update Torrents")
        self.notebook.add(self.repair_tab, text="Repair Categories")
        self.notebook.add(self.settings_tab, text="Settings")

        self.create_adder_ui()

        # Initialize Category Manager (needs log from adder_ui)
        self.cat_manager = CategoryManager(self.log)

        # Updater tab state
        self.updater_scanning = False
        self.updater_scan_results = []
        self.updater_qbit_session = None
        self.updater_selected_client = None
        self.updater_stop_event = threading.Event()
        self.create_updater_ui()

        # Repair tab state
        self.repair_scanning = False
        self.repair_scan_results = []
        self.repair_selected_client = None
        self.repair_stop_event = threading.Event()
        self.create_repair_ui()

        self.create_settings_ui()

        self.is_initializing = False

        # Auto-scan when switching to Update tab
        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)

        self.status_bar = tk.Label(self.root, text="Ready", bd=1, relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.pack(side="bottom", fill="x")

        # Start Category Manager (Auto-fetch if needed)
        threading.Thread(target=self._initial_category_fetch, daemon=True).start()

    def _get_rutracker_creds(self):
        rt_auth = self.config.get("rutracker_auth", {})
        user = rt_auth.get("username", "")
        pwd = rt_auth.get("password", "")
        if user and pwd:
            return user, pwd
        return None, None

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

    # --- Settings Tab UI ---
    def create_settings_ui(self):
        # 1. Global Auth Section
        global_frame = tk.LabelFrame(self.settings_tab, text="Global Authentication", padx=10, pady=10)
        global_frame.pack(fill="x", padx=10, pady=5)

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
        clients_frame = tk.LabelFrame(self.settings_tab, text="qBittorrent Clients", padx=10, pady=10)
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

        tk.Button(details_frame, text="Save Client Details", command=self.save_current_client).grid(row=7, column=1, sticky="e", pady=10)


        # 3. Rutracker Auth Section
        rt_frame = tk.LabelFrame(self.settings_tab, text="Rutracker Forum Login (for category fetching)", padx=10, pady=10)
        rt_frame.pack(fill="x", padx=10, pady=5)

        rt_auth_frame = tk.Frame(rt_frame)
        rt_auth_frame.pack(fill="x", padx=5, pady=2)

        tk.Label(rt_auth_frame, text="Username:").pack(side="left")
        self.entry_rt_user = tk.Entry(rt_auth_frame, width=20)
        self.entry_rt_user.pack(side="left", padx=5)
        self.entry_rt_user.insert(0, self.config.get("rutracker_auth", {}).get("username", ""))

        tk.Label(rt_auth_frame, text="Password:").pack(side="left")
        self.entry_rt_pass = tk.Entry(rt_auth_frame, show="*", width=20)
        self.entry_rt_pass.pack(side="left", padx=5)
        self.entry_rt_pass.insert(0, self.config.get("rutracker_auth", {}).get("password", ""))

        rt_ttl_frame = tk.Frame(rt_frame)
        rt_ttl_frame.pack(fill="x", padx=5, pady=2)

        tk.Label(rt_ttl_frame, text="Category cache TTL (hours):").pack(side="left")
        self.entry_cat_ttl = tk.Entry(rt_ttl_frame, width=5)
        self.entry_cat_ttl.pack(side="left", padx=5)
        self.entry_cat_ttl.insert(0, str(self.config.get("category_ttl_hours", 24)))

        tk.Button(rt_frame, text="Save Rutracker Settings", command=self.save_rutracker_settings).pack(pady=5)

        # 4. Data Sources Section
        data_frame = tk.LabelFrame(self.settings_tab, text="Data Sources")
        data_frame.pack(fill="x", padx=10, pady=5)

        top_row = tk.Frame(data_frame)
        top_row.pack(fill="x", padx=5, pady=5)

        self.refresh_cats_btn = tk.Button(top_row, text="Refresh Rutracker Categories", command=self.refresh_categories)
        self.refresh_cats_btn.pack(side="left")

        self.cats_status_label = tk.Label(top_row, text=self.get_cats_status_text())
        self.cats_status_label.pack(side="left", padx=10)

        self.cats_progress = ttk.Progressbar(data_frame, mode='determinate', length=300)
        self.cats_progress_label = tk.Label(data_frame, text="", fg="gray")

        self.current_client_index = -1
        self.refresh_client_list()

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
        threading.Thread(target=self._refresh_cats_thread, daemon=True).start()

    def _refresh_progress(self, current, total):
        """Called from refresh_cache thread to update progress bar."""
        pct = int(current / total * 100) if total > 0 else 0
        self.root.after(0, lambda c=current, t=total, p=pct: self._update_progress_ui(c, t, p))

    def _update_progress_ui(self, current, total, pct):
        self.cats_progress['value'] = pct
        self.cats_progress_label.config(text=f"Crawling sub-categories... {current}/{total} ({pct}%)")

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
        messagebox.showinfo("Saved", "Global settings saved.")

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
        self.log(f"Rutracker settings saved. TTL: {self.config['category_ttl_hours']}h")

    def refresh_client_list(self):
        self.client_listbox.delete(0, tk.END)
        for c in self.config["clients"]:
            self.client_listbox.insert(tk.END, c["name"])
        
        # Select first if exists or reset
        if self.config["clients"]:
            if self.current_client_index == -1:
                self.current_client_index = 0
            self.client_listbox.select_set(self.current_client_index)
            self.on_client_select(None)
        else:
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
        
        # Refresh list name if changed
        self.client_listbox.delete(idx)
        self.client_listbox.insert(idx, self.entry_name.get())
        self.client_listbox.select_set(idx)
        
        self.save_config()
        self.update_client_dropdown()
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

        self.use_custom_var = tk.BooleanVar(value=False)
        tk.Checkbutton(custom_frame, text="Use Custom Category & Path", variable=self.use_custom_var, command=self.toggle_custom_options).pack(anchor="w")

        opts_frame = tk.Frame(custom_frame)
        opts_frame.pack(fill="x", padx=20, pady=5)

        tk.Label(opts_frame, text="Category:").grid(row=0, column=0, sticky="w")
        self.custom_cat_entry = tk.Entry(opts_frame, width=30)
        self.custom_cat_entry.grid(row=0, column=1, padx=5, pady=2)

        tk.Label(opts_frame, text="Save Path:").grid(row=1, column=0, sticky="w")
        self.custom_path_entry = tk.Entry(opts_frame, width=30)
        self.custom_path_entry.grid(row=1, column=1, padx=5, pady=2)
        
        self.browse_custom_path_btn = tk.Button(opts_frame, text="Browse...", command=self.browse_custom_path, width=10)
        self.browse_custom_path_btn.grid(row=1, column=2, padx=5)

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

    def browse_custom_path(self):
        path = filedialog.askdirectory()
        if path:
            self.custom_path_entry.delete(0, tk.END)
            self.custom_path_entry.insert(0, path)

    # --- Shared Actions (Select File/Folder) ---
    def select_file(self):
        filepath = filedialog.askopenfilename(filetypes=[("Torrent/ZIP", "*.torrent *.zip"), ("All files", "*.*")])
        if filepath:
            self.selected_file_path = filepath
            self.selected_folder_path = None
            
            filename = os.path.basename(filepath)
            
            if filepath.lower().endswith('.zip'):
                cat_name, cat_id, count = self._extract_zip_info(filename)

                info_text = f"ZIP: {filename}\nCategory: {cat_name} (ID: {cat_id})\nTorrents: {count}\nTotal size: calculating..."
                self.file_label.config(text=info_text, fg="black")
                self._current_link = None
                self.link_label.config(text="")
                self.log(f"Selected ZIP: {filepath}")
                self.log(f" -> Category: {cat_name}, Count: {count}")
                self._current_torrent_info = None
                self.info_btn.config(state="disabled")
                # Calculate total download size in background
                threading.Thread(target=self._calc_zip_size, args=(filepath, info_text), daemon=True).start()
            else:
                # Parse torrent metadata
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
                if comment:
                    self.log(f"  Link: {comment}")
                
                # Look up category in background
                if topic_id:
                    threading.Thread(target=self._lookup_torrent_category, args=(topic_id, lines), daemon=True).start()

    def select_folder(self):
        folderpath = filedialog.askdirectory()
        if folderpath:
            self.selected_folder_path = folderpath
            self.selected_file_path = None
            self._current_link = None
            self.link_label.config(text="")
            self._current_torrent_info = None
            self.info_btn.config(state="disabled")
            
            try:
                count = sum(1 for f in os.listdir(folderpath) if f.lower().endswith('.torrent'))
                self.file_label.config(text=f"Folder: {folderpath}\n({count} .torrent files found)", fg="black")
                self.log(f"Selected folder: {folderpath} ({count} torrents)")
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
        if not self.selected_file_path and not self.selected_folder_path:
            messagebox.showwarning("Warning", "Please select a torrent/zip file or folder first.")
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

        threading.Thread(target=self._process_thread, args=(use_custom, custom_cat, custom_path)).start()

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

    def _process_thread(self, use_custom, custom_cat, custom_path):
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

        if self.selected_file_path:
            if self.selected_file_path.lower().endswith('.zip'):
                # Handle Single ZIP - Updated to use helper
                items = self._parse_zip_file(self.selected_file_path)
                work_items.extend(items)
            else:
                # Normal Torrent
                work_items.append({'type': 'file', 'path': self.selected_file_path, 'content': None, 'category_subpath': ''})

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

                ok = self._add_torrent_content_to_client(client, torrent_content, torrent_path, category_subpath, extracted_id, use_custom, custom_cat, custom_path)
                if ok:
                    success_count += 1
                else:
                    fail_count += 1

            processed += 1
            pct = int(processed / total_items * 100)
            self.root.after(0, lambda p=pct, c=processed, t=total_items:
                self.status_bar.config(text=f"Adding: {c}/{t} ({p}%)"))

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

    def _add_torrent_content_to_client(self, client, content, filename_display, category_subpath, extracted_id, use_custom, custom_cat, custom_path):
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
            # Auth
            try:
                resp = session.get(f"{url}/api/v2/app/version", timeout=10)
                if resp.status_code != 200:
                    resp = session.post(f"{url}/api/v2/auth/login", data={"username": user, "password": pw}, timeout=10)
                    if resp.status_code != 200 or resp.text != "Ok.":
                        self.log(f"[{name}] Auth Failed!")
                        return
            except Exception as e:
                self.log(f"[{name}] Connection Error: {e}")
                return

            # Construct Path
            save_path = ""
            final_cat = ""

            if use_custom:
                # Custom Override Logic
                if custom_path:
                    save_path = custom_path.replace("\\", "/")
                else:
                    # If custom path is empty, maybe fallback to base path? 
                    # For now, let's use client base path if custom is empty but checked
                    save_path = base_path.replace("\\", "/")
                
                if custom_cat:
                    final_cat = custom_cat
                else:
                    # If custom cat is empty but checked, maybe no category or keep detected?
                    # Let's keep detected if custom content is empty
                    final_cat = category_subpath
                
                self.log(f"  -> Using Custom Path: {save_path}")
                self.log(f"  -> Using Custom Cat: {final_cat}")

            else:
                # Standard Logic
                final_path_list = [base_path]
                if category_subpath:
                    final_path_list.append(category_subpath)
                
                if extracted_id:
                    final_path_list.append(extracted_id)
                else:
                    # Fallback to filename without extension
                    fname = os.path.basename(filename_display)
                    final_path_list.append(os.path.splitext(fname)[0])
                    
                save_path = os.path.join(*final_path_list).replace("\\", "/")
                final_cat = category_subpath

            files = {'torrents': (os.path.basename(filename_display), content)}
            data = {'savepath': save_path, 'paused': 'false', 'root_folder': 'true'}
            
            # Set qBittorrent category
            if final_cat:
                data['category'] = final_cat
            
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
        self.updater_progress = ttk.Progressbar(self.updater_prog_frame, mode='determinate')
        self.updater_progress.pack(fill="x", padx=5, pady=(2, 0))
        self.updater_progress_label = tk.Label(self.updater_prog_frame, text="", fg="gray")
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
        self.updater_tree.configure(yscrollcommand=tree_scroll.set)
        self.updater_tree.pack(side="left", fill="both", expand=True)
        tree_scroll.pack(side="right", fill="y")

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
        current_tab = self.notebook.index(self.notebook.select())
        # Update Torrents tab is index 1
        if current_tab == 1 and not self.updater_scanning:
            self.updater_start_scan()

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
        self.updater_readd_redown_btn.config(state=state)
        self.updater_skip_btn.config(state=state)

    def _updater_update_progress(self, current, total, phase):
        pct = int(current / total * 100) if total > 0 else 0
        def _update(p=pct, c=current, t=total, ph=phase):
            self.updater_progress.configure(value=p)
            self.updater_progress_label.config(text=f"{ph}... {c}/{t} ({p}%)")
        self.root.after(0, _update)

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
        threading.Thread(target=self._updater_action_thread,
            args=(action_type, selected_entries), daemon=True).start()

    def _updater_action_thread(self, action_type, entries):
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

        for entry in entries:
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

        summary = f"Done: {success} succeeded, {fail} failed"
        self.updater_log(summary)
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

        self.repair_scan_btn = tk.Button(ctrl_frame, text="Scan Now", command=self.repair_start_scan)
        self.repair_scan_btn.pack(side="left", padx=5)

        self.repair_stop_btn = tk.Button(ctrl_frame, text="Stop", command=self.repair_stop_scan, state="disabled")
        self.repair_stop_btn.pack(side="left", padx=5)

        # --- Progress (hidden until scan) ---
        self.repair_prog_frame = tk.Frame(self.repair_tab)
        self.repair_progress = ttk.Progressbar(self.repair_prog_frame, mode='determinate')
        self.repair_progress.pack(fill="x", padx=5, pady=(2, 0))
        self.repair_progress_label = tk.Label(self.repair_prog_frame, text="", fg="gray")
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
        self.repair_tree.configure(yscrollcommand=tree_scroll.set)
        self.repair_tree.pack(side="left", fill="both", expand=True)
        tree_scroll.pack(side="right", fill="y")

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

    def _repair_set_action_buttons(self, state):
        self.repair_selected_btn.config(state=state)
        self.repair_all_btn.config(state=state)

    def _repair_update_progress(self, current, total, phase):
        pct = int(current / total * 100) if total > 0 else 0
        def _update(p=pct, c=current, t=total, ph=phase):
            self.repair_progress.configure(value=p)
            self.repair_progress_label.config(text=f"{ph}... {c}/{t} ({p}%)")
        self.root.after(0, _update)

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
        scan_start = time.time()
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

        # Phase 1: Fetch all torrents
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

        elapsed = time.time() - repair_start
        summary = f"Done: {success} repaired, {fail} failed (Elapsed: {elapsed:.1f}s)"
        self.repair_log(summary)
        self.root.after(0, lambda: messagebox.showinfo("Repair Complete", summary))
        self.root.after(0, lambda: self._repair_set_action_buttons(
            "normal" if self.repair_scan_results else "disabled"))
        self.root.after(0, lambda: self.repair_scan_btn.config(state="normal"))


if __name__ == "__main__":
    root = tk.Tk()
    app = QBitAdderApp(root)
    root.mainloop()
