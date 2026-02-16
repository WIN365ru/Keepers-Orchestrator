
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
import requests
import os
import json
import threading

# Default Configuration (New Structure)
DEFAULT_CONFIG = {
    "global_auth": {
        "enabled": False,
        "username": "admin",
        "password": "adminadmin"
    },
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

        # UI Setup
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill="both", expand=True, padx=5, pady=5)

        self.adder_tab = tk.Frame(self.notebook)
        self.settings_tab = tk.Frame(self.notebook)
        
        self.notebook.add(self.adder_tab, text="Add Torrents")
        self.notebook.add(self.settings_tab, text="Settings")

        self.create_settings_ui()
        self.create_adder_ui()

    def log(self, message):
        self.log_area.config(state="normal")
        self.log_area.insert(tk.END, message + "\n")
        self.log_area.see(tk.END)
        self.log_area.config(state="disabled")

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
        # Global Auth Section
        global_frame = tk.LabelFrame(self.settings_tab, text="Global Authentication", padx=10, pady=10)
        global_frame.pack(fill="x", padx=10, pady=5)

        self.use_global_var = tk.BooleanVar(value=self.config["global_auth"]["enabled"])
        tk.Checkbutton(global_frame, text="Use Global Credentials for enabled clients", variable=self.use_global_var, command=self.update_settings_model).grid(row=0, column=0, columnspan=2, sticky="w")

        tk.Label(global_frame, text="Global Username:").grid(row=1, column=0, sticky="w")
        self.global_user_entry = tk.Entry(global_frame, width=30)
        self.global_user_entry.insert(0, self.config["global_auth"]["username"])
        self.global_user_entry.grid(row=1, column=1, padx=5, pady=2)
        self.global_user_entry.bind("<FocusOut>", self.update_settings_model)

        tk.Label(global_frame, text="Global Password:").grid(row=2, column=0, sticky="w")
        self.global_pass_entry = tk.Entry(global_frame, width=30, show="*")
        self.global_pass_entry.insert(0, self.config["global_auth"]["password"])
        self.global_pass_entry.grid(row=2, column=1, padx=5, pady=2)
        self.global_pass_entry.bind("<FocusOut>", self.update_settings_model)

        # Clients List Section
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

        # Client Details Editor
        details_frame = tk.Frame(clients_frame)
        details_frame.pack(side="left", fill="both", expand=True, padx=10)

        tk.Label(details_frame, text="Name:").grid(row=0, column=0, sticky="w")
        self.c_name = tk.Entry(details_frame, width=30)
        self.c_name.grid(row=0, column=1, pady=2)
        self.c_name.bind("<FocusOut>", self.save_current_client_details)

        tk.Label(details_frame, text="URL:").grid(row=1, column=0, sticky="w")
        self.c_url = tk.Entry(details_frame, width=30)
        self.c_url.grid(row=1, column=1, pady=2)
        self.c_url.bind("<FocusOut>", self.save_current_client_details)

        tk.Label(details_frame, text="Base Path:").grid(row=2, column=0, sticky="w")
        self.c_path = tk.Entry(details_frame, width=30)
        self.c_path.grid(row=2, column=1, pady=2)
        self.c_path.bind("<FocusOut>", self.save_current_client_details)

        self.c_use_global = tk.BooleanVar()
        tk.Checkbutton(details_frame, text="Use Global Auth", variable=self.c_use_global, command=self.toggle_client_auth_fields).grid(row=3, column=0, columnspan=2, sticky="w")

        tk.Label(details_frame, text="Username:").grid(row=4, column=0, sticky="w")
        self.c_user = tk.Entry(details_frame, width=30)
        self.c_user.grid(row=4, column=1, pady=2)
        self.c_user.bind("<FocusOut>", self.save_current_client_details)

        tk.Label(details_frame, text="Password:").grid(row=5, column=0, sticky="w")
        self.c_pass = tk.Entry(details_frame, width=30, show="*")
        self.c_pass.grid(row=5, column=1, pady=2)
        self.c_pass.bind("<FocusOut>", self.save_current_client_details)
        
        self.current_client_index = -1
        self.refresh_client_list()

    def refresh_client_list(self):
        self.client_listbox.delete(0, tk.END)
        for c in self.config["clients"]:
            self.client_listbox.insert(tk.END, c["name"])
        
        # Select first if exists
        if self.config["clients"]:
            self.client_listbox.select_set(0)
            self.on_client_select(None)

    def on_client_select(self, event):
        selection = self.client_listbox.curselection()
        if not selection:
            return
        
        idx = selection[0]
        self.current_client_index = idx
        client = self.config["clients"][idx]

        self.c_name.delete(0, tk.END); self.c_name.insert(0, client["name"])
        self.c_url.delete(0, tk.END); self.c_url.insert(0, client["url"])
        self.c_path.delete(0, tk.END); self.c_path.insert(0, client["base_save_path"])
        self.c_use_global.set(client["use_global_auth"])
        self.c_user.delete(0, tk.END); self.c_user.insert(0, client["username"])
        self.c_pass.delete(0, tk.END); self.c_pass.insert(0, client["password"])
        
        self.toggle_client_auth_fields()

    def toggle_client_auth_fields(self):
        # If Using Global Auth, disable specific fields
        state = "disabled" if self.c_use_global.get() else "normal"
        self.c_user.config(state=state)
        self.c_pass.config(state=state)
        self.save_current_client_details()

    def update_settings_model(self, event=None):
        self.config["global_auth"]["enabled"] = self.use_global_var.get()
        self.config["global_auth"]["username"] = self.global_user_entry.get()
        self.config["global_auth"]["password"] = self.global_pass_entry.get()
        self.save_config()

    def save_current_client_details(self, event=None):
        if self.current_client_index < 0: return
        
        idx = self.current_client_index
        self.config["clients"][idx]["name"] = self.c_name.get()
        self.config["clients"][idx]["url"] = self.c_url.get()
        self.config["clients"][idx]["base_save_path"] = self.c_path.get()
        self.config["clients"][idx]["use_global_auth"] = self.c_use_global.get()
        self.config["clients"][idx]["username"] = self.c_user.get()
        self.config["clients"][idx]["password"] = self.c_pass.get()
        
        # Refresh list name if changed
        self.client_listbox.delete(idx)
        self.client_listbox.insert(idx, self.c_name.get())
        self.client_listbox.select_set(idx)
        
        self.save_config()
        self.update_client_dropdown()

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
        self.refresh_client_list()
        self.client_listbox.select_set(len(self.config["clients"])-1)
        self.on_client_select(None)
        self.save_config()
        self.update_client_dropdown()

    def remove_client(self):
        if self.current_client_index < 0: return
        if not messagebox.askyesno("Confirm", "Delete this client?"): return
        
        del self.config["clients"][self.current_client_index]
        self.refresh_client_list()
        self.save_config()
        self.update_client_dropdown()


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
        
        self.file_label = tk.Label(sel_frame, text="No file/folder selected", fg="gray", wraplength=500)
        self.file_label.pack(pady=5)
        
        btn_frame = tk.Frame(sel_frame)
        btn_frame.pack(pady=5)
        tk.Button(btn_frame, text="Select Torrent File", command=self.select_file).pack(side="left", padx=5)
        tk.Button(btn_frame, text="Select Folder", command=self.select_folder).pack(side="left", padx=5)

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

    # --- Shared Actions (Select File/Folder) ---
    def select_file(self):
        filepath = filedialog.askopenfilename(filetypes=[("Torrent files", "*.torrent"), ("All files", "*.*")])
        if filepath:
            self.selected_file_path = filepath
            self.selected_folder_path = None
            self.file_label.config(text=f"File: {os.path.basename(filepath)}", fg="black")
            self.log(f"Selected file: {filepath}")

    def select_folder(self):
        folderpath = filedialog.askdirectory()
        if folderpath:
            self.selected_folder_path = folderpath
            self.selected_file_path = None
            self.file_label.config(text=f"Folder: {folderpath}", fg="black")
            self.log(f"Selected folder: {folderpath}")

    # --- Logic ---
    def process_torrent(self):
        if not self.selected_file_path and not self.selected_folder_path:
            messagebox.showwarning("Warning", "Please select a torrent file or folder first.")
            return

        # Save selected client index
        self.config["last_selected_client_index"] = self.client_selector.current()
        self.save_config()

        self.stop_event.clear()
        self.running_event.set()
        self.add_btn.config(state="disabled")
        self.stop_btn.config(state="normal")
        self.pause_btn.config(state="normal", text="Pause")

        threading.Thread(target=self._process_thread).start()

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

    def _process_thread(self):
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

        # Determine files
        files_to_process = []
        if self.selected_file_path:
            files_to_process.append(self.selected_file_path)
        elif self.selected_folder_path:
            try:
                for f in os.listdir(self.selected_folder_path):
                    if f.lower().endswith(".torrent"):
                        files_to_process.append(os.path.join(self.selected_folder_path, f))
            except Exception as e:
                self.log(f"Error reading folder: {e}")
                self.reset_buttons()
                return

        if not files_to_process:
            self.log("No torrents found.")
            self.reset_buttons()
            return
            
        total_ops = len(files_to_process) * len(target_clients)
        self.log(f"Starting job: {len(files_to_process)} file(s) on {len(target_clients)} client(s)...")

        # Processing Loop
        for torrent_path in files_to_process:
            for client in target_clients:
                # Flow Control
                if self.stop_event.is_set(): break
                if not self.running_event.is_set():
                    self.log("Paused...")
                    self.running_event.wait()
                    if self.stop_event.is_set(): break
                    self.log("Resuming...")

                self._add_torrent_to_client(torrent_path, client)

            if self.stop_event.is_set(): 
                self.log("Stopped by user.")
                break

        self.log("Job finished.")
        messagebox.showinfo("Done", "Processing complete.")
        
        self.reset_buttons()
        # Clear selection if successful? Optional.
        # if not self.stop_event.is_set():
        #    self.selected_file_path = None
        #    self.selected_folder_path = None
        #    self.file_label.config(text="No selection")

    def _add_torrent_to_client(self, torrent_path, client):
        name = client["name"]
        url = client["url"]
        path = client["base_save_path"]
        
        # Auth Resolution
        if client["use_global_auth"] and self.config["global_auth"]["enabled"]:
            user = self.config["global_auth"]["username"]
            pw = self.config["global_auth"]["password"]
        else:
            user = client["username"]
            pw = client["password"]
            
        filename = os.path.basename(torrent_path)
        
        try:
            self.log(f"[{name}] Adding: {filename}")
            
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

            # Calc Path same as before
            name_part = os.path.splitext(filename)[0]
            save_path = os.path.join(path, name_part).replace("\\", "/")

            files = {'torrents': open(torrent_path, 'rb')}
            data = {'savepath': save_path, 'paused': 'false', 'root_folder': 'true'}
            
            resp = session.post(f"{url}/api/v2/torrents/add", files=files, data=data, timeout=30)
            
            if resp.status_code == 200 and resp.text == "Ok.":
                self.log(f"[{name}] Success.")
            else:
                self.log(f"[{name}] Failed: {resp.text}")

        except Exception as e:
            self.log(f"[{name}] Error: {e}")

    def reset_buttons(self):
        self.add_btn.config(state="normal")
        self.stop_btn.config(state="disabled")
        self.pause_btn.config(state="disabled", text="Pause")

if __name__ == "__main__":
    root = tk.Tk()
    app = QBitAdderApp(root)
    root.mainloop()

