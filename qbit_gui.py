
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext
import requests
import os
import json
import threading

# Default Configuration
DEFAULT_CONFIG = {
    "qbit_url": "http://localhost:8080",
    "qbit_user": "admin",
    "qbit_pass": "adminadmin",
    "base_save_path": "C:/Torrents/Sport/"
}

CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "q_adder_config.json")

class QBitAdderApp:
    def __init__(self, root):
        self.root = root
        self.root.title("qBittorrent Auto-Adder")
        self.root.geometry("500x550")
        
        self.config = self.load_config()

        # UI Elements
        self.create_widgets()
        
    def create_widgets(self):
        # Configuration Section
        config_frame = tk.LabelFrame(self.root, text="Configuration", padx=10, pady=10)
        config_frame.pack(fill="x", padx=10, pady=5)
        
        # URL
        tk.Label(config_frame, text="qBittorrent URL:").grid(row=0, column=0, sticky="w")
        self.url_entry = tk.Entry(config_frame, width=40)
        self.url_entry.insert(0, self.config["qbit_url"])
        self.url_entry.grid(row=0, column=1, padx=5, pady=2)
        
        # Username
        tk.Label(config_frame, text="Username:").grid(row=1, column=0, sticky="w")
        self.user_entry = tk.Entry(config_frame, width=40)
        self.user_entry.insert(0, self.config["qbit_user"])
        self.user_entry.grid(row=1, column=1, padx=5, pady=2)
        
        # Password
        tk.Label(config_frame, text="Password:").grid(row=2, column=0, sticky="w")
        self.pass_entry = tk.Entry(config_frame, width=40, show="*")
        self.pass_entry.insert(0, self.config["qbit_pass"])
        self.pass_entry.grid(row=2, column=1, padx=5, pady=2)
        
        # Base Path
        tk.Label(config_frame, text="Base Save Path:").grid(row=3, column=0, sticky="w")
        self.path_entry = tk.Entry(config_frame, width=40)
        self.path_entry.insert(0, self.config["base_save_path"])
        self.path_entry.grid(row=3, column=1, padx=5, pady=2)

        # Update Config Button
        tk.Button(config_frame, text="Save Config", command=self.save_config).grid(row=4, column=1, sticky="e", pady=5)

        # Action Section
        action_frame = tk.LabelFrame(self.root, text="Add Torrent", padx=10, pady=10)
        action_frame.pack(fill="x", padx=10, pady=5)
        
        self.file_label = tk.Label(action_frame, text="No file selected", fg="gray", wraplength=450)
        self.file_label.pack(pady=5)
        
        btn_frame = tk.Frame(action_frame)
        btn_frame.pack(pady=5)
        
        tk.Button(btn_frame, text="Select Torrent File", command=self.select_file).pack(side="left", padx=5)
        tk.Button(btn_frame, text="Add to qBittorrent", command=self.process_torrent, bg="#dddddd").pack(side="left", padx=5)

        # Log Section
        log_frame = tk.LabelFrame(self.root, text="Log", padx=10, pady=10)
        log_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        self.log_area = scrolledtext.ScrolledText(log_frame, height=10, state="disabled")
        self.log_area.pack(fill="both", expand=True)

        self.selected_file_path = None

    def log(self, message):
        self.log_area.config(state="normal")
        self.log_area.insert(tk.END, message + "\n")
        self.log_area.see(tk.END)
        self.log_area.config(state="disabled")

    def load_config(self):
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r") as f:
                    return json.load(f)
            except:
                return DEFAULT_CONFIG
        return DEFAULT_CONFIG

    def save_config(self):
        new_config = {
            "qbit_url": self.url_entry.get(),
            "qbit_user": self.user_entry.get(),
            "qbit_pass": self.pass_entry.get(),
            "base_save_path": self.path_entry.get()
        }
        try:
            with open(CONFIG_FILE, "w") as f:
                json.dump(new_config, f, indent=4)
            self.config = new_config
            self.log("Configuration saved.")
            messagebox.showinfo("Success", "Configuration saved successfully!")
        except Exception as e:
            self.log(f"Error saving config: {e}")
            messagebox.showerror("Error", f"Could not save config: {e}")

    def select_file(self):
        filepath = filedialog.askopenfilename(filetypes=[("Torrent files", "*.torrent"), ("All files", "*.*")])
        if filepath:
            self.selected_file_path = filepath
            self.file_label.config(text=os.path.basename(filepath), fg="black")
            self.log(f"Selected file: {filepath}")

    def process_torrent(self):
        if not self.selected_file_path:
            messagebox.showwarning("Warning", "Please select a torrent file first.")
            return

        # Disable UI during processing
        # Use threading to keep GUI responsive
        threading.Thread(target=self._process_torrent_thread).start()

    def _process_torrent_thread(self):
        url = self.config["qbit_url"]
        user = self.config["qbit_user"]
        password = self.config["qbit_pass"]
        base_path = self.config["base_save_path"]
        torrent_path = self.selected_file_path

        try:
            session = requests.Session()
            
            # Authentication
            self.log("Authenticating...")
            try:
                # Try version check first to see if auth is needed/bypassed
                resp = session.get(f"{url}/api/v2/app/version", timeout=20)
                if resp.status_code != 200:
                    # Login
                    resp = session.post(f"{url}/api/v2/auth/login", data={"username": user, "password": password}, timeout=20)
                    if resp.status_code != 200 or resp.text != "Ok.":
                        self.log("Error: Authentication failed.")
                        messagebox.showerror("Error", "Authentication failed. Check logs.")
                        return
            except requests.ConnectionError:
                 self.log(f"Error: Could not connect to {url}")
                 messagebox.showerror("Error", f"Could not connect to {url}")
                 return

            # Construct Save Path
            filename = os.path.basename(torrent_path)
            name_part = os.path.splitext(filename)[0]
            save_path = os.path.join(base_path, name_part).replace("\\", "/")
            
            self.log(f"Adding: {filename}")
            self.log(f"Target Save Path: {save_path}")

            # Add Torrent
            files = {'torrents': open(torrent_path, 'rb')}
            data = {
                'savepath': save_path,
                'paused': 'false',
                'root_folder': 'true'
            }
            
            resp = session.post(f"{url}/api/v2/torrents/add", files=files, data=data, timeout=30)
            
            if resp.status_code == 200 and resp.text == "Ok.":
                self.log("Success: Torrent added!")
                messagebox.showinfo("Success", f"Torrent added to: {save_path}")
                self.selected_file_path = None
                self.file_label.config(text="No file selected", fg="gray")
            else:
                self.log(f"Error adding torrent: {resp.status_code} - {resp.text}")
                messagebox.showerror("Error", f"Failed to add torrent: {resp.text}")

        except Exception as e:
            self.log(f"Exception: {e}")
            messagebox.showerror("Exception", str(e))

if __name__ == "__main__":
    root = tk.Tk()
    app = QBitAdderApp(root)
    root.mainloop()
