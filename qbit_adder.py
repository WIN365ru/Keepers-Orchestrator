
import requests
import sys
import os
import argparse

# Configuration
# ------------------------------------------------------------------------------
# Default qBittorrent Web UI URL
QBIT_URL = "http://localhost:8080"
# Default credentials (leave empty if "Bypass authentication for clients on localhost" is enabled)
QBIT_USER = "admin"
QBIT_PASS = "adminadmin"
# Base directory where torrents should be downloaded
BASE_SAVE_PATH = "C:/Tors/Sport/"
# ------------------------------------------------------------------------------

def get_session():
    """Authenticates with qBittorrent and returns a session object."""
    session = requests.Session()
    
    # Try to access API without login first (in case of bypass localhost auth)
    try:
        response = session.get(f"{QBIT_URL}/api/v2/app/version")
        if response.status_code == 200:
            return session
    except requests.ConnectionError:
        print(f"Error: Could not connect to qBittorrent at {QBIT_URL}. Is it running?")
        sys.exit(1)

    # If login needed
    try:
        response = session.post(f"{QBIT_URL}/api/v2/auth/login", data={"username": QBIT_USER, "password": QBIT_PASS})
        if response.text == "Ok.":
            return session
        else:
            print("Error: Authentication failed. Please check your username and password in the script.")
            sys.exit(1)
    except Exception as e:
        print(f"Error during authentication: {e}")
        sys.exit(1)

def add_torrent(torrent_path):
    """Adds the torrent file to qBittorrent with the correct save path."""
    if not os.path.exists(torrent_path):
        print(f"Error: Torrent file not found at {torrent_path}")
        sys.exit(1)

    filename = os.path.basename(torrent_path)
    # Removing extension to get the name/number part (e.g. 3592805.torrent -> 3592805)
    name_part = os.path.splitext(filename)[0]
    
    # Construct the save path: C:/Tors/Sport/3592805
    save_path = os.path.join(BASE_SAVE_PATH, name_part).replace("\\", "/")
    
    print(f"Adding torrent: {filename}")
    print(f"Save Path: {save_path}")

    session = get_session()
    
    try:
        files = {'torrents': open(torrent_path, 'rb')}
        data = {
            'savepath': save_path,
            'category': 'Sport', # Optional: Set category if desired
            'paused': 'false',   # Start download immediately
            'root_folder': 'true' # Create subfolder (usually true by default)
        }
        
        response = session.post(f"{QBIT_URL}/api/v2/torrents/add", files=files, data=data)
        
        if response.status_code == 200 and response.text == "Ok.":
            print("Success: Torrent added successfully.")
        else:
            print(f"Error adding torrent. Status Code: {response.status_code}, Response: {response.text}")
            
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python qbit_adder.py <path_to_torrent_file>")
        print("Example: python qbit_adder.py \"C:\\Downloads\\3592805.torrent\"")
        input("Press Enter to exit...") # Keep window open if run via double-click without args
        sys.exit(1)
        
    torrent_file_path = sys.argv[1]
    add_torrent(torrent_file_path)
    
    # Optional: Pause for a moment to let user read output if run from a batch file/shortcut
    import time
    time.sleep(3)
