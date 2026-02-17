import requests, re

s = requests.Session()
s.headers.update({'User-Agent': 'Mozilla/5.0'})

tid = '3592805'
resp = s.get(f'https://rutracker.org/forum/viewtopic.php?t={tid}', timeout=15)
if resp.encoding == 'ISO-8859-1':
    resp.encoding = 'cp1251'

text = resp.text
print(f"Page length: {len(text)}")
print(f"Status code: {resp.status_code}")
print(f"URL: {resp.url}")

# Check if it's a login page
if 'login' in resp.url.lower():
    print("REDIRECTED TO LOGIN PAGE!")

# Look for any forum reference
print(f"\nviewforum count: {text.count('viewforum')}")
print(f"forum_id count: {text.count('forum_id')}")
print(f"f= count: {text.count('f=')}")

# Get first 2000 chars
print(f"\n--- First 2000 chars ---")
print(text[:2000])
