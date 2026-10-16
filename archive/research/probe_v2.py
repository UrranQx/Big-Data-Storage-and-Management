import base64
import json
import time
import urllib.request
import urllib.parse
from pathlib import Path
from collect import save

for verified in (1, 2, 0):
    params = {'params': {'gameId': 'o1y9j9v6', 'categoryId': '7kjpl1gk', 'obsolete': 1, 'video': 0, 'verified': verified}, 'page': 1}
    query = base64.urlsafe_b64encode(json.dumps(params, separators=(',', ':')).encode()).decode().rstrip('=')
    url = 'https://www.speedrun.com/api/v2/GetGameLeaderboard2?' + urllib.parse.urlencode({'_r': query})
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={'User-Agent': 'ETL-practice/1.0'}), timeout=60) as response:
            data = json.load(response)
        save(Path(f'data/celeste/probe_v2_{verified}.json'), data)
        print(json.dumps({'verified': verified, 'keys': list(data), 'pagination': data.get('pagination'), 'sample': data.get('runList', [])[:1]}, ensure_ascii=False), flush=True)
    except Exception as e:
        print(type(e).__name__, str(e), flush=True)
    time.sleep(1)
