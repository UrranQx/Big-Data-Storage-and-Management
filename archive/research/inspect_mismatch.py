import base64
import json
from pathlib import Path
from urllib.parse import urlsplit, parse_qs

v1, v2 = {}, {}
for file in Path('data/celeste/raw').glob('*.json'):
    entry = json.loads(file.read_text(encoding='utf-8'))
    url, response = entry['url'], entry['response']
    if '/api/v1/runs?' in url:
        params = parse_qs(urlsplit(url).query)
        if params.get('category') == ['jdz8oprd'] and params.get('status') == ['verified']:
            v1.update((r['id'], r) for r in response['data'])
    if '/api/v2/GetGameLeaderboard2?' in url:
        params = json.loads(base64.urlsafe_b64decode(parse_qs(urlsplit(url).query)['_r'][0] + '==='))
        if params['params'].get('categoryId') == 'jdz8oprd' and params['params'].get('verified') == 1:
            v2.update((r['id'], r) for r in response.get('runList', []))
print(json.dumps({'v1': len(v1), 'v2': len(v2), 'missing': [v1[r] for r in v1.keys()-v2.keys()]}, indent=2, ensure_ascii=False))
