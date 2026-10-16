import json
from pathlib import Path
from collect import Client, save

c = Client(Path('data/celeste'), 60)
for endpoint, params in (
    ('GetGameData', {'gameId': 'o1y9j9v6'}),
    ('GetGameLeaderboard2', {'params': {'gameId': 'o1y9j9v6', 'categoryId': '7kjpl1gk', 'obsolete': 1, 'video': 0, 'verified': 1, 'values': []}, 'page': 1}),
    ('GetGameLeaderboard2', {'params': {'gameId': 'o1y9j9v6', 'categoryId': '7kjpl1gk', 'obsolete': 1, 'video': 0, 'verified': 1, 'values': []}, 'page': 101}),
):
    data = c.v2(endpoint, params)
    save(Path('data/celeste') / f'probe_{endpoint}_{params.get("page", 0)}.json', data)
    print(json.dumps({'endpoint': endpoint, 'keys': list(data), 'pagination': data.get('pagination'), 'sample': data.get('runList', [])[:1]}, ensure_ascii=False), flush=True)
