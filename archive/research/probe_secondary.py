import json
from pathlib import Path
from collect import Client, save

c = Client(Path('data/celeste'), 10)
for category in ('jdz8oprd', '7kjpl1gk'):
    params = {'params': {'gameId': 'o1y9j9v6', 'categoryId': category, 'obsolete': 1,
                         'video': 0, 'emulator': 1, 'verified': 1, 'values': []}, 'page': 1}
    try:
        data = c.v2('GetGameLeaderboard', params)
        save(Path(f'data/celeste/probe_secondary_{category}.json'), data)
        board = data.get('leaderboard', data)
        print(json.dumps({'category': category, 'keys': list(board), 'pagination': board.get('pagination')}, ensure_ascii=False), flush=True)
    except RuntimeError as error:
        print(str(error), flush=True)
