import json
from pathlib import Path
from collect import Client

c = Client(Path('data/celeste'), 60)
for orderby in ('examiner', 'user', 'id'):
    try:
        data = c.get('runs', game='o1y9j9v6', category='7kjpl1gk', status='verified', max=1, orderby=orderby)
        print(orderby, 'ok', data['data'][0]['id'], flush=True)
    except RuntimeError as error:
        print(str(error), flush=True)
try:
    data = c.v2('GetGameSummary', {'gameId': 'o1y9j9v6'})
    print('summary keys', list(data), flush=True)
except RuntimeError as error:
    print(str(error), flush=True)
