import json
from pathlib import Path
from collect import Client

c = Client(Path('data/celeste'), 60)
game = c.get('games', abbreviation='celeste', max=200)['data'][0]
categories = c.get(f'games/{game["id"]}/categories')['data']
print(json.dumps({'game': game['id'], 'platforms': game['platforms'], 'categories': [(x['id'], x['name'], x['type']) for x in categories]}, indent=2), flush=True)
category = next(x for x in categories if x['name'] == 'Any%')
for offset in (0, 9800, 10000):
    try:
        page = c.get('runs', game=game['id'], category=category['id'], max=200, offset=offset, orderby='submitted', direction='asc')
        print(json.dumps({'offset': offset, 'size': len(page['data']), 'pagination': page.get('pagination'), 'first': page['data'][0]['submitted'] if page['data'] else None, 'last': page['data'][-1]['submitted'] if page['data'] else None}), flush=True)
    except RuntimeError as e:
        print(str(e), flush=True)
pc = '8gej2n93'
for status in ('verified', 'rejected', 'new'):
    for direction in ('asc', 'desc'):
        page = c.get('runs', game=game['id'], category=category['id'], platform=pc, status=status, max=200, offset=9800 if status=='verified' else 0, orderby='submitted', direction=direction)
        print(json.dumps({'platform': pc, 'status': status, 'direction': direction, 'size': len(page['data']), 'first': page['data'][0]['submitted'] if page['data'] else None, 'last': page['data'][-1]['submitted'] if page['data'] else None, 'examiners': list({x['status'].get('examiner') for x in page['data']})}), flush=True)
