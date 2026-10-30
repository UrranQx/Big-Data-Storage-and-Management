"""The two explicit ODS contracts; old snapshots keep their original meaning."""
import re
import unicodedata


V2 = 'entity-v2'
V3 = 'entity-v3'
CONTEXT_FIELDS = ('place', 'obsolete')


def version(conf):
    return conf.get('data_model', V2)


def namespace(conf):
    return 'entity_v3' if version(conf) == V3 else 'entity_v2'


def kinds(conf):
    return ('games_metadata', 'runs', 'wr_history', 'players') if version(conf) == V3 else ('games_metadata', 'runs', 'players')


def role(kind):
    return kind if kind in ('runs', 'wr_history') else 'all'


def run_core(raw):
    """Keep unknown fields and missing/null distinctions; remove query context only."""
    return {k:v for k,v in raw.items() if k not in CONTEXT_FIELDS}


def dag_run_id(conf, kind):
    if version(conf) != V3:
        return conf['collection_id'] + '-' + kind
    identity = conf.get('game_id') or conf.get('display_game_id')
    label = conf.get('game_label') or conf.get('game')
    if not label and not identity:
        return conf['collection_id'] + '-' + kind
    label = unicodedata.normalize('NFKD', label or 'game').encode('ascii', 'ignore').decode()
    label = re.sub(r'[^a-z0-9]+', '-', label.lower()).strip('-') or 'game'
    parts = [conf['collection_id'], *([identity] if identity else []), label[:48], kind]
    result = '--'.join(parts)
    if len(result) > 250:
        raise ValueError('Readable DAG run ID exceeds Airflow length limit')
    return result
