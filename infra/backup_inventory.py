"""Read-only inventory for a frozen stand or isolated restored storage services.

Uses only local PostgreSQL/MongoDB/Garage. Never contacts Speedrun.com.
Prints counters and digests, not credentials or raw profile data.
"""
import hashlib
import json
import os
from pathlib import Path
from urllib.parse import unquote, urlsplit

import psycopg

from speedrun_etl.entity_ods import DATASETS
from speedrun_etl.entity_storage import mongo_document
from speedrun_etl.jsonutil import digest, dumps, read, utcnow
from speedrun_etl.storage import Mongo, S3, pg_connect


def inventory():
    os.environ.setdefault('PGPASSWORD', os.environ.get('ODS_DB_PASSWORD', ''))
    result = {'observed_at': utcnow()}
    samples = {}
    with pg_connect() as conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        conn.execute('SET statement_timeout=60000')
        result['postgres_counts'] = {}
        result['postgres_samples'] = {}
        for dataset in DATASETS:
            # DATASETS is a fixed project constant, never external SQL input.
            result['postgres_counts'][dataset] = conn.execute('SELECT count(*) FROM entity_v2.' + dataset).fetchone()[0]
            row = conn.execute('SELECT key,document FROM entity_v2.' + dataset + ' ORDER BY key LIMIT 1').fetchone()
            if row:
                samples[dataset] = row
                result['postgres_samples'][dataset] = {'key': row[0], 'digest': digest(row[1])}
        result['run_ids'] = conn.execute('SELECT count(DISTINCT source_id) FROM entity_v2.runs').fetchone()[0]
        result['player_ids'] = conn.execute('SELECT count(DISTINCT source_id) FROM entity_v2.players').fetchone()[0]
    with pg_connect(True) as conn:
        conn.execute('SET TRANSACTION READ ONLY')
        receipts = conn.execute('SELECT snapshot_id,role,store,package_digest FROM entity_receipts ORDER BY snapshot_id,role,store').fetchall()
        result['receipts'] = {'count': len(receipts), 'digest': digest([list(r) for r in receipts])}
        result['queue_states'] = [list(r) for r in conn.execute('SELECT batch_id,state,count(*) FROM entity_batch_queue GROUP BY batch_id,state ORDER BY batch_id,state')]
        queue = conn.execute("SELECT position,game_id,details->>'name',details->>'runCount' FROM entity_batch_queue WHERE batch_id='lab1-popular10k-20261029' AND state!='excluded' ORDER BY position").fetchall()
        result['popular_queue_order'] = {
            'queued': len(queue), 'descending_catalog_run_count': queue == sorted(queue, key=lambda r: (-int(r[3]), r[1])),
            'first_ten': [list(r) for r in queue[:10]], 'ordered_ids_digest': digest([r[1] for r in queue])}
    with Mongo() as store:
        db = store.client['entity_v2']
        result['mongo_counts'] = {name: db[name].count_documents({}) for name in DATASETS}
        result['mongo_samples'] = {}
        for dataset, (key, expected) in samples.items():
            row = db[dataset].find_one({'_id': key})
            if row is None or digest(mongo_document(row)) != digest(expected):
                raise ValueError('PG/Mongo sample differs: ' + dataset)
            result['mongo_samples'][dataset] = {'key': key, 'digest': digest(expected)}
    if result['postgres_counts'] != result['mongo_counts']:
        raise ValueError('PG/Mongo counts differ; do not seal this baseline')
    with S3() as store:
        listing = []
        for page in store.client.get_paginator('list_objects_v2').paginate(Bucket=store.bucket):
            listing.extend([r['Key'], r['Size'], r['ETag']] for r in page.get('Contents', []))
        listing.sort()
        result['s3'] = {'objects': len(listing), 'bytes': sum(r[1] for r in listing), 'listing_digest': digest(listing)}
        selected = sorted({0, len(listing)//4, len(listing)//2, 3*len(listing)//4, len(listing)-1}) if listing else []
        result['s3']['samples'] = []
        for index in selected:
            key = listing[index][0]
            response = store.client.get_object(Bucket=store.bucket, Key=key)
            with response['Body'] as body:
                data = body.read()
            result['s3']['samples'].append({'key': key, 'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)})
    airflow_password = os.environ.get('AIRFLOW_DB_PASSWORD')
    if not airflow_password:
        airflow_password = unquote(urlsplit(os.environ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'].replace('postgresql+psycopg2:', 'postgresql:')).password)
    with psycopg.connect(host=os.environ.get('PGHOST', 'postgres'), dbname='airflow', user='airflow', password=airflow_password) as conn:
        conn.execute('SET TRANSACTION READ ONLY')
        result['airflow_active_runs'] = conn.execute("SELECT count(*) FROM dag_run WHERE state IN ('running','queued')").fetchone()[0]
        result['airflow_active_tasks'] = conn.execute("SELECT count(*) FROM task_instance WHERE state IN ('running','queued')").fetchone()[0]
    state = Path(os.environ.get('ETL_STATE', '/not-mounted'))
    if state.exists():
        from speedrun_etl.progress import driver_running
        batches = {}
        for root in sorted((state/'batches').iterdir()):
            if not root.is_dir():
                continue
            batches[root.name] = {'driver_running': driver_running(root/'driver.lock'),
                                  'controller_running': driver_running(root/'keep-running.lock'),
                                  'status': read(root/'status.json') if (root/'status.json').exists() else None,
                                  'control': read(root/'control.json') if (root/'control.json').exists() else None}
        result['batches'] = batches
        result['source_http_attempts'] = sum(read(p)['used'] for p in (state/'collections').glob('*/budget.json'))
        result['safe_to_stop'] = (not result['airflow_active_runs'] and not result['airflow_active_tasks']
                                  and not any(b['driver_running'] or b['controller_running'] for b in batches.values()))
    return result


if __name__ == '__main__':
    print(dumps(inventory()), flush=True)
