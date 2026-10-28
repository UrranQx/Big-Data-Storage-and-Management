"""Three isolated, retained comparisons of one verified entity-v2 collection."""
import argparse
from collections import Counter, defaultdict
from contextlib import contextmanager
import csv
from datetime import datetime, timezone
import gzip
import hashlib
import os
from pathlib import Path
import re
import time

from .capacity import check_capacity
from .cli import api
from .entity_ods import DATASETS, EntityPackage
from .entity_storage import init_entities, load, mongo_document, ready, shard
from .jsonutil import atomic_write, digest, dumps, file_lock, loads, read
from .progress import driver_running
from .storage import pg_connect, Mongo, S3


def collections_busy(state):
    for folder in ('batches', 'queues'):
        if any(driver_running(p) for p in (state/folder).glob('*/driver.lock')):
            return True
    token=api('POST','/auth/token',{'username':'admin','password':os.environ['AIRFLOW_ADMIN_PASSWORD']})['access_token']
    for kind in ('games_metadata','runs','players'):
        result=api('GET',f'/api/v2/dags/{kind}/dagRuns?limit=100&order_by=-start_date',token=token)
        if any(r['state'] in ('running','queued') for r in result['dag_runs']):
            return True
    return False


def prepare_input(state, collection, target):
    package=EntityPackage(target); inputs={}
    for kind in ('games_metadata','runs','players'):
        sid=collection+'-'+kind
        ref=read(state/'snapshots'/sid/'extracted.json')
        ready(sid,role=ref['role'],expected=ref['digest'])
        original=EntityPackage(ref['package_uri'],role=ref['role'])
        try:
            for dataset in DATASETS:
                for row in original.records(dataset):package.put(dataset,row)
            inputs[sid]=ref['digest']
        finally:original.db.close()
    package.db.commit()
    return package,inputs


@contextmanager
def reader(name, namespace, sid, summary):
    """Reuse connections and S3 index; do not cache S3 chunks in the client."""
    if name=='postgres':
        from psycopg import sql
        with pg_connect() as conn:
            def fetch(dataset,source_id=None,scope=None):
                conditions,args=[],[]
                if source_id is not None:
                    conditions.append(sql.SQL('source_id=%s'));args.append(source_id)
                if scope is not None:
                    conditions.append(sql.SQL('game_id=%s AND category_id=%s AND status=%s'));args.extend(scope)
                where=sql.SQL(' WHERE ')+sql.SQL(' AND ').join(conditions) if conditions else sql.SQL('')
                query=sql.SQL('SELECT document FROM {}').format(sql.Identifier(namespace,dataset))+where+sql.SQL(' ORDER BY key')
                return [r[0] for r in conn.execute(query,args)]
            yield fetch
    elif name=='mongo':
        with Mongo() as store:
            db=store.client[namespace]
            def fetch(dataset,source_id=None,scope=None):
                query={'source_id':source_id} if source_id is not None else {}
                if scope is not None:query.update(dict(zip(('game_id','category_id','status'),scope)))
                return [mongo_document(r) for r in db[dataset].find(query).sort('_id',1)]
            yield fetch
    else:
        with S3() as store:
            key=f'{namespace}/entity-v2/snapshots/{sid}/all/{digest(summary)}.json'
            response=store.client.get_object(Bucket=store.bucket,Key=key)
            with response['Body'] as stream:index=loads(stream.read())
            def fetch(dataset,source_id=None,scope=None):
                result=[]
                part=shard({'key':source_id,'source_id':source_id}) if source_id is not None else None
                for chunk in index['chunks']:
                    if chunk['dataset']!=dataset or (part is not None and chunk['shard']!=part):continue
                    response=store.client.get_object(Bucket=store.bucket,Key=chunk['key'])
                    with response['Body'] as stream:raw=stream.read()
                    if hashlib.sha256(raw).hexdigest()!=chunk['sha256']:raise ValueError('S3 benchmark checksum differs')
                    for row in map(loads,gzip.decompress(raw).splitlines()):
                        if source_id is not None and row.get('source_id')!=source_id:continue
                        if scope is not None and tuple(row.get(k) for k in ('game_id','category_id','status'))!=scope:continue
                        result.append(row)
                return sorted(result,key=lambda r:r['key'])
            yield fetch


def read_operation(fetch, operation, ids, sid, scope):
    if operation=='filter':return fetch('runs',scope=scope)
    result=[]
    for source_id in ids:
        checks=[c for c in fetch('checks',source_id) if c['dataset']=='runs' and c['snapshot_id']==sid]
        if operation=='history_100':result.append(checks)
        else:
            versions={r['key']:r for r in fetch('runs',source_id)}
            result.append([{'projection':c['projection'],'seen':c['last_seen'],
                            'version':versions[c['last_seen']['version_key']]} for c in checks])
    return result


def store_size(name, namespace):
    if name=='postgres':
        from psycopg import sql
        with pg_connect() as conn:
            data=indexes=count=0
            for dataset in DATASETS:
                table=f'"{namespace}"."{dataset}"'
                sizes=conn.execute('SELECT pg_table_size(%s::regclass),pg_indexes_size(%s::regclass)',[table,table]).fetchone()
                data+=sizes[0];indexes+=sizes[1]
                count+=conn.execute(sql.SQL('SELECT count(*) FROM {}').format(sql.Identifier(namespace,dataset))).fetchone()[0]
        return {'data_bytes':data,'index_bytes':indexes,'reported_bytes':data+indexes,'count':count,
                'size_method':'ODS schema only: pg_table_size + pg_indexes_size'}
    if name=='mongo':
        with Mongo() as store:
            store.client.admin.command({'fsync':1})
            db=store.client[namespace];stats=db.command('dbStats');count=sum(db[d].count_documents({}) for d in DATASETS)
        return {'data_bytes':stats['dataSize'],'index_bytes':stats['indexSize'],
                'reported_bytes':stats['storageSize']+stats['indexSize'],'count':count,
                'size_method':'ODS DB storageSize + indexSize after fsync; dataSize separate'}
    with S3() as store:
        objects=sorted([o['Key'],o['Size'],o['ETag']] for page in store.client.get_paginator('list_objects_v2').paginate(
            Bucket=store.bucket,Prefix=namespace+'/entity-v2/') for o in page.get('Contents',[]))
    total=sum(o[1] for o in objects)
    return {'data_bytes':total,'index_bytes':None,'reported_bytes':total,'object_count':len(objects),
            'object_inventory_digest':digest(objects),'size_method':'S3 object bodies; excludes physical Garage volume overhead'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--collection',required=True);parser.add_argument('--wait',action='store_true');parser.add_argument('--log',action='store_true')
    args=parser.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,80}',args.collection):parser.error('Invalid collection ID')
    state=Path(os.environ['ETL_STATE'])
    while collections_busy(state):
        if not args.wait:raise RuntimeError('Wait until collection drivers and DAGs finish')
        time.sleep(30)
    check_capacity(state)
    stamp=datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    root=state/'benchmarks'/('entity_v2_'+stamp);root.mkdir(parents=True,exist_ok=False)
    if args.log:
        import sys
        sys.stdout=sys.stderr=(root/'driver.log').open('a',buffering=1)
    with file_lock(state/'source.lock'):
        if collections_busy(state):raise RuntimeError('Collection started during preflight')
        package,inputs=prepare_input(state,args.collection,root/'package')
        sid=args.collection+'-runs';summary=package.summary();input_digest=digest(summary)
        checks=[c for c in package.records('checks') if c['dataset']=='runs' and c['snapshot_id']==sid]
        ids=sorted({c['source_id'] for c in checks},key=digest)[:100]
        scopes=Counter(tuple(r.get(k) for k in ('game_id','category_id','status')) for r in package.records('runs')
                       if r.get('status')==1 and r.get('category_id'))
        if len(ids)!=100 or not scopes:raise ValueError('Need 100 run IDs and a verified partition')
        scope=sorted(scopes,key=lambda k:(-scopes[k],k))[0]
        logical=sum(len(dumps(r).encode()) for d in DATASETS for r in package.records(d))
        records=sum(v['count'] for v in summary.values())
        atomic_write(root/'input.json',{'collection_id':args.collection,'source_snapshots':inputs,'format_version':'entity-v2',
            'digest':input_digest,'counts':summary,'logical_bytes':logical,'read_ids':ids,'filter':list(scope),'source_HTTP':0,
            'notes':'Full verified collection. Load includes logical readback/receipt; schemas/indexes created before timing. '
                    'Read connections/S3 index reused; data chunks not cached by client. No cold-cache claim. '
                    'Latest state is per projection, not a computed PB/WR. S3 filtering is client-side. '
                    'Logical bytes are canonical record bytes; service size metrics use different physical accounting.'})
        lookup={d:defaultdict(list) for d in ('runs','checks')};chosen=set(ids);filtered=[]
        for dataset in lookup:
            for row in package.records(dataset):
                if row.get('source_id') in chosen:lookup[dataset][row['source_id']].append(row)
                if dataset=='runs' and tuple(row.get(k) for k in ('game_id','category_id','status'))==scope:filtered.append(row)
        def local_fetch(dataset,source_id=None,scope=None):return filtered if scope is not None else lookup[dataset][source_id]
        expected={op:digest(read_operation(local_fetch,op,ids,sid,scope)) for op in ('history_100','latest_100','filter')}
        fields=['repeat','store','namespace','operation','pass','seconds','count','digest','data_bytes',
                'index_bytes','reported_bytes','size_method','object_count','logical_bytes','records_per_second']
        csv_path=root/'measurements.csv'
        with csv_path.open('w',newline='') as output:
            writer=csv.DictWriter(output,fieldnames=fields);writer.writeheader()
            for repeat in range(3):
                namespace=f'test_bench_{stamp}_{repeat}';check_capacity(state);init_entities(namespace)
                order=['postgres','mongo','garage'];order=order[repeat:]+order[:repeat]
                for name in order:
                    base={'repeat':repeat+1,'store':name,'namespace':namespace,'logical_bytes':logical};before=None
                    for operation in ('load_verified','replay_verified'):
                        check_capacity(state);started=time.perf_counter();result=load(name,package,sid,namespace)
                        elapsed=time.perf_counter()-started
                        if result['digest']!=input_digest:raise ValueError('Load changed content')
                        writer.writerow({**base,'operation':operation,'seconds':elapsed,'count':records,'digest':input_digest,'records_per_second':records/elapsed})
                        size=store_size(name,namespace)
                        if name=='garage':
                            if before and before['object_inventory_digest']!=size['object_inventory_digest']:raise ValueError('Replay changed S3 objects')
                        elif size['count']!=records:raise ValueError('Replay changed physical row count')
                        before=size;output.flush();print(repeat+1,name,operation,round(elapsed,3),flush=True)
                    writer.writerow({**base,'operation':'size',**{k:v for k,v in size.items() if k!='object_inventory_digest'}})
                    with reader(name,namespace,sid,summary) as fetch:
                        for pass_no in range(1,4):
                            for operation in expected:
                                started=time.perf_counter();values=read_operation(fetch,operation,ids,sid,scope)
                                elapsed=time.perf_counter()-started;checksum=digest(values)
                                if checksum!=expected[operation]:raise ValueError(f'{name} {operation} returned different records')
                                writer.writerow({**base,'operation':operation,'pass':pass_no,'seconds':elapsed,'count':len(values),'digest':checksum});output.flush()
                ready(sid,namespace,expected=input_digest)
        package.db.close()
        atomic_write(root/'completed.json',{'format_version':'entity-v2','collection_id':args.collection,'input_digest':input_digest,
            'csv':str(csv_path),'repeats':3,'source_HTTP':0,'namespaces_retained':True})
    print('Benchmark completed:',root,flush=True)


if __name__=='__main__':main()
