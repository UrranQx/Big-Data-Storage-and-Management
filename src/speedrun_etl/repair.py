"""Reviewable repair of incomplete partitions; existing snapshots stay immutable."""
from pathlib import Path
import argparse
import os
import re
from .collect import validate_conf
from .entity_ods import EntityPackage
from .entity_storage import ready
from .jsonutil import atomic_write, file_lock, read, utcnow
from .storage import pg_connect


def validate_base(state, conf):
    sid = conf["repair_from_snapshot_id"]
    root = Path(state) / "snapshots" / sid
    ref = read(root / "extracted.json")
    if ref["snapshot_id"] != sid or ref["role"] != "runs" or ref["format_version"] != "entity-v2":
        raise ValueError("Repair base must be an entity-v2 runs package")
    ready(sid, role="runs", expected=ref["digest"])
    package = EntityPackage(root / "entity-package")
    try:
        manifest = next(package.records("manifests"))
        result = manifest["source_result"]
    finally:
        package.db.close()
    if result.get("game_id") != conf["game_id"] or result.get("stop_reason"):
        raise ValueError("Wrong game or stopped source snapshot cannot be a repair base")
    requested = {(p["category_id"], p["status_code"]) for p in conf["repair_partitions"]}
    missing = {(p["category_id"], p["status_code"]) for p in result["partitions"]
               if p["coverage"] != "verified_public_partitions"}
    expected = {(cid, status) for cid in result["category_ids"] for status in (0,1,2)}
    actual = {(p["category_id"], p["status_code"]) for p in result["partitions"]}
    if requested != missing or expected != actual or len(actual) != len(result["partitions"]):
        raise ValueError("Repair must address every incomplete base partition exactly once")
    return result


def finish_repair(state, batch_id, game_id, collection, base_collection):
    """Publish queue completion only after matching three-store receipts and audit."""
    evidence = {}
    for kind in ("games_metadata", "runs", "players"):
        root = Path(state) / "snapshots" / (collection+"-"+kind)
        ref = read(root / "extracted.json")
        evidence[kind] = ready(ref["snapshot_id"], role=ref["role"], expected=ref["digest"])
    root = Path(state) / "snapshots" / (collection+"-runs")
    package = EntityPackage(root / "entity-package")
    try:
        result = next(package.records("manifests"))["source_result"]
    finally:
        package.db.close()
    if result.get("game_id") != game_id or result.get("repair_from_snapshot_id") != base_collection+"-runs":
        raise ValueError("Repair manifest does not match this queue entry")
    from .batch import result_state
    if result_state(result) != "complete":
        raise RuntimeError("Repaired source partitions remain partial; original queue gap retained")
    from .audit_entity_ods import audit
    checked = audit(root, root / "entity-package", Path(state) / "snapshots" / (collection+"-games_metadata") / "entity-package")
    from psycopg.types.json import Jsonb
    resolution = {"state": "resolved", "input_snapshot_id": base_collection+"-runs",
                  "repair_snapshot_id": collection+"-runs", "resolved_at": utcnow(),
                  "retained_base_collection_id": base_collection}
    with pg_connect(True) as conn:
        row = conn.execute("SELECT collection_id,state FROM entity_batch_queue WHERE batch_id=%s AND game_id=%s FOR UPDATE",
                           [batch_id,game_id]).fetchone()
        if row == (collection,"complete"):
            return {"state":"complete","evidence":evidence,"audit":checked}
        if row != (base_collection,"partial"):
            raise ValueError("Queue entry changed during repair")
        conn.execute("UPDATE entity_batch_queue SET state='complete',collection_id=%s,details=details || %s,updated_at=now() WHERE batch_id=%s AND game_id=%s",
                     [collection,Jsonb({"coverage_retry":resolution}),batch_id,game_id])
    return {"state":"complete","evidence":evidence,"audit":checked}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--batch',required=True)
    parser.add_argument('--game-id',required=True)
    parser.add_argument('--collection',required=True,help='New collection ID for repaired snapshot; reuse to resume')
    parser.add_argument('--request-budget',type=int,default=2500)
    parser.add_argument('--runs-strategy', choices=('standard', 'date-large'),
                        help='Explicit repair strategy; standard avoids known date-filter gaps')
    args=parser.parse_args()
    if any(not re.fullmatch(r'[a-zA-Z0-9_-]{1,80}',value) for value in (args.batch,args.game_id,args.collection)):
        parser.error('Invalid batch/game/collection ID')
    if not 100 <= args.request_budget <= 10000:
        parser.error('Repair request budget must be 100..10000')
    state=Path(os.environ['ETL_STATE']);root=state/'batches'/args.batch
    record=root/'repairs'/(args.collection+'.json')
    from .batch import start,wait,save_status
    with file_lock(root/'driver.lock'):
        if record.exists():
            saved=read(record);conf=saved['config'];base=saved['base_collection_id']
            if saved['game_id'] != args.game_id or conf['request_budget'] != args.request_budget:
                raise ValueError('Cannot change persisted repair scope')
            if args.runs_strategy and conf['run_partition_strategy'] != (
                    'standard' if args.runs_strategy == 'standard' else 'date_subdivision_v1'):
                raise ValueError('Cannot change persisted repair strategy')
        else:
            with pg_connect(True) as conn:
                row=conn.execute('SELECT collection_id,state FROM entity_batch_queue WHERE batch_id=%s AND game_id=%s',
                                 [args.batch,args.game_id]).fetchone()
            if not row or row[1]!='partial':
                raise ValueError('Only an existing partial queue entry can be repaired')
            base=row[0]
            old_conf=read(state/'snapshots'/(base+'-games_metadata')/'config.json')
            package=EntityPackage(state/'snapshots'/(base+'-runs')/'entity-package')
            try:
                result=next(package.records('manifests'))['source_result']
            finally:
                package.db.close()
            parts=[{'category_id':p['category_id'],'status_code':p['status_code']}
                   for p in result['partitions'] if p['coverage']!='verified_public_partitions']
            conf=validate_conf({**old_conf,'collection_id':args.collection,'request_budget':args.request_budget,
                'repair_from_snapshot_id':base+'-runs','repair_partitions':parts,
                'run_partition_strategy':'standard' if args.runs_strategy == 'standard' else 'date_subdivision_v1'})
            validate_base(state,conf)
            atomic_write(record,{'config':conf,'base_collection_id':base,'game_id':args.game_id,'created_at':utcnow()})
        save_status(root,'repair',collection_id=args.collection,game_id=args.game_id)
        from psycopg.types.json import Jsonb
        with pg_connect(True) as conn:
            conn.execute("UPDATE entity_batch_queue SET details=details || %s,updated_at=now() WHERE batch_id=%s AND game_id=%s AND state='partial'",
                [Jsonb({'coverage_retry':{'state':'running','input_snapshot_id':base+'-runs',
                    'repair_collection_id':args.collection,'partitions':conf['repair_partitions']}}),args.batch,args.game_id])
        try:
            print(f"Repair: {args.game_id}, {len(conf['repair_partitions'])} sections; base={base}; no catalog requests",flush=True)
            start(args.collection,conf)
            wait(args.collection,('games_metadata','runs','players'))
            result=finish_repair(state,args.batch,args.game_id,args.collection,base)
            atomic_write(record,{**read(record),'result':result,'completed_at':utcnow()})
        except Exception as error:
            save_status(root,'repair_paused',collection_id=args.collection,error=type(error).__name__+': '+str(error)[:300])
            raise
        with pg_connect(True) as conn:
            totals=dict(conn.execute("SELECT state,count(*) FROM entity_batch_queue WHERE batch_id=%s AND state!='excluded' GROUP BY state",[args.batch]).fetchall())
        phase = ('ready_to_resume' if totals.get('pending') else
                 'complete' if set(totals) <= {'complete','not_applicable'} else 'complete_with_gaps')
        save_status(root,phase,states=totals,total=sum(totals.values()))
        print(f'Repair confirmed in three stores; queue now {totals}',flush=True)


if __name__=='__main__':
    main()
