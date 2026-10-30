"""Entity-v2 extraction, preserving the established sequential source collector."""
from pathlib import Path

from .collect import validate_conf, location, snapshot_id, metadata, collect_runs
from .entity_ods import EntityPackage, build
from .jsonutil import atomic_write, digest, read
from .source import Client, BudgetExceeded
from .model import V2, V3, version, namespace, role as load_role


def profiles(source, target, sid, input_sid, additional_sources=(), format_version=V2):
    participants = set()
    sources = [(source,input_sid), *additional_sources]
    for package, source_sid in sources:
        for dataset in ('runs', 'wr_history'):
            total = package.db.execute('SELECT count(*) FROM records WHERE dataset=?',(dataset,)).fetchone()[0]
            print(f'Players: input={source_sid} {dataset}=0/{total}',flush=True)
            for index,row in enumerate(package.records(dataset),1):
                participants.update(p for p in row.get('player_ids',[]) if isinstance(p,str) and p)
                if index % 1000 == 0 or index == total:
                    print(f'Players: {dataset}={index}/{total} participant_ids={len(participants)}',flush=True)
    found = set()
    copied_blocks = set()
    for package, source_sid in sources:
        total = package.db.execute("SELECT count(*) FROM records WHERE dataset='players'").fetchone()[0]
        for index,row in enumerate(package.records('players'),1):
            if index % 1000 == 0 or index == total:
                print(f'Players: input={source_sid} profile_versions={index}/{total} matched_ids={len(found)}',flush=True)
            if row['source_id'] not in participants or package.decode(row['raw']).get('isGuest') is True:
                continue
            for key in row['raw'].get('blocks',[]):
                if key not in copied_blocks:
                    target.put('blocks',package.get('blocks',key)); copied_blocks.add(key)
            target.put('players',row); found.add(row['source_id'])
        for row in package.records('checks'):
            if row['dataset']=='players' and row['source_id'] in found:
                target.put('checks',row)
    result = {"participants": len(participants), "available_profiles": len(found),
              "unavailable_or_unresolved": sorted(participants - found),
              "coverage": "partial" if participants - found else "verified_public_partitions",
              "scope": "participants_in_input_snapshot", "profile_time": "original_response_time"}
    target.put("manifests", {"key": sid + ":" + format_version, "snapshot_id": sid, "format_version": format_version,
        "input_snapshot_ids": [item[1] for item in sources], "result": result,
        "provenance": "derived_embedded_profiles_no_new_HTTP"})
    target.db.commit()
    print(f"Players: completed profiles={len(found)}/{len(participants)} unresolved={len(participants-found)} no profile HTTP requests", flush=True)
    return result


def extract(state, conf, kind):
    conf = validate_conf(conf)
    fmt = version(conf)
    if kind == 'wr_history' and fmt != V3:
        raise ValueError('Separate WR DAG requires entity-v3')
    root = location(state, conf, kind)
    completed = root / "extracted.json"
    if completed.exists():
        print(f"Collect: {kind} already extracted; reusing immutable package", flush=True)
        reference = read(completed)
        if reference.get("format_version") != fmt:
            raise ValueError('Snapshot cannot change its ODS format during resume')
        return reference
    sid = snapshot_id(conf, kind)
    output = root / "entity-package"
    role = load_role(kind)
    if kind == "players":
        source = EntityPackage(location(state, conf, "runs") / "entity-package")
        target = EntityPackage(output)
        extra = EntityPackage(location(state,conf,'wr_history')/'entity-package') if fmt==V3 else None
        try:
            profiles(source, target, sid, snapshot_id(conf, "runs"),
                     [(extra,snapshot_id(conf,'wr_history'))] if extra else (),fmt)
        finally:
            source.db.close(); target.db.close()
            if extra:extra.db.close()
    else:
        client = Client(state, conf["collection_id"], sid, budget=conf["request_budget"],
                        adaptive_rate=conf.get("adaptive_rate", False), deadline=conf.get("deadline"))
        if kind == "games_metadata":
            if conf["scope"] == "catalog":
                from .catalog import collect_catalog
                result = collect_catalog(client, root)
            elif conf["scope"] == "ranking":
                from .ranking import rank_games
                result = rank_games(client, conf["top_n"])
            else:
                result = metadata(client, conf)
        elif kind == 'runs':
            source = EntityPackage(location(state, conf, "games_metadata") / "entity-package")
            try:
                result = collect_runs(client, root, source)
            except BudgetExceeded as error:
                if conf.get("repair_from_snapshot_id"):
                    raise  # A stopped repair must not seal/promote inherited coverage.
                result = {"coverage": "partial", "stop_reason": str(error),
                          "partitions": [read(p) for p in (root / "checkpoints").glob("*.json")]}
            finally:
                source.db.close()
        elif kind == 'wr_history':
            from .wr import collect_wr
            source = EntityPackage(location(state,conf,'games_metadata')/'entity-package')
            runs_source = EntityPackage(location(state,conf,'runs')/'entity-package')
            try:
                upstream = next(runs_source.records('manifests')).get('source_result') or {}
                result = ({'coverage':'not_checked','diagnostic':'upstream_stopped','stop_reason':upstream['stop_reason']}
                          if upstream.get('stop_reason') else collect_wr(client,root,source,conf))
            finally:
                source.db.close(); runs_source.db.close()
        else:
            raise ValueError('Unknown extraction kind')
        base_root = (Path(state) / "snapshots" / conf["repair_from_snapshot_id"] / "entity-package"
                     if kind == "runs" and conf.get("repair_from_snapshot_id") else None)
        runs_source = EntityPackage(location(state,conf,'runs')/'entity-package') if kind=='wr_history' else None
        inputs = ([snapshot_id(conf,'games_metadata')] if kind=='runs' else
                  [snapshot_id(conf,'games_metadata'),snapshot_id(conf,'runs')] if kind=='wr_history' else
                  [conf['selection_snapshot_id']] if conf.get('selection_snapshot_id') else
                  [conf['catalog_snapshot_id']] if conf.get('catalog_snapshot_id') else [])
        try:
            build(root, output, resume=True, source_result=result, load_role=role, base_root=base_root,
                  format_version=fmt,runs_input=runs_source,input_snapshot_ids=inputs if fmt==V3 else None)
        finally:
            if runs_source:runs_source.db.close()
    package = EntityPackage(output, role=role)
    summary = package.summary()
    package.db.close()
    reference = {"snapshot_id": sid, "format_version": fmt, "package_uri": str(output),
                 "role": role, "digest": digest(summary)}
    if fmt == V3:
        reference['namespace'] = namespace(conf)
    atomic_write(completed, reference)
    return reference
