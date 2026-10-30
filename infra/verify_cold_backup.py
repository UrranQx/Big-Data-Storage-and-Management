"""Actually restore a cold backup into disposable, isolated Docker volumes.

Production volumes/containers are never mounted or started. No source HTTP,
Airflow scheduler, published ports, or outbound network. Deletes only the
temporary resources created by this invocation after checking their labels.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import uuid
import zipfile


ROOT = Path(__file__).resolve().parents[1]
LABEL = 'speedrun.lab1.restore-check'


def run(args, **kwargs):
    return subprocess.run(args, cwd=ROOT, check=True, **kwargs)


def output(args, **kwargs):
    return run(args, capture_output=True, text=True, **kwargs).stdout


def sha(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        while chunk := stream.read(4 * 1024**2):
            result.update(chunk)
    return result.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--backup', required=True, type=Path)
    parser.add_argument('--keep-verified-volumes', action='store_true',
                        help='Disaster recovery: keep only successfully verified copies and emit a Compose override; never overwrite originals')
    args = parser.parse_args()
    backup = args.backup.resolve(strict=True)
    manifest = json.loads((backup/'manifest.json').read_text(encoding='utf-8'))
    if not manifest.get('complete') or manifest.get('format') != 'speedrun-lab1-cold-backup-v1':
        parser.error('A complete project cold backup is required')
    if shutil.disk_usage(ROOT).free < 45*1024**3:
        parser.error('Require 45 GiB free on the current Docker host disk for this restore test')
    for field in ('project_zip', 'git_bundle', 'images_archive'):
        row = manifest[field]
        if sha(backup/row['file']) != row['sha256']:
            parser.error('Recovery file SHA mismatch: ' + row['file'])
    baseline = json.loads((backup/'baseline.json').read_text(encoding='utf-8-sig'))
    config = backup/'restore-check-config'
    config.mkdir(exist_ok=True)
    with zipfile.ZipFile(backup/manifest['project_zip']['file']) as archive:
        for name, target in [('.env', config/'.env'), ('infra/garage.local.toml', config/'garage.toml'),
                             ('infra/backup_inventory.py', config/'inventory.py')]:
            target.write_bytes(archive.read(name))
    values = {}
    for line in (config/'.env').read_text(encoding='utf-8-sig').splitlines():
        if line.strip() and not line.lstrip().startswith('#'):
            name, value = line.split('=', 1)
            values[name.strip()] = value.strip().strip('"').strip("'")
    env = {**os.environ, 'GARAGE_DEFAULT_ACCESS_KEY': values['S3_ACCESS_KEY'],
           'GARAGE_DEFAULT_SECRET_KEY': values['S3_SECRET_KEY'],
           'GARAGE_DEFAULT_BUCKET': 'speedrun-ods'}
    key = uuid.uuid4().hex[:12]
    prefix = 'speedrun-lab1-restorecheck-' + key
    network = prefix+'-net'
    volumes = []
    containers = []
    network_created = False
    images = {r['service']: r['image_id'] for r in manifest['services']}
    report = {'started_at': datetime.now(timezone.utc).isoformat(), 'verified': False,
              'production_untouched': True, 'network_internal': True, 'published_ports': [],
              'temporary_prefix': prefix, 'filesystems': []}
    report_path = backup/'restore-verification.json'
    def save():
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    try:
        run(['docker', 'network', 'create', '--internal', '--label', LABEL+'='+key, network], capture_output=True)
        network_created = True
        for row in manifest['volumes']:
            archive = backup/row['file']
            if sha(archive) != row['sha256']:
                raise ValueError('Archive SHA mismatch: ' + row['name'])
            name = prefix+'-'+row['name']
            run(['docker', 'volume', 'create', '--label', LABEL+'='+key, name], capture_output=True)
            volumes.append(name)
            started = time.monotonic()
            # Read the Windows bind mount once; extraction and byte comparison
            # use a temporary Linux file, removed with this helper container.
            code = ("import shutil,subprocess; shutil.copyfile('/backup/" + row['file'] + "','/tmp/volume.tar.gz'); "
                    "subprocess.run(['tar','-xzf','/tmp/volume.tar.gz','-C','/target'],check=True); "
                    "subprocess.run(['tar','-dzf','/tmp/volume.tar.gz','-C','/target'],check=True)")
            run(['docker','run','--rm','--user','0','--entrypoint','python',
                 '--mount',f'type=bind,src={backup},dst=/backup,readonly',
                 '--mount',f'type=volume,src={name},dst=/target',images['scheduler'],'-c',code])
            checked = {'name':row['name'], 'sha256_verified':True, 'extracted_and_tar_compared':True,
                       'seconds':round(time.monotonic()-started,2)}
            report['filesystems'].append(checked)
            save(); print(json.dumps(checked),flush=True)
        def service(name, image, mounts, command):
            container = prefix+'-'+name
            argv = ['docker','run','-d','--name',container,'--label',LABEL+'='+key,
                    '--network',network,'--network-alias',name,'--memory','768m',
                    '--env-file',str(config/'.env')]
            for mount in mounts:
                argv += ['--mount',mount]
            if name == 'garage':
                argv += ['--env','GARAGE_DEFAULT_ACCESS_KEY','--env','GARAGE_DEFAULT_SECRET_KEY',
                         '--env','GARAGE_DEFAULT_BUCKET']
            run([*argv,image,*command],env=env,capture_output=True)
            containers.append(container)
        service('postgres',images['postgres'],[f'type=volume,src={prefix}-postgres-data,dst=/var/lib/postgresql/data'],[])
        service('mongo',images['mongo'],[f'type=volume,src={prefix}-mongo-data,dst=/data/db'],
                ['mongod','--wiredTigerCacheSizeGB','0.25'])
        service('garage',images['garage'],[
            f'type=volume,src={prefix}-garage-meta,dst=/var/lib/garage/meta',
            f'type=volume,src={prefix}-garage-data,dst=/var/lib/garage/data',
            f'type=bind,src={config / "garage.toml"},dst=/etc/garage.toml,readonly'],
                ['/garage','server','--single-node','--default-bucket'])
        for attempt in range(24):
            probe = subprocess.run(['docker','exec',prefix+'-postgres','pg_isready','-U','postgres'],capture_output=True)
            if probe.returncode == 0:
                break
            time.sleep(2)
        else:
            raise RuntimeError('Restored PostgreSQL did not become ready')
        # Mongo/Garage also need a short readiness period. Retry the read-only
        # probe on startup connection failures; comparisons happen only on success.
        argv = ['docker','run','--rm','--network',network,'--env-file',str(config/'.env'),
                '--env','PGHOST=postgres','--env','PGDATABASE=speedrun_ods','--env','PGUSER=speedrun_ods',
                '--env','PYTHONPATH=/opt/project/src',
                '--mount',f'type=bind,src={ROOT / "src"},dst=/opt/project/src,readonly',
                '--mount',f'type=bind,src={config / "inventory.py"},dst=/check/inventory.py,readonly',
                '--entrypoint','python',images['scheduler'],'/check/inventory.py']
        for attempt in range(8):
            result = subprocess.run(argv,cwd=ROOT,capture_output=True,text=True)
            if result.returncode == 0:
                restored = json.loads(result.stdout)
                break
            time.sleep(3)
        else:
            raise RuntimeError('Restored inventory failed: ' + result.stderr[-1500:])
        fields = ['postgres_counts','postgres_samples','mongo_counts','mongo_samples','run_ids','player_ids',
                  'receipts','queue_states','popular_queue_order','s3','airflow_active_runs','airflow_active_tasks']
        mismatches = [field for field in fields if restored[field] != baseline[field]]
        report['checked_fields'] = fields
        report['mismatches'] = mismatches
        report['restored_inventory'] = restored
        if mismatches:
            raise ValueError('Restored inventory differs: ' + ', '.join(mismatches))
        report.update(verified=True, finished_at=datetime.now(timezone.utc).isoformat())
        save()
        manifest['restore_verified'] = True
        manifest['restore_verification_file'] = report_path.name
        (backup/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
        print('RESTORE VERIFIED: all six filesystems and all three storage services',flush=True)
    finally:
        # Only explicitly created resources carrying this invocation's unique label.
        for name in reversed(containers):
            item = json.loads(output(['docker','inspect',name]))[0]
            if not name.startswith(prefix+'-') or item['Config']['Labels'].get(LABEL) != key:
                raise ValueError('Refusing cleanup of an unrelated container')
            run(['docker','stop','--time','60',name],capture_output=True)
            run(['docker','rm','-v',name],capture_output=True)
        keep = args.keep_verified_volumes and report['verified']
        for name in reversed(volumes):
            item = json.loads(output(['docker','volume','inspect',name]))[0]
            if not name.startswith(prefix+'-') or item['Labels'].get(LABEL) != key:
                raise ValueError('Refusing cleanup of an unrelated volume')
            if not keep:
                run(['docker','volume','rm',name],capture_output=True)
        if network_created:
            item = json.loads(output(['docker','network','inspect',network]))[0]
            if item['Labels'].get(LABEL) != key:
                raise ValueError('Refusing cleanup of an unrelated network')
            run(['docker','network','rm',network],capture_output=True)
        report['temporary_resources_removed'] = not keep
        if keep:
            override = {'name':prefix+'-recovered',
                        'services':{name:{'image':image} for name,image in images.items()},
                        'volumes':{row['name']:{'external':True, 'name':prefix+'-'+row['name']}
                                   for row in manifest['volumes']}}
            # JSON is valid YAML. Keep credentials in the backed-up .env, not here.
            override_path = backup/'recovered-volumes.compose.yaml'
            override_path.write_text(json.dumps(override,indent=2),encoding='utf-8')
            report['recovery_compose_file'] = str(override_path)
            report['restored_volumes_kept'] = volumes
            print('Verified copies kept. Compose override: ' + str(override_path),flush=True)
        save()


if __name__ == '__main__':
    main()
