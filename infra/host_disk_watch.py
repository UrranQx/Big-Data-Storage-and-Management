"""Run on the Docker host. Publish real disk headroom every 30 seconds, no source HTTP."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import time


PUBLISH = """import sys
from pathlib import Path
from speedrun_etl.jsonutil import atomic_write, loads
atomic_write(Path('/opt/airflow/state/safety/host-disk.json'), loads(sys.stdin.read()))
"""


def measure(path, minimum_gib):
    path = Path(path).resolve(strict=True)
    disk = shutil.disk_usage(path)
    return {"measured_at_unix": time.time(), "path": str(path),
            "total_bytes": disk.total, "free_bytes": disk.free,
            "minimum_free_bytes": int(minimum_gib * 1024**3), "producer": "host_disk_watch_v1"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--path', type=Path, default=Path.home()/'AppData/Local/Docker/wsl/disk/docker_data.vhdx',
                        help='Existing Docker VHDX or Docker storage filesystem on the host')
    parser.add_argument('--min-free-gib', type=float, default=20,
                        help='Real host disk reserve; >=10 GiB, default 20 GiB')
    parser.add_argument('--interval', type=float, default=30)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    if not args.min_free_gib >= 10 or not 5 <= args.interval <= 60:
        parser.error('Use a reserve >=10 GiB and interval 5..60 seconds')
    root = Path(__file__).resolve().parents[1]
    # Validate the actual host path before entering the loop; never substitute Docker virtual free space.
    measure(args.path, args.min_free_gib)
    while True:
        try:
            report = measure(args.path, args.min_free_gib)
            result = subprocess.run(['docker','compose','exec','-T','scheduler','python','-c',PUBLISH],
                                    input=json.dumps(report), text=True, cwd=root,
                                    capture_output=True, timeout=20)
            if result.returncode:
                raise RuntimeError('Docker host disk sample could not be delivered')
            print(time.strftime('%Y-%m-%d %H:%M:%S'),
                  f"host free={report['free_bytes']/1024**3:.1f} GiB reserve={args.min_free_gib:g} GiB", flush=True)
        except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
            print(type(error).__name__, str(error), flush=True)
            if args.once:
                raise
        if args.once:
            return
        time.sleep(args.interval)


if __name__ == '__main__':
    main()
