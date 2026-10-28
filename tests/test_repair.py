from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from speedrun_etl.entity_ods import EntityPackage
from speedrun_etl.jsonutil import atomic_write
from speedrun_etl.repair import validate_base,finish_repair


class RepairTests(unittest.TestCase):
    def test_all_missing_base_sections_must_be_selected_and_base_digest_confirmed(self):
        with TemporaryDirectory() as directory:
            root=Path(directory)/'snapshots/base-runs'
            atomic_write(root/'extracted.json',{'snapshot_id':'base-runs','role':'runs','format_version':'entity-v2','digest':'d'})
            package=EntityPackage(root/'entity-package')
            result={'game_id':'g','category_ids':['c'],'partitions':[
                {'category_id':'c','status_code':status,'coverage':'partial' if status else 'verified_public_partitions'}
                for status in (0,1,2)]}
            package.put('manifests',{'key':'m','source_result':result});package.db.commit();package.db.close()
            conf={'repair_from_snapshot_id':'base-runs','game_id':'g',
                  'repair_partitions':[{'category_id':'c','status_code':s} for s in (1,2)]}
            with patch('speedrun_etl.repair.ready') as ready:
                self.assertEqual(result,validate_base(directory,conf))
                ready.assert_called_with('base-runs',role='runs',expected='d')
                with self.assertRaisesRegex(ValueError,'every incomplete'):
                    validate_base(directory,{**conf,'repair_partitions':conf['repair_partitions'][:1]})
                with self.assertRaisesRegex(ValueError,'Wrong game'):
                    validate_base(directory,{**conf,'game_id':'other'})

    def test_missing_store_receipt_cannot_promote_queue_entry(self):
        with TemporaryDirectory() as directory:
            for kind in ('games_metadata','runs','players'):
                atomic_write(Path(directory)/'snapshots'/('repair-'+kind)/'extracted.json',
                    {'snapshot_id':'repair-'+kind,'role':'runs' if kind=='runs' else 'all','digest':'digest'})
            with patch('speedrun_etl.repair.ready',side_effect=[{},RuntimeError('missing third store')]),patch(
                    'speedrun_etl.repair.pg_connect') as db:
                with self.assertRaisesRegex(RuntimeError,'missing third'):
                    finish_repair(directory,'batch','g','repair','base')
                db.assert_not_called()
