from __future__ import annotations

import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / 'data_import.py'
SPEC = importlib.util.spec_from_file_location('data_import_contract', MODULE_PATH)
data_import = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(data_import)

REVISION = 'a' * 40
WHOLE = 'prepared/cytof/FR-FCM-Z3YR/StimBlood_cytof/sample.csv.zst'


def item(path: str, kind: str | None = None) -> dict:
    if kind is None:
        if path.endswith('.sha256'):
            kind = 'sha'
        elif '.part' in path:
            kind = 'part'
        else:
            kind = 'data'
    return {
        'name': Path(path).name,
        'repo_path': path,
        'kind': kind,
        'platform': 'cytof',
        'dataset': 'FR-FCM-Z3YR',
        'shortname': 'StimBlood_cytof',
        'size': 3,
        'blob_sha': hashlib.sha1(path.encode()).hexdigest(),
    }


class PreparedRepresentationTests(unittest.TestCase):
    def test_whole_file_continues_unchanged(self) -> None:
        samples = data_import._resolve_prepared_samples(
            [item(WHOLE), item(f'{WHOLE}.sha256')]
        )

        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0]['representation'], 'whole')
        self.assertEqual(samples[0]['source_objects'][0]['repo_path'], WHOLE)

    def test_successful_multipart_is_ordered_and_stream_assembled(self) -> None:
        files = [
            item(f'{WHOLE}.part0001'),
            item(f'{WHOLE}.sha256'),
            item(f'{WHOLE}.part0000'),
        ]
        sample = data_import._resolve_prepared_samples(files)[0]
        self.assertEqual(
            [part['repo_path'] for part in sample['source_objects']],
            [f'{WHOLE}.part0000', f'{WHOLE}.part0001'],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            parts = [root / 'part0', root / 'part1']
            parts[0].write_bytes(b'abc')
            parts[1].write_bytes(b'def')
            checksum = root / 'sample.sha256'
            checksum.write_text(f'{hashlib.sha256(b"abcdef").hexdigest()}  sample\n')
            target = root / 'assembled' / 'sample.csv.zst'

            data_import._assemble_split_parts(parts, target, checksum)

            self.assertEqual(target.read_bytes(), b'abcdef')
            self.assertFalse(target.with_name(f'{target.name}.partial').exists())

    def test_missing_part_fails_closed(self) -> None:
        files = [
            item(f'{WHOLE}.part0000'),
            item(f'{WHOLE}.part0002'),
            item(f'{WHOLE}.sha256'),
        ]

        with self.assertRaisesRegex(ValueError, 'noncontiguous'):
            data_import._resolve_prepared_samples(files)

    def test_corrupt_assembled_checksum_removes_partial_and_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            part = root / 'part0'
            part.write_bytes(b'abc')
            checksum = root / 'sample.sha256'
            checksum.write_text(f'{hashlib.sha256(b"xyz").hexdigest()}  sample\n')
            target = root / 'assembled' / 'sample.csv.zst'

            with self.assertRaisesRegex(ValueError, 'SHA256 mismatch'):
                data_import._assemble_split_parts([part], target, checksum)

            self.assertFalse(target.exists())
            self.assertFalse(target.with_name(f'{target.name}.partial').exists())

    def test_whole_and_parts_are_ambiguous(self) -> None:
        files = [
            item(WHOLE),
            item(f'{WHOLE}.part0000'),
            item(f'{WHOLE}.sha256'),
        ]

        with self.assertRaisesRegex(ValueError, 'ambiguous'):
            data_import._resolve_prepared_samples(files)

    def test_orphan_part_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, 'orphan'):
            data_import._resolve_prepared_samples([item(f'{WHOLE}.part0000')])

    def test_actual_stimblood_part_naming_is_supported(self) -> None:
        actual = (
            'prepared/cytof/FR-FCM-Z3YR/StimBlood_cytof/'
            '181017_reference_tube_day1_01.csv.zst'
        )
        samples = data_import._resolve_prepared_samples(
            [
                item(f'{actual}.part0000'),
                item(f'{actual}.part0001'),
                item(f'{actual}.sha256'),
            ]
        )

        self.assertEqual(samples[0]['repo_path'], actual)
        self.assertEqual(samples[0]['representation'], 'split')


class RevisionProvenanceTests(unittest.TestCase):
    def test_mutable_short_and_uppercase_revisions_are_rejected(self) -> None:
        for revision in ('main', '2338a62', 'A' * 40):
            with self.subTest(revision=revision):
                with self.assertRaisesRegex(ValueError, 'full 40-character'):
                    data_import._require_dataset_revision(revision)

    def test_cache_is_invalidated_by_dataset_revision(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_path = Path(tmpdir) / 'dataset.data.tar.gz'
            data_path.write_bytes(b'archive')
            manifest_path = data_import._import_manifest_path(str(data_path))
            manifest_path.write_text(
                json.dumps(
                    {
                        'dataset_name': 'FR-FCM-Z3YR',
                        'dataset_revision': REVISION,
                        'source_manifest': {'identity': 'one'},
                        'transformation_cofactor': None,
                        'feature_cofactors': None,
                        'source_checksums': {'sample': 'abc'},
                        'metadata_payload': {
                            'samples': {'sample_names': ['sample.csv']}
                        },
                    }
                )
            )

            reused = data_import._reuse_packaged_dataset_if_valid(
                'FR-FCM-Z3YR',
                str(data_path),
                None,
                {'sample': 'abc'},
                dataset_revision=REVISION,
                source_manifest={'identity': 'one'},
            )
            invalidated = data_import._reuse_packaged_dataset_if_valid(
                'FR-FCM-Z3YR',
                str(data_path),
                None,
                {'sample': 'abc'},
                dataset_revision='b' * 40,
                source_manifest={'identity': 'one'},
            )

            self.assertIsNotNone(reused)
            self.assertIsNone(invalidated)


if __name__ == '__main__':
    unittest.main()
