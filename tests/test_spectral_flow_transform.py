import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd
import zstandard


MODULE_PATH = Path(__file__).parents[1] / 'data_import.py'
SPEC = importlib.util.spec_from_file_location('data_import', MODULE_PATH)
data_import = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(data_import)


class SpectralFlowTransformTests(unittest.TestCase):
    def test_publication_marker_panel_and_cofactors(self):
        self.assertEqual(
            data_import.SPECTRAL_FLOW_15723074_COFACTORS,
            {
                'CD14': 10000.0,
                'CD19': 1000.0,
                'CD3': 3000.0,
                'CD56': 2000.0,
                'CD45RA': 4000.0,
                'CD8': 3000.0,
                'CD4': 5000.0,
                'CCR7': 6000.0,
            },
        )

    def test_materialization_selects_and_transforms_publication_markers(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            csv_bytes = (
                'extra,CD14,CD19,CD3,CD56,CD45RA,CD8,CD4,CCR7,label\n'
                '99,10000,1000,3000,2000,4000,3000,5000,6000,B cells\n'
            ).encode()
            compressed = zstandard.ZstdCompressor().compress(csv_bytes)
            archive = root / 'sample.csv.zst'
            checksum = root / 'sample.csv.zst.sha256'
            archive.write_bytes(compressed)
            checksum.write_text(
                f'{hashlib.sha256(compressed).hexdigest()}  {archive.name}\n'
            )

            output, summary = data_import._materialize_prepared_csv(
                'sample',
                archive,
                checksum,
                tmpdir,
                zstandard,
                feature_cofactors=data_import.SPECTRAL_FLOW_15723074_COFACTORS,
            )
            frame = pd.read_csv(output)

            self.assertEqual(list(frame.columns), [
                *data_import.SPECTRAL_FLOW_15723074_COFACTORS,
                'label',
            ])
            np.testing.assert_allclose(frame.iloc[0, :8].astype(float), np.arcsinh(1.0))
            self.assertEqual(summary['n_variables'], 8)

    def test_dataset_metadata_and_cache_include_publication_transform(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            sample_dir = root / 'prepared' / 'spectral' / '15723074' / 'sample'
            sample_dir.mkdir(parents=True)
            csv_bytes = (
                'extra,CD14,CD19,CD3,CD56,CD45RA,CD8,CD4,CCR7,label\n'
                '99,10000,1000,3000,2000,4000,3000,5000,6000,B cells\n'
            ).encode()
            compressed = zstandard.ZstdCompressor().compress(csv_bytes)
            archive = sample_dir / 'sample.csv.zst'
            checksum = sample_dir / 'sample.csv.zst.sha256'
            archive.write_bytes(compressed)
            checksum.write_text(
                f'{hashlib.sha256(compressed).hexdigest()}  {archive.name}\n'
            )
            data_path = root / 'output' / 'dataset.data.tar.gz'

            downloaded = data_import._download_prepared_dataset(
                '15723074',
                str(data_path),
                prepared_root=str(root / 'prepared'),
            )

            self.assertIsNotNone(downloaded)
            assert downloaded is not None
            _, metadata = downloaded
            self.assertEqual(metadata['dataset']['n_variables'], 8)
            self.assertEqual(
                metadata['dataset']['selected_features'],
                list(data_import.SPECTRAL_FLOW_15723074_COFACTORS),
            )
            self.assertEqual(
                metadata['dataset']['feature_cofactors'],
                data_import.SPECTRAL_FLOW_15723074_COFACTORS,
            )
            manifest_path = data_import._import_manifest_path(str(data_path))
            manifest = json.loads(manifest_path.read_text())
            self.assertEqual(
                manifest['feature_cofactors'],
                data_import.SPECTRAL_FLOW_15723074_COFACTORS,
            )

            with mock.patch.object(
                data_import,
                '_materialize_prepared_csv',
                side_effect=AssertionError('valid cache should be reused'),
            ):
                reused = data_import._download_prepared_dataset(
                    '15723074',
                    str(data_path),
                    prepared_root=str(root / 'prepared'),
                )
            self.assertIsNotNone(reused)
            assert reused is not None
            self.assertEqual(reused[0], [Path('sample.csv')])
            self.assertEqual(reused[1]['dataset'], metadata['dataset'])

            manifest.pop('feature_cofactors')
            manifest_path.write_text(json.dumps(manifest))
            self.assertIsNone(
                data_import._reuse_packaged_dataset_if_valid(
                    '15723074',
                    str(data_path),
                    None,
                    manifest['source_checksums'],
                    data_import.SPECTRAL_FLOW_15723074_COFACTORS,
                )
            )


if __name__ == '__main__':
    unittest.main()
