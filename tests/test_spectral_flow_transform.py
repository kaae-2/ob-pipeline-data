import hashlib
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd
import zstandard

MODULE_PATH = Path(__file__).parents[1] / "data_import.py"
SPEC = importlib.util.spec_from_file_location("data_import", MODULE_PATH)
data_import = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(data_import)


def commit_prepared_tree(root: Path) -> str:
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=root, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.invalid"], cwd=root, check=True
    )
    subprocess.run(["git", "add", "prepared"], cwd=root, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "fixture"], cwd=root, check=True)
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


class SpectralFlowTransformTests(unittest.TestCase):
    def test_cli_defaults_to_fcm_cofactor_150(self):
        with mock.patch.object(
            sys,
            "argv",
            [
                "data_import.py",
                "--dataset_name",
                "dataset",
                "--dataset-revision",
                "a" * 40,
                "--seed",
                "42",
            ],
        ):
            args = data_import.parse_args()

        self.assertEqual(args.transformation_cofactor, 150.0)

    def test_materialization_applies_generic_cofactor(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            csv_bytes = "CD14,CD19,label\n150,300,B cells\n".encode()
            compressed = zstandard.ZstdCompressor().compress(csv_bytes)
            archive = root / "sample.csv.zst"
            checksum = root / "sample.csv.zst.sha256"
            archive.write_bytes(compressed)
            checksum.write_text(
                f"{hashlib.sha256(compressed).hexdigest()}  {archive.name}\n"
            )

            output, summary = data_import._materialize_prepared_csv(
                "sample",
                archive,
                checksum,
                tmpdir,
                zstandard,
                transformation_cofactor=150.0,
            )
            frame = pd.read_csv(output)

            np.testing.assert_allclose(
                frame.iloc[0, :2].astype(float), np.arcsinh([1.0, 2.0])
            )
            self.assertEqual(summary["n_variables"], 2)

    def test_dataset_metadata_and_cache_include_generic_transform(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            sample_dir = root / "prepared" / "fcm" / "dataset" / "sample"
            sample_dir.mkdir(parents=True)
            csv_bytes = "CD14,CD19,label\n150,300,B cells\n".encode()
            compressed = zstandard.ZstdCompressor().compress(csv_bytes)
            archive = sample_dir / "sample.csv.zst"
            checksum = sample_dir / "sample.csv.zst.sha256"
            archive.write_bytes(compressed)
            checksum.write_text(
                f"{hashlib.sha256(compressed).hexdigest()}  {archive.name}\n"
            )
            revision = commit_prepared_tree(root)
            data_path = root / "output" / "dataset.data.tar.gz"

            downloaded = data_import._download_prepared_dataset(
                "dataset",
                str(data_path),
                revision,
                transformation_cofactor=150.0,
                prepared_root=str(root / "prepared"),
            )

            self.assertIsNotNone(downloaded)
            assert downloaded is not None
            _, metadata = downloaded
            self.assertEqual(metadata["dataset"]["n_variables"], 2)
            self.assertEqual(metadata["dataset"]["transformation_cofactor"], 150.0)
            self.assertNotIn("selected_features", metadata["dataset"])
            manifest_path = data_import._import_manifest_path(str(data_path))
            manifest = json.loads(manifest_path.read_text())
            self.assertEqual(manifest["transformation_cofactor"], 150.0)

            with mock.patch.object(
                data_import,
                "_materialize_prepared_csv",
                side_effect=AssertionError("valid cache should be reused"),
            ):
                reused = data_import._download_prepared_dataset(
                    "dataset",
                    str(data_path),
                    revision,
                    transformation_cofactor=150.0,
                    prepared_root=str(root / "prepared"),
                )
            self.assertIsNotNone(reused)


if __name__ == "__main__":
    unittest.main()
