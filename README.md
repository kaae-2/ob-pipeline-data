# Data Repository

## What this module does

This module imports prepared datasets and packages them for the benchmark.

- Main entrypoint: `data_import.py`
- Local runner: `run_data_import.sh`
- Output artifacts:
  - `{dataset}.data.tar.gz`
  - `{dataset}.order.json.gz`
  - `{dataset}.attachments.gz`

## Prepared dataset contract

`data_import.py` resolves files from the dataset repository by traversing:

`prepared/<platform>/<dataset_name>/<shortname>/`

where:

- `platform` is `cytof` or `fcm`
- `dataset_name` is the CLI `--dataset_name` value
- `shortname` is a compact cohort/abbreviation label

The importer currently expects:

- data files: `*.csv.zst`
- checksum files: `*.csv.zst.sha256`

Validation rules at import time:

- For a given `--dataset_name`, exactly one `platform` must be present.
- For a given `--dataset_name`, exactly one `shortname` (abbreviation) must be present.
- Import fails fast if either condition is violated.

For each selected dataset, the importer verifies checksums, decompresses `.csv.zst` to CSV, and packages CSVs into `{name}.data.tar.gz`.

The generated order metadata includes dataset-derived fields such as:

- `platform` (single required value)
- `expected_abbreviation` (single required value, derived from shortname)
- `platforms` and `expected_abbreviations` are also emitted for compatibility and contain one item.

## Run locally

From repo root:

```bash
bash data/run_data_import.sh
```

Or call the CLI directly:

```bash
python data/data_import.py --dataset_name FR-FCM-Z3YR --name FR-FCM-Z3YR --seed 42 --output_dir data/out/data/data_import
```

## Run as part of benchmark

The benchmark data stage calls this module from `benchmark/Clustering_conda.yml`.
You usually run it through:

```bash
just benchmark
```

## What `run_data_import.sh` needs

- Python environment with dependencies from the module/benchmark env
- Network access to the prepared dataset repository
- Writable output path under `data/out/...`
- Enough disk for extracted/repacked archives
