# Data Repository

## What this module does

This module imports prepared datasets and packages them for the benchmark.

- Main entrypoint: `data_import.py`
- Local runner: `run_data_import.sh`
- Output artifacts:
  - `{dataset}.data.tar.gz`
  - `{dataset}.order.json.gz`
  - `{dataset}.attachments.gz`

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
