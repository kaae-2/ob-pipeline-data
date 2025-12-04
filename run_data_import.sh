#!/usr/bin/env bash
set -euo pipefail

# Run data_import.py with the requested parameters.
script_dir="$(cd -- "$(dirname -- "$0")" && pwd)"

python "${script_dir}/data_import.py" \
  --dataset_name "covid" \
  --name "data_import" \
  --output_dir "${script_dir}/out/data/data_import"
