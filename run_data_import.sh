#!/usr/bin/env bash
set -euo pipefail

# Run data_import.py with the requested parameters.
script_dir="$(cd -- "$(dirname -- "$0")" && pwd)"
repo_dir="$(cd -- "${script_dir}/.." && pwd)"

DATASET_NAME="${1:-FR-FCM-Z3YR}"
if [[ $# -gt 0 ]]; then
  shift
fi

# Use the dataset name as the output 'name' so produced tarball is `<name>.data.tar.gz`.
python "${script_dir}/data_import.py" \
  --dataset_name "${DATASET_NAME}" \
  --name "${DATASET_NAME}" \
  --seed "42" \
  --prepared-root "${repo_dir}/datasets/prepared" \
  "$@" \
  --output_dir "${script_dir}/out/data/data_import"

"${script_dir}/verify_data_import.sh" \
  "${script_dir}/out/data/data_import/${DATASET_NAME}.data.tar.gz" \
  "${script_dir}/out/data/data_import/${DATASET_NAME}.metadata.json.gz"
