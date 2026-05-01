#!/usr/bin/env bash
set -euo pipefail

archive="${1:-out/data/data_import/data_import.data.tar.gz}"
metadata_file="${2:-${archive%.data.tar.gz}.metadata.json.gz}"

if [[ ! -f "$archive" ]]; then
  echo "Archive not found: $archive" >&2
  exit 1
fi

echo "Listing CSV files in $archive..."
tar -tzf "$archive" | tee /tmp/prepared_csv_list.txt

echo
echo "Summary:"
echo "Total entries: $(wc -l < /tmp/prepared_csv_list.txt)"
echo "Non-CSV entries (should be 0): $(grep -v -i '\.csv$' /tmp/prepared_csv_list.txt | wc -l)"

if [[ -f "$metadata_file" ]]; then
  echo
  echo "Metadata summary:"
  python - "$metadata_file" <<'PY'
import gzip
import json
import sys

with gzip.open(sys.argv[1], 'rt', encoding='utf-8') as handle:
    payload = json.load(handle)

dataset = payload.get('dataset', payload.get('metadata', {}))
samples = payload.get('samples', {})
print(f"dataset_name: {dataset.get('dataset_name')}")
print(f"sub_sampling: {dataset.get('sub_sampling')}")
print(f"transformation_cofactor: {dataset.get('transformation_cofactor')}")
print(f"potential_batches: {dataset.get('potential_batches')}")
print(f"sample_count: {samples.get('sample_count')}")
PY
fi
