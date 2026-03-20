#!/usr/bin/env bash
set -euo pipefail

archive="${1:-out/data/data_import/data_import.data.tar.gz}"
order_file="${2:-${archive%.data.tar.gz}.order.json.gz}"

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

if [[ -f "$order_file" ]]; then
  echo
  echo "Order metadata:"
  python - "$order_file" <<'PY'
import gzip
import json
import sys

with gzip.open(sys.argv[1], 'rt', encoding='utf-8') as handle:
    payload = json.load(handle)

metadata = payload.get('metadata', {})
print(f"dataset_name: {metadata.get('dataset_name')}")
print(f"sub_sampling: {metadata.get('sub_sampling')}")
print(f"transformation_cofactor: {metadata.get('transformation_cofactor')}")
print(f"potential_batches: {metadata.get('potential_batches')}")
PY
fi
