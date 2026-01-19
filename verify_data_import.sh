#!/usr/bin/env bash
set -euo pipefail

archive="${1:-out/data/data_import/data_import.data.gz}"

if [[ ! -f "$archive" ]]; then
  echo "Archive not found: $archive" >&2
  exit 1
fi

echo "Listing FCS files in $archive..."
tar -tzf "$archive" | tee /tmp/covid_fcs_list.txt

echo
echo "Summary:"
echo "Total entries: $(wc -l < /tmp/covid_fcs_list.txt)"
echo "Non-FCS entries (should be 0): $(grep -v -i '\.fcs$' /tmp/covid_fcs_list.txt | wc -l)"
