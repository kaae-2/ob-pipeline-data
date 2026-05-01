import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import random
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

# Base URL for raw downloads (GitHub raw endpoint via github.com)
BASE_URL = "https://github.com/kaae-2/ob-flow-datasets/raw/main"

LABEL_COLUMN_CANDIDATES = (
    "label",
    "population",
    "cell_type",
    "celltype",
    "cluster",
    "cluster_id",
)

DOWNLOAD_CHUNK_SIZE = 8 * 1024 * 1024
DOWNLOAD_MAX_WORKERS = 8
NORMALIZE_MAX_WORKERS = 4
METADATA_MAX_WORKERS = 4
TRANSFORM_CHUNK_ROWS = 100_000
DATA_TAR_GZIP_COMPRESSLEVEL = 1
DOWNLOAD_RETRIES = 3
DOWNLOAD_RETRY_BASE_SECONDS = 1.0
UNLABELED_VALUES = {"", "unlabeled", "ungated", "debris", "unknown", "other", "noise"}
IMPORT_MANIFEST_SUFFIX = ".manifest.json"


def _copy_json_object(value):
    return json.loads(json.dumps(value))


def _populate_metadata_legacy_aliases(payload: dict) -> dict:
    dataset = payload.get("dataset")
    samples = payload.get("samples")
    labels = payload.get("labels")
    stages = payload.get("stages")

    legacy_metadata = dict(dataset) if isinstance(dataset, dict) else {}
    if isinstance(samples, dict):
        for key in ("sample_names", "cells_per_sample", "sample_count"):
            if key in samples:
                legacy_metadata[key] = _copy_json_object(samples[key])
    if isinstance(stages, dict):
        stratify = stages.get("stratify")
        if isinstance(stratify, dict) and "stratification" in stratify:
            legacy_metadata["stratification"] = _copy_json_object(
                stratify["stratification"]
            )

    payload["metadata"] = legacy_metadata
    if isinstance(samples, dict) and "order" in samples:
        payload["order"] = _copy_json_object(samples["order"])
    if isinstance(labels, dict):
        if "id_to_label" in labels:
            payload["id_to_label"] = _copy_json_object(labels["id_to_label"])
        if "label_to_id" in labels:
            payload["label_to_id"] = _copy_json_object(labels["label_to_id"])
    return payload


def _build_metadata_payload(
    dataset_metadata: dict,
    dataset_name: str,
    order: list[int],
    seed: int,
    sub_sampling: int,
    potential_batches: Optional[int],
) -> dict:
    dataset = dict(dataset_metadata)
    dataset["dataset_name"] = dataset_name
    dataset["sub_sampling"] = sub_sampling
    dataset["potential_batches"] = potential_batches

    payload = {
        "schema_version": 1,
        "dataset": dataset,
        "samples": {
            "order": [int(item) for item in order],
            "sample_names": _copy_json_object(dataset_metadata.get("sample_names", [])),
            "cells_per_sample": _copy_json_object(
                dataset_metadata.get("cells_per_sample", [])
            ),
            "sample_count": int(dataset_metadata.get("sample_count", len(order))),
        },
        "labels": {
            "non_target_aliases": sorted(UNLABELED_VALUES),
        },
        "stages": {
            "data_import": {
                "seed": int(seed),
            }
        },
    }
    return _populate_metadata_legacy_aliases(payload)


def download_file(
    url: str, dest_path: str, chunk_size: int = DOWNLOAD_CHUNK_SIZE
) -> bool:
    if not url or not dest_path:
        raise ValueError("Both url and dest_path must be provided.")

    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            with (
                urllib.request.urlopen(url) as response,
                open(dest_path, "wb") as out_file,
            ):
                while chunk := response.read(chunk_size):
                    out_file.write(chunk)
            print(f"Downloaded {url} -> {dest_path}")
            return True
        except urllib.error.HTTPError as e:
            should_retry = e.code in {429, 500, 502, 503, 504}
            print(
                f"HTTP error for {url} (attempt {attempt}/{DOWNLOAD_RETRIES}): {e.code} {e.reason}",
                file=sys.stderr,
            )
            if not should_retry:
                break
        except urllib.error.URLError as e:
            print(
                f"Network error for {url} (attempt {attempt}/{DOWNLOAD_RETRIES}): {e.reason}",
                file=sys.stderr,
            )
        except Exception as e:
            print(
                f"Unexpected error for {url} (attempt {attempt}/{DOWNLOAD_RETRIES}): {e}",
                file=sys.stderr,
            )

        try:
            if os.path.exists(dest_path):
                os.remove(dest_path)
        except OSError:
            pass

        if attempt < DOWNLOAD_RETRIES:
            delay = DOWNLOAD_RETRY_BASE_SECONDS * (2 ** (attempt - 1))
            time.sleep(delay)
    return False


def _extract_repo_info(base_url: str):
    parsed = urllib.parse.urlparse(base_url)
    parts = parsed.path.strip("/").split("/")
    if parsed.netloc == "github.com" and len(parts) >= 4 and parts[2] == "raw":
        owner, repo, _, branch = parts[:4]
        return {"owner": owner, "repo": repo, "branch": branch}
    return None


def _ensure_trailing_newline(path: Path) -> None:
    size = path.stat().st_size
    if size == 0:
        raise ValueError("CSV file is empty.")

    with open(path, "rb+") as fh:
        fh.seek(0, os.SEEK_END)
        if fh.tell() == 0:
            raise ValueError("CSV file is empty.")
        fh.seek(-1, os.SEEK_END)
        if fh.read(1) != b"\n":
            fh.seek(0, os.SEEK_END)
            fh.write(b"\n")


def _list_prepared_files(dataset_name: str) -> list[dict]:
    repo_info = _extract_repo_info(BASE_URL)
    if not repo_info:
        raise ValueError("BASE_URL must be a GitHub raw URL to list prepared files.")

    target_dataset = dataset_name.strip()

    tree_url = (
        "https://api.github.com/repos/"
        f"{repo_info['owner']}/{repo_info['repo']}/git/trees/{repo_info['branch']}"
        "?recursive=1"
    )

    try:
        with urllib.request.urlopen(tree_url) as response:
            payload = json.loads(response.read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(
            f"HTTP error while listing prepared files: {e.code} {e.reason}"
        ) from e
    except urllib.error.URLError as e:
        raise RuntimeError(
            f"Network error while listing prepared files: {e.reason}"
        ) from e
    except Exception as e:
        raise RuntimeError(f"Unexpected error while listing prepared files: {e}") from e

    tree = payload.get("tree") if isinstance(payload, dict) else None
    if not isinstance(tree, list):
        return []

    files = []
    target_dataset_l = target_dataset.lower()
    for item in tree:
        if not isinstance(item, dict) or item.get("type") != "blob":
            continue

        repo_path = item.get("path")
        if not isinstance(repo_path, str) or not repo_path.startswith("prepared/"):
            continue

        rel = repo_path[len("prepared/") :]
        parts = rel.split("/")
        if len(parts) != 4:
            continue

        platform_part, dataset_part, shortname_part, file_name = parts
        if dataset_part.lower() != target_dataset_l:
            continue

        lower = file_name.lower()
        is_data = lower.endswith(".csv.zst")
        is_sha = lower.endswith(".csv.zst.sha256")
        if not (is_data or is_sha):
            continue

        files.append(
            {
                "name": file_name,
                "repo_path": repo_path,
                "url": f"{BASE_URL}/{repo_path}",
                "kind": "sha" if is_sha else "data",
                "platform": platform_part,
                "dataset": dataset_part,
                "shortname": shortname_part,
            }
        )
    return files


def _derive_expected_abbreviations(prepared_files: list[dict]) -> list[str]:
    abbreviations = {
        str(item.get("shortname", "")).strip()
        for item in prepared_files
        if item.get("kind") == "data" and str(item.get("shortname", "")).strip()
    }
    return sorted(abbreviations)


def _expected_sha_repo_path(data_repo_path: str) -> str:
    return f"{data_repo_path}.sha256"


def _read_sha256(sha_path: Path) -> str:
    text = sha_path.read_text(encoding="utf-8").strip()
    if not text:
        raise ValueError("SHA256 file is empty.")
    return text.split()[0]


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verify_sha256(path: Path, sha_path: Path) -> None:
    expected = _read_sha256(sha_path)
    actual = _file_sha256(path)
    if actual != expected:
        raise ValueError(f"SHA256 mismatch (expected {expected}, got {actual}).")


def _find_label_index(header: list[str]) -> Optional[int]:
    lower_map = {str(col).strip().lower(): idx for idx, col in enumerate(header)}
    for candidate in LABEL_COLUMN_CANDIDATES:
        if candidate in lower_map:
            return lower_map[candidate]
    return None


def _normalize_population_value(value: object) -> str:
    if isinstance(value, (str, bytes)):
        return str(value).strip()
    if value is None or value is pd.NA:
        return ""
    if isinstance(value, (float, np.floating)) and np.isnan(value):
        return ""
    return str(value).strip()


def _scan_csv_file(path: Path) -> dict:
    _ensure_trailing_newline(path)

    with open(path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration as exc:
            raise ValueError(f"CSV file has no header row: {path.name}") from exc

        label_index = _find_label_index(header)
        n_variables = len(header) if label_index is None else len(header) - 1
        cell_count = 0
        populations: set[str] = set()

        for row in reader:
            cell_count += 1
            if label_index is None or label_index >= len(row):
                continue
            value = str(row[label_index]).strip()
            if not value or value.lower() == "unlabeled":
                continue
            populations.add(value)

    return {
        "sample_name": path.name,
        "cell_count": cell_count,
        "n_variables": n_variables,
        "populations": populations,
    }


def _build_dataset_metadata(sample_summaries: list[dict]) -> dict:
    sorted_summaries = sorted(
        sample_summaries, key=lambda summary: str(summary["sample_name"])
    )
    if not sorted_summaries:
        return {
            "sample_count": 0,
            "sample_names": [],
            "cells_per_sample": [],
            "n_variables": 0,
            "population_count": 0,
        }

    sample_names: list[str] = []
    cells_per_sample: list[int] = []
    populations: set[str] = set()
    expected_variables: Optional[int] = None

    for summary in sorted_summaries:
        n_variables = int(summary["n_variables"])
        if expected_variables is None:
            expected_variables = n_variables
        elif expected_variables != n_variables:
            raise ValueError(
                "Inconsistent variable count: "
                f"{summary['sample_name']} has {n_variables}, expected {expected_variables}."
            )

        sample_names.append(str(summary["sample_name"]))
        cells_per_sample.append(int(summary["cell_count"]))
        populations.update(summary["populations"])

    return {
        "sample_count": len(sorted_summaries),
        "sample_names": sample_names,
        "cells_per_sample": cells_per_sample,
        "n_variables": expected_variables if expected_variables is not None else 0,
        "population_count": len(populations),
    }


def _import_manifest_path(data_path: str) -> Path:
    return Path(f"{data_path}{IMPORT_MANIFEST_SUFFIX}")


def _load_import_manifest(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _write_import_manifest(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    temp_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
    )
    temp_path.replace(path)


def _reuse_packaged_dataset_if_valid(
    dataset_name: str,
    data_path: str,
    transformation_cofactor: Optional[float],
    source_checksums: dict[str, str],
) -> Optional[tuple[list[Path], dict]]:
    manifest_path = _import_manifest_path(data_path)
    manifest = _load_import_manifest(manifest_path)
    if manifest is None or not Path(data_path).exists():
        return None

    manifest_checksums = manifest.get("source_checksums")
    metadata_payload = manifest.get("metadata_payload")
    if (
        not isinstance(manifest_checksums, dict)
        or not isinstance(metadata_payload, dict)
    ):
        return None
    if manifest.get("dataset_name") != dataset_name:
        return None
    if manifest.get("transformation_cofactor") != transformation_cofactor:
        return None
    if manifest_checksums != source_checksums:
        return None

    samples = metadata_payload.get("samples")
    sample_names = samples.get("sample_names") if isinstance(samples, dict) else None
    if not isinstance(sample_names, list) or not all(
        isinstance(name, str) for name in sample_names
    ):
        return None

    print(f"Reusing existing packaged dataset at {data_path}")
    return [Path(name) for name in sample_names], metadata_payload


def _download_all(download_specs: list[tuple[dict, Path]]) -> bool:
    if not download_specs:
        return True
    max_workers = min(DOWNLOAD_MAX_WORKERS, len(download_specs))
    failed_download = False
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(download_file, item["url"], str(dest)): (item, dest)
            for item, dest in download_specs
        }
        for future in as_completed(future_map):
            item, _ = future_map[future]
            try:
                succeeded = future.result()
            except Exception as exc:
                print(
                    f"Unexpected error for {item['url']}: {exc}",
                    file=sys.stderr,
                )
                failed_download = True
                continue
            if not succeeded:
                failed_download = True
    return not failed_download


def _materialize_prepared_csv(
    base: str,
    zst_path: Path,
    sha_path: Path,
    tmpdir: str,
    zstd_module,
    transformation_cofactor: Optional[float] = None,
) -> tuple[Path, dict]:
    arcname = f"{base}.csv"
    target = Path(tmpdir) / arcname

    _verify_sha256(zst_path, sha_path)
    wrote_any_chunk = False
    cell_count = 0
    n_variables: Optional[int] = None
    populations: set[str] = set()

    with open(zst_path, "rb") as fh_in:
        dctx = zstd_module.ZstdDecompressor()
        with (
            dctx.stream_reader(fh_in) as reader,
            io.TextIOWrapper(reader, encoding="utf-8", newline="") as text_reader,
        ):
            for chunk_index, chunk in enumerate(
                pd.read_csv(text_reader, chunksize=TRANSFORM_CHUNK_ROWS)
            ):
                wrote_any_chunk = True
                columns = [str(col) for col in chunk.columns]
                label_index = _find_label_index(columns)
                current_n_variables = (
                    len(columns) if label_index is None else len(columns) - 1
                )
                if n_variables is None:
                    n_variables = current_n_variables
                elif n_variables != current_n_variables:
                    raise ValueError(
                        f"Inconsistent variable count within {arcname}: "
                        f"{current_n_variables} vs {n_variables}."
                    )

                feature_columns = [
                    col
                    for idx, col in enumerate(chunk.columns)
                    if label_index is None or idx != label_index
                ]

                if label_index is not None:
                    label_series = chunk.iloc[:, label_index]
                    normalized = label_series.map(_normalize_population_value)
                    populations.update(
                        value
                        for value in normalized.unique().tolist()
                        if value and value.lower() not in UNLABELED_VALUES
                    )

                if transformation_cofactor is not None and feature_columns:
                    numeric_values = chunk.loc[:, feature_columns].apply(
                        pd.to_numeric, errors="coerce"
                    )
                    transformed_values = np.arcsinh(
                        numeric_values.to_numpy(dtype=np.float64, copy=False)
                        / transformation_cofactor
                    )
                    chunk.loc[:, feature_columns] = transformed_values

                cell_count += len(chunk)
                chunk.to_csv(
                    target,
                    mode="w" if chunk_index == 0 else "a",
                    index=False,
                    header=chunk_index == 0,
                )

    if not wrote_any_chunk:
        raise ValueError(f"CSV file has no rows: {arcname}")

    return target, {
        "sample_name": target.name,
        "cell_count": cell_count,
        "n_variables": n_variables if n_variables is not None else 0,
        "populations": populations,
    }


def _download_prepared_dataset(
    dataset_name: str, data_path: str, transformation_cofactor: Optional[float] = None
) -> Optional[tuple[list[Path], dict]]:
    try:
        prepared_files = _list_prepared_files(dataset_name)
    except Exception as exc:
        print(exc)
        return None

    if not prepared_files:
        print(
            f"No prepared CSV files found in the source repository for '{dataset_name}'."
        )
        return None

    os.makedirs(os.path.dirname(data_path), exist_ok=True)
    added = []
    try:
        import zstandard as zstd  # type: ignore
    except Exception:
        print(
            "Python package 'zstandard' is required to load prepared .csv.zst files.",
            file=sys.stderr,
        )
        return None

    with tempfile.TemporaryDirectory() as tmpdir:
        checksum_specs = [
            (item, Path(tmpdir) / item["repo_path"])
            for item in sorted(prepared_files, key=lambda payload: payload["repo_path"])
            if item.get("kind") == "sha"
        ]
        if not checksum_specs:
            print(
                f"No checksum files found for prepared dataset '{dataset_name}'.",
                file=sys.stderr,
            )
            return None
        if not _download_all(checksum_specs):
            return None

        source_checksums = {
            item["repo_path"]: _read_sha256(dest) for item, dest in checksum_specs
        }
        reused = _reuse_packaged_dataset_if_valid(
            dataset_name,
            data_path,
            transformation_cofactor,
            source_checksums,
        )
        if reused is not None:
            return reused

        download_specs = [
            (item, Path(tmpdir) / item["repo_path"])
            for item in sorted(prepared_files, key=lambda payload: payload["repo_path"])
        ]
        if not _download_all(download_specs):
            return None

        downloaded_by_repo_path = {
            item["repo_path"]: dest for item, dest in download_specs
        }

        data_items = [item for item in prepared_files if item.get("kind") == "data"]
        if not data_items:
            print(
                f"No data files were resolved for prepared dataset '{dataset_name}'.",
                file=sys.stderr,
            )
            return None

        normalize_workers = min(NORMALIZE_MAX_WORKERS, len(data_items))
        normalization_failed = False
        sample_summaries: list[dict] = []
        with ThreadPoolExecutor(max_workers=normalize_workers) as executor:
            future_map = {
                executor.submit(
                    _materialize_prepared_csv,
                    Path(item["repo_path"]).name[: -len(".csv.zst")],
                    downloaded_by_repo_path[item["repo_path"]],
                    downloaded_by_repo_path[_expected_sha_repo_path(item["repo_path"])],
                    tmpdir,
                    zstd,
                    transformation_cofactor,
                ): item["repo_path"]
                for item in sorted(data_items, key=lambda payload: payload["repo_path"])
                if item["repo_path"] in downloaded_by_repo_path
                and _expected_sha_repo_path(item["repo_path"])
                in downloaded_by_repo_path
            }

            missing = [
                item["repo_path"]
                for item in sorted(data_items, key=lambda payload: payload["repo_path"])
                if _expected_sha_repo_path(item["repo_path"])
                not in downloaded_by_repo_path
            ]
            if missing:
                print(
                    "Missing checksum files for prepared inputs: " + ", ".join(missing),
                    file=sys.stderr,
                )
                return None

            for future in as_completed(future_map):
                repo_path = future_map[future]
                try:
                    target, sample_summary = future.result()
                except Exception as exc:
                    print(
                        f"Failed to normalize prepared file '{repo_path}': {exc}",
                        file=sys.stderr,
                    )
                    normalization_failed = True
                    continue
                added.append(target)
                sample_summaries.append(sample_summary)

        if normalization_failed:
            return None

        try:
            dataset_metadata = _build_dataset_metadata(sample_summaries)
        except ValueError as exc:
            print(f"Validation failed: {exc}", file=sys.stderr)
            return None

        platforms: set[str] = set()
        shortnames: set[str] = set()
        for item in prepared_files:
            if item.get("kind") != "data":
                continue
            repo_path = str(item.get("repo_path", ""))
            rel = (
                repo_path[len("prepared/") :]
                if repo_path.startswith("prepared/")
                else ""
            )
            parts = rel.split("/") if rel else []
            if len(parts) == 4:
                platform, _dataset, shortname, _file_name = parts
                platforms.add(platform)
                shortnames.add(shortname)

        if len(platforms) != 1:
            print(
                "Invalid dataset layout: expected exactly one platform for "
                f"dataset '{dataset_name}', found {sorted(platforms)}.",
                file=sys.stderr,
            )
            return None
        dataset_metadata["platform"] = next(iter(platforms))
        dataset_metadata["platforms"] = sorted(platforms)
        dataset_metadata["transformation_cofactor"] = transformation_cofactor

        if shortnames:
            dataset_metadata["shortnames"] = sorted(shortnames)

        abbreviations = _derive_expected_abbreviations(prepared_files)
        if len(abbreviations) != 1:
            print(
                "Invalid dataset layout: expected exactly one shortname/abbreviation "
                f"for dataset '{dataset_name}', found {sorted(abbreviations)}.",
                file=sys.stderr,
            )
            return None
        dataset_metadata["expected_abbreviation"] = abbreviations[0]
        dataset_metadata["expected_abbreviations"] = abbreviations

        with tarfile.open(
            data_path,
            "w:gz",
            compresslevel=DATA_TAR_GZIP_COMPRESSLEVEL,
        ) as tar:
            for p in sorted(added, key=lambda x: x.name):
                tar.add(p, arcname=p.name)
        _write_import_manifest(
            _import_manifest_path(data_path),
            {
                "dataset_name": dataset_name,
                "transformation_cofactor": transformation_cofactor,
                "source_checksums": source_checksums,
                "metadata_payload": {
                    "schema_version": 1,
                    "dataset": dataset_metadata,
                    "samples": {
                        "sample_names": _copy_json_object(
                            dataset_metadata.get("sample_names", [])
                        ),
                        "cells_per_sample": _copy_json_object(
                            dataset_metadata.get("cells_per_sample", [])
                        ),
                        "sample_count": int(
                            dataset_metadata.get("sample_count", len(added))
                        ),
                    },
                    "labels": {
                        "non_target_aliases": sorted(UNLABELED_VALUES),
                    },
                    "stages": {"data_import": {}},
                },
            },
        )
        print(f"Packaged {len(added)} CSV files into {data_path}")
        return added, _populate_metadata_legacy_aliases(
            {
                "schema_version": 1,
                "dataset": dataset_metadata,
                "samples": {
                    "sample_names": _copy_json_object(
                        dataset_metadata.get("sample_names", [])
                    ),
                    "cells_per_sample": _copy_json_object(
                        dataset_metadata.get("cells_per_sample", [])
                    ),
                    "sample_count": int(dataset_metadata.get("sample_count", len(added))),
                },
                "labels": {"non_target_aliases": sorted(UNLABELED_VALUES)},
                "stages": {"data_import": {}},
            }
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download prepared datasets from GitHub and package them into omnibenchmark-ready tarballs."
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default=os.getcwd(),
        help="Directory to store downloaded files (default: current working directory).",
    )
    parser.add_argument(
        "--name",
        type=str,
        default="omni_dataset",
        help="Prefix for the saved files.",
    )

    parser.add_argument(
        "--dataset_name",
        type=str,
        required=True,
        help=(
            "Prepared dataset identifier (e.g. FR-FCM-Z3YR, FR-FCM-Z2KP). "
            "Dataset files are resolved by traversing prepared/ recursively and matching the dataset segment."
        ),
    )
    parser.add_argument(
        "--seed",
        type=int,
        required=True,
        help="Random seed used to generate the file order output.",
    )
    parser.add_argument(
        "--sub-sampling",
        type=int,
        default=0,
        help="Record sub-sampling level (0 means no sub-sampling).",
    )
    parser.add_argument(
        "--transformation-cofactor",
        type=float,
        default=None,
        help=(
            "Optional arcsinh cofactor. When set, apply arcsinh(x / cofactor) to all "
            "non-label columns before packaging the imported CSVs."
        ),
    )
    parser.add_argument(
        "--potential-batches",
        type=int,
        default=None,
        help=(
            "Optional count of potential batches present in the dataset. "
            "When set, record it in dataset metadata for downstream stages."
        ),
    )

    try:
        return parser.parse_args()
    except SystemExit:
        # Allow showing help without a stacktrace when no args are passed.
        parser.print_help()
        sys.exit(0)


def main() -> None:
    args = parse_args()
    if args.transformation_cofactor is not None and args.transformation_cofactor <= 0:
        raise ValueError(
            "--transformation-cofactor must be greater than 0 when provided."
        )
    if args.potential_batches is not None and args.potential_batches <= 0:
        raise ValueError("--potential-batches must be greater than 0 when provided.")
    outdir = args.output_dir
    data_filename = f"{args.name}.data.tar.gz"
    data_path = os.path.abspath(os.path.join(outdir, data_filename))

    downloaded = _download_prepared_dataset(
        args.dataset_name,
        data_path,
        transformation_cofactor=args.transformation_cofactor,
    )
    if downloaded is not None:
        csv_paths, metadata = downloaded
        attachments_path = os.path.abspath(
            os.path.join(outdir, f"{args.name}.attachments.gz")
        )
        with gzip.open(attachments_path, "wb") as lh:
            lh.write(b"")
        print(f"Wrote empty attachments file: {attachments_path}")

        order = list(range(1, len(csv_paths) + 1))
        random.Random(args.seed).shuffle(order)
        metadata_path = os.path.abspath(
            os.path.join(outdir, f"{args.name}.metadata.json.gz")
        )
        metadata_payload = _build_metadata_payload(
            dataset_metadata=metadata["dataset"] if "dataset" in metadata else metadata,
            dataset_name=args.dataset_name,
            order=order,
            seed=args.seed,
            sub_sampling=args.sub_sampling,
            potential_batches=args.potential_batches,
        )
        with gzip.open(metadata_path, "wt", encoding="utf-8") as oh:
            json.dump(metadata_payload, oh, indent=2)
        print(f"Wrote metadata file: {metadata_path}")
        print(f"Dataset saved to: {data_path}")
        return

    sys.exit(1)


if __name__ == "__main__":
    main()
