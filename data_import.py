import argparse
import csv
import gzip
import hashlib
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


def download_file(url: str, dest_path: str, chunk_size: int = DOWNLOAD_CHUNK_SIZE) -> bool:
    if not url or not dest_path:
        raise ValueError("Both url and dest_path must be provided.")

    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            with urllib.request.urlopen(url) as response, open(dest_path, "wb") as out_file:
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
        raise RuntimeError(f"Network error while listing prepared files: {e.reason}") from e
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
        raise ValueError(
            f"SHA256 mismatch (expected {expected}, got {actual})."
        )


def _find_label_index(header: list[str]) -> Optional[int]:
    lower_map = {str(col).strip().lower(): idx for idx, col in enumerate(header)}
    for candidate in LABEL_COLUMN_CANDIDATES:
        if candidate in lower_map:
            return lower_map[candidate]
    return None


def _transform_csv_with_arcsinh(path: Path, cofactor: float) -> None:
    temp_path = path.with_name(f"{path.name}.transforming")
    wrote_any_chunk = False

    for chunk_index, chunk in enumerate(pd.read_csv(path, chunksize=TRANSFORM_CHUNK_ROWS)):
        wrote_any_chunk = True
        columns = [str(col) for col in chunk.columns]
        label_index = _find_label_index(columns)
        feature_columns = [
            col for idx, col in enumerate(chunk.columns) if label_index is None or idx != label_index
        ]

        if feature_columns:
            numeric_values = chunk.loc[:, feature_columns].apply(pd.to_numeric, errors="coerce")
            transformed_values = np.arcsinh(
                numeric_values.to_numpy(dtype=np.float64, copy=False) / cofactor
            )
            chunk.loc[:, feature_columns] = transformed_values

        chunk.to_csv(
            temp_path,
            mode="w" if chunk_index == 0 else "a",
            index=False,
            header=chunk_index == 0,
        )

    if not wrote_any_chunk:
        raise ValueError(f"CSV file has no rows: {path.name}")

    temp_path.replace(path)


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


def _validate_and_collect_dataset_metadata(csv_paths: list[Path]) -> dict:
    sorted_paths = sorted(csv_paths, key=lambda p: p.name)
    if not sorted_paths:
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

    scanned_by_name: dict[str, dict] = {}
    max_workers = min(METADATA_MAX_WORKERS, len(sorted_paths))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_scan_csv_file, path): path
            for path in sorted_paths
        }
        for future in as_completed(future_map):
            path = future_map[future]
            try:
                scanned_by_name[path.name] = future.result()
            except Exception as exc:
                raise ValueError(f"Failed metadata scan for {path.name}: {exc}") from exc

    for path in sorted_paths:
        scanned = scanned_by_name[path.name]
        n_variables = int(scanned["n_variables"])
        if expected_variables is None:
            expected_variables = n_variables
        elif expected_variables != n_variables:
            raise ValueError(
                "Inconsistent variable count: "
                f"{path.name} has {n_variables}, expected {expected_variables}."
            )

        sample_names.append(path.name)
        cells_per_sample.append(int(scanned["cell_count"]))
        populations.update(scanned["populations"])

    return {
        "sample_count": len(sorted_paths),
        "sample_names": sample_names,
        "cells_per_sample": cells_per_sample,
        "n_variables": expected_variables if expected_variables is not None else 0,
        "population_count": len(populations),
    }


def _materialize_prepared_csv(
    base: str,
    zst_path: Path,
    sha_path: Path,
    tmpdir: str,
    zstd_module,
    transformation_cofactor: Optional[float] = None,
) -> Path:
    arcname = f"{base}.csv"
    target = Path(tmpdir) / arcname

    _verify_sha256(zst_path, sha_path)
    with open(zst_path, "rb") as fh_in, open(target, "wb") as fh_out:
        dctx = zstd_module.ZstdDecompressor()
        dctx.copy_stream(fh_in, fh_out)

    if transformation_cofactor is not None:
        _transform_csv_with_arcsinh(target, transformation_cofactor)

    return target


def _download_prepared_dataset(
    dataset_name: str, data_path: str, transformation_cofactor: Optional[float] = None
) -> Optional[tuple[list[Path], dict]]:
    try:
        prepared_files = _list_prepared_files(dataset_name)
    except Exception as exc:
        print(exc)
        return None

    if not prepared_files:
        print(f"No prepared CSV files found in the source repository for '{dataset_name}'.")
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

        download_specs = [
            (item, Path(tmpdir) / item["repo_path"])
            for item in sorted(prepared_files, key=lambda payload: payload["repo_path"])
        ]
        if not download_specs:
            print(
                f"No downloadable prepared files found for '{dataset_name}'.",
                file=sys.stderr,
            )
            return None
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
        if failed_download:
            return None

        downloaded_paths = [dest for _, dest in download_specs]

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
                and _expected_sha_repo_path(item["repo_path"]) in downloaded_by_repo_path
            }

            missing = [
                item["repo_path"]
                for item in sorted(data_items, key=lambda payload: payload["repo_path"])
                if _expected_sha_repo_path(item["repo_path"]) not in downloaded_by_repo_path
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
                    target = future.result()
                except Exception as exc:
                    print(
                        f"Failed to normalize prepared file '{repo_path}': {exc}",
                        file=sys.stderr,
                    )
                    normalization_failed = True
                    continue
                added.append(target)

        if normalization_failed:
            return None

        try:
            metadata = _validate_and_collect_dataset_metadata(added)
        except ValueError as exc:
            print(f"Validation failed: {exc}", file=sys.stderr)
            return None

        platforms: set[str] = set()
        shortnames: set[str] = set()
        for item in prepared_files:
            if item.get("kind") != "data":
                continue
            repo_path = str(item.get("repo_path", ""))
            rel = repo_path[len("prepared/") :] if repo_path.startswith("prepared/") else ""
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
        metadata["platform"] = next(iter(platforms))
        metadata["platforms"] = sorted(platforms)
        metadata["transformation_cofactor"] = transformation_cofactor

        if shortnames:
            metadata["shortnames"] = sorted(shortnames)

        abbreviations = _derive_expected_abbreviations(prepared_files)
        if len(abbreviations) != 1:
            print(
                "Invalid dataset layout: expected exactly one shortname/abbreviation "
                f"for dataset '{dataset_name}', found {sorted(abbreviations)}.",
                file=sys.stderr,
            )
            return None
        metadata["expected_abbreviation"] = abbreviations[0]
        metadata["expected_abbreviations"] = abbreviations

        with tarfile.open(
            data_path,
            "w:gz",
            compresslevel=DATA_TAR_GZIP_COMPRESSLEVEL,
        ) as tar:
            for p in sorted(added, key=lambda x: x.name):
                tar.add(p, arcname=p.name)
        print(f"Packaged {len(added)} CSV files into {data_path}")
        return added, metadata


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
            "When set, record it in order metadata for downstream stages."
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
        raise ValueError("--transformation-cofactor must be greater than 0 when provided.")
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
        attachments_path = os.path.abspath(os.path.join(outdir, f"{args.name}.attachments.gz"))
        with gzip.open(attachments_path, "wb") as lh:
            lh.write(b"")
        print(f"Wrote empty attachments file: {attachments_path}")

        order = list(range(1, len(csv_paths) + 1))
        random.Random(args.seed).shuffle(order)
        order_path = os.path.abspath(os.path.join(outdir, f"{args.name}.order.json.gz"))
        metadata["sub_sampling"] = args.sub_sampling
        metadata["dataset_name"] = args.dataset_name
        metadata["potential_batches"] = args.potential_batches
        with gzip.open(order_path, "wt", encoding="utf-8") as oh:
            json.dump({"order": order, "metadata": metadata}, oh)
        print(f"Wrote order file: {order_path}")
        print(f"Dataset saved to: {data_path}")
        return

    sys.exit(1)


if __name__ == "__main__":
    main()
