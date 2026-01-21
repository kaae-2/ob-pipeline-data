import argparse
import gzip
import os
import shutil
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import BinaryIO, Optional, cast


def _package_local_prepared_dataset(dataset_name: str, data_path: str) -> bool:
    """
    Package a local prepared dataset (datasets/prepared/<dataset_name>*) into a single tar.gz
    containing CSV files. If compressed variants exist, prefer uncompressed CSV, then
    .csv.gz (decompress), then .csv.zst (decompress if zstandard is available), otherwise
    include the compressed file as a fallback.

    Also emit an empty gzipped labels file alongside the dataset tarball.
    """
    repo_root = Path(__file__).resolve().parents[1]
    prepared_dir = repo_root / "datasets" / "prepared"
    if not prepared_dir.exists():
        print(f"No prepared datasets directory found at: {prepared_dir}")
        return False

    # Find matching subfolders
    # Exact match only (case-insensitive)
    matches = [p for p in prepared_dir.iterdir() if p.is_dir() and p.name.lower() == dataset_name.lower()]
    if not matches:
        print(f"No prepared dataset folder exactly matching '{dataset_name}' in {prepared_dir}")
        return False

    folder = matches[0]
    files = list(folder.iterdir())
    # Group by base name (without compression suffixes)
    by_base = {}
    for p in files:
        if p.name.endswith(".sha256"):
            continue
        name = p.name
        base = name
        for s in (".csv", ".csv.gz", ".csv.zst", ".csv.gz.sha256", ".csv.zst.sha256"):
            if base.lower().endswith(s):
                base = base[: -len(s)]
                break
        by_base.setdefault(base, []).append(p)

    if not by_base:
        print(f"No CSV-like files found in prepared folder: {folder}")
        return False

    os.makedirs(os.path.dirname(data_path), exist_ok=True)
    tmpdir = tempfile.mkdtemp()
    added = []
    try:
        # Try import zstandard optionally
        zstd = None  # type: Optional[object]
        try:
            import zstandard as zstd  # type: ignore

            zstd_available = True
        except Exception:
            zstd_available = False

        for base, paths in sorted(by_base.items()):
            # prefer uncompressed .csv
            chosen = None
            for p in paths:
                if p.name.lower().endswith(".csv") and not p.name.lower().endswith(".csv.gz"):
                    chosen = (p, "csv")
                    break
            if chosen is None:
                for p in paths:
                    if p.name.lower().endswith(".csv.gz"):
                        chosen = (p, "gz")
                        break
            if chosen is None:
                for p in paths:
                    if p.name.lower().endswith(".csv.zst"):
                        chosen = (p, "zst")
                        break
            if chosen is None:
                # take any file otherwise
                chosen = (paths[0], None)

            src, typ = chosen
            arcname = f"{base}.csv"
            target = Path(tmpdir) / arcname

            if typ == "csv":
                shutil.copy2(src, target)
            elif typ == "gz":
                # use binary-safe copy
                with gzip.open(src, "rb") as fh_in, open(target, "wb") as fh_out:
                    # mypy/LS tools may widen types; cast to BinaryIO to satisfy typing
                    shutil.copyfileobj(cast(BinaryIO, fh_in), fh_out)
            elif typ == "zst":
                # Always produce a decompressed CSV inside the tarball.
                if zstd_available and zstd is not None:
                    with open(src, "rb") as fh_in, open(target, "wb") as fh_out:
                        dctx = zstd.ZstdDecompressor()
                        dctx.copy_stream(fh_in, fh_out)
                else:
                    print(
                        "Error: found .zst file but Python package 'zstandard' is not installed; cannot decompress."
                    )
                    return False
            else:
                shutil.copy2(src, target)

            added.append(target)

        # create tar.gz
        with tarfile.open(data_path, "w:gz") as tar:
            for p in sorted(added, key=lambda x: x.name):
                tar.add(p, arcname=p.name)

        print(f"Packaged {len(added)} CSV files from {folder} into {data_path}")

        # write empty labels gz file next to the dataset
        labels_path = Path(data_path).with_name(Path(data_path).stem + ".input_labels.gz")
        with gzip.open(labels_path, "wb") as lh:
            # explicitly write zero bytes (empty file)
            lh.write(b"")
        print(f"Wrote empty labels file: {labels_path}")
        return True
    finally:
        shutil.rmtree(tmpdir)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Package prepared datasets into omnibenchmark-ready tarballs."
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
        help="Exact prepared dataset folder name under datasets/prepared/.",
    )

    try:
        return parser.parse_args()
    except SystemExit:
        # Allow showing help without a stacktrace when no args are passed.
        parser.print_help()
        sys.exit(0)


def main() -> None:
    args = parse_args()
    outdir = args.output_dir
    data_filename = f"{args.name}.data.gz"
    data_path = os.path.abspath(os.path.join(outdir, data_filename))

    # package prepared dataset only
    if _package_local_prepared_dataset(args.dataset_name, data_path):
        print(f"Dataset saved to: {data_path}")
        return

    sys.exit(1)


if __name__ == "__main__":
    main()
