import argparse
import argparse
import gzip
import json
import os
import random
import shutil
import sys
import tarfile
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import BinaryIO, Optional, cast

# Base URL for raw downloads (GitHub raw endpoint via github.com)
BASE_URL = "https://github.com/kaae-2/ob-flow-datasets/raw/main"


def download_file(url: str, dest_path: str, chunk_size: int = 8192) -> bool:
    if not url or not dest_path:
        raise ValueError("Both url and dest_path must be provided.")

    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    try:
        with urllib.request.urlopen(url) as response, open(dest_path, "wb") as out_file:
            while chunk := response.read(chunk_size):
                out_file.write(chunk)
        print(f"Downloaded {url} -> {dest_path}")
        return True
    except urllib.error.HTTPError as e:
        print(f"HTTP error for {url}: {e.code} {e.reason}")
    except urllib.error.URLError as e:
        print(f"Network error for {url}: {e.reason}")
    except Exception as e:
        print(f"Unexpected error for {url}: {e}")
    return False


def _extract_repo_info(base_url: str):
    parsed = urllib.parse.urlparse(base_url)
    parts = parsed.path.strip("/").split("/")
    if parsed.netloc == "github.com" and len(parts) >= 4 and parts[2] == "raw":
        owner, repo, _, branch = parts[:4]
        return {"owner": owner, "repo": repo, "branch": branch}
    return None


def _list_prepared_files(dataset_name: str) -> list[dict]:
    repo_info = _extract_repo_info(BASE_URL)
    if not repo_info:
        raise ValueError("BASE_URL must be a GitHub raw URL to list prepared files.")

    list_url = (
        "https://api.github.com/repos/"
        f"{repo_info['owner']}/{repo_info['repo']}/contents/prepared/{dataset_name}"
        f"?ref={repo_info['branch']}"
    )

    try:
        with urllib.request.urlopen(list_url) as response:
            payload = json.loads(response.read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP error while listing prepared files: {e.code} {e.reason}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Network error while listing prepared files: {e.reason}") from e
    except Exception as e:
        raise RuntimeError(f"Unexpected error while listing prepared files: {e}") from e

    files = []
    if isinstance(payload, list):
        for item in payload:
            if item.get("type") != "file":
                continue
            name = item.get("name")
            if not name:
                continue
            lower = name.lower()
            if lower.endswith(".sha256"):
                continue
            if not (lower.endswith(".csv") or lower.endswith(".csv.gz") or lower.endswith(".csv.zst")):
                continue
            download_url = item.get("download_url") or f"{BASE_URL}/prepared/{dataset_name}/{name}"
            files.append({"name": name, "url": download_url})
    return files


def _download_prepared_dataset(dataset_name: str, data_path: str) -> Optional[list[Path]]:
    try:
        prepared_files = _list_prepared_files(dataset_name)
    except Exception as exc:
        print(exc)
        return None

    if not prepared_files:
        print(f"No prepared CSV files found in the source repository for '{dataset_name}'.")
        return None

    os.makedirs(os.path.dirname(data_path), exist_ok=True)
    tmpdir = tempfile.mkdtemp()
    added = []
    try:
        zstd = None  # type: Optional[object]
        try:
            import zstandard as zstd  # type: ignore

            zstd_available = True
        except Exception:
            zstd_available = False

        downloaded_paths: list[Path] = []
        for item in prepared_files:
            dest = Path(tmpdir) / item["name"]
            if not download_file(item["url"], str(dest)):
                return None
            downloaded_paths.append(dest)

        by_base: dict[str, list[Path]] = {}
        for p in downloaded_paths:
            base = p.name
            for s in (".csv", ".csv.gz", ".csv.zst"):
                if base.lower().endswith(s):
                    base = base[: -len(s)]
                    break
            by_base.setdefault(base, []).append(p)

        for base, paths in sorted(by_base.items()):
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
                chosen = (paths[0], None)

            src, typ = chosen
            arcname = f"{base}.csv"
            target = Path(tmpdir) / arcname

            if typ == "csv":
                shutil.copy2(src, target)
            elif typ == "gz":
                with gzip.open(src, "rb") as fh_in, open(target, "wb") as fh_out:
                    shutil.copyfileobj(cast(BinaryIO, fh_in), fh_out)
            elif typ == "zst":
                if zstd_available and zstd is not None:
                    with open(src, "rb") as fh_in, open(target, "wb") as fh_out:
                        dctx = zstd.ZstdDecompressor()
                        dctx.copy_stream(fh_in, fh_out)
                else:
                    print(
                        "Error: found .zst file but Python package 'zstandard' is not installed; cannot decompress."
                    )
                    return None
            else:
                shutil.copy2(src, target)

            added.append(target)

        with tarfile.open(data_path, "w:gz") as tar:
            for p in sorted(added, key=lambda x: x.name):
                tar.add(p, arcname=p.name)
        print(f"Packaged {len(added)} CSV files into {data_path}")
        return added
    finally:
        shutil.rmtree(tmpdir)


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
        help="Prepared dataset folder name under the GitHub repo prepared/ directory.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        required=True,
        help="Random seed used to generate the file order output.",
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

    downloaded = _download_prepared_dataset(args.dataset_name, data_path)
    if downloaded is not None:
        attachments_path = os.path.abspath(os.path.join(outdir, f"{args.name}.attachments.gz"))
        with gzip.open(attachments_path, "wb") as lh:
            lh.write(b"")
        print(f"Wrote empty attachments file: {attachments_path}")

        order = list(range(1, len(downloaded) + 1))
        random.Random(args.seed).shuffle(order)
        order_path = os.path.abspath(os.path.join(outdir, f"{args.name}.order.json.gz"))
        with gzip.open(order_path, "wt", encoding="utf-8") as oh:
            json.dump({"order": order}, oh)
        print(f"Wrote order file: {order_path}")
        print(f"Dataset saved to: {data_path}")
        return

    sys.exit(1)


if __name__ == "__main__":
    main()
