import argparse
import json
import os
import sys
import tarfile
import tempfile
import urllib.error
import urllib.parse
import urllib.request

# TODO: Point this to the repository that hosts your datasets.
# The value below is just a placeholder and should be edited.
BASE_URL = "https://github.com/kaae-2/ob-flow-datasets/raw/main"


def download_file(url: str, dest_path: str, chunk_size: int = 8192) -> bool:
    """
    Download a file from a URL to a destination path in chunks.
    """
    if not url or not dest_path:
        raise ValueError("Both url and dest_path must be provided.")

    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

    try:
        with urllib.request.urlopen(url) as response, open(dest_path, "wb") as out_file:
            # report on destination directory
            dest_dir = os.path.dirname(dest_path) or "."
            if os.path.isdir(dest_dir):
                print(f"Destination directory found: {dest_dir}")
            else:
                print(f"Destination directory not found: {dest_dir}")

            # report on response availability and HTTP status
            if response:
                status = None
                try:
                    status = response.getcode()
                except Exception:
                    status = getattr(response, "status", None)
                if status is not None:
                    print(f"Response received, HTTP status: {status}")
                else:
                    print("Response object received (no status code available).")
            else:
                print("No response received from URL.")
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


def _str_to_bool(value: str) -> bool:
    """Convert 'true'/'false' (case-insensitive) to bool or raise ArgumentTypeError."""
    if isinstance(value, bool):
        return value
    val = str(value).strip().lower()
    if val == "true":
        return True
    if val == "false":
        return False
    raise argparse.ArgumentTypeError("Invalid boolean value: must be 'true' or 'false'")


def _extract_repo_info(base_url: str):
    """
    Attempt to extract GitHub repo owner/repo/branch from a raw URL.
    Returns a dict with keys owner, repo, branch or None if parsing fails.
    """
    parsed = urllib.parse.urlparse(base_url)
    parts = parsed.path.strip("/").split("/")
    if parsed.netloc == "github.com" and len(parts) >= 4 and parts[2] == "raw":
        owner, repo, _, branch = parts[:4]
        return {"owner": owner, "repo": repo, "branch": branch}
    return None


def _list_covid_files() -> list[dict]:
    """
    List the COVID folder contents on GitHub and return file metadata dicts.
    Each dict contains 'name' and 'url'.
    """
    repo_info = _extract_repo_info(BASE_URL)
    if not repo_info:
        raise ValueError("BASE_URL must be a GitHub raw URL to list COVID files automatically.")

    list_url = (
        "https://api.github.com/repos/"
        f"{repo_info['owner']}/{repo_info['repo']}/contents/data/covid"
        f"?ref={repo_info['branch']}"
    )

    try:
        with urllib.request.urlopen(list_url) as response:
            payload = json.loads(response.read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP error while listing COVID files: {e.code} {e.reason}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Network error while listing COVID files: {e.reason}") from e
    except Exception as e:
        raise RuntimeError(f"Unexpected error while listing COVID files: {e}") from e

    files = []
    if isinstance(payload, list):
        for item in payload:
            if item.get("type") != "file":
                continue
            name = item.get("name")
            if not name or not name.lower().endswith(".fcs"):
                continue
            download_url = item.get("download_url") or f"{BASE_URL}/data/covid/{name}"
            files.append({"name": name, "url": download_url})
    return files


def download_covid_dataset(data_path: str) -> bool:
    """
    Download all FCS files in the COVID subfolder, then bundle them into a tar.gz.
    """
    try:
        covid_files = _list_covid_files()
    except Exception as exc:
        print(exc)
        return False

    if not covid_files:
        print("No COVID FCS files found in the source repository.")
        return False

    os.makedirs(os.path.dirname(data_path), exist_ok=True)
    with tempfile.TemporaryDirectory() as tmpdir:
        downloaded_paths = []
        for file_info in covid_files:
            dest = os.path.join(tmpdir, file_info["name"])
            if not download_file(file_info["url"], dest):
                return False
            downloaded_paths.append(dest)

        with tarfile.open(data_path, "w:gz") as tar:
            for file_path in downloaded_paths:
                tar.add(file_path, arcname=os.path.basename(file_path))

    print(f"Downloaded and packaged {len(covid_files)} COVID files into {data_path}")
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch omnibenchmark-ready datasets.")
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
        "--transformed",
        type=_str_to_bool,
        default=False,
        help="If set to 'true', download the transformed variant instead of raw. Accepts 'true' or 'false'.",
        metavar="true|false",
    )
    parser.add_argument(
        "--dataset_name",
        type=str,
        required=True,
        help="Identifier of the dataset to fetch from the source repository.",
    )

    try:
        return parser.parse_args()
    except SystemExit:
        # Allow showing help without a stacktrace when no args are passed.
        parser.print_help()
        sys.exit(0)


def prepare_dataset_name(args):
    if not getattr(args, "dataset_name", None):
        raise ValueError("dataset_name must be provided")
    if args.transformed:
        return f"{args.dataset_name}.fcs"
    else:
        return f"{args.dataset_name}_notransform.fcs"


def prepare_labels_name(args):
    """
    Return the labels filename (without extension) based on dataset_name.
    Matches are case-insensitive.
    """
    if not getattr(args, "dataset_name", None):
        raise ValueError("dataset_name must be provided")

    ds = args.dataset_name.lower()
    if ds == "covid":
        return "01-May-2020_Human_COVID_analysis_template.wsp"
    if "levine" in ds or "samusik" in ds:
        # e.g. "Levine_13dim" -> "Levine_13dim_labels"
        return f"population_names_{args.dataset_name}.txt"
    elif "mosmann" in ds or "nilsson" in ds:
        # e.g. "Mosmann_immune" -> "Mosmann_immune.labels"
        return f"GatingML_{args.dataset_name}.xml"
    else:
        return None


def main() -> None:
    args = parse_args()

    if args.dataset_name.lower() == "covid":
        data_filename = f"{args.name}.data.gz"
        data_path = os.path.abspath(os.path.join(args.output_dir, data_filename))
        if not download_covid_dataset(data_path):
            sys.exit(1)
        print(f"Dataset saved to: {data_path}")

        labels = prepare_labels_name(args)
        labels = f"{BASE_URL}/attachments/{labels}" if labels else None
        if labels:
            labels_filename = f"{args.name}.input_labels.gz"
            labels_path = os.path.abspath(os.path.join(args.output_dir, labels_filename))
            if not download_file(labels, labels_path):
                sys.exit(1)
            print(f"Labels saved to: {labels_path}")
        return

    name = prepare_dataset_name(args)
    labels = prepare_labels_name(args)

    counts = f"{BASE_URL}/data/{name}.gz"
    # Attachments are plain text, not gzipped on the source; do not append .gz.
    labels = f"{BASE_URL}/attachments/{labels}" if labels else None

    data_filename = f"{args.name}.data.gz"
    data_path = os.path.abspath(os.path.join(args.output_dir, data_filename))
    if not download_file(counts, data_path):
        sys.exit(1)
    print(f"Dataset saved to: {data_path}")
    if labels:
        labels_filename = f"{args.name}.input_labels.gz"
        labels_path = os.path.abspath(os.path.join(args.output_dir, labels_filename))
        if not download_file(labels, labels_path):
            sys.exit(1)
        print(f"Labels saved to: {labels_path}")


if __name__ == "__main__":
    main()
