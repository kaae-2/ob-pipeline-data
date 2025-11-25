import argparse
import os
import sys
import urllib.error
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
        action="store_true",
        help="If set, download the transformed variant instead of raw.",
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

    name = prepare_dataset_name(args)
    labels = prepare_labels_name(args)

    counts = f"{BASE_URL}/data/{name}.gz"
    labels = f"{BASE_URL}/attachments/{labels}.gz" if labels else None

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
