from pathlib import Path
import requests
from tqdm import tqdm

requests.packages.urllib3.disable_warnings()

DATA_DIR = Path("./data")


def download_files(urls: list[str], target_dir: Path, skip_existing: bool = True):
    """Downloads files from a list of urls."""

    target_dir.mkdir(parents=True, exist_ok=True)

    for url in tqdm(urls):
        url = url.strip()

        target = target_dir.joinpath(url.split("/")[-1])

        # Skip existing downloads
        if skip_existing and target.exists():
            continue

        # print(f"Fetching: {url}")
        response = requests.get(url, verify=False)
        response.raise_for_status()
        target.write_bytes(response.content)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Download AHN3 or AHN4 data for the given filenames.",
    )
    parser.add_argument(
        "--dataset",
        choices=["AHN3", "AHN4"],
        help="specify dataset to download",
    )
    parser.add_argument(
        "--filenames",
        nargs="+",
        help="specify filenames to download, omit to download the full dataset",
    )
    parser.print_help()
    print("\n")

    args = parser.parse_args()
    print(args)

    filenames = (
        Path("./scripts/ahn.txt").open("r").readlines()
        if args.filenames is None
        else args.filenames
    )

    match args.dataset:
        case "AHN3":
            urls = [
                f"https://ns_hwh.fundaments.nl/hwh-ahn/AHN3/LAZ/{f.strip()}.LAZ"
                for f in filenames
            ]
        case "AHN4":
            urls = [
                f"https://ns_hwh.fundaments.nl/hwh-ahn/ahn4/01_LAZ/{f.strip()}.LAZ"
                for f in filenames
            ]

    download_files(urls, DATA_DIR.joinpath(args.dataset))
