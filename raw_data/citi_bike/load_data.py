"""
Downloader for Citi Bike tripdata (years 2023-2025).
Lists the official S3 bucket, stores the ZIP files under the existing year
folders, and extracts the contained CSVs into the same folders. Re-runs are
safe: existing ZIPs/CSVs are skipped.
"""

from __future__ import annotations

import argparse
import re
import shutil
import ssl
import sys
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Iterable, Sequence
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BUCKET_ROOT = "https://s3.amazonaws.com/tripdata"
TARGET_YEARS = (2023, 2024, 2025)
BASE_DIR = Path(__file__).parent


def _extract_year(name: str) -> int:
    match = re.search(r"(20\d{2})", name)
    if not match:
        raise ValueError(f"Cannot determine year from: {name}")
    return int(match.group(1))


def _ensure_year_dir(year: int) -> Path:
    dest = BASE_DIR / f"{year}-citibike-tripdata"
    dest.mkdir(parents=True, exist_ok=True)
    return dest


def fetch_zip_urls(
    bucket_url: str, years: Iterable[int], ssl_context: ssl.SSLContext | None = None
) -> list[str]:
    urls: set[str] = set()
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    for year in years:
        list_url = f"{bucket_url}?prefix={year}"
        req = Request(list_url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urlopen(req, context=ssl_context) as resp:
                xml_bytes = resp.read()
        except (HTTPError, URLError) as exc:
            print(f"Could not list bucket for {year}: {exc}")
            continue

        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError as exc:
            print(f"Could not parse bucket listing for {year}: {exc}")
            continue

        for key_el in root.findall("s3:Contents/s3:Key", ns):
            key = (key_el.text or "").strip()
            if not key:
                continue
            if "citibike-tripdata" not in key or not key.lower().endswith(".zip"):
                continue
            urls.add(f"{bucket_url}/{key}")

    return sorted(urls)


def download_zip(
    url: str, dest_dir: Path, ssl_context: ssl.SSLContext | None = None
) -> Path | None:
    zip_name = url.rsplit("/", 1)[-1]
    dest_path = dest_dir / zip_name
    if dest_path.exists() and dest_path.stat().st_size > 0:
        print(f"Skip download (already present): {dest_path}")
        return dest_path

    print(f"Downloading {url}")
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, context=ssl_context) as resp, dest_path.open("wb") as f:
            shutil.copyfileobj(resp, f)
        print(f"Saved to {dest_path}")
        return dest_path
    except (HTTPError, URLError) as exc:
        print(f"Download failed for {url}: {exc}")
        return None


def extract_zip(zip_path: Path, dest_dir: Path) -> list[Path]:
    extracted: list[Path] = []
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            target = dest_dir / Path(info.filename).name
            if target.exists() and target.stat().st_size > 0:
                print(f"Skip extract (already present): {target}")
                continue
            with zf.open(info) as src, target.open("wb") as dst:
                shutil.copyfileobj(src, dst)
            print(f"Extracted {target}")
            extracted.append(target)
    return extracted


def main(years: Sequence[int]) -> None:
    ssl_context = ssl._create_unverified_context()
    urls = fetch_zip_urls(
        bucket_url=BUCKET_ROOT, years=years, ssl_context=ssl_context
    )
    if not urls:
        print("No matching ZIP links found. Check bucket URL or requested years.")
        sys.exit(1)

    for url in urls:
        year = _extract_year(url)
        dest_dir = _ensure_year_dir(year)
        zip_path = download_zip(url, dest_dir, ssl_context=ssl_context)
        if zip_path:
            extract_zip(zip_path, dest_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download and unpack Citi Bike tripdata ZIPs (2023-2025) into raw_data/citi_bike."
    )
    parser.add_argument(
        "--years",
        nargs="*",
        type=int,
        default=TARGET_YEARS,
        help="Years to download (default: 2023 2024 2025)",
    )
    args = parser.parse_args()
    main(tuple(args.years))
