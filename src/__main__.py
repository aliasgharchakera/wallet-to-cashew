"""Main entry point for the wallet-to-cashew migration pipeline."""

from __future__ import annotations

import argparse
import sys

from src.extract import extract, load_from_raw
from src.generate import generate
from src.photos import download_photos
from src.transform import transform
from src.validate import validate


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate Wallet (Budget Bakers) data to Cashew")
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Skip API extraction and use previously saved raw data",
    )
    parser.add_argument(
        "--skip-photos",
        action="store_true",
        help="Skip downloading photo attachments",
    )
    parser.add_argument(
        "--output",
        default="cashew-migrated.db",
        help="Output database filename (default: cashew-migrated.db)",
    )
    args = parser.parse_args()

    # Step 1: Extract
    if args.skip_extract:
        print("Skipping extraction, loading from raw files...")
        bb_data = load_from_raw()
    else:
        bb_data = extract()

    # Step 2: Download photos
    if not args.skip_photos:
        download_photos(bb_data.records)
    else:
        print("\nSkipping photo download.")

    # Step 3: Transform
    cashew_data = transform(bb_data)

    # Step 4: Generate SQLite
    db_path = generate(cashew_data, args.output)

    # Step 5: Validate
    success = validate(bb_data, cashew_data, db_path)

    if success:
        print(f"\nMigration complete! Import {db_path} into Cashew via Settings → Backup → Import.")
    else:
        print("\nMigration completed with issues. Review the report above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
